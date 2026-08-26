"""Deterministic in-memory stock-client doubles for workflow tests."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException


@dataclass
class TableState:
    identifier: tuple[str, ...]
    metadata_location: str
    schema_names: list[str]
    properties: dict[str, str]
    table_uuid: uuid.UUID
    rows: list[dict[str, Any]] = field(default_factory=list)
    generation: int = 0
    snapshots: list[int] = field(default_factory=list)
    config: dict[str, str] = field(
        default_factory=lambda: {
            "s3.access-key-id": "not-persisted",
            "s3.secret-access-key": "not-persisted",
            "s3.session-token": "not-persisted",
        }
    )

    def advance(self) -> None:
        self.generation += 1
        root = self.metadata_location.rsplit("/", 1)[0]
        self.metadata_location = f"{root}/{self.generation:05d}.metadata.json"


class FakeScan:
    def __init__(self, state: TableState) -> None:
        self.state = state

    def to_arrow(self) -> pa.Table:
        columns = {
            name: [row.get(name) for row in self.state.rows]
            for name in self.state.schema_names
        }
        return pa.table(columns)


class FakeTransaction:
    def __init__(self, table: FakeTable) -> None:
        self.table = table
        self.updates: dict[str, str] = {}
        self.removals: set[str] = set()

    def set_properties(self, properties: Mapping[str, str]) -> FakeTransaction:
        self.updates.update(properties)
        return self

    def remove_properties(self, *removals: str) -> FakeTransaction:
        self.removals.update(removals)
        return self

    def commit_transaction(self) -> FakeTable:
        self.table.state.properties.update(self.updates)
        for key in self.removals:
            self.table.state.properties.pop(key, None)
        self.table.state.advance()
        return self.table.refresh()


class FakeSchemaUpdate:
    def __init__(self, table: FakeTable) -> None:
        self.table = table
        self.field: str | None = None

    def add_column(
        self, name: str, _field_type: Any, *, required: bool
    ) -> FakeSchemaUpdate:
        assert required is False
        self.field = name
        return self

    def commit(self) -> None:
        assert self.field is not None
        self.table.state.schema_names.append(self.field)
        for row in self.table.state.rows:
            row[self.field] = None
        self.table.state.advance()
        self.table.refresh()


class FakeTable:
    def __init__(self, catalog: FakeCatalog, state: TableState) -> None:
        self.catalog = catalog
        self.state = state
        self.loaded_generation = state.generation

    @property
    def metadata(self) -> Any:
        return SimpleNamespace(
            table_uuid=self.state.table_uuid,
            properties=self.state.properties,
        )

    @property
    def metadata_location(self) -> str:
        return self.state.metadata_location

    @property
    def config(self) -> dict[str, str]:
        return self.state.config

    def schema(self) -> Any:
        return SimpleNamespace(
            fields=[SimpleNamespace(name=name) for name in self.state.schema_names]
        )

    def snapshots(self) -> list[int]:
        return list(self.state.snapshots)

    def scan(self) -> FakeScan:
        return FakeScan(self.state)

    def append(self, batch: pa.Table) -> None:
        if self.loaded_generation != self.state.generation:
            raise CommitFailedException("stale test handle")
        self.state.rows.extend(batch.to_pylist())
        self.state.snapshots.append(len(self.state.snapshots) + 1)
        self.state.advance()
        self.refresh()

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self)

    def update_schema(self) -> FakeSchemaUpdate:
        return FakeSchemaUpdate(self)

    def delete(self, delete_filter: str) -> None:
        assert delete_filter == "id < 4"
        self.state.rows = [row for row in self.state.rows if int(row["id"]) >= 4]
        self.state.snapshots.append(len(self.state.snapshots) + 1)
        self.state.advance()
        self.refresh()

    def refresh(self) -> FakeTable:
        self.loaded_generation = self.state.generation
        return self


class FakeCatalog:
    def __init__(self, name: str, properties: Mapping[str, str]) -> None:
        self.name = name
        self.properties = dict(properties)
        self.namespaces: dict[tuple[str, ...], dict[str, str]] = {}
        self.tables: dict[tuple[str, ...], TableState] = {}
        self.retained: dict[str, TableState] = {}
        self.closed = False

    def namespace_exists(self, namespace: tuple[str, ...]) -> bool:
        return namespace in self.namespaces

    def create_namespace(
        self, namespace: tuple[str, ...], properties: Mapping[str, str]
    ) -> None:
        self.namespaces[namespace] = dict(properties)

    def list_namespaces(self) -> list[tuple[str, ...]]:
        return sorted(self.namespaces)

    def load_namespace_properties(self, namespace: tuple[str, ...]) -> dict[str, str]:
        return dict(self.namespaces[namespace])

    def drop_namespace(self, namespace: tuple[str, ...]) -> None:
        if any(identifier[:-1] == namespace for identifier in self.tables):
            raise ValueError("namespace is not empty")
        del self.namespaces[namespace]

    def create_table(
        self,
        identifier: tuple[str, ...],
        schema: Any,
        location: str | None,
        properties: Mapping[str, str],
    ) -> FakeTable:
        root = location or f"s3://warehouse/{'/'.join(identifier)}"
        state = TableState(
            identifier=identifier,
            metadata_location=f"{root}/metadata/00000.metadata.json",
            schema_names=[field.name for field in schema.fields],
            properties=dict(properties),
            table_uuid=uuid.uuid5(uuid.NAMESPACE_URL, ".".join(identifier)),
        )
        self.tables[identifier] = state
        return FakeTable(self, state)

    def list_tables(self, namespace: tuple[str, ...]) -> list[tuple[str, ...]]:
        return sorted(
            identifier for identifier in self.tables if identifier[:-1] == namespace
        )

    def load_table(self, identifier: tuple[str, ...]) -> FakeTable:
        return FakeTable(self, self.tables[identifier])

    def table_exists(self, identifier: tuple[str, ...]) -> bool:
        return identifier in self.tables

    def rename_table(
        self, source: tuple[str, ...], destination: tuple[str, ...]
    ) -> FakeTable:
        state = self.tables.pop(source)
        state.identifier = destination
        self.tables[destination] = state
        return FakeTable(self, state)

    def drop_table(self, identifier: tuple[str, ...], *, purge_requested: bool) -> None:
        assert purge_requested is False
        state = self.tables.pop(identifier)
        self.retained[state.metadata_location] = state

    def register_table(
        self, identifier: tuple[str, ...], metadata_location: str
    ) -> FakeTable:
        state = self.retained[metadata_location]
        state.identifier = identifier
        self.tables[identifier] = state
        return FakeTable(self, state)

    def close(self) -> None:
        self.closed = True


class FakeCatalogFactory:
    def __init__(self) -> None:
        self.catalogs: list[FakeCatalog] = []

    def __call__(self, name: str, properties: Mapping[str, str]) -> FakeCatalog:
        catalog = FakeCatalog(name, properties)
        self.catalogs.append(catalog)
        return catalog
