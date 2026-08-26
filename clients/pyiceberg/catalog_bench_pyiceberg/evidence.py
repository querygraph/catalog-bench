"""Deterministic, secret-resistant transcript assembly and persistence."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .contracts import ResolvedContracts, component_identity
from .model import OperationResult, assertion_evaluations, classify


@dataclass(frozen=True)
class RuntimeIdentity:
    python: str
    pyiceberg: str
    pyarrow: str
    operating_system: str
    architecture: str

    def as_json(self) -> dict[str, str]:
        return {
            "python": self.python,
            "pyiceberg": self.pyiceberg,
            "pyarrow": self.pyarrow,
            "operating_system": self.operating_system,
            "architecture": self.architecture,
        }


@dataclass(frozen=True)
class FixtureIdentity:
    id: str
    namespace: tuple[str, ...]
    table_candidates: tuple[str, ...]

    def as_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "namespace": list(self.namespace),
            "table_candidates": list(self.table_candidates),
        }


def build_transcript(
    contracts: ResolvedContracts,
    runtime: RuntimeIdentity,
    fixture: FixtureIdentity,
    operations: Sequence[OperationResult],
    *,
    forbidden_values: Sequence[str],
) -> dict[str, Any]:
    """Build a standalone transcript and reject any embedded sensitive value."""

    adapter = contracts.adapter
    authentication = adapter["authentication"]
    document: dict[str, Any] = {
        "format": contracts.scenario["parameters"]["transcript_format"],
        "contract_digests": {
            "profile_sha256": contracts.digests.profile_sha256,
            "scenario_sha256": contracts.digests.scenario_sha256,
        },
        "profile": {
            "id": contracts.profile["id"],
            "resolved_at": contracts.profile["resolved_at"],
        },
        "scenario": {
            "id": contracts.scenario["id"],
            "version": contracts.scenario["version"],
        },
        "adapter": {
            "catalog": contracts.catalog_component["id"],
            "name": contracts.catalog_component["name"],
            "version": contracts.catalog_component["version"],
            "protocol": adapter["protocol"],
            "endpoint": adapter["endpoint"]["base_url"],
            "authentication": authentication["kind"],
            "request_handling": dict(adapter["request_handling"]),
        },
        "client": {
            "runtime": component_identity(contracts.python_component),
            "catalog_client": component_identity(contracts.client_component),
            "data_plane": component_identity(contracts.arrow_component),
            "observed": runtime.as_json(),
            "shim": False,
        },
        "fixture": fixture.as_json(),
        "classification": classify(contracts.assertions, operations),
        "operations": [operation.as_json() for operation in operations],
        "assertions": assertion_evaluations(contracts.assertions, operations),
        "sanitization": {
            "policy": "stock-client-values-omitted-v1",
            "redactions": [],
            "raw_secrets_persisted": False,
            "raw_response_body_persisted": False,
            "raw_exception_message_persisted": False,
            "raw_row_values_persisted": False,
        },
    }
    leaked = sorted(
        value
        for value in set(forbidden_values)
        if value and _contains_string_fragment(document, value)
    )
    if leaked:
        raise ValueError(
            f"refusing to encode transcript containing {len(leaked)} sensitive value(s)"
        )
    return document


def encode_transcript(document: Mapping[str, Any]) -> bytes:
    return (json.dumps(document, indent=2, sort_keys=True) + "\n").encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_new(path: Path, data: bytes) -> None:
    """Create and fsync a transcript without any overwrite path."""

    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as error:
        raise FileExistsError(f"refusing to overwrite evidence file {path}") from error


def _contains_string_fragment(value: Any, forbidden: str) -> bool:
    if isinstance(value, str):
        return forbidden in value
    if isinstance(value, Mapping):
        return any(
            _contains_string_fragment(key, forbidden)
            or _contains_string_fragment(item, forbidden)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_string_fragment(item, forbidden) for item in value)
    return False
