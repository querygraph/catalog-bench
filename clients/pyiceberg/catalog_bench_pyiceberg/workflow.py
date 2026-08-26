"""No-shim stock PyIceberg workflow with independent optional classifications."""

from __future__ import annotations

import hashlib
import json
import os
import platform
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import urljoin

from .contracts import STEP_CAPABILITIES, ResolvedContracts, validate_fixture_id
from .evidence import FixtureIdentity, RuntimeIdentity
from .model import Limitation, OperationResult, SafeFailure, Status


class CatalogFactory(Protocol):
    def __call__(self, name: str, properties: Mapping[str, str]) -> Any: ...


class WorkflowAssertion(AssertionError):
    """An invariant failed after a stock-client operation completed."""


@dataclass(frozen=True)
class ProbeRun:
    runtime: RuntimeIdentity
    fixture: FixtureIdentity
    operations: tuple[OperationResult, ...]
    forbidden_values: tuple[str, ...]


@dataclass
class _State:
    fixture: FixtureIdentity
    catalog: Any | None = None
    ownership_safe: bool = False
    active_identifier: tuple[str, ...] | None = None
    rows: list[dict[str, Any]] = field(default_factory=list)
    object_io_verified: bool = False

    @property
    def namespace(self) -> tuple[str, ...]:
        return self.fixture.namespace

    @property
    def identifiers(self) -> tuple[tuple[str, ...], ...]:
        return tuple(
            (*self.namespace, table) for table in self.fixture.table_candidates
        )


def detect_runtime() -> RuntimeIdentity:
    import pyarrow
    import pyiceberg

    return RuntimeIdentity(
        python=platform.python_version(),
        pyiceberg=pyiceberg.__version__,
        pyarrow=pyarrow.__version__,
        operating_system=platform.system(),
        architecture=platform.machine(),
    )


def default_catalog_factory(name: str, properties: Mapping[str, str]) -> Any:
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(name, **dict(properties))


def run_probe(
    contracts: ResolvedContracts,
    fixture_id: str,
    *,
    getenv: Callable[[str], str | None] = os.environ.get,
    runtime: RuntimeIdentity | None = None,
    catalog_factory: CatalogFactory = default_catalog_factory,
) -> ProbeRun:
    """Execute every C1-07 step once and always attempt owned cleanup."""

    validate_fixture_id(fixture_id)
    runtime = runtime or detect_runtime()
    fixture = _fixture(contracts, fixture_id)
    state = _State(fixture)
    results: dict[str, OperationResult] = {}
    forbidden_values: list[str] = []

    results["verify-client-runtime"] = _verify_runtime(contracts, runtime)
    if results["verify-client-runtime"].status is Status.PASS:
        properties, forbidden_values = _catalog_properties(contracts, getenv)
        results["initialize-catalog"] = _initialize_catalog(
            contracts, state, properties, catalog_factory
        )
    else:
        results["initialize-catalog"] = _skip(
            "initialize-catalog", "pinned client runtime did not pass"
        )

    if results["initialize-catalog"].status is Status.PASS:
        results["preflight-fixture"] = _preflight(state)
    else:
        results["preflight-fixture"] = _skip(
            "preflight-fixture", "catalog initialization did not pass"
        )

    if results["preflight-fixture"].status is Status.PASS:
        results["namespace-round-trip"] = _namespace_round_trip(contracts, state)
    else:
        results["namespace-round-trip"] = _skip(
            "namespace-round-trip", "fixture isolation did not pass"
        )

    if results["namespace-round-trip"].status is Status.PASS:
        results["table-round-trip"] = _table_round_trip(contracts, state)
    else:
        results["table-round-trip"] = _skip(
            "table-round-trip", "namespace round trip did not pass"
        )

    if results["table-round-trip"].status is Status.PASS:
        results["append-scan"] = _append_scan(contracts, state)
    else:
        results["append-scan"] = _skip("append-scan", "table round trip did not pass")

    data_ready = results["append-scan"].status is Status.PASS
    results["update-properties"] = _optional_or_skip(
        contracts,
        "update-properties",
        data_ready,
        "initial append and scan did not pass",
        lambda: _update_properties(contracts, state),
    )
    results["evolve-schema"] = _optional_or_skip(
        contracts,
        "evolve-schema",
        data_ready,
        "initial append and scan did not pass",
        lambda: _evolve_schema(contracts, state),
    )
    results["delete-rows"] = _optional_or_skip(
        contracts,
        "delete-rows",
        data_ready,
        "initial append and scan did not pass",
        lambda: _delete_rows(contracts, state),
    )
    results["recover-conflict"] = _optional_or_skip(
        contracts,
        "recover-conflict",
        data_ready,
        "initial append and scan did not pass",
        lambda: _recover_conflict(contracts, state),
    )
    results["observe-delegated-access"] = _optional_or_skip(
        contracts,
        "observe-delegated-access",
        data_ready,
        "initial append and scan did not pass",
        lambda: _observe_delegated_access(state),
    )
    results["rename-table"] = _optional_or_skip(
        contracts,
        "rename-table",
        data_ready,
        "initial append and scan did not pass",
        lambda: _rename_table(state),
    )
    results["register-table"] = _optional_or_skip(
        contracts,
        "register-table",
        data_ready,
        "initial append and scan did not pass",
        lambda: _register_table(state),
    )

    results["classify-views"] = _known_client_limitation(contracts, "classify-views")
    results["classify-pagination"] = _known_client_limitation(
        contracts, "classify-pagination"
    )
    results["cleanup-fixture"] = _cleanup(state)
    results["sanitize-transcript"] = OperationResult.passed(
        "sanitize-transcript",
        None,
        {
            "credential_policy": "categories-only",
            "exception_policy": "class-and-fixed-explanation-only",
            "row_policy": "count-range-and-sha256-only",
        },
    )

    if state.catalog is not None:
        try:
            state.catalog.close()
        except Exception:  # noqa: BLE001, S110 - close cannot alter persisted facts
            pass

    ordered = tuple(results[step] for step in STEP_CAPABILITIES)
    return ProbeRun(
        runtime=runtime,
        fixture=fixture,
        operations=ordered,
        forbidden_values=tuple(forbidden_values),
    )


def _fixture(contracts: ResolvedContracts, fixture_id: str) -> FixtureIdentity:
    prefix = contracts.scenario["parameters"]["fixture_prefix"]
    catalog = contracts.catalog_component["id"]
    namespace = (f"{prefix}_{catalog}_{fixture_id}",)
    return FixtureIdentity(
        id=fixture_id,
        namespace=namespace,
        table_candidates=("events", "events_renamed", "events_registered"),
    )


def _verify_runtime(
    contracts: ResolvedContracts, runtime: RuntimeIdentity
) -> OperationResult:
    expected = {
        "python": contracts.python_component["version"],
        "pyiceberg": contracts.client_component["version"],
        "pyarrow": contracts.arrow_component["version"],
        "operating_system": contracts.profile["platform"]["operating_system"],
        "architecture": contracts.profile["platform"]["architecture"],
    }
    actual = runtime.as_json()
    if actual == expected:
        return OperationResult.passed("verify-client-runtime", None, actual)
    return OperationResult.failed(
        "verify-client-runtime",
        None,
        SafeFailure(
            category="runtime",
            exception_class="catalog_bench_pyiceberg.RuntimeMismatch",
            explanation="observed runtime identity does not equal the pinned profile",
        ),
        {"expected": expected, "observed": actual},
    )


def _catalog_properties(
    contracts: ResolvedContracts, getenv: Callable[[str], str | None]
) -> tuple[dict[str, str], list[str]]:
    adapter = contracts.adapter
    parameters = contracts.scenario["parameters"]
    object_store = parameters["object_store"]
    access_key = _required_environment(getenv, object_store["access_key_env"])
    secret_key = _required_environment(getenv, object_store["secret_key_env"])
    properties = {
        "uri": adapter["endpoint"]["base_url"],
        "s3.endpoint": object_store["endpoint"],
        "s3.region": object_store["region"],
        "s3.access-key-id": access_key,
        "s3.secret-access-key": secret_key,
        "s3.force-virtual-addressing": "false",
    }
    config_query = adapter["endpoint"]["config"].get("query", {})
    if warehouse := config_query.get("warehouse"):
        properties["warehouse"] = warehouse

    forbidden = [access_key, secret_key]
    authentication = adapter["authentication"]
    if authentication["kind"] == "oauth2-client-credentials":
        client_id = _required_environment(getenv, authentication["client_id_env"])
        client_secret = _required_environment(
            getenv, authentication["client_secret_env"]
        )
        properties.update(
            {
                "credential": f"{client_id}:{client_secret}",
                "oauth2-server-uri": urljoin(
                    f"{adapter['endpoint']['base_url'].rstrip('/')}/",
                    authentication["token_path"].lstrip("/"),
                ),
                "scope": authentication["scope"],
            }
        )
        forbidden.extend((client_id, client_secret, properties["credential"]))
    elif authentication["kind"] != "anonymous":
        raise ValueError("unsupported profile authentication mode")
    return properties, forbidden


def _required_environment(getenv: Callable[[str], str | None], name: str) -> str:
    value = getenv(name)
    if value is None or not value:
        raise ValueError(f"required environment variable {name} is missing")
    return value


def _initialize_catalog(
    contracts: ResolvedContracts,
    state: _State,
    properties: Mapping[str, str],
    catalog_factory: CatalogFactory,
) -> OperationResult:
    try:
        state.catalog = catalog_factory(
            f"catalog-bench-{contracts.catalog_component['id']}", properties
        )
        return OperationResult.passed(
            "initialize-catalog",
            STEP_CAPABILITIES["initialize-catalog"],
            {
                "authentication": contracts.adapter["authentication"]["kind"],
                "request_handling": "protocol-native",
                "shim": False,
            },
        )
    except Exception as error:  # noqa: BLE001 - persisted detail is class-only
        return _failure(
            "initialize-catalog",
            error,
            "stock RestCatalog initialization did not complete",
            category="catalog-initialization",
        )


def _preflight(state: _State) -> OperationResult:
    try:
        exists = state.catalog.namespace_exists(state.namespace)
        if exists:
            raise WorkflowAssertion("fixture collision")
        state.ownership_safe = True
        return OperationResult.passed(
            "preflight-fixture",
            STEP_CAPABILITIES["preflight-fixture"],
            {"namespace_absent": True, "mutation_attempted": False},
        )
    except Exception as error:  # noqa: BLE001
        return _failure(
            "preflight-fixture",
            error,
            "fixture absence could not be proven; no mutation or cleanup was attempted",
            category="fixture-isolation",
        )


def _namespace_round_trip(
    contracts: ResolvedContracts, state: _State
) -> OperationResult:
    properties = contracts.scenario["parameters"]["namespace_properties"]
    try:
        state.catalog.create_namespace(state.namespace, properties=properties)
        listed = state.catalog.list_namespaces()
        loaded = state.catalog.load_namespace_properties(state.namespace)
        if state.namespace not in listed:
            raise WorkflowAssertion("created namespace missing from stock listing")
        if any(loaded.get(key) != value for key, value in properties.items()):
            raise WorkflowAssertion("namespace property projection differs")
        return OperationResult.passed(
            "namespace-round-trip",
            STEP_CAPABILITIES["namespace-round-trip"],
            {
                "created": True,
                "listed": True,
                "loaded_property_count": len(properties),
            },
        )
    except Exception as error:  # noqa: BLE001
        return _failure(
            "namespace-round-trip",
            error,
            "stock namespace create, list, or load did not preserve the fixture",
            category="namespace-round-trip",
        )


def _table_round_trip(contracts: ResolvedContracts, state: _State) -> OperationResult:
    identifier = state.identifiers[0]
    parameters = contracts.scenario["parameters"]
    try:
        table = state.catalog.create_table(
            identifier,
            schema=_initial_schema(),
            location=_table_location(contracts, state),
            properties=dict(parameters["initial_properties"]),
        )
        listed = state.catalog.list_tables(state.namespace)
        loaded = state.catalog.load_table(identifier)
        if identifier not in listed:
            raise WorkflowAssertion("created table missing from stock listing")
        _assert_same_table(table, loaded)
        _assert_property_projection(loaded, parameters["initial_properties"])
        state.active_identifier = identifier
        return OperationResult.passed(
            "table-round-trip",
            STEP_CAPABILITIES["table-round-trip"],
            _table_observation(loaded),
        )
    except Exception as error:  # noqa: BLE001
        _resolve_active(state)
        return _failure(
            "table-round-trip",
            error,
            "stock table create, list, or load did not preserve committed state",
            category="table-round-trip",
        )


def _append_scan(contracts: ResolvedContracts, state: _State) -> OperationResult:
    count = contracts.scenario["parameters"]["initial_rows"]
    try:
        table = _load_active(state)
        rows, batch = _batch(table, 0, count)
        table.append(batch)
        loaded = _load_active(state)
        state.rows = rows
        observation = _assert_rows(loaded, state.rows)
        state.object_io_verified = True
        return OperationResult.passed(
            "append-scan",
            STEP_CAPABILITIES["append-scan"],
            {
                **observation,
                "snapshot_count": len(loaded.snapshots()),
                "metadata_location": loaded.metadata_location,
            },
        )
    except Exception as error:  # noqa: BLE001
        _refresh_rows(state)
        return _failure(
            "append-scan",
            error,
            "stock append and independent scan did not return the deterministic batch",
            category="append-scan",
        )


def _update_properties(contracts: ResolvedContracts, state: _State) -> OperationResult:
    parameters = contracts.scenario["parameters"]
    try:
        table = _load_active(state)
        transaction = table.transaction()
        transaction.set_properties(dict(parameters["property_updates"]))
        transaction.remove_properties(*parameters["property_removals"])
        transaction.commit_transaction()
        loaded = _load_active(state)
        properties = loaded.metadata.properties
        for key, value in parameters["property_updates"].items():
            if properties.get(key) != value:
                raise WorkflowAssertion("updated table property differs")
        if any(key in properties for key in parameters["property_removals"]):
            raise WorkflowAssertion("removed table property remains")
        return OperationResult.passed(
            "update-properties",
            STEP_CAPABILITIES["update-properties"],
            {
                "updated": sorted(parameters["property_updates"]),
                "removed": sorted(parameters["property_removals"]),
                "metadata_location": loaded.metadata_location,
            },
        )
    except NotImplementedError as error:
        return _unsupported_after_client_check(
            "update-properties", error, attributed_to="catalog"
        )
    except Exception as error:  # noqa: BLE001
        return _failure(
            "update-properties",
            error,
            "stock property update did not reload the requested projection",
            category="property-update",
        )


def _evolve_schema(contracts: ResolvedContracts, state: _State) -> OperationResult:
    count = contracts.scenario["parameters"]["evolved_rows"]
    try:
        from pyiceberg.types import StringType

        table = _load_active(state)
        table.update_schema().add_column("note", StringType(), required=False).commit()
        evolved = _load_active(state)
        if "note" not in _schema_names(evolved):
            raise WorkflowAssertion("evolved column missing")
        state.rows = [{**row, "note": None} for row in state.rows]
        rows, batch = _batch(evolved, _next_id(state.rows), count)
        evolved.append(batch)
        state.rows.extend(rows)
        loaded = _load_active(state)
        observation = _assert_rows(loaded, state.rows)
        return OperationResult.passed(
            "evolve-schema",
            STEP_CAPABILITIES["evolve-schema"],
            {**observation, "current_fields": _schema_names(loaded)},
        )
    except NotImplementedError as error:
        _refresh_rows(state)
        return _unsupported_after_client_check(
            "evolve-schema", error, attributed_to="catalog"
        )
    except Exception as error:  # noqa: BLE001
        _refresh_rows(state)
        return _failure(
            "evolve-schema",
            error,
            "stock schema evolution and evolved append did not preserve exact rows",
            category="schema-evolution",
        )


def _delete_rows(contracts: ResolvedContracts, state: _State) -> OperationResult:
    delete_filter = contracts.scenario["parameters"]["delete_filter"]
    try:
        table = _load_active(state)
        table.delete(delete_filter)
        state.rows = [row for row in state.rows if int(row["id"]) >= 4]
        loaded = _load_active(state)
        observation = _assert_rows(loaded, state.rows)
        return OperationResult.passed(
            "delete-rows",
            STEP_CAPABILITIES["delete-rows"],
            {**observation, "predicate": delete_filter},
        )
    except NotImplementedError as error:
        _refresh_rows(state)
        return _unsupported_after_client_check(
            "delete-rows", error, attributed_to="client"
        )
    except Exception as error:  # noqa: BLE001
        _refresh_rows(state)
        return _failure(
            "delete-rows",
            error,
            "stock delete and scan did not produce the exact surviving row set",
            category="row-delete",
        )


def _recover_conflict(contracts: ResolvedContracts, state: _State) -> OperationResult:
    count = contracts.scenario["parameters"]["conflict_rows_per_writer"]
    stale_conflict_class = None
    try:
        from pyiceberg.exceptions import CommitFailedException

        writer_a = _load_active(state)
        writer_b = _load_active(state)
        first_rows, first_batch = _batch(writer_a, _next_id(state.rows), count)
        second_rows, second_batch = _batch(
            writer_b, _next_id(state.rows) + count, count
        )
        writer_a.append(first_batch)
        state.rows.extend(first_rows)
        try:
            writer_b.append(second_batch)
        except CommitFailedException as conflict:
            stale_conflict_class = (
                f"{type(conflict).__module__}.{type(conflict).__qualname__}"
            )
        else:
            state.rows.extend(second_rows)
            loaded = _load_active(state)
            _assert_rows(loaded, state.rows)
            raise WorkflowAssertion("stale append unexpectedly succeeded")

        writer_b.refresh()
        _, retry_batch = _batch(writer_b, second_rows[0]["id"], count)
        writer_b.append(retry_batch)
        state.rows.extend(second_rows)
        loaded = _load_active(state)
        observation = _assert_rows(loaded, state.rows)
        return OperationResult.passed(
            "recover-conflict",
            STEP_CAPABILITIES["recover-conflict"],
            {
                **observation,
                "stale_attempt": "commit-failed",
                "stale_exception_class": stale_conflict_class,
                "refresh_count": 1,
                "retry_count": 1,
            },
        )
    except NotImplementedError as error:
        _refresh_rows(state)
        return _unsupported_after_client_check(
            "recover-conflict", error, attributed_to="client"
        )
    except Exception as error:  # noqa: BLE001
        _refresh_rows(state)
        return _failure(
            "recover-conflict",
            error,
            "stale append conflict and one refresh-and-retry did not complete exactly once",
            category="conflict-recovery",
            observations={
                "stale_exception_class": stale_conflict_class,
                "final_rows": _row_observation(state.rows),
            },
        )


def _observe_delegated_access(state: _State) -> OperationResult:
    try:
        table = _load_active(state)
        categories = _credential_categories(table.config)
        if not categories:
            return OperationResult.unsupported(
                "observe-delegated-access",
                STEP_CAPABILITIES["observe-delegated-access"],
                Limitation(
                    attributed_to="catalog",
                    explanation=(
                        "table response config supplied no delegated credential category; "
                        "the common workflow used fixed fixture credentials"
                    ),
                ),
            )
        return OperationResult.passed(
            "observe-delegated-access",
            STEP_CAPABILITIES["observe-delegated-access"],
            {
                "categories": categories,
                "credential_values_persisted": False,
                "object_io_verified": state.object_io_verified,
                "requested": "vended-credentials",
            },
        )
    except Exception as error:  # noqa: BLE001
        return _failure(
            "observe-delegated-access",
            error,
            "delegated-access response categories could not be classified safely",
            category="credential-vending",
        )


def _rename_table(state: _State) -> OperationResult:
    destination = state.identifiers[1]
    before = _row_observation(state.rows)
    try:
        source = _active_identifier(state)
        renamed = state.catalog.rename_table(source, destination)
        if state.catalog.table_exists(source):
            raise WorkflowAssertion("source remained after rename")
        state.active_identifier = destination
        observation = _assert_rows(renamed, state.rows)
        if observation["row_sha256"] != before["row_sha256"]:
            raise WorkflowAssertion("rename changed data digest")
        return OperationResult.passed(
            "rename-table",
            STEP_CAPABILITIES["rename-table"],
            {**observation, "source_absent": True, "destination_present": True},
        )
    except NotImplementedError as error:
        _resolve_active(state)
        return _unsupported_after_client_check(
            "rename-table", error, attributed_to="catalog"
        )
    except Exception as error:  # noqa: BLE001
        _resolve_active(state)
        return _failure(
            "rename-table",
            error,
            "stock rename did not preserve one active identifier and the data digest",
            category="table-rename",
        )


def _register_table(state: _State) -> OperationResult:
    destination = state.identifiers[2]
    try:
        active = _active_identifier(state)
        table = state.catalog.load_table(active)
        metadata_location = table.metadata_location
        before = _row_observation(state.rows)
        state.catalog.drop_table(active, purge_requested=False)
        state.active_identifier = None
        registered = state.catalog.register_table(destination, metadata_location)
        state.active_identifier = destination
        if registered.metadata_location != metadata_location:
            raise WorkflowAssertion("registration changed metadata location")
        observation = _assert_rows(registered, state.rows)
        if observation["row_sha256"] != before["row_sha256"]:
            raise WorkflowAssertion("registration changed data digest")
        return OperationResult.passed(
            "register-table",
            STEP_CAPABILITIES["register-table"],
            {
                **observation,
                "metadata_location": metadata_location,
                "purge_requested": False,
            },
        )
    except NotImplementedError as error:
        _resolve_active(state)
        return _unsupported_after_client_check(
            "register-table", error, attributed_to="catalog"
        )
    except Exception as error:  # noqa: BLE001
        _resolve_active(state)
        return _failure(
            "register-table",
            error,
            "stock drop and registration did not preserve retained metadata and data",
            category="table-register",
        )


def _cleanup(state: _State) -> OperationResult:
    capability = STEP_CAPABILITIES["cleanup-fixture"]
    if state.catalog is None:
        return OperationResult.not_evaluated(
            "cleanup-fixture", capability, "catalog was never initialized"
        )
    if not state.ownership_safe:
        return OperationResult.not_evaluated(
            "cleanup-fixture",
            capability,
            "fixture ownership was not proven; no cleanup mutation was attempted",
        )
    dropped: list[str] = []
    try:
        for identifier in state.identifiers:
            if state.catalog.table_exists(identifier):
                state.catalog.drop_table(identifier, purge_requested=False)
                dropped.append(identifier[-1])
        remaining = [
            identifier[-1]
            for identifier in state.identifiers
            if state.catalog.table_exists(identifier)
        ]
        if remaining:
            raise WorkflowAssertion("run-owned table remained after cleanup")
        if state.catalog.namespace_exists(state.namespace):
            state.catalog.drop_namespace(state.namespace)
        namespace_absent = not state.catalog.namespace_exists(state.namespace)
        if not namespace_absent:
            raise WorkflowAssertion("run-owned namespace remained after cleanup")
        state.active_identifier = None
        return OperationResult.passed(
            "cleanup-fixture",
            capability,
            {
                "dropped_tables": dropped,
                "namespace_absent": namespace_absent,
                "purge_requested": False,
                "table_candidates_absent": True,
            },
        )
    except Exception as error:  # noqa: BLE001
        return _failure(
            "cleanup-fixture",
            error,
            "owned fixture reconciliation did not prove every identifier absent",
            category="cleanup",
            observations={"dropped_tables": dropped, "purge_requested": False},
        )


def _optional_or_skip(
    contracts: ResolvedContracts,
    step: str,
    prerequisite: bool,
    reason: str,
    operation: Callable[[], OperationResult],
) -> OperationResult:
    capability = STEP_CAPABILITIES[step]
    assert capability is not None
    if limitation := contracts.adapter_limitation(capability):
        return OperationResult.unsupported(step, capability, limitation)
    if not prerequisite:
        return OperationResult.not_evaluated(step, capability, reason)
    return operation()


def _known_client_limitation(
    contracts: ResolvedContracts, step: str
) -> OperationResult:
    capability = STEP_CAPABILITIES[step]
    assert capability is not None
    limitation = contracts.known_client_limitation(capability)
    if limitation is None:
        return OperationResult.failed(
            step,
            capability,
            SafeFailure(
                category="client-policy",
                exception_class="catalog_bench_pyiceberg.MissingClientLimitation",
                explanation="pinned-client limitation is absent from the scenario",
            ),
        )
    return OperationResult.unsupported(step, capability, limitation)


def _skip(step: str, reason: str) -> OperationResult:
    return OperationResult.not_evaluated(step, STEP_CAPABILITIES[step], reason)


def _failure(
    step: str,
    error: BaseException,
    explanation: str,
    *,
    category: str,
    observations: Mapping[str, Any] | None = None,
) -> OperationResult:
    return OperationResult.failed(
        step,
        STEP_CAPABILITIES[step],
        SafeFailure.from_exception(error, category=category, explanation=explanation),
        observations,
    )


def _unsupported_after_client_check(
    step: str, error: NotImplementedError, *, attributed_to: str
) -> OperationResult:
    del error
    capability = STEP_CAPABILITIES[step]
    assert capability is not None
    return OperationResult.unsupported(
        step,
        capability,
        Limitation(
            attributed_to=attributed_to,
            explanation=(
                "the stock client declined this optional operation before a "
                "successful protocol mutation"
            ),
        ),
    )


def _table_location(contracts: ResolvedContracts, state: _State) -> str | None:
    root = contracts.adapter["endpoint"].get("create_table_location")
    if root is None:
        return None
    return (
        f"{root.rstrip('/')}/{state.namespace[0]}/{state.fixture.table_candidates[0]}"
    )


def _initial_schema() -> Any:
    from pyiceberg.schema import Schema
    from pyiceberg.types import DoubleType, LongType, NestedField, StringType

    return Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "category", StringType(), required=False),
        NestedField(3, "amount", DoubleType(), required=False),
    )


def _batch(table: Any, start: int, count: int) -> tuple[list[dict[str, Any]], Any]:
    import pyarrow as pa

    has_note = "note" in _schema_names(table)
    rows = [
        {
            "id": row_id,
            "category": f"g{row_id % 4}",
            "amount": float(row_id) / 2.0,
            **({"note": f"n{row_id % 3}"} if has_note else {}),
        }
        for row_id in range(start, start + count)
    ]
    fields = [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("category", pa.string(), nullable=True),
        pa.field("amount", pa.float64(), nullable=True),
    ]
    if has_note:
        fields.append(pa.field("note", pa.string(), nullable=True))
    schema = pa.schema(fields)
    arrays = [
        pa.array([row[field.name] for row in rows], type=field.type) for field in fields
    ]
    return rows, pa.Table.from_arrays(arrays, schema=schema)


def _assert_rows(table: Any, expected: list[dict[str, Any]]) -> dict[str, Any]:
    observed = _canonical_rows(table.scan().to_arrow().to_pylist())
    canonical_expected = _canonical_rows(expected)
    if observed != canonical_expected:
        raise WorkflowAssertion("scanned rows differ from expected canonical rows")
    return _row_observation(observed)


def _canonical_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [{key: row[key] for key in sorted(row)} for row in rows]
    return sorted(normalized, key=lambda row: int(row["id"]))


def _row_observation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    canonical = _canonical_rows(rows)
    encoded = json.dumps(
        canonical, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    ids = [int(row["id"]) for row in canonical]
    return {
        "row_count": len(canonical),
        "minimum_id": min(ids) if ids else None,
        "maximum_id": max(ids) if ids else None,
        "row_sha256": hashlib.sha256(encoded).hexdigest(),
    }


def _schema_names(table: Any) -> list[str]:
    return [field.name for field in table.schema().fields]


def _assert_same_table(created: Any, loaded: Any) -> None:
    if created.metadata.table_uuid != loaded.metadata.table_uuid:
        raise WorkflowAssertion("table UUID changed across load")
    if created.metadata_location != loaded.metadata_location:
        raise WorkflowAssertion("metadata location changed across immediate load")
    if _schema_names(created) != _schema_names(loaded):
        raise WorkflowAssertion("schema changed across immediate load")


def _assert_property_projection(table: Any, expected: Mapping[str, str]) -> None:
    properties = table.metadata.properties
    if any(properties.get(key) != value for key, value in expected.items()):
        raise WorkflowAssertion("scenario property projection differs")


def _table_observation(table: Any) -> dict[str, Any]:
    return {
        "table_uuid": str(table.metadata.table_uuid),
        "metadata_location": table.metadata_location,
        "schema_fields": _schema_names(table),
        "snapshot_count": len(table.snapshots()),
    }


def _credential_categories(config: Mapping[str, str]) -> list[str]:
    keys = {key.lower() for key in config}
    categories = []
    access_keys = {"s3.access-key-id", "client.access-key-id"}
    secret_keys = {"s3.secret-access-key", "client.secret-access-key"}
    if keys & access_keys and keys & secret_keys:
        categories.append("key-pair")
    if keys & {"s3.session-token", "client.session-token"}:
        categories.append("session-token")
    if any(key.startswith("s3.signer") for key in keys):
        categories.append("remote-signing")
    return categories


def _load_active(state: _State) -> Any:
    return state.catalog.load_table(_active_identifier(state))


def _active_identifier(state: _State) -> tuple[str, ...]:
    if state.active_identifier is None:
        raise WorkflowAssertion("no unambiguous active table identifier")
    return state.active_identifier


def _resolve_active(state: _State) -> None:
    if state.catalog is None or not state.ownership_safe:
        state.active_identifier = None
        return
    active = []
    for identifier in state.identifiers:
        try:
            if state.catalog.table_exists(identifier):
                active.append(identifier)
        except Exception:  # noqa: BLE001 - ambiguity is represented by None
            state.active_identifier = None
            return
    state.active_identifier = active[0] if len(active) == 1 else None


def _refresh_rows(state: _State) -> None:
    _resolve_active(state)
    if state.active_identifier is None:
        return
    try:
        rows = (
            state.catalog.load_table(state.active_identifier)
            .scan()
            .to_arrow()
            .to_pylist()
        )
        state.rows = _canonical_rows(rows)
    except Exception:  # noqa: BLE001, S110 - originating operation records failure
        pass


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row["id"]) for row in rows), default=-1) + 1
