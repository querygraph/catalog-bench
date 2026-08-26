"""Strict profile/scenario loading for the stock PyIceberg runner."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .model import AssertionContract, Limitation

SCENARIO_ID = "client.pyiceberg.interoperability"
SCENARIO_VERSION = 1
TRANSCRIPT_FORMAT = "catalog-bench/pyiceberg-transcript/v1"
CONTRACT_VERSION = "catalog-bench/v1"
FIXTURE_ID_PATTERN = re.compile(r"^[a-z0-9_]{1,24}$")

STEP_CAPABILITIES: Mapping[str, str | None] = {
    "verify-client-runtime": None,
    "initialize-catalog": "client.pyiceberg.round-trip",
    "preflight-fixture": "client.pyiceberg.round-trip",
    "namespace-round-trip": "client.pyiceberg.round-trip",
    "table-round-trip": "client.pyiceberg.round-trip",
    "append-scan": "client.pyiceberg.round-trip",
    "update-properties": "client.pyiceberg.property-update",
    "evolve-schema": "client.pyiceberg.schema-evolution",
    "delete-rows": "client.pyiceberg.row-delete",
    "recover-conflict": "client.pyiceberg.conflict-recovery",
    "observe-delegated-access": "client.pyiceberg.credential-vending",
    "rename-table": "client.pyiceberg.table-rename",
    "register-table": "client.pyiceberg.table-register",
    "classify-views": "client.pyiceberg.view-lifecycle",
    "classify-pagination": "client.pyiceberg.pagination",
    "cleanup-fixture": "client.pyiceberg.round-trip",
    "sanitize-transcript": None,
}

EXPECTED_ASSERTIONS = (
    AssertionContract("client-runtime-pinned", "verify-client-runtime", True),
    AssertionContract("catalog-auth-config-ready", "initialize-catalog", True),
    AssertionContract("fixture-isolated", "preflight-fixture", True),
    AssertionContract("namespace-round-trip", "namespace-round-trip", True),
    AssertionContract("table-round-trip", "table-round-trip", True),
    AssertionContract("append-scan-exact", "append-scan", True),
    AssertionContract("property-update-round-trip", "update-properties", False),
    AssertionContract("schema-evolution-round-trip", "evolve-schema", False),
    AssertionContract("row-delete-round-trip", "delete-rows", False),
    AssertionContract("conflict-recovery-round-trip", "recover-conflict", False),
    AssertionContract(
        "credential-vending-classified", "observe-delegated-access", False
    ),
    AssertionContract("table-rename-round-trip", "rename-table", False),
    AssertionContract("table-register-round-trip", "register-table", False),
    AssertionContract("view-lifecycle-classified", "classify-views", False),
    AssertionContract("pagination-classified", "classify-pagination", False),
    AssertionContract("fixture-clean", "cleanup-fixture", True),
    AssertionContract("transcript-sanitized", "sanitize-transcript", True),
)


class ContractError(ValueError):
    """The invocation does not match the implemented C1-07 contract."""


@dataclass(frozen=True)
class ContractDigests:
    profile_sha256: str
    scenario_sha256: str


@dataclass(frozen=True)
class ResolvedContracts:
    profile: Mapping[str, Any]
    scenario: Mapping[str, Any]
    adapter: Mapping[str, Any]
    catalog_component: Mapping[str, Any]
    client_component: Mapping[str, Any]
    python_component: Mapping[str, Any]
    arrow_component: Mapping[str, Any]
    digests: ContractDigests

    @property
    def assertions(self) -> tuple[AssertionContract, ...]:
        return EXPECTED_ASSERTIONS

    def adapter_limitation(self, capability: str) -> Limitation | None:
        coverage = self.adapter["capabilities"]
        if coverage["mode"] == "exercise-all":
            return None
        for limitation in coverage["unsupported"]:
            if limitation["capability"] == capability:
                return Limitation(
                    attributed_to=limitation["attributed_to"],
                    explanation=limitation["explanation"],
                    upstream_reference=limitation.get("upstream_reference"),
                )
        return None

    def known_client_limitation(self, capability: str) -> Limitation | None:
        limitation = self.scenario["parameters"]["known_client_limitations"].get(
            capability
        )
        if limitation is None:
            return None
        return Limitation(
            attributed_to="client",
            explanation=limitation["explanation"],
            upstream_reference=limitation["upstream_reference"],
        )


def validate_fixture_id(value: str) -> None:
    if FIXTURE_ID_PATTERN.fullmatch(value) is None:
        raise ContractError(
            "fixture ID must contain 1-24 lowercase ASCII letters, digits, or underscores"
        )


def load_contracts(
    profile_path: Path, scenario_path: Path, catalog: str
) -> ResolvedContracts:
    profile_bytes = _read_bytes(profile_path)
    scenario_bytes = _read_bytes(scenario_path)
    profile = _decode_document(profile_bytes, profile_path)
    scenario = _decode_document(scenario_bytes, scenario_path)
    _validate_profile(profile)
    _validate_scenario(scenario)

    adapter = _find_by(profile["catalog_adapters"], "catalog", catalog, "adapter")
    components = profile["components"]
    catalog_component = _find_by(components, "id", catalog, "catalog component")
    parameters = scenario["parameters"]
    client_component = _find_by(
        components, "id", parameters["client_component"], "client component"
    )
    python_component = _find_by(
        components, "id", parameters["python_component"], "Python component"
    )
    arrow_component = _find_by(
        components, "id", parameters["arrow_component"], "Arrow component"
    )

    _validate_component_version(client_component, parameters["client_version"])
    _validate_component_version(python_component, parameters["python_version"])
    _validate_component_version(arrow_component, parameters["arrow_version"])
    _validate_protocol_native_adapter(profile, scenario, adapter)

    return ResolvedContracts(
        profile=profile,
        scenario=scenario,
        adapter=adapter,
        catalog_component=catalog_component,
        client_component=client_component,
        python_component=python_component,
        arrow_component=arrow_component,
        digests=ContractDigests(
            profile_sha256=hashlib.sha256(profile_bytes).hexdigest(),
            scenario_sha256=hashlib.sha256(scenario_bytes).hexdigest(),
        ),
    )


def profile_catalog_ids(profile_path: Path) -> tuple[str, ...]:
    """Read one profile strictly and return its adapter order exactly once."""

    profile = _decode_document(_read_bytes(profile_path), profile_path)
    _validate_profile(profile)
    identifiers = tuple(
        adapter.get("catalog")
        for adapter in profile["catalog_adapters"]
        if isinstance(adapter, dict)
    )
    if not identifiers or any(
        not isinstance(item, str) or not item for item in identifiers
    ):
        raise ContractError("profile catalog adapters require non-empty string IDs")
    if len(identifiers) != len(profile["catalog_adapters"]):
        raise ContractError("profile catalog adapters must be objects")
    if len(set(identifiers)) != len(identifiers):
        raise ContractError("profile catalog adapter IDs must be unique")
    return identifiers


def component_identity(component: Mapping[str, Any]) -> dict[str, Any]:
    """Copy only immutable, non-secret identity fields into evidence."""

    value: dict[str, Any] = {
        "id": component["id"],
        "name": component["name"],
        "version": component["version"],
    }
    if source := component.get("source"):
        value["source"] = {
            "repository": source["repository"],
            "revision": source["revision"],
            **({"tag": source["tag"]} if "tag" in source else {}),
        }
    artifact = component["artifact"]
    safe_artifact = {key: artifact[key] for key in ("kind",) if key in artifact}
    for key in ("reference", "digest_scope", "ecosystem", "package", "version"):
        if key in artifact:
            safe_artifact[key] = artifact[key]
    for key in ("digest", "platform_digest"):
        if key in artifact:
            safe_artifact[key] = dict(artifact[key])
    value["artifact"] = safe_artifact
    return value


def _read_bytes(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except OSError as error:
        raise ContractError(f"failed to read contract {path}") from error


def _decode_document(data: bytes, path: Path) -> Mapping[str, Any]:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ContractError(f"duplicate key {key!r} in {path}")
            value[key] = item
        return value

    try:
        value = json.loads(data, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractError(f"invalid JSON contract {path}") from error
    if not isinstance(value, dict):
        raise ContractError(f"contract {path} must be a JSON object")
    return value


def _validate_profile(profile: Mapping[str, Any]) -> None:
    if (
        profile.get("contract_version") != CONTRACT_VERSION
        or profile.get("kind") != "profile"
    ):
        raise ContractError("profile must be a catalog-bench/v1 profile")
    if not isinstance(profile.get("components"), list):
        raise ContractError("profile components must be a list")
    if not isinstance(profile.get("catalog_adapters"), list):
        raise ContractError("profile catalog adapters must be a list")


def _validate_scenario(scenario: Mapping[str, Any]) -> None:
    if (
        scenario.get("contract_version") != CONTRACT_VERSION
        or scenario.get("kind") != "scenario"
    ):
        raise ContractError("scenario must be a catalog-bench/v1 scenario")
    if scenario.get("id") != SCENARIO_ID or scenario.get("version") != SCENARIO_VERSION:
        raise ContractError(f"runner requires {SCENARIO_ID} version {SCENARIO_VERSION}")
    if scenario.get("classification") != "strict-v1":
        raise ContractError("PyIceberg scenario requires strict-v1 classification")
    parameters = scenario.get("parameters")
    if not isinstance(parameters, dict):
        raise ContractError("scenario parameters must be an object")
    expected_parameters = {
        "client_component": "pyiceberg",
        "client_version": "0.11.1",
        "python_component": "cpython",
        "python_version": "3.13.15",
        "arrow_component": "pyarrow",
        "arrow_version": "25.0.1",
        "transcript_format": TRANSCRIPT_FORMAT,
        "fixture_prefix": "cb_c107",
    }
    for key, expected in expected_parameters.items():
        if parameters.get(key) != expected:
            raise ContractError(f"scenario parameter {key!r} drifted from {expected!r}")
    expected_workload = {
        "conflict_rows_per_writer": 4,
        "delete_filter": "id < 4",
        "evolved_rows": 8,
        "initial_rows": 32,
        "schema_evolution": {
            "field": {"name": "note", "required": False, "type": "string"}
        },
        "table_schema": {
            "fields": [
                {"id": 1, "name": "id", "required": True, "type": "long"},
                {
                    "id": 2,
                    "name": "category",
                    "required": False,
                    "type": "string",
                },
                {
                    "id": 3,
                    "name": "amount",
                    "required": False,
                    "type": "double",
                },
            ],
            "schema-id": 0,
            "type": "struct",
        },
    }
    for key, expected in expected_workload.items():
        if parameters.get(key) != expected:
            raise ContractError(f"scenario workload {key!r} drifted from v1")
    for key in ("initial_properties", "namespace_properties", "property_updates"):
        _validate_string_mapping(parameters.get(key), f"scenario parameter {key!r}")
    removals = parameters.get("property_removals")
    if not isinstance(removals, list) or any(
        not isinstance(item, str) or not item for item in removals
    ):
        raise ContractError("scenario property removals must be non-empty strings")
    object_store = parameters.get("object_store")
    if not isinstance(object_store, dict) or set(object_store) != {
        "access_key_env",
        "endpoint",
        "region",
        "secret_key_env",
    }:
        raise ContractError("scenario object-store parameters drifted from v1")
    _validate_string_mapping(object_store, "scenario object-store parameters")
    known = parameters.get("known_client_limitations")
    if not isinstance(known, dict) or set(known) != {
        "client.pyiceberg.pagination",
        "client.pyiceberg.view-lifecycle",
    }:
        raise ContractError("known client limitations drifted from the v1 policy")

    actual_steps = tuple(step.get("id") for step in scenario.get("steps", []))
    if actual_steps != tuple(STEP_CAPABILITIES):
        raise ContractError("scenario step order drifted from the v1 runner")
    actual_assertions = tuple(
        AssertionContract(
            assertion.get("id"), assertion.get("step"), assertion.get("required")
        )
        for assertion in scenario.get("assertions", [])
    )
    if actual_assertions != EXPECTED_ASSERTIONS:
        raise ContractError("scenario assertions drifted from the v1 runner")


def _validate_string_mapping(value: Any, role: str) -> None:
    if not isinstance(value, dict) or any(
        not isinstance(key, str)
        or not key
        or not isinstance(item, str)
        or not item
        for key, item in value.items()
    ):
        raise ContractError(f"{role} must map non-empty strings to non-empty strings")


def _validate_protocol_native_adapter(
    profile: Mapping[str, Any],
    scenario: Mapping[str, Any],
    adapter: Mapping[str, Any],
) -> None:
    if adapter.get("request_handling") != {"kind": "protocol-native"}:
        raise ContractError("stock-client evidence refuses behavior-changing shims")
    vocabulary = {item["id"] for item in profile["catalog_capabilities"]}
    requirements = {item["capability"] for item in scenario["capabilities"]}
    missing = requirements - vocabulary
    if missing:
        raise ContractError(
            f"scenario capabilities absent from profile: {sorted(missing)}"
        )

    coverage = adapter.get("capabilities")
    if coverage == {"mode": "exercise-all"}:
        return
    if not isinstance(coverage, dict) or coverage.get("mode") != "explicit":
        raise ContractError("adapter capability coverage is invalid")
    exercised = set(coverage.get("exercise", []))
    unsupported = {item["capability"] for item in coverage.get("unsupported", [])}
    if requirements - exercised - unsupported:
        raise ContractError("adapter does not classify every scenario capability")
    if exercised & unsupported:
        raise ContractError("adapter capability cannot be exercised and unsupported")


def _find_by(
    values: list[Mapping[str, Any]], key: str, expected: str, role: str
) -> Mapping[str, Any]:
    matches = [value for value in values if value.get(key) == expected]
    if len(matches) != 1:
        raise ContractError(f"profile must contain exactly one {role} {expected!r}")
    return matches[0]


def _validate_component_version(component: Mapping[str, Any], expected: str) -> None:
    if component.get("version") != expected:
        raise ContractError(
            f"component {component.get('id')!r} version does not equal {expected!r}"
        )
