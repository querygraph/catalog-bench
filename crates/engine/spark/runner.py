#!/usr/bin/env python3
"""Stock Spark renderer for the catalog-bench engine interoperability plan.

The Rust runner supplies one closed, secret-free plan. This module translates
that plan into Spark and Iceberg public operations without issuing HTTP itself
or selecting behavior by catalog identity. Persisted output is limited to the
fixed ``CATALOG_BENCH_EVENT`` protocol; Spark logs and exception messages are
never copied into evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import SplitResult, urlsplit


PLAN_FORMAT = "catalog-bench/spark-engine-plan/v1"
TRANSCRIPT_FORMAT = "catalog-bench/engine-interoperability-transcript/v1"
EVENT_PREFIX = "CATALOG_BENCH_EVENT "
MAXIMUM_PLAN_BYTES = 256 * 1024
MAXIMUM_GENERATED_ROWS = 100_000
MAXIMUM_SPARK_LONG = (1 << 63) - 1
MAXIMUM_OBSERVATION_TEXT_BYTES = 2048
COLLISION_EXIT = 3
FAILURE_EXIT = 2
OAUTH_CLIENT_ID_ENV = "CATALOG_BENCH_ENGINE_CLIENT_ID"
OAUTH_CLIENT_SECRET_ENV = "CATALOG_BENCH_ENGINE_CLIENT_SECRET"
S3_FILE_IO = "org.apache.iceberg.aws.s3.S3FileIO"

EXPECTED_EXECUTION = {
    "master": "local[2]",
    "shuffle_partitions": 1,
    "default_parallelism": 1,
}
EXPECTED_ENGINE_POLICY = {
    "catalog_specific_branches": "forbidden",
    "catalog_specific_shims": "forbidden",
    "connector": "stock-profile-component",
    "syntax_rendering": "engine-specific-but-catalog-neutral",
    "unsupported": "classify-before-mutation-without-a-substitute-request",
}


class PlanViolation(ValueError):
    """A fixed-schema plan is malformed or internally inconsistent."""


def require_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PlanViolation(f"{label} must be an object")
    return value


def require_keys(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    actual = set(value)
    if actual != expected:
        raise PlanViolation(f"{label} fields do not match the closed schema")


def require_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise PlanViolation(f"{label} must be a nonempty string")
    return value


def require_unsigned(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise PlanViolation(f"{label} must be an unsigned integer")
    return value


def require_bounded_text(value: Any, label: str) -> str:
    text = require_text(value, label)
    if len(text.encode("utf-8")) > MAXIMUM_OBSERVATION_TEXT_BYTES:
        raise PlanViolation(f"{label} exceeds the observation byte limit")
    if any(not character.isprintable() for character in text):
        raise PlanViolation(f"{label} contains a control character")
    return text


def require_s3_location(value: Any, label: str, bucket: str) -> SplitResult:
    location = require_bounded_text(value, label)
    try:
        parsed = urlsplit(location)
    except ValueError as error:
        raise PlanViolation(f"{label} is not a valid URI") from error
    if (
        parsed.scheme != "s3"
        or parsed.netloc != bucket
        or not parsed.path
        or parsed.path == "/"
        or parsed.query
        or parsed.fragment
        or "\\" in parsed.path
    ):
        raise PlanViolation(
            f"{label} must be a credential-free URI in the profile bucket"
        )
    return parsed


def load_plan(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        payload = stream.read(MAXIMUM_PLAN_BYTES + 1)
    if len(payload) > MAXIMUM_PLAN_BYTES:
        raise PlanViolation("Spark plan exceeds its byte limit")
    try:
        plan = require_object(json.loads(payload), "plan")
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PlanViolation("Spark plan is not valid UTF-8 JSON") from error
    validate_plan(plan)
    return plan


def validate_plan(plan: Mapping[str, Any]) -> None:
    require_keys(
        plan,
        {"format", "execution", "catalog", "file_io", "fixture", "scenario"},
        "plan",
    )
    if plan["format"] != PLAN_FORMAT:
        raise PlanViolation("unsupported Spark plan format")

    execution = require_object(plan["execution"], "execution")
    require_keys(
        execution,
        {"master", "shuffle_partitions", "default_parallelism"},
        "execution",
    )
    require_text(execution["master"], "execution.master")
    require_unsigned(execution["shuffle_partitions"], "shuffle_partitions")
    require_unsigned(execution["default_parallelism"], "default_parallelism")
    if execution != EXPECTED_EXECUTION:
        raise PlanViolation("unsupported Spark execution settings")

    catalog = require_object(plan["catalog"], "catalog")
    required_catalog = {"name", "uri", "authentication"}
    optional_catalog = {"warehouse", "prefix"}
    if not required_catalog.issubset(catalog) or not set(catalog).issubset(
        required_catalog | optional_catalog
    ):
        raise PlanViolation("catalog fields do not match the closed schema")
    require_identifier(catalog["name"], "catalog.name")
    require_text(catalog["uri"], "catalog.uri")
    for optional in optional_catalog:
        if optional in catalog:
            require_text(catalog[optional], f"catalog.{optional}")
    authentication = require_object(catalog["authentication"], "authentication")
    kind = authentication.get("kind")
    if kind == "anonymous":
        require_keys(authentication, {"kind"}, "anonymous authentication")
    elif kind == "oauth2-client-credentials":
        require_keys(
            authentication,
            {"kind", "oauth2_server_uri", "scope"},
            "OAuth2 authentication",
        )
        require_text(authentication["oauth2_server_uri"], "oauth2_server_uri")
        require_text(authentication["scope"], "scope")
    else:
        raise PlanViolation("unsupported catalog authentication kind")

    file_io = require_object(plan["file_io"], "file_io")
    require_keys(
        file_io,
        {"implementation", "endpoint", "bucket", "region", "path_style_access"},
        "file_io",
    )
    for key in ("implementation", "endpoint", "bucket", "region"):
        require_text(file_io[key], f"file_io.{key}")
    if file_io["implementation"] != S3_FILE_IO:
        raise PlanViolation("unsupported Iceberg file IO")
    if file_io["path_style_access"] is not True:
        raise PlanViolation("shared MinIO requires path-style access")

    fixture = require_object(plan["fixture"], "fixture")
    required_fixture = {"namespace", "table"}
    if not required_fixture.issubset(fixture) or not set(fixture).issubset(
        required_fixture | {"requested_location"}
    ):
        raise PlanViolation("fixture fields do not match the closed schema")
    require_identifier(fixture["namespace"], "fixture.namespace")
    require_identifier(fixture["table"], "fixture.table")
    if "requested_location" in fixture:
        require_text(fixture["requested_location"], "fixture.requested_location")

    scenario = require_object(plan["scenario"], "scenario")
    require_keys(
        scenario,
        {
            "catalog_protocol",
            "engine_policy",
            "fixture_prefix",
            "table",
            "schema_evolution",
            "row_generator",
            "batches",
            "canonical_reads",
            "object_audit",
            "transcript_format",
        },
        "scenario",
    )
    if scenario["catalog_protocol"] != "iceberg-rest-v1":
        raise PlanViolation("unsupported catalog protocol")
    engine_policy = require_object(scenario["engine_policy"], "engine_policy")
    if engine_policy != EXPECTED_ENGINE_POLICY:
        raise PlanViolation("engine policy does not authorize this renderer")
    fixture_prefix = require_identifier(scenario["fixture_prefix"], "fixture_prefix")
    if not fixture["namespace"].startswith(f"{fixture_prefix}_"):
        raise PlanViolation("fixture namespace is outside the scenario prefix")
    validate_table(require_object(scenario["table"], "scenario.table"))
    validate_evolution(
        require_object(scenario["schema_evolution"], "scenario.schema_evolution")
    )
    validate_generators(
        require_object(scenario["row_generator"], "scenario.row_generator")
    )
    validate_batches(require_object(scenario["batches"], "scenario.batches"))
    validate_reads(
        require_object(scenario["canonical_reads"], "scenario.canonical_reads")
    )
    validate_object_audit(
        require_object(scenario["object_audit"], "scenario.object_audit")
    )
    if scenario["transcript_format"] != TRANSCRIPT_FORMAT:
        raise PlanViolation("unsupported transcript format")
    validate_scenario_relations(scenario)


def require_identifier(value: Any, label: str) -> str:
    identifier = require_text(value, label)
    if not all(
        character.isascii() and (character.isalnum() or character == "_")
        for character in identifier
    ):
        raise PlanViolation(f"{label} contains an unsupported identifier character")
    return identifier


def validate_table(table: Mapping[str, Any]) -> None:
    require_keys(
        table,
        {"file_format", "format_version", "properties", "schema"},
        "scenario.table",
    )
    if table["file_format"] != "parquet" or table["format_version"] != 2:
        raise PlanViolation("renderer supports only format-v2 Parquet tables")
    properties = require_object(table["properties"], "table.properties")
    for key, value in properties.items():
        require_text(key, "table property key")
        require_text(value, f"table property {key}")
    schema = require_object(table["schema"], "table.schema")
    require_keys(schema, {"schema-id", "type", "fields"}, "table.schema")
    if schema["schema-id"] != 0 or schema["type"] != "struct":
        raise PlanViolation("renderer requires struct schema zero")
    fields = schema["fields"]
    if not isinstance(fields, list) or not fields:
        raise PlanViolation("table fields must be a nonempty array")
    field_ids = set()
    field_names = set()
    for field in fields:
        validate_field(require_object(field, "table field"), include_id=True)
        if field["id"] in field_ids or field["name"] in field_names:
            raise PlanViolation("table field IDs and names must be unique")
        field_ids.add(field["id"])
        field_names.add(field["name"])


def validate_evolution(evolution: Mapping[str, Any]) -> None:
    require_keys(
        evolution,
        {"field", "preserve_existing_field_ids"},
        "schema_evolution",
    )
    validate_field(
        require_object(evolution["field"], "evolution field"), include_id=False
    )
    if evolution["preserve_existing_field_ids"] is not True:
        raise PlanViolation("renderer requires existing field IDs to be preserved")


def validate_field(field: Mapping[str, Any], include_id: bool) -> None:
    expected = {"name", "required", "type"} | ({"id"} if include_id else set())
    require_keys(field, expected, "field")
    require_identifier(field["name"], "field.name")
    if not isinstance(field["required"], bool):
        raise PlanViolation("field.required must be a boolean")
    if field["type"] not in {"long", "string"}:
        raise PlanViolation("unsupported field type")
    if include_id and require_unsigned(field["id"], "field.id") == 0:
        raise PlanViolation("field.id must be positive")


def validate_generators(generators: Mapping[str, Any]) -> None:
    require_keys(generators, {"amount_cents", "category", "note"}, "row_generator")
    amount = require_object(generators["amount_cents"], "amount_cents generator")
    require_keys(amount, {"kind", "multiplier", "offset"}, "amount_cents generator")
    if amount["kind"] != "affine":
        raise PlanViolation("unsupported amount generator")
    require_unsigned(amount["multiplier"], "amount multiplier")
    require_unsigned(amount["offset"], "amount offset")
    category = require_object(generators["category"], "category generator")
    require_keys(category, {"kind", "modulus", "prefix"}, "category generator")
    if category["kind"] != "modulo-label" or require_unsigned(
        category["modulus"], "category modulus"
    ) == 0:
        raise PlanViolation("unsupported category generator")
    require_text(category["prefix"], "category prefix")
    note = require_object(generators["note"], "note generator")
    require_keys(note, {"kind", "prefix"}, "note generator")
    if note["kind"] != "id-label":
        raise PlanViolation("unsupported note generator")
    require_text(note["prefix"], "note prefix")


def validate_batches(batches: Mapping[str, Any]) -> None:
    require_keys(batches, {"initial", "evolved"}, "batches")
    decoded = []
    for name in ("initial", "evolved"):
        batch = require_object(batches[name], f"{name} batch")
        require_keys(batch, {"id_start", "rows"}, f"{name} batch")
        start = require_unsigned(batch["id_start"], f"{name}.id_start")
        rows = require_unsigned(batch["rows"], f"{name}.rows")
        if rows == 0:
            raise PlanViolation("batch row counts must be positive")
        decoded.append((start, rows))
    if decoded[0][0] + decoded[0][1] != decoded[1][0]:
        raise PlanViolation("batches must be contiguous")
    final_identifier = decoded[1][0] + decoded[1][1] - 1
    if sum(rows for _, rows in decoded) > MAXIMUM_GENERATED_ROWS:
        raise PlanViolation("generated row count exceeds the renderer limit")
    if final_identifier > MAXIMUM_SPARK_LONG:
        raise PlanViolation("generated identifier exceeds Spark LONG")


def validate_reads(reads: Mapping[str, Any]) -> None:
    require_keys(
        reads,
        {"encoding", "initial", "order_by", "after_evolution", "trailing_lf"},
        "canonical_reads",
    )
    if reads["encoding"] != "compact-rfc8259-json-array-per-row-utf8-lf":
        raise PlanViolation("unsupported canonical encoding")
    if reads["order_by"] != ["id"] or reads["trailing_lf"] is not True:
        raise PlanViolation("canonical reads require ID ordering and a final LF")
    for name in ("initial", "after_evolution"):
        identity = require_object(reads[name], f"{name} read")
        require_keys(identity, {"bytes", "columns", "rows", "sha256"}, f"{name} read")
        require_unsigned(identity["bytes"], f"{name}.bytes")
        require_unsigned(identity["rows"], f"{name}.rows")
        if not isinstance(identity["columns"], list) or not identity["columns"]:
            raise PlanViolation(f"{name}.columns must be a nonempty array")
        for column in identity["columns"]:
            require_identifier(column, f"{name} column")
        digest = require_text(identity["sha256"], f"{name}.sha256")
        if len(digest) != 64 or any(
            character not in "0123456789abcdef" for character in digest
        ):
            raise PlanViolation(f"{name}.sha256 must be lowercase SHA-256")


def validate_object_audit(object_audit: Mapping[str, Any]) -> None:
    require_keys(
        object_audit,
        {"minimum_metadata_objects", "minimum_parquet_objects", "scope"},
        "object_audit",
    )
    if require_unsigned(
        object_audit["minimum_metadata_objects"], "minimum_metadata_objects"
    ) == 0:
        raise PlanViolation("minimum_metadata_objects must be positive")
    if require_unsigned(
        object_audit["minimum_parquet_objects"], "minimum_parquet_objects"
    ) == 0:
        raise PlanViolation("minimum_parquet_objects must be positive")
    if object_audit["scope"] != "returned-table-root-in-profile-shared-object-store":
        raise PlanViolation("unsupported object audit scope")


def validate_scenario_relations(scenario: Mapping[str, Any]) -> None:
    table_fields = scenario["table"]["schema"]["fields"]
    evolved_field = scenario["schema_evolution"]["field"]
    if evolved_field["name"] in {field["name"] for field in table_fields}:
        raise PlanViolation("evolved field must be new")

    batches = scenario["batches"]
    reads = scenario["canonical_reads"]
    initial_columns = [field["name"] for field in table_fields]
    evolved_columns = initial_columns + [evolved_field["name"]]
    if reads["initial"]["columns"] != initial_columns:
        raise PlanViolation("initial read columns do not match the table schema")
    if reads["after_evolution"]["columns"] != evolved_columns:
        raise PlanViolation("evolved read columns do not match the evolved schema")
    if reads["initial"]["rows"] != batches["initial"]["rows"]:
        raise PlanViolation("initial read row count does not match its batch")
    if reads["after_evolution"]["rows"] != (
        batches["initial"]["rows"] + batches["evolved"]["rows"]
    ):
        raise PlanViolation("evolved read row count does not match both batches")

    final_identifier = batches["evolved"]["id_start"] + batches["evolved"]["rows"] - 1
    amount = scenario["row_generator"]["amount_cents"]
    if final_identifier * amount["multiplier"] + amount["offset"] > MAXIMUM_SPARK_LONG:
        raise PlanViolation("generated amount exceeds Spark LONG")


def generated_rows(plan: Mapping[str, Any], include_evolved: bool) -> list[list[Any]]:
    scenario = plan["scenario"]
    generators = scenario["row_generator"]
    batches = scenario["batches"]
    amount = generators["amount_cents"]
    category = generators["category"]
    note = generators["note"]

    def base(identifier: int) -> list[Any]:
        return [
            identifier,
            f"{category['prefix']}{identifier % category['modulus']}",
            identifier * amount["multiplier"] + amount["offset"],
        ]

    initial = batches["initial"]
    rows = [
        base(identifier) + ([None] if include_evolved else [])
        for identifier in range(
            initial["id_start"], initial["id_start"] + initial["rows"]
        )
    ]
    if include_evolved:
        evolved = batches["evolved"]
        rows.extend(
            base(identifier) + [f"{note['prefix']}{identifier}"]
            for identifier in range(
                evolved["id_start"], evolved["id_start"] + evolved["rows"]
            )
        )
    return rows


def canonical_identity(rows: Iterable[Sequence[Any]]) -> dict[str, Any]:
    payload = b"".join(
        json.dumps(list(row), ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        + b"\n"
        for row in rows
    )
    return {
        "rows": payload.count(b"\n"),
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def quote_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def full_table_name(plan: Mapping[str, Any]) -> str:
    return ".".join(
        quote_identifier(part)
        for part in (
            plan["catalog"]["name"],
            plan["fixture"]["namespace"],
            plan["fixture"]["table"],
        )
    )


def metadata_table_name(plan: Mapping[str, Any], name: str) -> str:
    return f"{full_table_name(plan)}.{quote_identifier(name)}"


def create_table_sql(plan: Mapping[str, Any]) -> str:
    table = plan["scenario"]["table"]
    fields = []
    for field in table["schema"]["fields"]:
        sql_type = {"long": "BIGINT", "string": "STRING"}[field["type"]]
        nullability = " NOT NULL" if field["required"] else ""
        fields.append(f"{quote_identifier(field['name'])} {sql_type}{nullability}")
    properties = dict(table["properties"])
    properties["format-version"] = str(table["format_version"])
    properties["write.format.default"] = table["file_format"]
    rendered_properties = ", ".join(
        f"{quote_literal(key)}={quote_literal(value)}"
        for key, value in sorted(properties.items())
    )
    location = plan["fixture"].get("requested_location")
    location_clause = f" LOCATION {quote_literal(location)}" if location else ""
    return (
        f"CREATE TABLE {full_table_name(plan)} ({', '.join(fields)}) "
        f"USING iceberg{location_clause} TBLPROPERTIES ({rendered_properties})"
    )


def validation_observation(
    plan: Mapping[str, Any], property_mismatch: bool = False
) -> dict[str, Any]:
    location = plan["fixture"].get(
        "requested_location", f"s3://{plan['file_io']['bucket']}/validation/table"
    )
    fields = [
        {
            "id": field["id"],
            "name": field["name"],
            "required": field["required"],
            "field_type": field["type"],
        }
        for field in plan["scenario"]["table"]["schema"]["fields"]
    ]
    properties = dict(plan["scenario"]["table"]["properties"])
    if property_mismatch:
        first_property = next(iter(properties))
        properties[first_property] = "validation-only-mismatch"
    return sanitize_table_observation(
        {
            "table_uuid": "00000000-0000-0000-0000-000000000001",
            "metadata_location": f"{location.rstrip('/')}/metadata/v1.metadata.json",
            "location": location,
            "format_version": 2,
            "last_column_id": max(field["id"] for field in fields),
            "schema": fields,
            "snapshots": 0,
            "properties": properties,
        },
        plan,
    )


def validate_oracles(plan: Mapping[str, Any]) -> dict[str, Any]:
    reads = plan["scenario"]["canonical_reads"]
    initial = canonical_identity(generated_rows(plan, include_evolved=False))
    evolved = canonical_identity(generated_rows(plan, include_evolved=True))
    if initial != {key: reads["initial"][key] for key in ("rows", "bytes", "sha256")}:
        raise PlanViolation(
            "initial canonical row identity does not match the scenario"
        )
    if evolved != {
        key: reads["after_evolution"][key] for key in ("rows", "bytes", "sha256")
    }:
        raise PlanViolation(
            "evolved canonical row identity does not match the scenario"
        )
    return {
        "initial": initial,
        "after_evolution": evolved,
        "create_sql_sha256": hashlib.sha256(
            create_table_sql(plan).encode("utf-8")
        ).hexdigest(),
        "observation": validation_observation(plan),
        "property_mismatch": validation_observation(plan, True)["properties"],
    }


def emit(event: str, **fields: Any) -> None:
    payload = {"event": event, **fields}
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    print(EVENT_PREFIX + encoded, flush=True)


def fail(stage: str, category: str) -> int:
    emit("failed", stage=stage, category=category)
    return FAILURE_EXIT


def catalog_options(plan: Mapping[str, Any]) -> dict[str, str]:
    catalog = plan["catalog"]
    file_io = plan["file_io"]
    prefix = f"spark.sql.catalog.{catalog['name']}"
    options = {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": catalog["uri"],
        f"{prefix}.io-impl": file_io["implementation"],
        f"{prefix}.s3.endpoint": file_io["endpoint"],
        f"{prefix}.s3.path-style-access": str(file_io["path_style_access"]).lower(),
        f"{prefix}.client.region": file_io["region"],
    }
    for key in ("warehouse", "prefix"):
        if key in catalog:
            options[f"{prefix}.{key}"] = catalog[key]
    authentication = catalog["authentication"]
    if authentication["kind"] == "oauth2-client-credentials":
        client_id = require_text(
            os.environ.get(OAUTH_CLIENT_ID_ENV), OAUTH_CLIENT_ID_ENV
        )
        client_secret = require_text(
            os.environ.get(OAUTH_CLIENT_SECRET_ENV), OAUTH_CLIENT_SECRET_ENV
        )
        options[f"{prefix}.credential"] = f"{client_id}:{client_secret}"
        options[f"{prefix}.oauth2-server-uri"] = authentication["oauth2_server_uri"]
        options[f"{prefix}.scope"] = authentication["scope"]
    return options


def build_spark(plan: Mapping[str, Any]) -> Any:
    from pyspark.sql import SparkSession

    execution = plan["execution"]
    builder = (
        SparkSession.builder.appName("catalog-bench-engine-interoperability")
        .master(execution["master"])
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", str(execution["shuffle_partitions"]))
        .config("spark.default.parallelism", str(execution["default_parallelism"]))
    )
    for key, value in catalog_options(plan).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def sanitize_runtime_observation(observation: Any) -> dict[str, str]:
    runtime = require_object(observation, "runtime observation")
    require_keys(
        runtime,
        {
            "spark_version",
            "scala_version",
            "java_version",
            "operating_system",
            "architecture",
        },
        "runtime observation",
    )
    return {
        key: require_bounded_text(runtime[key], f"runtime.{key}")
        for key in runtime
    }


def runtime_observation(spark: Any) -> dict[str, str]:
    java = spark._jvm.java.lang.System
    return sanitize_runtime_observation(
        {
            "spark_version": spark.version,
            "scala_version": spark._jvm.scala.util.Properties.versionNumberString(),
            "java_version": java.getProperty("java.version"),
            "operating_system": java.getProperty("os.name"),
            "architecture": java.getProperty("os.arch"),
        }
    )


def sanitize_table_observation(
    observation: Any, plan: Mapping[str, Any]
) -> dict[str, Any]:
    table = require_object(observation, "table observation")
    require_keys(
        table,
        {
            "table_uuid",
            "metadata_location",
            "location",
            "format_version",
            "last_column_id",
            "schema",
            "snapshots",
            "properties",
        },
        "table observation",
    )
    try:
        table_uuid = uuid.UUID(require_bounded_text(table["table_uuid"], "table UUID"))
    except (ValueError, AttributeError) as error:
        raise PlanViolation("table UUID is invalid") from error
    if table_uuid.int == 0:
        raise PlanViolation("table UUID must not be nil")

    bucket = plan["file_io"]["bucket"]
    location = require_bounded_text(table["location"], "table location")
    metadata_location = require_bounded_text(
        table["metadata_location"], "metadata location"
    )
    table_uri = require_s3_location(location, "table location", bucket)
    metadata_uri = require_s3_location(
        metadata_location, "metadata location", bucket
    )
    table_path = table_uri.path.rstrip("/")
    if (
        not metadata_uri.path.startswith(f"{table_path}/")
        or not metadata_uri.path.endswith(".metadata.json")
    ):
        raise PlanViolation("metadata location is outside the table root")
    requested_location = plan["fixture"].get("requested_location")
    if requested_location is not None and location != requested_location:
        raise PlanViolation("table location differs from the requested location")

    format_version = require_unsigned(table["format_version"], "format version")
    last_column_id = require_unsigned(table["last_column_id"], "last column ID")
    snapshots = require_unsigned(table["snapshots"], "snapshot count")
    if format_version != 2 or last_column_id == 0:
        raise PlanViolation("table observation is not a valid format-v2 table")

    expected_fields = {
        field["name"]: field for field in plan["scenario"]["table"]["schema"]["fields"]
    }
    evolved = dict(plan["scenario"]["schema_evolution"]["field"])
    evolved["id"] = max(field["id"] for field in expected_fields.values()) + 1
    expected_fields[evolved["name"]] = evolved
    fields = table["schema"]
    if not isinstance(fields, list) or not fields:
        raise PlanViolation("observed schema must be a nonempty array")
    field_ids: set[int] = set()
    field_names: set[str] = set()
    sanitized_fields = []
    for raw_field in fields:
        field = require_object(raw_field, "observed field")
        require_keys(
            field,
            {"id", "name", "required", "field_type"},
            "observed field",
        )
        field_id = require_unsigned(field["id"], "observed field ID")
        name = require_identifier(field["name"], "observed field name")
        expected = expected_fields.get(name)
        if (
            field_id == 0
            or field_id in field_ids
            or name in field_names
            or expected is None
            or field_id != expected["id"]
            or field["required"] is not expected["required"]
            or field["field_type"] != expected["type"]
        ):
            raise PlanViolation("observed field differs from the scenario vocabulary")
        field_ids.add(field_id)
        field_names.add(name)
        sanitized_fields.append(
            {
                "id": field_id,
                "name": name,
                "required": expected["required"],
                "field_type": expected["type"],
            }
        )
    if max(field_ids) != last_column_id:
        raise PlanViolation("last column ID differs from the observed schema")

    expected_properties = plan["scenario"]["table"]["properties"]
    properties = require_object(table["properties"], "observed properties")
    if not set(properties).issubset(expected_properties):
        raise PlanViolation("observed properties contain an unknown key")
    property_outcomes = {
        key: "match" if observed == expected_properties[key] else "mismatch"
        for key, observed in properties.items()
    }
    return {
        "table_uuid": str(table_uuid),
        "metadata_location": metadata_location,
        "location": location,
        "format_version": format_version,
        "last_column_id": last_column_id,
        "schema": sanitized_fields,
        "snapshots": snapshots,
        "properties": property_outcomes,
    }


def observe_table(spark: Any, plan: Mapping[str, Any]) -> dict[str, Any]:
    table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, full_table_name(plan)
    )
    table.refresh()
    metadata = table.operations().current()
    fields = []
    field_iterator = metadata.schema().columns().iterator()
    type_names = {"LONG": "long", "STRING": "string"}
    while field_iterator.hasNext():
        field = field_iterator.next()
        type_id = field.type().typeId().name()
        if type_id not in type_names:
            raise PlanViolation("observed unsupported Iceberg field type")
        fields.append(
            {
                "id": field.fieldId(),
                "name": field.name(),
                "required": field.isRequired(),
                "field_type": type_names[type_id],
            }
        )
    properties = {
        key: metadata.properties().get(key)
        for key in plan["scenario"]["table"]["properties"]
        if metadata.properties().get(key) is not None
    }
    return sanitize_table_observation(
        {
            "table_uuid": str(metadata.uuid()),
            "metadata_location": (
                spark._jvm.org.apache.iceberg.TableUtil.metadataFileLocation(table)
            ),
            "location": table.location(),
            "format_version": spark._jvm.org.apache.iceberg.TableUtil.formatVersion(
                table
            ),
            "last_column_id": metadata.lastColumnId(),
            "schema": fields,
            "snapshots": spark.table(metadata_table_name(plan, "snapshots")).count(),
            "properties": properties,
        },
        plan,
    )


def namespace_names(spark: Any, plan: Mapping[str, Any]) -> list[str]:
    catalog = quote_identifier(plan["catalog"]["name"])
    return [row[0] for row in spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()]


def dataframe_schema(plan: Mapping[str, Any], evolved: bool) -> Any:
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    type_map = {"long": LongType, "string": StringType}
    fields = list(plan["scenario"]["table"]["schema"]["fields"])
    if evolved:
        fields.append(plan["scenario"]["schema_evolution"]["field"])
    return StructType(
        [
            StructField(
                field["name"],
                type_map[field["type"]](),
                nullable=not field["required"],
            )
            for field in fields
        ]
    )


def append_rows(spark: Any, plan: Mapping[str, Any], evolved: bool) -> None:
    if evolved:
        rows = generated_rows(plan, include_evolved=True)[
            plan["scenario"]["batches"]["initial"]["rows"] :
        ]
    else:
        rows = generated_rows(plan, include_evolved=False)
    frame = spark.createDataFrame(rows, schema=dataframe_schema(plan, evolved))
    frame.coalesce(1).writeTo(full_table_name(plan)).append()


def read_rows(spark: Any, plan: Mapping[str, Any], evolved: bool) -> dict[str, Any]:
    read = plan["scenario"]["canonical_reads"][
        "after_evolution" if evolved else "initial"
    ]
    frame = (
        spark.table(full_table_name(plan))
        .select(*read["columns"])
        .orderBy(*plan["scenario"]["canonical_reads"]["order_by"])
    )
    identity = canonical_identity(
        [[row[column] for column in read["columns"]] for row in frame.collect()]
    )
    expected = {key: read[key] for key in ("rows", "bytes", "sha256")}
    if identity != expected:
        raise PlanViolation("stock-engine rows differ from the canonical scenario read")
    return expected


def run_spark(plan: Mapping[str, Any]) -> int:
    spark = None
    try:
        try:
            spark = build_spark(plan)
            emit("runtime-ready", runtime=runtime_observation(spark))
        except BaseException:
            return fail("verify-runtime", "runtime")

        try:
            spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergCatalog(
                spark._jsparkSession, plan["catalog"]["name"]
            )
            emit("catalog-ready")
        except BaseException:
            return fail("initialize-catalog", "connector")

        try:
            absent = plan["fixture"]["namespace"] not in namespace_names(spark, plan)
            emit("fixture-preflight", absent=absent)
            if not absent:
                return COLLISION_EXIT
        except BaseException:
            return fail("preflight-fixture", "catalog")

        try:
            namespace = ".".join(
                quote_identifier(part)
                for part in (plan["catalog"]["name"], plan["fixture"]["namespace"])
            )
            spark.sql(f"CREATE NAMESPACE {namespace}")
            listed = (
                namespace_names(spark, plan).count(plan["fixture"]["namespace"])
                == 1
            )
            if not listed:
                return fail("create-namespace", "catalog")
            emit("namespace-ready", listed_exactly=True)
        except BaseException:
            return fail("create-namespace", "catalog")

        try:
            spark.sql(create_table_sql(plan))
            emit("table-ready", table=observe_table(spark, plan))
        except BaseException:
            return fail("create-table", "connector")

        try:
            append_rows(spark, plan, evolved=False)
            emit("initial-appended", snapshots=observe_table(spark, plan)["snapshots"])
        except BaseException:
            return fail("append-initial", "data")

        try:
            emit("initial-read", read=read_rows(spark, plan, evolved=False))
        except BaseException:
            return fail("read-initial", "data")

        try:
            field = plan["scenario"]["schema_evolution"]["field"]
            sql_type = {"long": "BIGINT", "string": "STRING"}[field["type"]]
            spark.sql(
                f"ALTER TABLE {full_table_name(plan)} ADD COLUMN "
                f"{quote_identifier(field['name'])} {sql_type}"
            )
            emit("schema-evolved", table=observe_table(spark, plan))
        except BaseException:
            return fail("evolve-schema", "connector")

        try:
            append_rows(spark, plan, evolved=True)
            emit("evolved-appended", snapshots=observe_table(spark, plan)["snapshots"])
        except BaseException:
            return fail("append-evolved", "data")

        try:
            emit("evolved-read", read=read_rows(spark, plan, evolved=True))
        except BaseException:
            return fail("read-evolved", "data")

        try:
            emit("final-table", table=observe_table(spark, plan))
        except BaseException:
            return fail("observe-final-table", "connector")

        emit("completed")
        return 0
    finally:
        if spark is not None:
            try:
                spark.stop()
            except BaseException:
                pass


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a closed catalog-bench Spark plan"
    )
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--validate-plan", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    try:
        plan = load_plan(args.plan)
        validation = validate_oracles(plan)
        if args.validate_plan:
            print(json.dumps(validation, separators=(",", ":"), sort_keys=True))
            return 0
        return run_spark(plan)
    except BaseException:
        if "--validate-plan" in argv:
            return FAILURE_EXIT
        return fail("verify-runtime", "runtime")


if __name__ == "__main__":
    sys.tracebacklimit = 0
    raise SystemExit(main(sys.argv[1:]))
