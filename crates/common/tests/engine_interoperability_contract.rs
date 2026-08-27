use std::collections::BTreeSet;
use std::fmt::Write as _;

use catalog_bench_common::contract::{
    parse_contract, ActorRole, ContractDocument, RequirementLevel, Scenario,
};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};

const SCENARIO_BYTES: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.v2.json");

fn scenario() -> Scenario {
    let ContractDocument::Scenario(scenario) = parse_contract(SCENARIO_BYTES).unwrap() else {
        panic!("engine interoperability contract must be a scenario");
    };
    scenario
}

fn parameter<'a>(value: &'a Value, path: &[&str]) -> &'a Value {
    path.iter().fold(value, |current, segment| {
        current
            .get(segment)
            .unwrap_or_else(|| panic!("missing scenario parameter {}", path.join(".")))
    })
}

fn unsigned(value: &Value, path: &str) -> u64 {
    value
        .as_u64()
        .unwrap_or_else(|| panic!("{path} must be an unsigned integer"))
}

fn text<'a>(value: &'a Value, path: &str) -> &'a str {
    value
        .as_str()
        .unwrap_or_else(|| panic!("{path} must be a string"))
}

fn generated_rows(parameters: &Value, evolved_projection: bool) -> Vec<Value> {
    let category = parameter(parameters, &["row_generator", "category"]);
    let amount = parameter(parameters, &["row_generator", "amount_cents"]);
    let note = parameter(parameters, &["row_generator", "note"]);
    assert_eq!(
        text(&category["kind"], "row_generator.category.kind"),
        "modulo-label"
    );
    assert_eq!(
        text(&amount["kind"], "row_generator.amount_cents.kind"),
        "affine"
    );
    assert_eq!(text(&note["kind"], "row_generator.note.kind"), "id-label");
    let modulus = unsigned(&category["modulus"], "row_generator.category.modulus");
    let category_prefix = text(&category["prefix"], "row_generator.category.prefix");
    let multiplier = unsigned(
        &amount["multiplier"],
        "row_generator.amount_cents.multiplier",
    );
    let offset = unsigned(&amount["offset"], "row_generator.amount_cents.offset");
    let note_prefix = text(&note["prefix"], "row_generator.note.prefix");

    let batches = parameter(parameters, &["batches"]);
    let initial_start = unsigned(&batches["initial"]["id_start"], "batches.initial.id_start");
    let initial_rows = unsigned(&batches["initial"]["rows"], "batches.initial.rows");
    let evolved_start = unsigned(&batches["evolved"]["id_start"], "batches.evolved.id_start");
    let evolved_rows = unsigned(&batches["evolved"]["rows"], "batches.evolved.rows");
    assert_eq!(initial_start + initial_rows, evolved_start);

    let base = |id: u64| {
        (
            id,
            format!("{category_prefix}{}", id % modulus),
            id * multiplier + offset,
        )
    };
    let mut rows = (initial_start..initial_start + initial_rows)
        .map(|id| {
            let (id, category, amount) = base(id);
            if evolved_projection {
                json!([id, category, amount, null])
            } else {
                json!([id, category, amount])
            }
        })
        .collect::<Vec<_>>();
    if evolved_projection {
        rows.extend((evolved_start..evolved_start + evolved_rows).map(|id| {
            let (id, category, amount) = base(id);
            json!([id, category, amount, format!("{note_prefix}{id}")])
        }));
    }
    rows.sort_by_key(|row| row[0].as_u64().unwrap());
    rows
}

fn canonical_identity(rows: &[Value]) -> (usize, String) {
    let mut bytes = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut bytes, row).unwrap();
        bytes.push(b'\n');
    }
    let mut digest = String::with_capacity(64);
    for byte in Sha256::digest(&bytes) {
        write!(digest, "{byte:02x}").unwrap();
    }
    (bytes.len(), digest)
}

#[test]
fn common_engine_scenario_is_catalog_and_runtime_neutral() {
    let scenario = scenario();

    assert_eq!(scenario.id.as_str(), "engine.iceberg.write-read-evolution");
    assert_eq!(scenario.version, 2);
    assert!(scenario.steps.iter().all(|step| matches!(
        step.actor,
        ActorRole::Harness | ActorRole::Engine | ActorRole::ObjectStore
    )));
    assert!(scenario.steps.iter().all(|step| {
        step.operation.starts_with("engine.")
            || step.operation.starts_with("iceberg-rest.")
            || step.operation.starts_with("object-store.")
            || step.operation.starts_with("evidence.")
    }));

    let encoded = std::str::from_utf8(SCENARIO_BYTES)
        .unwrap()
        .to_ascii_lowercase();
    for implementation in [
        "lakecat",
        "polaris",
        "gravitino",
        "lakekeeper",
        "nessie",
        "spark",
        "flink",
        "trino",
        "duckdb",
    ] {
        assert!(
            !encoded.contains(implementation),
            "scenario contains implementation-specific name `{implementation}`"
        );
    }
}

#[test]
fn common_engine_scenario_has_one_strict_required_workflow() {
    let scenario = scenario();
    let capabilities = scenario
        .capabilities
        .iter()
        .map(|requirement| {
            assert_eq!(requirement.level, RequirementLevel::Required);
            requirement.capability.as_str()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        capabilities,
        BTreeSet::from([
            "engine.iceberg.additive-schema-evolution",
            "engine.iceberg.rest-round-trip",
        ])
    );
    assert!(scenario
        .assertions
        .iter()
        .all(|assertion| assertion.required));
    assert_eq!(scenario.assertions.len(), 14);

    let assertion_ids = scenario
        .assertions
        .iter()
        .map(|assertion| assertion.id.as_str())
        .collect::<BTreeSet<_>>();
    for required in [
        "engine-runtime-pinned",
        "stock-rest-catalog-ready",
        "fixture-isolated",
        "namespace-round-trip",
        "table-round-trip",
        "initial-append-committed",
        "initial-read-exact",
        "schema-evolved",
        "evolved-append-committed",
        "evolved-read-exact",
        "catalog-state-correlated",
        "shared-object-evidence-complete",
        "fixture-clean",
        "transcript-sanitized",
    ] {
        assert!(assertion_ids.contains(required), "missing `{required}`");
    }
}

#[test]
fn deterministic_row_oracle_matches_the_checked_in_hashes() {
    let scenario = scenario();
    let parameters = Value::Object(scenario.parameters.into_iter().collect());
    let reads = parameter(&parameters, &["canonical_reads"]);
    assert_eq!(
        text(&reads["encoding"], "canonical_reads.encoding"),
        "compact-rfc8259-json-array-per-row-utf8-lf"
    );
    assert_eq!(reads["order_by"], json!(["id"]));
    assert_eq!(reads["trailing_lf"], true);

    let initial = generated_rows(&parameters, false);
    let (initial_bytes, initial_digest) = canonical_identity(&initial);
    assert_eq!(
        initial.len() as u64,
        unsigned(&reads["initial"]["rows"], "initial.rows")
    );
    assert_eq!(
        initial_bytes as u64,
        unsigned(&reads["initial"]["bytes"], "initial.bytes")
    );
    assert_eq!(
        initial_digest,
        text(&reads["initial"]["sha256"], "initial.sha256")
    );

    let evolved = generated_rows(&parameters, true);
    let (evolved_bytes, evolved_digest) = canonical_identity(&evolved);
    assert_eq!(
        evolved.len() as u64,
        unsigned(&reads["after_evolution"]["rows"], "after_evolution.rows")
    );
    assert_eq!(
        evolved_bytes as u64,
        unsigned(&reads["after_evolution"]["bytes"], "after_evolution.bytes")
    );
    assert_eq!(
        evolved_digest,
        text(
            &reads["after_evolution"]["sha256"],
            "after_evolution.sha256"
        )
    );
}

#[test]
fn cleanup_and_sanitization_close_every_mutating_execution() {
    let scenario = scenario();
    let cleanup = scenario
        .steps
        .iter()
        .find(|step| step.id.as_str() == "cleanup-fixture")
        .unwrap();
    let sanitize = scenario
        .steps
        .iter()
        .find(|step| step.id.as_str() == "sanitize-transcript")
        .unwrap();

    assert_eq!(cleanup.operation, "iceberg-rest.reconcile-engine-fixture");
    assert!(cleanup.description.contains("without purge"));
    assert_eq!(sanitize.depends_on, vec![cleanup.id.clone()]);
    assert_eq!(scenario.steps.last().unwrap().id, sanitize.id);
}
