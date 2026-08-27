use std::fs;
use std::process::{Command, Stdio};

use catalog_bench_common::contract::{
    parse_contract, ComponentId, ContractDocument, Profile, Scenario,
};
use catalog_bench_engine::InteroperabilityPlan;
use serde_json::json;
use tempfile::tempdir;

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.json");
const RENDERER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/spark/runner.py");

#[test]
fn stock_renderer_validates_every_profile_derived_plan_without_spark() {
    let (profile, scenario) = contracts();
    for catalog in ["lakecat", "polaris", "gravitino", "lakekeeper"] {
        let plan = InteroperabilityPlan::from_contracts(
            &profile,
            &scenario,
            &ComponentId::from(catalog),
            "render01",
        )
        .unwrap();
        let output = validate(&serde_json::to_value(plan.spark()).unwrap());
        assert!(
            output.status.success(),
            "renderer rejected profile-derived plan for {catalog}"
        );
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(receipt["initial"]["rows"], 16);
        assert_eq!(receipt["initial"]["bytes"], 346);
        assert_eq!(
            receipt["initial"]["sha256"],
            "e78b526d7e757090a9a90c80802c2a543cbf8166cfac6d6ed48c618926e85a15"
        );
        assert_eq!(receipt["after_evolution"]["rows"], 20);
        assert_eq!(receipt["after_evolution"]["bytes"], 570);
        assert_eq!(
            receipt["after_evolution"]["sha256"],
            "b2af6f475851e07d1ace3706d8867530c13dd5938bee90cfcc62d3939e01bea2"
        );
        assert_eq!(receipt["observation"]["format_version"], 2);
        assert_eq!(receipt["observation"]["last_column_id"], 3);
        assert!(receipt["observation"]["properties"]
            .as_object()
            .unwrap()
            .values()
            .all(|outcome| outcome == "match"));
        assert_eq!(
            receipt["property_mismatch"]
                .as_object()
                .unwrap()
                .values()
                .filter(|outcome| *outcome == "mismatch")
                .count(),
            1
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("validation-only-mismatch"));
    }
}

#[test]
fn stock_renderer_rejects_policy_resource_and_oracle_drift() {
    let (profile, scenario) = contracts();
    let plan = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "reject01",
    )
    .unwrap();
    let plan = serde_json::to_value(plan.spark()).unwrap();
    let mutations = [
        ("execution", json!("local[*]")),
        ("policy", json!("allowed")),
        ("object-audit", json!(0)),
        ("read-columns", json!(["category", "id", "amount_cents"])),
        ("fixture-prefix", json!("outside")),
        ("bucket", json!("")),
        ("row-limit", json!(100_001)),
    ];

    for (name, replacement) in mutations {
        let mut drifted = plan.clone();
        match name {
            "execution" => drifted["execution"]["master"] = replacement,
            "policy" => {
                drifted["scenario"]["engine_policy"]["catalog_specific_shims"] = replacement
            }
            "object-audit" => {
                drifted["scenario"]["object_audit"]["minimum_metadata_objects"] = replacement
            }
            "read-columns" => {
                drifted["scenario"]["canonical_reads"]["initial"]["columns"] = replacement
            }
            "fixture-prefix" => drifted["scenario"]["fixture_prefix"] = replacement,
            "bucket" => drifted["file_io"]["bucket"] = replacement,
            "row-limit" => {
                drifted["scenario"]["batches"]["initial"]["rows"] = replacement.clone();
                drifted["scenario"]["batches"]["evolved"]["id_start"] = replacement;
            }
            _ => unreachable!(),
        }
        let output = validate(&drifted);
        assert!(!output.status.success(), "renderer accepted {name} drift");
        assert!(output.stdout.is_empty(), "renderer disclosed {name} drift");
    }
}

#[test]
fn renderer_projects_property_agreement_and_rejects_unsafe_table_values() {
    let (profile, scenario) = contracts();
    let plan = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "observe01",
    )
    .unwrap();
    let plan = serde_json::to_value(plan.spark()).unwrap();
    let location = plan["fixture"]["requested_location"]
        .as_str()
        .unwrap_or("s3://warehouse/table");
    let metadata_location = format!(
        "{}/metadata/v1.metadata.json",
        location.trim_end_matches('/')
    );
    let mut observation = json!({
        "table_uuid": "00000000-0000-0000-0000-000000000001",
        "metadata_location": metadata_location,
        "location": location,
        "format_version": 2,
        "last_column_id": 3,
        "schema": [
            {"id": 1, "name": "id", "required": true, "field_type": "long"},
            {"id": 2, "name": "category", "required": false, "field_type": "string"},
            {"id": 3, "name": "amount_cents", "required": true, "field_type": "long"}
        ],
        "snapshots": 0,
        "properties": plan["scenario"]["table"]["properties"].clone()
    });
    let private = "untrusted-property-value";
    observation["properties"]["catalog-bench.owner"] = json!(private);
    let output = sanitize_observation(&plan, &observation);
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains(private));
    let sanitized: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(sanitized["properties"]["catalog-bench.owner"], "mismatch");

    for (name, path, value) in [
        (
            "metadata escape",
            &["metadata_location"][..],
            json!("s3://warehouse/sibling/metadata/v1.metadata.json"),
        ),
        (
            "field vocabulary",
            &["schema", "0", "name"][..],
            json!("private_field"),
        ),
        (
            "credentialed root",
            &["location"][..],
            json!("s3://user:secret@warehouse/table"),
        ),
    ] {
        let mut unsafe_observation = observation.clone();
        set_json_path(&mut unsafe_observation, path, value);
        let output = sanitize_observation(&plan, &unsafe_observation);
        assert!(!output.status.success(), "renderer accepted {name}");
        assert!(output.stdout.is_empty(), "renderer disclosed {name}");
    }
}

#[test]
fn renderer_has_no_catalog_branch_http_substitute_or_raw_exception_path() {
    let source = fs::read_to_string(RENDERER).unwrap();
    for catalog in ["lakecat", "polaris", "gravitino", "lakekeeper", "nessie"] {
        assert!(
            !source.to_ascii_lowercase().contains(catalog),
            "renderer names catalog `{catalog}`"
        );
    }
    for forbidden in [
        "requests.",
        "urllib.request",
        "http.client",
        "/v1/config",
        "/namespaces/",
        "traceback.print",
        "print_exc",
    ] {
        assert!(
            !source.contains(forbidden),
            "renderer contains `{forbidden}`"
        );
    }
    assert!(source.contains("SparkCatalog"));
    assert!(source.contains("file_io[\"implementation\"]"));
    assert!(source.contains("writeTo(full_table_name(plan)).append()"));
    assert!(source.contains("metadata_table_name(plan, \"snapshots\")"));
    assert!(source.contains("sanitize_table_observation"));
    assert!(source.contains("require_s3_location"));
    assert!(source.contains("CATALOG_BENCH_EVENT"));
}

fn contracts() -> (Profile, Scenario) {
    let ContractDocument::Profile(profile) = parse_contract(PROFILE).unwrap() else {
        panic!("profile fixture must be a profile");
    };
    let ContractDocument::Scenario(scenario) = parse_contract(SCENARIO).unwrap() else {
        panic!("scenario fixture must be a scenario");
    };
    (profile, scenario)
}

fn validate(plan: &serde_json::Value) -> std::process::Output {
    let directory = tempdir().unwrap();
    let path = directory.path().join("plan.json");
    fs::write(&path, serde_json::to_vec(plan).unwrap()).unwrap();
    Command::new("python3")
        .arg(RENDERER)
        .arg("--plan")
        .arg(path)
        .arg("--validate-plan")
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .unwrap()
}

fn sanitize_observation(
    plan: &serde_json::Value,
    observation: &serde_json::Value,
) -> std::process::Output {
    const SANITIZE: &str = r#"
import importlib.util
import json
import sys
from pathlib import Path

spec = importlib.util.spec_from_file_location("catalog_bench_spark_runner", sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
plan = module.load_plan(Path(sys.argv[2]))
with open(sys.argv[3], "rb") as stream:
    observation = json.load(stream)
sanitized = module.sanitize_table_observation(observation, plan)
print(json.dumps(sanitized, separators=(",", ":"), sort_keys=True))
"#;
    let directory = tempdir().unwrap();
    let plan_path = directory.path().join("plan.json");
    let observation_path = directory.path().join("observation.json");
    fs::write(&plan_path, serde_json::to_vec(plan).unwrap()).unwrap();
    fs::write(&observation_path, serde_json::to_vec(observation).unwrap()).unwrap();
    Command::new("python3")
        .arg("-c")
        .arg(SANITIZE)
        .arg(RENDERER)
        .arg(plan_path)
        .arg(observation_path)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .unwrap()
}

fn set_json_path(value: &mut serde_json::Value, path: &[&str], replacement: serde_json::Value) {
    let (last, parents) = path.split_last().unwrap();
    let mut target = value;
    for segment in parents {
        target = if let Ok(index) = segment.parse::<usize>() {
            &mut target[index]
        } else {
            &mut target[*segment]
        };
    }
    target[*last] = replacement;
}
