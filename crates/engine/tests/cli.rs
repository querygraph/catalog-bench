use std::fs;
use std::path::Path;
use std::process::Command;

use catalog_bench_engine::{
    EngineBehaviorClassification, EngineContracts, EngineProcessOutcome, EngineTranscript,
};
use serde_json::Value;

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.json");
const SECRET_SENTINEL: &str = "engine-cli-secret-sentinel";

#[test]
fn cli_exposes_only_contract_driven_inputs() {
    let output = Command::new(env!("CARGO_BIN_EXE_catalog-bench-engine"))
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for required in [
        "--profile",
        "--scenario",
        "--catalog",
        "--fixture-id",
        "--output",
    ] {
        assert!(help.contains(required), "help omits {required}");
    }
    for forbidden in [
        "--base-url",
        "--warehouse",
        "--prefix",
        "--token",
        "--master",
        "--timeout",
        "--shuffle-partitions",
    ] {
        assert!(!help.contains(forbidden), "help exposes {forbidden}");
    }
}

#[test]
fn fail_transcript_is_published_before_nonzero_exit_without_clobbering() {
    let root = tempfile::tempdir().unwrap();
    let profile = profile_with_impossible_spark_submit_digest();
    let profile_path = root.path().join("profile.json");
    let scenario_path = root.path().join("scenario.json");
    let output_path = root.path().join("evidence/transcript.json");
    fs::write(&profile_path, &profile).unwrap();
    fs::write(&scenario_path, SCENARIO).unwrap();

    let first = run_engine(&profile_path, &scenario_path, &output_path);

    assert_eq!(first.status.code(), Some(2));
    assert_secret_absent(&first.stdout);
    assert_secret_absent(&first.stderr);
    assert!(String::from_utf8_lossy(&first.stdout).contains("classification=fail"));
    let evidence = fs::read(&output_path).unwrap();
    assert_eq!(evidence.last(), Some(&b'\n'));
    assert!(!evidence
        .windows(SECRET_SENTINEL.len())
        .any(|window| window == SECRET_SENTINEL.as_bytes()));
    assert!(!evidence.windows(7).any(|window| window == b"Bearer "));

    let contracts = EngineContracts::parse(&profile, SCENARIO).unwrap();
    let transcript: EngineTranscript = serde_json::from_slice(&evidence).unwrap();
    assert!(transcript.validate(&contracts).is_ok());
    assert_eq!(
        transcript.execution.classification,
        EngineBehaviorClassification::Fail
    );
    assert!(matches!(
        transcript.execution.process.outcome,
        EngineProcessOutcome::RuntimeRejected {}
    ));

    let second = run_engine(&profile_path, &scenario_path, &output_path);
    assert_eq!(second.status.code(), Some(1));
    assert_secret_absent(&second.stdout);
    assert_secret_absent(&second.stderr);
    assert_eq!(fs::read(&output_path).unwrap(), evidence);
}

#[test]
fn invalid_contract_creates_no_evidence() {
    let root = tempfile::tempdir().unwrap();
    let profile_path = root.path().join("profile.json");
    let scenario_path = root.path().join("scenario.json");
    let output_path = root.path().join("transcript.json");
    fs::write(&profile_path, b"not-json").unwrap();
    fs::write(&scenario_path, SCENARIO).unwrap();

    let execution = run_engine(&profile_path, &scenario_path, &output_path);

    assert_eq!(execution.status.code(), Some(1));
    assert!(!output_path.exists());
}

fn run_engine(profile: &Path, scenario: &Path, output: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_catalog-bench-engine"))
        .args([
            "--profile",
            profile.to_str().unwrap(),
            "--scenario",
            scenario.to_str().unwrap(),
            "--catalog",
            "lakecat",
            "--fixture-id",
            "cli01",
            "--output",
            output.to_str().unwrap(),
        ])
        .env("CATALOG_BENCH_S3_ACCESS_KEY_ID", SECRET_SENTINEL)
        .env("CATALOG_BENCH_S3_SECRET_ACCESS_KEY", SECRET_SENTINEL)
        .env("CATALOG_BENCH_POLARIS_CLIENT_ID", SECRET_SENTINEL)
        .env("CATALOG_BENCH_POLARIS_CLIENT_SECRET", SECRET_SENTINEL)
        .output()
        .unwrap()
}

fn assert_secret_absent(bytes: &[u8]) {
    assert!(!bytes
        .windows(SECRET_SENTINEL.len())
        .any(|window| window == SECRET_SENTINEL.as_bytes()));
}

fn profile_with_impossible_spark_submit_digest() -> Vec<u8> {
    let mut profile: Value = serde_json::from_slice(PROFILE).unwrap();
    let spark = profile["components"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|component| component["id"] == "spark-4.1")
        .unwrap();
    let spark_submit = spark["artifact"]["embedded_artifacts"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|artifact| artifact["location"] == "image:/opt/spark/bin/spark-submit")
        .unwrap();
    spark_submit["digest"]["value"] = Value::String("0".repeat(64));
    let mut bytes = serde_json::to_vec_pretty(&profile).unwrap();
    bytes.push(b'\n');
    bytes
}
