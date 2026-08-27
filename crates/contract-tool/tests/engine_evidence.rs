use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use catalog_bench_common::contract::ComponentId;
use catalog_bench_conformance::encode_evidence;
use catalog_bench_contract::validate_engine_evidence_set;
use catalog_bench_engine::{
    run_stock_spark_interoperability, EngineBehaviorClassification, EngineContracts,
    EngineTranscript, SecretRead, SecretSource,
};
use serde_json::Value;
use tempfile::TempDir;

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.json");
const FIXTURE_ID: &str = "evidence01";
const CATALOGS: [&str; 4] = ["lakecat", "polaris", "gravitino", "lakekeeper"];

#[tokio::test]
async fn admits_exact_profile_derived_catalog_set() {
    let fixture = EvidenceFixture::new().await;

    let evidence = fixture.validate().unwrap();

    assert_eq!(evidence.fixture_id(), FIXTURE_ID);
    assert_eq!(evidence.profile_bytes(), fixture.profile_bytes);
    assert_eq!(evidence.scenario_bytes(), SCENARIO);
    assert_eq!(evidence.contracts().profile().catalog_adapters.len(), 4);
    assert_eq!(
        evidence
            .transcripts()
            .iter()
            .map(|entry| entry.transcript().components.catalog.id.as_str())
            .collect::<Vec<_>>(),
        vec!["gravitino", "lakecat", "lakekeeper", "polaris"]
    );
    assert!(evidence
        .transcripts()
        .iter()
        .all(|entry| entry.path().is_file() && !entry.bytes().is_empty()));
    assert_eq!(
        evidence.summary(),
        catalog_bench_contract::EngineEvidenceSummary {
            total: 4,
            pass: 0,
            fail: 4,
            fixture_collision: 0,
        }
    );
}

#[tokio::test]
async fn cli_reports_only_validated_classification_counts() {
    let fixture = EvidenceFixture::new().await;

    let output = Command::new(env!("CARGO_BIN_EXE_catalog-bench-contract"))
        .args(["engine-evidence", "validate", "--profile"])
        .arg(&fixture.profile_path)
        .arg("--scenario")
        .arg(&fixture.scenario_path)
        .arg("--evidence-directory")
        .arg(&fixture.evidence_directory)
        .args(["--fixture-id", FIXTURE_ID])
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("4 transcript(s), 0 pass, 4 fail, 0 fixture collision"));
    for catalog in CATALOGS {
        assert!(!stdout.contains(catalog));
    }
}

#[tokio::test]
async fn rejects_missing_unexpected_and_nonregular_entries() {
    let fixture = EvidenceFixture::new().await;
    let lakecat = fixture.evidence_directory.join("lakecat.json");
    fs::remove_file(&lakecat).unwrap();
    assert_error_contains(fixture.validate(), "missing [lakecat.json]");

    fs::write(&lakecat, &fixture.transcripts["lakecat"]).unwrap();
    fs::write(fixture.evidence_directory.join("unexpected.json"), b"{}\n").unwrap();
    assert_error_contains(fixture.validate(), "unexpected [unexpected.json]");

    fs::remove_file(fixture.evidence_directory.join("unexpected.json")).unwrap();
    let gravitino = fixture.evidence_directory.join("gravitino.json");
    fs::remove_file(&gravitino).unwrap();
    fs::create_dir(&gravitino).unwrap();
    assert_error_contains(fixture.validate(), "is not a regular file");
}

#[tokio::test]
async fn rejects_catalog_fixture_contract_and_size_drift() {
    let fixture = EvidenceFixture::new().await;
    fs::write(
        fixture.evidence_directory.join("polaris.json"),
        &fixture.transcripts["lakecat"],
    )
    .unwrap();
    assert_error_contains(
        fixture.validate(),
        "contains catalog `lakecat`, expected `polaris`",
    );

    fs::write(
        fixture.evidence_directory.join("polaris.json"),
        &fixture.transcripts["polaris"],
    )
    .unwrap();
    let lakecat_path = fixture.evidence_directory.join("lakecat.json");
    let mut lakecat: Value = serde_json::from_slice(&fixture.transcripts["lakecat"]).unwrap();
    lakecat["fixture"]["id"] = Value::String("different_fixture".to_owned());
    fs::write(&lakecat_path, pretty_json(&lakecat)).unwrap();
    assert_error_contains(
        fixture.validate(),
        "contains fixture `different_fixture`, expected `evidence01`",
    );

    fs::write(&lakecat_path, &fixture.transcripts["lakecat"]).unwrap();
    let mut unsafe_transcript: EngineTranscript =
        serde_json::from_slice(&fixture.transcripts["lakecat"]).unwrap();
    unsafe_transcript.sanitization.transcript_sanitized = false;
    fs::write(&lakecat_path, encode_evidence(&unsafe_transcript).unwrap()).unwrap();
    assert_error_contains(fixture.validate(), "Sanitization");

    fs::write(&lakecat_path, &fixture.transcripts["lakecat"]).unwrap();
    let mut noncanonical = fixture.transcripts["lakecat"].clone();
    noncanonical.push(b'\n');
    fs::write(&lakecat_path, noncanonical).unwrap();
    assert_error_contains(
        fixture.validate(),
        "not in canonical newline-terminated encoding",
    );

    fs::write(&lakecat_path, &fixture.transcripts["lakecat"]).unwrap();
    let mut drifted_profile = fixture.profile_bytes.clone();
    drifted_profile.push(b'\n');
    fs::write(&fixture.profile_path, drifted_profile).unwrap();
    assert_error_contains(fixture.validate(), "ContractDigests");

    fs::write(&fixture.profile_path, &fixture.profile_bytes).unwrap();
    fs::write(
        fixture.evidence_directory.join("lakekeeper.json"),
        vec![b'x'; 4 * 1024 * 1024 + 1],
    )
    .unwrap();
    assert_error_contains(fixture.validate(), "expected 1 to 4194304");
}

struct EvidenceFixture {
    _directory: TempDir,
    profile_path: PathBuf,
    scenario_path: PathBuf,
    evidence_directory: PathBuf,
    profile_bytes: Vec<u8>,
    transcripts: BTreeMap<String, Vec<u8>>,
}

impl EvidenceFixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let profile_path = directory.path().join("profile.json");
        let scenario_path = directory.path().join("scenario.json");
        let evidence_directory = directory.path().join("evidence");
        let profile_bytes = profile_with_impossible_spark_submit_digest();
        fs::write(&profile_path, &profile_bytes).unwrap();
        fs::write(&scenario_path, SCENARIO).unwrap();
        fs::create_dir(&evidence_directory).unwrap();

        let contracts = EngineContracts::parse(&profile_bytes, SCENARIO).unwrap();
        let mut transcripts = BTreeMap::new();
        for catalog in CATALOGS {
            let transcript = run_stock_spark_interoperability(
                &contracts,
                &ComponentId::from(catalog),
                FIXTURE_ID,
                Arc::new(NoSecrets),
            )
            .await
            .unwrap();
            assert_eq!(
                transcript.execution.classification,
                EngineBehaviorClassification::Fail
            );
            let bytes = pretty_json(&transcript);
            fs::write(evidence_directory.join(format!("{catalog}.json")), &bytes).unwrap();
            transcripts.insert(catalog.to_owned(), bytes);
        }

        Self {
            _directory: directory,
            profile_path,
            scenario_path,
            evidence_directory,
            profile_bytes,
            transcripts,
        }
    }

    fn validate(&self) -> anyhow::Result<catalog_bench_contract::ValidatedEngineEvidenceSet> {
        validate_engine_evidence_set(
            &self.profile_path,
            &self.scenario_path,
            &self.evidence_directory,
            FIXTURE_ID,
        )
    }
}

struct NoSecrets;

impl SecretSource for NoSecrets {
    fn read_secret(&self, _name: &str) -> SecretRead {
        SecretRead::Missing
    }
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
    pretty_json(&profile)
}

fn pretty_json(value: &impl serde::Serialize) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(value).unwrap();
    bytes.push(b'\n');
    bytes
}

fn assert_error_contains<T>(result: anyhow::Result<T>, expected: &str) {
    let error = result.err().expect("validation must fail");
    let message = format!("{error:#}");
    assert!(
        message.contains(expected),
        "error `{message}` does not contain `{expected}`"
    );
}
