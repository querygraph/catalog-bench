#![allow(
    dead_code,
    reason = "each engine integration-test binary uses a different fixture subset"
)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use catalog_bench_common::contract::ComponentId;
use catalog_bench_contract::{validate_engine_evidence_set, ValidatedEngineEvidenceSet};
use catalog_bench_engine::{
    run_stock_spark_interoperability, EngineBehaviorClassification, EngineContracts, SecretRead,
    SecretSource,
};
use serde_json::Value;
use tempfile::TempDir;

const PROFILE: &[u8] =
    include_bytes!("../../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
pub const SCENARIO: &[u8] =
    include_bytes!("../../../../scenarios/v1/engine.iceberg.write-read-evolution.json");
pub const FIXTURE_ID: &str = "evidence01";
pub const CATALOGS: [&str; 4] = ["lakecat", "polaris", "gravitino", "lakekeeper"];

pub struct EvidenceFixture {
    directory: TempDir,
    pub profile_path: PathBuf,
    pub scenario_path: PathBuf,
    pub evidence_directory: PathBuf,
    pub profile_bytes: Vec<u8>,
    pub transcripts: BTreeMap<String, Vec<u8>>,
}

impl EvidenceFixture {
    pub async fn new() -> Self {
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
            directory,
            profile_path,
            scenario_path,
            evidence_directory,
            profile_bytes,
            transcripts,
        }
    }

    pub fn root(&self) -> &Path {
        self.directory.path()
    }

    pub fn validate(&self) -> anyhow::Result<ValidatedEngineEvidenceSet> {
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

pub fn pretty_json(value: &impl serde::Serialize) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(value).unwrap();
    bytes.push(b'\n');
    bytes
}

pub fn assert_error_contains<T>(result: anyhow::Result<T>, expected: &str) {
    let error = result.err().expect("validation must fail");
    let message = format!("{error:#}");
    assert!(
        message.contains(expected),
        "error `{message}` does not contain `{expected}`"
    );
}
