#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::Mutex;
use std::time::Duration;

use catalog_bench_common::contract::{
    parse_contract, ComponentId, ContractDocument, Profile, RuntimeArtifact, Scenario,
};
use catalog_bench_engine::{
    CatalogCredentialSource, EngineProcessOutcome, FlinkProcessExecutor, RuntimeVerifier,
    SecretRead, SecretSource, ENGINE_EVENT_PREFIX, FLINK_CLI_LOCATION,
};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tempfile::{tempdir, TempDir};

mod support;

use support::select_synthetic_materialized_flink;

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const CANDIDATE_PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.v2.json");
const ACCESS_VALUE: &str = "flink-process-access";
const SECRET_VALUE: &str = "flink-process-secret";
const CLIENT_ID_VALUE: &str = "flink-process-client";
const CLIENT_SECRET_VALUE: &str = "flink-process-client-secret";

#[tokio::test]
async fn verified_flink_collision_stages_only_the_closed_program_and_child_secrets() {
    let runtime = TestRuntime::new("polaris", &collision_script());
    let source = source_for(&runtime.plan);
    let execution = FlinkProcessExecutor::try_new(Duration::from_secs(2))
        .unwrap()
        .execute_with_source(&runtime.plan, &runtime.verifier, &source)
        .await;

    assert!(matches!(
        execution.outcome,
        EngineProcessOutcome::FixtureCollision {}
    ));
    assert!(execution.fixture_collision());
    assert_eq!(execution.exit_code, Some(3));
    assert_eq!(source.reads().len(), 4);
    let encoded = serde_json::to_string(&execution).unwrap();
    for secret in [
        ACCESS_VALUE,
        SECRET_VALUE,
        CLIENT_ID_VALUE,
        CLIENT_SECRET_VALUE,
    ] {
        assert!(!encoded.contains(secret));
    }
}

#[tokio::test]
async fn flink_runtime_rejection_precedes_staging_credentials_and_spawn() {
    let runtime = TestRuntime::new("lakecat", &collision_script());
    fs::write(
        runtime
            .root
            .path()
            .join("opt/flink/lib/iceberg-aws-bundle-1.11.0.jar"),
        b"drift",
    )
    .unwrap();
    let source = RecordingSource::default();
    let execution = FlinkProcessExecutor::default()
        .execute_with_source(&runtime.plan, &runtime.verifier, &source)
        .await;

    assert!(matches!(
        execution.outcome,
        EngineProcessOutcome::RuntimeRejected {}
    ));
    assert!(execution.capture.is_none());
    assert!(source.reads().is_empty());
}

#[test]
fn zero_flink_timeout_is_rejected() {
    assert!(FlinkProcessExecutor::try_new(Duration::ZERO).is_err());
}

#[derive(Default)]
struct RecordingSource {
    values: BTreeMap<String, String>,
    reads: Mutex<Vec<String>>,
}

impl SecretSource for RecordingSource {
    fn read_secret(&self, name: &str) -> SecretRead {
        self.reads.lock().unwrap().push(name.to_owned());
        self.values
            .get(name)
            .cloned()
            .map(SecretRead::Value)
            .unwrap_or(SecretRead::Missing)
    }
}

impl RecordingSource {
    fn reads(&self) -> Vec<String> {
        self.reads.lock().unwrap().clone()
    }
}

struct TestRuntime {
    root: TempDir,
    plan: catalog_bench_engine::InteroperabilityPlan,
    verifier: RuntimeVerifier,
}

impl TestRuntime {
    fn new(catalog: &str, cli_script: &[u8]) -> Self {
        let (mut profile, scenario) = contracts();
        rewrite_runtime_artifacts(&mut profile, cli_script);
        let plan = catalog_bench_engine::InteroperabilityPlan::from_contracts(
            &profile,
            &scenario,
            &ComponentId::from(catalog),
            "flinkprocess01",
        )
        .unwrap();
        let root = tempdir().unwrap();
        for artifact in plan.runtime_artifacts() {
            let path = root.path().join(artifact.location.trim_start_matches('/'));
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, artifact_bytes(&artifact.location, cli_script)).unwrap();
            if artifact.location == FLINK_CLI_LOCATION {
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
            }
        }
        let verifier = RuntimeVerifier::for_observation(root.path(), "linux", "arm64");
        Self {
            root,
            plan,
            verifier,
        }
    }
}

fn contracts() -> (Profile, Scenario) {
    let ContractDocument::Profile(mut profile) = parse_contract(PROFILE).unwrap() else {
        panic!("profile fixture must be a profile");
    };
    let ContractDocument::Profile(candidate) = parse_contract(CANDIDATE_PROFILE).unwrap() else {
        panic!("candidate fixture must be a profile");
    };
    let ContractDocument::Scenario(scenario) = parse_contract(SCENARIO).unwrap() else {
        panic!("scenario fixture must be a scenario");
    };
    select_synthetic_materialized_flink(&mut profile, &candidate);
    (profile, scenario)
}

fn source_for(plan: &catalog_bench_engine::InteroperabilityPlan) -> RecordingSource {
    let mut values = BTreeMap::from([
        (
            plan.object_store().access_key_env.clone(),
            ACCESS_VALUE.to_owned(),
        ),
        (
            plan.object_store().secret_key_env.clone(),
            SECRET_VALUE.to_owned(),
        ),
    ]);
    if let CatalogCredentialSource::OAuth2ClientCredentials {
        client_id_env,
        client_secret_env,
    } = plan.credential_source()
    {
        values.insert(client_id_env.clone(), CLIENT_ID_VALUE.to_owned());
        values.insert(client_secret_env.clone(), CLIENT_SECRET_VALUE.to_owned());
    }
    RecordingSource {
        values,
        reads: Mutex::default(),
    }
}

fn rewrite_runtime_artifacts(profile: &mut Profile, cli_script: &[u8]) {
    for component in profile.components.iter_mut().filter(|component| {
        matches!(
            component.id.as_str(),
            "catalog-bench-engine" | "flink" | "iceberg-java"
        )
    }) {
        let RuntimeArtifact::ContainerImage {
            embedded_artifacts, ..
        } = &mut component.artifact
        else {
            continue;
        };
        for artifact in embedded_artifacts {
            let bytes = artifact_bytes(&artifact.location, cli_script);
            artifact.bytes = Some(bytes.len() as u64);
            artifact.digest.value = sha256(bytes);
        }
    }
}

fn artifact_bytes<'a>(location: &str, cli_script: &'a [u8]) -> &'a [u8] {
    if location.ends_with("/flink") {
        cli_script
    } else if location.ends_with("catalog-bench-flink-runner.jar") {
        b"test source-bound Flink child JAR"
    } else if location.ends_with("catalog-bench-engine") {
        b"test catalog-bench ELF"
    } else if location.contains("iceberg-flink-runtime") {
        b"test Iceberg Flink runtime"
    } else if location.contains("iceberg-aws-bundle") {
        b"test Iceberg AWS bundle"
    } else if location.contains("hadoop-client-api") {
        b"test Hadoop client API"
    } else if location.contains("hadoop-client-runtime") {
        b"test Hadoop client runtime"
    } else {
        panic!("unexpected Flink runtime artifact `{location}`");
    }
}

fn collision_script() -> Vec<u8> {
    let mut script = String::from(
        "#!/bin/sh\n\
         [ \"$#\" -eq 8 ] || exit 80\n\
         [ \"$1\" = \"run\" ] || exit 81\n\
         [ \"$2\" = \"--target\" ] || exit 82\n\
         [ \"$3\" = \"local\" ] || exit 83\n\
         [ \"$4\" = \"--class\" ] || exit 84\n\
         [ \"$5\" = \"org.querygraph.catalogbench.flink.Runner\" ] || exit 85\n\
         [ \"$7\" = \"--program\" ] || exit 86\n\
         [ -f \"$6\" ] || exit 87\n\
         [ -f \"$8\" ] || exit 88\n\
         grep -q '\"operation\":\"add-column\"' \"$8\" || exit 89\n\
         grep -q '\"expected\":{\"bytes\":346' \"$8\" || exit 90\n\
         ! grep -q 'flink-process-secret' \"$8\" || exit 91\n\
         [ \"$AWS_ACCESS_KEY_ID\" = \"flink-process-access\" ] || exit 90\n\
         [ \"$AWS_SECRET_ACCESS_KEY\" = \"flink-process-secret\" ] || exit 91\n\
         [ \"$CATALOG_BENCH_ENGINE_CLIENT_ID\" = \"flink-process-client\" ] || exit 92\n\
         [ \"$CATALOG_BENCH_ENGINE_CLIENT_SECRET\" = \"flink-process-client-secret\" ] || exit 93\n",
    );
    append_event(&mut script, runtime_ready());
    append_event(&mut script, json!({"event": "catalog-ready"}));
    append_event(
        &mut script,
        json!({"event": "fixture-preflight", "absent": false}),
    );
    script.push_str("exit 3\n");
    script.into_bytes()
}

fn runtime_ready() -> serde_json::Value {
    json!({
        "event": "runtime-ready",
        "runtime": {
            "engine_version": "2.1.3",
            "dependencies": {"java": "17.0.20", "scala": "2.12.20"},
            "operating_system": "Linux",
            "architecture": "aarch64"
        }
    })
}

fn append_event(script: &mut String, event: serde_json::Value) {
    let payload = serde_json::to_string(&event).unwrap();
    script.push_str("printf '%s\\n' '");
    script.push_str(std::str::from_utf8(ENGINE_EVENT_PREFIX).unwrap());
    script.push_str(&payload);
    script.push_str("'\n");
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
