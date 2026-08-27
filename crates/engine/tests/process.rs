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
    CatalogCredentialSource, EngineFailureCategory, EngineProtocolFailureKind, EngineStage,
    RuntimeVerifier, SecretRead, SecretSource, SparkCredentialFailure, SparkCredentialFailureKind,
    SparkCredentialKind, SparkProcessExecutor, SparkProcessOutcome, ENGINE_EVENT_PREFIX,
    SPARK_SUBMIT_LOCATION,
};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tempfile::{tempdir, TempDir};

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.json");
const ACCESS_VALUE: &str = "process-access-value";
const SECRET_VALUE: &str = "process-secret-value";
const CLIENT_ID_VALUE: &str = "process-client-id";
const CLIENT_SECRET_VALUE: &str = "process-client-secret";

#[tokio::test]
async fn verified_collision_maps_secrets_only_to_child_environment() {
    let runtime = TestRuntime::new("polaris", &collision_script(), true);
    let source = source_for(&runtime.plan);
    let execution = SparkProcessExecutor::try_new(Duration::from_secs(2))
        .unwrap()
        .execute_with_source(&runtime.plan, &runtime.verifier, &source)
        .await;

    assert!(matches!(
        execution.outcome,
        SparkProcessOutcome::FixtureCollision
    ));
    assert_eq!(execution.exit_code, Some(3));
    assert!(execution.fixture_collision());
    assert!(!execution.cleanup_authorized());
    assert_eq!(source.reads().len(), 4);
    let serialized = serde_json::to_string(&execution).unwrap();
    for secret in [
        ACCESS_VALUE,
        SECRET_VALUE,
        CLIENT_ID_VALUE,
        CLIENT_SECRET_VALUE,
    ] {
        assert!(!serialized.contains(secret));
    }
    assert_eq!(
        format!("{:?}", SecretRead::Value(SECRET_VALUE.to_owned())),
        "Value(<redacted>)"
    );
}

#[tokio::test]
async fn complete_event_stream_and_engine_failure_map_to_closed_outcomes() {
    let runtime = TestRuntime::new("lakecat", &complete_script(), true);
    let source = source_for(&runtime.plan);
    let completed = SparkProcessExecutor::try_new(Duration::from_secs(2))
        .unwrap()
        .execute_with_source(&runtime.plan, &runtime.verifier, &source)
        .await;
    assert!(matches!(completed.outcome, SparkProcessOutcome::Completed));
    assert!(completed.passed());
    assert!(completed.cleanup_authorized());
    assert_eq!(completed.exit_code, Some(0));

    let runtime = TestRuntime::new("lakecat", &engine_failure_script(), true);
    let source = source_for(&runtime.plan);
    let failed = SparkProcessExecutor::try_new(Duration::from_secs(2))
        .unwrap()
        .execute_with_source(&runtime.plan, &runtime.verifier, &source)
        .await;
    assert!(matches!(
        failed.outcome,
        SparkProcessOutcome::EngineFailed {
            stage: EngineStage::CreateNamespace,
            category: EngineFailureCategory::Catalog,
        }
    ));
    assert!(!failed.passed());
    assert!(failed.cleanup_authorized());
    assert_eq!(failed.exit_code, Some(2));
}

#[tokio::test]
async fn runtime_rejection_happens_before_any_secret_read_or_process_start() {
    let runtime = TestRuntime::new("lakecat", &collision_script(), true);
    let spark_sql = runtime
        .root
        .path()
        .join("opt/spark/jars/spark-sql_2.13-4.1.3.jar");
    fs::write(spark_sql, b"drifted after plan construction").unwrap();
    let source = RecordingSource::default();

    let execution = SparkProcessExecutor::default()
        .execute_with_source(&runtime.plan, &runtime.verifier, &source)
        .await;

    assert!(matches!(
        execution.outcome,
        SparkProcessOutcome::RuntimeRejected
    ));
    assert!(execution.capture.is_none());
    assert!(execution.process_elapsed_micros.is_none());
    assert!(source.reads().is_empty());
}

#[tokio::test]
async fn malformed_stream_after_owned_preflight_preserves_cleanup_authority() {
    let runtime = TestRuntime::new("lakecat", &malformed_script(), true);
    let source = source_for(&runtime.plan);
    let execution = SparkProcessExecutor::try_new(Duration::from_secs(2))
        .unwrap()
        .execute_with_source(&runtime.plan, &runtime.verifier, &source)
        .await;

    assert!(matches!(
        execution.outcome,
        SparkProcessOutcome::ProtocolRejected {
            kind: EngineProtocolFailureKind::MalformedEvent
        }
    ));
    assert!(execution.cleanup_authorized());
    assert_eq!(execution.exit_code, None);
}

#[tokio::test]
async fn timeout_kills_the_child_but_retains_prior_fixture_ownership() {
    let runtime = TestRuntime::new("lakecat", &timeout_script(), true);
    let source = source_for(&runtime.plan);
    let execution = SparkProcessExecutor::try_new(Duration::from_secs(1))
        .unwrap()
        .execute_with_source(&runtime.plan, &runtime.verifier, &source)
        .await;

    assert!(matches!(execution.outcome, SparkProcessOutcome::TimedOut));
    assert!(execution.cleanup_authorized(), "{execution:#?}");
    assert_eq!(execution.exit_code, None);
    assert!(matches!(
        execution.capture.unwrap().failure.unwrap().kind,
        EngineProtocolFailureKind::MissingTerminal
    ));
}

#[tokio::test]
async fn timeout_terminates_descendants_in_the_isolated_process_group() {
    let marker_directory = tempdir().unwrap();
    let marker = marker_directory.path().join("survived");
    let runtime = TestRuntime::new("lakecat", &descendant_script(&marker), true);
    let source = source_for(&runtime.plan);
    let execution = SparkProcessExecutor::try_new(Duration::from_secs(1))
        .unwrap()
        .execute_with_source(&runtime.plan, &runtime.verifier, &source)
        .await;

    assert!(matches!(execution.outcome, SparkProcessOutcome::TimedOut));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!marker.exists(), "a timed-out descendant survived");
}

#[tokio::test]
async fn missing_credentials_and_nonexecutable_runtime_have_fixed_categories() {
    let runtime = TestRuntime::new("lakecat", &collision_script(), true);
    let missing = RecordingSource::default();
    let execution = SparkProcessExecutor::default()
        .execute_with_source(&runtime.plan, &runtime.verifier, &missing)
        .await;
    assert_eq!(
        execution.outcome,
        SparkProcessOutcome::CredentialRejected {
            failure: SparkCredentialFailure {
                credential: SparkCredentialKind::ObjectStoreAccessKey,
                kind: SparkCredentialFailureKind::Missing,
            }
        }
    );

    let runtime = TestRuntime::new("lakecat", &collision_script(), false);
    let source = source_for(&runtime.plan);
    let execution = SparkProcessExecutor::default()
        .execute_with_source(&runtime.plan, &runtime.verifier, &source)
        .await;
    assert!(matches!(
        execution.outcome,
        SparkProcessOutcome::SpawnFailed
    ));
    assert!(execution.capture.is_none());
}

#[test]
fn zero_timeout_is_rejected() {
    assert!(SparkProcessExecutor::try_new(Duration::ZERO).is_err());
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
    fn new(catalog: &str, submit_script: &[u8], executable: bool) -> Self {
        let (mut profile, scenario) = contracts();
        rewrite_runtime_artifacts(&mut profile, submit_script);
        let plan = catalog_bench_engine::InteroperabilityPlan::from_contracts(
            &profile,
            &scenario,
            &ComponentId::from(catalog),
            "process01",
        )
        .unwrap();
        let root = tempdir().unwrap();
        for artifact in plan.runtime_artifacts() {
            let path = root.path().join(artifact.location.trim_start_matches('/'));
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            let bytes = artifact_bytes(&artifact.location, submit_script);
            fs::write(&path, bytes).unwrap();
            if artifact.location == SPARK_SUBMIT_LOCATION {
                let mode = if executable { 0o700 } else { 0o600 };
                fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
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

fn contracts() -> (Profile, Scenario) {
    let ContractDocument::Profile(profile) = parse_contract(PROFILE).unwrap() else {
        panic!("profile fixture must be a profile");
    };
    let ContractDocument::Scenario(scenario) = parse_contract(SCENARIO).unwrap() else {
        panic!("scenario fixture must be a scenario");
    };
    (profile, scenario)
}

fn rewrite_runtime_artifacts(profile: &mut Profile, submit_script: &[u8]) {
    for component in profile
        .components
        .iter_mut()
        .filter(|component| matches!(component.id.as_str(), "spark-4.1" | "iceberg-java"))
    {
        let RuntimeArtifact::ContainerImage {
            embedded_artifacts, ..
        } = &mut component.artifact
        else {
            panic!("runtime fixture must use container images");
        };
        for artifact in embedded_artifacts {
            let bytes = artifact_bytes(&artifact.location, submit_script);
            artifact.bytes = Some(bytes.len() as u64);
            artifact.digest.value = sha256(bytes);
        }
    }
}

fn artifact_bytes<'a>(location: &str, submit_script: &'a [u8]) -> &'a [u8] {
    if location.ends_with("spark-submit") {
        submit_script
    } else if location.contains("spark-sql_") {
        b"test Spark SQL artifact"
    } else if location.contains("iceberg-spark-runtime") {
        b"test Iceberg Spark runtime artifact"
    } else if location.contains("iceberg-aws-bundle") {
        b"test Iceberg AWS bundle artifact"
    } else {
        panic!("unexpected runtime artifact `{location}`");
    }
}

fn collision_script() -> Vec<u8> {
    let mut script = String::from(
        "#!/bin/sh\n\
         [ \"$#\" -eq 3 ] || exit 80\n\
         [ \"$2\" = \"--plan\" ] || exit 81\n\
         for argument in \"$@\"; do\n\
           case \"$argument\" in\n\
             *process-access-value*|*process-secret-value*|*process-client-id*|*process-client-secret*) exit 82 ;;\n\
           esac\n\
         done\n\
         [ \"$AWS_ACCESS_KEY_ID\" = \"process-access-value\" ] || exit 83\n\
         [ \"$AWS_SECRET_ACCESS_KEY\" = \"process-secret-value\" ] || exit 84\n\
         [ \"$CATALOG_BENCH_ENGINE_CLIENT_ID\" = \"process-client-id\" ] || exit 85\n\
         [ \"$CATALOG_BENCH_ENGINE_CLIENT_SECRET\" = \"process-client-secret\" ] || exit 86\n\
         [ -z \"${CATALOG_BENCH_S3_ACCESS_KEY_ID+x}\" ] || exit 87\n\
         [ -z \"${CATALOG_BENCH_S3_SECRET_ACCESS_KEY+x}\" ] || exit 88\n\
         [ -z \"${CATALOG_BENCH_POLARIS_CLIENT_ID+x}\" ] || exit 89\n\
         [ -z \"${CATALOG_BENCH_POLARIS_CLIENT_SECRET+x}\" ] || exit 90\n",
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

fn malformed_script() -> Vec<u8> {
    let mut script = String::from("#!/bin/sh\n");
    append_event(&mut script, runtime_ready());
    append_event(&mut script, json!({"event": "catalog-ready"}));
    append_event(
        &mut script,
        json!({"event": "fixture-preflight", "absent": true}),
    );
    script.push_str("printf '%s\\n' 'CATALOG_BENCH_EVENT {malformed}'\nwhile :; do :; done\n");
    script.into_bytes()
}

fn timeout_script() -> Vec<u8> {
    let mut script = String::from("#!/bin/sh\n");
    append_event(&mut script, runtime_ready());
    append_event(&mut script, json!({"event": "catalog-ready"}));
    append_event(
        &mut script,
        json!({"event": "fixture-preflight", "absent": true}),
    );
    script.push_str("while :; do :; done\n");
    script.into_bytes()
}

fn descendant_script(marker: &std::path::Path) -> Vec<u8> {
    let marker = marker.to_str().unwrap();
    assert!(!marker.contains('\''));
    let mut script = String::from("#!/bin/sh\n");
    append_event(&mut script, runtime_ready());
    append_event(&mut script, json!({"event": "catalog-ready"}));
    append_event(
        &mut script,
        json!({"event": "fixture-preflight", "absent": true}),
    );
    script.push_str("parent=$$\n(\n  while kill -0 \"$parent\" 2>/dev/null; do :; done\n");
    script.push_str("  printf survived > '");
    script.push_str(marker);
    script.push_str("'\n) &\nwhile :; do :; done\n");
    script.into_bytes()
}

fn complete_script() -> Vec<u8> {
    let mut script = String::from("#!/bin/sh\n");
    append_event(&mut script, runtime_ready());
    append_event(&mut script, json!({"event": "catalog-ready"}));
    append_event(
        &mut script,
        json!({"event": "fixture-preflight", "absent": true}),
    );
    append_event(
        &mut script,
        json!({"event": "namespace-ready", "listed_exactly": true}),
    );
    append_event(
        &mut script,
        json!({"event": "table-ready", "table": table_observation(0, 3)}),
    );
    append_event(
        &mut script,
        json!({"event": "initial-appended", "snapshots": 1}),
    );
    append_event(
        &mut script,
        json!({
            "event": "initial-read",
            "read": {
                "rows": 16,
                "bytes": 346,
                "sha256": "e78b526d7e757090a9a90c80802c2a543cbf8166cfac6d6ed48c618926e85a15"
            }
        }),
    );
    append_event(
        &mut script,
        json!({"event": "schema-evolved", "table": table_observation(1, 4)}),
    );
    append_event(
        &mut script,
        json!({"event": "evolved-appended", "snapshots": 2}),
    );
    append_event(
        &mut script,
        json!({
            "event": "evolved-read",
            "read": {
                "rows": 20,
                "bytes": 570,
                "sha256": "b2af6f475851e07d1ace3706d8867530c13dd5938bee90cfcc62d3939e01bea2"
            }
        }),
    );
    append_event(
        &mut script,
        json!({"event": "final-table", "table": table_observation(2, 4)}),
    );
    append_event(&mut script, json!({"event": "completed"}));
    script.push_str("exit 0\n");
    script.into_bytes()
}

fn engine_failure_script() -> Vec<u8> {
    let mut script = String::from("#!/bin/sh\n");
    append_event(&mut script, runtime_ready());
    append_event(&mut script, json!({"event": "catalog-ready"}));
    append_event(
        &mut script,
        json!({"event": "fixture-preflight", "absent": true}),
    );
    append_event(
        &mut script,
        json!({
            "event": "failed",
            "stage": "create-namespace",
            "category": "catalog"
        }),
    );
    script.push_str("exit 2\n");
    script.into_bytes()
}

fn runtime_ready() -> serde_json::Value {
    json!({
        "event": "runtime-ready",
        "runtime": {
            "spark_version": "4.1.3",
            "scala_version": "2.13.17",
            "java_version": "21.0.11",
            "operating_system": "Linux",
            "architecture": "aarch64"
        }
    })
}

fn table_observation(snapshots: u64, last_column_id: i32) -> serde_json::Value {
    json!({
        "table_uuid": "00000000-0000-0000-0000-000000000001",
        "metadata_location": "s3://warehouse/table/metadata/v1.metadata.json",
        "location": "s3://warehouse/table",
        "format_version": 2,
        "last_column_id": last_column_id,
        "schema": [],
        "snapshots": snapshots,
        "properties": {}
    })
}

fn append_event(script: &mut String, event: serde_json::Value) {
    let prefix = std::str::from_utf8(ENGINE_EVENT_PREFIX).unwrap();
    let line = format!("{prefix}{}", serde_json::to_string(&event).unwrap());
    assert!(!line.contains('\''));
    script.push_str("printf '%s\\n' '");
    script.push_str(&line);
    script.push_str("'\n");
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
