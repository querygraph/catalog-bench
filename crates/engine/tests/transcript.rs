use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use catalog_bench_common::contract::ComponentId;
use catalog_bench_engine::{
    run_stock_spark_interoperability, EngineContracts, EngineEvidenceErrorKind,
    EngineSanitizationViolation, EngineTranscriptValidationFailureKind, SecretRead, SecretSource,
    ENGINE_TRANSCRIPT_FORMAT,
};

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.json");
const SECRET_SENTINEL: &str = "transcript-secret-sentinel";

#[test]
fn contracts_bind_the_exact_profile_and_scenario_bytes() {
    let contracts = EngineContracts::parse(PROFILE, SCENARIO).unwrap();
    let mut whitespace_variant = PROFILE.to_vec();
    whitespace_variant.push(b'\n');
    let variant = EngineContracts::parse(&whitespace_variant, SCENARIO).unwrap();

    assert_ne!(
        contracts.digests().profile_sha256,
        variant.digests().profile_sha256
    );
    assert_eq!(
        contracts.digests().scenario_sha256,
        variant.digests().scenario_sha256
    );
    assert_eq!(
        EngineContracts::parse(SCENARIO, SCENARIO)
            .unwrap_err()
            .kind(),
        EngineEvidenceErrorKind::ProfileDocumentKind
    );
    assert_eq!(
        EngineContracts::parse(PROFILE, PROFILE).unwrap_err().kind(),
        EngineEvidenceErrorKind::ScenarioDocumentKind
    );
    assert_eq!(
        EngineContracts::parse(b"not-json", SCENARIO)
            .unwrap_err()
            .kind(),
        EngineEvidenceErrorKind::ProfileContract
    );
}

#[tokio::test]
async fn production_entrypoint_emits_bound_fail_closed_evidence() {
    let contracts = EngineContracts::parse(PROFILE, SCENARIO).unwrap();
    let secrets = Arc::new(RecordingSecrets::with_values([(
        "CATALOG_BENCH_S3_ACCESS_KEY_ID",
        SECRET_SENTINEL,
    )]));
    let transcript = run_stock_spark_interoperability(
        &contracts,
        &ComponentId::from("lakecat"),
        "transcript01",
        Arc::clone(&secrets),
    )
    .await
    .unwrap();

    assert_eq!(transcript.format, ENGINE_TRANSCRIPT_FORMAT);
    assert_eq!(transcript.contract_digests, *contracts.digests());
    assert_eq!(transcript.components.runner.id.as_str(), "rust-runner");
    assert_eq!(transcript.components.catalog.id.as_str(), "lakecat");
    assert_eq!(transcript.components.engine.id.as_str(), "spark-4.1");
    assert_eq!(transcript.components.connector.id.as_str(), "iceberg-java");
    assert_eq!(transcript.components.object_store.id.as_str(), "minio");
    assert_eq!(transcript.fixture.id, "transcript01");
    assert!(transcript.sanitization.passed());
    assert!(transcript.validate(&contracts).is_ok());
    assert!(!transcript.passed());

    let serialized = serde_json::to_string(&transcript).unwrap();
    assert!(!serialized.contains(SECRET_SENTINEL));
    assert!(!serialized.contains("Bearer "));
}

#[tokio::test]
async fn validation_and_value_audit_reject_tampered_transcripts() {
    let contracts = EngineContracts::parse(PROFILE, SCENARIO).unwrap();
    let transcript = run_stock_spark_interoperability(
        &contracts,
        &ComponentId::from("lakecat"),
        "transcript02",
        Arc::new(RecordingSecrets::default()),
    )
    .await
    .unwrap();

    let mut tampered = transcript.clone();
    tampered.format = "catalog-bench/wrong".to_owned();
    assert_eq!(
        tampered.validate(&contracts).unwrap_err().kind,
        EngineTranscriptValidationFailureKind::Format
    );

    let mut tampered = transcript.clone();
    tampered.execution.checks.engine_runtime_pinned =
        !tampered.execution.checks.engine_runtime_pinned;
    assert_eq!(
        tampered.validate(&contracts).unwrap_err().kind,
        EngineTranscriptValidationFailureKind::Execution
    );

    let mut tampered = transcript.clone();
    tampered.components.catalog.id = ComponentId::from("polaris");
    assert!(matches!(
        tampered.validate(&contracts).unwrap_err().kind,
        EngineTranscriptValidationFailureKind::Components
            | EngineTranscriptValidationFailureKind::Execution
    ));

    let mut tampered = transcript.clone();
    tampered.sanitization.transcript_sanitized = false;
    assert_eq!(
        tampered.validate(&contracts).unwrap_err().kind,
        EngineTranscriptValidationFailureKind::Sanitization
    );
    assert!(!tampered.passed());

    let mut tampered = transcript.clone();
    tampered.sanitization.negotiation_redactions_observed += 1;
    assert_eq!(
        tampered.validate(&contracts).unwrap_err().kind,
        EngineTranscriptValidationFailureKind::Sanitization
    );

    let mut tampered = transcript.clone();
    tampered.sanitization.policy = SECRET_SENTINEL.to_owned();
    assert_eq!(
        tampered
            .audit_serialized_values(&contracts, &[SECRET_SENTINEL.to_owned()])
            .unwrap_err(),
        EngineSanitizationViolation::SensitiveRuntimeValue
    );

    let mut tampered = transcript.clone();
    tampered.sanitization.policy = "Bearer unredacted-token".to_owned();
    assert_eq!(
        tampered
            .audit_serialized_values(&contracts, &[])
            .unwrap_err(),
        EngineSanitizationViolation::RawCredentialForm
    );

    let mut tampered = transcript.clone();
    tampered.sanitization.policy = "captured [0,\"category-0\",7] row".to_owned();
    assert_eq!(
        tampered
            .audit_serialized_values(&contracts, &[])
            .unwrap_err(),
        EngineSanitizationViolation::RawEngineRow
    );

    let mut value = serde_json::to_value(&transcript).unwrap();
    value["unreviewed"] = serde_json::json!(true);
    assert!(serde_json::from_value::<catalog_bench_engine::EngineTranscript>(value).is_err());
}

#[derive(Default)]
struct RecordingSecrets {
    values: BTreeMap<String, String>,
    reads: Mutex<Vec<String>>,
}

impl RecordingSecrets {
    fn with_values<const N: usize>(values: [(&str, &str); N]) -> Self {
        Self {
            values: values
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
            reads: Mutex::default(),
        }
    }
}

impl SecretSource for RecordingSecrets {
    fn read_secret(&self, name: &str) -> SecretRead {
        self.reads.lock().unwrap().push(name.to_owned());
        self.values
            .get(name)
            .cloned()
            .map(SecretRead::Value)
            .unwrap_or(SecretRead::Missing)
    }
}
