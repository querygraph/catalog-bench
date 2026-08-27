use catalog_bench_engine::{
    EngineCredentialFailure, EngineCredentialFailureKind, EngineCredentialKind,
    EngineFailureCategory, EnginePreparationFailureKind, EngineProcessOutcome, EngineStage,
};
use serde_json::json;

#[test]
fn engine_neutral_outcomes_preserve_the_closed_transcript_vocabulary() {
    let cases = [
        (
            EngineProcessOutcome::RuntimeRejected {},
            json!({ "status": "runtime-rejected" }),
        ),
        (
            EngineProcessOutcome::SpawnFailed {},
            json!({ "status": "spawn-failed" }),
        ),
        (
            EngineProcessOutcome::TimedOut {},
            json!({ "status": "timed-out" }),
        ),
        (
            EngineProcessOutcome::StdoutFailed {},
            json!({ "status": "stdout-failed" }),
        ),
        (
            EngineProcessOutcome::WaitFailed {},
            json!({ "status": "wait-failed" }),
        ),
        (
            EngineProcessOutcome::ExitProtocolMismatch {},
            json!({ "status": "exit-protocol-mismatch" }),
        ),
        (
            EngineProcessOutcome::Completed {},
            json!({ "status": "completed" }),
        ),
        (
            EngineProcessOutcome::FixtureCollision {},
            json!({ "status": "fixture-collision" }),
        ),
        (
            EngineProcessOutcome::CredentialRejected {
                failure: EngineCredentialFailure {
                    credential: EngineCredentialKind::CatalogClientSecret,
                    kind: EngineCredentialFailureKind::Unreadable,
                },
            },
            json!({
                "status": "credential-rejected",
                "failure": {
                    "credential": "catalog-client-secret",
                    "kind": "unreadable"
                }
            }),
        ),
        (
            EngineProcessOutcome::PreparationFailed {
                kind: EnginePreparationFailureKind::WriteRenderer,
            },
            json!({
                "status": "preparation-failed",
                "kind": "write-renderer"
            }),
        ),
        (
            EngineProcessOutcome::EngineFailed {
                stage: EngineStage::ReadEvolved,
                category: EngineFailureCategory::Data,
            },
            json!({
                "status": "engine-failed",
                "stage": "read-evolved",
                "category": "data"
            }),
        ),
    ];

    for (outcome, expected) in cases {
        assert_eq!(serde_json::to_value(&outcome).unwrap(), expected);
        assert_eq!(
            serde_json::from_value::<EngineProcessOutcome>(expected).unwrap(),
            outcome
        );
    }

    assert!(serde_json::from_value::<EngineProcessOutcome>(json!({
        "status": "completed",
        "spark_only": true
    }))
    .is_err());
}
