use std::time::Duration;

use catalog_bench_commit::model::{
    AcceptedRequests, FinalStateAttribution, MetadataGrowthEvidence, RequestErrorKind,
    RequestIdentity, RequestLedger, RequestOutcome, SanitizedRequestError,
};

#[test]
fn ledger_preserves_every_outcome_and_latency_sample() {
    let accepted = RequestIdentity::new("catalog/round/accepted/1").unwrap();
    let conflict = RequestIdentity::new("catalog/round/conflict/1").unwrap();
    let failed = RequestIdentity::new("catalog/round/error/1").unwrap();
    let error = SanitizedRequestError {
        kind: RequestErrorKind::UnexpectedHttp,
        http_status: Some(503),
    };
    let mut ledger = RequestLedger::default();
    ledger
        .record(
            &accepted,
            Duration::from_millis(1),
            RequestOutcome::Accepted,
        )
        .unwrap();
    ledger
        .record(
            &conflict,
            Duration::from_millis(2),
            RequestOutcome::Conflict,
        )
        .unwrap();
    ledger
        .record(
            &failed,
            Duration::from_millis(3),
            RequestOutcome::Error(error.clone()),
        )
        .unwrap();

    let (phase, accepted_requests) = ledger.finish(Duration::from_secs(2)).unwrap();
    assert_eq!(phase.counts.attempts, 3);
    assert_eq!(phase.counts.accepted, 1);
    assert_eq!(phase.counts.conflicts, 1);
    assert_eq!(phase.counts.errors, 1);
    assert!(phase.counts.fully_accounted());
    assert_eq!(phase.latency_ms.all.as_ref().unwrap().samples, 3);
    assert_eq!(phase.latency_ms.accepted.as_ref().unwrap().samples, 1);
    assert_eq!(phase.latency_ms.conflicts.as_ref().unwrap().samples, 1);
    assert_eq!(phase.latency_ms.errors.as_ref().unwrap().samples, 1);
    assert_eq!(phase.attempted_throughput_per_second, 1.5);
    assert_eq!(phase.accepted_throughput_per_second, 0.5);
    assert_eq!(phase.conflict_rate, 1.0 / 3.0);
    assert_eq!(phase.error_rate, 1.0 / 3.0);
    assert_eq!(phase.error_counts[0].error, error);
    assert_eq!(phase.error_counts[0].count, 1);
    assert!(accepted_requests.contains_value(accepted.expose_for_request()));
    assert!(!accepted_requests.contains_value(conflict.expose_for_request()));
}

#[test]
fn ledger_rejects_reused_request_identities_without_partial_accounting() {
    let identity = RequestIdentity::new("catalog/round/sequential/1").unwrap();
    let mut ledger = RequestLedger::default();
    ledger
        .record(
            &identity,
            Duration::from_millis(1),
            RequestOutcome::Accepted,
        )
        .unwrap();
    assert!(ledger
        .record(
            &identity,
            Duration::from_millis(2),
            RequestOutcome::Conflict,
        )
        .is_err());

    let (phase, _) = ledger.finish(Duration::from_secs(1)).unwrap();
    assert_eq!(phase.counts.attempts, 1);
    assert_eq!(phase.counts.accepted, 1);
    assert_eq!(phase.counts.conflicts, 0);
}

#[test]
fn only_request_hashes_cross_the_serialization_boundary() {
    let raw = "lakecat/round/secret-request-identity";
    let identity = RequestIdentity::new(raw).unwrap();
    let mut accepted = AcceptedRequests::default();
    accepted.insert(&identity);
    let attribution = FinalStateAttribution::evaluate(Some(raw), &accepted, true);

    assert!(attribution.passed());
    let serialized = serde_json::to_string(&attribution).unwrap();
    assert!(!serialized.contains(raw));
    assert!(serialized.contains(identity.digest().as_str()));
    assert!(!format!("{identity:?}").contains(raw));
    assert!(
        serde_json::from_str::<catalog_bench_commit::model::RequestDigest>("\"not-a-sha256\"")
            .is_err()
    );
}

#[test]
fn accepted_sets_reject_cross_phase_identity_reuse() {
    let identity = RequestIdentity::new("catalog/round/shared/1").unwrap();
    let mut first = AcceptedRequests::default();
    first.insert(&identity);
    let mut second = AcceptedRequests::default();
    second.insert(&identity);

    assert!(first.try_extend(second).is_err());
    assert_eq!(first.len(), 1);
}

#[test]
fn ledgers_merge_without_losing_writer_outcomes() {
    let accepted = RequestIdentity::new("catalog/round/concurrent/0/1").unwrap();
    let conflict = RequestIdentity::new("catalog/round/concurrent/1/1").unwrap();
    let mut first = RequestLedger::default();
    first
        .record(
            &accepted,
            Duration::from_millis(1),
            RequestOutcome::Accepted,
        )
        .unwrap();
    let mut second = RequestLedger::default();
    second
        .record(
            &conflict,
            Duration::from_millis(2),
            RequestOutcome::Conflict,
        )
        .unwrap();

    first.try_merge(second).unwrap();
    let (phase, accepted_requests) = first.finish(Duration::from_secs(1)).unwrap();
    assert_eq!(phase.counts.attempts, 2);
    assert_eq!(phase.counts.accepted, 1);
    assert_eq!(phase.counts.conflicts, 1);
    assert_eq!(phase.latency_ms.all.unwrap().samples, 2);
    assert!(accepted_requests.contains_value(accepted.expose_for_request()));
}

#[test]
fn ledgers_reject_cross_writer_identity_reuse_atomically() {
    let identity = RequestIdentity::new("catalog/round/concurrent/shared").unwrap();
    let mut first = RequestLedger::default();
    first
        .record(
            &identity,
            Duration::from_millis(1),
            RequestOutcome::Accepted,
        )
        .unwrap();
    let mut second = RequestLedger::default();
    second
        .record(
            &identity,
            Duration::from_millis(2),
            RequestOutcome::Conflict,
        )
        .unwrap();

    assert!(first.try_merge(second).is_err());
    let (phase, _) = first.finish(Duration::from_secs(1)).unwrap();
    assert_eq!(phase.counts.attempts, 1);
    assert_eq!(phase.counts.accepted, 1);
    assert_eq!(phase.counts.conflicts, 0);
}

#[test]
fn final_state_and_object_growth_fail_closed() {
    let accepted = AcceptedRequests::default();
    let attribution = FinalStateAttribution::evaluate(Some("unknown"), &accepted, false);
    assert!(!attribution.passed());

    let counts = [
        catalog_bench_commit::model::RequestCounts {
            attempts: 2,
            accepted: 2,
            conflicts: 0,
            errors: 0,
        },
        catalog_bench_commit::model::RequestCounts {
            attempts: 3,
            accepted: 1,
            conflicts: 2,
            errors: 0,
        },
    ];
    let sufficient = MetadataGrowthEvidence::evaluate(1, 4, counts).unwrap();
    assert_eq!(sufficient.observed_growth, Some(3));
    assert_eq!(sufficient.minimum_required_growth, 3);
    assert!(sufficient.sufficient);

    let regressed = MetadataGrowthEvidence::evaluate(4, 3, counts).unwrap();
    assert_eq!(regressed.observed_growth, None);
    assert!(!regressed.sufficient);
}
