use std::collections::BTreeMap;

use catalog_bench_commit::aggregate::{aggregate_contention, AggregationError};
use catalog_bench_commit::model::{
    FinalStateAttribution, MetadataGrowthEvidence, OutcomeLatencies, PhaseEvidence, RequestCounts,
    RequestDigest,
};
use catalog_bench_commit::policy::{
    ContentionFixture, ContentionPlan, RoundKind, CONTENTION_TRANSCRIPT_FORMAT,
};
use catalog_bench_commit::protocol::{MutationReceipt, PresenceObservation, ResourcePresence};
use catalog_bench_commit::store::ObjectAuditSnapshot;
use catalog_bench_commit::transcript::{
    CatalogRoundOutcome, CatalogRoundTranscript, ContentionSanitization, ContentionTranscript,
    RankingBasis, RankingDisposition, RankingTieBreaker, RunnerTranscript, SanitizationViolation,
    SweepClassification, TranscriptProfile,
};
use catalog_bench_commit::workflow::{
    CleanupEvidence, FinalTableEvidence, OperationEvidence, RoundChecks, RoundClassification,
    RoundDescriptor, RoundExecution, RoundWorkload, SetupTableEvidence,
};
use catalog_bench_common::contract::{
    parse_contract, ComponentId, ContractDocument, Distribution, Profile, Scenario,
};
use catalog_bench_conformance::{
    CatalogNegotiationEvidence, CatalogNegotiationFailure, CatalogNegotiationFailureStage,
    ContractDigests, TranscriptScenario,
};
use serde_json::json;

const PROFILE_BYTES: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-26.json");
const SCENARIO_BYTES: &[u8] =
    include_bytes!("../../../scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json");

#[test]
fn five_measured_rounds_are_aggregated_and_ranked_by_concurrent_acceptance() {
    let (profile, scenario, plan) = contracts();
    let rounds = passing_rounds(&plan);
    let (aggregates, ranking, classification) = aggregate_contention(&plan, &rounds).unwrap();

    assert_eq!(classification, SweepClassification::Pass);
    assert_eq!(aggregates.len(), 5);
    assert!(aggregates.iter().all(|aggregate| aggregate.passed()));
    assert!(aggregates.iter().all(|aggregate| {
        aggregate.conditioning.scheduled == 1
            && aggregate.conditioning.passed == 1
            && aggregate.measured.scheduled == 5
            && aggregate.measured.passed == 5
            && aggregate.measurements.as_ref().is_some_and(|measurements| {
                measurements
                    .concurrent_accepted_throughput_per_second
                    .samples
                    == 5
            })
    }));
    assert_eq!(
        ranking.basis,
        RankingBasis::ConcurrentAcceptedThroughputPerSecond
    );
    assert_eq!(
        ranking.tie_breakers,
        vec![
            RankingTieBreaker::SequentialLatencyP50Ascending,
            RankingTieBreaker::CatalogIdAscending,
        ]
    );
    assert_eq!(
        ranking
            .entries
            .iter()
            .map(|entry| entry.catalog.catalog.as_str())
            .collect::<Vec<_>>(),
        vec!["lakecat", "polaris", "gravitino", "lakekeeper", "nessie"]
    );
    assert!(ranking.entries.iter().enumerate().all(|(index, entry)| {
        matches!(
            entry.disposition,
            RankingDisposition::Ranked { rank, .. } if rank == index as u32 + 1
        )
    }));

    let transcript = transcript(
        profile,
        scenario,
        plan,
        rounds,
        aggregates,
        ranking,
        classification,
    );
    assert!(transcript
        .audit_serialized_values(&["not-present".to_owned()])
        .is_ok());
    assert!(transcript.passed());
}

#[test]
fn one_failed_round_removes_only_that_catalog_from_the_full_ranking() {
    let (_, _, plan) = contracts();
    let mut rounds = passing_rounds(&plan);
    let failed_index = rounds
        .iter()
        .position(|round| {
            round.catalog.catalog.as_str() == "polaris" && round.kind == RoundKind::Measured
        })
        .unwrap();
    rounds[failed_index].outcome = CatalogRoundOutcome::NegotiationFailed {
        failure: CatalogNegotiationFailure {
            stage: CatalogNegotiationFailureStage::Config,
            detail: "injected config failure".to_owned(),
        },
    };

    let (aggregates, ranking, classification) = aggregate_contention(&plan, &rounds).unwrap();
    assert_eq!(
        classification,
        SweepClassification::Fail {
            failed_catalogs: vec![ComponentId::from("polaris")]
        }
    );
    let polaris = aggregates
        .iter()
        .find(|aggregate| aggregate.catalog.catalog.as_str() == "polaris")
        .unwrap();
    assert!(!polaris.passed());
    assert_eq!(polaris.measured.executed, 4);
    assert_eq!(polaris.measured.passed, 4);
    assert!(polaris.measurements.is_none());
    assert_eq!(ranking.entries.len(), 5);
    assert!(matches!(
        ranking.entries.last().unwrap().disposition,
        RankingDisposition::NotRanked { .. }
    ));
    assert_eq!(
        ranking.entries.last().unwrap().catalog.catalog.as_str(),
        "polaris"
    );
}

#[test]
fn equal_concurrent_scores_use_sequential_latency_then_catalog_id() {
    let (_, _, plan) = contracts();
    let mut rounds = passing_rounds(&plan);
    for round in rounds.iter_mut().filter(|round| {
        round.kind == RoundKind::Measured && round.catalog.catalog.as_str() == "polaris"
    }) {
        let CatalogRoundOutcome::Executed { execution } = &mut round.outcome else {
            panic!("fixture is executed");
        };
        let OperationEvidence::Succeeded { output: concurrent } = &mut execution.concurrent else {
            panic!("fixture has concurrent evidence");
        };
        concurrent.accepted_throughput_per_second =
            catalog_score(&ComponentId::from("lakecat")) + f64::from(round.repetition);
        let OperationEvidence::Succeeded { output: sequential } = &mut execution.sequential else {
            panic!("fixture has sequential evidence");
        };
        sequential
            .latency_ms
            .all
            .as_mut()
            .unwrap()
            .quantiles
            .insert("p50".to_owned(), 2.0);
    }

    let (_, ranking, _) = aggregate_contention(&plan, &rounds).unwrap();
    assert_eq!(ranking.entries[0].catalog.catalog.as_str(), "polaris");
    assert_eq!(ranking.entries[1].catalog.catalog.as_str(), "lakecat");
}

#[test]
fn schedule_reordering_and_nested_descriptor_drift_fail_closed() {
    let (_, _, plan) = contracts();
    let mut reordered = passing_rounds(&plan);
    reordered.swap(0, 1);
    assert_eq!(
        aggregate_contention(&plan, &reordered).unwrap_err(),
        AggregationError::ScheduleMismatch { index: 0 }
    );

    let mut drifted = passing_rounds(&plan);
    let CatalogRoundOutcome::Executed { execution } = &mut drifted[0].outcome else {
        panic!("fixture is executed");
    };
    execution.descriptor.repetition += 1;
    assert_eq!(
        aggregate_contention(&plan, &drifted).unwrap_err(),
        AggregationError::ScheduleMismatch { index: 0 }
    );
}

#[test]
fn serialization_audit_checks_values_for_secrets_and_raw_request_ids() {
    let (profile, scenario, plan) = contracts();
    let rounds = passing_rounds(&plan);
    let (aggregates, ranking, classification) = aggregate_contention(&plan, &rounds).unwrap();
    let mut transcript = transcript(
        profile,
        scenario,
        plan,
        rounds,
        aggregates,
        ranking,
        classification,
    );

    // A sensitive token equal to a schema key does not collide because keys are
    // vocabulary rather than captured runtime values.
    assert!(transcript
        .audit_serialized_values(&["raw_secrets_persisted".to_owned()])
        .is_ok());

    transcript.sanitization.policy = "contains-supersecret".to_owned();
    assert_eq!(
        transcript
            .audit_serialized_values(&["supersecret".to_owned()])
            .unwrap_err(),
        SanitizationViolation::SensitiveRuntimeValue
    );
    transcript.sanitization.policy = "catalog-bench/value-safe-v1".to_owned();

    transcript.sanitization.write_mode = "lakecat/fixture/1/concurrent/0/0".to_owned();
    assert_eq!(
        transcript.audit_serialized_values(&[]).unwrap_err(),
        SanitizationViolation::RawRequestIdentity
    );
}

fn contracts() -> (Profile, Scenario, ContentionPlan) {
    let ContractDocument::Profile(profile) = parse_contract(PROFILE_BYTES).unwrap() else {
        panic!("profile fixture");
    };
    let ContractDocument::Scenario(scenario) = parse_contract(SCENARIO_BYTES).unwrap() else {
        panic!("scenario fixture");
    };
    let plan = ContentionPlan::from_contracts(&profile, &scenario).unwrap();
    (profile, scenario, plan)
}

fn passing_rounds(plan: &ContentionPlan) -> Vec<CatalogRoundTranscript> {
    let workload = RoundWorkload::try_from(&plan.parameters().workload).unwrap();
    plan.rounds()
        .iter()
        .flat_map(|round| {
            let workload = workload.clone();
            round
                .catalogs
                .iter()
                .enumerate()
                .map(move |(position, catalog)| {
                    let score = catalog_score(&catalog.catalog) + f64::from(round.repetition);
                    CatalogRoundTranscript {
                        repetition: round.repetition,
                        kind: round.kind,
                        position: u32::try_from(position + 1).unwrap(),
                        catalog: catalog.clone(),
                        negotiation: negotiation(&catalog.catalog),
                        outcome: CatalogRoundOutcome::Executed {
                            execution: Box::new(passing_execution(
                                catalog.catalog.as_str(),
                                round.repetition,
                                round.kind,
                                workload.clone(),
                                score,
                            )),
                        },
                    }
                })
        })
        .collect()
}

fn catalog_score(catalog: &ComponentId) -> f64 {
    match catalog.as_str() {
        "lakecat" => 500.0,
        "polaris" => 400.0,
        "gravitino" => 300.0,
        "lakekeeper" => 200.0,
        "nessie" => 100.0,
        other => panic!("unexpected catalog {other}"),
    }
}

fn negotiation(catalog: &ComponentId) -> CatalogNegotiationEvidence {
    serde_json::from_value(json!({
        "adapter": {
            "catalog": catalog.as_str(),
            "name": catalog.as_str(),
            "version": "test",
            "protocol": "iceberg-rest-v1",
            "request_handling": {"kind": "protocol-native"}
        },
        "authentication": {"mode": "anonymous", "outcome": "ready"},
        "config": {
            "request": {
                "method": "GET",
                "url": format!("http://{catalog}/v1/config"),
                "headers": {"accept": "application/json"}
            },
            "prefix": {"mode": "unprefixed"},
            "namespace_separator": {"mode": "default", "encoded": "%1F"}
        },
        "redactions": []
    }))
    .unwrap()
}

fn passing_execution(
    catalog: &str,
    repetition: u32,
    kind: RoundKind,
    workload: RoundWorkload,
    concurrent_throughput: f64,
) -> RoundExecution {
    let location = format!("s3://warehouse/{catalog}/fixture/table");
    let metadata_location = format!("{location}/metadata/01850.metadata.json");
    let presence = |present| PresenceObservation {
        http_status: if present { 200 } else { 404 },
        presence: if present {
            ResourcePresence::Present
        } else {
            ResourcePresence::Absent
        },
    };
    let mutation = || OperationEvidence::Succeeded {
        output: MutationReceipt { http_status: 200 },
    };
    RoundExecution {
        descriptor: RoundDescriptor {
            catalog: catalog.to_owned(),
            repetition,
            kind,
            fixture: ContentionFixture {
                id: "fixture".to_owned(),
                namespace: format!("cb_c108_{catalog}_fixture_r{repetition:02}"),
                table: "same_table_contention".to_owned(),
            },
        },
        workload,
        classification: RoundClassification::Pass,
        preflight: OperationEvidence::Succeeded {
            output: presence(false),
        },
        create_namespace: mutation(),
        create_table: OperationEvidence::Succeeded {
            output: SetupTableEvidence {
                format_version: 2,
                table_uuid: "table-uuid".to_owned(),
                location: location.clone(),
                metadata_location: format!("{location}/metadata/00000.metadata.json"),
                requested_location: Some(location.clone()),
            },
        },
        baseline_object_audit: OperationEvidence::Succeeded {
            output: object_audit(
                &location,
                &format!("{location}/metadata/00000.metadata.json"),
                1,
            ),
        },
        warmup: OperationEvidence::Succeeded {
            output: serial_phase(50, 50.0, 2.0),
        },
        sequential: OperationEvidence::Succeeded {
            output: serial_phase(1_000, 100.0, 3.0),
        },
        concurrent: OperationEvidence::Succeeded {
            output: concurrent_phase(concurrent_throughput),
        },
        final_table: OperationEvidence::Succeeded {
            output: FinalTableEvidence {
                format_version: 2,
                table_uuid: "table-uuid".to_owned(),
                metadata_location: metadata_location.clone(),
                table_uuid_matches_setup: true,
                table_location_matches_setup: true,
                attribution: FinalStateAttribution {
                    property_present: true,
                    request_sha256: Some(RequestDigest::from_bytes(b"accepted")),
                    belongs_to_accepted_request: true,
                    metadata_location_within_table_root: true,
                },
            },
        },
        final_object_audit: OperationEvidence::Succeeded {
            output: object_audit(&location, &metadata_location, 1_851),
        },
        metadata_growth: OperationEvidence::Succeeded {
            output: MetadataGrowthEvidence {
                baseline_metadata_objects: 1,
                final_metadata_objects: 1_851,
                observed_growth: Some(1_850),
                minimum_required_growth: 1_850,
                sufficient: true,
            },
        },
        cleanup: CleanupEvidence {
            drop_table_without_purge: mutation(),
            verify_table_absent: OperationEvidence::Succeeded {
                output: presence(false),
            },
            drop_namespace: mutation(),
            verify_namespace_absent: OperationEvidence::Succeeded {
                output: presence(false),
            },
        },
        checks: RoundChecks {
            fixture_isolated: true,
            setup_succeeded: true,
            warmup_accounted: true,
            sequential_accounted: true,
            sequential_latency_complete: true,
            all_requests_accounted: true,
            zero_request_errors: true,
            concurrent_progress: true,
            final_state_accounted: true,
            metadata_persisted: true,
            fixture_clean: true,
        },
    }
}

fn serial_phase(attempts: u64, throughput: f64, latency: f64) -> PhaseEvidence {
    phase(
        RequestCounts {
            attempts,
            accepted: attempts,
            conflicts: 0,
            errors: 0,
        },
        throughput,
        throughput,
        latency,
    )
}

fn concurrent_phase(accepted_throughput: f64) -> PhaseEvidence {
    phase(
        RequestCounts {
            attempts: 1_000,
            accepted: 800,
            conflicts: 200,
            errors: 0,
        },
        accepted_throughput * 1.25,
        accepted_throughput,
        4.0,
    )
}

fn phase(
    counts: RequestCounts,
    attempted_throughput_per_second: f64,
    accepted_throughput_per_second: f64,
    latency: f64,
) -> PhaseEvidence {
    let conflict_rate = counts.conflicts as f64 / counts.attempts as f64;
    let distribution = Distribution {
        samples: counts.attempts,
        minimum: latency / 2.0,
        maximum: latency * 2.0,
        mean: Some(latency),
        standard_deviation: Some(0.1),
        quantiles: BTreeMap::from([
            ("p50".to_owned(), latency),
            ("p95".to_owned(), latency * 1.5),
            ("p99".to_owned(), latency * 1.8),
        ]),
    };
    let accepted = (counts.accepted > 0).then_some(Distribution {
        samples: counts.accepted,
        ..distribution.clone()
    });
    let conflicts = (counts.conflicts > 0).then_some(Distribution {
        samples: counts.conflicts,
        ..distribution.clone()
    });
    PhaseEvidence {
        elapsed_ms: 1_000.0,
        counts,
        latency_ms: OutcomeLatencies {
            all: Some(distribution),
            accepted,
            conflicts,
            errors: None,
        },
        attempted_throughput_per_second,
        accepted_throughput_per_second,
        conflict_rate,
        error_rate: 0.0,
        error_counts: Vec::new(),
    }
}

fn object_audit(root: &str, metadata: &str, objects: u64) -> ObjectAuditSnapshot {
    ObjectAuditSnapshot {
        table_root: root.to_owned(),
        metadata_objects: objects,
        metadata_bytes: objects * 100,
        referenced_metadata_location: metadata.to_owned(),
        referenced_metadata_exists: true,
    }
}

#[allow(clippy::too_many_arguments)]
fn transcript(
    profile: Profile,
    scenario: Scenario,
    plan: ContentionPlan,
    rounds: Vec<CatalogRoundTranscript>,
    aggregates: Vec<catalog_bench_commit::transcript::CatalogAggregate>,
    ranking: catalog_bench_commit::transcript::ContentionRanking,
    classification: SweepClassification,
) -> ContentionTranscript {
    let runner = profile
        .components
        .iter()
        .find(|component| component.id.as_str() == "catalog-bench-commit")
        .unwrap();
    let runner = RunnerTranscript {
        component: runner.id.clone(),
        name: runner.name.clone(),
        version: runner.version.clone(),
        source_revision: runner.version.clone(),
        operating_system: "Linux".to_owned(),
        architecture: "aarch64".to_owned(),
        profile_runtime_matches: true,
        profile_source_matches: true,
    };
    ContentionTranscript {
        format: CONTENTION_TRANSCRIPT_FORMAT.to_owned(),
        scenario: TranscriptScenario {
            id: scenario.id,
            version: scenario.version,
        },
        contract_digests: ContractDigests {
            profile_sha256: "0".repeat(64),
            scenario_sha256: "1".repeat(64),
        },
        profile: TranscriptProfile {
            id: profile.id,
            resolved_at: profile.resolved_at,
        },
        runner,
        fixture_id: "fixture".to_owned(),
        parameters: plan.parameters().clone(),
        rounds,
        aggregates,
        ranking,
        classification,
        sanitization: ContentionSanitization {
            policy: "catalog-bench/value-safe-v1".to_owned(),
            redactions: Vec::new(),
            raw_secrets_persisted: false,
            raw_response_body_persisted: false,
            raw_request_identities_persisted: false,
            write_mode: "create-new".to_owned(),
        },
    }
}
