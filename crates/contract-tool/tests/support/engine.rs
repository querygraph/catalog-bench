#![allow(
    dead_code,
    reason = "each engine integration-test binary uses a different fixture subset"
)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use catalog_bench_commit::store::TableObjectAuditSnapshot;
use catalog_bench_common::contract::{AdapterRequestHandling, CatalogAuthentication, ComponentId};
use catalog_bench_conformance::{
    encode_evidence, sha256_hex, AuthenticationOutcome, TranscriptAdapter,
};
use catalog_bench_contract::{validate_engine_evidence_set, ValidatedEngineEvidenceSet};
use catalog_bench_engine::{
    run_stock_spark_interoperability, EngineAuthenticationEvidence, EngineAuthenticationMode,
    EngineBehaviorChecks, EngineBehaviorClassification, EngineCatalogConfigEvidence,
    EngineCatalogConnectionEvidence, EngineCatalogNegotiationEvidence, EngineCatalogTable,
    EngineCleanupEvidence, EngineCleanupReceipt, EngineContracts, EngineEvent, EngineEventCapture,
    EngineFieldObservation, EngineOperationEvidence, EngineProcessOutcome,
    EnginePropertyObservation, EngineProtocolFailure, EngineProtocolFailureKind,
    EngineResourcePresence, EngineRoutingResolution, EngineRuntimeObservation, EngineSkipReason,
    EngineTableLoad, EngineTableObservation, EngineTranscript, InteroperabilityPlan,
    RowReadObservation, RuntimeArtifactOutcome, SecretRead, SecretSource, SPARK_JAVA_VERSION,
    SPARK_SCALA_VERSION,
};
use serde_json::{json, Value};
use tempfile::TempDir;

const PROFILE: &[u8] =
    include_bytes!("../../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
pub const SCENARIO: &[u8] =
    include_bytes!("../../../../scenarios/v1/engine.iceberg.write-read-evolution.json");
pub const FIXTURE_ID: &str = "evidence01";
pub const CATALOGS: [&str; 4] = ["lakecat", "polaris", "gravitino", "lakekeeper"];
pub const PROFILE_LOCATION: &str = "profiles/v1/profile.json";
pub const SCENARIO_LOCATION: &str = "scenarios/v1/scenario.json";
pub const SOURCE_DIRECTORY: &str = "results/source/engine/evidence01";
pub const EVIDENCE_LOCATION: &str = "results/source/engine/evidence01/transcripts";
pub const REVIEW_LOCATION: &str = "results/source/engine/evidence01/review.json";

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
        let profile_path = directory.path().join(PROFILE_LOCATION);
        let scenario_path = directory.path().join(SCENARIO_LOCATION);
        let evidence_directory = directory.path().join(EVIDENCE_LOCATION);
        let profile_bytes = profile_with_impossible_spark_submit_digest();
        fs::create_dir_all(profile_path.parent().unwrap()).unwrap();
        fs::create_dir_all(scenario_path.parent().unwrap()).unwrap();
        fs::create_dir_all(&evidence_directory).unwrap();
        fs::write(&profile_path, &profile_bytes).unwrap();
        fs::write(&scenario_path, SCENARIO).unwrap();

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

    fn rewrite_all(&mut self, mode: TranscriptMode) {
        let contracts = EngineContracts::parse(&self.profile_bytes, SCENARIO).unwrap();
        for catalog in CATALOGS {
            let mut transcript: EngineTranscript =
                serde_json::from_slice(&self.transcripts[catalog]).unwrap();
            match mode {
                TranscriptMode::Pass => make_passing(&contracts, &mut transcript),
                TranscriptMode::FixtureCollision => make_collision(&contracts, &mut transcript),
                TranscriptMode::HarnessFailure => {
                    make_passing(&contracts, &mut transcript);
                    make_harness_failure(&mut transcript);
                }
            }
            transcript.validate(&contracts).unwrap();
            let bytes = encode_evidence(&transcript).unwrap();
            fs::write(
                self.evidence_directory.join(format!("{catalog}.json")),
                &bytes,
            )
            .unwrap();
            self.transcripts.insert(catalog.to_owned(), bytes);
        }
    }
}

pub struct ReviewFixture {
    pub evidence: EvidenceFixture,
    pub review_path: PathBuf,
    baseline: Value,
    pub review: Value,
}

impl ReviewFixture {
    pub async fn new() -> Self {
        Self::from_evidence(EvidenceFixture::new().await)
    }

    pub async fn passing() -> Self {
        let mut evidence = EvidenceFixture::new().await;
        evidence.rewrite_all(TranscriptMode::Pass);
        Self::from_evidence(evidence)
    }

    pub async fn fixture_collisions() -> Self {
        let mut evidence = EvidenceFixture::new().await;
        evidence.rewrite_all(TranscriptMode::FixtureCollision);
        Self::from_evidence(evidence)
    }

    pub async fn harness_failures() -> Self {
        let mut evidence = EvidenceFixture::new().await;
        evidence.rewrite_all(TranscriptMode::HarnessFailure);
        Self::from_evidence(evidence)
    }

    fn from_evidence(evidence: EvidenceFixture) -> Self {
        let review_path = evidence.root().join(REVIEW_LOCATION);
        let baseline = review_value(&evidence);
        fs::create_dir_all(review_path.parent().unwrap()).unwrap();
        fs::write(&review_path, pretty_json(&baseline)).unwrap();
        Self {
            evidence,
            review_path,
            review: baseline.clone(),
            baseline,
        }
    }

    pub fn validate(&self) -> anyhow::Result<catalog_bench_contract::ValidatedEngineResultReview> {
        catalog_bench_contract::validate_engine_result_review(
            self.evidence.root(),
            &self.review_path,
        )
    }

    pub fn write_review(&self) {
        fs::write(&self.review_path, pretty_json(&self.review)).unwrap();
    }

    pub fn reset_review(&mut self) {
        self.review = self.baseline.clone();
        self.write_review();
    }
}

#[derive(Clone, Copy)]
enum TranscriptMode {
    Pass,
    FixtureCollision,
    HarnessFailure,
}

fn make_harness_failure(transcript: &mut EngineTranscript) {
    let capture = transcript.execution.process.capture.as_mut().unwrap();
    assert!(matches!(capture.events.pop(), Some(EngineEvent::Completed)));
    capture.failure = Some(EngineProtocolFailure {
        kind: EngineProtocolFailureKind::MissingTerminal,
    });
    transcript.execution.process.outcome = EngineProcessOutcome::ProtocolRejected {
        kind: EngineProtocolFailureKind::MissingTerminal,
    };
    transcript.execution.process.exit_code = None;
    transcript.execution.classification = EngineBehaviorClassification::Fail;
}

fn make_collision(contracts: &EngineContracts, transcript: &mut EngineTranscript) {
    let plan = plan(contracts, transcript);
    make_runtime_match(transcript);
    transcript.execution.process.outcome = EngineProcessOutcome::FixtureCollision {};
    transcript.execution.process.capture = Some(EngineEventCapture {
        events: vec![
            EngineEvent::RuntimeReady {
                runtime: runtime_observation(contracts, transcript),
            },
            EngineEvent::CatalogReady,
            EngineEvent::FixturePreflight { absent: false },
        ],
        failure: None,
        stdout_bytes_observed: 256,
    });
    transcript.execution.process.exit_code = Some(3);
    transcript.execution.process.process_elapsed_micros = Some(1_000);
    transcript.execution.catalog_connection = EngineCatalogConnectionEvidence::NotAttempted {
        reason: EngineSkipReason::FixtureCollision,
    };
    transcript.execution.catalog_state = EngineOperationEvidence::Skipped {
        reason: EngineSkipReason::FixtureCollision,
    };
    transcript.execution.object_state = EngineOperationEvidence::Skipped {
        reason: EngineSkipReason::FixtureCollision,
    };
    transcript.execution.cleanup = skipped_cleanup(EngineSkipReason::FixtureCollision);
    transcript.execution.checks = EngineBehaviorChecks {
        engine_runtime_pinned: true,
        ..EngineBehaviorChecks::default()
    };
    transcript.execution.classification = EngineBehaviorClassification::FixtureCollision;
    transcript.sanitization.negotiation_redactions_observed = 0;
    assert_eq!(
        plan.catalog().id,
        transcript.components.catalog.id,
        "test plan must retain the selected catalog"
    );
}

fn make_passing(contracts: &EngineContracts, transcript: &mut EngineTranscript) {
    let plan = plan(contracts, transcript);
    let (initial, evolved, final_table) = table_observations(&plan);
    make_runtime_match(transcript);
    transcript.execution.process.outcome = EngineProcessOutcome::Completed {};
    transcript.execution.process.capture = Some(EngineEventCapture {
        events: vec![
            EngineEvent::RuntimeReady {
                runtime: runtime_observation(contracts, transcript),
            },
            EngineEvent::CatalogReady,
            EngineEvent::FixturePreflight { absent: true },
            EngineEvent::NamespaceReady {
                listed_exactly: true,
            },
            EngineEvent::TableReady {
                table: initial.clone(),
            },
            EngineEvent::InitialAppended { snapshots: 1 },
            EngineEvent::InitialRead {
                read: canonical_read(&plan, false),
            },
            EngineEvent::SchemaEvolved {
                table: evolved.clone(),
            },
            EngineEvent::EvolvedAppended { snapshots: 2 },
            EngineEvent::EvolvedRead {
                read: canonical_read(&plan, true),
            },
            EngineEvent::FinalTable {
                table: final_table.clone(),
            },
            EngineEvent::Completed,
        ],
        failure: None,
        stdout_bytes_observed: 1_024,
    });
    transcript.execution.process.exit_code = Some(0);
    transcript.execution.process.process_elapsed_micros = Some(1_000);
    transcript.execution.catalog_connection = EngineCatalogConnectionEvidence::Ready {
        negotiation: passing_negotiation(contracts, transcript),
    };
    transcript.execution.catalog_state = EngineOperationEvidence::Succeeded {
        output: EngineTableLoad::Present {
            http_status: 200,
            response_bytes: 1_024,
            state: EngineCatalogTable {
                current_schema_id: 1,
                table: final_table.clone(),
            },
        },
    };
    transcript.execution.object_state = EngineOperationEvidence::Succeeded {
        output: TableObjectAuditSnapshot {
            table_root: final_table.location.clone(),
            metadata_objects: 4,
            metadata_bytes: 4_096,
            parquet_objects: 2,
            parquet_bytes: 2_048,
            referenced_metadata_location: final_table.metadata_location.clone(),
            referenced_metadata_exists: true,
        },
    };
    transcript.execution.cleanup = passing_cleanup();
    transcript.execution.checks = EngineBehaviorChecks {
        engine_runtime_pinned: true,
        stock_rest_catalog_ready: true,
        fixture_isolated: true,
        namespace_round_trip: true,
        table_round_trip: true,
        initial_append_committed: true,
        initial_read_exact: true,
        schema_evolved: true,
        evolved_append_committed: true,
        evolved_read_exact: true,
        catalog_state_correlated: true,
        shared_object_evidence_complete: true,
        fixture_clean: true,
    };
    transcript.execution.classification = EngineBehaviorClassification::Pass;
    transcript.sanitization.negotiation_redactions_observed = 0;
}

fn plan(contracts: &EngineContracts, transcript: &EngineTranscript) -> InteroperabilityPlan {
    InteroperabilityPlan::from_contracts(
        contracts.profile(),
        contracts.scenario(),
        &transcript.components.catalog.id,
        &transcript.fixture.id,
    )
    .unwrap()
}

fn make_runtime_match(transcript: &mut EngineTranscript) {
    let platform = &mut transcript.execution.process.runtime.platform;
    platform.observed_operating_system = platform.expected_operating_system.clone();
    platform.operating_system_matches = true;
    platform.observed_architecture = platform.expected_architecture.clone();
    platform.architecture_matches = true;
    for artifact in &mut transcript.execution.process.runtime.artifacts {
        artifact.outcome = RuntimeArtifactOutcome::Match {
            observed_bytes: artifact.expected_bytes,
            observed_sha256: artifact.expected_sha256.clone(),
        };
    }
}

fn runtime_observation(
    contracts: &EngineContracts,
    transcript: &EngineTranscript,
) -> EngineRuntimeObservation {
    EngineRuntimeObservation {
        spark_version: transcript.components.engine.version.clone(),
        scala_version: SPARK_SCALA_VERSION.to_owned(),
        java_version: SPARK_JAVA_VERSION.to_owned(),
        operating_system: contracts.profile().platform.operating_system.clone(),
        architecture: contracts.profile().platform.architecture.clone(),
    }
}

fn table_observations(
    plan: &InteroperabilityPlan,
) -> (
    EngineTableObservation,
    EngineTableObservation,
    EngineTableObservation,
) {
    let location = plan
        .spark()
        .fixture
        .requested_location
        .clone()
        .unwrap_or_else(|| {
            format!(
                "s3://warehouse/{}/{}",
                plan.spark().fixture.namespace,
                plan.spark().fixture.table
            )
        });
    let properties = plan
        .spark()
        .scenario
        .table
        .properties
        .keys()
        .map(|key| (key.clone(), EnginePropertyObservation::Match))
        .collect::<BTreeMap<_, _>>();
    let initial_fields = plan
        .spark()
        .scenario
        .table
        .schema
        .fields
        .iter()
        .map(|field| EngineFieldObservation {
            id: field.id,
            name: field.name.clone(),
            required: field.required,
            field_type: field.field_type,
        })
        .collect::<Vec<_>>();
    let mut evolved_fields = initial_fields.clone();
    let evolved = &plan.spark().scenario.schema_evolution.field;
    let evolved_id = initial_fields.iter().map(|field| field.id).max().unwrap() + 1;
    evolved_fields.push(EngineFieldObservation {
        id: evolved_id,
        name: evolved.name.clone(),
        required: evolved.required,
        field_type: evolved.field_type,
    });
    let table = |metadata: &str, schema, snapshots, last_column_id| EngineTableObservation {
        table_uuid: "00000000-0000-0000-0000-000000000001".to_owned(),
        metadata_location: format!("{location}/metadata/{metadata}.metadata.json"),
        location: location.clone(),
        format_version: 2,
        last_column_id,
        schema,
        snapshots,
        properties: properties.clone(),
    };
    (
        table(
            "v1",
            initial_fields.clone(),
            0,
            initial_fields.iter().map(|field| field.id).max().unwrap(),
        ),
        table("v3", evolved_fields.clone(), 1, evolved_id),
        table("v4", evolved_fields, 2, evolved_id),
    )
}

fn canonical_read(plan: &InteroperabilityPlan, evolved: bool) -> RowReadObservation {
    let expected = if evolved {
        &plan.spark().scenario.canonical_reads.after_evolution
    } else {
        &plan.spark().scenario.canonical_reads.initial
    };
    RowReadObservation {
        rows: expected.rows,
        bytes: expected.bytes,
        sha256: expected.sha256.clone(),
    }
}

fn passing_negotiation(
    contracts: &EngineContracts,
    transcript: &EngineTranscript,
) -> EngineCatalogNegotiationEvidence {
    let adapter = contracts
        .profile()
        .catalog_adapters
        .iter()
        .find(|adapter| adapter.catalog == transcript.components.catalog.id)
        .unwrap();
    let mode = match adapter.authentication {
        CatalogAuthentication::Anonymous => EngineAuthenticationMode::Anonymous,
        CatalogAuthentication::OAuth2ClientCredentials { .. } => {
            EngineAuthenticationMode::OAuth2ClientCredentials
        }
    };
    EngineCatalogNegotiationEvidence {
        adapter: TranscriptAdapter {
            catalog: transcript.components.catalog.id.clone(),
            name: transcript.components.catalog.name.clone(),
            version: transcript.components.catalog.version.clone(),
            protocol: adapter.protocol,
            request_handling: AdapterRequestHandling::ProtocolNative,
        },
        authentication: EngineAuthenticationEvidence {
            mode,
            outcome: AuthenticationOutcome::Ready,
            http_status: None,
        },
        config: EngineCatalogConfigEvidence {
            http_status: Some(200),
            response_bytes: Some(2),
            prefix: EngineRoutingResolution::Unprefixed,
            namespace_separator: EngineRoutingResolution::Default,
            failure_stage: None,
        },
        redactions_observed: 0,
    }
}

fn passing_cleanup() -> EngineCleanupEvidence {
    EngineCleanupEvidence {
        drop_table_without_purge: EngineOperationEvidence::Succeeded {
            output: cleanup_receipt(),
        },
        verify_table_absent: EngineOperationEvidence::Succeeded { output: absent() },
        drop_namespace: EngineOperationEvidence::Succeeded {
            output: cleanup_receipt(),
        },
        verify_namespace_absent: EngineOperationEvidence::Succeeded { output: absent() },
    }
}

fn skipped_cleanup(reason: EngineSkipReason) -> EngineCleanupEvidence {
    EngineCleanupEvidence {
        drop_table_without_purge: EngineOperationEvidence::Skipped { reason },
        verify_table_absent: EngineOperationEvidence::Skipped { reason },
        drop_namespace: EngineOperationEvidence::Skipped { reason },
        verify_namespace_absent: EngineOperationEvidence::Skipped { reason },
    }
}

fn cleanup_receipt() -> EngineCleanupReceipt {
    EngineCleanupReceipt {
        http_status: 204,
        response_bytes: 0,
        already_absent: false,
    }
}

fn absent() -> EngineResourcePresence {
    EngineResourcePresence::Absent {
        http_status: 404,
        response_bytes: 0,
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

fn review_value(evidence: &EvidenceFixture) -> Value {
    let transcripts = evidence
        .transcripts
        .iter()
        .map(|(catalog, bytes)| {
            json!({
                "catalog": catalog,
                "source": source_identity(
                    &format!("{EVIDENCE_LOCATION}/{catalog}.json"),
                    bytes
                )
            })
        })
        .collect::<Vec<_>>();
    json!({
        "format": "catalog-bench/engine-result-review/v1",
        "bundle": {
            "id": "spark-review-test",
            "title": "Stock Spark interoperability test review",
            "output_directory": "results/v1/spark-review-test",
            "created_at": "2026-08-27T12:02:00Z"
        },
        "run": {
            "fixture_id": FIXTURE_ID,
            "sanitized_invocation": format!(
                "docker/run-spark-interoperability.sh \"{FIXTURE_ID}\""
            ),
            "started_at": "2026-08-27T12:00:00Z",
            "started_at_basis": "Captured immediately before the launcher invocation.",
            "completed_at": "2026-08-27T12:01:00Z",
            "completed_at_basis": "Captured after all four runner processes returned."
        },
        "profile": source_identity(PROFILE_LOCATION, &evidence.profile_bytes),
        "scenario": source_identity(SCENARIO_LOCATION, SCENARIO),
        "transcripts": transcripts,
        "environment": {
            "operating_system": "Linux",
            "architecture": "aarch64",
            "cpu_model": {
                "precision": "unknown",
                "explanation": "The test fixture does not substitute a host CPU model."
            },
            "logical_cpus": { "precision": "exact", "value": 10 },
            "memory_bytes": { "precision": "exact", "value": 8321712128_u64 },
            "network": "catalog-bench-net",
            "container_runtime": {
                "precision": "exact",
                "value": "Docker Engine test fixture"
            },
            "runtime_flags": {
                "docker_compose": "test fixture",
                "same_docker_boundary": "all workflow processes",
                "workflow_execution_order": "sequential"
            }
        },
        "redaction": {
            "reviewed": true,
            "policy": "catalog-bench/value-safe-engine-v1 plus manual source review",
            "removed_fields": [
                "catalog OAuth client credentials and bearer tokens",
                "object-store access and secret keys",
                "raw engine rows and response bodies",
                "raw backend exception and log detail"
            ]
        }
    })
}

fn source_identity(location: &str, bytes: &[u8]) -> Value {
    json!({
        "location": location,
        "sha256": sha256_hex(bytes),
        "bytes": bytes.len() as u64
    })
}
