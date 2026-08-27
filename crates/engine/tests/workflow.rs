use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use catalog_bench_commit::store::{
    ObjectStoreFailure, TableObjectAuditSnapshot, TableObjectStore, TableRoot,
};
use catalog_bench_common::contract::{
    parse_contract, ComponentId, ContractDocument, Profile, Scenario,
};
use catalog_bench_conformance::CatalogNegotiationEvidence;
use catalog_bench_engine::{
    run_engine_workflow, EngineBehaviorClassification, EngineCatalog, EngineCatalogConnection,
    EngineCatalogConnectionEvidence, EngineCatalogConnectionFailure,
    EngineCatalogConnectionFailureKind, EngineCatalogConnector, EngineCatalogFailure,
    EngineCatalogFailureKind, EngineCatalogNegotiationEvidence, EngineCatalogTable,
    EngineCleanupReceipt, EngineEvent, EngineEventCapture, EngineFailureCategory,
    EngineFieldObservation, EngineObjectStoreConnector, EngineOperationEvidence,
    EnginePropertyObservation, EngineProtocolFailure, EngineProtocolFailureKind,
    EngineResourcePresence, EngineRunner, EngineRuntimeObservation, EngineStage, EngineTableLoad,
    EngineTableObservation, InteroperabilityPlan, RowReadObservation, RuntimeArtifactObservation,
    RuntimeArtifactOutcome, RuntimePlatformObservation, RuntimeVerification, SparkProcessExecution,
    SparkProcessOutcome,
};
use serde_json::json;

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.json");

#[tokio::test]
async fn complete_workflow_reconciles_all_three_authorities_before_cleanup() {
    let plan = plan("success01");
    let fixture = PassingFixture::new(&plan);
    let log = OperationLog::default();
    let execution = run_engine_workflow(
        &plan,
        FakeRunner::new(fixture.process(&plan), log.clone()),
        FakeCatalogConnector::ready(fixture.catalog(), log.clone()),
        FakeObjectStoreConnector::ready(fixture.object_audit(), log.clone()),
    )
    .await;

    assert!(execution.passed(), "{execution:#?}");
    assert_eq!(execution.classification, EngineBehaviorClassification::Pass);
    assert!(execution.checks.all_passed());
    assert_eq!(
        log.entries(),
        [
            "engine",
            "catalog-connect",
            "catalog-load",
            "object-connect",
            "object-audit",
            "drop-table",
            "table-presence",
            "drop-namespace",
            "namespace-presence",
        ]
    );
}

#[tokio::test]
async fn collision_and_runtime_rejection_never_open_harness_effects() {
    let plan = plan("premutation01");
    for process in [collision_process(&plan), runtime_rejected_process(&plan)] {
        let log = OperationLog::default();
        let execution = run_engine_workflow(
            &plan,
            FakeRunner::new(process, log.clone()),
            FakeCatalogConnector::ready(FakeCatalog::passing_absent(log.clone()), log.clone()),
            FakeObjectStoreConnector::ready(unused_audit(), log.clone()),
        )
        .await;

        assert_eq!(log.entries(), ["engine"]);
        assert!(matches!(
            execution.catalog_connection,
            EngineCatalogConnectionEvidence::NotAttempted { .. }
        ));
        assert!(matches!(
            execution.object_state,
            EngineOperationEvidence::Skipped { .. }
        ));
        assert!(!execution.cleanup.drop_table_without_purge.passed());
    }
}

#[tokio::test]
async fn contradictory_collision_claim_is_terminal_and_fails_closed() {
    let plan = plan("collision01");
    let fixture = PassingFixture::new(&plan);
    let log = OperationLog::default();
    let mut process = fixture.process(&plan);
    process.outcome = SparkProcessOutcome::FixtureCollision;
    process.exit_code = Some(3);

    let execution = run_engine_workflow(
        &plan,
        FakeRunner::new(process, log.clone()),
        FakeCatalogConnector::ready(fixture.catalog(), log.clone()),
        FakeObjectStoreConnector::ready(fixture.object_audit(), log.clone()),
    )
    .await;

    assert_eq!(log.entries(), ["engine"]);
    assert_eq!(execution.classification, EngineBehaviorClassification::Fail);
    assert!(!execution.process.fixture_collision());
    assert!(!execution.process.cleanup_authorized());
    assert!(!execution.passed());
}

#[tokio::test]
async fn every_owned_partial_engine_outcome_still_runs_complete_cleanup() {
    let plan = plan("partial01");
    for process in [engine_failed_process(&plan), protocol_failed_process(&plan)] {
        let log = OperationLog::default();
        let execution = run_engine_workflow(
            &plan,
            FakeRunner::new(process, log.clone()),
            FakeCatalogConnector::ready(FakeCatalog::passing_absent(log.clone()), log.clone()),
            FakeObjectStoreConnector::ready(unused_audit(), log.clone()),
        )
        .await;

        assert_eq!(
            log.entries(),
            [
                "engine",
                "catalog-connect",
                "catalog-load",
                "drop-table",
                "table-presence",
                "drop-namespace",
                "namespace-presence",
            ]
        );
        assert!(execution.cleanup.passed());
        assert!(!execution.passed());
        assert!(matches!(
            execution.object_state,
            EngineOperationEvidence::Skipped { .. }
        ));
    }
}

#[tokio::test]
async fn rest_or_object_drift_fails_closed_but_preserves_cleanup_evidence() {
    let plan = plan("drift01");
    let fixture = PassingFixture::new(&plan);

    let log = OperationLog::default();
    let mut drifted_catalog = fixture.catalog();
    let EngineTableLoad::Present { state, .. } = drifted_catalog.load.as_mut().unwrap() else {
        panic!("passing catalog must retain a table");
    };
    state.table.metadata_location = format!("{}/metadata/v99.metadata.json", state.table.location);
    let execution = run_engine_workflow(
        &plan,
        FakeRunner::new(fixture.process(&plan), log.clone()),
        FakeCatalogConnector::ready(drifted_catalog, log.clone()),
        FakeObjectStoreConnector::ready(fixture.object_audit(), log),
    )
    .await;
    assert!(!execution.checks.catalog_state_correlated);
    assert!(!execution.checks.shared_object_evidence_complete);
    assert!(execution.cleanup.passed());
    assert!(!execution.passed());

    let log = OperationLog::default();
    let mut undercount = fixture.object_audit();
    undercount.metadata_objects = 3;
    undercount.parquet_objects = 1;
    let execution = run_engine_workflow(
        &plan,
        FakeRunner::new(fixture.process(&plan), log.clone()),
        FakeCatalogConnector::ready(fixture.catalog(), log.clone()),
        FakeObjectStoreConnector::ready(undercount, log),
    )
    .await;
    assert!(execution.checks.catalog_state_correlated);
    assert!(!execution.checks.shared_object_evidence_complete);
    assert!(execution.cleanup.passed());
    assert!(!execution.passed());

    let log = OperationLog::default();
    let execution = run_engine_workflow(
        &plan,
        FakeRunner::new(fixture.process(&plan), log.clone()),
        FakeCatalogConnector::ready_as(
            fixture.catalog(),
            ComponentId::from("polaris"),
            log.clone(),
        ),
        FakeObjectStoreConnector::ready(fixture.object_audit(), log),
    )
    .await;
    assert!(!execution.checks.stock_rest_catalog_ready);
    assert!(!execution.passed());
}

#[tokio::test]
async fn complete_semantics_cannot_override_an_untrusted_process_terminal() {
    let plan = plan("terminal01");
    let fixture = PassingFixture::new(&plan);
    let log = OperationLog::default();
    let mut process = fixture.process(&plan);
    let capture = process.capture.as_mut().unwrap();
    assert!(matches!(capture.events.pop(), Some(EngineEvent::Completed)));
    capture.failure = Some(EngineProtocolFailure {
        kind: EngineProtocolFailureKind::MissingTerminal,
    });
    process.outcome = SparkProcessOutcome::ProtocolRejected {
        kind: EngineProtocolFailureKind::MissingTerminal,
    };
    process.exit_code = None;

    let execution = run_engine_workflow(
        &plan,
        FakeRunner::new(process, log.clone()),
        FakeCatalogConnector::ready(fixture.catalog(), log.clone()),
        FakeObjectStoreConnector::ready(fixture.object_audit(), log),
    )
    .await;

    assert!(execution.checks.all_passed(), "{execution:#?}");
    assert_eq!(execution.classification, EngineBehaviorClassification::Fail);
    assert!(!execution.process.passed());
    assert!(!execution.passed());
}

#[tokio::test]
async fn runtime_artifact_drift_fails_closed_after_other_authorities_agree() {
    let plan = plan("artifact01");
    let fixture = PassingFixture::new(&plan);
    let log = OperationLog::default();
    let mut process = fixture.process(&plan);
    assert!(process.runtime.artifacts.pop().is_some());

    let execution = run_engine_workflow(
        &plan,
        FakeRunner::new(process, log.clone()),
        FakeCatalogConnector::ready(fixture.catalog(), log.clone()),
        FakeObjectStoreConnector::ready(fixture.object_audit(), log),
    )
    .await;

    assert!(!execution.checks.engine_runtime_pinned);
    assert!(execution.checks.stock_rest_catalog_ready);
    assert!(execution.checks.catalog_state_correlated);
    assert!(execution.checks.shared_object_evidence_complete);
    assert!(execution.checks.fixture_clean);
    assert_eq!(execution.classification, EngineBehaviorClassification::Fail);
    assert!(!execution.passed());
}

#[tokio::test]
async fn cleanup_failures_are_retained_without_short_circuiting_later_attempts() {
    let plan = plan("cleanup01");
    let fixture = PassingFixture::new(&plan);
    let log = OperationLog::default();
    let mut catalog = fixture.catalog();
    catalog.drop_table = Err(catalog_failure(EngineCatalogFailureKind::Transport));
    catalog.table_presence = Ok(present());
    catalog.drop_namespace = Err(catalog_failure(EngineCatalogFailureKind::UnexpectedHttp));
    catalog.namespace_presence = Ok(present());
    let execution = run_engine_workflow(
        &plan,
        FakeRunner::new(fixture.process(&plan), log.clone()),
        FakeCatalogConnector::ready(catalog, log.clone()),
        FakeObjectStoreConnector::ready(fixture.object_audit(), log.clone()),
    )
    .await;

    assert_eq!(
        &log.entries()[5..],
        [
            "drop-table",
            "table-presence",
            "drop-namespace",
            "namespace-presence",
        ]
    );
    assert!(matches!(
        execution.cleanup.drop_table_without_purge,
        EngineOperationEvidence::Failed { .. }
    ));
    assert!(matches!(
        execution.cleanup.drop_namespace,
        EngineOperationEvidence::Failed { .. }
    ));
    assert!(!execution.checks.fixture_clean);
    assert!(!execution.passed());
}

#[tokio::test]
async fn owned_fixture_with_failed_connection_records_the_cleanup_impasse() {
    let plan = plan("connect01");
    let fixture = PassingFixture::new(&plan);
    let log = OperationLog::default();
    let execution = run_engine_workflow(
        &plan,
        FakeRunner::new(fixture.process(&plan), log.clone()),
        FakeCatalogConnector::failed(log.clone()),
        FakeObjectStoreConnector::ready(fixture.object_audit(), log.clone()),
    )
    .await;

    assert_eq!(log.entries(), ["engine", "catalog-connect"]);
    assert!(matches!(
        execution.catalog_connection,
        EngineCatalogConnectionEvidence::Failed { .. }
    ));
    assert!(matches!(
        execution.cleanup.drop_table_without_purge,
        EngineOperationEvidence::Skipped { .. }
    ));
    assert!(!execution.passed());
}

#[derive(Clone, Default)]
struct OperationLog(Arc<Mutex<Vec<&'static str>>>);

impl OperationLog {
    fn push(&self, operation: &'static str) {
        self.0.lock().unwrap().push(operation);
    }

    fn entries(&self) -> Vec<&'static str> {
        self.0.lock().unwrap().clone()
    }
}

#[derive(Clone)]
struct FakeRunner {
    execution: SparkProcessExecution,
    log: OperationLog,
}

impl FakeRunner {
    fn new(execution: SparkProcessExecution, log: OperationLog) -> Self {
        Self { execution, log }
    }
}

impl EngineRunner for FakeRunner {
    async fn execute(&self, _plan: &InteroperabilityPlan) -> SparkProcessExecution {
        self.log.push("engine");
        self.execution.clone()
    }
}

#[derive(Clone)]
struct FakeCatalogConnector {
    catalog: Option<FakeCatalog>,
    negotiation_catalog: Option<ComponentId>,
    log: OperationLog,
}

impl FakeCatalogConnector {
    fn ready(mut catalog: FakeCatalog, log: OperationLog) -> Self {
        catalog.log = log.clone();
        Self {
            catalog: Some(catalog),
            negotiation_catalog: None,
            log,
        }
    }

    fn ready_as(
        mut catalog: FakeCatalog,
        negotiation_catalog: ComponentId,
        log: OperationLog,
    ) -> Self {
        catalog.log = log.clone();
        Self {
            catalog: Some(catalog),
            negotiation_catalog: Some(negotiation_catalog),
            log,
        }
    }

    fn failed(log: OperationLog) -> Self {
        Self {
            catalog: None,
            negotiation_catalog: None,
            log,
        }
    }
}

impl EngineCatalogConnector for FakeCatalogConnector {
    type Catalog = FakeCatalog;

    async fn connect(&self, plan: &InteroperabilityPlan) -> EngineCatalogConnection<Self::Catalog> {
        self.log.push("catalog-connect");
        match &self.catalog {
            Some(catalog) => {
                let mut negotiation = negotiation(plan);
                if let Some(catalog_id) = &self.negotiation_catalog {
                    negotiation.adapter.catalog = catalog_id.clone();
                }
                EngineCatalogConnection::Ready {
                    negotiation,
                    catalog: catalog.clone(),
                }
            }
            None => EngineCatalogConnection::Failed {
                negotiation: None,
                failure: EngineCatalogConnectionFailure {
                    kind: EngineCatalogConnectionFailureKind::Setup,
                },
            },
        }
    }
}

#[derive(Clone)]
struct FakeCatalog {
    load: Result<EngineTableLoad, EngineCatalogFailure>,
    drop_table: Result<EngineCleanupReceipt, EngineCatalogFailure>,
    table_presence: Result<EngineResourcePresence, EngineCatalogFailure>,
    drop_namespace: Result<EngineCleanupReceipt, EngineCatalogFailure>,
    namespace_presence: Result<EngineResourcePresence, EngineCatalogFailure>,
    log: OperationLog,
}

impl FakeCatalog {
    fn passing_absent(log: OperationLog) -> Self {
        Self {
            load: Ok(EngineTableLoad::Absent {
                http_status: 404,
                response_bytes: 0,
            }),
            drop_table: Ok(cleanup_receipt()),
            table_presence: Ok(absent()),
            drop_namespace: Ok(cleanup_receipt()),
            namespace_presence: Ok(absent()),
            log,
        }
    }
}

impl EngineCatalog for FakeCatalog {
    async fn load_table(&self) -> Result<EngineTableLoad, EngineCatalogFailure> {
        self.log.push("catalog-load");
        self.load.clone()
    }

    async fn drop_table_without_purge(&self) -> Result<EngineCleanupReceipt, EngineCatalogFailure> {
        self.log.push("drop-table");
        self.drop_table
    }

    async fn drop_namespace(&self) -> Result<EngineCleanupReceipt, EngineCatalogFailure> {
        self.log.push("drop-namespace");
        self.drop_namespace
    }

    async fn table_presence(&self) -> Result<EngineResourcePresence, EngineCatalogFailure> {
        self.log.push("table-presence");
        self.table_presence
    }

    async fn namespace_presence(&self) -> Result<EngineResourcePresence, EngineCatalogFailure> {
        self.log.push("namespace-presence");
        self.namespace_presence
    }
}

#[derive(Clone)]
struct FakeObjectStoreConnector {
    audit: TableObjectAuditSnapshot,
    log: OperationLog,
}

impl FakeObjectStoreConnector {
    fn ready(audit: TableObjectAuditSnapshot, log: OperationLog) -> Self {
        Self { audit, log }
    }
}

impl EngineObjectStoreConnector for FakeObjectStoreConnector {
    type Store = FakeObjectStore;

    fn connect(&self, _plan: &InteroperabilityPlan) -> Result<Self::Store, ObjectStoreFailure> {
        self.log.push("object-connect");
        Ok(FakeObjectStore {
            audit: self.audit.clone(),
            log: self.log.clone(),
        })
    }
}

#[derive(Clone)]
struct FakeObjectStore {
    audit: TableObjectAuditSnapshot,
    log: OperationLog,
}

impl TableObjectStore for FakeObjectStore {
    async fn audit_table(
        &self,
        _root: &TableRoot,
        _metadata_location: &str,
    ) -> Result<TableObjectAuditSnapshot, ObjectStoreFailure> {
        self.log.push("object-audit");
        Ok(self.audit.clone())
    }
}

struct PassingFixture {
    initial: EngineTableObservation,
    evolved: EngineTableObservation,
    final_table: EngineTableObservation,
}

impl PassingFixture {
    fn new(plan: &InteroperabilityPlan) -> Self {
        let location = plan
            .spark()
            .fixture
            .requested_location
            .clone()
            .unwrap_or_else(|| "s3://warehouse/engine-fixture/events".to_owned());
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
        evolved_fields.push(EngineFieldObservation {
            id: 4,
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
        Self {
            initial: table("v1", initial_fields, 0, 3),
            evolved: table("v3", evolved_fields.clone(), 1, 4),
            final_table: table("v4", evolved_fields, 2, 4),
        }
    }

    fn process(&self, plan: &InteroperabilityPlan) -> SparkProcessExecution {
        let events = vec![
            EngineEvent::RuntimeReady {
                runtime: runtime_event(),
            },
            EngineEvent::CatalogReady,
            EngineEvent::FixturePreflight { absent: true },
            EngineEvent::NamespaceReady {
                listed_exactly: true,
            },
            EngineEvent::TableReady {
                table: self.initial.clone(),
            },
            EngineEvent::InitialAppended { snapshots: 1 },
            EngineEvent::InitialRead {
                read: canonical_initial(plan),
            },
            EngineEvent::SchemaEvolved {
                table: self.evolved.clone(),
            },
            EngineEvent::EvolvedAppended { snapshots: 2 },
            EngineEvent::EvolvedRead {
                read: canonical_evolved(plan),
            },
            EngineEvent::FinalTable {
                table: self.final_table.clone(),
            },
            EngineEvent::Completed,
        ];
        process(
            plan,
            SparkProcessOutcome::Completed,
            Some(capture(events, None)),
        )
    }

    fn catalog(&self) -> FakeCatalog {
        FakeCatalog {
            load: Ok(EngineTableLoad::Present {
                http_status: 200,
                response_bytes: 1_024,
                state: EngineCatalogTable {
                    current_schema_id: 1,
                    table: self.final_table.clone(),
                },
            }),
            drop_table: Ok(cleanup_receipt()),
            table_presence: Ok(absent()),
            drop_namespace: Ok(cleanup_receipt()),
            namespace_presence: Ok(absent()),
            log: OperationLog::default(),
        }
    }

    fn object_audit(&self) -> TableObjectAuditSnapshot {
        TableObjectAuditSnapshot {
            table_root: self.final_table.location.clone(),
            metadata_objects: 4,
            metadata_bytes: 4_096,
            parquet_objects: 2,
            parquet_bytes: 2_048,
            referenced_metadata_location: self.final_table.metadata_location.clone(),
            referenced_metadata_exists: true,
        }
    }
}

fn process(
    plan: &InteroperabilityPlan,
    outcome: SparkProcessOutcome,
    capture: Option<EngineEventCapture>,
) -> SparkProcessExecution {
    let exit_code = match &outcome {
        SparkProcessOutcome::Completed => Some(0),
        SparkProcessOutcome::FixtureCollision => Some(3),
        SparkProcessOutcome::EngineFailed { .. } => Some(2),
        SparkProcessOutcome::RuntimeRejected
        | SparkProcessOutcome::CredentialRejected { .. }
        | SparkProcessOutcome::PreparationFailed { .. }
        | SparkProcessOutcome::SpawnFailed
        | SparkProcessOutcome::TimedOut
        | SparkProcessOutcome::StdoutFailed
        | SparkProcessOutcome::WaitFailed
        | SparkProcessOutcome::ProtocolRejected { .. }
        | SparkProcessOutcome::ExitProtocolMismatch => None,
    };
    SparkProcessExecution {
        runtime: passing_runtime_verification(plan),
        outcome,
        capture,
        exit_code,
        process_elapsed_micros: Some(1_000),
    }
}

fn collision_process(plan: &InteroperabilityPlan) -> SparkProcessExecution {
    process(
        plan,
        SparkProcessOutcome::FixtureCollision,
        Some(capture(
            vec![
                EngineEvent::RuntimeReady {
                    runtime: runtime_event(),
                },
                EngineEvent::CatalogReady,
                EngineEvent::FixturePreflight { absent: false },
            ],
            None,
        )),
    )
}

fn runtime_rejected_process(plan: &InteroperabilityPlan) -> SparkProcessExecution {
    let mut execution = process(plan, SparkProcessOutcome::RuntimeRejected, None);
    execution.runtime.platform.operating_system_matches = false;
    execution
}

fn engine_failed_process(plan: &InteroperabilityPlan) -> SparkProcessExecution {
    process(
        plan,
        SparkProcessOutcome::EngineFailed {
            stage: EngineStage::CreateNamespace,
            category: EngineFailureCategory::Catalog,
        },
        Some(capture(
            vec![
                EngineEvent::RuntimeReady {
                    runtime: runtime_event(),
                },
                EngineEvent::CatalogReady,
                EngineEvent::FixturePreflight { absent: true },
                EngineEvent::Failed {
                    stage: EngineStage::CreateNamespace,
                    category: EngineFailureCategory::Catalog,
                },
            ],
            None,
        )),
    )
}

fn protocol_failed_process(plan: &InteroperabilityPlan) -> SparkProcessExecution {
    process(
        plan,
        SparkProcessOutcome::ProtocolRejected {
            kind: EngineProtocolFailureKind::MalformedEvent,
        },
        Some(capture(
            vec![
                EngineEvent::RuntimeReady {
                    runtime: runtime_event(),
                },
                EngineEvent::CatalogReady,
                EngineEvent::FixturePreflight { absent: true },
            ],
            Some(EngineProtocolFailure {
                kind: EngineProtocolFailureKind::MalformedEvent,
            }),
        )),
    )
}

fn capture(events: Vec<EngineEvent>, failure: Option<EngineProtocolFailure>) -> EngineEventCapture {
    EngineEventCapture {
        events,
        failure,
        stdout_bytes_observed: 512,
    }
}

fn passing_runtime_verification(plan: &InteroperabilityPlan) -> RuntimeVerification {
    RuntimeVerification {
        platform: RuntimePlatformObservation {
            expected_operating_system: plan.runtime_platform().operating_system.clone(),
            observed_operating_system: "linux".to_owned(),
            operating_system_matches: true,
            expected_architecture: plan.runtime_platform().architecture.clone(),
            observed_architecture: "arm64".to_owned(),
            architecture_matches: true,
        },
        artifacts: plan
            .runtime_artifacts()
            .iter()
            .map(|artifact| RuntimeArtifactObservation {
                location: artifact.location.clone(),
                media_type: artifact.media_type.clone(),
                components: artifact.components.clone(),
                expected_bytes: artifact.bytes,
                expected_sha256: artifact.sha256.clone(),
                outcome: RuntimeArtifactOutcome::Match {
                    observed_bytes: artifact.bytes,
                    observed_sha256: artifact.sha256.clone(),
                },
            })
            .collect(),
    }
}

fn runtime_event() -> EngineRuntimeObservation {
    EngineRuntimeObservation {
        spark_version: "4.1.3".to_owned(),
        scala_version: "2.13.17".to_owned(),
        java_version: "21.0.11".to_owned(),
        operating_system: "Linux".to_owned(),
        architecture: "aarch64".to_owned(),
    }
}

fn canonical_initial(plan: &InteroperabilityPlan) -> RowReadObservation {
    let expected = &plan.spark().scenario.canonical_reads.initial;
    RowReadObservation {
        rows: expected.rows,
        bytes: expected.bytes,
        sha256: expected.sha256.clone(),
    }
}

fn canonical_evolved(plan: &InteroperabilityPlan) -> RowReadObservation {
    let expected = &plan.spark().scenario.canonical_reads.after_evolution;
    RowReadObservation {
        rows: expected.rows,
        bytes: expected.bytes,
        sha256: expected.sha256.clone(),
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

fn present() -> EngineResourcePresence {
    EngineResourcePresence::Present {
        http_status: 200,
        response_bytes: 0,
    }
}

fn catalog_failure(kind: EngineCatalogFailureKind) -> EngineCatalogFailure {
    EngineCatalogFailure {
        kind,
        http_status: None,
    }
}

fn unused_audit() -> TableObjectAuditSnapshot {
    TableObjectAuditSnapshot {
        table_root: "s3://warehouse/unused".to_owned(),
        metadata_objects: 0,
        metadata_bytes: 0,
        parquet_objects: 0,
        parquet_bytes: 0,
        referenced_metadata_location: "s3://warehouse/unused/metadata/v1.metadata.json".to_owned(),
        referenced_metadata_exists: false,
    }
}

fn negotiation(plan: &InteroperabilityPlan) -> EngineCatalogNegotiationEvidence {
    let evidence: CatalogNegotiationEvidence = serde_json::from_value(json!({
        "adapter": {
            "catalog": "lakecat",
            "name": plan.catalog().name,
            "version": plan.catalog().version,
            "protocol": "iceberg-rest-v1",
            "request_handling": {"kind": "protocol-native"}
        },
        "authentication": {"mode": "anonymous", "outcome": "ready"},
        "config": {
            "request": {
                "method": "GET",
                "url": "http://lakecat/catalog/v1/config",
                "headers": {"accept": "application/json"}
            },
            "prefix": {"mode": "unprefixed"},
            "namespace_separator": {"mode": "default", "encoded": "%1F"}
        },
        "redactions": []
    }))
    .unwrap();
    evidence.try_into().unwrap()
}

fn plan(fixture: &str) -> InteroperabilityPlan {
    let (profile, scenario) = contracts();
    InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        fixture,
    )
    .unwrap()
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
