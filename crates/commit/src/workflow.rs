use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Barrier};
use tokio::task::JoinSet;

use crate::model::{
    AcceptedRequests, FinalStateAttribution, MetadataGrowthEvidence, ModelError, PhaseEvidence,
    RequestCounts, RequestIdentity, RequestLedger,
};
use crate::policy::{ContentionFixture, RoundKind, WorkloadPolicy};
use crate::protocol::{
    CatalogFailure, CatalogPort, MutationReceipt, PresenceObservation, ResourcePresence,
    TableSnapshot,
};
use crate::store::{MetadataStore, ObjectAuditSnapshot, ObjectStoreFailure, TableRoot};

const SKIPPED: &str = "a required predecessor did not complete";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoundWorkload {
    pub warmup_commits: u64,
    pub sequential_commits: u64,
    pub concurrent_writers: u32,
    pub concurrent_duration_ms: u64,
    pub commit_property: String,
}

impl RoundWorkload {
    pub fn new(
        warmup_commits: u64,
        sequential_commits: u64,
        concurrent_writers: u32,
        concurrent_duration_ms: u64,
        commit_property: impl Into<String>,
    ) -> Result<Self, WorkflowError> {
        let workload = Self {
            warmup_commits,
            sequential_commits,
            concurrent_writers,
            concurrent_duration_ms,
            commit_property: commit_property.into(),
        };
        if workload.warmup_commits == 0
            || workload.sequential_commits == 0
            || workload.concurrent_writers == 0
            || workload.concurrent_duration_ms == 0
            || workload.commit_property.trim().is_empty()
        {
            return Err(WorkflowError::InvalidConfiguration(
                "round workload values must all be positive and the property must be nonempty"
                    .to_owned(),
            ));
        }
        Ok(workload)
    }
}

impl TryFrom<&WorkloadPolicy> for RoundWorkload {
    type Error = WorkflowError;

    fn try_from(policy: &WorkloadPolicy) -> Result<Self, Self::Error> {
        Self::new(
            policy.warmup_commits,
            policy.sequential_commits,
            policy.concurrent_writers,
            policy.concurrent_duration_ms,
            policy.commit_property.clone(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoundExecutionConfig {
    catalog: String,
    repetition: u32,
    kind: RoundKind,
    fixture: ContentionFixture,
    workload: RoundWorkload,
    object_store_bucket: String,
}

impl RoundExecutionConfig {
    pub fn new(
        catalog: impl Into<String>,
        repetition: u32,
        kind: RoundKind,
        fixture: ContentionFixture,
        workload: RoundWorkload,
        object_store_bucket: impl Into<String>,
    ) -> Result<Self, WorkflowError> {
        let config = Self {
            catalog: catalog.into(),
            repetition,
            kind,
            fixture,
            workload,
            object_store_bucket: object_store_bucket.into(),
        };
        if config.catalog.trim().is_empty()
            || config.repetition == 0
            || config.object_store_bucket.trim().is_empty()
        {
            return Err(WorkflowError::InvalidConfiguration(
                "catalog, repetition, and object-store bucket must identify the round".to_owned(),
            ));
        }
        Ok(config)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoundDescriptor {
    pub catalog: String,
    pub repetition: u32,
    pub kind: RoundKind,
    pub fixture: ContentionFixture,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RoundClassification {
    Pass,
    Fail,
    FixtureCollision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "kebab-case", deny_unknown_fields)]
pub enum StepFailure {
    Catalog { failure: CatalogFailure },
    ObjectStore { failure: ObjectStoreFailure },
    Harness { detail: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum OperationEvidence<T> {
    Succeeded { output: T },
    Failed { failure: StepFailure },
    NotAttempted { reason: String },
}

impl<T> OperationEvidence<T> {
    fn succeeded(output: T) -> Self {
        Self::Succeeded { output }
    }

    fn catalog_failure(failure: CatalogFailure) -> Self {
        Self::Failed {
            failure: StepFailure::Catalog { failure },
        }
    }

    fn object_store_failure(failure: ObjectStoreFailure) -> Self {
        Self::Failed {
            failure: StepFailure::ObjectStore { failure },
        }
    }

    fn harness_failure(error: impl Display) -> Self {
        Self::Failed {
            failure: StepFailure::Harness {
                detail: error.to_string(),
            },
        }
    }

    fn skipped() -> Self {
        Self::NotAttempted {
            reason: SKIPPED.to_owned(),
        }
    }

    fn output(&self) -> Option<&T> {
        match self {
            Self::Succeeded { output } => Some(output),
            Self::Failed { .. } | Self::NotAttempted { .. } => None,
        }
    }

    fn passed(&self) -> bool {
        matches!(self, Self::Succeeded { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetupTableEvidence {
    pub format_version: u8,
    pub table_uuid: String,
    pub location: String,
    pub metadata_location: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_location: Option<String>,
}

impl SetupTableEvidence {
    fn from_snapshot(snapshot: &TableSnapshot, requested_location: Option<&str>) -> Self {
        Self {
            format_version: snapshot.format_version,
            table_uuid: snapshot.table_uuid.clone(),
            location: snapshot.location.clone(),
            metadata_location: snapshot.metadata_location.clone(),
            requested_location: requested_location.map(str::to_owned),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalTableEvidence {
    pub format_version: u8,
    pub table_uuid: String,
    pub metadata_location: String,
    pub table_uuid_matches_setup: bool,
    pub table_location_matches_setup: bool,
    pub attribution: FinalStateAttribution,
}

impl FinalTableEvidence {
    fn passed(&self) -> bool {
        self.format_version == 2
            && self.table_uuid_matches_setup
            && self.table_location_matches_setup
            && self.attribution.passed()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupEvidence {
    pub drop_table_without_purge: OperationEvidence<MutationReceipt>,
    pub verify_table_absent: OperationEvidence<PresenceObservation>,
    pub drop_namespace: OperationEvidence<MutationReceipt>,
    pub verify_namespace_absent: OperationEvidence<PresenceObservation>,
}

impl CleanupEvidence {
    fn skipped() -> Self {
        Self {
            drop_table_without_purge: OperationEvidence::skipped(),
            verify_table_absent: OperationEvidence::skipped(),
            drop_namespace: OperationEvidence::skipped(),
            verify_namespace_absent: OperationEvidence::skipped(),
        }
    }

    fn passed(&self) -> bool {
        self.drop_table_without_purge.passed()
            && self
                .verify_table_absent
                .output()
                .is_some_and(|observation| observation.presence == ResourcePresence::Absent)
            && self.drop_namespace.passed()
            && self
                .verify_namespace_absent
                .output()
                .is_some_and(|observation| observation.presence == ResourcePresence::Absent)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoundChecks {
    pub fixture_isolated: bool,
    pub setup_succeeded: bool,
    pub warmup_accounted: bool,
    pub sequential_accounted: bool,
    pub sequential_latency_complete: bool,
    pub all_requests_accounted: bool,
    pub zero_request_errors: bool,
    pub concurrent_progress: bool,
    pub final_state_accounted: bool,
    pub metadata_persisted: bool,
    pub fixture_clean: bool,
}

impl RoundChecks {
    fn pending() -> Self {
        Self {
            fixture_isolated: false,
            setup_succeeded: false,
            warmup_accounted: false,
            sequential_accounted: false,
            sequential_latency_complete: false,
            all_requests_accounted: false,
            zero_request_errors: false,
            concurrent_progress: false,
            final_state_accounted: false,
            metadata_persisted: false,
            fixture_clean: false,
        }
    }

    #[must_use]
    pub fn all_passed(&self) -> bool {
        self.fixture_isolated
            && self.setup_succeeded
            && self.warmup_accounted
            && self.sequential_accounted
            && self.sequential_latency_complete
            && self.all_requests_accounted
            && self.zero_request_errors
            && self.concurrent_progress
            && self.final_state_accounted
            && self.metadata_persisted
            && self.fixture_clean
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoundExecution {
    pub descriptor: RoundDescriptor,
    pub workload: RoundWorkload,
    pub classification: RoundClassification,
    pub preflight: OperationEvidence<PresenceObservation>,
    pub create_namespace: OperationEvidence<MutationReceipt>,
    pub create_table: OperationEvidence<SetupTableEvidence>,
    pub baseline_object_audit: OperationEvidence<ObjectAuditSnapshot>,
    pub warmup: OperationEvidence<PhaseEvidence>,
    pub sequential: OperationEvidence<PhaseEvidence>,
    pub concurrent: OperationEvidence<PhaseEvidence>,
    pub final_table: OperationEvidence<FinalTableEvidence>,
    pub final_object_audit: OperationEvidence<ObjectAuditSnapshot>,
    pub metadata_growth: OperationEvidence<MetadataGrowthEvidence>,
    pub cleanup: CleanupEvidence,
    pub checks: RoundChecks,
}

impl RoundExecution {
    fn pending(config: &RoundExecutionConfig) -> Self {
        Self {
            descriptor: RoundDescriptor {
                catalog: config.catalog.clone(),
                repetition: config.repetition,
                kind: config.kind,
                fixture: config.fixture.clone(),
            },
            workload: config.workload.clone(),
            classification: RoundClassification::Fail,
            preflight: OperationEvidence::skipped(),
            create_namespace: OperationEvidence::skipped(),
            create_table: OperationEvidence::skipped(),
            baseline_object_audit: OperationEvidence::skipped(),
            warmup: OperationEvidence::skipped(),
            sequential: OperationEvidence::skipped(),
            concurrent: OperationEvidence::skipped(),
            final_table: OperationEvidence::skipped(),
            final_object_audit: OperationEvidence::skipped(),
            metadata_growth: OperationEvidence::skipped(),
            cleanup: CleanupEvidence::skipped(),
            checks: RoundChecks::pending(),
        }
    }

    fn finalize(&mut self, fixture_collision: bool) {
        self.checks = evaluate_checks(self);
        self.classification = if fixture_collision {
            RoundClassification::FixtureCollision
        } else if self.checks.all_passed() {
            RoundClassification::Pass
        } else {
            RoundClassification::Fail
        };
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.classification == RoundClassification::Pass
    }
}

/// Execute one isolated catalog round. Catalog and assertion failures become
/// evidence; `Err` is reserved for invalid invocation or an internal invariant.
pub async fn run_contention_round<P, S>(
    catalog: P,
    store: S,
    config: RoundExecutionConfig,
) -> Result<RoundExecution, WorkflowError>
where
    P: CatalogPort,
    S: MetadataStore,
{
    let mut execution = RoundExecution::pending(&config);
    match catalog.namespace_presence().await {
        Ok(observation) => {
            let collision = observation.presence == ResourcePresence::Present;
            execution.preflight = OperationEvidence::succeeded(observation);
            if collision {
                execution.finalize(true);
                return Ok(execution);
            }
        }
        Err(failure) => {
            execution.preflight = OperationEvidence::catalog_failure(failure);
            execution.finalize(false);
            return Ok(execution);
        }
    }

    match catalog.create_namespace().await {
        Ok(receipt) => {
            execution.create_namespace = OperationEvidence::succeeded(receipt);
        }
        Err(failure) => {
            execution.create_namespace = OperationEvidence::catalog_failure(failure);
            execution.cleanup = cleanup(&catalog).await;
            execution.finalize(false);
            return Ok(execution);
        }
    }

    let created = match catalog.create_table().await {
        Ok(snapshot) => {
            execution.create_table = OperationEvidence::succeeded(
                SetupTableEvidence::from_snapshot(&snapshot, catalog.requested_location()),
            );
            snapshot
        }
        Err(failure) => {
            execution.create_table = OperationEvidence::catalog_failure(failure);
            execution.cleanup = cleanup(&catalog).await;
            execution.finalize(false);
            return Ok(execution);
        }
    };
    let table_root = match TableRoot::from_snapshot(&created, &config.object_store_bucket) {
        Ok(root) => root,
        Err(failure) => {
            execution.baseline_object_audit = OperationEvidence::object_store_failure(failure);
            execution.cleanup = cleanup(&catalog).await;
            execution.finalize(false);
            return Ok(execution);
        }
    };
    let baseline = match store.audit(&table_root, &created.metadata_location).await {
        Ok(audit) => {
            let usable = audit.metadata_objects > 0 && audit.referenced_metadata_exists;
            execution.baseline_object_audit = OperationEvidence::succeeded(audit.clone());
            if !usable {
                execution.cleanup = cleanup(&catalog).await;
                execution.finalize(false);
                return Ok(execution);
            }
            audit
        }
        Err(failure) => {
            execution.baseline_object_audit = OperationEvidence::object_store_failure(failure);
            execution.cleanup = cleanup(&catalog).await;
            execution.finalize(false);
            return Ok(execution);
        }
    };

    let mut accepted_requests = AcceptedRequests::default();
    let warmup = run_serial_phase(
        &catalog,
        &config,
        "warmup",
        config.workload.warmup_commits,
        &created.table_uuid,
    )
    .await;
    if !record_phase(&mut execution.warmup, &mut accepted_requests, warmup) {
        execution.cleanup = cleanup(&catalog).await;
        execution.finalize(false);
        return Ok(execution);
    }

    let sequential = run_serial_phase(
        &catalog,
        &config,
        "sequential",
        config.workload.sequential_commits,
        &created.table_uuid,
    )
    .await;
    if !record_phase(
        &mut execution.sequential,
        &mut accepted_requests,
        sequential,
    ) {
        execution.cleanup = cleanup(&catalog).await;
        execution.finalize(false);
        return Ok(execution);
    }

    let concurrent = run_concurrent_phase(catalog.clone(), &config, &created.table_uuid).await;
    if !record_phase(
        &mut execution.concurrent,
        &mut accepted_requests,
        concurrent,
    ) {
        execution.cleanup = cleanup(&catalog).await;
        execution.finalize(false);
        return Ok(execution);
    }

    let final_snapshot = match catalog.load_table().await {
        Ok(snapshot) => snapshot,
        Err(failure) => {
            execution.final_table = OperationEvidence::catalog_failure(failure);
            execution.cleanup = cleanup(&catalog).await;
            execution.finalize(false);
            return Ok(execution);
        }
    };
    let final_property = final_snapshot
        .properties
        .get(&config.workload.commit_property)
        .map(String::as_str);
    let attribution = FinalStateAttribution::evaluate(
        final_property,
        &accepted_requests,
        table_root.contains_metadata_location(&final_snapshot.metadata_location),
    );
    execution.final_table = OperationEvidence::succeeded(FinalTableEvidence {
        format_version: final_snapshot.format_version,
        table_uuid: final_snapshot.table_uuid.clone(),
        metadata_location: final_snapshot.metadata_location.clone(),
        table_uuid_matches_setup: final_snapshot.table_uuid == created.table_uuid,
        table_location_matches_setup: final_snapshot.location == created.location,
        attribution,
    });

    match store
        .audit(&table_root, &final_snapshot.metadata_location)
        .await
    {
        Ok(audit) => execution.final_object_audit = OperationEvidence::succeeded(audit),
        Err(failure) => {
            execution.final_object_audit = OperationEvidence::object_store_failure(failure)
        }
    }
    let phase_counts = [
        execution.warmup.output().map(|phase| phase.counts),
        execution.sequential.output().map(|phase| phase.counts),
        execution.concurrent.output().map(|phase| phase.counts),
    ];
    if phase_counts.iter().all(Option::is_some) {
        if let Some(final_audit) = execution.final_object_audit.output() {
            match MetadataGrowthEvidence::evaluate(
                baseline.metadata_objects,
                final_audit.metadata_objects,
                phase_counts.into_iter().flatten(),
            ) {
                Ok(growth) => execution.metadata_growth = OperationEvidence::succeeded(growth),
                Err(error) => execution.metadata_growth = OperationEvidence::harness_failure(error),
            }
        }
    }

    execution.cleanup = cleanup(&catalog).await;
    execution.finalize(false);
    Ok(execution)
}

fn record_phase(
    evidence: &mut OperationEvidence<PhaseEvidence>,
    accepted: &mut AcceptedRequests,
    result: Result<(PhaseEvidence, AcceptedRequests), WorkflowError>,
) -> bool {
    match result {
        Ok((phase, phase_accepted)) => match accepted.try_extend(phase_accepted) {
            Ok(()) => {
                *evidence = OperationEvidence::succeeded(phase);
                true
            }
            Err(error) => {
                *evidence = OperationEvidence::harness_failure(error);
                false
            }
        },
        Err(error) => {
            *evidence = OperationEvidence::harness_failure(error);
            false
        }
    }
}

async fn run_serial_phase<P: CatalogPort>(
    catalog: &P,
    config: &RoundExecutionConfig,
    phase: &str,
    attempts: u64,
    table_uuid: &str,
) -> Result<(PhaseEvidence, AcceptedRequests), WorkflowError> {
    let started = Instant::now();
    let mut ledger = RequestLedger::default();
    for sequence in 0..attempts {
        let identity = request_identity(config, phase, None, sequence)?;
        let request_started = Instant::now();
        let outcome = catalog
            .commit(table_uuid, &config.workload.commit_property, &identity)
            .await;
        ledger.record(&identity, request_started.elapsed(), outcome)?;
    }
    ledger
        .finish(started.elapsed())
        .map_err(WorkflowError::from)
}

async fn run_concurrent_phase<P: CatalogPort>(
    catalog: P,
    config: &RoundExecutionConfig,
    table_uuid: &str,
) -> Result<(PhaseEvidence, AcceptedRequests), WorkflowError> {
    let writer_count = usize::try_from(config.workload.concurrent_writers).map_err(|_| {
        WorkflowError::InvalidConfiguration("writer count does not fit this runner".to_owned())
    })?;
    let participant_count = writer_count.checked_add(1).ok_or_else(|| {
        WorkflowError::InvalidConfiguration("writer participant count overflowed usize".to_owned())
    })?;
    let barrier = Arc::new(Barrier::new(participant_count));
    let (window_sender, window_receiver) = watch::channel::<Option<WriterWindow>>(None);
    let table_uuid = Arc::<str>::from(table_uuid);
    let identity_root = Arc::<str>::from(format!(
        "{}/{}/{}/concurrent",
        config.catalog, config.fixture.id, config.repetition
    ));
    let property = Arc::<str>::from(config.workload.commit_property.clone());
    let mut writers = JoinSet::new();
    for writer in 0..config.workload.concurrent_writers {
        let catalog = catalog.clone();
        let barrier = Arc::clone(&barrier);
        let mut window_receiver = window_receiver.clone();
        let table_uuid = Arc::clone(&table_uuid);
        let identity_root = Arc::clone(&identity_root);
        let property = Arc::clone(&property);
        writers.spawn(async move {
            barrier.wait().await;
            window_receiver
                .changed()
                .await
                .map_err(|_| WorkflowError::WriterStartChannelClosed)?;
            let window = (*window_receiver.borrow_and_update())
                .ok_or(WorkflowError::WriterStartChannelClosed)?;
            let mut ledger = RequestLedger::default();
            let mut sequence = 0_u64;
            loop {
                let identity =
                    RequestIdentity::new(format!("{identity_root}/{writer}/{sequence}"))?;
                let request_started = Instant::now();
                if request_started >= window.deadline {
                    break;
                }
                let outcome = catalog.commit(&table_uuid, &property, &identity).await;
                ledger.record(&identity, request_started.elapsed(), outcome)?;
                sequence = sequence
                    .checked_add(1)
                    .ok_or(WorkflowError::SequenceOverflow)?;
            }
            Ok::<_, WorkflowError>(ledger)
        });
    }
    drop(window_receiver);
    barrier.wait().await;
    let started = Instant::now();
    let deadline = started
        .checked_add(Duration::from_millis(
            config.workload.concurrent_duration_ms,
        ))
        .ok_or(WorkflowError::DeadlineOverflow)?;
    window_sender
        .send(Some(WriterWindow { deadline }))
        .map_err(|_| WorkflowError::WriterStartChannelClosed)?;
    drop(window_sender);

    let mut merged = RequestLedger::default();
    let mut first_error = None;
    while let Some(result) = writers.join_next().await {
        match result {
            Ok(Ok(ledger)) => {
                if let Err(error) = merged.try_merge(ledger) {
                    first_error.get_or_insert(WorkflowError::Model(error));
                }
            }
            Ok(Err(error)) => {
                first_error.get_or_insert(error);
            }
            Err(error) => {
                first_error.get_or_insert_with(|| {
                    WorkflowError::WriterTask(format!("concurrent writer task failed: {error}"))
                });
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    merged
        .finish(started.elapsed())
        .map_err(WorkflowError::from)
}

fn request_identity(
    config: &RoundExecutionConfig,
    phase: &str,
    writer: Option<u32>,
    sequence: u64,
) -> Result<RequestIdentity, WorkflowError> {
    let writer = writer.map_or_else(String::new, |writer| format!("/{writer}"));
    RequestIdentity::new(format!(
        "{}/{}/{}/{phase}{writer}/{sequence}",
        config.catalog, config.fixture.id, config.repetition
    ))
    .map_err(WorkflowError::from)
}

async fn cleanup<P: CatalogPort>(catalog: &P) -> CleanupEvidence {
    let drop_table_without_purge = match catalog.drop_table_without_purge().await {
        Ok(receipt) => OperationEvidence::succeeded(receipt),
        Err(failure) => OperationEvidence::catalog_failure(failure),
    };
    let verify_table_absent = match catalog.table_presence().await {
        Ok(observation) => OperationEvidence::succeeded(observation),
        Err(failure) => OperationEvidence::catalog_failure(failure),
    };
    let drop_namespace = match catalog.drop_namespace().await {
        Ok(receipt) => OperationEvidence::succeeded(receipt),
        Err(failure) => OperationEvidence::catalog_failure(failure),
    };
    let verify_namespace_absent = match catalog.namespace_presence().await {
        Ok(observation) => OperationEvidence::succeeded(observation),
        Err(failure) => OperationEvidence::catalog_failure(failure),
    };
    CleanupEvidence {
        drop_table_without_purge,
        verify_table_absent,
        drop_namespace,
        verify_namespace_absent,
    }
}

fn evaluate_checks(execution: &RoundExecution) -> RoundChecks {
    let fixture_isolated = execution
        .preflight
        .output()
        .is_some_and(|observation| observation.presence == ResourcePresence::Absent);
    let setup_succeeded = execution.create_namespace.passed()
        && execution.create_table.passed()
        && execution
            .baseline_object_audit
            .output()
            .is_some_and(|audit| audit.metadata_objects > 0 && audit.referenced_metadata_exists);
    let warmup_accounted = execution
        .warmup
        .output()
        .is_some_and(|phase| serial_accounted(phase, execution.workload.warmup_commits));
    let sequential_accounted = execution
        .sequential
        .output()
        .is_some_and(|phase| serial_accounted(phase, execution.workload.sequential_commits));
    let sequential_latency_complete = execution
        .sequential
        .output()
        .is_some_and(|phase| latency_complete(phase, execution.workload.sequential_commits));
    let all_requests_accounted = execution.concurrent.output().is_some_and(|phase| {
        phase.counts.fully_accounted()
            && phase
                .latency_ms
                .all
                .as_ref()
                .map(|distribution| distribution.samples)
                .unwrap_or_default()
                == phase.counts.attempts
    });
    let zero_request_errors = execution
        .concurrent
        .output()
        .is_some_and(|phase| phase.counts.errors == 0);
    let concurrent_progress = execution
        .concurrent
        .output()
        .is_some_and(PhaseEvidence::made_progress);
    let final_state_accounted = execution
        .final_table
        .output()
        .is_some_and(FinalTableEvidence::passed);
    let metadata_persisted = execution
        .metadata_growth
        .output()
        .is_some_and(|growth| growth.sufficient)
        && execution
            .final_object_audit
            .output()
            .is_some_and(|audit| audit.referenced_metadata_exists);
    RoundChecks {
        fixture_isolated,
        setup_succeeded,
        warmup_accounted,
        sequential_accounted,
        sequential_latency_complete,
        all_requests_accounted,
        zero_request_errors,
        concurrent_progress,
        final_state_accounted,
        metadata_persisted,
        fixture_clean: execution.cleanup.passed(),
    }
}

fn serial_accounted(phase: &PhaseEvidence, expected: u64) -> bool {
    phase.counts
        == (RequestCounts {
            attempts: expected,
            accepted: expected,
            conflicts: 0,
            errors: 0,
        })
        && phase.counts.fully_accounted()
}

fn latency_complete(phase: &PhaseEvidence, expected: u64) -> bool {
    let Some(distribution) = &phase.latency_ms.all else {
        return false;
    };
    let Some(p50) = distribution.quantiles.get("p50") else {
        return false;
    };
    let Some(p95) = distribution.quantiles.get("p95") else {
        return false;
    };
    let Some(p99) = distribution.quantiles.get("p99") else {
        return false;
    };
    distribution.samples == expected
        && distribution.minimum.is_finite()
        && distribution.maximum.is_finite()
        && p50.is_finite()
        && p95.is_finite()
        && p99.is_finite()
        && distribution.minimum <= *p50
        && p50 <= p95
        && p95 <= p99
        && p99 <= &distribution.maximum
}

#[derive(Debug, Clone, Copy)]
struct WriterWindow {
    deadline: Instant,
}

#[derive(Debug)]
pub enum WorkflowError {
    InvalidConfiguration(String),
    Model(ModelError),
    WriterStartChannelClosed,
    WriterTask(String),
    SequenceOverflow,
    DeadlineOverflow,
}

impl Display for WorkflowError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfiguration(detail) | Self::WriterTask(detail) => {
                formatter.write_str(detail)
            }
            Self::Model(error) => Display::fmt(error, formatter),
            Self::WriterStartChannelClosed => {
                formatter.write_str("concurrent writer start channel closed")
            }
            Self::SequenceOverflow => formatter.write_str("writer request sequence overflowed u64"),
            Self::DeadlineOverflow => formatter.write_str("concurrent deadline overflowed"),
        }
    }
}

impl Error for WorkflowError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Model(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ModelError> for WorkflowError {
    fn from(error: ModelError) -> Self {
        Self::Model(error)
    }
}
