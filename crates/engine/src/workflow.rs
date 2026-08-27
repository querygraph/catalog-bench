use std::future::Future;

use catalog_bench_commit::store::{
    ObjectStoreFailure, ObjectStoreFailureKind, TableObjectAuditSnapshot, TableObjectStore,
    TableRoot,
};
use serde::{Deserialize, Serialize};

use crate::reconcile::evaluate_checks;
use crate::{
    EngineCatalog, EngineCatalogFailure, EngineCatalogNegotiationEvidence, EngineCleanupReceipt,
    EngineProcessExecution, EngineResourcePresence, EngineTableLoad, InteroperabilityPlan,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EngineSkipReason {
    ProcessDidNotOwnFixture,
    FixtureCollision,
    CatalogUnavailable,
    TableUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "kebab-case", deny_unknown_fields)]
pub enum EngineOperationFailure {
    Catalog { failure: EngineCatalogFailure },
    ObjectStore { kind: ObjectStoreFailureKind },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum EngineOperationEvidence<T> {
    Succeeded { output: T },
    Failed { failure: EngineOperationFailure },
    Skipped { reason: EngineSkipReason },
}

impl<T> EngineOperationEvidence<T> {
    fn succeeded(output: T) -> Self {
        Self::Succeeded { output }
    }

    fn catalog_failure(failure: EngineCatalogFailure) -> Self {
        Self::Failed {
            failure: EngineOperationFailure::Catalog { failure },
        }
    }

    fn object_store_failure(failure: ObjectStoreFailure) -> Self {
        Self::Failed {
            failure: EngineOperationFailure::ObjectStore { kind: failure.kind },
        }
    }

    fn skipped(reason: EngineSkipReason) -> Self {
        Self::Skipped { reason }
    }

    #[must_use]
    pub fn output(&self) -> Option<&T> {
        match self {
            Self::Succeeded { output } => Some(output),
            Self::Failed { .. } | Self::Skipped { .. } => None,
        }
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        matches!(self, Self::Succeeded { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EngineCatalogConnectionFailureKind {
    Setup,
    Authentication,
    Config,
    Routing,
    FixtureRoute,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineCatalogConnectionFailure {
    pub kind: EngineCatalogConnectionFailureKind,
}

pub enum EngineCatalogConnection<C> {
    Ready {
        negotiation: EngineCatalogNegotiationEvidence,
        catalog: C,
    },
    Failed {
        negotiation: Option<EngineCatalogNegotiationEvidence>,
        failure: EngineCatalogConnectionFailure,
    },
}

pub trait EngineRunner: Clone + Send + Sync + 'static {
    fn execute(
        &self,
        plan: &InteroperabilityPlan,
    ) -> impl Future<Output = EngineProcessExecution> + Send;
}

pub trait EngineCatalogConnector: Clone + Send + Sync + 'static {
    type Catalog: EngineCatalog;

    fn connect(
        &self,
        plan: &InteroperabilityPlan,
    ) -> impl Future<Output = EngineCatalogConnection<Self::Catalog>> + Send;
}

pub trait EngineObjectStoreConnector: Clone + Send + Sync + 'static {
    type Store: TableObjectStore;

    fn connect(&self, plan: &InteroperabilityPlan) -> Result<Self::Store, ObjectStoreFailure>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum EngineCatalogConnectionEvidence {
    NotAttempted {
        reason: EngineSkipReason,
    },
    Ready {
        negotiation: EngineCatalogNegotiationEvidence,
    },
    Failed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        negotiation: Option<EngineCatalogNegotiationEvidence>,
        failure: EngineCatalogConnectionFailure,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineCleanupEvidence {
    pub drop_table_without_purge: EngineOperationEvidence<EngineCleanupReceipt>,
    pub verify_table_absent: EngineOperationEvidence<EngineResourcePresence>,
    pub drop_namespace: EngineOperationEvidence<EngineCleanupReceipt>,
    pub verify_namespace_absent: EngineOperationEvidence<EngineResourcePresence>,
}

impl EngineCleanupEvidence {
    fn skipped(reason: EngineSkipReason) -> Self {
        Self {
            drop_table_without_purge: EngineOperationEvidence::skipped(reason),
            verify_table_absent: EngineOperationEvidence::skipped(reason),
            drop_namespace: EngineOperationEvidence::skipped(reason),
            verify_namespace_absent: EngineOperationEvidence::skipped(reason),
        }
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.drop_table_without_purge.passed()
            && self
                .verify_table_absent
                .output()
                .copied()
                .is_some_and(EngineResourcePresence::is_absent)
            && self.drop_namespace.passed()
            && self
                .verify_namespace_absent
                .output()
                .copied()
                .is_some_and(EngineResourcePresence::is_absent)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EngineBehaviorClassification {
    Pass,
    Fail,
    FixtureCollision,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineBehaviorChecks {
    pub engine_runtime_pinned: bool,
    pub stock_rest_catalog_ready: bool,
    pub fixture_isolated: bool,
    pub namespace_round_trip: bool,
    pub table_round_trip: bool,
    pub initial_append_committed: bool,
    pub initial_read_exact: bool,
    pub schema_evolved: bool,
    pub evolved_append_committed: bool,
    pub evolved_read_exact: bool,
    pub catalog_state_correlated: bool,
    pub shared_object_evidence_complete: bool,
    pub fixture_clean: bool,
}

impl EngineBehaviorChecks {
    #[must_use]
    pub fn all_passed(&self) -> bool {
        self.engine_runtime_pinned
            && self.stock_rest_catalog_ready
            && self.fixture_isolated
            && self.namespace_round_trip
            && self.table_round_trip
            && self.initial_append_committed
            && self.initial_read_exact
            && self.schema_evolved
            && self.evolved_append_committed
            && self.evolved_read_exact
            && self.catalog_state_correlated
            && self.shared_object_evidence_complete
            && self.fixture_clean
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineExecution {
    pub process: EngineProcessExecution,
    pub catalog_connection: EngineCatalogConnectionEvidence,
    pub catalog_state: EngineOperationEvidence<EngineTableLoad>,
    pub object_state: EngineOperationEvidence<TableObjectAuditSnapshot>,
    pub cleanup: EngineCleanupEvidence,
    pub checks: EngineBehaviorChecks,
    pub classification: EngineBehaviorClassification,
}

impl EngineExecution {
    fn pending(process: EngineProcessExecution) -> Self {
        let reason = if process.fixture_collision() {
            EngineSkipReason::FixtureCollision
        } else {
            EngineSkipReason::ProcessDidNotOwnFixture
        };
        Self {
            process,
            catalog_connection: EngineCatalogConnectionEvidence::NotAttempted { reason },
            catalog_state: EngineOperationEvidence::skipped(reason),
            object_state: EngineOperationEvidence::skipped(reason),
            cleanup: EngineCleanupEvidence::skipped(reason),
            checks: EngineBehaviorChecks::default(),
            classification: EngineBehaviorClassification::Fail,
        }
    }

    pub(crate) fn finalize(&mut self, plan: &InteroperabilityPlan) {
        self.checks = evaluate_checks(plan, self);
        self.classification = if self.process.fixture_collision() {
            EngineBehaviorClassification::FixtureCollision
        } else if self.process.passed() && self.checks.all_passed() {
            EngineBehaviorClassification::Pass
        } else {
            EngineBehaviorClassification::Fail
        };
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.classification == EngineBehaviorClassification::Pass
            && self.process.passed()
            && self.checks.all_passed()
    }
}

pub async fn run_engine_workflow<R, C, O>(
    plan: &InteroperabilityPlan,
    runner: R,
    catalog_connector: C,
    object_store_connector: O,
) -> EngineExecution
where
    R: EngineRunner,
    C: EngineCatalogConnector,
    O: EngineObjectStoreConnector,
{
    let process = runner.execute(plan).await;
    let mut execution = EngineExecution::pending(process);
    if !execution.process.cleanup_authorized() {
        execution.finalize(plan);
        return execution;
    }

    let catalog = match catalog_connector.connect(plan).await {
        EngineCatalogConnection::Ready {
            negotiation,
            catalog,
        } => {
            execution.catalog_connection = EngineCatalogConnectionEvidence::Ready { negotiation };
            catalog
        }
        EngineCatalogConnection::Failed {
            negotiation,
            failure,
        } => {
            execution.catalog_connection = EngineCatalogConnectionEvidence::Failed {
                negotiation,
                failure,
            };
            execution.catalog_state =
                EngineOperationEvidence::skipped(EngineSkipReason::CatalogUnavailable);
            execution.object_state =
                EngineOperationEvidence::skipped(EngineSkipReason::CatalogUnavailable);
            execution.cleanup =
                EngineCleanupEvidence::skipped(EngineSkipReason::CatalogUnavailable);
            execution.finalize(plan);
            return execution;
        }
    };

    execution.catalog_state = match catalog.load_table().await {
        Ok(load) => EngineOperationEvidence::succeeded(load),
        Err(failure) => EngineOperationEvidence::catalog_failure(failure),
    };
    execution.object_state = audit_object_state(
        plan,
        execution.catalog_state.output(),
        &object_store_connector,
    )
    .await;
    execution.cleanup = cleanup_fixture(&catalog).await;
    execution.finalize(plan);
    execution
}

async fn audit_object_state<O>(
    plan: &InteroperabilityPlan,
    catalog_state: Option<&EngineTableLoad>,
    connector: &O,
) -> EngineOperationEvidence<TableObjectAuditSnapshot>
where
    O: EngineObjectStoreConnector,
{
    let Some(EngineTableLoad::Present { state, .. }) = catalog_state else {
        return EngineOperationEvidence::skipped(EngineSkipReason::TableUnavailable);
    };
    let root = match TableRoot::new(
        &state.table.location,
        &state.table.metadata_location,
        &plan.object_store().bucket,
    ) {
        Ok(root) => root,
        Err(failure) => return EngineOperationEvidence::object_store_failure(failure),
    };
    let store = match connector.connect(plan) {
        Ok(store) => store,
        Err(failure) => return EngineOperationEvidence::object_store_failure(failure),
    };
    match store
        .audit_table(&root, &state.table.metadata_location)
        .await
    {
        Ok(audit) => EngineOperationEvidence::succeeded(audit),
        Err(failure) => EngineOperationEvidence::object_store_failure(failure),
    }
}

async fn cleanup_fixture<C>(catalog: &C) -> EngineCleanupEvidence
where
    C: EngineCatalog,
{
    let drop_table_without_purge = match catalog.drop_table_without_purge().await {
        Ok(receipt) => EngineOperationEvidence::succeeded(receipt),
        Err(failure) => EngineOperationEvidence::catalog_failure(failure),
    };
    let verify_table_absent = match catalog.table_presence().await {
        Ok(presence) => EngineOperationEvidence::succeeded(presence),
        Err(failure) => EngineOperationEvidence::catalog_failure(failure),
    };
    let drop_namespace = match catalog.drop_namespace().await {
        Ok(receipt) => EngineOperationEvidence::succeeded(receipt),
        Err(failure) => EngineOperationEvidence::catalog_failure(failure),
    };
    let verify_namespace_absent = match catalog.namespace_presence().await {
        Ok(presence) => EngineOperationEvidence::succeeded(presence),
        Err(failure) => EngineOperationEvidence::catalog_failure(failure),
    };
    EngineCleanupEvidence {
        drop_table_without_purge,
        verify_table_absent,
        drop_namespace,
        verify_namespace_absent,
    }
}
