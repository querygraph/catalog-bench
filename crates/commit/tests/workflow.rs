use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use catalog_bench_commit::model::{RequestErrorKind, RequestOutcome, SanitizedRequestError};
use catalog_bench_commit::policy::{ContentionFixture, RoundKind};
use catalog_bench_commit::protocol::{
    CatalogFailure, CatalogFailureKind, CatalogPort, MutationReceipt, PresenceObservation,
    ResourcePresence, TableSnapshot,
};
use catalog_bench_commit::store::{
    MetadataStore, ObjectAuditSnapshot, ObjectStoreFailure, ObjectStoreFailureKind, TableRoot,
};
use catalog_bench_commit::workflow::{
    run_contention_round, OperationEvidence, RoundClassification, RoundExecutionConfig,
    RoundWorkload,
};

const LOCATION: &str = "s3://warehouse/catalog/fixture/table";
const PROPERTY: &str = "catalog-bench.contention.request-id";

#[derive(Debug, Clone, Copy, Default)]
struct CatalogBehavior {
    fixture_collision: bool,
    create_table_fails_after_mutation: bool,
    concurrent_request_errors: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Call {
    NamespacePresence,
    TablePresence,
    CreateNamespace,
    CreateTable,
    LoadTable,
    DropTable,
    DropNamespace,
}

#[derive(Debug, Default)]
struct FakeState {
    namespace_present: bool,
    table_present: bool,
    metadata_objects: u64,
    property: Option<String>,
    request_identities: Vec<String>,
    accepted: u64,
    conflicts: u64,
    errors: u64,
    calls: Vec<Call>,
}

#[derive(Clone)]
struct FakeCatalog {
    state: Arc<Mutex<FakeState>>,
    behavior: CatalogBehavior,
}

impl FakeCatalog {
    fn new(behavior: CatalogBehavior) -> Self {
        let state = FakeState {
            namespace_present: behavior.fixture_collision,
            ..FakeState::default()
        };
        Self {
            state: Arc::new(Mutex::new(state)),
            behavior,
        }
    }

    fn state(&self) -> MutexGuard<'_, FakeState> {
        self.state.lock().expect("fake catalog state poisoned")
    }
}

impl CatalogPort for FakeCatalog {
    fn requested_location(&self) -> Option<&str> {
        Some(LOCATION)
    }

    fn namespace_presence(
        &self,
    ) -> impl Future<Output = Result<PresenceObservation, CatalogFailure>> + Send {
        let state = Arc::clone(&self.state);
        async move {
            let mut state = state.lock().expect("fake catalog state poisoned");
            state.calls.push(Call::NamespacePresence);
            Ok(presence(state.namespace_present))
        }
    }

    fn table_presence(
        &self,
    ) -> impl Future<Output = Result<PresenceObservation, CatalogFailure>> + Send {
        let state = Arc::clone(&self.state);
        async move {
            let mut state = state.lock().expect("fake catalog state poisoned");
            state.calls.push(Call::TablePresence);
            Ok(presence(state.table_present))
        }
    }

    fn create_namespace(
        &self,
    ) -> impl Future<Output = Result<MutationReceipt, CatalogFailure>> + Send {
        let state = Arc::clone(&self.state);
        async move {
            let mut state = state.lock().expect("fake catalog state poisoned");
            state.calls.push(Call::CreateNamespace);
            state.namespace_present = true;
            Ok(MutationReceipt { http_status: 200 })
        }
    }

    fn create_table(&self) -> impl Future<Output = Result<TableSnapshot, CatalogFailure>> + Send {
        let state = Arc::clone(&self.state);
        let fails = self.behavior.create_table_fails_after_mutation;
        async move {
            let mut state = state.lock().expect("fake catalog state poisoned");
            state.calls.push(Call::CreateTable);
            state.table_present = true;
            state.metadata_objects = 1;
            if fails {
                Err(catalog_failure("create response was lost after mutation"))
            } else {
                Ok(snapshot(&state))
            }
        }
    }

    fn load_table(&self) -> impl Future<Output = Result<TableSnapshot, CatalogFailure>> + Send {
        let state = Arc::clone(&self.state);
        async move {
            let mut state = state.lock().expect("fake catalog state poisoned");
            state.calls.push(Call::LoadTable);
            if state.table_present {
                Ok(snapshot(&state))
            } else {
                Err(catalog_failure("table is absent"))
            }
        }
    }

    fn commit(
        &self,
        table_uuid: &str,
        property: &str,
        request_identity: &catalog_bench_commit::model::RequestIdentity,
    ) -> impl Future<Output = RequestOutcome> + Send {
        let state = Arc::clone(&self.state);
        let identity = request_identity.expose_for_request().to_owned();
        let valid_request = table_uuid == "table-uuid" && property == PROPERTY;
        let request_errors = self.behavior.concurrent_request_errors;
        async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let mut state = state.lock().expect("fake catalog state poisoned");
            state.request_identities.push(identity.clone());
            if !valid_request {
                state.errors += 1;
                return request_error();
            }
            if identity.contains("/concurrent/1/") {
                if request_errors {
                    state.errors += 1;
                    request_error()
                } else {
                    state.conflicts += 1;
                    RequestOutcome::Conflict
                }
            } else {
                state.accepted += 1;
                state.metadata_objects += 1;
                state.property = Some(identity);
                RequestOutcome::Accepted
            }
        }
    }

    fn drop_table_without_purge(
        &self,
    ) -> impl Future<Output = Result<MutationReceipt, CatalogFailure>> + Send {
        let state = Arc::clone(&self.state);
        async move {
            let mut state = state.lock().expect("fake catalog state poisoned");
            state.calls.push(Call::DropTable);
            state.table_present = false;
            Ok(MutationReceipt { http_status: 204 })
        }
    }

    fn drop_namespace(
        &self,
    ) -> impl Future<Output = Result<MutationReceipt, CatalogFailure>> + Send {
        let state = Arc::clone(&self.state);
        async move {
            let mut state = state.lock().expect("fake catalog state poisoned");
            state.calls.push(Call::DropNamespace);
            state.namespace_present = false;
            Ok(MutationReceipt { http_status: 204 })
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct StoreBehavior {
    fail_final_audit: bool,
    undercount_final_audit: bool,
}

#[derive(Clone)]
struct FakeStore {
    state: Arc<Mutex<FakeState>>,
    audit_calls: Arc<AtomicUsize>,
    behavior: StoreBehavior,
}

impl FakeStore {
    fn new(catalog: &FakeCatalog, behavior: StoreBehavior) -> Self {
        Self {
            state: Arc::clone(&catalog.state),
            audit_calls: Arc::new(AtomicUsize::new(0)),
            behavior,
        }
    }
}

impl MetadataStore for FakeStore {
    fn audit(
        &self,
        root: &TableRoot,
        metadata_location: &str,
    ) -> impl Future<Output = Result<ObjectAuditSnapshot, ObjectStoreFailure>> + Send {
        let state = Arc::clone(&self.state);
        let audit_index = self.audit_calls.fetch_add(1, Ordering::SeqCst);
        let behavior = self.behavior;
        let table_root = root.location().to_owned();
        let metadata_location = metadata_location.to_owned();
        async move {
            if behavior.fail_final_audit && audit_index > 0 {
                return Err(ObjectStoreFailure {
                    kind: ObjectStoreFailureKind::List,
                    detail: "injected final audit failure".to_owned(),
                });
            }
            let state = state.lock().expect("fake catalog state poisoned");
            let metadata_objects = if behavior.undercount_final_audit && audit_index > 0 {
                1
            } else {
                state.metadata_objects
            };
            Ok(ObjectAuditSnapshot {
                table_root,
                metadata_objects,
                metadata_bytes: metadata_objects * 100,
                referenced_metadata_location: metadata_location,
                referenced_metadata_exists: true,
            })
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn complete_round_accounts_for_contention_and_redacts_request_identities() {
    let catalog = FakeCatalog::new(CatalogBehavior::default());
    let store = FakeStore::new(&catalog, StoreBehavior::default());
    let execution = run_contention_round(catalog.clone(), store, config())
        .await
        .unwrap();

    assert!(execution.passed());
    assert_eq!(execution.classification, RoundClassification::Pass);
    assert!(execution.checks.all_passed());
    let OperationEvidence::Succeeded { output: setup } = &execution.create_table else {
        panic!("setup must succeed");
    };
    assert_eq!(setup.requested_location.as_deref(), Some(LOCATION));
    let OperationEvidence::Succeeded { output: concurrent } = &execution.concurrent else {
        panic!("concurrent phase must succeed");
    };
    assert!(concurrent.counts.accepted > 0);
    assert!(concurrent.counts.conflicts > 0);
    assert_eq!(concurrent.counts.errors, 0);

    let state = catalog.state();
    assert!(!state.namespace_present);
    assert!(!state.table_present);
    assert_eq!(state.accepted, metadata_growth(&execution));
    let serialized = serde_json::to_string(&execution).unwrap();
    for identity in &state.request_identities {
        assert!(!serialized.contains(identity));
    }
    assert_eq!(
        state
            .calls
            .iter()
            .filter(|call| **call == Call::LoadTable)
            .count(),
        1,
        "only the post-workload final-state load is allowed"
    );
}

#[tokio::test]
async fn fixture_collision_performs_no_mutation_or_cleanup() {
    let catalog = FakeCatalog::new(CatalogBehavior {
        fixture_collision: true,
        ..CatalogBehavior::default()
    });
    let store = FakeStore::new(&catalog, StoreBehavior::default());
    let audits = Arc::clone(&store.audit_calls);
    let execution = run_contention_round(catalog.clone(), store, config())
        .await
        .unwrap();

    assert_eq!(
        execution.classification,
        RoundClassification::FixtureCollision
    );
    assert!(!execution.passed());
    assert_eq!(catalog.state().calls, vec![Call::NamespacePresence]);
    assert_eq!(audits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ambiguous_create_failure_still_cleans_the_owned_fixture() {
    let catalog = FakeCatalog::new(CatalogBehavior {
        create_table_fails_after_mutation: true,
        ..CatalogBehavior::default()
    });
    let store = FakeStore::new(&catalog, StoreBehavior::default());
    let audits = Arc::clone(&store.audit_calls);
    let execution = run_contention_round(catalog.clone(), store, config())
        .await
        .unwrap();

    assert_eq!(execution.classification, RoundClassification::Fail);
    assert!(execution.checks.fixture_clean);
    assert!(matches!(
        execution.create_table,
        OperationEvidence::Failed { .. }
    ));
    let state = catalog.state();
    assert!(!state.namespace_present);
    assert!(!state.table_present);
    assert!(state.calls.contains(&Call::DropTable));
    assert!(state.calls.contains(&Call::DropNamespace));
    assert_eq!(audits.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_request_errors_fail_the_round_but_preserve_complete_evidence() {
    let catalog = FakeCatalog::new(CatalogBehavior {
        concurrent_request_errors: true,
        ..CatalogBehavior::default()
    });
    let store = FakeStore::new(&catalog, StoreBehavior::default());
    let execution = run_contention_round(catalog, store, config())
        .await
        .unwrap();

    assert_eq!(execution.classification, RoundClassification::Fail);
    assert!(!execution.checks.zero_request_errors);
    assert!(execution.checks.all_requests_accounted);
    assert!(execution.checks.fixture_clean);
    let OperationEvidence::Succeeded { output: concurrent } = execution.concurrent else {
        panic!("request errors are benchmark evidence, not harness errors");
    };
    assert!(concurrent.counts.errors > 0);
    assert_eq!(concurrent.error_counts.len(), 1);
    assert_eq!(
        concurrent.error_counts[0].error.kind,
        RequestErrorKind::Transport
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_final_audit_leaves_growth_unattempted_and_cleans_up() {
    let catalog = FakeCatalog::new(CatalogBehavior::default());
    let store = FakeStore::new(
        &catalog,
        StoreBehavior {
            fail_final_audit: true,
            ..StoreBehavior::default()
        },
    );
    let execution = run_contention_round(catalog, store, config())
        .await
        .unwrap();

    assert_eq!(execution.classification, RoundClassification::Fail);
    assert!(matches!(
        execution.final_object_audit,
        OperationEvidence::Failed { .. }
    ));
    assert!(matches!(
        execution.metadata_growth,
        OperationEvidence::NotAttempted { .. }
    ));
    assert!(!execution.checks.metadata_persisted);
    assert!(execution.checks.fixture_clean);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_undercount_is_retained_as_failed_evidence() {
    let catalog = FakeCatalog::new(CatalogBehavior::default());
    let store = FakeStore::new(
        &catalog,
        StoreBehavior {
            undercount_final_audit: true,
            ..StoreBehavior::default()
        },
    );
    let execution = run_contention_round(catalog, store, config())
        .await
        .unwrap();

    assert_eq!(execution.classification, RoundClassification::Fail);
    let OperationEvidence::Succeeded { output: growth } = execution.metadata_growth else {
        panic!("an undercount must remain inspectable evidence");
    };
    assert!(!growth.sufficient);
    assert!(!execution.checks.metadata_persisted);
    assert!(execution.checks.fixture_clean);
}

#[test]
fn workload_rejects_zero_or_empty_dimensions() {
    assert!(RoundWorkload::new(0, 1, 1, 1, PROPERTY).is_err());
    assert!(RoundWorkload::new(1, 0, 1, 1, PROPERTY).is_err());
    assert!(RoundWorkload::new(1, 1, 0, 1, PROPERTY).is_err());
    assert!(RoundWorkload::new(1, 1, 1, 0, PROPERTY).is_err());
    assert!(RoundWorkload::new(1, 1, 1, 1, " ").is_err());
}

fn config() -> RoundExecutionConfig {
    RoundExecutionConfig::new(
        "fake",
        1,
        RoundKind::Measured,
        ContentionFixture {
            id: "fixture".to_owned(),
            namespace: "cb_c108_fake_fixture_r01".to_owned(),
            table: "same_table_contention".to_owned(),
        },
        RoundWorkload::new(2, 3, 2, 25, PROPERTY).unwrap(),
        "warehouse",
    )
    .unwrap()
}

fn snapshot(state: &FakeState) -> TableSnapshot {
    let sequence = state
        .metadata_objects
        .checked_sub(1)
        .expect("a present fake table has setup metadata");
    let properties = state
        .property
        .as_ref()
        .map(|value| BTreeMap::from([(PROPERTY.to_owned(), value.clone())]))
        .unwrap_or_default();
    TableSnapshot {
        format_version: 2,
        table_uuid: "table-uuid".to_owned(),
        location: LOCATION.to_owned(),
        metadata_location: format!("{LOCATION}/metadata/{sequence:05}.metadata.json"),
        properties,
    }
}

fn presence(present: bool) -> PresenceObservation {
    PresenceObservation {
        http_status: if present { 200 } else { 404 },
        presence: if present {
            ResourcePresence::Present
        } else {
            ResourcePresence::Absent
        },
    }
}

fn catalog_failure(detail: &str) -> CatalogFailure {
    CatalogFailure {
        kind: CatalogFailureKind::Transport,
        http_status: None,
        detail: detail.to_owned(),
    }
}

fn request_error() -> RequestOutcome {
    RequestOutcome::Error(SanitizedRequestError {
        kind: RequestErrorKind::Transport,
        http_status: None,
    })
}

fn metadata_growth(execution: &catalog_bench_commit::workflow::RoundExecution) -> u64 {
    let OperationEvidence::Succeeded { output: growth } = &execution.metadata_growth else {
        panic!("metadata growth must be recorded");
    };
    growth.minimum_required_growth
}
