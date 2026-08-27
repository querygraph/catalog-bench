use std::sync::Arc;

use catalog_bench_commit::store::{ObjectStoreAuditor, ObjectStoreFailure};
use catalog_bench_common::contract::Profile;
use catalog_bench_conformance::{
    connect_catalog_adapter, CatalogConnectionOutcome, CatalogNegotiationFailureStage,
    CATALOG_RESPONSE_LIMIT_BYTES,
};

use crate::{
    EngineCatalogConnection, EngineCatalogConnectionFailure, EngineCatalogConnectionFailureKind,
    EngineCatalogConnector, EngineCatalogNegotiationEvidence, EngineObjectStoreConnector,
    EngineProcessExecution, EngineRunner, FlinkProcessExecutor, InteroperabilityPlan,
    RestEngineCatalog, RuntimeVerifier, SecretRead, SecretSource, SparkProcessExecutor,
};

const CATALOG_REQUEST_TIMEOUT_MS: u64 = 30_000;

pub struct StockSparkRunner<S> {
    executor: SparkProcessExecutor,
    verifier: RuntimeVerifier,
    secrets: Arc<S>,
}

impl<S> StockSparkRunner<S> {
    #[must_use]
    pub fn production(secrets: Arc<S>) -> Self {
        Self {
            executor: SparkProcessExecutor::default(),
            verifier: RuntimeVerifier::host(),
            secrets,
        }
    }

    #[must_use]
    pub fn from_parts(
        executor: SparkProcessExecutor,
        verifier: RuntimeVerifier,
        secrets: Arc<S>,
    ) -> Self {
        Self {
            executor,
            verifier,
            secrets,
        }
    }
}

impl<S> Clone for StockSparkRunner<S> {
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
            verifier: self.verifier.clone(),
            secrets: Arc::clone(&self.secrets),
        }
    }
}

impl<S> EngineRunner for StockSparkRunner<S>
where
    S: SecretSource + Send + Sync + 'static,
{
    async fn execute(&self, plan: &InteroperabilityPlan) -> EngineProcessExecution {
        self.executor
            .execute_with_source(plan, &self.verifier, self.secrets.as_ref())
            .await
    }
}

pub struct StockFlinkRunner<S> {
    executor: FlinkProcessExecutor,
    verifier: RuntimeVerifier,
    secrets: Arc<S>,
}

impl<S> StockFlinkRunner<S> {
    #[must_use]
    pub fn production(secrets: Arc<S>) -> Self {
        Self {
            executor: FlinkProcessExecutor::default(),
            verifier: RuntimeVerifier::host(),
            secrets,
        }
    }

    #[must_use]
    pub fn from_parts(
        executor: FlinkProcessExecutor,
        verifier: RuntimeVerifier,
        secrets: Arc<S>,
    ) -> Self {
        Self {
            executor,
            verifier,
            secrets,
        }
    }
}

impl<S> Clone for StockFlinkRunner<S> {
    fn clone(&self) -> Self {
        Self {
            executor: self.executor.clone(),
            verifier: self.verifier.clone(),
            secrets: Arc::clone(&self.secrets),
        }
    }
}

impl<S> EngineRunner for StockFlinkRunner<S>
where
    S: SecretSource + Send + Sync + 'static,
{
    async fn execute(&self, plan: &InteroperabilityPlan) -> EngineProcessExecution {
        self.executor
            .execute_with_source(plan, &self.verifier, self.secrets.as_ref())
            .await
    }
}

pub struct RestEngineCatalogConnector<S> {
    profile: Arc<Profile>,
    secrets: Arc<S>,
}

impl<S> RestEngineCatalogConnector<S> {
    #[must_use]
    pub fn new(profile: Arc<Profile>, secrets: Arc<S>) -> Self {
        Self { profile, secrets }
    }
}

impl<S> Clone for RestEngineCatalogConnector<S> {
    fn clone(&self) -> Self {
        Self {
            profile: Arc::clone(&self.profile),
            secrets: Arc::clone(&self.secrets),
        }
    }
}

impl<S> EngineCatalogConnector for RestEngineCatalogConnector<S>
where
    S: SecretSource + Send + Sync + 'static,
{
    type Catalog = RestEngineCatalog;

    async fn connect(&self, plan: &InteroperabilityPlan) -> EngineCatalogConnection<Self::Catalog> {
        let attempt = connect_catalog_adapter(
            &self.profile,
            &plan.catalog().id,
            CATALOG_REQUEST_TIMEOUT_MS,
            CATALOG_RESPONSE_LIMIT_BYTES,
            |name| optional_secret(self.secrets.as_ref(), name),
        )
        .await;
        let attempt = match attempt {
            Ok(attempt) => attempt,
            Err(_) => return failed_connection(None, EngineCatalogConnectionFailureKind::Setup),
        };
        let negotiation = match EngineCatalogNegotiationEvidence::try_from(attempt.evidence) {
            Ok(negotiation) => negotiation,
            Err(_) => return failed_connection(None, EngineCatalogConnectionFailureKind::Setup),
        };
        match attempt.outcome {
            CatalogConnectionOutcome::Ready(session) => {
                match RestEngineCatalog::from_plan(session, plan) {
                    Ok(catalog) => EngineCatalogConnection::Ready {
                        negotiation,
                        catalog,
                    },
                    Err(_) => failed_connection(
                        Some(negotiation),
                        EngineCatalogConnectionFailureKind::FixtureRoute,
                    ),
                }
            }
            CatalogConnectionOutcome::Failed(failure) => failed_connection(
                Some(negotiation),
                match failure.stage {
                    CatalogNegotiationFailureStage::Authentication => {
                        EngineCatalogConnectionFailureKind::Authentication
                    }
                    CatalogNegotiationFailureStage::Config => {
                        EngineCatalogConnectionFailureKind::Config
                    }
                    CatalogNegotiationFailureStage::Routing => {
                        EngineCatalogConnectionFailureKind::Routing
                    }
                },
            ),
        }
    }
}

fn failed_connection<C>(
    negotiation: Option<EngineCatalogNegotiationEvidence>,
    kind: EngineCatalogConnectionFailureKind,
) -> EngineCatalogConnection<C> {
    EngineCatalogConnection::Failed {
        negotiation,
        failure: EngineCatalogConnectionFailure { kind },
    }
}

pub struct SharedObjectStoreConnector<S> {
    secrets: Arc<S>,
}

impl<S> SharedObjectStoreConnector<S> {
    #[must_use]
    pub fn new(secrets: Arc<S>) -> Self {
        Self { secrets }
    }
}

impl<S> Clone for SharedObjectStoreConnector<S> {
    fn clone(&self) -> Self {
        Self {
            secrets: Arc::clone(&self.secrets),
        }
    }
}

impl<S> EngineObjectStoreConnector for SharedObjectStoreConnector<S>
where
    S: SecretSource + Send + Sync + 'static,
{
    type Store = ObjectStoreAuditor;

    fn connect(&self, plan: &InteroperabilityPlan) -> Result<Self::Store, ObjectStoreFailure> {
        ObjectStoreAuditor::from_connection(plan.object_store(), |name| {
            optional_secret(self.secrets.as_ref(), name)
        })
    }
}

pub(crate) fn optional_secret(source: &(impl SecretSource + ?Sized), name: &str) -> Option<String> {
    match source.read_secret(name) {
        SecretRead::Value(value) => Some(value),
        SecretRead::Missing | SecretRead::Unreadable => None,
    }
}
