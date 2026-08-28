mod error;
mod sanitize;

use std::sync::Arc;

use catalog_bench_common::contract::{
    parse_contract, Component, ComponentId, ContractDocument, Profile, ProfileId, Scenario,
};
use catalog_bench_conformance::{sha256_hex, ContractDigests, TranscriptScenario};
use serde::{Deserialize, Serialize};

pub use error::{
    EngineEvidenceError, EngineEvidenceErrorKind, EngineTranscriptValidationFailure,
    EngineTranscriptValidationFailureKind,
};
pub use sanitize::EngineSanitizationViolation;
use sanitize::{audit_base_values, audit_with_plan, ObservedSecretSource};

use crate::{
    run_engine_workflow, ComponentIdentity, EngineCatalogConnectionEvidence, EngineExecution,
    InteroperabilityPlan, RestEngineCatalogConnector, SecretSource, SharedObjectStoreConnector,
    StockFlinkRunner, StockSparkRunner, ENGINE_SCENARIO_ID, ENGINE_SCENARIO_VERSION,
    ENGINE_TRANSCRIPT_FORMAT,
};

const SANITIZATION_POLICY: &str = "catalog-bench/value-safe-engine-v1";

#[derive(Debug, Clone)]
pub struct EngineContracts {
    profile: Arc<Profile>,
    scenario: Arc<Scenario>,
    digests: ContractDigests,
}

impl EngineContracts {
    pub fn parse(profile_bytes: &[u8], scenario_bytes: &[u8]) -> Result<Self, EngineEvidenceError> {
        let profile = parse_contract(profile_bytes)
            .map_err(|_| EngineEvidenceError::fixed(EngineEvidenceErrorKind::ProfileContract))?;
        let ContractDocument::Profile(profile) = profile else {
            return Err(EngineEvidenceError::fixed(
                EngineEvidenceErrorKind::ProfileDocumentKind,
            ));
        };
        let scenario = parse_contract(scenario_bytes)
            .map_err(|_| EngineEvidenceError::fixed(EngineEvidenceErrorKind::ScenarioContract))?;
        let ContractDocument::Scenario(scenario) = scenario else {
            return Err(EngineEvidenceError::fixed(
                EngineEvidenceErrorKind::ScenarioDocumentKind,
            ));
        };
        Ok(Self {
            profile: Arc::new(profile),
            scenario: Arc::new(scenario),
            digests: ContractDigests {
                profile_sha256: sha256_hex(profile_bytes),
                scenario_sha256: sha256_hex(scenario_bytes),
            },
        })
    }

    #[must_use]
    pub fn profile(&self) -> &Profile {
        &self.profile
    }

    #[must_use]
    pub fn scenario(&self) -> &Scenario {
        &self.scenario
    }

    #[must_use]
    pub fn digests(&self) -> &ContractDigests {
        &self.digests
    }

    fn plan(
        &self,
        catalog: &ComponentId,
        fixture_id: &str,
    ) -> Result<InteroperabilityPlan, EngineEvidenceError> {
        InteroperabilityPlan::from_contracts(&self.profile, &self.scenario, catalog, fixture_id)
            .map_err(EngineEvidenceError::policy)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineTranscriptProfile {
    pub id: ProfileId,
    pub resolved_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineTranscriptComponent {
    pub id: ComponentId,
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
}

impl EngineTranscriptComponent {
    fn from_identity(identity: &ComponentIdentity) -> Self {
        Self {
            id: identity.id.clone(),
            name: identity.name.clone(),
            version: identity.version.clone(),
            source_revision: identity.source_revision.clone(),
        }
    }

    fn from_component(component: &Component) -> Self {
        Self {
            id: component.id.clone(),
            name: component.name.clone(),
            version: component.version.clone(),
            source_revision: component
                .source
                .as_ref()
                .map(|source| source.revision.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineTranscriptComponents {
    pub runner: EngineTranscriptComponent,
    pub catalog: EngineTranscriptComponent,
    pub engine: EngineTranscriptComponent,
    pub connector: EngineTranscriptComponent,
    pub object_store: EngineTranscriptComponent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineTranscriptFixture {
    pub id: String,
    pub namespace: String,
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_location: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineTranscriptSanitization {
    pub policy: String,
    pub negotiation_redactions_observed: u64,
    pub raw_secrets_persisted: bool,
    pub raw_engine_rows_persisted: bool,
    pub raw_response_body_persisted: bool,
    pub raw_backend_failure_detail_persisted: bool,
    pub transcript_sanitized: bool,
}

impl EngineTranscriptSanitization {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.policy == SANITIZATION_POLICY
            && !self.raw_secrets_persisted
            && !self.raw_engine_rows_persisted
            && !self.raw_response_body_persisted
            && !self.raw_backend_failure_detail_persisted
            && self.transcript_sanitized
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineTranscript {
    pub format: String,
    pub scenario: TranscriptScenario,
    pub contract_digests: ContractDigests,
    pub profile: EngineTranscriptProfile,
    pub components: EngineTranscriptComponents,
    pub fixture: EngineTranscriptFixture,
    pub execution: EngineExecution,
    pub sanitization: EngineTranscriptSanitization,
}

impl EngineTranscript {
    fn from_execution(
        contracts: &EngineContracts,
        plan: &InteroperabilityPlan,
        fixture_id: &str,
        execution: EngineExecution,
        sensitive_values: &[String],
    ) -> Result<Self, EngineEvidenceError> {
        let components = expected_components(contracts.profile(), plan)
            .ok_or_else(|| EngineEvidenceError::fixed(EngineEvidenceErrorKind::ProfileComponent))?;
        let mut transcript = Self {
            format: ENGINE_TRANSCRIPT_FORMAT.to_owned(),
            scenario: TranscriptScenario {
                id: contracts.scenario.id.clone(),
                version: contracts.scenario.version,
            },
            contract_digests: contracts.digests.clone(),
            profile: EngineTranscriptProfile {
                id: contracts.profile.id.clone(),
                resolved_at: contracts.profile.resolved_at.clone(),
            },
            components,
            fixture: EngineTranscriptFixture {
                id: fixture_id.to_owned(),
                namespace: plan.fixture().namespace.clone(),
                table: plan.fixture().table.clone(),
                requested_location: plan.fixture().requested_location.clone(),
            },
            sanitization: EngineTranscriptSanitization {
                policy: SANITIZATION_POLICY.to_owned(),
                negotiation_redactions_observed: negotiation_redactions(&execution),
                raw_secrets_persisted: false,
                raw_engine_rows_persisted: false,
                raw_response_body_persisted: false,
                raw_backend_failure_detail_persisted: false,
                transcript_sanitized: false,
            },
            execution,
        };
        audit_with_plan(&transcript, plan, sensitive_values)
            .map_err(EngineEvidenceError::sanitization)?;
        transcript.sanitization.transcript_sanitized = true;
        transcript
            .validate(contracts)
            .map_err(EngineEvidenceError::validation)?;
        Ok(transcript)
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.format == ENGINE_TRANSCRIPT_FORMAT
            && self.scenario.id.as_str() == ENGINE_SCENARIO_ID
            && self.scenario.version == ENGINE_SCENARIO_VERSION
            && self.execution.passed()
            && self.sanitization.passed()
            && self.sanitization.negotiation_redactions_observed
                == negotiation_redactions(&self.execution)
            && audit_base_values(self, &[]).is_ok()
    }

    pub fn validate(
        &self,
        contracts: &EngineContracts,
    ) -> Result<(), EngineTranscriptValidationFailure> {
        if self.format != ENGINE_TRANSCRIPT_FORMAT {
            return Err(EngineTranscriptValidationFailure::new(
                EngineTranscriptValidationFailureKind::Format,
            ));
        }
        if self.scenario.id != contracts.scenario.id
            || self.scenario.version != contracts.scenario.version
        {
            return Err(EngineTranscriptValidationFailure::new(
                EngineTranscriptValidationFailureKind::Scenario,
            ));
        }
        if self.contract_digests != contracts.digests {
            return Err(EngineTranscriptValidationFailure::new(
                EngineTranscriptValidationFailureKind::ContractDigests,
            ));
        }
        if self.profile.id != contracts.profile.id
            || self.profile.resolved_at != contracts.profile.resolved_at
        {
            return Err(EngineTranscriptValidationFailure::new(
                EngineTranscriptValidationFailureKind::Profile,
            ));
        }
        let plan = contracts
            .plan(&self.components.catalog.id, &self.fixture.id)
            .map_err(|_| {
                EngineTranscriptValidationFailure::new(
                    EngineTranscriptValidationFailureKind::Policy,
                )
            })?;
        if expected_components(contracts.profile(), &plan).as_ref() != Some(&self.components) {
            return Err(EngineTranscriptValidationFailure::new(
                EngineTranscriptValidationFailureKind::Components,
            ));
        }
        let expected_fixture = EngineTranscriptFixture {
            id: self.fixture.id.clone(),
            namespace: plan.fixture().namespace.clone(),
            table: plan.fixture().table.clone(),
            requested_location: plan.fixture().requested_location.clone(),
        };
        if self.fixture != expected_fixture {
            return Err(EngineTranscriptValidationFailure::new(
                EngineTranscriptValidationFailureKind::Fixture,
            ));
        }
        let mut expected_execution = self.execution.clone();
        expected_execution.finalize(&plan);
        if self.execution.checks != expected_execution.checks
            || self.execution.classification != expected_execution.classification
        {
            return Err(EngineTranscriptValidationFailure::new(
                EngineTranscriptValidationFailureKind::Execution,
            ));
        }
        if !self.sanitization.passed()
            || self.sanitization.negotiation_redactions_observed
                != negotiation_redactions(&self.execution)
            || audit_with_plan(self, &plan, &[]).is_err()
        {
            return Err(EngineTranscriptValidationFailure::new(
                EngineTranscriptValidationFailureKind::Sanitization,
            ));
        }
        Ok(())
    }

    pub fn audit_serialized_values(
        &self,
        contracts: &EngineContracts,
        sensitive_values: &[String],
    ) -> Result<(), EngineSanitizationViolation> {
        let plan = contracts
            .plan(&self.components.catalog.id, &self.fixture.id)
            .map_err(|_| EngineSanitizationViolation::ContractBinding)?;
        audit_with_plan(self, &plan, sensitive_values)
    }
}

pub async fn run_stock_spark_interoperability<S>(
    contracts: &EngineContracts,
    catalog: &ComponentId,
    fixture_id: &str,
    secrets: Arc<S>,
) -> Result<EngineTranscript, EngineEvidenceError>
where
    S: SecretSource + Send + Sync + 'static,
{
    let plan = contracts.plan(catalog, fixture_id)?;
    let observed = Arc::new(ObservedSecretSource::new(secrets));
    let execution = run_engine_workflow(
        &plan,
        StockSparkRunner::production(Arc::clone(&observed)),
        RestEngineCatalogConnector::new(Arc::clone(&contracts.profile), Arc::clone(&observed)),
        SharedObjectStoreConnector::new(Arc::clone(&observed)),
    )
    .await;
    let sensitive_values = observed.sensitive_values();
    EngineTranscript::from_execution(
        contracts,
        &plan,
        fixture_id,
        execution,
        sensitive_values.as_slice(),
    )
}

pub async fn run_stock_flink_interoperability<S>(
    contracts: &EngineContracts,
    catalog: &ComponentId,
    fixture_id: &str,
    secrets: Arc<S>,
) -> Result<EngineTranscript, EngineEvidenceError>
where
    S: SecretSource + Send + Sync + 'static,
{
    let plan = contracts.plan(catalog, fixture_id)?;
    let observed = Arc::new(ObservedSecretSource::new(secrets));
    let execution = run_engine_workflow(
        &plan,
        StockFlinkRunner::production(Arc::clone(&observed)),
        RestEngineCatalogConnector::new(Arc::clone(&contracts.profile), Arc::clone(&observed)),
        SharedObjectStoreConnector::new(Arc::clone(&observed)),
    )
    .await;
    let sensitive_values = observed.sensitive_values();
    EngineTranscript::from_execution(
        contracts,
        &plan,
        fixture_id,
        execution,
        sensitive_values.as_slice(),
    )
}

pub async fn run_stock_engine_interoperability<S>(
    contracts: &EngineContracts,
    catalog: &ComponentId,
    fixture_id: &str,
    secrets: Arc<S>,
) -> Result<EngineTranscript, EngineEvidenceError>
where
    S: SecretSource + Send + Sync + 'static,
{
    let plan = contracts.plan(catalog, fixture_id)?;
    if plan.flink().is_some() {
        run_stock_flink_interoperability(contracts, catalog, fixture_id, secrets).await
    } else {
        run_stock_spark_interoperability(contracts, catalog, fixture_id, secrets).await
    }
}

fn expected_components(
    profile: &Profile,
    plan: &InteroperabilityPlan,
) -> Option<EngineTranscriptComponents> {
    let runner = plan
        .runner()
        .map(EngineTranscriptComponent::from_identity)
        .or_else(|| {
            let runner_id = profile.platform.container_runtime.as_ref()?;
            profile
                .components
                .iter()
                .find(|component| &component.id == runner_id)
                .map(EngineTranscriptComponent::from_component)
        })?;
    let object_store = profile
        .components
        .iter()
        .find(|component| component.id == plan.object_store().component)?;
    Some(EngineTranscriptComponents {
        runner,
        catalog: EngineTranscriptComponent::from_identity(plan.catalog()),
        engine: EngineTranscriptComponent::from_identity(plan.engine()),
        connector: EngineTranscriptComponent::from_identity(plan.connector()),
        object_store: EngineTranscriptComponent::from_component(object_store),
    })
}

fn negotiation_redactions(execution: &EngineExecution) -> u64 {
    let negotiation = match &execution.catalog_connection {
        EngineCatalogConnectionEvidence::Ready { negotiation }
        | EngineCatalogConnectionEvidence::Failed {
            negotiation: Some(negotiation),
            ..
        } => Some(negotiation),
        EngineCatalogConnectionEvidence::NotAttempted { .. }
        | EngineCatalogConnectionEvidence::Failed {
            negotiation: None, ..
        } => None,
    };
    negotiation.map_or(0, |negotiation| negotiation.redactions_observed)
}
