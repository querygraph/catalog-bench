use std::error::Error;
use std::fmt::{Display, Formatter};

use catalog_bench_common::contract::{
    CatalogAuthentication, ComponentId, ComponentKind, Profile, Scenario,
};
use catalog_bench_conformance::{
    connect_catalog, CatalogConnectionOutcome, ContractDigests, TranscriptScenario,
};
use serde::{Deserialize, Serialize};

use crate::aggregate::{aggregate_contention, AggregationError};
use crate::policy::{
    ContentionPlan, PolicyError, RoundKind, CONTENTION_TRANSCRIPT_FORMAT, RUNNER_COMPONENT_ID,
};
use crate::protocol::{RestCatalog, RestCatalogFixture};
use crate::store::{ObjectStoreAuditor, ObjectStoreFailure};
use crate::transcript::{
    CatalogRoundOutcome, CatalogRoundTranscript, ContentionSanitization, ContentionTranscript,
    RunnerTranscript, SanitizationViolation, TranscriptProfile,
};
use crate::workflow::{run_contention_round, RoundExecutionConfig, RoundWorkload, WorkflowError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunnerObservation {
    pub operating_system: String,
    pub architecture: String,
    pub source_revision: String,
}

impl RunnerObservation {
    pub fn new(
        operating_system: impl Into<String>,
        architecture: impl Into<String>,
        source_revision: impl Into<String>,
    ) -> Result<Self, SweepError> {
        let observation = Self {
            operating_system: operating_system.into(),
            architecture: architecture.into(),
            source_revision: source_revision.into(),
        };
        if observation.operating_system.trim().is_empty()
            || observation.architecture.trim().is_empty()
            || !is_git_revision(&observation.source_revision)
        {
            return Err(SweepError::InvalidRunnerObservation);
        }
        Ok(observation)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SweepProgress {
    Starting {
        repetition: u32,
        kind: RoundKind,
        position: u32,
        catalog: ComponentId,
    },
    Completed {
        repetition: u32,
        kind: RoundKind,
        position: u32,
        catalog: ComponentId,
        passed: bool,
    },
}

pub async fn run_contention_sweep<F, P>(
    profile: &Profile,
    scenario: &Scenario,
    contract_digests: ContractDigests,
    fixture_id: &str,
    runner_observation: &RunnerObservation,
    getenv: F,
    progress: P,
) -> Result<ContentionTranscript, SweepError>
where
    F: Fn(&str) -> Option<String>,
    P: Fn(SweepProgress),
{
    let plan = ContentionPlan::from_contracts(profile, scenario)?;
    let runner = verify_runner(profile, runner_observation)?;
    let parameters = plan.parameters();
    let maximum_response_bytes = usize::try_from(parameters.limits.maximum_response_bytes)
        .map_err(|_| SweepError::CountOverflow)?;
    let store = ObjectStoreAuditor::from_policy(&parameters.object_store, &getenv)?;
    let workload = RoundWorkload::try_from(&parameters.workload)?;
    let sensitive_values = sensitive_runtime_values(profile, parameters, &getenv);
    let mut rounds = Vec::new();
    let mut redactions = Vec::new();

    for round in plan.rounds() {
        for (position, catalog) in round.catalogs.iter().enumerate() {
            let position = u32::try_from(position + 1).map_err(|_| SweepError::CountOverflow)?;
            progress(SweepProgress::Starting {
                repetition: round.repetition,
                kind: round.kind,
                position,
                catalog: catalog.catalog.clone(),
            });
            let fixture = plan.fixture(&catalog.catalog, fixture_id, round.repetition)?;
            let connection = connect_catalog(
                profile,
                scenario,
                &catalog.catalog,
                parameters.limits.request_timeout_ms,
                maximum_response_bytes,
                &getenv,
            )
            .await
            .map_err(|error| SweepError::CatalogSetup {
                catalog: catalog.catalog.clone(),
                detail: error.to_string(),
            })?;
            let round_index = rounds.len();
            redactions.extend(
                connection
                    .evidence
                    .redactions
                    .iter()
                    .map(|path| format!("rounds.{round_index}.negotiation.{path}")),
            );
            let outcome = match connection.outcome {
                CatalogConnectionOutcome::Ready(session) => {
                    let adapter = profile
                        .catalog_adapters
                        .iter()
                        .find(|adapter| adapter.catalog == catalog.catalog)
                        .ok_or_else(|| SweepError::MissingAdapter(catalog.catalog.clone()))?;
                    let catalog_port = RestCatalog::new(
                        session,
                        adapter.endpoint.create_table_location.as_deref(),
                    )
                    .and_then(|catalog| catalog.bind(&fixture))
                    .map_err(|error| SweepError::CatalogSetup {
                        catalog: catalog.catalog.clone(),
                        detail: error.to_string(),
                    })?;
                    let config = RoundExecutionConfig::new(
                        catalog.catalog.as_str(),
                        round.repetition,
                        round.kind,
                        fixture,
                        workload.clone(),
                        &parameters.object_store.bucket,
                    )?;
                    execute_round(catalog_port, store.clone(), config).await?
                }
                CatalogConnectionOutcome::Failed(failure) => {
                    CatalogRoundOutcome::NegotiationFailed { failure }
                }
            };
            let passed = matches!(
                &outcome,
                CatalogRoundOutcome::Executed { execution } if execution.passed()
            );
            progress(SweepProgress::Completed {
                repetition: round.repetition,
                kind: round.kind,
                position,
                catalog: catalog.catalog.clone(),
                passed,
            });
            rounds.push(CatalogRoundTranscript {
                repetition: round.repetition,
                kind: round.kind,
                position,
                catalog: catalog.clone(),
                negotiation: connection.evidence,
                outcome,
            });
        }
    }

    redactions.sort();
    redactions.dedup();
    let (aggregates, ranking, classification) = aggregate_contention(&plan, &rounds)?;
    let transcript = ContentionTranscript {
        format: CONTENTION_TRANSCRIPT_FORMAT.to_owned(),
        scenario: TranscriptScenario {
            id: scenario.id.clone(),
            version: scenario.version,
        },
        contract_digests,
        profile: TranscriptProfile {
            id: profile.id.clone(),
            resolved_at: profile.resolved_at.clone(),
        },
        runner,
        fixture_id: fixture_id.to_owned(),
        parameters: parameters.clone(),
        rounds,
        aggregates,
        ranking,
        classification,
        sanitization: ContentionSanitization {
            policy: "catalog-bench/value-safe-contention-v1".to_owned(),
            redactions,
            raw_secrets_persisted: false,
            raw_response_body_persisted: false,
            raw_request_identities_persisted: false,
            write_mode: "create-new".to_owned(),
        },
    };
    transcript.audit_serialized_values(&sensitive_values)?;
    Ok(transcript)
}

async fn execute_round(
    catalog_port: RestCatalogFixture,
    store: ObjectStoreAuditor,
    config: RoundExecutionConfig,
) -> Result<CatalogRoundOutcome, SweepError> {
    let execution = run_contention_round(catalog_port, store, config).await?;
    Ok(CatalogRoundOutcome::Executed {
        execution: Box::new(execution),
    })
}

pub fn verify_runner(
    profile: &Profile,
    observation: &RunnerObservation,
) -> Result<RunnerTranscript, SweepError> {
    let runner = profile
        .components
        .iter()
        .find(|component| component.id.as_str() == RUNNER_COMPONENT_ID)
        .ok_or(SweepError::MissingRunner)?;
    if runner.kind != ComponentKind::BenchmarkHarness {
        return Err(SweepError::InvalidRunnerComponent);
    }
    let source_revision = runner
        .source
        .as_ref()
        .map(|source| source.revision.as_str())
        .ok_or(SweepError::MissingRunnerSource)?;
    let profile_runtime_matches = observation.operating_system == profile.platform.operating_system
        && observation.architecture == profile.platform.architecture;
    let profile_source_matches = observation.source_revision == source_revision
        && observation.source_revision == runner.version;
    let transcript = RunnerTranscript {
        component: runner.id.clone(),
        name: runner.name.clone(),
        version: runner.version.clone(),
        source_revision: observation.source_revision.clone(),
        operating_system: observation.operating_system.clone(),
        architecture: observation.architecture.clone(),
        profile_runtime_matches,
        profile_source_matches,
    };
    if !profile_runtime_matches {
        return Err(SweepError::RunnerRuntimeMismatch {
            expected_operating_system: profile.platform.operating_system.clone(),
            expected_architecture: profile.platform.architecture.clone(),
            observed_operating_system: observation.operating_system.clone(),
            observed_architecture: observation.architecture.clone(),
        });
    }
    if !profile_source_matches {
        return Err(SweepError::RunnerSourceMismatch {
            expected: source_revision.to_owned(),
            observed: observation.source_revision.clone(),
        });
    }
    Ok(transcript)
}

fn sensitive_runtime_values<F>(
    profile: &Profile,
    parameters: &crate::policy::ContentionParameters,
    getenv: &F,
) -> Vec<String>
where
    F: Fn(&str) -> Option<String>,
{
    let mut names = vec![
        parameters.object_store.access_key_env.as_str(),
        parameters.object_store.secret_key_env.as_str(),
    ];
    for adapter in &profile.catalog_adapters {
        if let CatalogAuthentication::OAuth2ClientCredentials {
            client_id_env,
            client_secret_env,
            ..
        } = &adapter.authentication
        {
            names.extend([client_id_env.as_str(), client_secret_env.as_str()]);
        }
    }
    let mut values = names
        .into_iter()
        .filter_map(getenv)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

fn is_git_revision(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug)]
pub enum SweepError {
    Policy(PolicyError),
    InvalidRunnerObservation,
    MissingRunner,
    InvalidRunnerComponent,
    MissingRunnerSource,
    RunnerRuntimeMismatch {
        expected_operating_system: String,
        expected_architecture: String,
        observed_operating_system: String,
        observed_architecture: String,
    },
    RunnerSourceMismatch {
        expected: String,
        observed: String,
    },
    MissingAdapter(ComponentId),
    CatalogSetup {
        catalog: ComponentId,
        detail: String,
    },
    ObjectStore(ObjectStoreFailure),
    Workflow(WorkflowError),
    Aggregation(AggregationError),
    Sanitization(SanitizationViolation),
    CountOverflow,
}

impl Display for SweepError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Policy(error) => Display::fmt(error, formatter),
            Self::InvalidRunnerObservation => formatter.write_str(
                "runner observation requires an OS, architecture, and 40-character Git revision",
            ),
            Self::MissingRunner => formatter.write_str("profile omits the contention runner"),
            Self::InvalidRunnerComponent => {
                formatter.write_str("profile contention runner has the wrong component kind")
            }
            Self::MissingRunnerSource => {
                formatter.write_str("profile contention runner has no source revision")
            }
            Self::RunnerRuntimeMismatch {
                expected_operating_system,
                expected_architecture,
                observed_operating_system,
                observed_architecture,
            } => write!(
                formatter,
                "runner runtime is {observed_operating_system}/{observed_architecture}, expected {expected_operating_system}/{expected_architecture}"
            ),
            Self::RunnerSourceMismatch { expected, observed } => write!(
                formatter,
                "runner source revision is {observed}, expected {expected}"
            ),
            Self::MissingAdapter(catalog) => {
                write!(formatter, "profile omits adapter for catalog `{catalog}`")
            }
            Self::CatalogSetup { catalog, detail } => {
                write!(formatter, "catalog `{catalog}` setup failed: {detail}")
            }
            Self::ObjectStore(error) => Display::fmt(error, formatter),
            Self::Workflow(error) => Display::fmt(error, formatter),
            Self::Aggregation(error) => Display::fmt(error, formatter),
            Self::Sanitization(error) => Display::fmt(error, formatter),
            Self::CountOverflow => formatter.write_str("sweep count does not fit this runner"),
        }
    }
}

impl Error for SweepError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Policy(error) => Some(error),
            Self::ObjectStore(error) => Some(error),
            Self::Workflow(error) => Some(error),
            Self::Aggregation(error) => Some(error),
            Self::Sanitization(error) => Some(error),
            _ => None,
        }
    }
}

impl From<PolicyError> for SweepError {
    fn from(error: PolicyError) -> Self {
        Self::Policy(error)
    }
}

impl From<ObjectStoreFailure> for SweepError {
    fn from(error: ObjectStoreFailure) -> Self {
        Self::ObjectStore(error)
    }
}

impl From<WorkflowError> for SweepError {
    fn from(error: WorkflowError) -> Self {
        Self::Workflow(error)
    }
}

impl From<AggregationError> for SweepError {
    fn from(error: AggregationError) -> Self {
        Self::Aggregation(error)
    }
}

impl From<SanitizationViolation> for SweepError {
    fn from(error: SanitizationViolation) -> Self {
        Self::Sanitization(error)
    }
}
