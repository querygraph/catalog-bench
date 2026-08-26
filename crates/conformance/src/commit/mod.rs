mod fixture;
mod policy;
mod workflow;

use anyhow::Result;
use catalog_bench_common::contract::{
    AssertionCheck, CapabilityId, ComponentId, Profile, ProfileId, Scenario,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::encode_evidence;
use crate::evidence::{
    not_evaluated_assertions, passed_required_assertions, transcript_adapter, transcript_scenario,
    AuthenticationOutcome, AuthenticationTranscript, ContractDigests, ProbeAssertion,
    ProbeClassification, SanitizationTranscript, TranscriptAdapter, TranscriptScenario,
};
use crate::iceberg::CatalogRoutes;
use crate::idempotency::IdempotencyKey;
use crate::operation::{
    Fact, OperationExecution, OperationHttpRequestTranscript, OperationRecorder,
    OperationTranscript,
};
use crate::routing::{negotiate_routing, not_evaluated_config, RoutingConfigTranscript};
use crate::sanitize::contains_sensitive_value;
use crate::target::ProbeTarget;
use crate::transport::{authentication_mode, http_client};

pub use fixture::CommitFixture;

pub type CommitConfigTranscript = RoutingConfigTranscript;
pub type CommitHttpRequestTranscript = OperationHttpRequestTranscript;
pub type CommitOperationExecution = OperationExecution;
pub type CommitOperationTranscript = OperationTranscript;

pub const COMMIT_TRANSCRIPT_FORMAT: &str = "catalog-bench/commit-transcript/v1";
pub const COMMIT_SCENARIO_ID: &str = "iceberg-rest.commit.correctness";
const COMMIT_SCENARIO_VERSION: u32 = 1;
const REQUEST_TIMEOUT_MS: u64 = 30_000;

const CREATE_CAPABILITY: &str = "iceberg-rest.table.create";
const LOAD_CAPABILITY: &str = "iceberg-rest.table.load";
const UPDATE_CAPABILITY: &str = "iceberg-rest.table.update";
const DROP_CAPABILITY: &str = "iceberg-rest.table.drop";
const EXACT_RETRY_CAPABILITY: &str = "iceberg-rest.table.commit.exact-retry";
const CONTENT_BINDING_CAPABILITY: &str = "iceberg-rest.idempotency-key.content-binding";

const IDEMPOTENCY_POINTERS: [&str; 3] = [
    "/idempotency-key-lifetime",
    "/overrides/idempotency-key-lifetime",
    "/defaults/idempotency-key-lifetime",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum IdempotencyAdvertisement {
    Advertised { source: String, lifetime: String },
    NotAdvertised,
    Malformed { source: String, explanation: String },
    NotEvaluated { reason: String },
}

impl IdempotencyAdvertisement {
    fn inspect(config: Option<&Value>, config_routing: &Fact) -> Self {
        if !config_routing.passed() {
            return Self::NotEvaluated {
                reason: config_routing.explanation("config routing did not pass"),
            };
        }
        let Some(config) = config else {
            return Self::NotEvaluated {
                reason: "config response has no captured JSON body".to_owned(),
            };
        };

        for pointer in IDEMPOTENCY_POINTERS {
            let Some(value) = config.pointer(pointer) else {
                continue;
            };
            return match value {
                Value::String(lifetime) if !lifetime.trim().is_empty() => Self::Advertised {
                    source: pointer.to_owned(),
                    lifetime: lifetime.clone(),
                },
                Value::String(_) => Self::Malformed {
                    source: pointer.to_owned(),
                    explanation: "idempotency-key-lifetime is empty".to_owned(),
                },
                _ => Self::Malformed {
                    source: pointer.to_owned(),
                    explanation: "idempotency-key-lifetime must be a nonempty string".to_owned(),
                },
            };
        }
        Self::NotAdvertised
    }

    fn fact(&self) -> Fact {
        match self {
            Self::Advertised { .. } => Fact::Pass,
            Self::NotAdvertised => {
                Fact::NotEvaluated("config does not advertise idempotency-key-lifetime".to_owned())
            }
            Self::Malformed {
                source,
                explanation,
            } => Fact::Fail(format!(
                "malformed advertisement at `{source}`: {explanation}"
            )),
            Self::NotEvaluated { reason } => Fact::NotEvaluated(reason.clone()),
        }
    }

    fn advertised(&self) -> bool {
        matches!(self, Self::Advertised { .. })
    }

    fn unavailable_reason(&self) -> String {
        match self {
            Self::NotAdvertised => "config does not advertise idempotency-key-lifetime".to_owned(),
            Self::Malformed {
                source,
                explanation,
            } => format!("malformed advertisement at `{source}`: {explanation}"),
            Self::NotEvaluated { reason } => reason.clone(),
            Self::Advertised { .. } => "idempotency support is available".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitTranscript {
    pub format: String,
    pub profile: ProfileId,
    pub scenario: TranscriptScenario,
    pub contract_digests: ContractDigests,
    pub adapter: TranscriptAdapter,
    pub fixture: CommitFixture,
    pub classification: ProbeClassification,
    pub authentication: AuthenticationTranscript,
    pub config: CommitConfigTranscript,
    pub idempotency: IdempotencyAdvertisement,
    pub operations: Vec<CommitOperationTranscript>,
    pub assertions: Vec<ProbeAssertion>,
    pub sanitization: SanitizationTranscript,
}

impl CommitTranscript {
    #[must_use]
    pub fn passed(&self) -> bool {
        matches!(self.classification, ProbeClassification::Pass)
    }
}

/// Run deterministic Iceberg REST commit-correctness checks against one adapter.
///
/// Observed protocol failures become transcript assertions. `Err` is reserved
/// for invalid contracts, unsafe fixture identifiers, or internal invariants.
pub async fn run_commit_probe<F>(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &ComponentId,
    fixture_id: &str,
    contract_digests: ContractDigests,
    getenv: F,
) -> Result<CommitTranscript>
where
    F: Fn(&str) -> Option<String>,
{
    policy::validate_invocation(profile, scenario, catalog)?;
    let target = ProbeTarget::resolve(profile, scenario, catalog)?;
    let fixture = CommitFixture::new(catalog, fixture_id)?;

    if let Some(limitation) = target.first_required_limitation(scenario) {
        let reason = format!(
            "required capability `{}` is declared unsupported before execution",
            limitation.capability
        );
        return Ok(CommitTranscript {
            format: COMMIT_TRANSCRIPT_FORMAT.to_owned(),
            profile: profile.id.clone(),
            scenario: transcript_scenario(scenario),
            contract_digests,
            adapter: transcript_adapter(target.adapter, target.component),
            fixture,
            classification: ProbeClassification::Unsupported {
                capability: limitation.capability.clone(),
                attributed_to: limitation.attributed_to,
                explanation: limitation.explanation.clone(),
            },
            authentication: AuthenticationTranscript {
                mode: authentication_mode(&target.adapter.authentication).to_owned(),
                outcome: AuthenticationOutcome::NotAttempted,
                token_url: None,
                scope: None,
                http_status: None,
            },
            config: not_evaluated_config(target.adapter, &reason)?,
            idempotency: IdempotencyAdvertisement::NotEvaluated {
                reason: reason.clone(),
            },
            operations: Vec::new(),
            assertions: not_evaluated_assertions(scenario, &reason),
            sanitization: SanitizationTranscript {
                policy: "automated-v1".to_owned(),
                redactions: Vec::new(),
                raw_secrets_persisted: false,
                raw_response_body_persisted: false,
            },
        });
    }

    let optional_operations = OptionalCommitOperations {
        exact_retry_limitation: optional_limitation(target.adapter, EXACT_RETRY_CAPABILITY),
        content_binding_limitation: optional_limitation(target.adapter, CONTENT_BINDING_CAPABILITY),
    };
    let create_locations = crate::table_protocol::TableCreateLocations::new(
        target.adapter.endpoint.create_table_location.as_deref(),
    )?;
    let client = http_client(REQUEST_TIMEOUT_MS)?;
    let routing = negotiate_routing(&client, target.adapter, &getenv).await?;
    let idempotency = IdempotencyAdvertisement::inspect(
        routing
            .config
            .response
            .as_ref()
            .and_then(crate::evidence::HttpResponseTranscript::json),
        &routing.config_routing,
    );
    let idempotency_key = (idempotency.advertised()
        && optional_operations.exact_retry_limitation.is_none())
    .then(IdempotencyKey::generate);

    let authentication = routing.authentication;
    let config = routing.config;
    let codec = routing.codec;
    let mut redactions = routing.redactions;
    let mut facts = CommitFacts::new(
        authentication.failure.is_none(),
        routing.config_routing,
        idempotency.fact(),
    );
    let mut recorder = OperationRecorder::new(
        &client,
        authentication.bearer_token.as_deref(),
        &authentication.sensitive_values,
    );

    let routes = codec
        .ok_or_else(|| anyhow::anyhow!("commit routing unavailable after config negotiation"))
        .and_then(|codec| CatalogRoutes::new(target.adapter, &config.prefix, codec, "commit"));
    match (routes, &facts.config_routing) {
        (Ok(routes), Fact::Pass) => {
            workflow::execute_commit_workflow(
                &mut recorder,
                &routes,
                &fixture,
                &create_locations,
                CommitIdempotency {
                    advertisement: &idempotency,
                    operations: &optional_operations,
                    key: idempotency_key.as_ref(),
                },
                &mut facts,
            )
            .await?;
        }
        (Err(error), _) => facts.skip_commit_scenario(&error.to_string()),
        (Ok(_), fact) => facts.skip_commit_scenario(&fact.explanation("config routing failed")),
    }

    let (operations, operation_redactions) = recorder.finish();
    redactions.extend(operation_redactions);
    redactions.sort();
    redactions.dedup();
    let serialized = encode_evidence(&(
        &authentication.transcript,
        &config,
        &idempotency,
        &operations,
    ))?;
    let sensitive_values = authentication
        .sensitive_values
        .iter()
        .cloned()
        .chain(idempotency_key.as_ref().map(|key| key.as_str().to_owned()))
        .collect::<Vec<_>>();
    let transcript_sanitized = !contains_sensitive_value(&serialized, &sensitive_values);
    facts.transcript_sanitized = Fact::from_bool(
        transcript_sanitized,
        "sanitized evidence still contains a credential, token, or idempotency key",
    );
    let assertions = policy::evaluate_assertions(scenario, &facts);
    let classification = if passed_required_assertions(&assertions) {
        ProbeClassification::Pass
    } else {
        ProbeClassification::Fail {
            summary: "one or more required commit-correctness assertions did not pass".to_owned(),
        }
    };

    Ok(CommitTranscript {
        format: COMMIT_TRANSCRIPT_FORMAT.to_owned(),
        profile: profile.id.clone(),
        scenario: transcript_scenario(scenario),
        contract_digests,
        adapter: transcript_adapter(target.adapter, target.component),
        fixture,
        classification,
        authentication: authentication.transcript,
        config,
        idempotency,
        operations,
        assertions,
        sanitization: SanitizationTranscript {
            policy: "automated-v1".to_owned(),
            redactions,
            raw_secrets_persisted: !transcript_sanitized,
            raw_response_body_persisted: false,
        },
    })
}

fn optional_limitation(
    adapter: &catalog_bench_common::contract::CatalogAdapter,
    capability: &str,
) -> Option<String> {
    adapter
        .capabilities
        .limitation(&CapabilityId::new(capability))
        .map(|limitation| {
            format!(
                "profile declares `{capability}` unsupported: {}",
                limitation.explanation
            )
        })
}

pub(super) struct OptionalCommitOperations {
    exact_retry_limitation: Option<String>,
    content_binding_limitation: Option<String>,
}

#[derive(Clone, Copy)]
pub(super) struct CommitIdempotency<'a> {
    advertisement: &'a IdempotencyAdvertisement,
    operations: &'a OptionalCommitOperations,
    key: Option<&'a IdempotencyKey>,
}

pub(super) struct CommitFacts {
    authentication: Fact,
    config_routing: Fact,
    fixture_isolated: Fact,
    fixture_ready: Fact,
    current_requirements: Fact,
    schema_transition: Fact,
    stale_rejection: Fact,
    idempotency_advertisement: Fact,
    exact_replay: Fact,
    content_binding: Fact,
    required_final: Fact,
    cleanup: Fact,
    transcript_sanitized: Fact,
}

impl CommitFacts {
    fn new(authentication_ready: bool, config_routing: Fact, advertisement: Fact) -> Self {
        let pending = || Fact::NotEvaluated("commit workflow did not reach this check".to_owned());
        Self {
            authentication: Fact::from_bool(
                authentication_ready,
                "authentication negotiation did not complete",
            ),
            config_routing,
            fixture_isolated: pending(),
            fixture_ready: pending(),
            current_requirements: pending(),
            schema_transition: pending(),
            stale_rejection: pending(),
            idempotency_advertisement: advertisement,
            exact_replay: pending(),
            content_binding: pending(),
            required_final: pending(),
            cleanup: pending(),
            transcript_sanitized: pending(),
        }
    }

    fn skip_after_preflight(&mut self, reason: &str) {
        self.fixture_ready = Fact::NotEvaluated(reason.to_owned());
        self.skip_after_fixture(reason);
        self.cleanup = Fact::NotEvaluated(reason.to_owned());
    }

    fn skip_after_fixture(&mut self, reason: &str) {
        self.current_requirements = Fact::NotEvaluated(reason.to_owned());
        self.schema_transition = Fact::NotEvaluated(reason.to_owned());
        self.stale_rejection = Fact::NotEvaluated(reason.to_owned());
        self.exact_replay = Fact::NotEvaluated(reason.to_owned());
        self.content_binding = Fact::NotEvaluated(reason.to_owned());
        self.required_final = Fact::NotEvaluated(reason.to_owned());
    }

    fn skip_commit_scenario(&mut self, reason: &str) {
        self.fixture_isolated = Fact::NotEvaluated(reason.to_owned());
        self.skip_after_preflight(reason);
    }

    fn for_assertion(&self, check: &AssertionCheck) -> Fact {
        if matches!(check, AssertionCheck::ExactReplay) {
            return self.exact_replay.clone();
        }
        let AssertionCheck::Custom { name, .. } = check else {
            return Fact::Fail("commit policy received an unsupported assertion kind".to_owned());
        };
        match name.as_str() {
            "querygraph/catalog-bench/authentication-ready-v1" => self.authentication.clone(),
            "querygraph/catalog-bench/commit-config-routing-v1" => self.config_routing.clone(),
            "querygraph/catalog-bench/commit-fixture-isolation-v1" => self.fixture_isolated.clone(),
            "querygraph/catalog-bench/commit-fixture-v1" => self.fixture_ready.clone(),
            "querygraph/catalog-bench/commit-requirements-v1" => self.current_requirements.clone(),
            "querygraph/catalog-bench/commit-schema-transition-v1" => {
                self.schema_transition.clone()
            }
            "querygraph/catalog-bench/commit-stale-rejection-v1" => self.stale_rejection.clone(),
            "querygraph/catalog-bench/idempotency-advertisement-v1" => {
                self.idempotency_advertisement.clone()
            }
            "querygraph/catalog-bench/idempotency-content-binding-v1" => {
                self.content_binding.clone()
            }
            "querygraph/catalog-bench/commit-final-state-v1" => self.required_final.clone(),
            "querygraph/catalog-bench/commit-cleanup-v1" => self.cleanup.clone(),
            "querygraph/catalog-bench/sanitized-commit-transcript-v1" => {
                self.transcript_sanitized.clone()
            }
            _ => Fact::Fail(format!("commit policy received unknown assertion `{name}`")),
        }
    }
}
