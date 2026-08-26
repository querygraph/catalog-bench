mod fixture;
mod policy;
mod workflow;

use anyhow::Result;
use catalog_bench_common::contract::{
    AssertionCheck, CapabilityId, ComponentId, Profile, ProfileId, Scenario,
};
use serde::{Deserialize, Serialize};

use crate::encode_evidence;
use crate::evidence::{
    not_evaluated_assertions, passed_required_assertions, transcript_adapter, transcript_scenario,
    AuthenticationOutcome, AuthenticationTranscript, ContractDigests, ProbeAssertion,
    ProbeClassification, SanitizationTranscript, TranscriptAdapter, TranscriptScenario,
};
use crate::iceberg::CatalogRoutes;
use crate::operation::{
    Fact, OperationExecution, OperationHttpRequestTranscript, OperationRecorder,
    OperationTranscript,
};
use crate::routing::{negotiate_routing, not_evaluated_config, RoutingConfigTranscript};
use crate::sanitize::contains_sensitive_value;
use crate::target::ProbeTarget;
use crate::transport::{authentication_mode, http_client};

pub use fixture::{TableFixture, TableIdentifier};

pub type TableConfigTranscript = RoutingConfigTranscript;
pub type TableHttpRequestTranscript = OperationHttpRequestTranscript;
pub type TableOperationExecution = OperationExecution;
pub type TableOperationTranscript = OperationTranscript;

pub const TABLE_TRANSCRIPT_FORMAT: &str = "catalog-bench/table-transcript/v1";
pub const TABLE_SCENARIO_ID: &str = "iceberg-rest.table.behavior";
const TABLE_SCENARIO_VERSION: u32 = 1;
const REQUEST_TIMEOUT_MS: u64 = 30_000;

const CREATE_CAPABILITY: &str = "iceberg-rest.table.create";
const LIST_CAPABILITY: &str = "iceberg-rest.table.list";
const LOAD_CAPABILITY: &str = "iceberg-rest.table.load";
const REGISTER_CAPABILITY: &str = "iceberg-rest.table.register";
const RENAME_CAPABILITY: &str = "iceberg-rest.table.rename";
const UPDATE_CAPABILITY: &str = "iceberg-rest.table.update";
const DROP_CAPABILITY: &str = "iceberg-rest.table.drop";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum TablePaginationTranscript {
    Paginated { pages: usize, unique_tables: usize },
    UnpaginatedFallback { unique_tables: usize },
    Failed { explanation: String },
    NotEvaluated { reason: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableTranscript {
    pub format: String,
    pub profile: ProfileId,
    pub scenario: TranscriptScenario,
    pub contract_digests: ContractDigests,
    pub adapter: TranscriptAdapter,
    pub fixture: TableFixture,
    pub classification: ProbeClassification,
    pub authentication: AuthenticationTranscript,
    pub config: TableConfigTranscript,
    pub operations: Vec<TableOperationTranscript>,
    pub pagination: TablePaginationTranscript,
    pub assertions: Vec<ProbeAssertion>,
    pub sanitization: SanitizationTranscript,
}

impl TableTranscript {
    #[must_use]
    pub fn passed(&self) -> bool {
        matches!(self.classification, ProbeClassification::Pass)
    }
}

/// Run the table-behavior scenario against one validated profile adapter.
///
/// Observed protocol failures become transcript assertions. `Err` is reserved
/// for invalid contracts, unsafe fixture identifiers, or internal invariants.
pub async fn run_table_probe<F>(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &ComponentId,
    fixture_id: &str,
    contract_digests: ContractDigests,
    getenv: F,
) -> Result<TableTranscript>
where
    F: Fn(&str) -> Option<String>,
{
    policy::validate_invocation(profile, scenario, catalog)?;
    let target = ProbeTarget::resolve(profile, scenario, catalog)?;
    let fixture = TableFixture::new(catalog, fixture_id)?;

    if let Some(limitation) = target.first_required_limitation(scenario) {
        let reason = format!(
            "required capability `{}` is declared unsupported before execution",
            limitation.capability
        );
        return Ok(TableTranscript {
            format: TABLE_TRANSCRIPT_FORMAT.to_owned(),
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
            operations: Vec::new(),
            pagination: TablePaginationTranscript::NotEvaluated {
                reason: reason.clone(),
            },
            assertions: not_evaluated_assertions(scenario, &reason),
            sanitization: SanitizationTranscript {
                policy: "automated-v1".to_owned(),
                redactions: Vec::new(),
                raw_secrets_persisted: false,
                raw_response_body_persisted: false,
            },
        });
    }

    let optional_operations = OptionalTableOperations {
        rename_limitation: target
            .adapter
            .capabilities
            .limitation(&CapabilityId::new(RENAME_CAPABILITY))
            .map(|limitation| {
                format!(
                    "profile declares `{RENAME_CAPABILITY}` unsupported: {}",
                    limitation.explanation
                )
            }),
        register_limitation: target
            .adapter
            .capabilities
            .limitation(&CapabilityId::new(REGISTER_CAPABILITY))
            .map(|limitation| {
                format!(
                    "profile declares `{REGISTER_CAPABILITY}` unsupported: {}",
                    limitation.explanation
                )
            }),
    };
    let create_locations = crate::table_protocol::TableCreateLocations::new(
        target.adapter.endpoint.create_table_location.as_deref(),
    )?;

    let client = http_client(REQUEST_TIMEOUT_MS)?;
    let routing = negotiate_routing(&client, target.adapter, &getenv).await?;
    let authentication = routing.authentication;
    let config = routing.config;
    let codec = routing.codec;
    let mut redactions = routing.redactions;
    let mut facts = TableFacts::new(authentication.failure.is_none(), routing.config_routing);
    let mut recorder = OperationRecorder::new(
        &client,
        authentication.bearer_token.as_deref(),
        &authentication.sensitive_values,
    );

    let routes = codec
        .ok_or_else(|| anyhow::anyhow!("table routing unavailable after config negotiation"))
        .and_then(|codec| CatalogRoutes::new(target.adapter, &config.prefix, codec, "table"));
    let pagination = match (routes, &facts.config_routing) {
        (Ok(routes), Fact::Pass) => {
            workflow::execute_table_workflow(
                &mut recorder,
                &routes,
                &fixture,
                &create_locations,
                &optional_operations,
                &mut facts,
            )
            .await?
        }
        (Err(error), _) => {
            let reason = error.to_string();
            facts.skip_table_scenario(&reason);
            TablePaginationTranscript::NotEvaluated { reason }
        }
        (Ok(_), fact) => {
            let reason = fact.explanation("config routing did not pass");
            facts.skip_table_scenario(&reason);
            TablePaginationTranscript::NotEvaluated { reason }
        }
    };

    let (operations, operation_redactions) = recorder.finish();
    redactions.extend(operation_redactions);
    redactions.sort();
    redactions.dedup();
    let serialized = encode_evidence(&(
        &authentication.transcript,
        &config,
        &operations,
        &pagination,
    ))?;
    let transcript_sanitized =
        !contains_sensitive_value(&serialized, &authentication.sensitive_values);
    facts.transcript_sanitized = Fact::from_bool(
        transcript_sanitized,
        "sanitized evidence still contains a sensitive runtime value",
    );
    let assertions = policy::evaluate_assertions(scenario, &facts);
    let classification = if passed_required_assertions(&assertions) {
        ProbeClassification::Pass
    } else {
        ProbeClassification::Fail {
            summary: "one or more required table-behavior assertions did not pass".to_owned(),
        }
    };

    Ok(TableTranscript {
        format: TABLE_TRANSCRIPT_FORMAT.to_owned(),
        profile: profile.id.clone(),
        scenario: transcript_scenario(scenario),
        contract_digests,
        adapter: transcript_adapter(target.adapter, target.component),
        fixture,
        classification,
        authentication: authentication.transcript,
        config,
        operations,
        pagination,
        assertions,
        sanitization: SanitizationTranscript {
            policy: "automated-v1".to_owned(),
            redactions,
            raw_secrets_persisted: !transcript_sanitized,
            raw_response_body_persisted: false,
        },
    })
}

pub(super) struct OptionalTableOperations {
    rename_limitation: Option<String>,
    register_limitation: Option<String>,
}

pub(super) struct TableFacts {
    authentication: Fact,
    config_routing: Fact,
    fixture_isolated: Fact,
    fixture_namespace: Fact,
    create: Fact,
    list: Fact,
    load: Fact,
    pagination: Fact,
    update: Fact,
    duplicate: Fact,
    missing_table: Fact,
    missing_namespace: Fact,
    rename: Fact,
    drop_table: Fact,
    register: Fact,
    cleanup: Fact,
    transcript_sanitized: Fact,
}

impl TableFacts {
    fn new(authentication_ready: bool, config_routing: Fact) -> Self {
        let pending = || Fact::NotEvaluated("table workflow did not reach this check".to_owned());
        Self {
            authentication: Fact::from_bool(
                authentication_ready,
                "authentication negotiation did not complete",
            ),
            config_routing,
            fixture_isolated: pending(),
            fixture_namespace: pending(),
            create: pending(),
            list: pending(),
            load: pending(),
            pagination: pending(),
            update: pending(),
            duplicate: pending(),
            missing_table: pending(),
            missing_namespace: pending(),
            rename: pending(),
            drop_table: pending(),
            register: pending(),
            cleanup: pending(),
            transcript_sanitized: pending(),
        }
    }

    fn skip_table_behavior(&mut self, reason: &str) {
        self.create = Fact::NotEvaluated(reason.to_owned());
        self.list = Fact::NotEvaluated(reason.to_owned());
        self.load = Fact::NotEvaluated(reason.to_owned());
        self.pagination = Fact::NotEvaluated(reason.to_owned());
        self.update = Fact::NotEvaluated(reason.to_owned());
        self.duplicate = Fact::NotEvaluated(reason.to_owned());
        self.rename = Fact::NotEvaluated(reason.to_owned());
        self.drop_table = Fact::NotEvaluated(reason.to_owned());
        self.register = Fact::NotEvaluated(reason.to_owned());
    }

    fn skip_mutating_behavior(&mut self, reason: &str) {
        self.fixture_namespace = Fact::NotEvaluated(reason.to_owned());
        self.skip_table_behavior(reason);
        self.missing_table = Fact::NotEvaluated(reason.to_owned());
        self.cleanup = Fact::NotEvaluated(reason.to_owned());
    }

    fn skip_table_scenario(&mut self, reason: &str) {
        self.fixture_isolated = Fact::NotEvaluated(reason.to_owned());
        self.skip_mutating_behavior(reason);
        self.missing_namespace = Fact::NotEvaluated(reason.to_owned());
    }

    fn for_assertion(&self, check: &AssertionCheck) -> Fact {
        let AssertionCheck::Custom { name, .. } = check else {
            return Fact::Fail("table policy received a non-custom assertion".to_owned());
        };
        match name.as_str() {
            "querygraph/catalog-bench/authentication-ready-v1" => self.authentication.clone(),
            "querygraph/catalog-bench/table-config-routing-v1" => self.config_routing.clone(),
            "querygraph/catalog-bench/table-fixture-isolation-v1" => self.fixture_isolated.clone(),
            "querygraph/catalog-bench/table-namespace-create-v1" => self.fixture_namespace.clone(),
            "querygraph/catalog-bench/table-create-v1" => self.create.clone(),
            "querygraph/catalog-bench/table-list-v1" => self.list.clone(),
            "querygraph/catalog-bench/table-load-v1" => self.load.clone(),
            "querygraph/catalog-bench/table-pagination-v1" => self.pagination.clone(),
            "querygraph/catalog-bench/table-update-v1" => self.update.clone(),
            "querygraph/catalog-bench/table-duplicate-error-v1" => self.duplicate.clone(),
            "querygraph/catalog-bench/table-missing-error-v1" => self.missing_table.clone(),
            "querygraph/catalog-bench/table-missing-namespace-error-v1" => {
                self.missing_namespace.clone()
            }
            "querygraph/catalog-bench/table-rename-v1" => self.rename.clone(),
            "querygraph/catalog-bench/table-drop-v1" => self.drop_table.clone(),
            "querygraph/catalog-bench/table-register-v1" => self.register.clone(),
            "querygraph/catalog-bench/table-cleanup-v1" => self.cleanup.clone(),
            "querygraph/catalog-bench/sanitized-http-transcript-v1" => {
                self.transcript_sanitized.clone()
            }
            _ => Fact::Fail(format!("table policy received unknown assertion `{name}`")),
        }
    }
}
