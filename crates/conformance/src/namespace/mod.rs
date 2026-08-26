mod policy;
mod routes;

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use catalog_bench_common::contract::{AssertionCheck, ComponentId, Profile, ProfileId, Scenario};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::config::{resolve_prefix, PrefixResolution};
use crate::encode_evidence;
use crate::evidence::{
    not_evaluated_assertions, passed_required_assertions, transcript_adapter, transcript_scenario,
    AuthenticationOutcome, AuthenticationTranscript, ContractDigests, HttpRequestTranscript,
    HttpResponseTranscript, ProbeAssertion, ProbeClassification, ProbeFailure,
    SanitizationTranscript, TranscriptAdapter, TranscriptScenario,
};
use crate::operation::{
    all_results, parse_json_response, validate_error_response, validate_status, Fact, Observation,
    OperationExecution, OperationHttpRequestTranscript, OperationRecorder, OperationTranscript,
};
use crate::sanitize::contains_sensitive_value;
use crate::target::ProbeTarget;
use crate::transport::{
    acquire_authentication, authentication_mode, endpoint_url, execute_json_request, http_client,
    CapturedResponse, REDACTED,
};

pub use crate::iceberg::{NamespaceIdentifier, NamespaceSeparatorResolution};
pub use routes::NamespaceFixture;

pub type NamespaceHttpRequestTranscript = OperationHttpRequestTranscript;
pub type NamespaceOperationExecution = OperationExecution;
pub type NamespaceOperationTranscript = OperationTranscript;

use crate::iceberg::{CatalogRoutes, NamespaceCodec};

pub const NAMESPACE_TRANSCRIPT_FORMAT: &str = "catalog-bench/namespace-transcript/v1";
pub const NAMESPACE_SCENARIO_ID: &str = "iceberg-rest.namespace.behavior";
const NAMESPACE_SCENARIO_VERSION: u32 = 1;
const REQUEST_TIMEOUT_MS: u64 = 30_000;
const PAGE_SIZE: usize = 1;
const MAXIMUM_PAGES: usize = 64;

const CREATE_CAPABILITY: &str = "iceberg-rest.namespace.create";
const LIST_CAPABILITY: &str = "iceberg-rest.namespace.list";
const LOAD_CAPABILITY: &str = "iceberg-rest.namespace.load";
const UPDATE_CAPABILITY: &str = "iceberg-rest.namespace.update-properties";
const DROP_CAPABILITY: &str = "iceberg-rest.namespace.drop";
const HIERARCHY_CAPABILITY: &str = "iceberg-rest.namespace.hierarchy";
const PAGINATION_CAPABILITY: &str = "iceberg-rest.namespace.pagination";
const DUPLICATE_CAPABILITY: &str = "iceberg-rest.namespace.error.duplicate";
const MISSING_PARENT_CAPABILITY: &str = "iceberg-rest.namespace.error.missing-parent";

const OWNER_PROPERTY: &str = "owner";
const REMOVE_PROPERTY: &str = "c1-04.remove";
const STATE_PROPERTY: &str = "c1-04.state";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceConfigTranscript {
    pub request: HttpRequestTranscript,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<HttpResponseTranscript>,
    pub prefix: PrefixResolution,
    pub namespace_separator: NamespaceSeparatorResolution,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<ProbeFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum PaginationTranscript {
    Paginated {
        pages: usize,
        unique_namespaces: usize,
    },
    UnpaginatedFallback {
        unique_namespaces: usize,
    },
    Failed {
        explanation: String,
    },
    NotEvaluated {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceTranscript {
    pub format: String,
    pub profile: ProfileId,
    pub scenario: TranscriptScenario,
    pub contract_digests: ContractDigests,
    pub adapter: TranscriptAdapter,
    pub fixture: NamespaceFixture,
    pub classification: ProbeClassification,
    pub authentication: AuthenticationTranscript,
    pub config: NamespaceConfigTranscript,
    pub operations: Vec<NamespaceOperationTranscript>,
    pub pagination: PaginationTranscript,
    pub assertions: Vec<ProbeAssertion>,
    pub sanitization: SanitizationTranscript,
}

impl NamespaceTranscript {
    #[must_use]
    pub fn passed(&self) -> bool {
        matches!(self.classification, ProbeClassification::Pass)
    }
}

/// Run the namespace-behavior scenario against one validated profile adapter.
///
/// Observed protocol failures become transcript assertions. `Err` is reserved
/// for invalid contracts, unsafe fixture identifiers, or internal invariants.
pub async fn run_namespace_probe<F>(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &ComponentId,
    fixture_id: &str,
    contract_digests: ContractDigests,
    getenv: F,
) -> Result<NamespaceTranscript>
where
    F: Fn(&str) -> Option<String>,
{
    policy::validate_invocation(profile, scenario, catalog)?;
    let target = ProbeTarget::resolve(profile, scenario, catalog)?;
    let fixture = NamespaceFixture::new(catalog, fixture_id)?;
    let config_url = endpoint_url(
        &target.adapter.endpoint.base_url,
        &target.adapter.endpoint.config.path,
        &target.adapter.endpoint.config.query,
    )?;
    let unauthenticated_config_request = config_request(config_url.as_str(), false);

    if let Some(limitation) = target.first_required_limitation(scenario) {
        let reason = format!(
            "required capability `{}` is declared unsupported before execution",
            limitation.capability
        );
        return Ok(NamespaceTranscript {
            format: NAMESPACE_TRANSCRIPT_FORMAT.to_owned(),
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
            config: NamespaceConfigTranscript {
                request: unauthenticated_config_request,
                response: None,
                prefix: PrefixResolution::NotEvaluated {
                    reason: reason.clone(),
                },
                namespace_separator: NamespaceSeparatorResolution::NotEvaluated {
                    reason: reason.clone(),
                },
                failure: None,
            },
            operations: Vec::new(),
            pagination: PaginationTranscript::NotEvaluated {
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

    let client = http_client(REQUEST_TIMEOUT_MS)?;
    let authentication = acquire_authentication(&client, target.adapter, &getenv).await;
    let config_request = config_request(config_url.as_str(), authentication.bearer_token.is_some());
    let captured_config = if authentication.failure.is_none() {
        execute_json_request(
            &client,
            Method::GET,
            config_url,
            authentication.bearer_token.as_deref(),
            None,
            &authentication.sensitive_values,
            "config",
        )
        .await
    } else {
        CapturedResponse {
            response: None,
            private_json: None,
            redactions: Vec::new(),
            failure: None,
        }
    };

    let config_json = captured_config.private_json;
    let prefix = resolve_prefix(target.adapter, config_json.as_ref());
    let (namespace_separator, codec) = NamespaceCodec::resolve(config_json.as_ref());
    let config_ready = validate_config_routing(
        authentication.failure.as_ref(),
        captured_config.response.as_ref(),
        config_json.as_ref(),
        &prefix,
        codec.as_ref(),
    );
    let config = NamespaceConfigTranscript {
        request: config_request,
        response: captured_config.response,
        prefix: prefix.clone(),
        namespace_separator,
        failure: authentication.failure.clone().or(captured_config.failure),
    };

    let mut redactions = captured_config
        .redactions
        .into_iter()
        .map(|path| format!("config.{path}"))
        .collect::<Vec<_>>();
    if authentication.bearer_token.is_some() {
        redactions.extend([
            "config.request.headers.authorization".to_owned(),
            "authentication.oauth2-request-credentials".to_owned(),
            "authentication.oauth2-response-token".to_owned(),
        ]);
    }

    let mut facts = NamespaceFacts::new(
        authentication.failure.is_none(),
        Fact::from_result(config_ready),
    );
    let mut recorder = OperationRecorder::new(
        &client,
        authentication.bearer_token.as_deref(),
        &authentication.sensitive_values,
    );

    let routes = codec
        .ok_or_else(|| anyhow::anyhow!("namespace routing unavailable after config negotiation"))
        .and_then(|codec| CatalogRoutes::new(target.adapter, &prefix, codec, "namespace"));
    let pagination = match (routes, &facts.config_routing) {
        (Ok(routes), Fact::Pass) => {
            execute_namespace_workflow(&mut recorder, &routes, &fixture, &mut facts).await?
        }
        (Err(error), _) => {
            let reason = error.to_string();
            facts.skip_namespace_behavior(&reason);
            PaginationTranscript::NotEvaluated { reason }
        }
        (Ok(_), fact) => {
            let reason = fact.explanation("config routing did not pass");
            facts.skip_namespace_behavior(&reason);
            PaginationTranscript::NotEvaluated { reason }
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
            summary: "one or more required namespace-behavior assertions did not pass".to_owned(),
        }
    };

    Ok(NamespaceTranscript {
        format: NAMESPACE_TRANSCRIPT_FORMAT.to_owned(),
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

fn config_request(url: &str, authenticated: bool) -> HttpRequestTranscript {
    let mut headers = BTreeMap::from([("accept".to_owned(), "application/json".to_owned())]);
    if authenticated {
        headers.insert("authorization".to_owned(), REDACTED.to_owned());
    }
    HttpRequestTranscript {
        method: "GET".to_owned(),
        url: url.to_owned(),
        headers,
    }
}

fn validate_config_routing(
    authentication_failure: Option<&ProbeFailure>,
    response: Option<&HttpResponseTranscript>,
    private_json: Option<&Value>,
    prefix: &PrefixResolution,
    codec: Option<&NamespaceCodec>,
) -> std::result::Result<(), String> {
    if let Some(failure) = authentication_failure {
        return Err(failure.explanation.clone());
    }
    let response = response.ok_or_else(|| "no config response was received".to_owned())?;
    if response.status != 200 {
        return Err(format!(
            "config returned HTTP {} instead of 200",
            response.status
        ));
    }
    if !response
        .headers
        .get("content-type")
        .is_some_and(|value| is_json_media_type(value))
    {
        return Err("config response did not declare application/json".to_owned());
    }
    if private_json.is_none() {
        return Err("config response did not contain valid captured JSON".to_owned());
    }
    match prefix {
        PrefixResolution::Unprefixed
        | PrefixResolution::Static { .. }
        | PrefixResolution::Negotiated { .. } => {}
        PrefixResolution::Failed { explanation } => return Err(explanation.clone()),
        PrefixResolution::NotEvaluated { reason } => return Err(reason.clone()),
    }
    if codec.is_none() {
        return Err("namespace separator did not resolve".to_owned());
    }
    Ok(())
}

async fn execute_namespace_workflow(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &NamespaceFixture,
    facts: &mut NamespaceFacts,
) -> Result<PaginationTranscript> {
    let preflight = preflight_fixture(recorder, routes, fixture).await?;
    facts.fixture_isolated = Fact::from_result(all_results(preflight.iter()));
    if !facts.fixture_isolated.passed() {
        let reason = facts
            .fixture_isolated
            .explanation("fixture preflight did not pass");
        skip_remaining_mutations(recorder, &reason);
        facts.skip_mutating_behavior(&reason);
        facts.missing_parent = Fact::from_result(
            probe_missing_parent(recorder, routes, &fixture.missing_parent).await?,
        );
        return Ok(PaginationTranscript::NotEvaluated { reason });
    }

    let primary_create = recorder
        .attempt(
            "create-primary",
            Some(CREATE_CAPABILITY),
            Method::POST,
            routes.namespace_collection()?,
            Some(json!({
                "namespace": fixture.primary.parts(),
                "properties": {
                    (OWNER_PROPERTY): "catalog-bench",
                    (REMOVE_PROPERTY): "before"
                }
            })),
        )
        .await;
    let sibling_create = recorder
        .attempt(
            "create-sibling",
            Some(CREATE_CAPABILITY),
            Method::POST,
            routes.namespace_collection()?,
            Some(json!({"namespace": fixture.sibling.parts(), "properties": {}})),
        )
        .await;
    let primary_created = validate_namespace_response(&primary_create, 200, &fixture.primary);
    let sibling_created = validate_namespace_response(&sibling_create, 200, &fixture.sibling);

    let child_create = if primary_created.is_ok() {
        Some(
            recorder
                .attempt(
                    "create-child",
                    Some(CREATE_CAPABILITY),
                    Method::POST,
                    routes.namespace_collection()?,
                    Some(json!({"namespace": fixture.child.parts(), "properties": {}})),
                )
                .await,
        )
    } else {
        recorder.skip(
            "create-child",
            Some(CREATE_CAPABILITY),
            "primary namespace creation did not pass",
        );
        None
    };
    let child_created = child_create.as_ref().map_or_else(
        || Err("child create was not attempted".to_owned()),
        |observation| validate_namespace_response(observation, 200, &fixture.child),
    );
    facts.create = Fact::from_result(all_results([
        &primary_created,
        &sibling_created,
        &child_created,
    ]));

    if primary_created.is_ok() && sibling_created.is_ok() {
        let listing = recorder
            .attempt(
                "list-top-level",
                Some(LIST_CAPABILITY),
                Method::GET,
                routes.namespace_collection()?,
                None,
            )
            .await;
        facts.list = Fact::from_result(validate_top_level_listing(
            &listing,
            [&fixture.primary, &fixture.sibling],
        ));
    } else {
        let reason = "both top-level namespace creates must pass before listing";
        recorder.skip("list-top-level", Some(LIST_CAPABILITY), reason);
        facts.list = Fact::NotEvaluated(reason.to_owned());
    }

    if primary_created.is_ok() {
        let load = recorder
            .attempt(
                "load-primary",
                Some(LOAD_CAPABILITY),
                Method::GET,
                routes.namespace(&fixture.primary)?,
                None,
            )
            .await;
        facts.load = Fact::from_result(
            validate_namespace_response(&load, 200, &fixture.primary).map(|_| ()),
        );

        let update = recorder
            .attempt(
                "update-primary-properties",
                Some(UPDATE_CAPABILITY),
                Method::POST,
                routes.namespace_properties(&fixture.primary)?,
                Some(json!({
                    "removals": [REMOVE_PROPERTY],
                    "updates": {(STATE_PROPERTY): "after"}
                })),
            )
            .await;
        let reload = recorder
            .attempt(
                "reload-primary-properties",
                Some(UPDATE_CAPABILITY),
                Method::GET,
                routes.namespace(&fixture.primary)?,
                None,
            )
            .await;
        facts.properties = Fact::from_result(validate_property_update(&update, &reload, fixture));

        let duplicate = recorder
            .attempt(
                "create-primary-duplicate",
                Some(DUPLICATE_CAPABILITY),
                Method::POST,
                routes.namespace_collection()?,
                Some(json!({
                    "namespace": fixture.primary.parts(),
                    "properties": {
                        (OWNER_PROPERTY): "catalog-bench",
                        (REMOVE_PROPERTY): "before"
                    }
                })),
            )
            .await;
        facts.duplicate = Fact::from_result(validate_error_response(
            &duplicate,
            409,
            "AlreadyExistsException",
        ));
    } else {
        let reason = "primary namespace creation did not pass";
        for (id, capability) in [
            ("load-primary", LOAD_CAPABILITY),
            ("update-primary-properties", UPDATE_CAPABILITY),
            ("reload-primary-properties", UPDATE_CAPABILITY),
            ("create-primary-duplicate", DUPLICATE_CAPABILITY),
        ] {
            recorder.skip(id, Some(capability), reason);
        }
        facts.load = Fact::NotEvaluated(reason.to_owned());
        facts.properties = Fact::NotEvaluated(reason.to_owned());
        facts.duplicate = Fact::NotEvaluated(reason.to_owned());
    }

    if child_created.is_ok() {
        let hierarchy = recorder
            .attempt(
                "list-primary-children",
                Some(HIERARCHY_CAPABILITY),
                Method::GET,
                routes.namespaces_under(&fixture.primary)?,
                None,
            )
            .await;
        facts.hierarchy = Fact::from_result(validate_hierarchy_listing(&hierarchy, &fixture.child));
    } else {
        let reason = "multipart child creation did not pass";
        recorder.skip("list-primary-children", Some(HIERARCHY_CAPABILITY), reason);
        facts.hierarchy = Fact::NotEvaluated(reason.to_owned());
    }

    let pagination = if primary_created.is_ok() && sibling_created.is_ok() {
        let (pagination, result) = traverse_pages(recorder, routes, fixture).await?;
        facts.pagination = Fact::from_result(result);
        pagination
    } else {
        let reason = "both top-level namespace creates must pass before pagination".to_owned();
        recorder.skip("list-page-001", Some(PAGINATION_CAPABILITY), &reason);
        facts.pagination = Fact::NotEvaluated(reason.clone());
        PaginationTranscript::NotEvaluated { reason }
    };

    facts.missing_parent =
        Fact::from_result(probe_missing_parent(recorder, routes, &fixture.missing_parent).await?);
    facts.cleanup = Fact::from_result(cleanup_fixture(recorder, routes, fixture).await?);
    Ok(pagination)
}

async fn preflight_fixture(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &NamespaceFixture,
) -> Result<Vec<std::result::Result<(), String>>> {
    let mut results = Vec::new();
    for (id, namespace) in [
        ("preflight-primary", &fixture.primary),
        ("preflight-sibling", &fixture.sibling),
        ("preflight-child", &fixture.child),
    ] {
        let observation = recorder
            .attempt(
                id,
                Some(LOAD_CAPABILITY),
                Method::GET,
                routes.namespace(namespace)?,
                None,
            )
            .await;
        results.push(validate_error_response(
            &observation,
            404,
            "NoSuchNamespaceException",
        ));
    }
    Ok(results)
}

async fn probe_missing_parent(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    missing_parent: &NamespaceIdentifier,
) -> Result<std::result::Result<(), String>> {
    let observation = recorder
        .attempt(
            "list-missing-parent",
            Some(MISSING_PARENT_CAPABILITY),
            Method::GET,
            routes.namespaces_under(missing_parent)?,
            None,
        )
        .await;
    Ok(validate_error_response(
        &observation,
        404,
        "NoSuchNamespaceException",
    ))
}

async fn cleanup_fixture(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &NamespaceFixture,
) -> Result<std::result::Result<(), String>> {
    let mut results = Vec::new();
    for (id, namespace) in [
        ("drop-child", &fixture.child),
        ("drop-sibling", &fixture.sibling),
        ("drop-primary", &fixture.primary),
    ] {
        let observation = recorder
            .attempt(
                id,
                Some(DROP_CAPABILITY),
                Method::DELETE,
                routes.namespace(namespace)?,
                None,
            )
            .await;
        results.push(validate_status(&observation, &[204, 404]));
    }
    for (id, namespace) in [
        ("verify-child-absent", &fixture.child),
        ("verify-sibling-absent", &fixture.sibling),
        ("verify-primary-absent", &fixture.primary),
    ] {
        let observation = recorder
            .attempt(
                id,
                Some(DROP_CAPABILITY),
                Method::GET,
                routes.namespace(namespace)?,
                None,
            )
            .await;
        results.push(validate_status(&observation, &[404]));
    }
    Ok(all_results(results.iter()))
}

async fn traverse_pages(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &NamespaceFixture,
) -> Result<(PaginationTranscript, std::result::Result<(), String>)> {
    let mut token = String::new();
    let mut tokens = BTreeSet::new();
    let mut namespaces = BTreeSet::new();

    for page_index in 1..=MAXIMUM_PAGES {
        let observation = recorder
            .attempt(
                format!("list-page-{page_index:03}"),
                Some(PAGINATION_CAPABILITY),
                Method::GET,
                routes.namespace_page(&token, PAGE_SIZE)?,
                None,
            )
            .await;
        let page = match parse_list_response(&observation, 200) {
            Ok(page) => page,
            Err(error) => {
                return Ok((
                    PaginationTranscript::Failed {
                        explanation: error.clone(),
                    },
                    Err(error),
                ));
            }
        };
        let page_namespaces = match parse_namespaces(page.namespaces) {
            Ok(namespaces) => namespaces,
            Err(error) => {
                return Ok((
                    PaginationTranscript::Failed {
                        explanation: error.clone(),
                    },
                    Err(error),
                ));
            }
        };
        if page_namespaces.len() > PAGE_SIZE {
            if page_index == 1 && page.next_page_token.is_none() {
                let result = validate_complete_namespace_set(
                    &page_namespaces,
                    [&fixture.primary, &fixture.sibling],
                );
                return Ok((
                    PaginationTranscript::UnpaginatedFallback {
                        unique_namespaces: page_namespaces.len(),
                    },
                    result,
                ));
            }
            let error = format!(
                "page {page_index} returned {} namespaces above pageSize {PAGE_SIZE}",
                page_namespaces.len()
            );
            return Ok((
                PaginationTranscript::Failed {
                    explanation: error.clone(),
                },
                Err(error),
            ));
        }
        for namespace in page_namespaces {
            if !namespaces.insert(namespace.clone()) {
                let error = format!("pagination returned duplicate namespace {namespace:?}");
                return Ok((
                    PaginationTranscript::Failed {
                        explanation: error.clone(),
                    },
                    Err(error),
                ));
            }
        }
        match page.next_page_token {
            Some(next) if next.is_empty() => {
                let error = "pagination returned an empty next-page-token".to_owned();
                return Ok((
                    PaginationTranscript::Failed {
                        explanation: error.clone(),
                    },
                    Err(error),
                ));
            }
            Some(next) if !tokens.insert(next.clone()) => {
                let error = "pagination repeated a next-page-token".to_owned();
                return Ok((
                    PaginationTranscript::Failed {
                        explanation: error.clone(),
                    },
                    Err(error),
                ));
            }
            Some(next) => token = next,
            None => {
                let result = validate_complete_namespace_set(
                    &namespaces,
                    [&fixture.primary, &fixture.sibling],
                );
                return Ok((
                    PaginationTranscript::Paginated {
                        pages: page_index,
                        unique_namespaces: namespaces.len(),
                    },
                    result,
                ));
            }
        }
    }
    let error = format!("pagination exceeded the {MAXIMUM_PAGES}-page safety bound");
    Ok((
        PaginationTranscript::Failed {
            explanation: error.clone(),
        },
        Err(error),
    ))
}

fn validate_complete_namespace_set<'a>(
    namespaces: &BTreeSet<NamespaceIdentifier>,
    expected: impl IntoIterator<Item = &'a NamespaceIdentifier>,
) -> std::result::Result<(), String> {
    for expected in expected {
        if !namespaces.contains(expected) {
            return Err(format!(
                "listing omitted expected namespace {:?}",
                expected.parts()
            ));
        }
    }
    Ok(())
}

fn validate_top_level_listing<'a>(
    observation: &Observation,
    expected: impl IntoIterator<Item = &'a NamespaceIdentifier>,
) -> std::result::Result<(), String> {
    let response = parse_list_response(observation, 200)?;
    let namespaces = parse_namespaces(response.namespaces)?;
    validate_complete_namespace_set(&namespaces, expected)
}

fn validate_hierarchy_listing(
    observation: &Observation,
    child: &NamespaceIdentifier,
) -> std::result::Result<(), String> {
    let response = parse_list_response(observation, 200)?;
    let namespaces = parse_namespaces(response.namespaces)?;
    if namespaces == BTreeSet::from([child.clone()]) {
        Ok(())
    } else {
        Err(format!(
            "parent listing returned {namespaces:?}, expected only {:?}",
            child.parts()
        ))
    }
}

fn validate_property_update(
    update: &Observation,
    reload: &Observation,
    fixture: &NamespaceFixture,
) -> std::result::Result<(), String> {
    let update: UpdateNamespacePropertiesResponse = parse_json_response(update, 200)?;
    if !update.updated.iter().any(|key| key == STATE_PROPERTY) {
        return Err(format!("update response did not report `{STATE_PROPERTY}`"));
    }
    if !update.removed.iter().any(|key| key == REMOVE_PROPERTY) {
        return Err(format!(
            "update response did not report `{REMOVE_PROPERTY}`"
        ));
    }
    let loaded = validate_namespace_response(reload, 200, &fixture.primary)?;
    let properties = loaded
        .properties
        .ok_or_else(|| "loaded namespace returned null or omitted properties".to_owned())?;
    if properties.get(OWNER_PROPERTY).map(String::as_str) != Some("catalog-bench") {
        return Err(format!(
            "unmentioned `{OWNER_PROPERTY}` property was not preserved"
        ));
    }
    if properties.get(STATE_PROPERTY).map(String::as_str) != Some("after") {
        return Err(format!(
            "updated `{STATE_PROPERTY}` property was not persisted"
        ));
    }
    if properties.contains_key(REMOVE_PROPERTY) {
        return Err(format!(
            "removed `{REMOVE_PROPERTY}` property is still present"
        ));
    }
    Ok(())
}

fn validate_namespace_response(
    observation: &Observation,
    status: u16,
    expected: &NamespaceIdentifier,
) -> std::result::Result<NamespaceResponse, String> {
    let response: NamespaceResponse = parse_json_response(observation, status)?;
    let actual = NamespaceIdentifier::from_parts(response.namespace.clone())
        .map_err(|error| error.to_string())?;
    if actual != *expected {
        return Err(format!(
            "response namespace {:?} does not match {:?}",
            actual.parts(),
            expected.parts()
        ));
    }
    Ok(response)
}

fn parse_list_response(
    observation: &Observation,
    status: u16,
) -> std::result::Result<ListNamespacesResponse, String> {
    parse_json_response(observation, status)
}

fn parse_namespaces(
    namespaces: Vec<Vec<String>>,
) -> std::result::Result<BTreeSet<NamespaceIdentifier>, String> {
    let input_count = namespaces.len();
    let parsed = namespaces
        .into_iter()
        .map(NamespaceIdentifier::from_parts)
        .collect::<Result<BTreeSet<_>>>()
        .map_err(|error| error.to_string())?;
    if parsed.len() != input_count {
        return Err("listing contains duplicate namespace identifiers".to_owned());
    }
    Ok(parsed)
}

fn is_json_media_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

#[derive(Debug, Deserialize)]
struct NamespaceResponse {
    namespace: Vec<String>,
    #[serde(default)]
    properties: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct ListNamespacesResponse {
    namespaces: Vec<Vec<String>>,
    #[serde(rename = "next-page-token", default)]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UpdateNamespacePropertiesResponse {
    updated: Vec<String>,
    removed: Vec<String>,
}

fn skip_remaining_mutations(recorder: &mut OperationRecorder<'_>, reason: &str) {
    for (id, capability) in [
        ("create-primary", CREATE_CAPABILITY),
        ("create-sibling", CREATE_CAPABILITY),
        ("create-child", CREATE_CAPABILITY),
        ("list-top-level", LIST_CAPABILITY),
        ("load-primary", LOAD_CAPABILITY),
        ("update-primary-properties", UPDATE_CAPABILITY),
        ("reload-primary-properties", UPDATE_CAPABILITY),
        ("create-primary-duplicate", DUPLICATE_CAPABILITY),
        ("list-primary-children", HIERARCHY_CAPABILITY),
        ("list-page-001", PAGINATION_CAPABILITY),
        ("drop-child", DROP_CAPABILITY),
        ("drop-sibling", DROP_CAPABILITY),
        ("drop-primary", DROP_CAPABILITY),
        ("verify-child-absent", DROP_CAPABILITY),
        ("verify-sibling-absent", DROP_CAPABILITY),
        ("verify-primary-absent", DROP_CAPABILITY),
    ] {
        recorder.skip(id, Some(capability), reason);
    }
}

pub(super) struct NamespaceFacts {
    authentication: Fact,
    config_routing: Fact,
    fixture_isolated: Fact,
    create: Fact,
    list: Fact,
    load: Fact,
    properties: Fact,
    duplicate: Fact,
    hierarchy: Fact,
    pagination: Fact,
    missing_parent: Fact,
    cleanup: Fact,
    transcript_sanitized: Fact,
}

impl NamespaceFacts {
    fn new(authentication_ready: bool, config_routing: Fact) -> Self {
        let pending =
            || Fact::NotEvaluated("namespace workflow did not reach this check".to_owned());
        Self {
            authentication: Fact::from_bool(
                authentication_ready,
                "authentication negotiation did not complete",
            ),
            config_routing,
            fixture_isolated: pending(),
            create: pending(),
            list: pending(),
            load: pending(),
            properties: pending(),
            duplicate: pending(),
            hierarchy: pending(),
            pagination: pending(),
            missing_parent: pending(),
            cleanup: pending(),
            transcript_sanitized: pending(),
        }
    }

    fn skip_mutating_behavior(&mut self, reason: &str) {
        self.create = Fact::NotEvaluated(reason.to_owned());
        self.list = Fact::NotEvaluated(reason.to_owned());
        self.load = Fact::NotEvaluated(reason.to_owned());
        self.properties = Fact::NotEvaluated(reason.to_owned());
        self.duplicate = Fact::NotEvaluated(reason.to_owned());
        self.hierarchy = Fact::NotEvaluated(reason.to_owned());
        self.pagination = Fact::NotEvaluated(reason.to_owned());
        self.cleanup = Fact::NotEvaluated(reason.to_owned());
    }

    fn skip_namespace_behavior(&mut self, reason: &str) {
        self.fixture_isolated = Fact::NotEvaluated(reason.to_owned());
        self.skip_mutating_behavior(reason);
        self.missing_parent = Fact::NotEvaluated(reason.to_owned());
    }

    pub(super) fn for_assertion(&self, check: &AssertionCheck) -> Fact {
        let AssertionCheck::Custom { name, .. } = check else {
            return Fact::Fail("namespace policy received a non-custom assertion".to_owned());
        };
        match name.as_str() {
            "querygraph/catalog-bench/authentication-ready-v1" => self.authentication.clone(),
            "querygraph/catalog-bench/namespace-config-routing-v1" => self.config_routing.clone(),
            "querygraph/catalog-bench/namespace-fixture-isolation-v1" => {
                self.fixture_isolated.clone()
            }
            "querygraph/catalog-bench/namespace-create-v1" => self.create.clone(),
            "querygraph/catalog-bench/namespace-list-v1" => self.list.clone(),
            "querygraph/catalog-bench/namespace-load-v1" => self.load.clone(),
            "querygraph/catalog-bench/namespace-properties-v1" => self.properties.clone(),
            "querygraph/catalog-bench/namespace-duplicate-error-v1" => self.duplicate.clone(),
            "querygraph/catalog-bench/namespace-hierarchy-v1" => self.hierarchy.clone(),
            "querygraph/catalog-bench/namespace-pagination-v1" => self.pagination.clone(),
            "querygraph/catalog-bench/namespace-missing-parent-error-v1" => {
                self.missing_parent.clone()
            }
            "querygraph/catalog-bench/namespace-cleanup-v1" => self.cleanup.clone(),
            "querygraph/catalog-bench/sanitized-http-transcript-v1" => {
                self.transcript_sanitized.clone()
            }
            _ => Fact::Fail(format!(
                "namespace policy received unknown assertion `{name}`"
            )),
        }
    }
}
