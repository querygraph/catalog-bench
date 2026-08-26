use std::collections::BTreeMap;

use anyhow::{bail, Result};
use catalog_bench_common::contract::{
    ActorRole, AssertionCheck, AssertionOutcome, CatalogAdapter, CatalogRoutePrefix, ComponentId,
    Profile, ProfileId, RequirementLevel, Scenario,
};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::encode_evidence;
use crate::evidence::{
    not_evaluated_assertions, passed_required_assertions, transcript_adapter,
    AuthenticationOutcome, AuthenticationTranscript, ContractDigests, HttpRequestTranscript,
    HttpResponseTranscript, ProbeAssertion, ProbeClassification, ProbeFailure,
    SanitizationTranscript, TranscriptAdapter, TranscriptScenario,
};
use crate::sanitize::contains_sensitive_value;
use crate::spec::{ICEBERG_REST_OPENAPI_SHA256, ICEBERG_REST_OPENAPI_SOURCE};
use crate::target::ProbeTarget;
use crate::transport::{
    acquire_authentication, authentication_mode, endpoint_url, execute_json_request, http_client,
    MAXIMUM_RESPONSE_BYTES, REDACTED,
};

pub const CONFIG_TRANSCRIPT_FORMAT: &str = "catalog-bench/config-transcript/v1";
pub const CONFIG_SCENARIO_ID: &str = "iceberg-rest.config.negotiation";
const CONFIG_SCENARIO_VERSION: u32 = 1;
const RESPONSE_MEDIA_TYPE: &str = "application/json";

const STANDARD_ENDPOINTS: &[&str] = &[
    "GET /v1/config",
    "POST /v1/oauth/tokens",
    "GET /v1/{prefix}/namespaces",
    "POST /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "DELETE /v1/{prefix}/namespaces/{namespace}",
    "POST /v1/{prefix}/namespaces/{namespace}/properties",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/tables",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks",
    "POST /v1/{prefix}/namespaces/{namespace}/register",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/sign",
    "POST /v1/{prefix}/tables/rename",
    "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
    "POST /v1/{prefix}/transactions/commit",
    "GET /v1/{prefix}/namespaces/{namespace}/views",
    "POST /v1/{prefix}/namespaces/{namespace}/views",
    "GET /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "POST /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}",
    "POST /v1/{prefix}/views/rename",
    "POST /v1/{prefix}/namespaces/{namespace}/register-view",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum PrefixResolution {
    Unprefixed,
    Static { value: String },
    Negotiated { json_pointer: String, value: String },
    Failed { explanation: String },
    NotEvaluated { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum EndpointAdvertisement {
    Explicit { endpoints: Vec<String> },
    Omitted,
    Invalid { explanation: String },
    NotEvaluated { reason: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigTranscript {
    pub format: String,
    pub profile: ProfileId,
    pub scenario: TranscriptScenario,
    pub contract_digests: ContractDigests,
    pub adapter: TranscriptAdapter,
    pub classification: ProbeClassification,
    pub authentication: AuthenticationTranscript,
    pub request: HttpRequestTranscript,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<HttpResponseTranscript>,
    pub prefix: PrefixResolution,
    pub endpoints: EndpointAdvertisement,
    pub assertions: Vec<ProbeAssertion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<ProbeFailure>,
    pub sanitization: SanitizationTranscript,
}

impl ConfigTranscript {
    #[must_use]
    pub fn passed(&self) -> bool {
        matches!(self.classification, ProbeClassification::Pass)
    }
}

struct ProbeFacts {
    authentication_ready: bool,
    response_status: Option<u16>,
    response_content_type: Option<String>,
    config_map_shape: Option<std::result::Result<(), String>>,
    prefix: PrefixResolution,
    endpoints: EndpointAdvertisement,
    transcript_sanitized: bool,
}

/// Run the config-negotiation scenario through one validated profile adapter.
///
/// Transport and assertion failures are returned as evidence, not as `Err`, so
/// the caller can persist an auditable failed probe. `Err` is reserved for an
/// invalid invocation or an internally inconsistent contract.
pub async fn run_config_probe<F>(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &ComponentId,
    contract_digests: ContractDigests,
    getenv: F,
) -> Result<ConfigTranscript>
where
    F: Fn(&str) -> Option<String>,
{
    validate_invocation(profile, scenario, catalog)?;
    let target = ProbeTarget::resolve(profile, scenario, catalog)?;
    let adapter = target.adapter;
    let component = target.component;
    let config_url = endpoint_url(
        &adapter.endpoint.base_url,
        &adapter.endpoint.config.path,
        &adapter.endpoint.config.query,
    )?;
    let request = HttpRequestTranscript {
        method: "GET".to_owned(),
        url: config_url.to_string(),
        headers: BTreeMap::from([("accept".to_owned(), "application/json".to_owned())]),
    };

    if let Some(limitation) = target.first_required_limitation(scenario) {
        let reason = format!(
            "required capability `{}` is declared unsupported before execution",
            limitation.capability
        );
        return Ok(ConfigTranscript {
            format: CONFIG_TRANSCRIPT_FORMAT.to_owned(),
            profile: profile.id.clone(),
            scenario: TranscriptScenario {
                id: scenario.id.clone(),
                version: scenario.version,
            },
            contract_digests,
            adapter: transcript_adapter(adapter, component),
            classification: ProbeClassification::Unsupported {
                capability: limitation.capability.clone(),
                attributed_to: limitation.attributed_to,
                explanation: limitation.explanation.clone(),
            },
            authentication: AuthenticationTranscript {
                mode: authentication_mode(&adapter.authentication).to_owned(),
                outcome: AuthenticationOutcome::NotAttempted,
                token_url: None,
                scope: None,
                http_status: None,
            },
            request,
            response: None,
            prefix: PrefixResolution::NotEvaluated {
                reason: reason.clone(),
            },
            endpoints: EndpointAdvertisement::NotEvaluated {
                reason: reason.clone(),
            },
            assertions: not_evaluated_assertions(scenario, &reason),
            failure: None,
            sanitization: SanitizationTranscript {
                policy: "automated-v1".to_owned(),
                redactions: Vec::new(),
                raw_secrets_persisted: false,
                raw_response_body_persisted: false,
            },
        });
    }

    let client = http_client(30_000)?;
    let authentication = acquire_authentication(&client, adapter, &getenv).await;
    let mut request = request;
    if authentication.bearer_token.is_some() {
        request
            .headers
            .insert("authorization".to_owned(), REDACTED.to_owned());
    }

    let captured = if let Some(token) = authentication.bearer_token.as_deref() {
        execute_json_request(
            &client,
            Method::GET,
            config_url,
            Some(token),
            None,
            &authentication.sensitive_values,
            "config",
        )
        .await
    } else if authentication.failure.is_none() {
        execute_json_request(
            &client,
            Method::GET,
            config_url,
            None,
            None,
            &authentication.sensitive_values,
            "config",
        )
        .await
    } else {
        crate::transport::CapturedResponse {
            response: None,
            private_json: None,
            redactions: Vec::new(),
            failure: None,
        }
    };
    let response = captured.response;

    let failure = authentication.failure.clone().or(captured.failure);
    let prefix = resolve_prefix(
        adapter,
        response.as_ref().and_then(HttpResponseTranscript::json),
    );
    let endpoints = inspect_endpoints(response.as_ref().and_then(HttpResponseTranscript::json));
    let config_map_shape = response
        .as_ref()
        .and_then(HttpResponseTranscript::json)
        .map(validate_config_map_shape);

    let mut redactions = captured.redactions;
    if authentication.bearer_token.is_some() {
        redactions.push("request.headers.authorization".to_owned());
        redactions.push("authentication.oauth2-request-credentials".to_owned());
        redactions.push("authentication.oauth2-response-token".to_owned());
    }
    redactions.sort();
    redactions.dedup();

    let serialized_evidence = encode_evidence(&(
        &authentication.transcript,
        &request,
        &response,
        &prefix,
        &endpoints,
    ))?;
    let transcript_sanitized =
        !contains_sensitive_value(&serialized_evidence, &authentication.sensitive_values);
    let facts = ProbeFacts {
        authentication_ready: authentication.failure.is_none(),
        response_status: response.as_ref().map(|response| response.status),
        response_content_type: response
            .as_ref()
            .and_then(|response| response.headers.get("content-type"))
            .cloned(),
        config_map_shape,
        prefix,
        endpoints,
        transcript_sanitized,
    };
    let assertions = evaluate_assertions(scenario, &facts);
    let classification = if passed_required_assertions(&assertions) {
        ProbeClassification::Pass
    } else {
        ProbeClassification::Fail {
            summary: "one or more required config-negotiation assertions did not pass".to_owned(),
        }
    };

    Ok(ConfigTranscript {
        format: CONFIG_TRANSCRIPT_FORMAT.to_owned(),
        profile: profile.id.clone(),
        scenario: TranscriptScenario {
            id: scenario.id.clone(),
            version: scenario.version,
        },
        contract_digests,
        adapter: transcript_adapter(adapter, component),
        classification,
        authentication: authentication.transcript,
        request,
        response,
        prefix: facts.prefix,
        endpoints: facts.endpoints,
        assertions,
        failure,
        sanitization: SanitizationTranscript {
            policy: "automated-v1".to_owned(),
            redactions,
            raw_secrets_persisted: false,
            raw_response_body_persisted: false,
        },
    })
}

fn validate_invocation(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &ComponentId,
) -> Result<()> {
    if scenario.id.as_str() != CONFIG_SCENARIO_ID {
        bail!(
            "config probe requires scenario `{CONFIG_SCENARIO_ID}`, found `{}`",
            scenario.id
        );
    }
    if scenario.version != CONFIG_SCENARIO_VERSION {
        bail!(
            "config probe supports scenario version {CONFIG_SCENARIO_VERSION}, found {}",
            scenario.version
        );
    }
    validate_scenario_policy(scenario)?;
    ProbeTarget::resolve(profile, scenario, catalog)?;
    Ok(())
}

fn validate_scenario_policy(scenario: &Scenario) -> Result<()> {
    let expected_parameters = BTreeMap::from([
        (
            "maximum_response_bytes".to_owned(),
            Value::from(MAXIMUM_RESPONSE_BYTES as u64),
        ),
        (
            "response_media_type".to_owned(),
            Value::from(RESPONSE_MEDIA_TYPE),
        ),
        (
            "transcript_format".to_owned(),
            Value::from(CONFIG_TRANSCRIPT_FORMAT),
        ),
        (
            "iceberg_openapi_source".to_owned(),
            Value::from(ICEBERG_REST_OPENAPI_SOURCE),
        ),
        (
            "iceberg_openapi_sha256".to_owned(),
            Value::from(ICEBERG_REST_OPENAPI_SHA256),
        ),
    ]);
    if scenario.parameters != expected_parameters {
        bail!(
            "config probe scenario parameters drifted from the implemented v{CONFIG_SCENARIO_VERSION} policy"
        );
    }

    let expected_capabilities = [
        "iceberg-rest.config.read",
        "iceberg-rest.config.endpoint-advertisement",
        "iceberg-rest.config.warehouse-routing",
    ];
    if scenario.capabilities.len() != expected_capabilities.len() {
        bail!(
            "config probe scenario must declare exactly {} capabilities",
            expected_capabilities.len()
        );
    }
    for (requirement, expected) in scenario.capabilities.iter().zip(expected_capabilities) {
        if requirement.capability.as_str() != expected
            || requirement.level != RequirementLevel::Required
            || requirement.specification.as_deref() != Some(ICEBERG_REST_OPENAPI_SOURCE)
        {
            bail!("config probe scenario capability policy drifted at `{expected}`");
        }
    }

    let expected_steps = [
        (
            "negotiate-authentication",
            ActorRole::Harness,
            "authentication.negotiate",
            &[][..],
            None,
        ),
        (
            "request-config",
            ActorRole::Client,
            "config.get",
            &["negotiate-authentication"][..],
            Some(30_000),
        ),
        (
            "resolve-prefix",
            ActorRole::Harness,
            "config.resolve-prefix",
            &["request-config"][..],
            None,
        ),
        (
            "validate-endpoints",
            ActorRole::Harness,
            "config.validate-endpoints",
            &["request-config"][..],
            None,
        ),
        (
            "sanitize-transcript",
            ActorRole::Harness,
            "evidence.sanitize-http-transcript",
            &["resolve-prefix", "validate-endpoints"][..],
            None,
        ),
    ];
    if scenario.steps.len() != expected_steps.len() {
        bail!(
            "config probe scenario must declare exactly {} steps",
            expected_steps.len()
        );
    }
    for (step, (id, actor, operation, dependencies, timeout_ms)) in
        scenario.steps.iter().zip(expected_steps)
    {
        let actual_dependencies = step
            .depends_on
            .iter()
            .map(|dependency| dependency.as_str())
            .collect::<Vec<_>>();
        if step.id.as_str() != id
            || step.actor != actor
            || step.operation != operation
            || actual_dependencies != dependencies
            || step.timeout_ms != timeout_ms
            || !step.parameters.is_empty()
        {
            bail!("config probe scenario step policy drifted at `{id}`");
        }
    }

    let expected_assertions = [
        (
            "authentication-ready",
            "negotiate-authentication",
            serde_json::json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/authentication-ready-v1",
                "configuration": {"persist_token": false}
            }),
        ),
        (
            "config-http-success",
            "request-config",
            serde_json::json!({"kind": "http-status", "allowed": [200]}),
        ),
        (
            "config-media-type",
            "request-config",
            serde_json::json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/config-media-type-v1",
                "configuration": {"allowed": [RESPONSE_MEDIA_TYPE]}
            }),
        ),
        (
            "config-map-shape",
            "request-config",
            serde_json::json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/config-map-shape-v1",
                "configuration": {"map_fields": ["defaults", "overrides"]}
            }),
        ),
        (
            "route-prefix-resolved",
            "resolve-prefix",
            serde_json::json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/route-prefix-resolution-v1",
                "configuration": {"allowed_modes": ["unprefixed", "static", "negotiated"]}
            }),
        ),
        (
            "endpoint-advertisement-valid",
            "validate-endpoints",
            serde_json::json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/endpoint-advertisement-v1",
                "configuration": {
                    "absence_semantics": "client-defined-legacy-defaults",
                    "path_prefix": "/v1/"
                }
            }),
        ),
        (
            "transcript-sanitized",
            "sanitize-transcript",
            serde_json::json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/sanitized-http-transcript-v1",
                "configuration": {
                    "request_header_policy": "allowlist-and-redact-authorization",
                    "response_header_policy": "allowlist",
                    "response_json_policy": "recursive-secret-key-redaction"
                }
            }),
        ),
    ];
    if scenario.assertions.len() != expected_assertions.len() {
        bail!(
            "config probe scenario must declare exactly {} assertions",
            expected_assertions.len()
        );
    }
    for (assertion, (id, step, check)) in scenario.assertions.iter().zip(expected_assertions) {
        if assertion.id.as_str() != id
            || assertion.step.as_str() != step
            || !assertion.required
            || serde_json::to_value(&assertion.check)? != check
        {
            bail!("config probe scenario assertion policy drifted at `{id}`");
        }
    }

    if !scenario.extensions.is_empty() {
        bail!("config probe scenario v{CONFIG_SCENARIO_VERSION} does not accept extensions");
    }
    Ok(())
}

pub(crate) fn resolve_prefix(adapter: &CatalogAdapter, body: Option<&Value>) -> PrefixResolution {
    match &adapter.endpoint.route_prefix {
        CatalogRoutePrefix::Unprefixed => PrefixResolution::Unprefixed,
        CatalogRoutePrefix::Static { value } => PrefixResolution::Static {
            value: value.clone(),
        },
        CatalogRoutePrefix::Negotiated {
            config_json_pointer,
        } => match body
            .and_then(|body| body.pointer(config_json_pointer))
            .and_then(Value::as_str)
        {
            Some(value) if valid_path_segment(value) => PrefixResolution::Negotiated {
                json_pointer: config_json_pointer.clone(),
                value: value.to_owned(),
            },
            Some(_) => PrefixResolution::Failed {
                explanation: format!(
                    "config value at `{config_json_pointer}` is not one nonempty path segment"
                ),
            },
            None => PrefixResolution::Failed {
                explanation: format!(
                    "config response has no string prefix at `{config_json_pointer}`"
                ),
            },
        },
    }
}

fn valid_path_segment(value: &str) -> bool {
    !value.is_empty()
        && !value
            .chars()
            .any(|character| character.is_whitespace() || "/?#".contains(character))
}

fn inspect_endpoints(body: Option<&Value>) -> EndpointAdvertisement {
    let Some(body) = body else {
        return EndpointAdvertisement::NotEvaluated {
            reason: "config response has no captured JSON body".to_owned(),
        };
    };
    let Some(endpoints) = body.get("endpoints") else {
        return EndpointAdvertisement::Omitted;
    };
    let Some(endpoints) = endpoints.as_array() else {
        return EndpointAdvertisement::Invalid {
            explanation: "config `endpoints` must be an array when present".to_owned(),
        };
    };
    let mut parsed = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        let Some(endpoint) = endpoint.as_str() else {
            return EndpointAdvertisement::Invalid {
                explanation: "every config endpoint must be a string".to_owned(),
            };
        };
        if !STANDARD_ENDPOINTS.contains(&endpoint) {
            return EndpointAdvertisement::Invalid {
                explanation: format!("`{endpoint}` is not an Apache Iceberg 1.11.0 REST endpoint"),
            };
        }
        parsed.push(endpoint.to_owned());
    }
    EndpointAdvertisement::Explicit { endpoints: parsed }
}

fn validate_config_map_shape(body: &Value) -> std::result::Result<(), String> {
    let Some(object) = body.as_object() else {
        return Err("config response must be a JSON object".to_owned());
    };
    for field in ["defaults", "overrides"] {
        let Some(value) = object.get(field) else {
            return Err(format!("config response is missing required `{field}` map"));
        };
        let Some(properties) = value.as_object() else {
            return Err(format!(
                "config `{field}` must be a JSON object when present"
            ));
        };
        if let Some((name, _)) = properties.iter().find(|(_, value)| !value.is_string()) {
            return Err(format!(
                "config `{field}.{name}` must be a string map value"
            ));
        }
    }
    Ok(())
}

fn evaluate_assertions(scenario: &Scenario, facts: &ProbeFacts) -> Vec<ProbeAssertion> {
    scenario
        .assertions
        .iter()
        .map(|assertion| ProbeAssertion {
            assertion: assertion.id.clone(),
            required: assertion.required,
            outcome: evaluate_assertion(&assertion.check, facts),
        })
        .collect()
}

fn evaluate_assertion(check: &AssertionCheck, facts: &ProbeFacts) -> AssertionOutcome {
    match check {
        AssertionCheck::HttpStatus { allowed } => match facts.response_status {
            Some(status) if allowed.contains(&status) => AssertionOutcome::Pass,
            Some(status) => AssertionOutcome::Fail {
                explanation: format!("HTTP {status} is not in {allowed:?}"),
            },
            None => AssertionOutcome::NotEvaluated {
                reason: "no config response was received".to_owned(),
            },
        },
        AssertionCheck::Custom { name, .. }
            if name == "querygraph/catalog-bench/authentication-ready-v1" =>
        {
            boolean_outcome(
                facts.authentication_ready,
                "authentication negotiation did not complete",
            )
        }
        AssertionCheck::Custom { name, .. }
            if name == "querygraph/catalog-bench/config-media-type-v1" =>
        {
            match (&facts.response_content_type, facts.response_status) {
                (Some(content_type), _) if is_json_media_type(content_type) => {
                    AssertionOutcome::Pass
                }
                (Some(content_type), _) => AssertionOutcome::Fail {
                    explanation: format!(
                        "response Content-Type `{content_type}` is not `{RESPONSE_MEDIA_TYPE}`"
                    ),
                },
                (None, Some(_)) => AssertionOutcome::Fail {
                    explanation: "config response omitted Content-Type".to_owned(),
                },
                (None, None) => AssertionOutcome::NotEvaluated {
                    reason: "no config response was received".to_owned(),
                },
            }
        }
        AssertionCheck::Custom { name, .. }
            if name == "querygraph/catalog-bench/config-map-shape-v1" =>
        {
            match &facts.config_map_shape {
                Some(Ok(())) => AssertionOutcome::Pass,
                Some(Err(explanation)) => AssertionOutcome::Fail {
                    explanation: explanation.clone(),
                },
                None => AssertionOutcome::NotEvaluated {
                    reason: "no JSON config body was captured".to_owned(),
                },
            }
        }
        AssertionCheck::Custom { name, .. }
            if name == "querygraph/catalog-bench/route-prefix-resolution-v1" =>
        {
            match &facts.prefix {
                PrefixResolution::Unprefixed
                | PrefixResolution::Static { .. }
                | PrefixResolution::Negotiated { .. } => AssertionOutcome::Pass,
                PrefixResolution::Failed { explanation } => AssertionOutcome::Fail {
                    explanation: explanation.clone(),
                },
                PrefixResolution::NotEvaluated { reason } => AssertionOutcome::NotEvaluated {
                    reason: reason.clone(),
                },
            }
        }
        AssertionCheck::Custom { name, .. }
            if name == "querygraph/catalog-bench/endpoint-advertisement-v1" =>
        {
            match &facts.endpoints {
                EndpointAdvertisement::Explicit { .. } | EndpointAdvertisement::Omitted => {
                    AssertionOutcome::Pass
                }
                EndpointAdvertisement::Invalid { explanation } => AssertionOutcome::Fail {
                    explanation: explanation.clone(),
                },
                EndpointAdvertisement::NotEvaluated { reason } => AssertionOutcome::NotEvaluated {
                    reason: reason.clone(),
                },
            }
        }
        AssertionCheck::Custom { name, .. }
            if name == "querygraph/catalog-bench/sanitized-http-transcript-v1" =>
        {
            boolean_outcome(
                facts.transcript_sanitized,
                "sanitized evidence still contains a sensitive runtime value",
            )
        }
        _ => AssertionOutcome::Fail {
            explanation: "assertion check is not implemented by the config probe".to_owned(),
        },
    }
}

fn boolean_outcome(value: bool, failure: &str) -> AssertionOutcome {
    if value {
        AssertionOutcome::Pass
    } else {
        AssertionOutcome::Fail {
            explanation: failure.to_owned(),
        }
    }
}

fn is_json_media_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case(RESPONSE_MEDIA_TYPE))
}
