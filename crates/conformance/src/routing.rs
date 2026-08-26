use std::collections::BTreeMap;

use anyhow::Result;
use catalog_bench_common::contract::CatalogAdapter;
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{resolve_prefix, PrefixResolution};
use crate::evidence::{HttpRequestTranscript, HttpResponseTranscript, ProbeFailure};
use crate::iceberg::{NamespaceCodec, NamespaceSeparatorResolution};
use crate::operation::Fact;
use crate::transport::{
    acquire_authentication, endpoint_url, execute_json_request, AuthenticationAttempt,
    CapturedResponse, REDACTED,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingConfigTranscript {
    pub request: HttpRequestTranscript,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<HttpResponseTranscript>,
    pub prefix: PrefixResolution,
    pub namespace_separator: NamespaceSeparatorResolution,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<ProbeFailure>,
}

pub(crate) struct RoutingNegotiation {
    pub(crate) authentication: AuthenticationAttempt,
    pub(crate) config: RoutingConfigTranscript,
    pub(crate) codec: Option<NamespaceCodec>,
    pub(crate) config_routing: Fact,
    pub(crate) redactions: Vec<String>,
}

pub(crate) fn not_evaluated_config(
    adapter: &CatalogAdapter,
    reason: &str,
) -> Result<RoutingConfigTranscript> {
    Ok(RoutingConfigTranscript {
        request: config_request(adapter, false)?,
        response: None,
        prefix: PrefixResolution::NotEvaluated {
            reason: reason.to_owned(),
        },
        namespace_separator: NamespaceSeparatorResolution::NotEvaluated {
            reason: reason.to_owned(),
        },
        failure: None,
    })
}

pub(crate) async fn negotiate_routing<F>(
    client: &Client,
    adapter: &CatalogAdapter,
    getenv: &F,
) -> Result<RoutingNegotiation>
where
    F: Fn(&str) -> Option<String>,
{
    let authentication = acquire_authentication(client, adapter, getenv).await;
    let request = config_request(adapter, authentication.bearer_token.is_some())?;
    let captured = if authentication.failure.is_none() {
        execute_json_request(
            client,
            Method::GET,
            endpoint_url(
                &adapter.endpoint.base_url,
                &adapter.endpoint.config.path,
                &adapter.endpoint.config.query,
            )?,
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

    let prefix = resolve_prefix(adapter, captured.private_json.as_ref());
    let (namespace_separator, codec) = NamespaceCodec::resolve(captured.private_json.as_ref());
    let config_routing = Fact::from_result(validate_config_routing(
        authentication.failure.as_ref(),
        captured.response.as_ref(),
        captured.private_json.as_ref(),
        &prefix,
        codec.as_ref(),
    ));
    let config = RoutingConfigTranscript {
        request,
        response: captured.response,
        prefix,
        namespace_separator,
        failure: authentication.failure.clone().or(captured.failure),
    };
    let mut redactions = captured
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

    Ok(RoutingNegotiation {
        authentication,
        config,
        codec,
        config_routing,
        redactions,
    })
}

fn config_request(adapter: &CatalogAdapter, authenticated: bool) -> Result<HttpRequestTranscript> {
    let url = endpoint_url(
        &adapter.endpoint.base_url,
        &adapter.endpoint.config.path,
        &adapter.endpoint.config.query,
    )?;
    let mut headers = BTreeMap::from([("accept".to_owned(), "application/json".to_owned())]);
    if authenticated {
        headers.insert("authorization".to_owned(), REDACTED.to_owned());
    }
    Ok(HttpRequestTranscript {
        method: "GET".to_owned(),
        url: url.to_string(),
        headers,
    })
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

fn is_json_media_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}
