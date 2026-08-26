use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result};
use catalog_bench_common::contract::{CatalogAdapter, CatalogAuthentication};
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use reqwest::{Client, Method, Response};
use serde_json::Value;
use url::form_urlencoded::Serializer;
use url::Url;

use crate::evidence::{
    AuthenticationOutcome, AuthenticationTranscript, HttpResponseTranscript, ProbeFailure,
    ProbeFailureStage, SanitizedResponseBody,
};
use crate::sanitize::sanitize_json;
use crate::sha256_hex;

pub(crate) const MAXIMUM_RESPONSE_BYTES: usize = 1024 * 1024;
pub(crate) const REDACTED: &str = "<redacted>";

pub(crate) struct AuthenticationAttempt {
    pub(crate) transcript: AuthenticationTranscript,
    pub(crate) bearer_token: Option<String>,
    pub(crate) sensitive_values: Vec<String>,
    pub(crate) failure: Option<ProbeFailure>,
}

pub(crate) struct CapturedResponse {
    pub(crate) response: Option<HttpResponseTranscript>,
    /// Parsed response retained only in memory for protocol evaluation. This is
    /// never part of a serializable evidence type.
    pub(crate) private_json: Option<Value>,
    pub(crate) redactions: Vec<String>,
    pub(crate) failure: Option<ProbeFailure>,
}

pub(crate) fn http_client(timeout_ms: u64) -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        .build()
        .context("build conformance HTTP client")
}

pub(crate) async fn acquire_authentication<F>(
    client: &Client,
    adapter: &CatalogAdapter,
    getenv: &F,
) -> AuthenticationAttempt
where
    F: Fn(&str) -> Option<String>,
{
    match &adapter.authentication {
        CatalogAuthentication::Anonymous => AuthenticationAttempt {
            transcript: AuthenticationTranscript {
                mode: "anonymous".to_owned(),
                outcome: AuthenticationOutcome::Ready,
                token_url: None,
                scope: None,
                http_status: None,
            },
            bearer_token: None,
            sensitive_values: Vec::new(),
            failure: None,
        },
        CatalogAuthentication::OAuth2ClientCredentials {
            token_path,
            scope,
            client_id_env,
            client_secret_env,
        } => {
            let token_url = endpoint_url(&adapter.endpoint.base_url, token_path, &BTreeMap::new());
            let transcript =
                |outcome, http_status, token_url: Option<&Url>| AuthenticationTranscript {
                    mode: "oauth2-client-credentials".to_owned(),
                    outcome,
                    token_url: token_url.map(ToString::to_string),
                    scope: Some(scope.clone()),
                    http_status,
                };
            let token_url = match token_url {
                Ok(url) => url,
                Err(error) => {
                    return authentication_failure(
                        transcript(AuthenticationOutcome::Failed, None, None),
                        format!("invalid token endpoint: {error}"),
                    );
                }
            };
            let Some(client_id) = getenv(client_id_env).filter(|value| !value.is_empty()) else {
                return authentication_failure(
                    transcript(AuthenticationOutcome::Failed, None, Some(&token_url)),
                    format!("environment variable `{client_id_env}` is not set or is empty"),
                );
            };
            let Some(client_secret) = getenv(client_secret_env).filter(|value| !value.is_empty())
            else {
                return authentication_failure(
                    transcript(AuthenticationOutcome::Failed, None, Some(&token_url)),
                    format!("environment variable `{client_secret_env}` is not set or is empty"),
                );
            };
            let body = Serializer::new(String::new())
                .append_pair("grant_type", "client_credentials")
                .append_pair("client_id", &client_id)
                .append_pair("client_secret", &client_secret)
                .append_pair("scope", scope)
                .finish();
            let response = match client
                .post(token_url.clone())
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(ACCEPT, "application/json")
                .body(body)
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    let explanation = redact_text(
                        &error.to_string(),
                        &[client_id.clone(), client_secret.clone()],
                    );
                    return authentication_failure(
                        transcript(AuthenticationOutcome::Failed, None, Some(&token_url)),
                        format!("OAuth2 request failed: {explanation}"),
                    );
                }
            };
            let status = response.status().as_u16();
            let bytes = match read_limited_body(response, MAXIMUM_RESPONSE_BYTES).await {
                Ok(CollectedBody::Complete(bytes)) => bytes,
                Ok(CollectedBody::TooLarge { .. }) => {
                    return authentication_failure(
                        transcript(
                            AuthenticationOutcome::Failed,
                            Some(status),
                            Some(&token_url),
                        ),
                        "OAuth2 response exceeds the evidence body limit".to_owned(),
                    );
                }
                Err(error) => {
                    return authentication_failure(
                        transcript(
                            AuthenticationOutcome::Failed,
                            Some(status),
                            Some(&token_url),
                        ),
                        format!("OAuth2 response read failed: {error}"),
                    );
                }
            };
            if !(200..300).contains(&status) {
                return authentication_failure(
                    transcript(
                        AuthenticationOutcome::Failed,
                        Some(status),
                        Some(&token_url),
                    ),
                    format!("OAuth2 endpoint returned HTTP {status}"),
                );
            }
            let body: Value = match serde_json::from_slice(&bytes) {
                Ok(body) => body,
                Err(_) => {
                    return authentication_failure(
                        transcript(
                            AuthenticationOutcome::Failed,
                            Some(status),
                            Some(&token_url),
                        ),
                        "OAuth2 endpoint did not return JSON".to_owned(),
                    );
                }
            };
            let Some(token) = body.get("access_token").and_then(Value::as_str) else {
                return authentication_failure(
                    transcript(
                        AuthenticationOutcome::Failed,
                        Some(status),
                        Some(&token_url),
                    ),
                    "OAuth2 response omitted string `access_token`".to_owned(),
                );
            };
            if token.is_empty() {
                return authentication_failure(
                    transcript(
                        AuthenticationOutcome::Failed,
                        Some(status),
                        Some(&token_url),
                    ),
                    "OAuth2 response returned an empty access token".to_owned(),
                );
            }
            AuthenticationAttempt {
                transcript: transcript(
                    AuthenticationOutcome::Ready,
                    Some(status),
                    Some(&token_url),
                ),
                bearer_token: Some(token.to_owned()),
                sensitive_values: vec![client_id, client_secret, token.to_owned()],
                failure: None,
            }
        }
    }
}

fn authentication_failure(
    transcript: AuthenticationTranscript,
    explanation: String,
) -> AuthenticationAttempt {
    AuthenticationAttempt {
        transcript,
        bearer_token: None,
        sensitive_values: Vec::new(),
        failure: Some(ProbeFailure {
            stage: ProbeFailureStage::Authentication,
            explanation,
        }),
    }
}

pub(crate) async fn execute_json_request(
    client: &Client,
    method: Method,
    url: Url,
    bearer_token: Option<&str>,
    body: Option<&Value>,
    sensitive_values: &[String],
    response_label: &str,
) -> CapturedResponse {
    let mut request = client
        .request(method, url)
        .header(ACCEPT, "application/json");
    if let Some(token) = bearer_token {
        request = request.bearer_auth(token);
    }
    if let Some(body) = body {
        request = request.json(body);
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            return CapturedResponse {
                response: None,
                private_json: None,
                redactions: Vec::new(),
                failure: Some(ProbeFailure {
                    stage: ProbeFailureStage::Request,
                    explanation: redact_text(&error.to_string(), sensitive_values),
                }),
            };
        }
    };
    let status = response.status().as_u16();
    let headers = allowlisted_response_headers(&response, sensitive_values);
    let body = match read_limited_body(response, MAXIMUM_RESPONSE_BYTES).await {
        Ok(body) => body,
        Err(error) => {
            return CapturedResponse {
                response: None,
                private_json: None,
                redactions: Vec::new(),
                failure: Some(ProbeFailure {
                    stage: ProbeFailureStage::Response,
                    explanation: redact_text(&error.to_string(), sensitive_values),
                }),
            };
        }
    };
    match body {
        CollectedBody::TooLarge { observed } => CapturedResponse {
            response: Some(HttpResponseTranscript {
                status,
                headers,
                body_bytes_observed: observed as u64,
                raw_body_sha256: None,
                body: SanitizedResponseBody::Omitted {
                    reason: format!(
                        "response exceeds maximum capture size of {MAXIMUM_RESPONSE_BYTES} bytes"
                    ),
                },
            }),
            private_json: None,
            redactions: Vec::new(),
            failure: Some(ProbeFailure {
                stage: ProbeFailureStage::Response,
                explanation: format!("{response_label} response exceeded the evidence body limit"),
            }),
        },
        CollectedBody::Complete(bytes) => {
            capture_complete_body(status, headers, bytes, sensitive_values)
        }
    }
}

fn capture_complete_body(
    status: u16,
    headers: BTreeMap<String, String>,
    bytes: Vec<u8>,
    sensitive_values: &[String],
) -> CapturedResponse {
    if bytes.is_empty() {
        return CapturedResponse {
            response: Some(HttpResponseTranscript {
                status,
                headers,
                body_bytes_observed: 0,
                raw_body_sha256: None,
                body: SanitizedResponseBody::Omitted {
                    reason: "response body is empty".to_owned(),
                },
            }),
            private_json: None,
            redactions: Vec::new(),
            failure: None,
        };
    }

    let digest = sha256_hex(&bytes);
    match serde_json::from_slice::<Value>(&bytes) {
        Ok(value) => {
            let private_json = value.clone();
            let sanitized = sanitize_json(value, sensitive_values);
            let raw_body_sha256 = sanitized.redactions.is_empty().then_some(digest);
            CapturedResponse {
                response: Some(HttpResponseTranscript {
                    status,
                    headers,
                    body_bytes_observed: bytes.len() as u64,
                    raw_body_sha256,
                    body: SanitizedResponseBody::Json {
                        value: sanitized.value,
                    },
                }),
                private_json: Some(private_json),
                redactions: sanitized
                    .redactions
                    .into_iter()
                    .map(|pointer| format!("response.body{pointer}"))
                    .collect(),
                failure: None,
            }
        }
        Err(_) => CapturedResponse {
            response: Some(HttpResponseTranscript {
                status,
                headers,
                body_bytes_observed: bytes.len() as u64,
                raw_body_sha256: None,
                body: SanitizedResponseBody::Omitted {
                    reason: "response body is not valid JSON".to_owned(),
                },
            }),
            private_json: None,
            redactions: Vec::new(),
            failure: None,
        },
    }
}

enum CollectedBody {
    Complete(Vec<u8>),
    TooLarge { observed: usize },
}

async fn read_limited_body(mut response: Response, limit: usize) -> Result<CollectedBody> {
    if let Some(length) = response.content_length() {
        if length > limit as u64 {
            return Ok(CollectedBody::TooLarge {
                observed: usize::try_from(length).unwrap_or(usize::MAX),
            });
        }
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        let observed = bytes.len().saturating_add(chunk.len());
        if observed > limit {
            return Ok(CollectedBody::TooLarge { observed });
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(CollectedBody::Complete(bytes))
}

fn allowlisted_response_headers(
    response: &Response,
    sensitive_values: &[String],
) -> BTreeMap<String, String> {
    [
        "content-type",
        "iceberg-version",
        "x-request-id",
        "traceparent",
    ]
    .into_iter()
    .filter_map(|name| {
        response.headers().get(name).and_then(|value| {
            value
                .to_str()
                .ok()
                .map(|value| (name.to_owned(), redact_text(value, sensitive_values)))
        })
    })
    .collect()
}

pub(crate) fn endpoint_url(
    base_url: &str,
    path: &str,
    query: &BTreeMap<String, String>,
) -> Result<Url> {
    let mut url = Url::parse(&format!("{base_url}{path}"))
        .with_context(|| format!("invalid adapter endpoint `{base_url}{path}`"))?;
    if !query.is_empty() {
        url.query_pairs_mut().extend_pairs(query);
    }
    Ok(url)
}

pub(crate) fn authentication_mode(authentication: &CatalogAuthentication) -> &'static str {
    match authentication {
        CatalogAuthentication::Anonymous => "anonymous",
        CatalogAuthentication::OAuth2ClientCredentials { .. } => "oauth2-client-credentials",
    }
}

pub(crate) fn redact_text(text: &str, sensitive_values: &[String]) -> String {
    sensitive_values
        .iter()
        .filter(|value| value.len() >= 4)
        .fold(text.to_owned(), |redacted, value| {
            redacted.replace(value, REDACTED)
        })
}
