use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use anyhow::{bail, Result};
use catalog_bench_common::contract::{ComponentId, Profile, Scenario};
use reqwest::{Method, Response};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::evidence::{
    transcript_adapter, AuthenticationTranscript, ProbeFailureStage, TranscriptAdapter,
};
use crate::iceberg::{CatalogRoutes, NamespaceIdentifier};
use crate::routing::{negotiate_routing, RoutingConfigTranscript};
use crate::target::ProbeTarget;
use crate::transport::{
    drain_limited_body, http_client, read_limited_body, redact_text, CollectedBody,
    MAXIMUM_RESPONSE_BYTES,
};

pub const CATALOG_RESPONSE_LIMIT_BYTES: usize = MAXIMUM_RESPONSE_BYTES;
const MAXIMUM_FAILURE_DETAIL_CHARACTERS: usize = 512;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogNegotiationEvidence {
    pub adapter: TranscriptAdapter,
    pub authentication: AuthenticationTranscript,
    pub config: RoutingConfigTranscript,
    pub redactions: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CatalogNegotiationFailureStage {
    Authentication,
    Config,
    Routing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogNegotiationFailure {
    pub stage: CatalogNegotiationFailureStage,
    pub detail: String,
}

pub struct CatalogConnectionAttempt {
    pub evidence: CatalogNegotiationEvidence,
    pub outcome: CatalogConnectionOutcome,
}

pub enum CatalogConnectionOutcome {
    Ready(CatalogSession),
    Failed(CatalogNegotiationFailure),
}

/// Authenticate, read standard Iceberg REST config, and resolve profile-driven
/// routing without exposing credentials to the caller.
pub async fn connect_catalog<F>(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &ComponentId,
    request_timeout_ms: u64,
    maximum_response_bytes: usize,
    getenv: F,
) -> Result<CatalogConnectionAttempt>
where
    F: Fn(&str) -> Option<String>,
{
    validate_connection_limits(request_timeout_ms, maximum_response_bytes)?;
    let target = ProbeTarget::resolve(profile, scenario, catalog)?;
    if let Some(limitation) = target.first_required_limitation(scenario) {
        bail!(
            "catalog `{catalog}` declares required capability `{}` unsupported",
            limitation.capability
        );
    }
    connect_target(target, request_timeout_ms, maximum_response_bytes, getenv).await
}

/// Authenticate, negotiate standard Iceberg REST configuration, and construct
/// profile-driven routes without classifying scenario capabilities.
///
/// This is the reusable boundary for orchestration whose capabilities belong
/// to a client or engine. Catalog conformance probes must continue to use
/// [`connect_catalog`], which additionally validates the catalog capability
/// vocabulary and declared limitations before any network access.
pub async fn connect_catalog_adapter<F>(
    profile: &Profile,
    catalog: &ComponentId,
    request_timeout_ms: u64,
    maximum_response_bytes: usize,
    getenv: F,
) -> Result<CatalogConnectionAttempt>
where
    F: Fn(&str) -> Option<String>,
{
    validate_connection_limits(request_timeout_ms, maximum_response_bytes)?;
    let target = ProbeTarget::resolve_adapter(profile, catalog)?;
    connect_target(target, request_timeout_ms, maximum_response_bytes, getenv).await
}

async fn connect_target<F>(
    target: ProbeTarget<'_>,
    request_timeout_ms: u64,
    maximum_response_bytes: usize,
    getenv: F,
) -> Result<CatalogConnectionAttempt>
where
    F: Fn(&str) -> Option<String>,
{
    let client = http_client(request_timeout_ms)?;
    let routing = negotiate_routing(&client, target.adapter, &getenv).await?;
    let mut redactions = routing.redactions.clone();
    redactions.sort();
    redactions.dedup();
    let evidence = CatalogNegotiationEvidence {
        adapter: transcript_adapter(target.adapter, target.component),
        authentication: routing.authentication.transcript.clone(),
        config: routing.config.clone(),
        redactions,
    };
    if let Some(failure) = &routing.authentication.failure {
        let stage = match failure.stage {
            ProbeFailureStage::Authentication => CatalogNegotiationFailureStage::Authentication,
            ProbeFailureStage::Request | ProbeFailureStage::Response => {
                CatalogNegotiationFailureStage::Config
            }
        };
        return Ok(CatalogConnectionAttempt {
            evidence,
            outcome: CatalogConnectionOutcome::Failed(CatalogNegotiationFailure {
                stage,
                detail: bounded_detail(&failure.explanation),
            }),
        });
    }
    if !routing.config_routing.passed() {
        return Ok(CatalogConnectionAttempt {
            evidence,
            outcome: CatalogConnectionOutcome::Failed(CatalogNegotiationFailure {
                stage: CatalogNegotiationFailureStage::Config,
                detail: bounded_detail(
                    &routing
                        .config_routing
                        .explanation("catalog config routing did not pass"),
                ),
            }),
        });
    }
    let Some(codec) = routing.codec else {
        return Ok(CatalogConnectionAttempt {
            evidence,
            outcome: CatalogConnectionOutcome::Failed(CatalogNegotiationFailure {
                stage: CatalogNegotiationFailureStage::Routing,
                detail: "namespace codec was unavailable after successful config validation"
                    .to_owned(),
            }),
        });
    };
    let routes = match CatalogRoutes::new(target.adapter, &routing.config.prefix, codec, "catalog")
    {
        Ok(routes) => routes,
        Err(error) => {
            return Ok(CatalogConnectionAttempt {
                evidence,
                outcome: CatalogConnectionOutcome::Failed(CatalogNegotiationFailure {
                    stage: CatalogNegotiationFailureStage::Routing,
                    detail: bounded_detail(&error.to_string()),
                }),
            });
        }
    };
    Ok(CatalogConnectionAttempt {
        evidence,
        outcome: CatalogConnectionOutcome::Ready(CatalogSession(Arc::new(CatalogSessionInner {
            client,
            bearer_token: routing.authentication.bearer_token,
            sensitive_values: routing.authentication.sensitive_values,
            routes,
            maximum_response_bytes,
        }))),
    })
}

fn validate_connection_limits(
    request_timeout_ms: u64,
    maximum_response_bytes: usize,
) -> Result<()> {
    if request_timeout_ms == 0 {
        bail!("catalog request timeout must be positive");
    }
    if maximum_response_bytes != CATALOG_RESPONSE_LIMIT_BYTES {
        bail!(
            "shared catalog runtime supports a {}-byte response limit, found {maximum_response_bytes}",
            CATALOG_RESPONSE_LIMIT_BYTES
        );
    }
    Ok(())
}

#[derive(Clone)]
pub struct CatalogSession(Arc<CatalogSessionInner>);

struct CatalogSessionInner {
    client: reqwest::Client,
    bearer_token: Option<String>,
    sensitive_values: Vec<String>,
    routes: CatalogRoutes,
    maximum_response_bytes: usize,
}

impl CatalogSession {
    pub fn namespace_collection_url(&self) -> Result<Url> {
        self.0.routes.namespace_collection()
    }

    pub fn namespace_url(&self, namespace: &NamespaceIdentifier) -> Result<Url> {
        self.0.routes.namespace(namespace)
    }

    pub fn table_collection_url(&self, namespace: &NamespaceIdentifier) -> Result<Url> {
        self.0.routes.table_collection(namespace)
    }

    pub fn table_url(&self, namespace: &NamespaceIdentifier, table: &str) -> Result<Url> {
        self.0.routes.table(namespace, table)
    }

    /// Send one standard JSON request. Headers are deliberately closed: callers
    /// cannot add implementation-specific behavior such as idempotency keys.
    pub async fn request_json(
        &self,
        method: Method,
        url: Url,
        body: Option<&Value>,
        capture: ResponseCapture,
    ) -> std::result::Result<CatalogResponse, CatalogRequestFailure> {
        let mut request = self
            .0
            .client
            .request(method, url)
            .header(reqwest::header::ACCEPT, "application/json");
        if let Some(token) = &self.0.bearer_token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.map_err(|error| {
            let kind = if error.is_timeout() {
                CatalogRequestFailureKind::Timeout
            } else {
                CatalogRequestFailureKind::Transport
            };
            CatalogRequestFailure::new(
                kind,
                None,
                bounded_detail(&redact_text(&error.to_string(), &self.0.sensitive_values)),
            )
        })?;
        collect_response(
            response,
            capture,
            self.0.maximum_response_bytes,
            &self.0.sensitive_values,
        )
        .await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseCapture {
    Json,
    Discard,
}

pub struct CatalogResponse {
    status: u16,
    body_bytes: u64,
    body: Option<Vec<u8>>,
}

impl CatalogResponse {
    #[must_use]
    pub fn status(&self) -> u16 {
        self.status
    }

    #[must_use]
    pub fn body_bytes(&self) -> u64 {
        self.body_bytes
    }

    pub fn json<T: DeserializeOwned>(&self) -> std::result::Result<T, CatalogRequestFailure> {
        let body = self.body.as_ref().ok_or_else(|| {
            CatalogRequestFailure::new(
                CatalogRequestFailureKind::BodyNotRetained,
                Some(self.status),
                "response body was deliberately discarded".to_owned(),
            )
        })?;
        serde_json::from_slice(body).map_err(|error| {
            CatalogRequestFailure::new(
                CatalogRequestFailureKind::MalformedJson,
                Some(self.status),
                bounded_detail(&format!("response was not valid expected JSON: {error}")),
            )
        })
    }
}

impl Debug for CatalogResponse {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CatalogResponse")
            .field("status", &self.status)
            .field("body_bytes", &self.body_bytes)
            .field("body", &self.body.as_ref().map(|_| "<private>"))
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CatalogRequestFailureKind {
    Timeout,
    Transport,
    ResponseRead,
    ResponseTooLarge,
    BodyNotRetained,
    MalformedJson,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogRequestFailure {
    pub kind: CatalogRequestFailureKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    pub detail: String,
}

impl CatalogRequestFailure {
    fn new(kind: CatalogRequestFailureKind, http_status: Option<u16>, detail: String) -> Self {
        Self {
            kind,
            http_status,
            detail,
        }
    }
}

impl Display for CatalogRequestFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self.http_status {
            Some(status) => write!(formatter, "HTTP {status}: {}", self.detail),
            None => formatter.write_str(&self.detail),
        }
    }
}

impl Error for CatalogRequestFailure {}

async fn collect_response(
    response: Response,
    capture: ResponseCapture,
    maximum_response_bytes: usize,
    sensitive_values: &[String],
) -> std::result::Result<CatalogResponse, CatalogRequestFailure> {
    let status = response.status().as_u16();
    let body = match capture {
        ResponseCapture::Json => read_limited_body(response, maximum_response_bytes).await,
        ResponseCapture::Discard => drain_limited_body(response, maximum_response_bytes).await,
    }
    .map_err(|error| {
        CatalogRequestFailure::new(
            CatalogRequestFailureKind::ResponseRead,
            Some(status),
            bounded_detail(&redact_text(
                &format!("failed to read bounded response: {error}"),
                sensitive_values,
            )),
        )
    })?;
    match body {
        CollectedBody::Complete { bytes, observed } => Ok(CatalogResponse {
            status,
            body_bytes: observed as u64,
            body: matches!(capture, ResponseCapture::Json).then_some(bytes),
        }),
        CollectedBody::TooLarge { observed } => Err(CatalogRequestFailure::new(
            CatalogRequestFailureKind::ResponseTooLarge,
            Some(status),
            format!(
                "response exceeded the {maximum_response_bytes}-byte limit after {observed} bytes"
            ),
        )),
    }
}

fn bounded_detail(detail: &str) -> String {
    let mut characters = detail.chars();
    let bounded = characters
        .by_ref()
        .take(MAXIMUM_FAILURE_DETAIL_CHARACTERS)
        .collect::<String>();
    if characters.next().is_some() {
        format!("{bounded}…")
    } else {
        bounded
    }
}
