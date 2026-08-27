use std::error::Error;
use std::fmt::{Display, Formatter};

use catalog_bench_conformance::{
    AuthenticationOutcome, CatalogNegotiationEvidence, NamespaceSeparatorResolution,
    PrefixResolution, ProbeFailureStage, TranscriptAdapter,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EngineAuthenticationMode {
    Anonymous,
    OAuth2ClientCredentials,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineAuthenticationEvidence {
    pub mode: EngineAuthenticationMode,
    pub outcome: AuthenticationOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EngineRoutingResolution {
    Unprefixed,
    Static,
    Negotiated,
    Default,
    Configured,
    Failed,
    NotEvaluated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineCatalogConfigEvidence {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_bytes: Option<u64>,
    pub prefix: EngineRoutingResolution,
    pub namespace_separator: EngineRoutingResolution,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_stage: Option<ProbeFailureStage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineCatalogNegotiationEvidence {
    pub adapter: TranscriptAdapter,
    pub authentication: EngineAuthenticationEvidence,
    pub config: EngineCatalogConfigEvidence,
    pub redactions_observed: u64,
}

impl TryFrom<CatalogNegotiationEvidence> for EngineCatalogNegotiationEvidence {
    type Error = EngineNegotiationProjectionFailure;

    fn try_from(evidence: CatalogNegotiationEvidence) -> Result<Self, Self::Error> {
        let mode = match evidence.authentication.mode.as_str() {
            "anonymous" => EngineAuthenticationMode::Anonymous,
            "oauth2-client-credentials" => EngineAuthenticationMode::OAuth2ClientCredentials,
            _ => return Err(EngineNegotiationProjectionFailure),
        };
        let redactions_observed = u64::try_from(evidence.redactions.len())
            .map_err(|_| EngineNegotiationProjectionFailure)?;
        Ok(Self {
            adapter: evidence.adapter,
            authentication: EngineAuthenticationEvidence {
                mode,
                outcome: evidence.authentication.outcome,
                http_status: evidence.authentication.http_status,
            },
            config: EngineCatalogConfigEvidence {
                http_status: evidence
                    .config
                    .response
                    .as_ref()
                    .map(|response| response.status),
                response_bytes: evidence
                    .config
                    .response
                    .as_ref()
                    .map(|response| response.body_bytes_observed),
                prefix: prefix_resolution(&evidence.config.prefix),
                namespace_separator: namespace_separator_resolution(
                    &evidence.config.namespace_separator,
                ),
                failure_stage: evidence.config.failure.map(|failure| failure.stage),
            },
            redactions_observed,
        })
    }
}

fn prefix_resolution(resolution: &PrefixResolution) -> EngineRoutingResolution {
    match resolution {
        PrefixResolution::Unprefixed => EngineRoutingResolution::Unprefixed,
        PrefixResolution::Static { .. } => EngineRoutingResolution::Static,
        PrefixResolution::Negotiated { .. } => EngineRoutingResolution::Negotiated,
        PrefixResolution::Failed { .. } => EngineRoutingResolution::Failed,
        PrefixResolution::NotEvaluated { .. } => EngineRoutingResolution::NotEvaluated,
    }
}

fn namespace_separator_resolution(
    resolution: &NamespaceSeparatorResolution,
) -> EngineRoutingResolution {
    match resolution {
        NamespaceSeparatorResolution::Default { .. } => EngineRoutingResolution::Default,
        NamespaceSeparatorResolution::Configured { .. } => EngineRoutingResolution::Configured,
        NamespaceSeparatorResolution::Failed { .. } => EngineRoutingResolution::Failed,
        NamespaceSeparatorResolution::NotEvaluated { .. } => EngineRoutingResolution::NotEvaluated,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineNegotiationProjectionFailure;

impl Display for EngineNegotiationProjectionFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("catalog negotiation could not be projected into bounded evidence")
    }
}

impl Error for EngineNegotiationProjectionFailure {}
