use std::collections::BTreeMap;

use catalog_bench_common::contract::{
    AdapterRequestHandling, AssertionId, AssertionOutcome, CapabilityId,
    CapabilityLimitationSource, CatalogProtocol, ComponentId, ScenarioId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractDigests {
    pub profile_sha256: String,
    pub scenario_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProbeFailureStage {
    Authentication,
    Request,
    Response,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeFailure {
    pub stage: ProbeFailureStage,
    pub explanation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ProbeClassification {
    Pass,
    Fail {
        summary: String,
    },
    Unsupported {
        capability: CapabilityId,
        attributed_to: CapabilityLimitationSource,
        explanation: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptScenario {
    pub id: ScenarioId,
    pub version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptAdapter {
    pub catalog: ComponentId,
    pub name: String,
    pub version: String,
    pub protocol: CatalogProtocol,
    pub request_handling: AdapterRequestHandling,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthenticationOutcome {
    Ready,
    Failed,
    NotAttempted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticationTranscript {
    pub mode: String,
    pub outcome: AuthenticationOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpRequestTranscript {
    pub method: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SanitizedResponseBody {
    Json { value: Value },
    Omitted { reason: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpResponseTranscript {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body_bytes_observed: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_body_sha256: Option<String>,
    pub body: SanitizedResponseBody,
}

impl HttpResponseTranscript {
    pub(crate) fn json(&self) -> Option<&Value> {
        match &self.body {
            SanitizedResponseBody::Json { value } => Some(value),
            SanitizedResponseBody::Omitted { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeAssertion {
    pub assertion: AssertionId,
    pub required: bool,
    pub outcome: AssertionOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SanitizationTranscript {
    pub policy: String,
    pub redactions: Vec<String>,
    pub raw_secrets_persisted: bool,
    pub raw_response_body_persisted: bool,
}

#[must_use]
pub(crate) fn transcript_adapter(
    adapter: &catalog_bench_common::contract::CatalogAdapter,
    component: &catalog_bench_common::contract::Component,
) -> TranscriptAdapter {
    TranscriptAdapter {
        catalog: adapter.catalog.clone(),
        name: component.name.clone(),
        version: component.version.clone(),
        protocol: adapter.protocol,
        request_handling: adapter.request_handling.clone(),
    }
}

#[must_use]
pub(crate) fn not_evaluated_assertions(
    scenario: &catalog_bench_common::contract::Scenario,
    reason: &str,
) -> Vec<ProbeAssertion> {
    scenario
        .assertions
        .iter()
        .map(|assertion| ProbeAssertion {
            assertion: assertion.id.clone(),
            required: assertion.required,
            outcome: AssertionOutcome::NotEvaluated {
                reason: reason.to_owned(),
            },
        })
        .collect()
}

#[must_use]
pub(crate) fn passed_required_assertions(assertions: &[ProbeAssertion]) -> bool {
    assertions.iter().all(|evaluation| {
        !evaluation.required || matches!(evaluation.outcome, AssertionOutcome::Pass)
    })
}

#[must_use]
pub(crate) fn transcript_scenario(
    scenario: &catalog_bench_common::contract::Scenario,
) -> TranscriptScenario {
    TranscriptScenario {
        id: scenario.id.clone(),
        version: scenario.version,
    }
}
