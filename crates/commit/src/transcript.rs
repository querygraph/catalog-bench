use std::error::Error;
use std::fmt::{Display, Formatter};

use catalog_bench_common::contract::{ComponentId, ProfileId};
use catalog_bench_common::sanitization::{audit_serialized_values, SerializedValueAuditFailure};
use catalog_bench_conformance::{
    CatalogNegotiationEvidence, CatalogNegotiationFailure, ContractDigests, TranscriptScenario,
};
use serde::{Deserialize, Serialize};

use crate::policy::{CatalogRun, ContentionParameters, RoundKind};
use crate::stats::MedianRange;
use crate::workflow::RoundExecution;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptProfile {
    pub id: ProfileId,
    pub resolved_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerTranscript {
    pub component: ComponentId,
    pub name: String,
    pub version: String,
    pub source_revision: String,
    pub operating_system: String,
    pub architecture: String,
    pub profile_runtime_matches: bool,
    pub profile_source_matches: bool,
}

impl RunnerTranscript {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.profile_runtime_matches && self.profile_source_matches
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogRoundTranscript {
    pub repetition: u32,
    pub kind: RoundKind,
    pub position: u32,
    pub catalog: CatalogRun,
    pub negotiation: CatalogNegotiationEvidence,
    pub outcome: CatalogRoundOutcome,
}

impl CatalogRoundTranscript {
    #[must_use]
    pub fn passed(&self) -> bool {
        matches!(
            &self.outcome,
            CatalogRoundOutcome::Executed { execution } if execution.passed()
        )
    }

    #[must_use]
    pub fn execution(&self) -> Option<&RoundExecution> {
        match &self.outcome {
            CatalogRoundOutcome::Executed { execution } => Some(execution),
            CatalogRoundOutcome::NegotiationFailed { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CatalogRoundOutcome {
    Executed { execution: Box<RoundExecution> },
    NegotiationFailed { failure: CatalogNegotiationFailure },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoundTally {
    pub scheduled: u32,
    pub executed: u32,
    pub passed: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentionMeasurements {
    pub sequential_latency_p50_ms: MedianRange,
    pub sequential_latency_p95_ms: MedianRange,
    pub sequential_latency_p99_ms: MedianRange,
    pub sequential_accepted_throughput_per_second: MedianRange,
    pub concurrent_latency_p50_ms: MedianRange,
    pub concurrent_latency_p95_ms: MedianRange,
    pub concurrent_latency_p99_ms: MedianRange,
    pub concurrent_attempted_throughput_per_second: MedianRange,
    pub concurrent_accepted_throughput_per_second: MedianRange,
    pub concurrent_conflict_rate: MedianRange,
    pub concurrent_error_rate: MedianRange,
    pub concurrent_attempts: MedianRange,
    pub concurrent_accepted: MedianRange,
    pub concurrent_conflicts: MedianRange,
    pub metadata_object_growth: MedianRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CatalogAggregateClassification {
    Pass,
    Fail { reasons: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogAggregate {
    pub catalog: CatalogRun,
    pub conditioning: RoundTally,
    pub measured: RoundTally,
    pub classification: CatalogAggregateClassification,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measurements: Option<ContentionMeasurements>,
}

impl CatalogAggregate {
    #[must_use]
    pub fn passed(&self) -> bool {
        matches!(self.classification, CatalogAggregateClassification::Pass)
            && self.measurements.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RankingBasis {
    ConcurrentAcceptedThroughputPerSecond,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RankingTieBreaker {
    SequentialLatencyP50Ascending,
    CatalogIdAscending,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum RankingDisposition {
    Ranked { rank: u32, score: MedianRange },
    NotRanked { reasons: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RankingEntry {
    pub catalog: CatalogRun,
    pub disposition: RankingDisposition,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentionRanking {
    pub basis: RankingBasis,
    pub tie_breakers: Vec<RankingTieBreaker>,
    pub entries: Vec<RankingEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SweepClassification {
    Pass,
    Fail { failed_catalogs: Vec<ComponentId> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentionSanitization {
    pub policy: String,
    pub redactions: Vec<String>,
    pub raw_secrets_persisted: bool,
    pub raw_response_body_persisted: bool,
    pub raw_request_identities_persisted: bool,
    pub write_mode: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentionTranscript {
    pub format: String,
    pub scenario: TranscriptScenario,
    pub contract_digests: ContractDigests,
    pub profile: TranscriptProfile,
    pub runner: RunnerTranscript,
    pub fixture_id: String,
    pub parameters: ContentionParameters,
    pub rounds: Vec<CatalogRoundTranscript>,
    pub aggregates: Vec<CatalogAggregate>,
    pub ranking: ContentionRanking,
    pub classification: SweepClassification,
    pub sanitization: ContentionSanitization,
}

impl ContentionTranscript {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.runner.passed()
            && matches!(self.classification, SweepClassification::Pass)
            && !self.sanitization.raw_secrets_persisted
            && !self.sanitization.raw_response_body_persisted
            && !self.sanitization.raw_request_identities_persisted
    }

    pub fn audit_serialized_values(
        &self,
        sensitive_values: &[String],
    ) -> Result<(), SanitizationViolation> {
        let identity_prefixes = self
            .rounds
            .iter()
            .map(|round| {
                format!(
                    "{}/{}/{}/",
                    round.catalog.catalog, self.fixture_id, round.repetition
                )
            })
            .collect::<Vec<_>>();
        audit_serialized_values(self, sensitive_values, &identity_prefixes).map_err(Into::into)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SanitizationViolation {
    SerializationFailed,
    SensitiveRuntimeValue,
    RawRequestIdentity,
}

impl Display for SanitizationViolation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SerializationFailed => {
                formatter.write_str("contention transcript could not be serialized for audit")
            }
            Self::SensitiveRuntimeValue => {
                formatter.write_str("contention transcript contains a sensitive runtime value")
            }
            Self::RawRequestIdentity => {
                formatter.write_str("contention transcript contains a raw request identity")
            }
        }
    }
}

impl Error for SanitizationViolation {}

impl From<SerializedValueAuditFailure> for SanitizationViolation {
    fn from(failure: SerializedValueAuditFailure) -> Self {
        match failure {
            SerializedValueAuditFailure::Serialization => Self::SerializationFailed,
            SerializedValueAuditFailure::SensitiveValue => Self::SensitiveRuntimeValue,
            SerializedValueAuditFailure::ForbiddenValue => Self::RawRequestIdentity,
        }
    }
}
