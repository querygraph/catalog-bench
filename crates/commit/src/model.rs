use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::time::Duration;

use catalog_bench_common::contract::Distribution;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};

use crate::stats::{latency_distribution, per_second, StatisticsError};

#[derive(Clone, PartialEq, Eq)]
pub struct RequestIdentity(String);

impl RequestIdentity {
    pub fn new(value: impl Into<String>) -> Result<Self, ModelError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ModelError::EmptyRequestIdentity);
        }
        Ok(Self(value))
    }

    /// Expose the identity only at the request-construction boundary. It must
    /// never be copied into a serializable transcript type.
    #[must_use]
    pub fn expose_for_request(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn digest(&self) -> RequestDigest {
        RequestDigest::from_bytes(self.0.as_bytes())
    }
}

impl Debug for RequestIdentity {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RequestIdentity(<redacted>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct RequestDigest(String);

impl RequestDigest {
    #[must_use]
    pub fn from_bytes(value: &[u8]) -> Self {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let value = Sha256::digest(value)
            .iter()
            .flat_map(|byte| {
                [
                    HEX[(byte >> 4) as usize] as char,
                    HEX[(byte & 0x0f) as usize] as char,
                ]
            })
            .collect();
        Self(value)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for RequestDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        if value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            Ok(Self(value))
        } else {
            Err(serde::de::Error::custom(
                "request digest must contain 64 lowercase hexadecimal characters",
            ))
        }
    }
}

#[derive(Debug, Default)]
pub struct AcceptedRequests(HashSet<RequestDigest>);

impl AcceptedRequests {
    pub fn insert(&mut self, identity: &RequestIdentity) {
        self.0.insert(identity.digest());
    }

    pub fn try_extend(&mut self, other: Self) -> Result<(), ModelError> {
        if !self.0.is_disjoint(&other.0) {
            return Err(ModelError::DuplicateRequestIdentity);
        }
        self.0.extend(other.0);
        Ok(())
    }

    #[must_use]
    pub fn contains_value(&self, value: &str) -> bool {
        self.0
            .contains(&RequestDigest::from_bytes(value.as_bytes()))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RequestErrorKind {
    Transport,
    Timeout,
    ResponseTooLarge,
    UnexpectedHttp,
    MalformedResponse,
    Harness,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SanitizedRequestError {
    pub kind: RequestErrorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestOutcome {
    Accepted,
    Conflict,
    Error(SanitizedRequestError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestCounts {
    pub attempts: u64,
    pub accepted: u64,
    pub conflicts: u64,
    pub errors: u64,
}

impl RequestCounts {
    #[must_use]
    pub fn fully_accounted(&self) -> bool {
        self.accepted
            .checked_add(self.conflicts)
            .and_then(|count| count.checked_add(self.errors))
            == Some(self.attempts)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeLatencies {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub all: Option<Distribution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted: Option<Distribution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflicts: Option<Distribution>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub errors: Option<Distribution>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorCount {
    pub error: SanitizedRequestError,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseEvidence {
    pub elapsed_ms: f64,
    pub counts: RequestCounts,
    pub latency_ms: OutcomeLatencies,
    pub attempted_throughput_per_second: f64,
    pub accepted_throughput_per_second: f64,
    pub conflict_rate: f64,
    pub error_rate: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub error_counts: Vec<ErrorCount>,
}

impl PhaseEvidence {
    #[must_use]
    pub fn made_progress(&self) -> bool {
        self.counts.accepted > 0
    }
}

#[derive(Debug, Default)]
pub struct RequestLedger {
    seen_requests: HashSet<RequestDigest>,
    all: Vec<Duration>,
    accepted: Vec<Duration>,
    conflicts: Vec<Duration>,
    errors: Vec<Duration>,
    error_counts: BTreeMap<SanitizedRequestError, u64>,
    accepted_requests: AcceptedRequests,
}

impl RequestLedger {
    pub fn record(
        &mut self,
        identity: &RequestIdentity,
        latency: Duration,
        outcome: RequestOutcome,
    ) -> Result<(), ModelError> {
        if !self.seen_requests.insert(identity.digest()) {
            return Err(ModelError::DuplicateRequestIdentity);
        }
        self.all.push(latency);
        match outcome {
            RequestOutcome::Accepted => {
                self.accepted.push(latency);
                self.accepted_requests.insert(identity);
            }
            RequestOutcome::Conflict => self.conflicts.push(latency),
            RequestOutcome::Error(error) => {
                self.errors.push(latency);
                let count = self.error_counts.entry(error).or_default();
                *count = count.checked_add(1).ok_or(ModelError::CountOverflow)?;
            }
        }
        Ok(())
    }

    pub fn finish(
        self,
        elapsed: Duration,
    ) -> Result<(PhaseEvidence, AcceptedRequests), ModelError> {
        let counts = RequestCounts {
            attempts: count(&self.all)?,
            accepted: count(&self.accepted)?,
            conflicts: count(&self.conflicts)?,
            errors: count(&self.errors)?,
        };
        if !counts.fully_accounted() {
            return Err(ModelError::UnaccountedRequests);
        }
        let denominator = counts.attempts;
        let phase = PhaseEvidence {
            elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
            counts,
            latency_ms: OutcomeLatencies {
                all: optional_distribution(&self.all)?,
                accepted: optional_distribution(&self.accepted)?,
                conflicts: optional_distribution(&self.conflicts)?,
                errors: optional_distribution(&self.errors)?,
            },
            attempted_throughput_per_second: per_second(counts.attempts, elapsed)?,
            accepted_throughput_per_second: per_second(counts.accepted, elapsed)?,
            conflict_rate: ratio(counts.conflicts, denominator),
            error_rate: ratio(counts.errors, denominator),
            error_counts: self
                .error_counts
                .into_iter()
                .map(|(error, count)| ErrorCount { error, count })
                .collect(),
        };
        Ok((phase, self.accepted_requests))
    }

    pub fn try_merge(&mut self, mut other: Self) -> Result<(), ModelError> {
        if !self.seen_requests.is_disjoint(&other.seen_requests) {
            return Err(ModelError::DuplicateRequestIdentity);
        }
        for (error, count) in &other.error_counts {
            self.error_counts
                .get(error)
                .copied()
                .unwrap_or_default()
                .checked_add(*count)
                .ok_or(ModelError::CountOverflow)?;
        }
        self.accepted_requests.try_extend(other.accepted_requests)?;
        self.seen_requests.extend(other.seen_requests);
        self.all.append(&mut other.all);
        self.accepted.append(&mut other.accepted);
        self.conflicts.append(&mut other.conflicts);
        self.errors.append(&mut other.errors);
        for (error, count) in other.error_counts {
            *self.error_counts.entry(error).or_default() += count;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalStateAttribution {
    pub property_present: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_sha256: Option<RequestDigest>,
    pub belongs_to_accepted_request: bool,
    pub metadata_location_within_table_root: bool,
}

impl FinalStateAttribution {
    #[must_use]
    pub fn evaluate(
        final_property: Option<&str>,
        accepted: &AcceptedRequests,
        metadata_location_within_table_root: bool,
    ) -> Self {
        Self {
            property_present: final_property.is_some(),
            request_sha256: final_property.map(|value| RequestDigest::from_bytes(value.as_bytes())),
            belongs_to_accepted_request: final_property
                .is_some_and(|value| accepted.contains_value(value)),
            metadata_location_within_table_root,
        }
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        self.property_present
            && self.belongs_to_accepted_request
            && self.metadata_location_within_table_root
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataGrowthEvidence {
    pub baseline_metadata_objects: u64,
    pub final_metadata_objects: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_growth: Option<u64>,
    pub minimum_required_growth: u64,
    pub sufficient: bool,
}

impl MetadataGrowthEvidence {
    pub fn evaluate(
        baseline_metadata_objects: u64,
        final_metadata_objects: u64,
        phase_counts: impl IntoIterator<Item = RequestCounts>,
    ) -> Result<Self, ModelError> {
        let minimum_required_growth =
            phase_counts.into_iter().try_fold(0_u64, |total, counts| {
                total
                    .checked_add(counts.accepted)
                    .ok_or(ModelError::CountOverflow)
            })?;
        let observed_growth = final_metadata_objects.checked_sub(baseline_metadata_objects);
        Ok(Self {
            baseline_metadata_objects,
            final_metadata_objects,
            observed_growth,
            minimum_required_growth,
            sufficient: observed_growth.is_some_and(|growth| growth >= minimum_required_growth),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelError {
    EmptyRequestIdentity,
    DuplicateRequestIdentity,
    CountOverflow,
    UnaccountedRequests,
    Statistics(StatisticsError),
}

impl Display for ModelError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyRequestIdentity => formatter.write_str("request identity must not be empty"),
            Self::DuplicateRequestIdentity => {
                formatter.write_str("request identity was reused within one phase")
            }
            Self::CountOverflow => formatter.write_str("request accounting overflowed u64"),
            Self::UnaccountedRequests => {
                formatter.write_str("one or more requests were unaccounted")
            }
            Self::Statistics(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ModelError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Statistics(error) => Some(error),
            _ => None,
        }
    }
}

impl From<StatisticsError> for ModelError {
    fn from(error: StatisticsError) -> Self {
        Self::Statistics(error)
    }
}

fn count(samples: &[Duration]) -> Result<u64, ModelError> {
    u64::try_from(samples.len()).map_err(|_| ModelError::CountOverflow)
}

fn optional_distribution(samples: &[Duration]) -> Result<Option<Distribution>, StatisticsError> {
    if samples.is_empty() {
        Ok(None)
    } else {
        latency_distribution(samples).map(Some)
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}
