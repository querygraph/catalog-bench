use std::error::Error;
use std::fmt::{Display, Formatter};

use crate::model::{MetadataGrowthEvidence, PhaseEvidence};
use crate::policy::{CatalogRun, ContentionPlan, RoundKind};
use crate::stats::{median_with_range, MedianRange, StatisticsError};
use crate::transcript::{
    CatalogAggregate, CatalogAggregateClassification, CatalogRoundTranscript,
    ContentionMeasurements, ContentionRanking, RankingBasis, RankingDisposition, RankingEntry,
    RankingTieBreaker, RoundTally, SweepClassification,
};
use crate::workflow::{OperationEvidence, RoundExecution};

pub fn aggregate_contention(
    plan: &ContentionPlan,
    rounds: &[CatalogRoundTranscript],
) -> Result<
    (
        Vec<CatalogAggregate>,
        ContentionRanking,
        SweepClassification,
    ),
    AggregationError,
> {
    validate_schedule(plan, rounds)?;
    let catalog_order = plan
        .rounds()
        .first()
        .ok_or(AggregationError::EmptySchedule)?
        .catalogs
        .clone();
    let mut aggregates = Vec::with_capacity(catalog_order.len());
    for catalog in catalog_order {
        aggregates.push(aggregate_catalog(plan, rounds, catalog)?);
    }
    let ranking = rank_catalogs(&aggregates)?;
    let failed_catalogs = aggregates
        .iter()
        .filter(|aggregate| !aggregate.passed())
        .map(|aggregate| aggregate.catalog.catalog.clone())
        .collect::<Vec<_>>();
    let classification = if failed_catalogs.is_empty() {
        SweepClassification::Pass
    } else {
        SweepClassification::Fail { failed_catalogs }
    };
    Ok((aggregates, ranking, classification))
}

fn validate_schedule(
    plan: &ContentionPlan,
    actual: &[CatalogRoundTranscript],
) -> Result<(), AggregationError> {
    let expected_count = plan
        .rounds()
        .iter()
        .try_fold(0_usize, |count, round| {
            count.checked_add(round.catalogs.len())
        })
        .ok_or(AggregationError::CountOverflow)?;
    if actual.len() != expected_count {
        return Err(AggregationError::ScheduleLength {
            expected: expected_count,
            actual: actual.len(),
        });
    }
    let expected = plan.rounds().iter().flat_map(|round| {
        round
            .catalogs
            .iter()
            .enumerate()
            .map(move |(position, catalog)| (round, position, catalog))
    });
    for (index, ((round, position, catalog), observed)) in expected.zip(actual).enumerate() {
        let expected_position =
            u32::try_from(position + 1).map_err(|_| AggregationError::CountOverflow)?;
        if observed.repetition != round.repetition
            || observed.kind != round.kind
            || observed.position != expected_position
            || observed.catalog != *catalog
            || observed.execution().is_some_and(|execution| {
                execution.descriptor.catalog != catalog.catalog.as_str()
                    || execution.descriptor.repetition != round.repetition
                    || execution.descriptor.kind != round.kind
            })
        {
            return Err(AggregationError::ScheduleMismatch { index });
        }
    }
    Ok(())
}

fn aggregate_catalog(
    plan: &ContentionPlan,
    rounds: &[CatalogRoundTranscript],
    catalog: CatalogRun,
) -> Result<CatalogAggregate, AggregationError> {
    let catalog_rounds = rounds
        .iter()
        .filter(|round| round.catalog.catalog == catalog.catalog)
        .collect::<Vec<_>>();
    let conditioning = tally(&catalog_rounds, RoundKind::Conditioning)?;
    let measured = tally(&catalog_rounds, RoundKind::Measured)?;
    let expected_conditioning = plan.parameters().round_policy.conditioning_rounds;
    let expected_measured = plan.parameters().round_policy.measured_rounds;
    let mut reasons = Vec::new();
    if conditioning.scheduled != expected_conditioning
        || conditioning.executed != expected_conditioning
        || conditioning.passed != expected_conditioning
    {
        reasons.push(format!(
            "conditioning rounds passed {}/{} ({} executed)",
            conditioning.passed, expected_conditioning, conditioning.executed
        ));
    }
    if measured.scheduled != expected_measured
        || measured.executed != expected_measured
        || measured.passed != expected_measured
    {
        reasons.push(format!(
            "measured rounds passed {}/{} ({} executed)",
            measured.passed, expected_measured, measured.executed
        ));
    }
    let (classification, measurements) = if reasons.is_empty() {
        let metrics = catalog_rounds
            .iter()
            .filter(|round| round.kind == RoundKind::Measured)
            .map(|round| {
                round
                    .execution()
                    .ok_or(AggregationError::MissingExecution)
                    .and_then(RoundMetrics::from_execution)
            })
            .collect::<Result<Vec<_>, _>>()?;
        (
            CatalogAggregateClassification::Pass,
            Some(ContentionMeasurements::from_rounds(&metrics)?),
        )
    } else {
        (CatalogAggregateClassification::Fail { reasons }, None)
    };
    Ok(CatalogAggregate {
        catalog,
        conditioning,
        measured,
        classification,
        measurements,
    })
}

fn tally(
    rounds: &[&CatalogRoundTranscript],
    kind: RoundKind,
) -> Result<RoundTally, AggregationError> {
    let mut scheduled = 0_u32;
    let mut executed = 0_u32;
    let mut passed = 0_u32;
    for round in rounds.iter().filter(|round| round.kind == kind) {
        scheduled = scheduled
            .checked_add(1)
            .ok_or(AggregationError::CountOverflow)?;
        if round.execution().is_some() {
            executed = executed
                .checked_add(1)
                .ok_or(AggregationError::CountOverflow)?;
        }
        if round.passed() {
            passed = passed
                .checked_add(1)
                .ok_or(AggregationError::CountOverflow)?;
        }
    }
    Ok(RoundTally {
        scheduled,
        executed,
        passed,
    })
}

fn rank_catalogs(aggregates: &[CatalogAggregate]) -> Result<ContentionRanking, AggregationError> {
    let mut ranked = aggregates
        .iter()
        .filter_map(|aggregate| {
            aggregate.measurements.as_ref().map(|measurements| {
                (
                    aggregate,
                    &measurements.concurrent_accepted_throughput_per_second,
                    &measurements.sequential_latency_p50_ms,
                )
            })
        })
        .collect::<Vec<_>>();
    ranked.sort_by(
        |(left, left_score, left_latency), (right, right_score, right_latency)| {
            right_score
                .median
                .total_cmp(&left_score.median)
                .then_with(|| left_latency.median.total_cmp(&right_latency.median))
                .then_with(|| {
                    left.catalog
                        .catalog
                        .as_str()
                        .cmp(right.catalog.catalog.as_str())
                })
        },
    );

    let mut entries = Vec::with_capacity(aggregates.len());
    for (index, (aggregate, score, _)) in ranked.into_iter().enumerate() {
        entries.push(RankingEntry {
            catalog: aggregate.catalog.clone(),
            disposition: RankingDisposition::Ranked {
                rank: u32::try_from(index + 1).map_err(|_| AggregationError::CountOverflow)?,
                score: score.clone(),
            },
        });
    }
    for aggregate in aggregates.iter().filter(|aggregate| !aggregate.passed()) {
        let CatalogAggregateClassification::Fail { reasons } = &aggregate.classification else {
            return Err(AggregationError::InvalidAggregate);
        };
        entries.push(RankingEntry {
            catalog: aggregate.catalog.clone(),
            disposition: RankingDisposition::NotRanked {
                reasons: reasons.clone(),
            },
        });
    }
    Ok(ContentionRanking {
        basis: RankingBasis::ConcurrentAcceptedThroughputPerSecond,
        tie_breakers: vec![
            RankingTieBreaker::SequentialLatencyP50Ascending,
            RankingTieBreaker::CatalogIdAscending,
        ],
        entries,
    })
}

#[derive(Debug)]
struct RoundMetrics {
    sequential_latency_p50_ms: f64,
    sequential_latency_p95_ms: f64,
    sequential_latency_p99_ms: f64,
    sequential_accepted_throughput_per_second: f64,
    concurrent_latency_p50_ms: f64,
    concurrent_latency_p95_ms: f64,
    concurrent_latency_p99_ms: f64,
    concurrent_attempted_throughput_per_second: f64,
    concurrent_accepted_throughput_per_second: f64,
    concurrent_conflict_rate: f64,
    concurrent_error_rate: f64,
    concurrent_attempts: f64,
    concurrent_accepted: f64,
    concurrent_conflicts: f64,
    metadata_object_growth: f64,
}

impl RoundMetrics {
    fn from_execution(execution: &RoundExecution) -> Result<Self, AggregationError> {
        if !execution.passed() {
            return Err(AggregationError::FailedRoundInPassingAggregate);
        }
        let sequential = phase(&execution.sequential)?;
        let concurrent = phase(&execution.concurrent)?;
        let growth = growth(&execution.metadata_growth)?;
        Ok(Self {
            sequential_latency_p50_ms: quantile(sequential, "p50")?,
            sequential_latency_p95_ms: quantile(sequential, "p95")?,
            sequential_latency_p99_ms: quantile(sequential, "p99")?,
            sequential_accepted_throughput_per_second: sequential.accepted_throughput_per_second,
            concurrent_latency_p50_ms: quantile(concurrent, "p50")?,
            concurrent_latency_p95_ms: quantile(concurrent, "p95")?,
            concurrent_latency_p99_ms: quantile(concurrent, "p99")?,
            concurrent_attempted_throughput_per_second: concurrent.attempted_throughput_per_second,
            concurrent_accepted_throughput_per_second: concurrent.accepted_throughput_per_second,
            concurrent_conflict_rate: concurrent.conflict_rate,
            concurrent_error_rate: concurrent.error_rate,
            concurrent_attempts: concurrent.counts.attempts as f64,
            concurrent_accepted: concurrent.counts.accepted as f64,
            concurrent_conflicts: concurrent.counts.conflicts as f64,
            metadata_object_growth: growth
                .observed_growth
                .ok_or(AggregationError::MissingMeasurement)?
                as f64,
        })
    }
}

impl ContentionMeasurements {
    fn from_rounds(rounds: &[RoundMetrics]) -> Result<Self, AggregationError> {
        Ok(Self {
            sequential_latency_p50_ms: metric(rounds, |round| round.sequential_latency_p50_ms)?,
            sequential_latency_p95_ms: metric(rounds, |round| round.sequential_latency_p95_ms)?,
            sequential_latency_p99_ms: metric(rounds, |round| round.sequential_latency_p99_ms)?,
            sequential_accepted_throughput_per_second: metric(rounds, |round| {
                round.sequential_accepted_throughput_per_second
            })?,
            concurrent_latency_p50_ms: metric(rounds, |round| round.concurrent_latency_p50_ms)?,
            concurrent_latency_p95_ms: metric(rounds, |round| round.concurrent_latency_p95_ms)?,
            concurrent_latency_p99_ms: metric(rounds, |round| round.concurrent_latency_p99_ms)?,
            concurrent_attempted_throughput_per_second: metric(rounds, |round| {
                round.concurrent_attempted_throughput_per_second
            })?,
            concurrent_accepted_throughput_per_second: metric(rounds, |round| {
                round.concurrent_accepted_throughput_per_second
            })?,
            concurrent_conflict_rate: metric(rounds, |round| round.concurrent_conflict_rate)?,
            concurrent_error_rate: metric(rounds, |round| round.concurrent_error_rate)?,
            concurrent_attempts: metric(rounds, |round| round.concurrent_attempts)?,
            concurrent_accepted: metric(rounds, |round| round.concurrent_accepted)?,
            concurrent_conflicts: metric(rounds, |round| round.concurrent_conflicts)?,
            metadata_object_growth: metric(rounds, |round| round.metadata_object_growth)?,
        })
    }
}

fn phase(evidence: &OperationEvidence<PhaseEvidence>) -> Result<&PhaseEvidence, AggregationError> {
    match evidence {
        OperationEvidence::Succeeded { output } => Ok(output),
        OperationEvidence::Failed { .. } | OperationEvidence::NotAttempted { .. } => {
            Err(AggregationError::MissingMeasurement)
        }
    }
}

fn growth(
    evidence: &OperationEvidence<MetadataGrowthEvidence>,
) -> Result<&MetadataGrowthEvidence, AggregationError> {
    match evidence {
        OperationEvidence::Succeeded { output } => Ok(output),
        OperationEvidence::Failed { .. } | OperationEvidence::NotAttempted { .. } => {
            Err(AggregationError::MissingMeasurement)
        }
    }
}

fn quantile(phase: &PhaseEvidence, name: &str) -> Result<f64, AggregationError> {
    phase
        .latency_ms
        .all
        .as_ref()
        .and_then(|distribution| distribution.quantiles.get(name))
        .copied()
        .ok_or(AggregationError::MissingMeasurement)
}

fn metric(
    rounds: &[RoundMetrics],
    select: impl Fn(&RoundMetrics) -> f64,
) -> Result<MedianRange, AggregationError> {
    median_with_range(&rounds.iter().map(select).collect::<Vec<_>>())
        .map_err(AggregationError::Statistics)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregationError {
    EmptySchedule,
    CountOverflow,
    ScheduleLength { expected: usize, actual: usize },
    ScheduleMismatch { index: usize },
    MissingExecution,
    FailedRoundInPassingAggregate,
    MissingMeasurement,
    InvalidAggregate,
    Statistics(StatisticsError),
}

impl Display for AggregationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptySchedule => formatter.write_str("cannot aggregate an empty schedule"),
            Self::CountOverflow => formatter.write_str("aggregate count overflowed"),
            Self::ScheduleLength { expected, actual } => write!(
                formatter,
                "round transcript count is {actual}, expected {expected}"
            ),
            Self::ScheduleMismatch { index } => {
                write!(
                    formatter,
                    "round transcript {index} does not match the schedule"
                )
            }
            Self::MissingExecution => {
                formatter.write_str("passing aggregate contains a negotiation failure")
            }
            Self::FailedRoundInPassingAggregate => {
                formatter.write_str("passing aggregate contains a failed round")
            }
            Self::MissingMeasurement => {
                formatter.write_str("passing round is missing required measurements")
            }
            Self::InvalidAggregate => formatter.write_str("aggregate classification is invalid"),
            Self::Statistics(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for AggregationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Statistics(error) => Some(error),
            _ => None,
        }
    }
}
