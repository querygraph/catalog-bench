use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use catalog_bench_common::contract::Distribution;
use serde::{Deserialize, Serialize};

const QUANTILES: [(&str, f64); 3] = [("p50", 0.50), ("p95", 0.95), ("p99", 0.99)];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatisticsError {
    EmptySamples,
    NonFiniteSample { index: usize },
    NegativeSample { index: usize },
    ZeroElapsed,
}

impl Display for StatisticsError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptySamples => formatter.write_str("statistics require at least one sample"),
            Self::NonFiniteSample { index } => {
                write!(formatter, "sample {index} is not finite")
            }
            Self::NegativeSample { index } => {
                write!(formatter, "sample {index} is negative")
            }
            Self::ZeroElapsed => formatter.write_str("throughput requires nonzero elapsed time"),
        }
    }
}

impl Error for StatisticsError {}

/// Median and full observed range for repeated scalar measurements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MedianRange {
    pub samples: u64,
    pub minimum: f64,
    pub median: f64,
    pub maximum: f64,
}

/// Summarize nonempty latency samples in milliseconds.
pub fn latency_distribution(samples: &[Duration]) -> Result<Distribution, StatisticsError> {
    let milliseconds = samples
        .iter()
        .map(|sample| sample.as_secs_f64() * 1_000.0)
        .collect::<Vec<_>>();
    nonnegative_distribution(&milliseconds)
}

/// Summarize nonempty, finite, nonnegative values with a deterministic
/// nearest-rank quantile estimator.
pub fn nonnegative_distribution(samples: &[f64]) -> Result<Distribution, StatisticsError> {
    let sorted = sorted_nonnegative(samples)?;
    let (mean, standard_deviation) = population_moments(&sorted);
    let quantiles = QUANTILES
        .into_iter()
        .map(|(name, probability)| (name.to_owned(), nearest_rank(&sorted, probability)))
        .collect::<BTreeMap<_, _>>();

    Ok(Distribution {
        samples: sorted.len() as u64,
        minimum: sorted[0],
        maximum: sorted[sorted.len() - 1],
        mean: Some(mean),
        standard_deviation: Some(standard_deviation),
        quantiles,
    })
}

/// Summarize repeated nonnegative scalars using a conventional midpoint for an
/// even-sized sample. The benchmark's five measured rounds use the exact middle
/// observation.
pub fn median_with_range(samples: &[f64]) -> Result<MedianRange, StatisticsError> {
    let sorted = sorted_nonnegative(samples)?;
    let middle = sorted.len() / 2;
    let median = if sorted.len() % 2 == 0 {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    };
    Ok(MedianRange {
        samples: sorted.len() as u64,
        minimum: sorted[0],
        median,
        maximum: sorted[sorted.len() - 1],
    })
}

/// Operations per second over an observed wall-clock interval.
pub fn per_second(count: u64, elapsed: Duration) -> Result<f64, StatisticsError> {
    if elapsed.is_zero() {
        return Err(StatisticsError::ZeroElapsed);
    }
    Ok(count as f64 / elapsed.as_secs_f64())
}

fn sorted_nonnegative(samples: &[f64]) -> Result<Vec<f64>, StatisticsError> {
    if samples.is_empty() {
        return Err(StatisticsError::EmptySamples);
    }
    let mut sorted = Vec::with_capacity(samples.len());
    for (index, sample) in samples.iter().copied().enumerate() {
        if !sample.is_finite() {
            return Err(StatisticsError::NonFiniteSample { index });
        }
        if sample < 0.0 {
            return Err(StatisticsError::NegativeSample { index });
        }
        sorted.push(sample);
    }
    sorted.sort_by(f64::total_cmp);
    Ok(sorted)
}

fn nearest_rank(sorted: &[f64], probability: f64) -> f64 {
    let rank = (probability * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn population_moments(samples: &[f64]) -> (f64, f64) {
    let mut count = 0_u64;
    let mut mean = 0.0;
    let mut squared_deviation = 0.0;
    for sample in samples {
        count += 1;
        let delta = sample - mean;
        mean += delta / count as f64;
        squared_deviation += delta * (sample - mean);
    }
    let variance = (squared_deviation / count as f64).max(0.0);
    (mean, variance.sqrt())
}
