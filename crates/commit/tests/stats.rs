use std::time::Duration;

use catalog_bench_commit::stats::{
    latency_distribution, median_with_range, nonnegative_distribution, per_second, StatisticsError,
};

#[test]
fn latency_distribution_is_complete_finite_and_monotonic() {
    let samples = [1_u64, 2, 3, 4, 5].map(Duration::from_millis);
    let distribution = latency_distribution(&samples).unwrap();

    assert_eq!(distribution.samples, 5);
    assert_eq!(distribution.minimum, 1.0);
    assert_eq!(distribution.maximum, 5.0);
    assert_eq!(distribution.mean, Some(3.0));
    assert!((distribution.standard_deviation.unwrap() - 2_f64.sqrt()).abs() < 1e-12);
    assert_eq!(distribution.quantiles["p50"], 3.0);
    assert_eq!(distribution.quantiles["p95"], 5.0);
    assert_eq!(distribution.quantiles["p99"], 5.0);
    assert!(distribution.quantiles["p50"] <= distribution.quantiles["p95"]);
    assert!(distribution.quantiles["p95"] <= distribution.quantiles["p99"]);
    assert!(distribution.quantiles["p99"] <= distribution.maximum);
}

#[test]
fn scalar_aggregation_reports_median_and_full_range() {
    let odd = median_with_range(&[9.0, 1.0, 5.0, 3.0, 7.0]).unwrap();
    assert_eq!(
        (odd.samples, odd.minimum, odd.median, odd.maximum),
        (5, 1.0, 5.0, 9.0)
    );

    let even = median_with_range(&[4.0, 1.0, 3.0, 2.0]).unwrap();
    assert_eq!(even.median, 2.5);
}

#[test]
fn invalid_statistics_are_rejected_instead_of_fabricated() {
    assert_eq!(
        nonnegative_distribution(&[]).unwrap_err(),
        StatisticsError::EmptySamples
    );
    assert_eq!(
        nonnegative_distribution(&[1.0, f64::NAN]).unwrap_err(),
        StatisticsError::NonFiniteSample { index: 1 }
    );
    assert_eq!(
        nonnegative_distribution(&[-1.0]).unwrap_err(),
        StatisticsError::NegativeSample { index: 0 }
    );
    assert_eq!(
        per_second(1, Duration::ZERO).unwrap_err(),
        StatisticsError::ZeroElapsed
    );
    assert_eq!(per_second(25, Duration::from_millis(500)).unwrap(), 50.0);
}
