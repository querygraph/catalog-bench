use std::time::Duration;

use catalog_bench_common::{percentile, throughput, BenchReport, BenchStatus};

#[test]
fn percentile_uses_nearest_rank() {
    let samples: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();

    assert!((percentile(&samples, 50.0) - 50.0).abs() < 1.5);
    assert!((percentile(&samples, 95.0) - 95.0).abs() < 1.5);
    assert_eq!(percentile(&[], 50.0), 0.0);
}

#[test]
fn throughput_handles_elapsed_time() {
    assert_eq!(throughput(100, Duration::from_secs(10)), 10.0);
    assert_eq!(throughput(5, Duration::ZERO), 0.0);
}

#[test]
fn bench_report_round_trips() {
    let report = BenchReport::scaffold("x", "todo");
    let decoded: BenchReport = serde_json::from_str(&report.to_json()).unwrap();

    assert_eq!(decoded.status, BenchStatus::Scaffold);
}
