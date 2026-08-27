use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use catalog_bench_common::contract::{AssertionOutcome, MetricValue, ResultOutcome};
use catalog_bench_contract::{
    check_contention_result_bundle, load_bundle, render_commit_matrix,
    write_contention_result_bundle, ValidatedBundle,
};
use sha2::{Digest as _, Sha256};

const MANIFEST: &str = "results/v1/2026-08-27/manifest.json";
const MATRIX: &str = "results/v1/2026-08-27/MATRIX.md";

#[test]
fn checked_in_production_bundle_recomputes_and_validates() -> Result<()> {
    let fixture = materialize_fixture(true)?;
    let manifest = check_contention_result_bundle(fixture.path())?;
    let bundle = load_bundle(&manifest)?;

    assert_eq!(bundle.scenarios().len(), 1);
    assert_eq!(bundle.scenarios()[0].scenario().version, 2);
    assert_eq!(bundle.results().len(), 5);
    assert_eq!(bundle.manifest().source_evidence.len(), 2);

    let mut ranked = bundle
        .results()
        .iter()
        .filter(|result| matches!(result.result().outcome, ResultOutcome::Pass { .. }))
        .map(|result| {
            Ok((
                result.result().catalog.name.as_str(),
                distribution_median(result.result(), "concurrent", "successful-throughput")?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    ranked.sort_by(|left, right| right.1.total_cmp(&left.1));
    assert_eq!(
        ranked,
        vec![
            ("LakeCat", 147.535_829_974_679_33),
            ("Apache Polaris", 58.109_807_707_832_3),
            ("Apache Gravitino", 56.822_860_899_828_235),
        ]
    );
    Ok(())
}

#[test]
fn failed_catalogs_keep_complete_diagnostics_but_cannot_rank() -> Result<()> {
    let fixture = materialize_fixture(true)?;
    let bundle = load_bundle(&fixture.path().join(MANIFEST))?;

    for (catalog, measured_errors) in [("lakekeeper", 47), ("nessie", 88)] {
        let result = result(&bundle, catalog)?;
        assert!(matches!(result.outcome, ResultOutcome::Fail { .. }));
        assert_eq!(result.assertions.len(), 14);
        let failed = result
            .assertions
            .iter()
            .filter(|evaluation| matches!(evaluation.outcome, AssertionOutcome::Fail { .. }))
            .map(|evaluation| evaluation.assertion.as_str())
            .collect::<Vec<_>>();
        assert_eq!(failed, vec!["zero-request-errors"]);
        assert_eq!(
            counter(result, "concurrent", "request-errors")?,
            measured_errors
        );
        assert_eq!(counter(result, "concurrent", "valid-rounds")?, 0);
        assert_eq!(result.measurements.len(), 2);
        let ResultOutcome::Fail { failure } = &result.outcome else {
            unreachable!("checked above");
        };
        assert_eq!(failure.evidence.len(), 2);
    }

    let lakecat = result(&bundle, "lakecat")?;
    let nessie = result(&bundle, "nessie")?;
    assert!(
        distribution_median(nessie, "concurrent", "successful-throughput")?
            > distribution_median(lakecat, "concurrent", "successful-throughput")?
    );
    let matrix = render_commit_matrix(&bundle)?;
    assert!(matrix.contains("| 1 | LakeCat"));
    assert!(matrix.contains("| — | Apache Nessie"));
    Ok(())
}

#[test]
fn checked_in_matrix_is_generated_with_reviewed_failure_attribution() -> Result<()> {
    let fixture = materialize_fixture(true)?;
    let bundle = load_bundle(&fixture.path().join(MANIFEST))?;
    let rendered = render_commit_matrix(&bundle)?;
    let checked_in = fs::read_to_string(fixture.path().join(MATRIX))?;

    assert_eq!(rendered, checked_in);
    assert!(rendered.contains("PostgreSQL deadlock-backed HTTP 503"));
    assert!(rendered.contains("ContextNotActiveException"));
    assert!(rendered.contains("results/v1/2026-08-27/manifest.json"));
    Ok(())
}

#[test]
fn matrix_uses_sequential_latency_then_catalog_id_as_tie_breakers() -> Result<()> {
    let fixture = materialize_fixture(true)?;
    let lakecat_path = fixture.path().join("results/v1/2026-08-27/lakecat.json");
    let polaris_path = fixture.path().join("results/v1/2026-08-27/polaris.json");
    let mut lakecat: serde_json::Value = serde_json::from_slice(&fs::read(&lakecat_path)?)?;
    let mut polaris: serde_json::Value = serde_json::from_slice(&fs::read(&polaris_path)?)?;

    let lakecat_throughput = json_distribution(&lakecat, "concurrent", "successful-throughput")?;
    set_json_distribution(
        &mut polaris,
        "concurrent",
        "successful-throughput",
        lakecat_throughput,
    )?;
    let lakecat_latency = json_distribution(&lakecat, "sequential", "p50-latency")?;
    let polaris_latency = json_distribution(&polaris, "sequential", "p50-latency")?;
    set_json_distribution(&mut lakecat, "sequential", "p50-latency", polaris_latency)?;
    set_json_distribution(
        &mut polaris,
        "sequential",
        "p50-latency",
        lakecat_latency.clone(),
    )?;
    write_json(&lakecat_path, &lakecat)?;
    write_json(&polaris_path, &polaris)?;
    refresh_result_artifacts(fixture.path())?;

    let bundle = load_bundle(&fixture.path().join(MANIFEST))?;
    let matrix = render_commit_matrix(&bundle)?;
    assert!(matrix.contains("| 1 | Apache Polaris"));
    assert!(matrix.contains("| 2 | LakeCat"));

    set_json_distribution(&mut lakecat, "sequential", "p50-latency", lakecat_latency)?;
    write_json(&lakecat_path, &lakecat)?;
    refresh_result_artifacts(fixture.path())?;

    let bundle = load_bundle(&fixture.path().join(MANIFEST))?;
    let matrix = render_commit_matrix(&bundle)?;
    assert!(matrix.contains("| 1 | LakeCat"));
    assert!(matrix.contains("| 2 | Apache Polaris"));
    Ok(())
}

#[test]
fn writer_reconstructs_the_bundle_from_only_reviewed_inputs() -> Result<()> {
    let fixture = materialize_fixture(false)?;
    let manifest = write_contention_result_bundle(fixture.path())?;
    let bundle = load_bundle(&manifest)?;

    assert_eq!(bundle.results().len(), 5);
    assert!(fixture.path().join(MATRIX).is_file());
    check_contention_result_bundle(fixture.path())?;
    Ok(())
}

#[test]
fn import_rejects_transcript_tampering_before_using_measurements() -> Result<()> {
    let fixture = materialize_fixture(true)?;
    let path = fixture
        .path()
        .join("results/contention-2026-08-27-transcript.json");
    let mut transcript: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
    transcript["ranking"]["entries"][0]["disposition"]["score"]["median"] =
        serde_json::json!(999_999.0);
    fs::write(path, serde_json::to_vec_pretty(&transcript)?)?;

    let error = check_contention_result_bundle(fixture.path()).unwrap_err();
    assert!(format!("{error:#}").contains("hash mismatch"));
    Ok(())
}

#[test]
fn import_rejects_unreviewed_failure_claim_drift() -> Result<()> {
    let fixture = materialize_fixture(true)?;
    let path = fixture
        .path()
        .join("results/contention-2026-08-27-review.json");
    let mut review: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
    review["failures"][0]["detail"] = serde_json::json!("unsubstantiated replacement");
    fs::write(path, serde_json::to_vec_pretty(&review)?)?;

    let error = check_contention_result_bundle(fixture.path()).unwrap_err();
    assert!(format!("{error:#}").contains("hash mismatch"));
    Ok(())
}

#[test]
fn bundle_validation_rejects_tampered_generated_result() -> Result<()> {
    let fixture = materialize_fixture(true)?;
    let path = fixture.path().join("results/v1/2026-08-27/lakecat.json");
    let mut bytes = fs::read(&path)?;
    bytes.push(b' ');
    fs::write(path, bytes)?;

    let error = load_bundle(&fixture.path().join(MANIFEST)).unwrap_err();
    assert!(format!("{error:#}").contains("expected"));
    Ok(())
}

fn result<'a>(
    bundle: &'a ValidatedBundle,
    catalog: &str,
) -> Result<&'a catalog_bench_common::contract::ResultRecord> {
    bundle
        .results()
        .iter()
        .find(|result| result.result().catalog.profile_component.as_str() == catalog)
        .map(|result| result.result())
        .with_context(|| format!("missing catalog {catalog}"))
}

fn distribution_median(
    result: &catalog_bench_common::contract::ResultRecord,
    phase: &str,
    metric_name: &str,
) -> Result<f64> {
    let metric = metric(result, phase, metric_name)?;
    let MetricValue::Distribution { distribution } = &metric.value else {
        anyhow::bail!("metric is not a distribution: {phase}.{}", metric.name);
    };
    distribution
        .quantiles
        .get("p50")
        .copied()
        .context("distribution has no p50")
}

fn counter(
    result: &catalog_bench_common::contract::ResultRecord,
    phase: &str,
    metric_name: &str,
) -> Result<u64> {
    let metric = metric(result, phase, metric_name)?;
    let MetricValue::Counter { value } = metric.value else {
        anyhow::bail!("metric is not a counter: {phase}.{}", metric.name);
    };
    Ok(value)
}

fn metric<'a>(
    result: &'a catalog_bench_common::contract::ResultRecord,
    phase: &str,
    metric_name: &str,
) -> Result<&'a catalog_bench_common::contract::Metric> {
    result
        .measurements
        .iter()
        .find(|measurement| measurement.name == phase)
        .with_context(|| format!("missing phase {phase}"))?
        .metrics
        .iter()
        .find(|candidate| candidate.name == metric_name)
        .with_context(|| format!("missing metric {phase}.{metric_name}"))
}

fn materialize_fixture(include_outputs: bool) -> Result<tempfile::TempDir> {
    let temporary = tempfile::tempdir()?;
    let inputs = [
        (
            "profiles/v1/contention-2026-08-27.json",
            include_bytes!("../../../profiles/v1/contention-2026-08-27.json").as_slice(),
        ),
        (
            "scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json",
            include_bytes!(
                "../../../scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json"
            )
            .as_slice(),
        ),
        (
            "results/contention-2026-08-27-transcript.json",
            include_bytes!("../../../results/contention-2026-08-27-transcript.json").as_slice(),
        ),
        (
            "results/contention-2026-08-27-review.json",
            include_bytes!("../../../results/contention-2026-08-27-review.json").as_slice(),
        ),
    ];
    write_files(temporary.path(), &inputs)?;
    if include_outputs {
        let outputs = [
            (
                "results/v1/2026-08-27/lakecat.json",
                include_bytes!("../../../results/v1/2026-08-27/lakecat.json").as_slice(),
            ),
            (
                "results/v1/2026-08-27/polaris.json",
                include_bytes!("../../../results/v1/2026-08-27/polaris.json").as_slice(),
            ),
            (
                "results/v1/2026-08-27/gravitino.json",
                include_bytes!("../../../results/v1/2026-08-27/gravitino.json").as_slice(),
            ),
            (
                "results/v1/2026-08-27/lakekeeper.json",
                include_bytes!("../../../results/v1/2026-08-27/lakekeeper.json").as_slice(),
            ),
            (
                "results/v1/2026-08-27/nessie.json",
                include_bytes!("../../../results/v1/2026-08-27/nessie.json").as_slice(),
            ),
            (
                MANIFEST,
                include_bytes!("../../../results/v1/2026-08-27/manifest.json").as_slice(),
            ),
            (
                MATRIX,
                include_bytes!("../../../results/v1/2026-08-27/MATRIX.md").as_slice(),
            ),
        ];
        write_files(temporary.path(), &outputs)?;
    }
    Ok(temporary)
}

fn write_files(root: &Path, files: &[(&str, &[u8])]) -> Result<()> {
    for (relative, bytes) in files {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().context("fixture path has no parent")?)?;
        fs::write(path, bytes)?;
    }
    Ok(())
}

fn json_distribution(
    document: &serde_json::Value,
    phase: &str,
    metric_name: &str,
) -> Result<serde_json::Value> {
    document["measurements"]
        .as_array()
        .context("result measurements are not an array")?
        .iter()
        .find(|measurement| measurement["name"] == phase)
        .with_context(|| format!("missing JSON phase {phase}"))?["metrics"]
        .as_array()
        .context("phase metrics are not an array")?
        .iter()
        .find(|metric| metric["name"] == metric_name)
        .with_context(|| format!("missing JSON metric {phase}.{metric_name}"))?
        .pointer("/value/distribution")
        .cloned()
        .with_context(|| format!("missing JSON distribution {phase}.{metric_name}"))
}

fn set_json_distribution(
    document: &mut serde_json::Value,
    phase: &str,
    metric_name: &str,
    distribution: serde_json::Value,
) -> Result<()> {
    let metric = document["measurements"]
        .as_array_mut()
        .context("result measurements are not an array")?
        .iter_mut()
        .find(|measurement| measurement["name"] == phase)
        .with_context(|| format!("missing JSON phase {phase}"))?["metrics"]
        .as_array_mut()
        .context("phase metrics are not an array")?
        .iter_mut()
        .find(|metric| metric["name"] == metric_name)
        .with_context(|| format!("missing JSON metric {phase}.{metric_name}"))?;
    *metric
        .pointer_mut("/value/distribution")
        .with_context(|| format!("missing JSON distribution {phase}.{metric_name}"))? =
        distribution;
    Ok(())
}

fn refresh_result_artifacts(root: &Path) -> Result<()> {
    let path = root.join(MANIFEST);
    let mut manifest: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
    for artifact in manifest["results"]
        .as_array_mut()
        .context("manifest results are not an array")?
    {
        let location = artifact["location"]
            .as_str()
            .context("result artifact has no location")?;
        let bytes = fs::read(
            path.parent()
                .context("manifest has no parent")?
                .join(location),
        )?;
        artifact["bytes"] = serde_json::json!(bytes.len());
        artifact["digest"]["value"] = serde_json::json!(sha256_hex(&bytes));
    }
    write_json(&path, &manifest)
}

fn write_json(path: &Path, value: &serde_json::Value) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    fs::write(path, bytes)?;
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}
