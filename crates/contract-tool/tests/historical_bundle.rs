use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use catalog_bench_common::contract::ResultOutcome;
use catalog_bench_contract::{check_historical_commit_bundle, load_bundle, render_commit_matrix};
use serde_json::json;
use sha2::{Digest as _, Sha256};

const MANIFEST: &str = "results/v1/2026-08-08/manifest.json";
const MATRIX: &str = "results/v1/2026-08-08/MATRIX.md";

#[test]
fn historical_import_is_reproducible_and_fully_linked() -> Result<()> {
    let fixture = materialize_bundle_inputs()?;
    let manifest = check_historical_commit_bundle(fixture.path())?;
    let bundle = load_bundle(&manifest)?;

    assert_eq!(bundle.scenarios().len(), 1);
    assert_eq!(bundle.results().len(), 4);
    assert_eq!(
        bundle
            .results()
            .iter()
            .filter(|record| matches!(&record.result().outcome, ResultOutcome::Pass { .. }))
            .count(),
        3
    );
    assert_eq!(
        bundle
            .results()
            .iter()
            .filter(|record| matches!(&record.result().outcome, ResultOutcome::Fail { .. }))
            .count(),
        1
    );
    Ok(())
}

#[test]
fn checked_in_matrix_is_derived_from_results_and_ranks_only_passes() -> Result<()> {
    let fixture = materialize_bundle_inputs()?;
    let bundle = load_bundle(&fixture.path().join(MANIFEST))?;
    let rendered = render_commit_matrix(&bundle)?;
    let checked_in = fs::read_to_string(fixture.path().join(MATRIX))?;

    assert_eq!(checked_in, rendered);
    assert!(rendered.contains("| 1 | LakeCat 3cca8d1c | `pass`"));
    assert!(rendered.contains("| 2 | Apache Polaris 1.5.0 | `pass`"));
    assert!(rendered.contains("| 3 | Apache Gravitino 1.1.0 | `pass`"));
    assert!(rendered.contains("| — | Apache Nessie 0.108.4 | `fail`"));
    assert!(!rendered.contains("| DQ |"));
    assert!(!rendered.contains("| Err |"));
    Ok(())
}

#[test]
fn bundle_validation_rejects_tampered_result_bytes() -> Result<()> {
    let temporary = materialize_bundle_inputs()?;

    let result_path = temporary.path().join("results/v1/2026-08-08/lakecat.json");
    let mut bytes = fs::read(&result_path)?;
    bytes.push(b' ');
    fs::write(&result_path, bytes)?;

    let error = load_bundle(&temporary.path().join("results/v1/2026-08-08/manifest.json"))
        .expect_err("tampered result must fail immutable artifact validation");
    let message = format!("{error:#}");
    assert!(message.contains("has") && message.contains("bytes, expected"));
    Ok(())
}

#[test]
fn matrix_represents_unsupported_results_without_fabricated_measurements() -> Result<()> {
    let temporary = materialize_bundle_inputs()?;
    replace_nessie_with_unsupported_result(temporary.path())?;

    let bundle = load_bundle(&temporary.path().join(MANIFEST))?;
    let rendered = render_commit_matrix(&bundle)?;
    assert!(rendered.contains("| — | Apache Nessie 0.108.4 | `unsupported` | — | — | —"));
    assert!(rendered.contains(
        "Unsupported capability `iceberg-rest.table.commit.set-properties`: fixture limitation"
    ));
    Ok(())
}

fn materialize_bundle_inputs() -> Result<tempfile::TempDir> {
    let temporary = tempfile::tempdir()?;
    for (relative, bytes) in fixture_files() {
        let destination = temporary.path().join(relative);
        fs::create_dir_all(
            destination
                .parent()
                .with_context(|| format!("{relative} has no parent"))?,
        )?;
        fs::write(destination, bytes)?;
    }
    Ok(temporary)
}

fn fixture_files() -> [(&'static str, &'static [u8]); 11] {
    [
        (
            "profiles/v1/reproduction-2026-08-08.json",
            include_bytes!("../../../profiles/v1/reproduction-2026-08-08.json"),
        ),
        (
            "scenarios/v1/iceberg-rest.commit.same-table-contention.json",
            include_bytes!("../../../scenarios/v1/iceberg-rest.commit.same-table-contention.json"),
        ),
        (
            "results/commit-2026-08-08-summary.tsv",
            include_bytes!("../../../results/commit-2026-08-08-summary.tsv"),
        ),
        (
            "results/commit-2026-08-08-runs.tsv",
            include_bytes!("../../../results/commit-2026-08-08-runs.tsv"),
        ),
        (
            "results/commit-2026-08-08-object-audit.tsv",
            include_bytes!("../../../results/commit-2026-08-08-object-audit.tsv"),
        ),
        (
            "results/v1/2026-08-08/lakecat.json",
            include_bytes!("../../../results/v1/2026-08-08/lakecat.json"),
        ),
        (
            "results/v1/2026-08-08/polaris.json",
            include_bytes!("../../../results/v1/2026-08-08/polaris.json"),
        ),
        (
            "results/v1/2026-08-08/gravitino.json",
            include_bytes!("../../../results/v1/2026-08-08/gravitino.json"),
        ),
        (
            "results/v1/2026-08-08/nessie.json",
            include_bytes!("../../../results/v1/2026-08-08/nessie.json"),
        ),
        (
            MANIFEST,
            include_bytes!("../../../results/v1/2026-08-08/manifest.json"),
        ),
        (
            MATRIX,
            include_bytes!("../../../results/v1/2026-08-08/MATRIX.md"),
        ),
    ]
}

fn replace_nessie_with_unsupported_result(root: &Path) -> Result<()> {
    let result_path = root.join("results/v1/2026-08-08/nessie.json");
    let mut result: serde_json::Value = serde_json::from_slice(&fs::read(&result_path)?)?;
    result["outcome"] = json!({
        "status": "unsupported",
        "limitation": {
            "capability": "iceberg-rest.table.commit.set-properties",
            "explanation": "fixture limitation"
        }
    });
    result["assertions"] = json!([]);
    result["measurements"] = json!([]);
    let mut result_bytes = serde_json::to_vec_pretty(&result)?;
    result_bytes.push(b'\n');
    fs::write(&result_path, &result_bytes)?;

    let manifest_path = root.join(MANIFEST);
    let mut manifest: serde_json::Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    let artifact = manifest["results"]
        .as_array_mut()
        .context("manifest results must be an array")?
        .iter_mut()
        .find(|artifact| artifact["location"] == "nessie.json")
        .context("manifest must reference nessie.json")?;
    artifact["bytes"] = json!(result_bytes.len());
    artifact["digest"]["value"] = json!(sha256_hex(&result_bytes));
    let mut manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    manifest_bytes.push(b'\n');
    fs::write(manifest_path, manifest_bytes)?;
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
