//! Cross-scenario publication index, known gaps, and bundle secret review.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{AssertionOutcome, Provenance, ResultOutcome};

use crate::{
    check_contention_result_bundle, check_historical_commit_bundle, check_phase1_result_bundle,
    load_bundle, ValidatedBundle,
};

const INDEX: &str = "results/v1/INDEX.md";
const KNOWN_GAPS: &str = "results/v1/KNOWN-GAPS.md";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationProfile {
    Smoke,
    Full,
}

pub fn write_publication(root: &Path, profile: PublicationProfile) -> Result<()> {
    let publication = render_publication(root, profile)?;
    fs::write(root.join(INDEX), publication.index).context("failed to write publication index")?;
    fs::write(root.join(KNOWN_GAPS), publication.known_gaps)
        .context("failed to write known-gaps report")?;
    check_publication(root, profile)
}

pub fn check_publication(root: &Path, profile: PublicationProfile) -> Result<()> {
    if profile == PublicationProfile::Full {
        check_historical_commit_bundle(root)?;
        check_contention_result_bundle(root)?;
        check_phase1_result_bundle(root)?;
    }
    let publication = render_publication(root, profile)?;
    check_bytes(root, INDEX, publication.index.as_bytes())?;
    check_bytes(root, KNOWN_GAPS, publication.known_gaps.as_bytes())?;
    Ok(())
}

struct Publication {
    index: String,
    known_gaps: String,
}

fn render_publication(root: &Path, _profile: PublicationProfile) -> Result<Publication> {
    let manifests = discover_manifests(&root.join("results/v1"))?;
    if manifests.is_empty() {
        bail!("publication contains no result bundle manifests");
    }
    let mut bundles = Vec::new();
    for manifest in manifests {
        let bundle = load_bundle(&manifest)?;
        scan_bundle(&bundle)?;
        bundles.push(bundle);
    }

    let mut index = String::from(
        "# Catalog Bench Published Bundles\n\nThis page is generated from validated immutable manifests. Smoke evidence under `target/` is not included.\n\n| Bundle | Created | Provenance | Scenarios | Results | Pass | Non-pass |\n| --- | --- | --- | ---: | ---: | ---: | ---: |\n",
    );
    let mut gaps = String::from(
        "# Catalog Bench Known Gaps\n\nThis page is generated from validated non-pass results and assertion outcomes. Absence from this page is not a claim of untested support.\n\n",
    );
    let mut gap_count = 0usize;
    for bundle in &bundles {
        let pass = bundle
            .results()
            .iter()
            .filter(|result| matches!(result.result().outcome, ResultOutcome::Pass { .. }))
            .count();
        let relative = bundle
            .manifest_path()
            .strip_prefix(root.join("results/v1"))
            .context("bundle manifest escaped result publication root")?;
        index.push_str(&format!(
            "| [{}]({}) | {} | {} | {} | {} | {} | {} |\n",
            markdown(&bundle.manifest().title),
            relative.display(),
            bundle.manifest().created_at,
            provenance_name(&bundle.manifest().provenance),
            bundle.scenarios().len(),
            bundle.results().len(),
            pass,
            bundle.results().len() - pass,
        ));
        for result in bundle.results() {
            let record = result.result();
            let non_pass = !matches!(record.outcome, ResultOutcome::Pass { .. });
            let assertion_gaps = record
                .assertions
                .iter()
                .filter(|assertion| !matches!(assertion.outcome, AssertionOutcome::Pass))
                .collect::<Vec<_>>();
            if !non_pass && assertion_gaps.is_empty() {
                continue;
            }
            gap_count += 1;
            gaps.push_str(&format!(
                "## {} — {}\n\n- Bundle: `{}`\n- Scenario: `{}` v{}\n- Outcome: `{}`\n",
                markdown(&record.catalog.name),
                markdown(&record.catalog.version),
                bundle.manifest().id,
                record.scenario.id,
                record.scenario.version,
                outcome_name(&record.outcome),
            ));
            if assertion_gaps.is_empty() {
                gaps.push_str("- Assertion gaps: none (the result-level outcome is non-pass).\n\n");
            } else {
                gaps.push_str("- Assertion gaps:\n");
                for assertion in assertion_gaps {
                    let detail = match &assertion.outcome {
                        AssertionOutcome::Pass => unreachable!(),
                        AssertionOutcome::Fail { explanation } => explanation,
                        AssertionOutcome::NotEvaluated { reason } => reason,
                    };
                    gaps.push_str(&format!(
                        "  - `{}` ({}): {}\n",
                        assertion.assertion,
                        if assertion.required {
                            "required"
                        } else {
                            "optional"
                        },
                        markdown(detail),
                    ));
                }
                gaps.push('\n');
            }
        }
    }
    index.push_str("\nRegenerate and verify with `./publish-results.sh smoke`; use `./publish-results.sh full` to recompute every source-backed checked-in bundle first.\n");
    if gap_count == 0 {
        gaps.push_str("No checked-in result currently has a non-pass outcome or assertion.\n");
    }
    Ok(Publication {
        index,
        known_gaps: gaps,
    })
}

fn discover_manifests(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut manifests = Vec::new();
    for entry in fs::read_dir(directory)
        .with_context(|| format!("failed to read {}", directory.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            let manifest = path.join("manifest.json");
            if manifest.is_file() {
                manifests.push(manifest);
            }
        }
    }
    manifests.sort();
    Ok(manifests)
}

fn scan_bundle(bundle: &ValidatedBundle) -> Result<()> {
    let mut paths = BTreeSet::new();
    paths.insert(bundle.manifest_path().to_path_buf());
    paths.insert(bundle.profile_path().to_path_buf());
    for scenario in bundle.scenarios() {
        paths.insert(scenario.path().to_path_buf());
    }
    for result in bundle.results() {
        paths.insert(result.path().to_path_buf());
        for evidence in &result.result().evidence {
            paths.insert(resolve_artifact(
                bundle.manifest_path(),
                &evidence.artifact.location,
            )?);
        }
        for artifact in &result.result().artifacts {
            paths.insert(resolve_artifact(
                bundle.manifest_path(),
                &artifact.location,
            )?);
        }
    }
    for artifact in &bundle.manifest().source_evidence {
        paths.insert(resolve_artifact(
            bundle.manifest_path(),
            &artifact.location,
        )?);
    }
    for path in paths {
        scan_file(&path)?;
    }
    Ok(())
}

fn resolve_artifact(manifest: &Path, location: &str) -> Result<PathBuf> {
    if location.contains("://") || location.starts_with('/') {
        bail!("secret scan refuses non-local artifact `{location}`");
    }
    Ok(manifest
        .parent()
        .context("manifest has no parent")?
        .join(location))
}

fn scan_file(path: &Path) -> Result<()> {
    let bytes =
        fs::read(path).with_context(|| format!("failed to secret-scan {}", path.display()))?;
    let text = String::from_utf8_lossy(&bytes);
    let lower = text.to_ascii_lowercase();
    for forbidden in [
        "authorization: bearer ",
        "-----begin private key-----",
        "-----begin rsa private key-----",
        "minioadmin",
    ] {
        if lower.contains(forbidden) {
            bail!(
                "bundle secret scan rejected {}: matched `{forbidden}`",
                path.display()
            );
        }
    }
    if contains_aws_access_key(text.as_bytes()) {
        bail!(
            "bundle secret scan rejected {}: AWS access-key shape",
            path.display()
        );
    }
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
        scan_json(&value, path)?;
    }
    Ok(())
}

fn scan_json(value: &serde_json::Value, path: &Path) -> Result<()> {
    match value {
        serde_json::Value::Object(object) => {
            for (key, value) in object {
                let normalized = key.to_ascii_lowercase().replace('-', "_");
                if matches!(
                    normalized.as_str(),
                    "password"
                        | "secret"
                        | "secret_key"
                        | "access_key"
                        | "authorization"
                        | "bearer_token"
                        | "client_secret"
                ) && !matches!(value.as_str(), Some("<redacted>" | "[REDACTED]"))
                {
                    bail!(
                        "bundle secret scan rejected {}: non-redacted `{key}` field",
                        path.display()
                    );
                }
                scan_json(value, path)?;
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                scan_json(value, path)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn contains_aws_access_key(bytes: &[u8]) -> bool {
    bytes.windows(20).any(|window| {
        window.starts_with(b"AKIA")
            && window[4..]
                .iter()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    })
}

fn outcome_name(outcome: &ResultOutcome) -> &'static str {
    match outcome {
        ResultOutcome::Pass { .. } => "pass",
        ResultOutcome::Fail { .. } => "fail",
        ResultOutcome::Unsupported { .. } => "unsupported",
        ResultOutcome::NotTested { .. } => "not-tested",
    }
}

fn provenance_name(provenance: &Provenance) -> &'static str {
    match provenance {
        Provenance::LiveRun { .. } => "live-run",
        Provenance::HistoricalImport { .. } => "historical-import",
        Provenance::Fixture { .. } => "fixture",
    }
}

fn markdown(value: &str) -> String {
    value.replace('|', "\\|").replace(['\r', '\n'], " ")
}

fn check_bytes(root: &Path, relative: &str, expected: &[u8]) -> Result<()> {
    let path = root.join(relative);
    let actual = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
    if actual != expected {
        bail!(
            "{} differs from generated publication output",
            path.display()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_scan_accepts_explicit_redaction() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("evidence.json");
        fs::write(&path, br#"{"client_secret":"<redacted>"}"#).unwrap();
        scan_file(&path).unwrap();
    }

    #[test]
    fn secret_scan_rejects_structured_secret_values() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("evidence.json");
        fs::write(&path, br#"{"client_secret":"do-not-publish"}"#).unwrap();
        let error = scan_file(&path).unwrap_err().to_string();
        assert!(error.contains("non-redacted `client_secret` field"));
    }

    #[test]
    fn secret_scan_rejects_credential_shapes_in_non_json_evidence() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("evidence.log");
        fs::write(&path, b"Authorization: Bearer should-not-publish\n").unwrap();
        let error = scan_file(&path).unwrap_err().to_string();
        assert!(error.contains("authorization: bearer"));
    }
}
