use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{
    ArtifactReference, AssertionEvaluation, AssertionOutcome, Captured, Component,
    ContractDocument, ContractVersion, Digest, DigestAlgorithm, Distribution, EnvironmentManifest,
    Evidence, EvidenceKind, ExecutedComponent, Extensions, Failure, FailureCategory,
    ManifestDocumentKind, MeasuredPhase, Metric, MetricValue, Profile, ProfileReference,
    Provenance, RedactionStatement, ResultBundleManifest, ResultDocumentKind, ResultId,
    ResultOutcome, ResultRecord, RunIdentity, Scenario, ScenarioReference, Validate,
};
use serde::{Deserialize, Serialize};

use crate::sha256_hex;

const OUTPUT_DIRECTORY: &str = "results/v1/2026-08-08";
const SUMMARY_PATH: &str = "results/commit-2026-08-08-summary.tsv";
const RUNS_PATH: &str = "results/commit-2026-08-08-runs.tsv";
const AUDIT_PATH: &str = "results/commit-2026-08-08-object-audit.tsv";
const PROFILE_PATH: &str = "profiles/v1/reproduction-2026-08-08.json";
const SCENARIO_PATH: &str = "scenarios/v1/iceberg-rest.commit.same-table-contention.json";
const SUMMARY_SHA256: &str = "ce0730e6212c087d72fde2983830736e4989b29d3c361f1a00f32ea586b3bdd9";
const RUNS_SHA256: &str = "6aa5cd519aaa2e4c776be360394ea10d5be33ee130d8c7f3cd8b34eec2772819";
const AUDIT_SHA256: &str = "9cdfb8bbbfef079cd0c934c81308aef1e7bf71bf10dd1e488fba1fd7e494a8c3";
const CATALOGS: [&str; 4] = ["lakecat", "polaris", "gravitino", "nessie"];
const CONDITIONING_AND_MEASURED_ROUNDS: std::ops::RangeInclusive<u32> = 1..=6;
const WARMUP_AND_SEQUENTIAL_COMMITS: u64 = 1_050;

#[derive(Debug, Deserialize)]
struct SummaryRow {
    display_order: u32,
    rank: String,
    catalog: String,
    eligible: String,
    valid_rounds: u64,
    measured_rounds: u64,
    concurrent_median: f64,
    concurrent_min: f64,
    concurrent_max: f64,
    seq_median: f64,
    seq_min: f64,
    seq_max: f64,
    p50_median_ms: f64,
    p99_median_ms: f64,
    conflict_median_pct: f64,
    error_median_pct: f64,
    total_errors: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct RunRow {
    round: u32,
    catalog: String,
    namespace: String,
    valid: String,
    exit_status: i32,
    seq_per_s: f64,
    p50_ms: f64,
    p99_ms: f64,
    concurrent_per_s: f64,
    conflict_pct: f64,
    error_pct: f64,
    ok: u64,
    conflicts: u64,
    errors: u64,
    object_delta: u64,
    expected_min: u64,
}

#[derive(Debug, Deserialize)]
struct AuditRow {
    round: u32,
    catalog: String,
    objects_before: u64,
    objects_after: u64,
    delta: u64,
    expected_min: u64,
}

struct SourceEvidence {
    summary: ArtifactReference,
    runs: ArtifactReference,
    audit: ArtifactReference,
}

struct GeneratedBundle {
    directory: PathBuf,
    files: BTreeMap<String, Vec<u8>>,
}

/// Recompute and write the canonical historical result records and manifest.
pub fn write_historical_commit_bundle(repository_root: &Path) -> Result<PathBuf> {
    let generated = generate(repository_root)?;
    fs::create_dir_all(&generated.directory)
        .with_context(|| format!("failed to create {}", generated.directory.display()))?;
    for (name, bytes) in &generated.files {
        let path = generated.directory.join(name);
        fs::write(&path, bytes).with_context(|| format!("failed to write {}", path.display()))?;
    }
    Ok(generated.directory.join("manifest.json"))
}

/// Fail if checked-in historical records differ from a fresh import.
pub fn check_historical_commit_bundle(repository_root: &Path) -> Result<PathBuf> {
    let generated = generate(repository_root)?;
    for (name, expected) in &generated.files {
        let path = generated.directory.join(name);
        let actual = fs::read(&path)
            .with_context(|| format!("failed to read generated artifact {}", path.display()))?;
        if actual != *expected {
            bail!(
                "{} is stale; rerun the historical import writer",
                path.display()
            );
        }
    }
    Ok(generated.directory.join("manifest.json"))
}

fn generate(repository_root: &Path) -> Result<GeneratedBundle> {
    let summary_bytes = read_hashed(repository_root.join(SUMMARY_PATH), SUMMARY_SHA256)?;
    let runs_bytes = read_hashed(repository_root.join(RUNS_PATH), RUNS_SHA256)?;
    let audit_bytes = read_hashed(repository_root.join(AUDIT_PATH), AUDIT_SHA256)?;
    let profile_bytes = fs::read(repository_root.join(PROFILE_PATH))?;
    let scenario_bytes = fs::read(repository_root.join(SCENARIO_PATH))?;

    let profile = parse_profile(&profile_bytes)?;
    let scenario = parse_scenario(&scenario_bytes)?;
    let summaries = parse_tsv::<SummaryRow>(&summary_bytes)?;
    let runs = parse_tsv::<RunRow>(&runs_bytes)?;
    let audits = parse_tsv::<AuditRow>(&audit_bytes)?;
    verify_raw_evidence(&summaries, &runs, &audits)?;

    let source = SourceEvidence {
        summary: artifact(
            "../../commit-2026-08-08-summary.tsv",
            "text/tab-separated-values",
            &summary_bytes,
            "Published five-round aggregate summary.",
        ),
        runs: artifact(
            "../../commit-2026-08-08-runs.tsv",
            "text/tab-separated-values",
            &runs_bytes,
            "All 24 runs, including conditioning round 1.",
        ),
        audit: artifact(
            "../../commit-2026-08-08-object-audit.tsv",
            "text/tab-separated-values",
            &audit_bytes,
            "Per-run MinIO object-growth audit.",
        ),
    };
    let profile_artifact = artifact(
        "../../../profiles/v1/reproduction-2026-08-08.json",
        "application/json",
        &profile_bytes,
        "Runnable historical reproduction profile.",
    );
    let scenario_artifact = artifact(
        "../../../scenarios/v1/iceberg-rest.commit.same-table-contention.json",
        "application/json",
        &scenario_bytes,
        "Versioned neutral same-table contention scenario.",
    );

    let mut grouped = BTreeMap::<String, Vec<RunRow>>::new();
    for run in runs.iter().filter(|run| run.round >= 2) {
        grouped
            .entry(run.catalog.clone())
            .or_default()
            .push(run.clone());
    }
    let summary_by_catalog: BTreeMap<_, _> = summaries
        .iter()
        .map(|summary| (summary.catalog.as_str(), summary))
        .collect();
    let components: BTreeMap<_, _> = profile
        .components
        .iter()
        .map(|component| (component.id.as_str(), component))
        .collect();

    let mut files = BTreeMap::new();
    let mut result_artifacts = Vec::new();
    for catalog in CATALOGS {
        let catalog_runs = grouped
            .get(catalog)
            .with_context(|| format!("missing measured runs for {catalog}"))?;
        let summary = summary_by_catalog
            .get(catalog)
            .with_context(|| format!("missing summary for {catalog}"))?;
        let component = components
            .get(catalog)
            .with_context(|| format!("profile has no `{catalog}` component"))?;
        let result = build_result(
            catalog,
            component,
            catalog_runs,
            summary,
            &profile,
            &profile_artifact.digest,
            &scenario,
            &scenario_artifact.digest,
            &source,
        )?;
        let name = format!("{catalog}.json");
        let bytes = pretty_json(&result)?;
        result_artifacts.push(artifact(
            &name,
            "application/json",
            &bytes,
            &format!("Historical aggregate result for {}.", component.name),
        ));
        files.insert(name, bytes);
    }

    let manifest = ResultBundleManifest {
        contract_version: ContractVersion::V1,
        kind: ManifestDocumentKind::Manifest,
        id: "commit-2026-08-08".into(),
        title: "2026-08-08 same-table commit ranking".to_owned(),
        created_at: "2026-08-26T18:00:00-04:00".to_owned(),
        provenance: Provenance::HistoricalImport {
            source_date: "2026-08-08 (America/Los_Angeles)".to_owned(),
            imported_at: "2026-08-26T18:00:00-04:00".to_owned(),
            explanation: "Recomputed from the three preserved TSV artifacts. Docker timing was not rerun during import because the local Docker VM reported no space left on device; no images or volumes were deleted.".to_owned(),
        },
        profile: profile_artifact,
        scenarios: vec![scenario_artifact],
        results: result_artifacts,
        source_evidence: vec![source.summary, source.runs, source.audit],
        redaction: RedactionStatement {
            reviewed: true,
            policy: "The source TSVs and generated records contain numeric measurements, public component identities, and sanitized namespace names; no credentials, authorization headers, or secrets are present.".to_owned(),
            removed_fields: Vec::new(),
        },
        extensions: BTreeMap::from([(
            "querygraph/reproduction-status".to_owned(),
            serde_json::json!({
                "arithmetic": "recomputed",
                "artifact_hashes": "verified",
                "live_timing": "not rerun: Docker VM no space left on device"
            }),
        )]),
    };
    manifest.validate()?;
    files.insert("manifest.json".to_owned(), pretty_json(&manifest)?);

    Ok(GeneratedBundle {
        directory: repository_root.join(OUTPUT_DIRECTORY),
        files,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_result(
    catalog: &str,
    component: &Component,
    runs: &[RunRow],
    summary: &SummaryRow,
    profile: &Profile,
    profile_digest: &Digest,
    scenario: &Scenario,
    scenario_digest: &Digest,
    source: &SourceEvidence,
) -> Result<ResultRecord> {
    let errors = runs.iter().map(|run| run.errors).sum::<u64>();
    let valid_rounds = runs.iter().filter(|run| run.valid == "yes").count() as u64;
    let zero_errors = errors == 0;
    let evidence = vec![
        evidence("summary-tsv", EvidenceKind::CatalogState, &source.summary),
        evidence("runs-tsv", EvidenceKind::Log, &source.runs),
        evidence("object-audit-tsv", EvidenceKind::ObjectAudit, &source.audit),
    ];
    let assertions = vec![
        assertion("setup-succeeded", AssertionOutcome::Pass, &["runs-tsv"]),
        assertion(
            "all-requests-accounted",
            AssertionOutcome::Pass,
            &["runs-tsv"],
        ),
        assertion(
            "zero-request-errors",
            if zero_errors {
                AssertionOutcome::Pass
            } else {
                AssertionOutcome::Fail {
                    explanation: format!(
                        "{errors} non-conflict request errors occurred across all five measured rounds."
                    ),
                }
            },
            &["runs-tsv"],
        ),
        assertion(
            "metadata-persisted",
            AssertionOutcome::Pass,
            &["object-audit-tsv"],
        ),
    ];
    let outcome = if zero_errors && valid_rounds == runs.len() as u64 {
        ResultOutcome::Pass {
            summary: Some(
                "All five measured rounds passed strict request and object-growth validity checks."
                    .to_owned(),
            ),
        }
    } else {
        ResultOutcome::Fail {
            failure: Failure {
                category: FailureCategory::Assertion,
                summary: format!(
                    "The zero-request-errors assertion failed with {errors} errors and {valid_rounds}/{} valid measured rounds.",
                    runs.len()
                ),
                detail: "Every measured run exited nonzero after emitting its complete report. The raw throughput counts only accepted commits and remains diagnostic; it is not rank-eligible. Server-side HTTP 500 analysis is documented in docs/NESSIE-ERROR.md and is not silently relabeled as a conflict.".to_owned(),
                retryable: false,
                evidence: vec!["runs-tsv".into()],
            },
        }
    };

    let result = ResultRecord {
        contract_version: ContractVersion::V1,
        kind: ResultDocumentKind::Result,
        id: ResultId::new(format!("commit-2026-08-08-{catalog}")),
        scenario: ScenarioReference {
            id: scenario.id.clone(),
            version: scenario.version,
            digest: scenario_digest.clone(),
        },
        profile: ProfileReference {
            id: profile.id.clone(),
            digest: profile_digest.clone(),
        },
        catalog: ExecutedComponent {
            profile_component: component.id.clone(),
            name: component.name.clone(),
            version: component.version.clone(),
        },
        client: None,
        adapters: Vec::new(),
        run: RunIdentity::Aggregate {
            id: format!("commit-2026-08-08-{catalog}-rounds-2-6"),
            period: "2026-08-08 (America/Los_Angeles)".to_owned(),
            included_repetitions: vec![2, 3, 4, 5, 6],
            excluded_repetitions: vec![1],
            aggregation: "Median of rounds 2 through 6; minimum and maximum retained. Round 1 was conditioning.".to_owned(),
        },
        outcome,
        environment: EnvironmentManifest {
            operating_system: "Linux".to_owned(),
            architecture: "aarch64".to_owned(),
            cpu_model: Captured::Unknown {
                explanation: "The historical report recorded CPU count but not the model.".to_owned(),
            },
            logical_cpus: Captured::Exact { value: 10 },
            memory_bytes: Captured::Approximate {
                value: 8_375_186_227,
                explanation: "Converted from the rounded historical value 7.8 GiB; exact bytes were not retained.".to_owned(),
            },
            cpu_limit: None,
            memory_limit_bytes: None,
            network: "iceberg_lakehouse-net".to_owned(),
            container_runtime: Captured::Unknown {
                explanation: "The run used Docker on one Apple Silicon host, but the Docker version was not retained.".to_owned(),
            },
            runtime_flags: BTreeMap::from([
                ("conditioning_rounds".to_owned(), "1".to_owned()),
                ("concurrent_duration_seconds".to_owned(), "6".to_owned()),
                ("concurrent_writers".to_owned(), "8".to_owned()),
                ("measured_rounds".to_owned(), "5".to_owned()),
                ("sequential_commits".to_owned(), "1000".to_owned()),
                ("warmup_commits".to_owned(), "50".to_owned()),
            ]),
            extensions: Extensions::new(),
        },
        assertions,
        measurements: measurements(runs),
        evidence,
        artifacts: Vec::new(),
        extensions: BTreeMap::from([(
            "querygraph/legacy-summary".to_owned(),
            serde_json::json!({
                "eligible": &summary.eligible,
                "legacy_rank": &summary.rank,
                "source": SUMMARY_PATH
            }),
        )]),
    };
    result.validate()?;
    Ok(result)
}

fn measurements(runs: &[RunRow]) -> Vec<MeasuredPhase> {
    let sequential_throughput = values(runs, |run| run.seq_per_s);
    let sequential_elapsed = values(runs, |run| 1_000_000.0 / run.seq_per_s);
    let p50 = values(runs, |run| run.p50_ms);
    let p99 = values(runs, |run| run.p99_ms);
    let concurrent_throughput = values(runs, |run| run.concurrent_per_s);
    let concurrent_elapsed = values(runs, |run| run.ok as f64 * 1_000.0 / run.concurrent_per_s);
    let conflict_rate = values(runs, |run| run.conflict_pct);
    let error_rate = values(runs, |run| run.error_pct);
    let accepted = values(runs, |run| run.ok as f64);
    vec![
        MeasuredPhase {
            name: "sequential".to_owned(),
            elapsed_ms: median(&sequential_elapsed),
            operations: Some(1000),
            latency_ms: None,
            metrics: vec![
                distribution_metric(
                    "successful-throughput",
                    "operations/s",
                    &sequential_throughput,
                ),
                distribution_metric("p50-latency", "ms", &p50),
                distribution_metric("p99-latency", "ms", &p99),
            ],
        },
        MeasuredPhase {
            name: "concurrent".to_owned(),
            elapsed_ms: median(&concurrent_elapsed),
            operations: None,
            latency_ms: None,
            metrics: vec![
                distribution_metric(
                    "successful-throughput",
                    "operations/s",
                    &concurrent_throughput,
                ),
                distribution_metric("successful-commits", "commits/round", &accepted),
                distribution_metric("conflict-rate", "percent", &conflict_rate),
                distribution_metric("error-rate", "percent", &error_rate),
                counter_metric("request-errors", runs.iter().map(|run| run.errors).sum()),
                counter_metric(
                    "valid-rounds",
                    runs.iter().filter(|run| run.valid == "yes").count() as u64,
                ),
                counter_metric("measured-rounds", runs.len() as u64),
            ],
        },
    ]
}

fn distribution_metric(name: &str, unit: &str, values: &[f64]) -> Metric {
    Metric {
        name: name.to_owned(),
        unit: unit.to_owned(),
        value: MetricValue::Distribution {
            distribution: distribution(values),
        },
    }
}

fn counter_metric(name: &str, value: u64) -> Metric {
    Metric {
        name: name.to_owned(),
        unit: "count".to_owned(),
        value: MetricValue::Counter { value },
    }
}

fn distribution(values: &[f64]) -> Distribution {
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64;
    Distribution {
        samples: values.len() as u64,
        minimum: values.iter().copied().fold(f64::INFINITY, f64::min),
        maximum: values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        mean: Some(mean),
        standard_deviation: Some(variance.sqrt()),
        quantiles: BTreeMap::from([("p50".to_owned(), median(values))]),
    }
}

fn values(runs: &[RunRow], select: impl Fn(&RunRow) -> f64) -> Vec<f64> {
    runs.iter().map(select).collect()
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    }
}

fn verify_raw_evidence(
    summaries: &[SummaryRow],
    runs: &[RunRow],
    audits: &[AuditRow],
) -> Result<()> {
    if summaries.len() != 4 || runs.len() != 24 || audits.len() != 24 {
        bail!(
            "unexpected evidence dimensions: {} summaries, {} runs, {} audits",
            summaries.len(),
            runs.len(),
            audits.len()
        );
    }

    let summary_catalogs = summaries
        .iter()
        .map(|summary| summary.catalog.as_str())
        .collect::<BTreeSet<_>>();
    if summary_catalogs != CATALOGS.into_iter().collect() {
        bail!("summary catalogs differ from the expected four-catalog sweep");
    }
    let expected_pairs = CATALOGS
        .into_iter()
        .flat_map(|catalog| CONDITIONING_AND_MEASURED_ROUNDS.map(move |round| (round, catalog)))
        .collect::<BTreeSet<_>>();
    let run_pairs = runs
        .iter()
        .map(|run| (run.round, run.catalog.as_str()))
        .collect::<BTreeSet<_>>();
    let audit_pairs = audits
        .iter()
        .map(|audit| (audit.round, audit.catalog.as_str()))
        .collect::<BTreeSet<_>>();
    if run_pairs != expected_pairs || audit_pairs != expected_pairs {
        bail!(
            "run or audit evidence does not contain exactly rounds 1 through 6 for every catalog"
        );
    }

    let audit_by_run: BTreeMap<_, _> = audits
        .iter()
        .map(|audit| ((audit.round, audit.catalog.as_str()), audit))
        .collect();
    for run in runs {
        let audit = audit_by_run
            .get(&(run.round, run.catalog.as_str()))
            .with_context(|| {
                format!(
                    "missing object audit for round {} {}",
                    run.round, run.catalog
                )
            })?;
        let computed_delta = audit
            .objects_after
            .checked_sub(audit.objects_before)
            .with_context(|| {
                format!(
                    "object count decreased for round {} {}",
                    run.round, run.catalog
                )
            })?;
        if computed_delta != audit.delta
            || audit.delta != run.object_delta
            || audit.expected_min != run.expected_min
            || audit.delta < audit.expected_min
        {
            bail!(
                "object audit mismatch for round {} {}",
                run.round,
                run.catalog
            );
        }
    }

    for summary in summaries {
        let measured = runs
            .iter()
            .filter(|run| run.catalog == summary.catalog && run.round >= 2)
            .collect::<Vec<_>>();
        if measured.len() != 5 {
            bail!(
                "{} has {} measured rounds, expected 5",
                summary.catalog,
                measured.len()
            );
        }
        compare_aggregate(summary, &measured)?;
    }
    verify_legacy_order_and_ranks(summaries)?;
    Ok(())
}

fn compare_aggregate(summary: &SummaryRow, runs: &[&RunRow]) -> Result<()> {
    let concurrent = runs
        .iter()
        .map(|run| run.concurrent_per_s)
        .collect::<Vec<_>>();
    let sequential = runs.iter().map(|run| run.seq_per_s).collect::<Vec<_>>();
    let p50 = runs.iter().map(|run| run.p50_ms).collect::<Vec<_>>();
    let p99 = runs.iter().map(|run| run.p99_ms).collect::<Vec<_>>();
    let conflict = runs.iter().map(|run| run.conflict_pct).collect::<Vec<_>>();
    let error_rate = runs.iter().map(|run| run.error_pct).collect::<Vec<_>>();
    let checks = [
        (
            "concurrent_median",
            summary.concurrent_median,
            median(&concurrent),
        ),
        (
            "concurrent_min",
            summary.concurrent_min,
            minimum(&concurrent),
        ),
        (
            "concurrent_max",
            summary.concurrent_max,
            maximum(&concurrent),
        ),
        ("seq_median", summary.seq_median, median(&sequential)),
        ("seq_min", summary.seq_min, minimum(&sequential)),
        ("seq_max", summary.seq_max, maximum(&sequential)),
        ("p50_median_ms", summary.p50_median_ms, median(&p50)),
        ("p99_median_ms", summary.p99_median_ms, median(&p99)),
        (
            "conflict_median_pct",
            summary.conflict_median_pct,
            median(&conflict),
        ),
        (
            "error_median_pct",
            summary.error_median_pct,
            median(&error_rate),
        ),
    ];
    for (name, expected, actual) in checks {
        if (expected - actual).abs() > 0.000_001 {
            bail!(
                "summary mismatch for {} {name}: expected {expected}, recomputed {actual}",
                summary.catalog
            );
        }
    }
    let valid_rounds = runs.iter().filter(|run| run.valid == "yes").count() as u64;
    let total_errors = runs.iter().map(|run| run.errors).sum::<u64>();
    let expected_eligible = valid_rounds == runs.len() as u64 && total_errors == 0;
    if summary.valid_rounds != valid_rounds
        || summary.measured_rounds != runs.len() as u64
        || summary.total_errors != total_errors
        || (summary.eligible == "yes") != expected_eligible
    {
        bail!("validity summary mismatch for {}", summary.catalog);
    }
    for run in runs {
        let expected_valid =
            run.exit_status == 0 && run.errors == 0 && run.object_delta >= run.expected_min;
        if (run.valid == "yes") != expected_valid {
            bail!(
                "run validity mismatch for {} round {}",
                run.catalog,
                run.round
            );
        }
        if run.ok + run.conflicts == 0 {
            bail!(
                "run {} round {} attempted no requests",
                run.catalog,
                run.round
            );
        }
        if run.namespace.is_empty() {
            bail!(
                "run {} round {} has an empty namespace",
                run.catalog,
                run.round
            );
        }
        if run.expected_min != WARMUP_AND_SEQUENTIAL_COMMITS + run.ok {
            bail!(
                "object-growth minimum for {} round {} does not cover warmup, sequential, and accepted concurrent commits",
                run.catalog,
                run.round
            );
        }
        let attempts = run.ok + run.conflicts + run.errors;
        compare_rate(
            run,
            "conflict_pct",
            run.conflict_pct,
            run.conflicts as f64 * 100.0 / (run.ok + run.conflicts) as f64,
        )?;
        compare_rate(
            run,
            "error_pct",
            run.error_pct,
            run.errors as f64 * 100.0 / attempts as f64,
        )?;
    }
    Ok(())
}

fn compare_rate(run: &RunRow, name: &str, expected: f64, actual: f64) -> Result<()> {
    if (expected - actual).abs() > 0.000_001 {
        bail!(
            "{} mismatch for {} round {}: expected {expected}, recomputed {actual}",
            name,
            run.catalog,
            run.round
        );
    }
    Ok(())
}

fn verify_legacy_order_and_ranks(summaries: &[SummaryRow]) -> Result<()> {
    let mut by_display_order = summaries.iter().collect::<Vec<_>>();
    by_display_order.sort_by_key(|summary| summary.display_order);
    let expected = [
        ("nessie", "DQ"),
        ("lakecat", "1"),
        ("polaris", "2"),
        ("gravitino", "3"),
    ];
    for (summary, (catalog, rank)) in by_display_order.into_iter().zip(expected) {
        if summary.catalog != catalog || summary.rank != rank {
            bail!(
                "legacy display order or rank mismatch: expected {catalog} {rank}, found {} {}",
                summary.catalog,
                summary.rank
            );
        }
    }
    Ok(())
}

fn minimum(values: &[f64]) -> f64 {
    values.iter().copied().fold(f64::INFINITY, f64::min)
}

fn maximum(values: &[f64]) -> f64 {
    values.iter().copied().fold(f64::NEG_INFINITY, f64::max)
}

fn parse_tsv<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<Vec<T>> {
    csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .from_reader(bytes)
        .deserialize()
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("failed to parse TSV evidence")
}

fn parse_profile(bytes: &[u8]) -> Result<Profile> {
    match catalog_bench_common::contract::parse_contract(bytes)? {
        ContractDocument::Profile(profile) => Ok(profile),
        document => bail!("expected profile, found {}", document.kind()),
    }
}

fn parse_scenario(bytes: &[u8]) -> Result<Scenario> {
    match catalog_bench_common::contract::parse_contract(bytes)? {
        ContractDocument::Scenario(scenario) => Ok(scenario),
        document => bail!("expected scenario, found {}", document.kind()),
    }
}

fn read_hashed(path: PathBuf, expected_sha256: &str) -> Result<Vec<u8>> {
    let bytes = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let actual = sha256(&bytes);
    if actual != expected_sha256 {
        bail!(
            "{} hash mismatch: expected {expected_sha256}, got {actual}",
            path.display()
        );
    }
    Ok(bytes)
}

fn artifact(
    location: &str,
    media_type: &str,
    bytes: &[u8],
    description: &str,
) -> ArtifactReference {
    ArtifactReference {
        location: location.to_owned(),
        media_type: media_type.to_owned(),
        digest: Digest {
            algorithm: DigestAlgorithm::Sha256,
            value: sha256(bytes),
        },
        bytes: Some(bytes.len() as u64),
        description: Some(description.to_owned()),
        extensions: Extensions::new(),
    }
}

fn evidence(id: &str, kind: EvidenceKind, artifact: &ArtifactReference) -> Evidence {
    Evidence {
        id: id.into(),
        kind,
        artifact: artifact.clone(),
        sanitized: true,
        redactions: vec!["Numeric TSV reviewed; no secret-bearing fields are present.".to_owned()],
        extensions: Extensions::new(),
    }
}

fn assertion(id: &str, outcome: AssertionOutcome, evidence: &[&str]) -> AssertionEvaluation {
    AssertionEvaluation {
        assertion: id.into(),
        required: true,
        outcome,
        evidence: evidence.iter().map(|id| (*id).into()).collect(),
    }
}

fn pretty_json(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn sha256(bytes: &[u8]) -> String {
    sha256_hex(bytes)
}
