//! Deterministic materialization of the canonical C110 contention result bundle.
//!
//! The production runner owns execution and emits a typed, sanitized transcript.
//! This module independently replays its schedule and aggregation rules, binds a
//! reviewed environment/failure sidecar, and emits publishable contract records.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use catalog_bench_commit::aggregate::aggregate_contention;
use catalog_bench_commit::model::{PhaseEvidence, RequestErrorKind};
use catalog_bench_commit::policy::{
    ContentionPlan, RoundKind, CONTENTION_TRANSCRIPT_FORMAT, RUNNER_COMPONENT_ID,
};
use catalog_bench_commit::stats::nonnegative_distribution;
use catalog_bench_commit::transcript::{
    CatalogAggregate, CatalogRoundOutcome, CatalogRoundTranscript, ContentionTranscript,
    RankingDisposition,
};
use catalog_bench_commit::workflow::{OperationEvidence, RoundChecks};
use catalog_bench_common::contract::{
    ArtifactReference, AssertionEvaluation, AssertionOutcome, AssertionSpec, Component,
    ComponentId, ContractVersion, Distribution, EnvironmentManifest, Evidence, EvidenceKind,
    ExecutedComponent, Failure, FailureCategory, ManifestDocumentKind, MeasuredPhase, Metric,
    MetricValue, Profile, ProfileReference, Provenance, RedactionStatement, ResultBundleManifest,
    ResultDocumentKind, ResultId, ResultOutcome, ResultRecord, RunIdentity, Scenario,
    ScenarioReference, Validate,
};
use catalog_bench_conformance::AuthenticationOutcome;
use serde::Deserialize;

use crate::publication::{artifact, parse_profile, parse_scenario, pretty_json, read_hashed};
use crate::{load_bundle, render_commit_matrix, sha256_hex};

const OUTPUT_DIRECTORY: &str = "results/v1/2026-08-27";
const TRANSCRIPT_PATH: &str = "results/contention-2026-08-27-transcript.json";
const REVIEW_PATH: &str = "results/contention-2026-08-27-review.json";
const PROFILE_PATH: &str = "profiles/v1/contention-2026-08-27.json";
const SCENARIO_PATH: &str = "scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json";
const TRANSCRIPT_SHA256: &str = "af8057846da68036723ce96de09965e3fc18381b949f4c34e078749c26cae71e";
const REVIEW_SHA256: &str = "9f225fd562b114ac41655de43649a074a3a4d5ea8b348489a9a632d7c1d4fe95";
const REVIEW_FORMAT: &str = "catalog-bench/contention-result-review/v1";
const BUNDLE_ID: &str = "contention-2026-08-27-c110";
const TRANSCRIPT_EVIDENCE_ID: &str = "contention-transcript";
const REVIEW_EVIDENCE_ID: &str = "materialization-review";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultReview {
    format: String,
    bundle: BundleReview,
    run: RunReview,
    environment: EnvironmentManifest,
    failures: Vec<FailureReview>,
    redaction: RedactionStatement,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleReview {
    id: String,
    title: String,
    output_directory: String,
    created_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunReview {
    fixture_id: String,
    transcript: SourceIdentity,
    sanitized_invocation: String,
    started_at: String,
    started_at_basis: String,
    completed_at: String,
    completed_at_basis: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceIdentity {
    location: String,
    sha256: String,
    bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FailureReview {
    catalog: ComponentId,
    category: FailureCategory,
    summary: String,
    detail: String,
    retryable: bool,
    conditioning_errors: u64,
    measured_errors: u64,
    http_status_counts: BTreeMap<u16, u64>,
    log_observations: Vec<LogObservation>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogObservation {
    component: ComponentId,
    observed_at: String,
    signature: String,
    context: Vec<String>,
}

struct GeneratedBundle {
    directory: PathBuf,
    files: BTreeMap<String, Vec<u8>>,
}

struct ContractInputs<'a> {
    profile: &'a Profile,
    profile_bytes: &'a [u8],
    scenario: &'a Scenario,
    scenario_bytes: &'a [u8],
    plan: &'a ContentionPlan,
}

struct ResultBuildContext<'a> {
    transcript: &'a ContentionTranscript,
    review: &'a ResultReview,
    profile: &'a Profile,
    profile_artifact: &'a ArtifactReference,
    scenario: &'a Scenario,
    scenario_artifact: &'a ArtifactReference,
    transcript_artifact: &'a ArtifactReference,
    review_artifact: &'a ArtifactReference,
}

enum AssertionObservation {
    Pass,
    Fail(String),
}

/// Recompute, validate, and write the canonical production contention bundle.
pub fn write_contention_result_bundle(repository_root: &Path) -> Result<PathBuf> {
    let generated = generate(repository_root)?;
    fs::create_dir_all(&generated.directory)
        .with_context(|| format!("failed to create {}", generated.directory.display()))?;
    for (name, bytes) in &generated.files {
        let path = generated.directory.join(name);
        fs::write(&path, bytes).with_context(|| format!("failed to write {}", path.display()))?;
    }
    let manifest = generated.directory.join("manifest.json");
    let matrix = render_commit_matrix(&load_bundle(&manifest)?)?;
    fs::write(generated.directory.join("MATRIX.md"), matrix)
        .context("failed to write generated contention matrix")?;
    Ok(manifest)
}

/// Fail if any checked-in production record or generated matrix has drifted.
pub fn check_contention_result_bundle(repository_root: &Path) -> Result<PathBuf> {
    let generated = generate(repository_root)?;
    for (name, expected) in &generated.files {
        let path = generated.directory.join(name);
        let actual = fs::read(&path)
            .with_context(|| format!("failed to read generated artifact {}", path.display()))?;
        if actual != *expected {
            bail!(
                "{} is stale; rerun `catalog-bench-contract contention-import write`",
                path.display()
            );
        }
    }
    let manifest = generated.directory.join("manifest.json");
    let expected_matrix = render_commit_matrix(&load_bundle(&manifest)?)?;
    let matrix_path = generated.directory.join("MATRIX.md");
    let actual_matrix = fs::read_to_string(&matrix_path)
        .with_context(|| format!("failed to read {}", matrix_path.display()))?;
    if actual_matrix != expected_matrix {
        bail!(
            "{} is stale; rerun `catalog-bench-contract contention-import write`",
            matrix_path.display()
        );
    }
    Ok(manifest)
}

fn generate(repository_root: &Path) -> Result<GeneratedBundle> {
    let transcript_bytes = read_hashed(&repository_root.join(TRANSCRIPT_PATH), TRANSCRIPT_SHA256)?;
    let review_bytes = read_hashed(&repository_root.join(REVIEW_PATH), REVIEW_SHA256)?;
    let profile_bytes = fs::read(repository_root.join(PROFILE_PATH))?;
    let scenario_bytes = fs::read(repository_root.join(SCENARIO_PATH))?;

    let transcript: ContentionTranscript = serde_json::from_slice(&transcript_bytes)
        .context("invalid production contention transcript")?;
    let review: ResultReview =
        serde_json::from_slice(&review_bytes).context("invalid contention result review")?;
    let profile = parse_profile(&profile_bytes)?;
    let scenario = parse_scenario(&scenario_bytes)?;
    let plan = ContentionPlan::from_contracts(&profile, &scenario)
        .context("profile and scenario do not form a valid contention plan")?;

    let contracts = ContractInputs {
        profile: &profile,
        profile_bytes: &profile_bytes,
        scenario: &scenario,
        scenario_bytes: &scenario_bytes,
        plan: &plan,
    };
    verify_transcript(&transcript, &transcript_bytes, &contracts)?;
    let (aggregates, ranking, classification) = aggregate_contention(&plan, &transcript.rounds)
        .context("failed to independently aggregate transcript rounds")?;
    if transcript.aggregates != aggregates
        || transcript.ranking != ranking
        || transcript.classification != classification
    {
        bail!("transcript aggregates, ranking, or sweep classification do not recompute exactly");
    }
    verify_review(
        &review,
        &transcript,
        &transcript_bytes,
        &profile,
        &aggregates,
    )?;

    let profile_artifact = artifact(
        "../../../profiles/v1/contention-2026-08-27.json",
        "application/json",
        &profile_bytes,
        "Runnable production contention profile with exact source, image, and executable identities.",
    );
    let scenario_artifact = artifact(
        "../../../scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json",
        "application/json",
        &scenario_bytes,
        "Canonical same-table contention v2 scenario.",
    );
    let transcript_artifact = artifact(
        "../../contention-2026-08-27-transcript.json",
        "application/json",
        &transcript_bytes,
        "Complete sanitized 30-round production contention transcript.",
    );
    let review_artifact = artifact(
        "../../contention-2026-08-27-review.json",
        "application/json",
        &review_bytes,
        "Reviewed runtime capture and sanitized server-side failure attribution.",
    );

    let components = profile
        .components
        .iter()
        .map(|component| (component.id.as_str(), component))
        .collect::<BTreeMap<_, _>>();
    let failure_reviews = review
        .failures
        .iter()
        .map(|failure| (failure.catalog.as_str(), failure))
        .collect::<BTreeMap<_, _>>();
    let result_context = ResultBuildContext {
        transcript: &transcript,
        review: &review,
        profile: &profile,
        profile_artifact: &profile_artifact,
        scenario: &scenario,
        scenario_artifact: &scenario_artifact,
        transcript_artifact: &transcript_artifact,
        review_artifact: &review_artifact,
    };
    let mut files = BTreeMap::new();
    let mut result_artifacts = Vec::new();
    for aggregate in &aggregates {
        let catalog_id = aggregate.catalog.catalog.as_str();
        let component = components
            .get(catalog_id)
            .with_context(|| format!("profile has no `{catalog_id}` component"))?;
        let result = build_result(
            component,
            aggregate,
            failure_reviews.get(catalog_id).copied(),
            &result_context,
        )?;
        let name = format!("{catalog_id}.json");
        let bytes = pretty_json(&result)?;
        result_artifacts.push(artifact(
            &name,
            "application/json",
            &bytes,
            &format!("Production contention aggregate for {}.", component.name),
        ));
        files.insert(name, bytes);
    }

    let manifest = ResultBundleManifest {
        contract_version: ContractVersion::V1,
        kind: ManifestDocumentKind::Manifest,
        id: review.bundle.id.clone().into(),
        title: review.bundle.title.clone(),
        created_at: review.bundle.created_at.clone(),
        provenance: Provenance::LiveRun {
            runner: RUNNER_COMPONENT_ID.into(),
            sanitized_invocation: review.run.sanitized_invocation.clone(),
            started_at: review.run.started_at.clone(),
            completed_at: review.run.completed_at.clone(),
        },
        profile: profile_artifact,
        scenarios: vec![scenario_artifact],
        results: result_artifacts,
        source_evidence: vec![transcript_artifact, review_artifact],
        redaction: review.redaction.clone(),
        extensions: BTreeMap::from([
            (
                "querygraph/fixture-id".to_owned(),
                serde_json::json!(&transcript.fixture_id),
            ),
            (
                "querygraph/ranking".to_owned(),
                serde_json::json!({
                    "basis": &transcript.ranking.basis,
                    "tie_breakers": &transcript.ranking.tie_breakers,
                    "strict_pass_only": true
                }),
            ),
            (
                "querygraph/transcript".to_owned(),
                serde_json::json!({
                    "format": &transcript.format,
                    "sha256": TRANSCRIPT_SHA256,
                    "rounds": transcript.rounds.len()
                }),
            ),
        ]),
    };
    manifest.validate()?;
    files.insert("manifest.json".to_owned(), pretty_json(&manifest)?);

    Ok(GeneratedBundle {
        directory: repository_root.join(OUTPUT_DIRECTORY),
        files,
    })
}

fn build_result(
    component: &Component,
    aggregate: &CatalogAggregate,
    failure_review: Option<&FailureReview>,
    context: &ResultBuildContext<'_>,
) -> Result<ResultRecord> {
    let ResultBuildContext {
        transcript,
        review,
        profile,
        profile_artifact,
        scenario,
        scenario_artifact,
        transcript_artifact,
        review_artifact,
    } = context;
    let catalog_id = component.id.as_str();
    let rounds = transcript
        .rounds
        .iter()
        .filter(|round| round.catalog.catalog == component.id)
        .collect::<Vec<_>>();
    let measured_rounds = rounds
        .iter()
        .copied()
        .filter(|round| round.kind == RoundKind::Measured)
        .collect::<Vec<_>>();
    let evidence = result_evidence(
        transcript,
        &review.redaction,
        transcript_artifact,
        review_artifact,
    );
    let assertions = scenario
        .assertions
        .iter()
        .map(|assertion| evaluate_assertion(assertion, transcript, &rounds))
        .collect::<Result<Vec<_>>>()?;
    let failed_required = assertions
        .iter()
        .filter(|evaluation| {
            evaluation.required && matches!(evaluation.outcome, AssertionOutcome::Fail { .. })
        })
        .map(|evaluation| evaluation.assertion.to_string())
        .collect::<Vec<_>>();
    let measurements = measurements(&measured_rounds, aggregate.measured.passed)?;
    let ranking_entry = transcript
        .ranking
        .entries
        .iter()
        .find(|entry| entry.catalog.catalog == component.id)
        .with_context(|| format!("ranking has no `{catalog_id}` entry"))?;

    let outcome = if failed_required.is_empty() {
        if failure_review.is_some() {
            bail!("passing catalog `{catalog_id}` unexpectedly has a failure review");
        }
        let RankingDisposition::Ranked { score, .. } = &ranking_entry.disposition else {
            bail!("passing catalog `{catalog_id}` is not ranked");
        };
        ResultOutcome::Pass {
            summary: Some(format!(
                "Conditioning repetition 1 and all five measured repetitions passed every required assertion; median concurrent accepted throughput was {:.6} operations/s.",
                score.median
            )),
        }
    } else {
        let failure_review = failure_review.with_context(|| {
            format!("failed catalog `{catalog_id}` has no reviewed failure attribution")
        })?;
        if !matches!(
            ranking_entry.disposition,
            RankingDisposition::NotRanked { .. }
        ) {
            bail!("failed catalog `{catalog_id}` is unexpectedly ranked");
        }
        ResultOutcome::Fail {
            failure: Failure {
                category: failure_review.category,
                summary: failure_review.summary.clone(),
                detail: failure_review.detail.clone(),
                retryable: failure_review.retryable,
                evidence: vec![TRANSCRIPT_EVIDENCE_ID.into(), REVIEW_EVIDENCE_ID.into()],
            },
        }
    };

    let total_errors = sum_phase_count(&measured_rounds, |phase| phase.counts.errors)?;
    let result = ResultRecord {
        contract_version: ContractVersion::V1,
        kind: ResultDocumentKind::Result,
        id: ResultId::new(format!("{BUNDLE_ID}-{catalog_id}")),
        scenario: ScenarioReference {
            id: scenario.id.clone(),
            version: scenario.version,
            digest: scenario_artifact.digest.clone(),
        },
        profile: ProfileReference {
            id: profile.id.clone(),
            digest: profile_artifact.digest.clone(),
        },
        catalog: ExecutedComponent {
            profile_component: component.id.clone(),
            name: component.name.clone(),
            version: component.version.clone(),
        },
        client: None,
        adapters: Vec::new(),
        run: RunIdentity::Aggregate {
            id: format!("{}-{catalog_id}-measured", transcript.fixture_id),
            period: format!("{}/{}", review.run.started_at, review.run.completed_at),
            included_repetitions: vec![2, 3, 4, 5, 6],
            excluded_repetitions: vec![1],
            aggregation: "Performance distributions use measured repetitions 2 through 6 with full range, population moments, and deterministic quantiles. Strict classification additionally requires conditioning repetition 1 and every measured repetition to pass.".to_owned(),
        },
        outcome,
        environment: review.environment.clone(),
        assertions,
        measurements,
        evidence,
        artifacts: Vec::new(),
        extensions: BTreeMap::from([
            (
                "querygraph/rounds".to_owned(),
                serde_json::json!({
                    "conditioning": &aggregate.conditioning,
                    "measured": &aggregate.measured,
                    "failed_required_assertions": failed_required
                }),
            ),
            (
                "querygraph/ranking-disposition".to_owned(),
                serde_json::to_value(&ranking_entry.disposition)?,
            ),
            (
                "querygraph/request-handling".to_owned(),
                serde_json::json!({
                    "protocol": "iceberg-rest-v1",
                    "behavior_changing_shim": false
                }),
            ),
            (
                "querygraph/measured-request-errors".to_owned(),
                serde_json::json!(total_errors),
            ),
        ]),
    };
    result.validate()?;
    Ok(result)
}

fn result_evidence(
    transcript: &ContentionTranscript,
    redaction: &RedactionStatement,
    transcript_artifact: &ArtifactReference,
    review_artifact: &ArtifactReference,
) -> Vec<Evidence> {
    vec![
        Evidence {
            id: TRANSCRIPT_EVIDENCE_ID.into(),
            kind: EvidenceKind::HttpTranscript,
            artifact: transcript_artifact.clone(),
            sanitized: true,
            redactions: transcript.sanitization.redactions.clone(),
            extensions: BTreeMap::from([(
                "querygraph/transcript-format".to_owned(),
                serde_json::json!(&transcript.format),
            )]),
        },
        Evidence {
            id: REVIEW_EVIDENCE_ID.into(),
            kind: EvidenceKind::Log,
            artifact: review_artifact.clone(),
            sanitized: true,
            redactions: redaction.removed_fields.clone(),
            extensions: BTreeMap::from([(
                "querygraph/reviewed".to_owned(),
                serde_json::json!(redaction.reviewed),
            )]),
        },
    ]
}

fn evaluate_assertion(
    assertion: &AssertionSpec,
    transcript: &ContentionTranscript,
    rounds: &[&CatalogRoundTranscript],
) -> Result<AssertionEvaluation> {
    let observation = match assertion.id.as_str() {
        "runner-runtime-pinned" => observe_global(
            transcript.runner.passed()
                && transcript.runner.component.as_str() == RUNNER_COMPONENT_ID,
            "The runner runtime or source identity did not match the selected profile.",
        ),
        "catalog-auth-config-ready" => observe_rounds(rounds, negotiation_ready, |failed| {
            format!(
                "Authentication, config, or profile-native routing was not ready in {}.",
                failed.join(", ")
            )
        }),
        "fixture-isolated" => observe_check(rounds, |checks| checks.fixture_isolated),
        "setup-succeeded" => observe_check(rounds, |checks| checks.setup_succeeded),
        "warmup-accounted" => observe_check(rounds, |checks| checks.warmup_accounted),
        "sequential-accounted" => observe_check(rounds, |checks| checks.sequential_accounted),
        "sequential-latency-complete" => {
            observe_check(rounds, |checks| checks.sequential_latency_complete)
        }
        "all-requests-accounted" => observe_check(rounds, |checks| checks.all_requests_accounted),
        "zero-request-errors" => {
            let observation = observe_check(rounds, |checks| checks.zero_request_errors);
            match observation {
                AssertionObservation::Pass => AssertionObservation::Pass,
                AssertionObservation::Fail(_) => {
                    let errors = sum_phase_count(rounds, |phase| phase.counts.errors)?;
                    AssertionObservation::Fail(format!(
                        "{errors} non-conflict request errors occurred across conditioning and measured repetitions."
                    ))
                }
            }
        }
        "concurrent-progress" => observe_check(rounds, |checks| checks.concurrent_progress),
        "final-state-accounted" => observe_check(rounds, |checks| checks.final_state_accounted),
        "metadata-persisted" => observe_check(rounds, |checks| checks.metadata_persisted),
        "fixture-clean" => observe_check(rounds, |checks| checks.fixture_clean),
        "transcript-sanitized" => observe_global(
            transcript.sanitization.policy == "catalog-bench/value-safe-contention-v1"
                && !transcript.sanitization.raw_secrets_persisted
                && !transcript.sanitization.raw_response_body_persisted
                && !transcript.sanitization.raw_request_identities_persisted
                && transcript.sanitization.write_mode == "create-new",
            "The transcript sanitization or create-new policy was not satisfied.",
        ),
        unknown => bail!("contention result materializer does not implement assertion `{unknown}`"),
    };
    let outcome = match observation {
        AssertionObservation::Pass => AssertionOutcome::Pass,
        AssertionObservation::Fail(explanation) => AssertionOutcome::Fail { explanation },
    };
    Ok(AssertionEvaluation {
        assertion: assertion.id.clone(),
        required: assertion.required,
        outcome,
        evidence: vec![TRANSCRIPT_EVIDENCE_ID.into()],
    })
}

fn observe_global(passed: bool, explanation: &str) -> AssertionObservation {
    if passed {
        AssertionObservation::Pass
    } else {
        AssertionObservation::Fail(explanation.to_owned())
    }
}

fn observe_check(
    rounds: &[&CatalogRoundTranscript],
    check: impl Fn(&RoundChecks) -> bool,
) -> AssertionObservation {
    observe_rounds(
        rounds,
        |round| {
            round
                .execution()
                .is_some_and(|execution| check(&execution.checks))
        },
        |failed| format!("The check failed in {}.", failed.join(", ")),
    )
}

fn observe_rounds(
    rounds: &[&CatalogRoundTranscript],
    passed: impl Fn(&CatalogRoundTranscript) -> bool,
    explanation: impl FnOnce(&[String]) -> String,
) -> AssertionObservation {
    let failed = rounds
        .iter()
        .filter(|round| !passed(round))
        .map(|round| round_label(round))
        .collect::<Vec<_>>();
    if failed.is_empty() {
        AssertionObservation::Pass
    } else {
        AssertionObservation::Fail(explanation(&failed))
    }
}

fn negotiation_ready(round: &CatalogRoundTranscript) -> bool {
    matches!(round.outcome, CatalogRoundOutcome::Executed { .. })
        && round.negotiation.adapter.catalog == round.catalog.catalog
        && round.negotiation.adapter.name == round.catalog.name
        && round.negotiation.adapter.version == round.catalog.version
        && round.negotiation.authentication.outcome == AuthenticationOutcome::Ready
        && round
            .negotiation
            .config
            .response
            .as_ref()
            .is_some_and(|response| (200..300).contains(&response.status))
}

fn round_label(round: &CatalogRoundTranscript) -> String {
    let kind = match round.kind {
        RoundKind::Conditioning => "conditioning",
        RoundKind::Measured => "measured",
    };
    format!("{kind} repetition {}", round.repetition)
}

fn measurements(
    rounds: &[&CatalogRoundTranscript],
    valid_rounds: u32,
) -> Result<Vec<MeasuredPhase>> {
    if rounds.is_empty() {
        bail!("contention result requires measured rounds");
    }
    let sequential = rounds
        .iter()
        .map(|round| phase(round, |execution| &execution.sequential, "sequential"))
        .collect::<Result<Vec<_>>>()?;
    let concurrent = rounds
        .iter()
        .map(|round| phase(round, |execution| &execution.concurrent, "concurrent"))
        .collect::<Result<Vec<_>>>()?;

    let sequential_elapsed = select(&sequential, |phase| phase.elapsed_ms);
    let concurrent_elapsed = select(&concurrent, |phase| phase.elapsed_ms);
    let sequential_throughput = select(&sequential, |phase| phase.accepted_throughput_per_second);
    let concurrent_attempted = select(&concurrent, |phase| phase.attempted_throughput_per_second);
    let concurrent_accepted = select(&concurrent, |phase| phase.accepted_throughput_per_second);
    let attempts = select(&concurrent, |phase| phase.counts.attempts as f64);
    let accepted = select(&concurrent, |phase| phase.counts.accepted as f64);
    let conflicts = select(&concurrent, |phase| phase.counts.conflicts as f64);
    let errors = select(&concurrent, |phase| phase.counts.errors as f64);
    let conflict_percent = select(&concurrent, |phase| phase.conflict_rate * 100.0);
    let error_percent = select(&concurrent, |phase| phase.error_rate * 100.0);
    let growth = rounds
        .iter()
        .map(|round| {
            operation_output(
                &round
                    .execution()
                    .context("measured round has no execution")?
                    .metadata_growth,
                "metadata growth",
            )?
            .observed_growth
            .context("metadata object count decreased")
            .map(|value| value as f64)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(vec![
        MeasuredPhase {
            name: "sequential".to_owned(),
            elapsed_ms: median(&sequential_elapsed)?,
            operations: Some(sequential[0].counts.attempts),
            latency_ms: None,
            metrics: vec![
                distribution_metric(
                    "successful-throughput",
                    "operations/s",
                    &sequential_throughput,
                )?,
                quantile_metric("p50-latency", &sequential, "p50")?,
                quantile_metric("p95-latency", &sequential, "p95")?,
                quantile_metric("p99-latency", &sequential, "p99")?,
            ],
        },
        MeasuredPhase {
            name: "concurrent".to_owned(),
            elapsed_ms: median(&concurrent_elapsed)?,
            operations: None,
            latency_ms: None,
            metrics: vec![
                distribution_metric(
                    "successful-throughput",
                    "operations/s",
                    &concurrent_accepted,
                )?,
                distribution_metric(
                    "attempted-throughput",
                    "operations/s",
                    &concurrent_attempted,
                )?,
                quantile_metric("p50-latency", &concurrent, "p50")?,
                quantile_metric("p95-latency", &concurrent, "p95")?,
                quantile_metric("p99-latency", &concurrent, "p99")?,
                distribution_metric("request-attempts-per-round", "requests/round", &attempts)?,
                distribution_metric("successful-commits", "commits/round", &accepted)?,
                distribution_metric("conflicts-per-round", "conflicts/round", &conflicts)?,
                distribution_metric("errors-per-round", "errors/round", &errors)?,
                distribution_metric("conflict-rate", "percent", &conflict_percent)?,
                distribution_metric("error-rate", "percent", &error_percent)?,
                distribution_metric("metadata-object-growth", "objects/round", &growth)?,
                counter_metric("request-attempts", sum(&attempts)?),
                counter_metric("accepted-commits", sum(&accepted)?),
                counter_metric("conflicts", sum(&conflicts)?),
                counter_metric("request-errors", sum(&errors)?),
                counter_metric("valid-rounds", u64::from(valid_rounds)),
                counter_metric("measured-rounds", rounds.len() as u64),
            ],
        },
    ])
}

fn phase<'a>(
    round: &'a CatalogRoundTranscript,
    select: impl FnOnce(
        &'a catalog_bench_commit::workflow::RoundExecution,
    ) -> &'a OperationEvidence<PhaseEvidence>,
    name: &str,
) -> Result<&'a PhaseEvidence> {
    let execution = round
        .execution()
        .with_context(|| format!("{} has no execution", round_label(round)))?;
    operation_output(select(execution), name)
}

fn operation_output<'a, T>(operation: &'a OperationEvidence<T>, name: &str) -> Result<&'a T> {
    match operation {
        OperationEvidence::Succeeded { output } => Ok(output),
        OperationEvidence::Failed { .. } => bail!("{name} operation failed"),
        OperationEvidence::NotAttempted { .. } => bail!("{name} operation was not attempted"),
    }
}

fn select(phases: &[&PhaseEvidence], select: impl Fn(&PhaseEvidence) -> f64) -> Vec<f64> {
    phases.iter().map(|phase| select(phase)).collect()
}

fn quantile_metric(name: &str, phases: &[&PhaseEvidence], quantile: &str) -> Result<Metric> {
    let values = phases
        .iter()
        .map(|phase| {
            phase
                .latency_ms
                .all
                .as_ref()
                .context("phase has no complete latency distribution")?
                .quantiles
                .get(quantile)
                .copied()
                .with_context(|| format!("phase latency has no `{quantile}` quantile"))
        })
        .collect::<Result<Vec<_>>>()?;
    distribution_metric(name, "ms", &values)
}

fn distribution_metric(name: &str, unit: &str, values: &[f64]) -> Result<Metric> {
    Ok(Metric {
        name: name.to_owned(),
        unit: unit.to_owned(),
        value: MetricValue::Distribution {
            distribution: nonnegative_distribution(values)?,
        },
    })
}

fn counter_metric(name: &str, value: u64) -> Metric {
    Metric {
        name: name.to_owned(),
        unit: "count".to_owned(),
        value: MetricValue::Counter { value },
    }
}

fn median(values: &[f64]) -> Result<f64> {
    distribution_median(&nonnegative_distribution(values)?)
}

fn distribution_median(distribution: &Distribution) -> Result<f64> {
    distribution
        .quantiles
        .get("p50")
        .copied()
        .context("distribution has no p50")
}

fn sum(values: &[f64]) -> Result<u64> {
    values.iter().try_fold(0_u64, |total, value| {
        if value.fract() != 0.0 || *value < 0.0 || *value > u64::MAX as f64 {
            bail!("count distribution contains non-integral value {value}");
        }
        total
            .checked_add(*value as u64)
            .context("count total overflowed")
    })
}

fn sum_phase_count(
    rounds: &[&CatalogRoundTranscript],
    select: impl Fn(&PhaseEvidence) -> u64,
) -> Result<u64> {
    rounds.iter().try_fold(0_u64, |total, round| {
        let phase = phase(round, |execution| &execution.concurrent, "concurrent")?;
        total
            .checked_add(select(phase))
            .context("request count overflowed")
    })
}

fn verify_transcript(
    transcript: &ContentionTranscript,
    transcript_bytes: &[u8],
    contracts: &ContractInputs<'_>,
) -> Result<()> {
    let ContractInputs {
        profile,
        profile_bytes,
        scenario,
        scenario_bytes,
        plan,
    } = contracts;
    if transcript.format != CONTENTION_TRANSCRIPT_FORMAT {
        bail!(
            "unexpected contention transcript format `{}`",
            transcript.format
        );
    }
    if transcript.contract_digests.profile_sha256 != sha256_hex(profile_bytes)
        || transcript.contract_digests.scenario_sha256 != sha256_hex(scenario_bytes)
    {
        bail!("transcript contract digests do not match the exact profile and scenario bytes");
    }
    if transcript.profile.id != profile.id || transcript.profile.resolved_at != profile.resolved_at
    {
        bail!("transcript profile identity does not match the runnable profile");
    }
    if transcript.scenario.id != scenario.id || transcript.scenario.version != scenario.version {
        bail!("transcript scenario identity does not match the v2 scenario");
    }
    if transcript.parameters != *plan.parameters() {
        bail!("transcript parameters differ from the scenario-derived contention plan");
    }
    let runner = profile
        .components
        .iter()
        .find(|component| component.id.as_str() == RUNNER_COMPONENT_ID)
        .context("profile omits the contention runner component")?;
    let runner_revision = runner
        .source
        .as_ref()
        .context("runner component has no source revision")?
        .revision
        .as_str();
    if transcript.runner.component != runner.id
        || transcript.runner.name != runner.name
        || transcript.runner.version != runner.version
        || transcript.runner.source_revision != runner_revision
        || transcript.runner.operating_system != profile.platform.operating_system
        || transcript.runner.architecture != profile.platform.architecture
        || !transcript.runner.passed()
    {
        bail!("transcript runner identity or runtime does not match the selected profile");
    }
    if transcript.sanitization.policy != "catalog-bench/value-safe-contention-v1"
        || transcript.sanitization.raw_secrets_persisted
        || transcript.sanitization.raw_response_body_persisted
        || transcript.sanitization.raw_request_identities_persisted
        || transcript.sanitization.write_mode != "create-new"
    {
        bail!("transcript does not satisfy the publishable sanitization contract");
    }
    transcript
        .audit_serialized_values(&[])
        .context("transcript contains an unsanitized request identity")?;
    if transcript_bytes.len() as u64 != 434_978 || sha256_hex(transcript_bytes) != TRANSCRIPT_SHA256
    {
        bail!("transcript bytes differ from the reviewed C110 source evidence");
    }
    Ok(())
}

fn verify_review(
    review: &ResultReview,
    transcript: &ContentionTranscript,
    transcript_bytes: &[u8],
    profile: &Profile,
    aggregates: &[CatalogAggregate],
) -> Result<()> {
    if review.format != REVIEW_FORMAT
        || review.bundle.id != BUNDLE_ID
        || review.bundle.output_directory != OUTPUT_DIRECTORY
    {
        bail!("result review identifies an unexpected format, bundle, or output directory");
    }
    require_text(&review.bundle.title, "bundle title")?;
    require_utc_timestamp(&review.bundle.created_at, "bundle created_at")?;
    require_utc_timestamp(&review.run.started_at, "run started_at")?;
    require_utc_timestamp(&review.run.completed_at, "run completed_at")?;
    if review.run.started_at >= review.run.completed_at
        || review.run.completed_at >= review.bundle.created_at
    {
        bail!("reviewed run and bundle timestamps are not strictly ordered");
    }
    require_text(&review.run.started_at_basis, "started_at basis")?;
    require_text(&review.run.completed_at_basis, "completed_at basis")?;
    require_text(&review.run.sanitized_invocation, "sanitized invocation")?;
    if review.run.fixture_id != transcript.fixture_id
        || review.run.transcript.location != TRANSCRIPT_PATH
        || review.run.transcript.sha256 != TRANSCRIPT_SHA256
        || review.run.transcript.bytes != transcript_bytes.len() as u64
    {
        bail!("result review does not bind the exact C110 transcript");
    }
    if review.environment.operating_system != profile.platform.operating_system
        || review.environment.architecture != profile.platform.architecture
        || review.environment.cpu_limit.is_some()
        || review.environment.memory_limit_bytes.is_some()
    {
        bail!("reviewed environment differs from the profile platform or observed no-limit setup");
    }
    if !review.redaction.reviewed {
        bail!("result review has not completed redaction review");
    }
    require_text(&review.redaction.policy, "redaction policy")?;

    let failed_catalogs = aggregates
        .iter()
        .filter(|aggregate| !aggregate.passed())
        .map(|aggregate| aggregate.catalog.catalog.clone())
        .collect::<BTreeSet<_>>();
    let reviewed_catalogs = review
        .failures
        .iter()
        .map(|failure| failure.catalog.clone())
        .collect::<BTreeSet<_>>();
    if review.failures.len() != reviewed_catalogs.len() || reviewed_catalogs != failed_catalogs {
        bail!("failure reviews do not cover exactly the failed catalogs");
    }
    let profile_components = profile
        .components
        .iter()
        .map(|component| &component.id)
        .collect::<BTreeSet<_>>();
    for failure in &review.failures {
        verify_failure_review(failure, transcript, &profile_components)?;
    }
    Ok(())
}

fn verify_failure_review(
    review: &FailureReview,
    transcript: &ContentionTranscript,
    profile_components: &BTreeSet<&ComponentId>,
) -> Result<()> {
    require_text(&review.summary, "failure summary")?;
    require_text(&review.detail, "failure detail")?;
    if review.log_observations.is_empty() {
        bail!(
            "failure review for `{}` has no log observation",
            review.catalog
        );
    }
    for observation in &review.log_observations {
        if !profile_components.contains(&observation.component) {
            bail!(
                "failure review for `{}` references unknown component `{}`",
                review.catalog,
                observation.component
            );
        }
        require_utc_timestamp(&observation.observed_at, "log observation timestamp")?;
        require_text(&observation.signature, "log observation signature")?;
        if observation.context.is_empty() || observation.context.iter().any(|line| line.is_empty())
        {
            bail!("failure log observation has empty context");
        }
    }

    let rounds = transcript
        .rounds
        .iter()
        .filter(|round| round.catalog.catalog == review.catalog)
        .collect::<Vec<_>>();
    let conditioning = rounds
        .iter()
        .copied()
        .filter(|round| round.kind == RoundKind::Conditioning)
        .collect::<Vec<_>>();
    let measured = rounds
        .iter()
        .copied()
        .filter(|round| round.kind == RoundKind::Measured)
        .collect::<Vec<_>>();
    let conditioning_errors = sum_phase_count(&conditioning, |phase| phase.counts.errors)?;
    let measured_errors = sum_phase_count(&measured, |phase| phase.counts.errors)?;
    if review.conditioning_errors != conditioning_errors
        || review.measured_errors != measured_errors
    {
        bail!(
            "reviewed error totals for `{}` do not match the transcript",
            review.catalog
        );
    }
    let status_counts = http_status_counts(&rounds)?;
    if review.http_status_counts != status_counts {
        bail!(
            "reviewed HTTP status counts for `{}` do not match the transcript",
            review.catalog
        );
    }
    Ok(())
}

fn http_status_counts(rounds: &[&CatalogRoundTranscript]) -> Result<BTreeMap<u16, u64>> {
    let mut counts = BTreeMap::<u16, u64>::new();
    for round in rounds {
        let phase = phase(round, |execution| &execution.concurrent, "concurrent")?;
        for error_count in &phase.error_counts {
            if error_count.error.kind != RequestErrorKind::UnexpectedHttp {
                bail!("reviewed failure contains a non-HTTP request error");
            }
            let status = error_count
                .error
                .http_status
                .context("unexpected HTTP error omits status")?;
            let count = counts.entry(status).or_default();
            *count = count
                .checked_add(error_count.count)
                .context("HTTP error count overflowed")?;
        }
    }
    Ok(counts)
}

fn require_text(value: &str, name: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(())
}

fn require_utc_timestamp(value: &str, name: &str) -> Result<()> {
    if value.len() < 20 || !value.ends_with('Z') || !value.contains('T') {
        bail!("{name} must be a UTC RFC 3339 timestamp");
    }
    Ok(())
}
