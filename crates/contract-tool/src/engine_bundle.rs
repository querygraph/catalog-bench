//! Deterministic stock-engine result and bundle materialization.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{
    ArtifactReference, AssertionEvaluation, AssertionOutcome, AssertionSpec, ContractVersion,
    Evidence, EvidenceKind, ExecutedComponent, Failure, FailureCategory, ManifestDocumentKind,
    NotTestedReason, ProfileReference, Provenance, ResultBundleManifest, ResultDocumentKind,
    ResultId, ResultOutcome, ResultRecord, RunIdentity, ScenarioReference, Validate,
};
use catalog_bench_engine::{
    EngineBehaviorClassification, EngineProcessOutcome, EngineTranscript, EngineTranscriptComponent,
};

use crate::engine_matrix::render_engine_matrix;
use crate::publication::{artifact, pretty_json};
use crate::{load_bundle, validate_engine_result_review, ValidatedEngineResultReview};

const TRANSCRIPT_EVIDENCE_ID: &str = "engine-transcript";
const REVIEW_EVIDENCE_ID: &str = "materialization-review";
const PROFILE_COPY: &str = "source/profile.json";
const SCENARIO_COPY: &str = "source/scenario.json";
const REVIEW_COPY: &str = "source/review.json";
const TRANSCRIPT_DIRECTORY: &str = "source/transcripts";
const MANIFEST_FILE: &str = "manifest.json";
const MATRIX_FILE: &str = "MATRIX.md";

struct GeneratedEngineBundle {
    directory: PathBuf,
    files: BTreeMap<PathBuf, Vec<u8>>,
}

/// Create one immutable bundle without overwriting any existing output.
pub fn write_engine_result_bundle(repository_root: &Path, review_path: &Path) -> Result<PathBuf> {
    let generated = generate(repository_root, review_path)?;
    let parent = generated
        .directory
        .parent()
        .context("engine bundle output has no parent")?;
    create_output_parent(repository_root, parent)?;
    fs::create_dir(&generated.directory).with_context(|| {
        format!(
            "refusing to replace existing engine bundle output {}",
            generated.directory.display()
        )
    })?;

    for (relative, bytes) in &generated.files {
        write_new_file(&generated.directory, relative, bytes)?;
    }
    let manifest = generated.directory.join(MANIFEST_FILE);
    let matrix = render_engine_matrix(&load_bundle(&manifest)?)?;
    write_new_file(
        &generated.directory,
        Path::new(MATRIX_FILE),
        matrix.as_bytes(),
    )?;
    check_engine_result_bundle(repository_root, review_path)
}

/// Recompute every byte and reject stale, missing, or unexpected output.
pub fn check_engine_result_bundle(repository_root: &Path, review_path: &Path) -> Result<PathBuf> {
    let generated = generate(repository_root, review_path)?;
    for (relative, expected) in &generated.files {
        let path = generated.directory.join(relative);
        let actual = fs::read(&path)
            .with_context(|| format!("failed to read generated artifact {}", path.display()))?;
        if actual != *expected {
            bail!(
                "{} is stale; rerun `catalog-bench-contract engine-import write` with a new output directory",
                path.display()
            );
        }
    }

    let manifest = generated.directory.join(MANIFEST_FILE);
    let expected_matrix = render_engine_matrix(&load_bundle(&manifest)?)?;
    let matrix_path = generated.directory.join(MATRIX_FILE);
    let actual_matrix = fs::read(&matrix_path)
        .with_context(|| format!("failed to read {}", matrix_path.display()))?;
    if actual_matrix != expected_matrix.as_bytes() {
        bail!(
            "{} is stale; rerun `catalog-bench-contract engine-import write` with a new output directory",
            matrix_path.display()
        );
    }

    let mut expected_entries = expected_output_entries(generated.files.keys());
    expected_entries.insert(PathBuf::from(MATRIX_FILE), OutputEntryKind::File);
    let actual_entries = output_entries(&generated.directory)?;
    if actual_entries != expected_entries {
        bail!(
            "engine bundle {} contains a missing, unexpected, or nonregular entry",
            generated.directory.display()
        );
    }
    Ok(manifest)
}

fn generate(repository_root: &Path, review_path: &Path) -> Result<GeneratedEngineBundle> {
    let reviewed = validate_engine_result_review(repository_root, review_path)?;
    require_publication_sources(repository_root, &reviewed)?;
    let evidence = reviewed.evidence();
    let profile = evidence.contracts().profile();
    let scenario = evidence.contracts().scenario();

    let profile_artifact = artifact(
        PROFILE_COPY,
        "application/json",
        evidence.profile_bytes(),
        "Exact runnable stock-engine profile admitted by the review.",
    );
    let scenario_artifact = artifact(
        SCENARIO_COPY,
        "application/json",
        evidence.scenario_bytes(),
        "Exact common stock-engine interoperability scenario admitted by the review.",
    );
    let review_artifact = artifact(
        REVIEW_COPY,
        "application/json",
        reviewed.review_bytes(),
        "Reviewed live-run, environment, source-identity, and redaction metadata.",
    );

    let mut files = BTreeMap::from([
        (
            PathBuf::from(PROFILE_COPY),
            evidence.profile_bytes().to_vec(),
        ),
        (
            PathBuf::from(SCENARIO_COPY),
            evidence.scenario_bytes().to_vec(),
        ),
        (PathBuf::from(REVIEW_COPY), reviewed.review_bytes().to_vec()),
    ]);
    let mut transcript_artifacts = BTreeMap::new();
    for admitted in evidence.transcripts() {
        let transcript = admitted.transcript();
        let catalog = transcript.components.catalog.id.clone();
        let location = format!("{TRANSCRIPT_DIRECTORY}/{catalog}.json");
        let transcript_artifact = artifact(
            &location,
            "application/json",
            admitted.bytes(),
            &format!(
                "Canonical sanitized stock-engine transcript for {}.",
                transcript.components.catalog.name
            ),
        );
        files.insert(PathBuf::from(&location), admitted.bytes().to_vec());
        if transcript_artifacts
            .insert(catalog.clone(), transcript_artifact)
            .is_some()
        {
            bail!("duplicate admitted transcript for `{catalog}`");
        }
    }

    let mut result_artifacts = Vec::new();
    for admitted in evidence.transcripts() {
        let catalog = &admitted.transcript().components.catalog.id;
        let transcript_artifact = transcript_artifacts
            .get(catalog)
            .context("admitted transcript artifact map changed during generation")?;
        let result = build_result(
            &reviewed,
            admitted.transcript(),
            &profile_artifact,
            &scenario_artifact,
            transcript_artifact,
            &review_artifact,
        )?;
        let name = format!("{catalog}.json");
        let bytes = pretty_json(&result)?;
        result_artifacts.push(artifact(
            &name,
            "application/json",
            &bytes,
            &format!(
                "Stock-engine interoperability result for {}.",
                admitted.transcript().components.catalog.name
            ),
        ));
        files.insert(PathBuf::from(name), bytes);
    }

    let first = evidence
        .transcripts()
        .first()
        .context("reviewed engine evidence contains no transcripts")?;
    let summary = evidence.summary();
    let mut source_evidence = vec![review_artifact];
    source_evidence.extend(transcript_artifacts.into_values());
    let manifest = ResultBundleManifest {
        contract_version: ContractVersion::V1,
        kind: ManifestDocumentKind::Manifest,
        id: reviewed.bundle_id().into(),
        title: reviewed.title().to_owned(),
        created_at: reviewed.created_at().to_owned(),
        provenance: Provenance::LiveRun {
            runner: first.transcript().components.runner.id.clone(),
            sanitized_invocation: reviewed.sanitized_invocation().to_owned(),
            started_at: reviewed.started_at().to_owned(),
            completed_at: reviewed.completed_at().to_owned(),
        },
        profile: profile_artifact,
        scenarios: vec![scenario_artifact],
        results: result_artifacts,
        source_evidence,
        redaction: reviewed.redaction().clone(),
        extensions: BTreeMap::from([
            (
                "querygraph/fixture-id".to_owned(),
                serde_json::json!(evidence.fixture_id()),
            ),
            (
                "querygraph/interoperability".to_owned(),
                serde_json::json!({
                    "ranking": false,
                    "workflow": scenario.id,
                    "execution_order": "sequential",
                    "shared_docker_network": profile.platform.network
                }),
            ),
            (
                "querygraph/classifications".to_owned(),
                serde_json::json!({
                    "total": summary.total,
                    "pass": summary.pass,
                    "fail": summary.fail,
                    "fixture_collision": summary.fixture_collision
                }),
            ),
        ]),
    };
    manifest.validate()?;
    files.insert(PathBuf::from(MANIFEST_FILE), pretty_json(&manifest)?);

    Ok(GeneratedEngineBundle {
        directory: repository_root.join(reviewed.output_directory()),
        files,
    })
}

fn require_publication_sources(
    repository_root: &Path,
    reviewed: &ValidatedEngineResultReview,
) -> Result<()> {
    require_repository_subtree(
        repository_root,
        reviewed.review_path(),
        Path::new("results/source"),
        "review sidecar",
    )?;
    require_location_prefix(
        reviewed.profile_source_location(),
        "profiles/v1/",
        "reviewed profile",
    )?;
    require_location_prefix(
        reviewed.scenario_source_location(),
        "scenarios/v1/",
        "reviewed scenario",
    )?;
    for admitted in reviewed.evidence().transcripts() {
        let catalog = &admitted.transcript().components.catalog.id;
        let location = reviewed
            .transcript_source_location(catalog)
            .context("review omits an admitted transcript source")?;
        require_location_prefix(location, "results/source/", "reviewed transcript")?;
    }
    Ok(())
}

fn require_repository_subtree(
    repository_root: &Path,
    source: &Path,
    subtree: &Path,
    name: &str,
) -> Result<()> {
    let canonical_root = fs::canonicalize(repository_root).with_context(|| {
        format!(
            "failed to resolve repository root {}",
            repository_root.display()
        )
    })?;
    let canonical_source = fs::canonicalize(source)
        .with_context(|| format!("failed to resolve {name} {}", source.display()))?;
    let relative = canonical_source
        .strip_prefix(&canonical_root)
        .with_context(|| format!("{name} resolves outside the repository root"))?;
    if !relative.starts_with(subtree) {
        bail!(
            "{name} {} is not in the public `{}/` evidence boundary",
            source.display(),
            subtree.display()
        );
    }
    Ok(())
}

fn require_location_prefix(location: &str, prefix: &str, name: &str) -> Result<()> {
    if !location.starts_with(prefix) {
        bail!("{name} `{location}` is not in the public `{prefix}` evidence boundary");
    }
    Ok(())
}

fn build_result(
    reviewed: &ValidatedEngineResultReview,
    transcript: &EngineTranscript,
    profile_artifact: &ArtifactReference,
    scenario_artifact: &ArtifactReference,
    transcript_artifact: &ArtifactReference,
    review_artifact: &ArtifactReference,
) -> Result<ResultRecord> {
    let scenario = reviewed.evidence().contracts().scenario();
    let collision =
        transcript.execution.classification == EngineBehaviorClassification::FixtureCollision;
    let assertions = scenario
        .assertions
        .iter()
        .map(|assertion| evaluate_assertion(assertion, transcript, collision))
        .collect::<Result<Vec<_>>>()?;
    let failed_required = assertions
        .iter()
        .filter(|evaluation| {
            evaluation.required && matches!(evaluation.outcome, AssertionOutcome::Fail { .. })
        })
        .map(|evaluation| evaluation.assertion.to_string())
        .collect::<Vec<_>>();
    let outcome = result_outcome(transcript, &assertions, &failed_required)?;
    let catalog = &transcript.components.catalog;
    let engine = &transcript.components.engine;
    let connector = &transcript.components.connector;
    let evidence = vec![
        Evidence {
            id: TRANSCRIPT_EVIDENCE_ID.into(),
            kind: EvidenceKind::QueryOutput,
            artifact: transcript_artifact.clone(),
            sanitized: true,
            redactions: reviewed.redaction().removed_fields.clone(),
            extensions: BTreeMap::from([
                (
                    "querygraph/transcript-format".to_owned(),
                    serde_json::json!(&transcript.format),
                ),
                (
                    "querygraph/classification".to_owned(),
                    serde_json::json!(classification_label(transcript.execution.classification)),
                ),
            ]),
        },
        Evidence {
            id: REVIEW_EVIDENCE_ID.into(),
            kind: EvidenceKind::Other,
            artifact: review_artifact.clone(),
            sanitized: true,
            redactions: reviewed.redaction().removed_fields.clone(),
            extensions: BTreeMap::from([(
                "querygraph/reviewed".to_owned(),
                serde_json::json!(reviewed.redaction().reviewed),
            )]),
        },
    ];
    let result = ResultRecord {
        contract_version: ContractVersion::V1,
        kind: ResultDocumentKind::Result,
        id: ResultId::new(format!("{}-{}", reviewed.bundle_id(), catalog.id)),
        scenario: ScenarioReference {
            id: scenario.id.clone(),
            version: scenario.version,
            digest: scenario_artifact.digest.clone(),
        },
        profile: ProfileReference {
            id: reviewed.evidence().contracts().profile().id.clone(),
            digest: profile_artifact.digest.clone(),
        },
        catalog: executed_component(catalog),
        client: Some(executed_component(engine)),
        adapters: vec![connector.id.clone()],
        run: RunIdentity::Single {
            id: format!("{}-{}", transcript.fixture.id, catalog.id),
            started_at: reviewed.started_at().to_owned(),
            finished_at: reviewed.completed_at().to_owned(),
            repetition: 1,
            random_seed: None,
        },
        outcome,
        environment: reviewed.environment().clone(),
        assertions,
        measurements: Vec::new(),
        evidence,
        artifacts: Vec::new(),
        extensions: BTreeMap::from([
            (
                "querygraph/execution".to_owned(),
                serde_json::json!({
                    "classification": classification_label(transcript.execution.classification),
                    "process_outcome": process_outcome_label(&transcript.execution.process.outcome),
                    "exit_code": transcript.execution.process.exit_code,
                    "failed_required_assertions": failed_required
                }),
            ),
            (
                "querygraph/run-interval".to_owned(),
                serde_json::json!({
                    "scope": "complete-four-catalog-launcher",
                    "catalogs_executed_sequentially": true
                }),
            ),
            (
                "querygraph/request-handling".to_owned(),
                serde_json::json!({
                    "protocol": "iceberg-rest-v1",
                    "behavior_changing_shim": false,
                    "connector": connector.id
                }),
            ),
        ]),
    };
    result.validate()?;
    Ok(result)
}

fn executed_component(component: &EngineTranscriptComponent) -> ExecutedComponent {
    ExecutedComponent {
        profile_component: component.id.clone(),
        name: component.name.clone(),
        version: component.version.clone(),
    }
}

fn evaluate_assertion(
    assertion: &AssertionSpec,
    transcript: &EngineTranscript,
    collision: bool,
) -> Result<AssertionEvaluation> {
    let outcome = if collision {
        AssertionOutcome::NotEvaluated {
            reason: "A pre-existing fixture collision prevented a safe scenario attempt."
                .to_owned(),
        }
    } else if assertion_passed(assertion.id.as_str(), transcript)? {
        AssertionOutcome::Pass
    } else {
        AssertionOutcome::Fail {
            explanation: format!(
                "The independently recomputed `{}` transcript check did not pass.",
                assertion.id
            ),
        }
    };
    Ok(AssertionEvaluation {
        assertion: assertion.id.clone(),
        required: assertion.required,
        outcome,
        evidence: vec![TRANSCRIPT_EVIDENCE_ID.into()],
    })
}

fn assertion_passed(id: &str, transcript: &EngineTranscript) -> Result<bool> {
    let checks = &transcript.execution.checks;
    let passed = match id {
        "engine-runtime-pinned" => checks.engine_runtime_pinned,
        "stock-rest-catalog-ready" => checks.stock_rest_catalog_ready,
        "fixture-isolated" => checks.fixture_isolated,
        "namespace-round-trip" => checks.namespace_round_trip,
        "table-round-trip" => checks.table_round_trip,
        "initial-append-committed" => checks.initial_append_committed,
        "initial-read-exact" => checks.initial_read_exact,
        "schema-evolved" => checks.schema_evolved,
        "evolved-append-committed" => checks.evolved_append_committed,
        "evolved-read-exact" => checks.evolved_read_exact,
        "catalog-state-correlated" => checks.catalog_state_correlated,
        "shared-object-evidence-complete" => checks.shared_object_evidence_complete,
        "fixture-clean" => checks.fixture_clean,
        "transcript-sanitized" => transcript.sanitization.passed(),
        unknown => bail!("engine result materializer does not implement assertion `{unknown}`"),
    };
    Ok(passed)
}

fn result_outcome(
    transcript: &EngineTranscript,
    assertions: &[AssertionEvaluation],
    failed_required: &[String],
) -> Result<ResultOutcome> {
    let required = assertions
        .iter()
        .filter(|assertion| assertion.required)
        .count();
    match transcript.execution.classification {
        EngineBehaviorClassification::Pass => {
            if !failed_required.is_empty()
                || assertions.iter().any(|evaluation| {
                    evaluation.required && !matches!(evaluation.outcome, AssertionOutcome::Pass)
                })
            {
                bail!("passing engine transcript does not project to all passing assertions");
            }
            Ok(ResultOutcome::Pass {
                summary: Some(format!(
                    "All {required} required stock-engine interoperability assertions passed."
                )),
            })
        }
        EngineBehaviorClassification::Fail => {
            let (category, detail) = if failed_required.is_empty() {
                (
                    FailureCategory::Harness,
                    format!(
                        "All scenario checks were observed, but the closed process outcome `{}` did not establish a trusted successful terminal. No retryability claim is inferred.",
                        process_outcome_label(&transcript.execution.process.outcome)
                    ),
                )
            } else {
                (
                    FailureCategory::Assertion,
                    format!(
                        "Required assertions failed: {}. The transcript retains only bounded failure categories; no deeper cause or retryability claim is inferred.",
                        failed_required
                            .iter()
                            .map(|id| format!("`{id}`"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                )
            };
            Ok(ResultOutcome::Fail {
                failure: Failure {
                    category,
                    summary: "The stock-engine workflow did not satisfy its publication contract."
                        .to_owned(),
                    detail,
                    retryable: false,
                    evidence: vec![TRANSCRIPT_EVIDENCE_ID.into(), REVIEW_EVIDENCE_ID.into()],
                },
            })
        }
        EngineBehaviorClassification::FixtureCollision => Ok(ResultOutcome::NotTested {
            reason: NotTestedReason {
                explanation: "A pre-existing run-owned fixture was observed before mutation, so the runner refused to execute or clean it up.".to_owned(),
                blocking_dependency: Some("fresh collision-free fixture state".to_owned()),
            },
        }),
    }
}

fn classification_label(classification: EngineBehaviorClassification) -> &'static str {
    match classification {
        EngineBehaviorClassification::Pass => "pass",
        EngineBehaviorClassification::Fail => "fail",
        EngineBehaviorClassification::FixtureCollision => "fixture-collision",
    }
}

fn process_outcome_label(outcome: &EngineProcessOutcome) -> &'static str {
    match outcome {
        EngineProcessOutcome::RuntimeRejected {} => "runtime-rejected",
        EngineProcessOutcome::CredentialRejected { .. } => "credential-rejected",
        EngineProcessOutcome::PreparationFailed { .. } => "preparation-failed",
        EngineProcessOutcome::SpawnFailed {} => "spawn-failed",
        EngineProcessOutcome::TimedOut {} => "timed-out",
        EngineProcessOutcome::StdoutFailed {} => "stdout-failed",
        EngineProcessOutcome::WaitFailed {} => "wait-failed",
        EngineProcessOutcome::ProtocolRejected { .. } => "protocol-rejected",
        EngineProcessOutcome::ExitProtocolMismatch {} => "exit-protocol-mismatch",
        EngineProcessOutcome::Completed {} => "completed",
        EngineProcessOutcome::FixtureCollision {} => "fixture-collision",
        EngineProcessOutcome::EngineFailed { .. } => "engine-failed",
    }
}

fn write_new_file(root: &Path, relative: &Path, bytes: &[u8]) -> Result<()> {
    let path = root.join(relative);
    let parent = path.parent().context("generated file has no parent")?;
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| format!("refusing to replace {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("failed to write {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to synchronize {}", path.display()))?;
    Ok(())
}

fn create_output_parent(repository_root: &Path, parent: &Path) -> Result<()> {
    let relative = parent.strip_prefix(repository_root).with_context(|| {
        format!(
            "engine bundle parent {} escaped repository root {}",
            parent.display(),
            repository_root.display()
        )
    })?;
    let mut current = repository_root.to_owned();
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_dir() => {}
            Ok(_) => bail!(
                "engine bundle parent is not a real directory: {}",
                current.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)
                    .with_context(|| format!("failed to create {}", current.display()))?;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to inspect {}", current.display()));
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum OutputEntryKind {
    Directory,
    File,
}

fn expected_output_entries<'a>(
    files: impl Iterator<Item = &'a PathBuf>,
) -> BTreeMap<PathBuf, OutputEntryKind> {
    let mut entries = BTreeMap::new();
    for file in files {
        entries.insert(file.clone(), OutputEntryKind::File);
        let mut parent = file.parent();
        while let Some(directory) = parent.filter(|directory| !directory.as_os_str().is_empty()) {
            entries.insert(directory.to_owned(), OutputEntryKind::Directory);
            parent = directory.parent();
        }
    }
    entries
}

fn output_entries(root: &Path) -> Result<BTreeMap<PathBuf, OutputEntryKind>> {
    let metadata = fs::symlink_metadata(root)
        .with_context(|| format!("failed to inspect {}", root.display()))?;
    if !metadata.file_type().is_dir() {
        bail!(
            "engine bundle output is not a real directory: {}",
            root.display()
        );
    }
    let mut entries = BTreeMap::new();
    collect_output_entries(root, root, &mut entries)?;
    Ok(entries)
}

fn collect_output_entries(
    root: &Path,
    directory: &Path,
    entries: &mut BTreeMap<PathBuf, OutputEntryKind>,
) -> Result<()> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("failed to list {}", directory.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .context("generated output escaped its root")?
            .to_owned();
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to inspect {}", path.display()))?;
        let kind = if file_type.is_file() {
            OutputEntryKind::File
        } else if file_type.is_dir() {
            OutputEntryKind::Directory
        } else {
            bail!(
                "engine bundle entry is not a regular file or directory: {}",
                path.display()
            );
        };
        if entries.insert(relative, kind).is_some() {
            bail!("engine bundle contains a duplicate output entry");
        }
        if kind == OutputEntryKind::Directory {
            collect_output_entries(root, &path, entries)?;
        }
    }
    Ok(())
}
