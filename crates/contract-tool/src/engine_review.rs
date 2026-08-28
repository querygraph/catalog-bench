//! Reviewed live-run metadata bound to an admitted stock-engine evidence set.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{
    Captured, ComponentId, EnvironmentManifest, RedactionStatement, Validate,
};
use serde::Deserialize;

use crate::engine_evidence::{
    validate_engine_evidence_set, ValidatedEngineEvidenceSet, ValidatedEngineTranscript,
};
use crate::publication::{parse_utc_timestamp, require_text, ReviewBundle, ReviewSource};
use crate::sha256_hex;

const REVIEW_FORMAT: &str = "catalog-bench/engine-result-review/v1";
const MAX_REVIEW_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EngineResultReview {
    format: String,
    bundle: ReviewBundle,
    run: EngineRunReview,
    profile: ReviewSource,
    scenario: ReviewSource,
    transcripts: Vec<ReviewedTranscript>,
    environment: EnvironmentManifest,
    redaction: RedactionStatement,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EngineRunReview {
    fixture_id: String,
    sanitized_invocation: String,
    started_at: String,
    started_at_basis: String,
    completed_at: String,
    completed_at_basis: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewedTranscript {
    catalog: ComponentId,
    source: ReviewSource,
}

/// Validated reviewed metadata plus the exact independently admitted evidence.
#[derive(Debug)]
pub struct ValidatedEngineResultReview {
    review_path: PathBuf,
    review_bytes: Vec<u8>,
    review: EngineResultReview,
    evidence: ValidatedEngineEvidenceSet,
}

impl ValidatedEngineResultReview {
    #[must_use]
    pub fn review_path(&self) -> &Path {
        &self.review_path
    }

    #[must_use]
    pub fn review_bytes(&self) -> &[u8] {
        &self.review_bytes
    }

    #[must_use]
    pub fn evidence(&self) -> &ValidatedEngineEvidenceSet {
        &self.evidence
    }

    #[must_use]
    pub fn bundle_id(&self) -> &str {
        &self.review.bundle.id
    }

    #[must_use]
    pub fn title(&self) -> &str {
        &self.review.bundle.title
    }

    #[must_use]
    pub fn output_directory(&self) -> &str {
        &self.review.bundle.output_directory
    }

    #[must_use]
    pub fn created_at(&self) -> &str {
        &self.review.bundle.created_at
    }

    #[must_use]
    pub fn started_at(&self) -> &str {
        &self.review.run.started_at
    }

    #[must_use]
    pub fn completed_at(&self) -> &str {
        &self.review.run.completed_at
    }

    #[must_use]
    pub fn sanitized_invocation(&self) -> &str {
        &self.review.run.sanitized_invocation
    }

    #[must_use]
    pub fn environment(&self) -> &EnvironmentManifest {
        &self.review.environment
    }

    #[must_use]
    pub fn redaction(&self) -> &RedactionStatement {
        &self.review.redaction
    }

    pub(crate) fn profile_source_location(&self) -> &str {
        &self.review.profile.location
    }

    pub(crate) fn scenario_source_location(&self) -> &str {
        &self.review.scenario.location
    }

    pub(crate) fn transcript_source_location(&self, catalog: &ComponentId) -> Option<&str> {
        self.review
            .transcripts
            .iter()
            .find(|transcript| &transcript.catalog == catalog)
            .map(|transcript| transcript.source.location.as_str())
    }
}

/// Validate one bounded review and every source identity it claims.
pub fn validate_engine_result_review(
    repository_root: &Path,
    review_path: &Path,
) -> Result<ValidatedEngineResultReview> {
    let review_path = if review_path.is_absolute() {
        review_path.to_owned()
    } else {
        repository_root.join(review_path)
    };
    let review_bytes = read_bounded_review(&review_path)?;
    let review: EngineResultReview = serde_json::from_slice(&review_bytes).with_context(|| {
        format!(
            "invalid engine result review JSON in {}",
            review_path.display()
        )
    })?;
    if review_bytes.last() != Some(&b'\n') {
        bail!(
            "engine result review is not newline-terminated: {}",
            review_path.display()
        );
    }
    validate_review_shape(&review)?;

    let canonical_root = fs::canonicalize(repository_root).with_context(|| {
        format!(
            "failed to resolve repository root {}",
            repository_root.display()
        )
    })?;
    let profile_path = resolve_source(repository_root, &review.profile, "profile")?;
    let scenario_path = resolve_source(repository_root, &review.scenario, "scenario")?;
    let evidence_directory = reviewed_transcript_directory(repository_root, &review.transcripts)?;
    require_within_repository(&canonical_root, &profile_path, "profile source")?;
    require_within_repository(&canonical_root, &scenario_path, "scenario source")?;
    require_within_repository(
        &canonical_root,
        &evidence_directory,
        "transcript source directory",
    )?;
    let evidence = validate_engine_evidence_set(
        &profile_path,
        &scenario_path,
        &evidence_directory,
        &review.run.fixture_id,
    )?;
    validate_reviewed_invocation(&review, &evidence)?;

    verify_source_bytes(&review.profile, evidence.profile_bytes(), "profile")?;
    verify_source_bytes(&review.scenario, evidence.scenario_bytes(), "scenario")?;
    verify_transcript_sources(repository_root, &review.transcripts, &evidence)?;
    verify_environment(&review.environment, &evidence)?;

    Ok(ValidatedEngineResultReview {
        review_path,
        review_bytes,
        review,
        evidence,
    })
}

fn require_within_repository(canonical_root: &Path, path: &Path, name: &str) -> Result<()> {
    let canonical = fs::canonicalize(path)
        .with_context(|| format!("failed to resolve {name} {}", path.display()))?;
    if !canonical.starts_with(canonical_root) {
        bail!(
            "{name} resolves outside the repository root: {}",
            path.display()
        );
    }
    Ok(())
}

fn read_bounded_review(path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect engine result review {}", path.display()))?;
    if !metadata.file_type().is_file() {
        bail!(
            "engine result review is not a regular file: {}",
            path.display()
        );
    }
    if metadata.len() == 0 || metadata.len() > MAX_REVIEW_BYTES {
        bail!(
            "engine result review {} has {} bytes; expected 1 to {MAX_REVIEW_BYTES}",
            path.display(),
            metadata.len()
        );
    }
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read engine result review {}", path.display()))?;
    if bytes.len() as u64 != metadata.len() {
        bail!(
            "engine result review changed while reading: {}",
            path.display()
        );
    }
    Ok(bytes)
}

fn validate_review_shape(review: &EngineResultReview) -> Result<()> {
    if review.format != REVIEW_FORMAT {
        bail!("engine result review has an unsupported format");
    }
    require_text(&review.bundle.id, "bundle id")?;
    require_text(&review.bundle.title, "bundle title")?;
    validate_result_output_directory(&review.bundle.output_directory)?;
    require_text(&review.run.fixture_id, "run fixture id")?;
    require_text(&review.run.started_at_basis, "run started_at basis")?;
    require_text(&review.run.completed_at_basis, "run completed_at basis")?;

    let created_at = parse_utc_timestamp(&review.bundle.created_at, "bundle created_at")?;
    let started_at = parse_utc_timestamp(&review.run.started_at, "run started_at")?;
    let completed_at = parse_utc_timestamp(&review.run.completed_at, "run completed_at")?;
    if started_at >= completed_at || completed_at >= created_at {
        bail!("reviewed run and bundle timestamps are not strictly ordered");
    }

    validate_source(&review.profile, "profile source")?;
    validate_source(&review.scenario, "scenario source")?;
    validate_transcript_review_order(&review.transcripts)?;
    review
        .environment
        .validate()
        .context("invalid reviewed engine environment")?;
    if !review.redaction.reviewed {
        bail!("engine result review has not completed redaction review");
    }
    require_text(&review.redaction.policy, "redaction policy")?;
    if review.redaction.removed_fields.is_empty() {
        bail!("redaction review must name at least one removed field category");
    }
    let mut removed_fields = BTreeSet::new();
    for field in &review.redaction.removed_fields {
        require_text(field, "removed field category")?;
        if !removed_fields.insert(field) {
            bail!("redaction review contains a duplicate removed field category");
        }
    }
    Ok(())
}

fn validate_reviewed_invocation(
    review: &EngineResultReview,
    evidence: &ValidatedEngineEvidenceSet,
) -> Result<()> {
    let engine_name = &evidence.transcripts()[0]
        .transcript()
        .components
        .engine
        .name;
    let launcher = match engine_name.as_str() {
        "Apache Spark" => "docker/run-spark-interoperability.sh",
        "Apache Flink" => "docker/run-flink-interoperability.sh",
        _ => bail!("reviewed engine has no canonical interoperability launcher"),
    };
    let expected = format!("{launcher} \"{}\"", review.run.fixture_id);
    if review.run.sanitized_invocation != expected {
        bail!("reviewed invocation does not match the canonical engine launcher");
    }
    Ok(())
}

fn validate_transcript_review_order(transcripts: &[ReviewedTranscript]) -> Result<()> {
    if transcripts.is_empty() {
        bail!("engine result review contains no transcripts");
    }
    let mut previous: Option<&ComponentId> = None;
    for transcript in transcripts {
        if previous.is_some_and(|catalog| catalog >= &transcript.catalog) {
            bail!("reviewed transcript catalogs must be unique and strictly sorted");
        }
        previous = Some(&transcript.catalog);
        validate_source(
            &transcript.source,
            &format!("transcript source for {}", transcript.catalog),
        )?;
        let path = Path::new(&transcript.source.location);
        let expected_name = format!("{}.json", transcript.catalog);
        if path.file_name().and_then(|name| name.to_str()) != Some(expected_name.as_str()) {
            bail!(
                "reviewed transcript for `{}` must be named `{expected_name}`",
                transcript.catalog
            );
        }
    }
    Ok(())
}

fn validate_source(source: &ReviewSource, name: &str) -> Result<()> {
    relative_path(&source.location, name)?;
    if source.bytes == 0 {
        bail!("{name} byte count must be greater than zero");
    }
    if source.sha256.len() != 64
        || !source
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("{name} SHA-256 must contain 64 lowercase hexadecimal characters");
    }
    Ok(())
}

fn relative_path<'a>(location: &'a str, name: &str) -> Result<&'a Path> {
    require_text(location, name)?;
    if location.contains(['\\', ':']) || location.bytes().any(|byte| byte.is_ascii_control()) {
        bail!("{name} must be a portable repository-relative path");
    }
    if location
        .split('/')
        .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        bail!("{name} must be a normalized repository-relative path");
    }
    Ok(Path::new(location))
}

fn validate_result_output_directory(location: &str) -> Result<()> {
    relative_path(location, "bundle output directory")?;
    let mut components = location.split('/');
    if components.next() != Some("results")
        || components.next() != Some("v1")
        || components.next().is_none()
    {
        bail!("bundle output directory must be below results/v1");
    }
    Ok(())
}

fn resolve_source(repository_root: &Path, source: &ReviewSource, name: &str) -> Result<PathBuf> {
    let relative = relative_path(&source.location, name)?;
    Ok(repository_root.join(relative))
}

fn reviewed_transcript_directory(
    repository_root: &Path,
    transcripts: &[ReviewedTranscript],
) -> Result<PathBuf> {
    let first = transcripts
        .first()
        .context("engine result review contains no transcripts")?;
    let parent = Path::new(&first.source.location)
        .parent()
        .context("reviewed transcript has no parent directory")?;
    for transcript in &transcripts[1..] {
        if Path::new(&transcript.source.location).parent() != Some(parent) {
            bail!("all reviewed engine transcripts must share one evidence directory");
        }
    }
    Ok(repository_root.join(parent))
}

fn verify_source_bytes(source: &ReviewSource, bytes: &[u8], name: &str) -> Result<()> {
    let actual_sha256 = sha256_hex(bytes);
    if source.bytes != bytes.len() as u64 || source.sha256 != actual_sha256 {
        bail!(
            "reviewed {name} identity differs from admitted bytes: expected {}/{}, got {}/{}",
            source.sha256,
            source.bytes,
            actual_sha256,
            bytes.len()
        );
    }
    Ok(())
}

fn verify_transcript_sources(
    repository_root: &Path,
    reviewed: &[ReviewedTranscript],
    evidence: &ValidatedEngineEvidenceSet,
) -> Result<()> {
    let reviewed = reviewed
        .iter()
        .map(|transcript| (&transcript.catalog, transcript))
        .collect::<BTreeMap<_, _>>();
    if reviewed.len() != evidence.transcripts().len() {
        bail!("reviewed transcripts do not cover the admitted catalog set");
    }
    for transcript in evidence.transcripts() {
        let catalog = &transcript.transcript().components.catalog.id;
        let source = reviewed
            .get(catalog)
            .with_context(|| format!("review omits admitted transcript for `{catalog}`"))?;
        let expected_path = resolve_source(repository_root, &source.source, "transcript source")?;
        if transcript.path() != expected_path {
            bail!("reviewed transcript path differs from admitted path for `{catalog}`");
        }
        verify_admitted_transcript_source(&source.source, transcript, catalog)?;
    }
    Ok(())
}

fn verify_admitted_transcript_source(
    source: &ReviewSource,
    transcript: &ValidatedEngineTranscript,
    catalog: &ComponentId,
) -> Result<()> {
    verify_source_bytes(
        source,
        transcript.bytes(),
        &format!("transcript for `{catalog}`"),
    )
}

fn verify_environment(
    environment: &EnvironmentManifest,
    evidence: &ValidatedEngineEvidenceSet,
) -> Result<()> {
    let platform = &evidence.contracts().profile().platform;
    if environment.operating_system != platform.operating_system
        || environment.architecture != platform.architecture
        || environment.network != platform.network
    {
        bail!("reviewed environment differs from the runnable profile platform");
    }
    if !matches!(environment.container_runtime, Captured::Exact { .. }) {
        bail!("live engine review requires an exact container runtime capture");
    }
    Ok(())
}
