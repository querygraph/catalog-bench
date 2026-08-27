//! Independent admission of a complete stock-engine transcript set.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::ComponentId;
use catalog_bench_conformance::encode_evidence;
use catalog_bench_engine::{EngineBehaviorClassification, EngineContracts, EngineTranscript};

const MAX_TRANSCRIPT_BYTES: u64 = 4 * 1024 * 1024;

/// Counts derived from independently validated transcript classifications.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineEvidenceSummary {
    pub total: usize,
    pub pass: usize,
    pub fail: usize,
    pub fixture_collision: usize,
}

/// One bounded transcript admitted against exact contract bytes.
#[derive(Debug)]
pub struct ValidatedEngineTranscript {
    path: PathBuf,
    bytes: Vec<u8>,
    transcript: EngineTranscript,
}

impl ValidatedEngineTranscript {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[must_use]
    pub fn transcript(&self) -> &EngineTranscript {
        &self.transcript
    }
}

/// Exact profile, scenario, fixture, and complete catalog transcript set.
#[derive(Debug)]
pub struct ValidatedEngineEvidenceSet {
    contracts: EngineContracts,
    profile_bytes: Vec<u8>,
    scenario_bytes: Vec<u8>,
    fixture_id: String,
    transcripts: Vec<ValidatedEngineTranscript>,
}

impl ValidatedEngineEvidenceSet {
    #[must_use]
    pub fn contracts(&self) -> &EngineContracts {
        &self.contracts
    }

    #[must_use]
    pub fn profile_bytes(&self) -> &[u8] {
        &self.profile_bytes
    }

    #[must_use]
    pub fn scenario_bytes(&self) -> &[u8] {
        &self.scenario_bytes
    }

    #[must_use]
    pub fn fixture_id(&self) -> &str {
        &self.fixture_id
    }

    #[must_use]
    pub fn transcripts(&self) -> &[ValidatedEngineTranscript] {
        &self.transcripts
    }

    #[must_use]
    pub fn summary(&self) -> EngineEvidenceSummary {
        let mut summary = EngineEvidenceSummary {
            total: self.transcripts.len(),
            pass: 0,
            fail: 0,
            fixture_collision: 0,
        };
        for evidence in &self.transcripts {
            match evidence.transcript.execution.classification {
                EngineBehaviorClassification::Pass => summary.pass += 1,
                EngineBehaviorClassification::Fail => summary.fail += 1,
                EngineBehaviorClassification::FixtureCollision => {
                    summary.fixture_collision += 1;
                }
            }
        }
        summary
    }
}

/// Admit exactly one transcript for every catalog adapter selected by a profile.
pub fn validate_engine_evidence_set(
    profile_path: &Path,
    scenario_path: &Path,
    evidence_directory: &Path,
    fixture_id: &str,
) -> Result<ValidatedEngineEvidenceSet> {
    let profile_bytes = read_contract(profile_path, "profile")?;
    let scenario_bytes = read_contract(scenario_path, "scenario")?;
    let contracts = EngineContracts::parse(&profile_bytes, &scenario_bytes)
        .context("invalid engine interoperability contracts")?;
    let expected = expected_transcript_files(&contracts)?;
    let actual = transcript_files(evidence_directory)?;
    require_exact_file_set(&expected, &actual, evidence_directory)?;

    let mut transcripts = Vec::with_capacity(expected.len());
    for (file_name, catalog) in expected {
        let path = actual
            .get(&file_name)
            .context("exact transcript set changed during admission")?;
        let bytes = read_bounded_transcript(path)?;
        let transcript: EngineTranscript = serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid engine transcript JSON in {}", path.display()))?;
        if transcript.components.catalog.id != catalog {
            bail!(
                "{} contains catalog `{}`, expected `{catalog}`",
                path.display(),
                transcript.components.catalog.id
            );
        }
        if transcript.fixture.id != fixture_id {
            bail!(
                "{} contains fixture `{}`, expected `{fixture_id}`",
                path.display(),
                transcript.fixture.id
            );
        }
        let canonical = encode_evidence(&transcript)
            .with_context(|| format!("failed to re-encode engine transcript {}", path.display()))?;
        if bytes != canonical {
            bail!(
                "engine transcript is not in canonical newline-terminated encoding: {}",
                path.display()
            );
        }
        transcript
            .validate(&contracts)
            .with_context(|| format!("invalid engine transcript {}", path.display()))?;
        transcripts.push(ValidatedEngineTranscript {
            path: path.clone(),
            bytes,
            transcript,
        });
    }

    Ok(ValidatedEngineEvidenceSet {
        contracts,
        profile_bytes,
        scenario_bytes,
        fixture_id: fixture_id.to_owned(),
        transcripts,
    })
}

fn read_contract(path: &Path, kind: &str) -> Result<Vec<u8>> {
    fs::read(path).with_context(|| format!("failed to read engine {kind} {}", path.display()))
}

fn expected_transcript_files(contracts: &EngineContracts) -> Result<BTreeMap<String, ComponentId>> {
    let mut expected = BTreeMap::new();
    for adapter in &contracts.profile().catalog_adapters {
        let name = format!("{}.json", adapter.catalog);
        if expected.insert(name, adapter.catalog.clone()).is_some() {
            bail!(
                "profile contains duplicate catalog adapter `{}`",
                adapter.catalog
            );
        }
    }
    if expected.is_empty() {
        bail!("profile selects no catalog adapters");
    }
    Ok(expected)
}

fn transcript_files(directory: &Path) -> Result<BTreeMap<String, PathBuf>> {
    let metadata = fs::symlink_metadata(directory).with_context(|| {
        format!(
            "failed to inspect evidence directory {}",
            directory.display()
        )
    })?;
    if !metadata.file_type().is_dir() {
        bail!(
            "engine evidence path is not a directory: {}",
            directory.display()
        );
    }

    let mut files = BTreeMap::new();
    for entry in fs::read_dir(directory)
        .with_context(|| format!("failed to list evidence directory {}", directory.display()))?
    {
        let entry =
            entry.with_context(|| format!("failed to read entry in {}", directory.display()))?;
        let path = entry.path();
        if !entry
            .file_type()
            .with_context(|| format!("failed to inspect {}", path.display()))?
            .is_file()
        {
            bail!(
                "engine evidence entry is not a regular file: {}",
                path.display()
            );
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("engine evidence file name is not UTF-8"))?;
        if files.insert(name.clone(), path).is_some() {
            bail!("duplicate engine evidence file name `{name}`");
        }
    }
    Ok(files)
}

fn require_exact_file_set(
    expected: &BTreeMap<String, ComponentId>,
    actual: &BTreeMap<String, PathBuf>,
    directory: &Path,
) -> Result<()> {
    let missing = expected
        .keys()
        .filter(|name| !actual.contains_key(*name))
        .cloned()
        .collect::<Vec<_>>();
    let unexpected = actual
        .keys()
        .filter(|name| !expected.contains_key(*name))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() || !unexpected.is_empty() {
        bail!(
            "engine evidence set {} differs from profile: missing [{}], unexpected [{}]",
            directory.display(),
            missing.join(", "),
            unexpected.join(", ")
        );
    }
    Ok(())
}

fn read_bounded_transcript(path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("failed to inspect engine transcript {}", path.display()))?;
    if metadata.len() == 0 || metadata.len() > MAX_TRANSCRIPT_BYTES {
        bail!(
            "engine transcript {} has {} bytes; expected 1 to {MAX_TRANSCRIPT_BYTES}",
            path.display(),
            metadata.len()
        );
    }
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read engine transcript {}", path.display()))?;
    if bytes.len() as u64 != metadata.len() {
        bail!(
            "engine transcript changed while reading: {}",
            path.display()
        );
    }
    Ok(bytes)
}
