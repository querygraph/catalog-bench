use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{
    ArtifactReference, AssertionSpec, Component, ComponentId, ComponentKind, ContractDocument,
    Digest, DigestAlgorithm, ExecutedComponent, Profile, ProfileReadiness, ResultBundleManifest,
    ResultOutcome, ResultRecord, Scenario,
};

use crate::sha256_hex;

#[derive(Debug)]
pub struct ValidatedScenario {
    artifact: ArtifactReference,
    path: PathBuf,
    scenario: Scenario,
}

#[derive(Debug)]
pub struct ValidatedResult {
    artifact: ArtifactReference,
    path: PathBuf,
    result: ResultRecord,
}

#[derive(Debug)]
pub struct ValidatedBundle {
    manifest_path: PathBuf,
    manifest: ResultBundleManifest,
    profile_path: PathBuf,
    profile: Profile,
    scenarios: Vec<ValidatedScenario>,
    results: Vec<ValidatedResult>,
}

impl ValidatedScenario {
    pub fn artifact(&self) -> &ArtifactReference {
        &self.artifact
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn scenario(&self) -> &Scenario {
        &self.scenario
    }
}

impl ValidatedResult {
    pub fn artifact(&self) -> &ArtifactReference {
        &self.artifact
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn result(&self) -> &ResultRecord {
        &self.result
    }
}

impl ValidatedBundle {
    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    pub fn manifest(&self) -> &ResultBundleManifest {
        &self.manifest
    }

    pub fn profile_path(&self) -> &Path {
        &self.profile_path
    }

    pub fn profile(&self) -> &Profile {
        &self.profile
    }

    pub fn scenarios(&self) -> &[ValidatedScenario] {
        &self.scenarios
    }

    pub fn results(&self) -> &[ValidatedResult] {
        &self.results
    }
}

/// Validate exact artifact bytes and every cross-document reference in a bundle.
pub fn load_bundle(manifest_path: &Path) -> Result<ValidatedBundle> {
    let manifest_bytes = fs::read(manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let manifest = match catalog_bench_common::contract::parse_contract(&manifest_bytes)
        .with_context(|| format!("invalid manifest {}", manifest_path.display()))?
    {
        ContractDocument::Manifest(manifest) => manifest,
        document => bail!(
            "{} is {}, not a manifest",
            manifest_path.display(),
            document.kind()
        ),
    };
    let base = manifest_path
        .parent()
        .context("manifest path has no parent directory")?;

    let (profile_path, profile_bytes) = read_verified_artifact(base, &manifest.profile)?;
    let profile = match catalog_bench_common::contract::parse_contract(&profile_bytes)
        .with_context(|| format!("invalid profile {}", profile_path.display()))?
    {
        ContractDocument::Profile(profile) => profile,
        document => bail!(
            "{} is {}, not a profile",
            profile_path.display(),
            document.kind()
        ),
    };
    if !matches!(profile.readiness, ProfileReadiness::Runnable) {
        bail!(
            "bundle profile {} is draft; results require a runnable profile",
            profile.id
        );
    }

    let mut scenarios = Vec::new();
    let mut scenario_ids = BTreeSet::new();
    for artifact in &manifest.scenarios {
        let (path, bytes) = read_verified_artifact(base, artifact)?;
        let scenario = match catalog_bench_common::contract::parse_contract(&bytes)
            .with_context(|| format!("invalid scenario {}", path.display()))?
        {
            ContractDocument::Scenario(scenario) => scenario,
            document => bail!("{} is {}, not a scenario", path.display(), document.kind()),
        };
        if !scenario_ids.insert(scenario.id.clone()) {
            bail!("manifest contains duplicate scenario `{}`", scenario.id);
        }
        scenarios.push(ValidatedScenario {
            artifact: artifact.clone(),
            path,
            scenario,
        });
    }

    for artifact in &manifest.source_evidence {
        read_verified_artifact(base, artifact)?;
    }

    let components: BTreeMap<_, _> = profile
        .components
        .iter()
        .map(|component| (&component.id, component))
        .collect();
    let mut results = Vec::new();
    let mut result_ids = BTreeSet::new();
    for artifact in &manifest.results {
        let (path, bytes) = read_verified_artifact(base, artifact)?;
        let result = match catalog_bench_common::contract::parse_contract(&bytes)
            .with_context(|| format!("invalid result {}", path.display()))?
        {
            ContractDocument::Result(result) => *result,
            document => bail!("{} is {}, not a result", path.display(), document.kind()),
        };
        if !result_ids.insert(result.id.clone()) {
            bail!("manifest contains duplicate result `{}`", result.id);
        }
        validate_result_links(
            &result,
            &manifest.profile.digest,
            &profile,
            &components,
            &scenarios,
        )
        .with_context(|| format!("invalid links in {}", path.display()))?;
        for evidence in &result.evidence {
            read_verified_artifact(base, &evidence.artifact).with_context(|| {
                format!("invalid evidence `{}` in {}", evidence.id, path.display())
            })?;
        }
        for result_artifact in &result.artifacts {
            read_verified_artifact(base, result_artifact)
                .with_context(|| format!("invalid result artifact in {}", path.display()))?;
        }
        results.push(ValidatedResult {
            artifact: artifact.clone(),
            path,
            result,
        });
    }

    Ok(ValidatedBundle {
        manifest_path: manifest_path.to_owned(),
        manifest,
        profile_path,
        profile,
        scenarios,
        results,
    })
}

fn read_verified_artifact(base: &Path, artifact: &ArtifactReference) -> Result<(PathBuf, Vec<u8>)> {
    if artifact.location.contains("://") || artifact.location.starts_with("image:") {
        bail!(
            "bundle artifact `{}` is not a local file",
            artifact.location
        );
    }
    let path = base.join(&artifact.location);
    let bytes = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
    if let Some(expected) = artifact.bytes {
        if bytes.len() as u64 != expected {
            bail!(
                "{} has {} bytes, expected {expected}",
                path.display(),
                bytes.len()
            );
        }
    }
    verify_digest(&bytes, &artifact.digest)
        .with_context(|| format!("digest mismatch for {}", path.display()))?;
    Ok((path, bytes))
}

fn verify_digest(bytes: &[u8], expected: &Digest) -> Result<()> {
    let actual = match expected.algorithm {
        DigestAlgorithm::Sha256 => sha256_hex(bytes),
    };
    if actual != expected.value {
        bail!("expected {}, got {actual}", expected.value);
    }
    Ok(())
}

fn validate_result_links(
    result: &ResultRecord,
    profile_digest: &Digest,
    profile: &Profile,
    components: &BTreeMap<&ComponentId, &Component>,
    scenarios: &[ValidatedScenario],
) -> Result<()> {
    if result.profile.id != profile.id || result.profile.digest != *profile_digest {
        bail!("profile reference does not match the manifest profile");
    }
    validate_executed_component(&result.catalog, ComponentKind::Catalog, components)?;
    if let Some(client) = &result.client {
        let component = components
            .get(&client.profile_component)
            .with_context(|| format!("unknown client component `{}`", client.profile_component))?;
        if !matches!(
            component.kind,
            ComponentKind::Client | ComponentKind::Engine
        ) {
            bail!(
                "client component `{}` has incompatible kind {:?}",
                client.profile_component,
                component.kind
            );
        }
        validate_component_identity(client, component)?;
    }
    for adapter in &result.adapters {
        if !components.contains_key(adapter) {
            bail!("unknown adapter component `{adapter}`");
        }
    }

    let scenario = scenarios
        .iter()
        .find(|candidate| candidate.scenario.id == result.scenario.id)
        .with_context(|| format!("unknown scenario `{}`", result.scenario.id))?;
    if result.scenario.version != scenario.scenario.version
        || result.scenario.digest != scenario.artifact.digest
    {
        bail!("scenario version or digest does not match the manifest artifact");
    }

    let assertions: BTreeMap<_, _> = scenario
        .scenario
        .assertions
        .iter()
        .map(|assertion| (&assertion.id, assertion))
        .collect();
    for evaluation in &result.assertions {
        let assertion = assertions
            .get(&evaluation.assertion)
            .with_context(|| format!("unknown scenario assertion `{}`", evaluation.assertion))?;
        if evaluation.required != assertion.required {
            bail!(
                "assertion `{}` copied required={} but scenario says {}",
                evaluation.assertion,
                evaluation.required,
                assertion.required
            );
        }
    }
    if matches!(
        &result.outcome,
        ResultOutcome::Pass { .. } | ResultOutcome::Fail { .. }
    ) {
        require_complete_assertions(result, &assertions)?;
    }
    Ok(())
}

fn validate_executed_component(
    executed: &ExecutedComponent,
    expected_kind: ComponentKind,
    components: &BTreeMap<&ComponentId, &Component>,
) -> Result<()> {
    let component = components
        .get(&executed.profile_component)
        .with_context(|| format!("unknown component `{}`", executed.profile_component))?;
    if component.kind != expected_kind {
        bail!(
            "component `{}` has kind {:?}, expected {:?}",
            executed.profile_component,
            component.kind,
            expected_kind
        );
    }
    validate_component_identity(executed, component)
}

fn validate_component_identity(executed: &ExecutedComponent, component: &Component) -> Result<()> {
    if executed.name != component.name || executed.version != component.version {
        bail!(
            "component `{}` identity differs from profile: result has {} {}, profile has {} {}",
            executed.profile_component,
            executed.name,
            executed.version,
            component.name,
            component.version
        );
    }
    Ok(())
}

fn require_complete_assertions(
    result: &ResultRecord,
    assertions: &BTreeMap<&catalog_bench_common::contract::AssertionId, &AssertionSpec>,
) -> Result<()> {
    let evaluated: BTreeSet<_> = result
        .assertions
        .iter()
        .map(|evaluation| &evaluation.assertion)
        .collect();
    let missing = assertions
        .keys()
        .filter(|assertion| !evaluated.contains(**assertion))
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!("attempted result omits assertions: {}", missing.join(", "));
    }
    Ok(())
}
