//! Deterministic materialization of the runnable contention profile.
//!
//! The checked-in current profile is a broad version-selection document. This
//! module narrows it to the same-table contention topology and replaces the
//! three source-built runtime artifacts with audited local-image observations.
//! The source profile digest makes the transformation fail closed if its input
//! drifts; the resulting profile remains an ordinary validated v1 contract.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{
    parse_contract, ArtifactReference, ComponentId, ContractDocument, Digest, DigestAlgorithm,
    ImageDigestScope, Profile, ProfileId, ProfilePurpose, ProfileReadiness, RuntimeArtifact,
    Validate,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::sha256_hex;

const MATERIALIZATION_FORMAT: &str = "catalog-bench/contention-profile-materialization/v1";
const CONTENTION_SCOPE: &str = "iceberg-rest.commit.same-table-contention/v2";
const MATERIALIZED_COMPONENTS: [&str; 10] = [
    "rust-runner",
    "catalog-bench-commit",
    "minio",
    "lakecat",
    "lakecat-turso",
    "polaris",
    "gravitino",
    "lakekeeper",
    "nessie",
    "postgresql",
];
const MATERIALIZED_IMAGES: [(&str, &str, &[&str]); 3] = [
    (
        "catalog-bench-commit",
        "bench",
        &["image:/usr/local/bin/catalog-bench-commit"],
    ),
    (
        "minio",
        "minio",
        &[
            "image:/usr/local/bin/minio",
            "image:/usr/local/bin/ensure-bucket",
            "image:/usr/local/bin/healthcheck",
            "image:/usr/local/bin/lakekeeper-setup",
            "image:/usr/local/bin/polaris-setup",
            "image:/usr/local/bin/wait-http",
        ],
    ),
    (
        "lakecat",
        "lakecat",
        &["image:/usr/local/bin/lakecat-service"],
    ),
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContentionProfileMaterialization {
    format: String,
    source_profile: SourceProfileObservation,
    output_profile: OutputProfile,
    images: Vec<ImageObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceProfileObservation {
    id: ProfileId,
    digest: Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputProfile {
    id: ProfileId,
    title: String,
    description: String,
    resolved_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ImageObservation {
    component: ComponentId,
    reference: String,
    image_id: Digest,
    operating_system: String,
    architecture: String,
    labels: BTreeMap<String, String>,
    embedded_artifacts: Vec<ArtifactReference>,
}

/// Render a scenario-scoped runnable profile from a broader draft and audited
/// local-image observations.
///
/// # Errors
///
/// Returns an error when either input is malformed, the source profile has
/// drifted from its recorded digest, an image observation does not match the
/// selected component and platform, or the derived profile violates the v1
/// contract.
pub fn render_contention_profile(
    source_profile_bytes: &[u8],
    materialization_bytes: &[u8],
) -> Result<Vec<u8>> {
    let mut profile = decode_profile(source_profile_bytes)?;
    let materialization: ContentionProfileMaterialization =
        serde_json::from_slice(materialization_bytes)
            .context("invalid contention profile materialization")?;
    validate_materialization(&profile, source_profile_bytes, &materialization)?;

    let selected = MATERIALIZED_COMPONENTS.into_iter().collect::<BTreeSet<_>>();
    profile
        .components
        .retain(|component| selected.contains(component.id.as_str()));
    profile
        .services
        .retain(|service| selected.contains(service.component.as_str()));

    let observations = materialization
        .images
        .iter()
        .map(|image| (image.component.as_str(), image))
        .collect::<BTreeMap<_, _>>();
    for component in &mut profile.components {
        let Some(observation) = observations.get(component.id.as_str()) else {
            continue;
        };
        component.artifact = RuntimeArtifact::ContainerImage {
            reference: observation.reference.clone(),
            digest_scope: ImageDigestScope::LocalImage,
            digest: observation.image_id.clone(),
            platform_digest: None,
            embedded_artifacts: observation.embedded_artifacts.clone(),
        };
        component.extensions.insert(
            "querygraph/materialized-image-observation".to_owned(),
            json!({
                "operating_system": observation.operating_system,
                "architecture": observation.architecture,
                "labels": observation.labels,
            }),
        );
    }

    profile.id = materialization.output_profile.id.clone();
    profile.title = materialization.output_profile.title.clone();
    profile.description = materialization.output_profile.description.clone();
    profile.resolved_at = materialization.output_profile.resolved_at.clone();
    profile.purpose = ProfilePurpose::Performance;
    profile.readiness = ProfileReadiness::Runnable;
    profile.extensions.insert(
        "querygraph/profile-state".to_owned(),
        Value::String(
            "Runnable scenario-scoped profile; every selected runtime artifact has an immutable identity."
                .to_owned(),
        ),
    );
    profile.extensions.insert(
        "querygraph/materialization".to_owned(),
        json!({
            "format": MATERIALIZATION_FORMAT,
            "scope": CONTENTION_SCOPE,
            "source_profile": {
                "id": materialization.source_profile.id,
                "digest": materialization.source_profile.digest,
            },
            "observation_sha256": sha256_hex(materialization_bytes),
        }),
    );

    profile
        .validate()
        .context("materialized contention profile is invalid")?;
    let mut rendered = serde_json::to_vec_pretty(&profile)?;
    rendered.push(b'\n');
    Ok(rendered)
}

/// Write a deterministically materialized contention profile to `output`.
///
/// # Errors
///
/// Returns an error when either input cannot be read or validated, the output
/// directory cannot be created, or the rendered profile cannot be written.
pub fn write_contention_profile(
    source_profile: &Path,
    materialization: &Path,
    output: &Path,
) -> Result<()> {
    let rendered = render_from_paths(source_profile, materialization)?;
    if let Some(parent) = output.parent().filter(|path| !path.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(output, rendered).with_context(|| format!("failed to write {}", output.display()))?;
    Ok(())
}

/// Verify that a checked-in contention profile exactly matches its two inputs.
///
/// # Errors
///
/// Returns an error when an input cannot be read or validated, or when `output`
/// is not byte-for-byte equal to a fresh deterministic materialization.
pub fn check_contention_profile(
    source_profile: &Path,
    materialization: &Path,
    output: &Path,
) -> Result<()> {
    let expected = render_from_paths(source_profile, materialization)?;
    let actual =
        fs::read(output).with_context(|| format!("failed to read {}", output.display()))?;
    if actual != expected {
        bail!(
            "{} is stale; rerun `catalog-bench-contract profile materialize-contention`",
            output.display()
        );
    }
    Ok(())
}

fn render_from_paths(source_profile: &Path, materialization: &Path) -> Result<Vec<u8>> {
    let source_profile_bytes = fs::read(source_profile)
        .with_context(|| format!("failed to read {}", source_profile.display()))?;
    let materialization_bytes = fs::read(materialization)
        .with_context(|| format!("failed to read {}", materialization.display()))?;
    render_contention_profile(&source_profile_bytes, &materialization_bytes)
}

fn decode_profile(bytes: &[u8]) -> Result<Profile> {
    match parse_contract(bytes).context("invalid source profile")? {
        ContractDocument::Profile(profile) => Ok(profile),
        document => bail!("source document is {}, not a profile", document.kind()),
    }
}

fn validate_materialization(
    profile: &Profile,
    source_profile_bytes: &[u8],
    materialization: &ContentionProfileMaterialization,
) -> Result<()> {
    if materialization.format != MATERIALIZATION_FORMAT {
        bail!(
            "unsupported materialization format `{}`",
            materialization.format
        );
    }
    if materialization.source_profile.id != profile.id {
        bail!("materialization source profile ID does not match the input profile");
    }
    if materialization.output_profile.id == profile.id {
        bail!("materialized profile ID must differ from the source profile ID");
    }
    require_sha256(
        &materialization.source_profile.digest,
        "source profile digest",
    )?;
    let source_digest = sha256_hex(source_profile_bytes);
    if materialization.source_profile.digest.value != source_digest {
        bail!(
            "source profile digest mismatch: expected {}, got {source_digest}",
            materialization.source_profile.digest.value
        );
    }
    for (field, value) in [
        (
            "output profile title",
            materialization.output_profile.title.as_str(),
        ),
        (
            "output profile description",
            materialization.output_profile.description.as_str(),
        ),
        (
            "output profile resolved_at",
            materialization.output_profile.resolved_at.as_str(),
        ),
    ] {
        if value.trim().is_empty() {
            bail!("{field} must not be empty");
        }
    }

    let components = profile
        .components
        .iter()
        .map(|component| (component.id.as_str(), component))
        .collect::<BTreeMap<_, _>>();
    for id in MATERIALIZED_COMPONENTS {
        if !components.contains_key(id) {
            bail!("source profile omits required contention component `{id}`");
        }
    }

    let mut observed = BTreeMap::new();
    for image in &materialization.images {
        if observed.insert(image.component.as_str(), image).is_some() {
            bail!("duplicate image observation for `{}`", image.component);
        }
    }
    let expected_images = MATERIALIZED_IMAGES
        .iter()
        .map(|(component, _, _)| *component)
        .collect::<BTreeSet<_>>();
    let observed_images = observed.keys().copied().collect::<BTreeSet<_>>();
    if observed_images != expected_images {
        bail!(
            "image observations must be exactly: {}",
            expected_images.into_iter().collect::<Vec<_>>().join(", ")
        );
    }

    for (component_id, service, required_artifacts) in MATERIALIZED_IMAGES {
        let component = components
            .get(component_id)
            .with_context(|| format!("source profile omits `{component_id}`"))?;
        let image = observed
            .get(component_id)
            .with_context(|| format!("materialization omits `{component_id}`"))?;
        validate_image(profile, component, image, service, required_artifacts)?;
    }
    require_consistent_compose_version(&materialization.images)?;
    Ok(())
}

fn validate_image(
    profile: &Profile,
    component: &catalog_bench_common::contract::Component,
    image: &ImageObservation,
    service: &str,
    required_artifacts: &[&str],
) -> Result<()> {
    if image.reference.trim().is_empty() {
        bail!("image reference for `{}` must not be empty", component.id);
    }
    require_sha256(&image.image_id, "local image ID")?;
    if image.operating_system != profile.platform.operating_system.to_ascii_lowercase()
        || image.architecture != docker_architecture(&profile.platform.architecture)?
    {
        bail!(
            "image `{}` runtime {}/{} does not match profile {}/{}",
            component.id,
            image.operating_system,
            image.architecture,
            profile.platform.operating_system,
            profile.platform.architecture
        );
    }
    require_label(image, "com.docker.compose.project", "catalog-bench")?;
    require_label(image, "com.docker.compose.service", service)?;
    require_non_empty_label(image, "com.docker.compose.version")?;
    let source = component
        .source
        .as_ref()
        .with_context(|| format!("component `{}` has no source revision", component.id))?;
    require_label(image, "org.opencontainers.image.revision", &source.revision)?;

    if component.id.as_str() == "minio" {
        let helper_revision = component
            .build
            .as_ref()
            .and_then(|build| build.extensions.get("querygraph/helper-source"))
            .and_then(|source| source.get("revision"))
            .and_then(Value::as_str)
            .context("MinIO build omits querygraph/helper-source revision")?;
        require_label(
            image,
            "io.querygraph.catalog-bench.helper-source-revision",
            helper_revision,
        )?;
    }

    let artifacts = image
        .embedded_artifacts
        .iter()
        .map(|artifact| (artifact.location.as_str(), artifact))
        .collect::<BTreeMap<_, _>>();
    if artifacts.len() != image.embedded_artifacts.len() {
        bail!(
            "image `{}` contains duplicate artifact locations",
            component.id
        );
    }
    for location in required_artifacts {
        let artifact = artifacts.get(location).with_context(|| {
            format!(
                "image `{}` omits required artifact `{location}`",
                component.id
            )
        })?;
        require_sha256(&artifact.digest, "embedded artifact digest")?;
        if artifact.media_type != "application/vnd.elf" {
            bail!("embedded artifact `{location}` must use media type `application/vnd.elf`");
        }
        if artifact.bytes == Some(0) || artifact.bytes.is_none() {
            bail!("embedded artifact `{location}` must record a positive byte size");
        }
    }
    Ok(())
}

fn require_non_empty_label<'a>(image: &'a ImageObservation, name: &str) -> Result<&'a str> {
    let value = image
        .labels
        .get(name)
        .with_context(|| format!("image `{}` omits label `{name}`", image.component))?;
    if value.trim().is_empty() {
        bail!(
            "image `{}` label `{name}` must not be empty",
            image.component
        );
    }
    Ok(value)
}

fn require_consistent_compose_version(images: &[ImageObservation]) -> Result<()> {
    let mut versions = images
        .iter()
        .map(|image| require_non_empty_label(image, "com.docker.compose.version"));
    let expected = versions
        .next()
        .transpose()?
        .context("contention materialization contains no image observations")?;
    for version in versions {
        let version = version?;
        if version != expected {
            bail!(
                "all materialized images must use one Docker Compose version; found `{expected}` and `{version}`"
            );
        }
    }
    Ok(())
}

fn require_label(image: &ImageObservation, name: &str, expected: &str) -> Result<()> {
    let actual = image
        .labels
        .get(name)
        .with_context(|| format!("image `{}` omits label `{name}`", image.component))?;
    if actual != expected {
        bail!(
            "image `{}` label `{name}` is `{actual}`, expected `{expected}`",
            image.component
        );
    }
    Ok(())
}

fn require_sha256(digest: &Digest, field: &str) -> Result<()> {
    if digest.algorithm != DigestAlgorithm::Sha256
        || digest.value.len() != 64
        || !digest.value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("{field} must be a 64-character SHA-256 digest");
    }
    Ok(())
}

fn docker_architecture(profile_architecture: &str) -> Result<&'static str> {
    match profile_architecture {
        "aarch64" => Ok("arm64"),
        other => bail!("unsupported Docker architecture mapping for `{other}`"),
    }
}
