//! Deterministic materialization of scenario-scoped runnable profiles.
//!
//! A checked-in candidate profile is a broad version-selection document. This
//! module narrows one to a policy-selected topology and replaces unresolved
//! runtime artifacts with audited local-image observations. A source-profile
//! digest makes every transformation fail closed if its input drifts; the
//! resulting document remains an ordinary validated v1 profile.

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

#[derive(Debug, Clone, Copy)]
pub struct ArtifactPolicy {
    /// Absolute in-image location recorded by the observation.
    pub location: &'static str,
    /// Exact media type required at that location.
    pub media_type: &'static str,
}

/// Require an artifact copied between two materialized images to retain its
/// exact content identity.
#[derive(Debug, Clone, Copy)]
pub struct ArtifactCopyPolicy {
    /// Component owning the independently observed source artifact.
    pub source_component: &'static str,
    /// Absolute in-image source location recorded by its observation.
    pub source_location: &'static str,
    /// Component whose image contains the copied artifact.
    pub destination_component: &'static str,
    /// Absolute in-image destination location recorded by its observation.
    pub destination_location: &'static str,
}

/// Derive one required image label from a string field in the selected
/// component's build extensions.
#[derive(Debug, Clone, Copy)]
pub struct BuildExtensionLabelPolicy {
    /// Image label to validate.
    pub label: &'static str,
    /// Component build-extension key containing the expected value.
    pub extension: &'static str,
    /// String field within the extension object.
    pub field: &'static str,
}

/// Exact image-label value required by a scenario policy.
#[derive(Debug, Clone, Copy)]
pub struct RequiredLabelPolicy {
    /// Image label to validate.
    pub label: &'static str,
    /// Exact required value.
    pub value: &'static str,
}

/// Immutable image requirements for one selected profile component.
#[derive(Debug, Clone, Copy)]
pub struct ImagePolicy {
    /// Profile component replaced by the observed local image.
    pub component: &'static str,
    /// Docker Compose service label expected on the image.
    pub compose_service: &'static str,
    /// In-image artifacts that must be digest- and size-addressed.
    pub required_artifacts: &'static [ArtifactPolicy],
    /// Scenario-specific immutable labels beyond the component revision.
    pub required_labels: &'static [RequiredLabelPolicy],
    /// Optional source-derived label beyond the ordinary component revision.
    pub build_extension_label: Option<BuildExtensionLabelPolicy>,
}

/// Code-owned constraints for one deterministic scenario-profile projection.
#[derive(Debug, Clone, Copy)]
pub struct ScenarioProfilePolicy {
    /// Short name used in diagnostics.
    pub name: &'static str,
    /// Closed sidecar format identifier.
    pub materialization_format: &'static str,
    /// Scenario or workload scope written into the output profile.
    pub scope: &'static str,
    /// Purpose assigned to the derived runnable profile.
    pub purpose: ProfilePurpose,
    /// Exhaustive component set retained from the broad source profile.
    pub selected_components: &'static [&'static str],
    /// Exhaustive local-image observations required by this projection.
    pub images: &'static [ImagePolicy],
    /// Artifacts whose digest, byte count, and media type must survive a copy
    /// between independently observed images.
    pub artifact_copies: &'static [ArtifactCopyPolicy],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScenarioProfileMaterialization {
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

#[derive(Debug)]
struct ValidatedPolicy {
    selected_components: BTreeSet<&'static str>,
    image_components: BTreeSet<&'static str>,
}

/// Render a scenario-scoped runnable profile from a broad draft, an audited
/// observation sidecar, and code-owned projection policy.
///
/// # Errors
///
/// Returns an error for malformed inputs, source-byte drift, incomplete or
/// inconsistent observations, invalid component selection, or an invalid
/// derived v1 profile.
pub fn render_scenario_profile(
    source_profile_bytes: &[u8],
    materialization_bytes: &[u8],
    policy: &ScenarioProfilePolicy,
) -> Result<Vec<u8>> {
    let validated_policy = validate_policy(policy)?;
    let mut profile = decode_profile(source_profile_bytes)?;
    let materialization: ScenarioProfileMaterialization =
        serde_json::from_slice(materialization_bytes)
            .with_context(|| format!("invalid {} profile materialization", policy.name))?;
    validate_materialization(
        &profile,
        source_profile_bytes,
        &materialization,
        policy,
        &validated_policy,
    )?;

    profile.components.retain(|component| {
        validated_policy
            .selected_components
            .contains(component.id.as_str())
    });
    profile.services.retain(|service| {
        validated_policy
            .selected_components
            .contains(service.component.as_str())
    });
    profile.catalog_adapters.retain(|adapter| {
        validated_policy
            .selected_components
            .contains(adapter.catalog.as_str())
    });

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
    profile.purpose = policy.purpose;
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
            "format": policy.materialization_format,
            "scope": policy.scope,
            "source_profile": {
                "id": materialization.source_profile.id,
                "digest": materialization.source_profile.digest,
            },
            "observation_sha256": sha256_hex(materialization_bytes),
        }),
    );

    profile
        .validate()
        .with_context(|| format!("materialized {} profile is invalid", policy.name))?;
    let mut rendered = serde_json::to_vec_pretty(&profile)?;
    rendered.push(b'\n');
    Ok(rendered)
}

/// Write one deterministically rendered scenario profile.
///
/// # Errors
///
/// Returns an error when rendering fails or the output cannot be created.
pub fn write_scenario_profile(
    source_profile: &Path,
    materialization: &Path,
    output: &Path,
    policy: &ScenarioProfilePolicy,
) -> Result<()> {
    let rendered = render_from_paths(source_profile, materialization, policy)?;
    if let Some(parent) = output.parent().filter(|path| !path.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    fs::write(output, rendered).with_context(|| format!("failed to write {}", output.display()))?;
    Ok(())
}

/// Verify that a checked-in scenario profile is byte-identical to a fresh
/// rendering of its source and sidecar.
///
/// # Errors
///
/// Returns an error when rendering fails, the output cannot be read, or bytes
/// differ.
pub fn check_scenario_profile(
    source_profile: &Path,
    materialization: &Path,
    output: &Path,
    policy: &ScenarioProfilePolicy,
    write_command: &str,
) -> Result<()> {
    let expected = render_from_paths(source_profile, materialization, policy)?;
    let actual =
        fs::read(output).with_context(|| format!("failed to read {}", output.display()))?;
    if actual != expected {
        bail!("{} is stale; rerun `{write_command}`", output.display(),);
    }
    Ok(())
}

fn render_from_paths(
    source_profile: &Path,
    materialization: &Path,
    policy: &ScenarioProfilePolicy,
) -> Result<Vec<u8>> {
    let source_profile_bytes = fs::read(source_profile)
        .with_context(|| format!("failed to read {}", source_profile.display()))?;
    let materialization_bytes = fs::read(materialization)
        .with_context(|| format!("failed to read {}", materialization.display()))?;
    render_scenario_profile(&source_profile_bytes, &materialization_bytes, policy)
}

fn decode_profile(bytes: &[u8]) -> Result<Profile> {
    match parse_contract(bytes).context("invalid source profile")? {
        ContractDocument::Profile(profile) => Ok(profile),
        document => bail!("source document is {}, not a profile", document.kind()),
    }
}

fn validate_policy(policy: &ScenarioProfilePolicy) -> Result<ValidatedPolicy> {
    for (field, value) in [
        ("name", policy.name),
        ("materialization format", policy.materialization_format),
        ("scope", policy.scope),
    ] {
        if value.trim().is_empty() {
            bail!("scenario profile policy {field} must not be empty");
        }
    }
    if policy.selected_components.is_empty() {
        bail!("{} policy must select at least one component", policy.name);
    }
    if policy.images.is_empty() {
        bail!("{} policy must materialize at least one image", policy.name);
    }

    let selected_components = policy
        .selected_components
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if selected_components.len() != policy.selected_components.len() {
        bail!(
            "{} policy contains duplicate selected components",
            policy.name
        );
    }

    let image_components = policy
        .images
        .iter()
        .map(|image| image.component)
        .collect::<BTreeSet<_>>();
    if image_components.len() != policy.images.len() {
        bail!("{} policy contains duplicate image components", policy.name);
    }

    for image in policy.images {
        if image.component.trim().is_empty() {
            bail!("{} policy contains an empty image component", policy.name);
        }
        if image.compose_service.trim().is_empty() {
            bail!(
                "{} image policy has an empty Compose service",
                image.component
            );
        }
        if !selected_components.contains(image.component) {
            bail!(
                "{} image policy targets unselected component `{}`",
                policy.name,
                image.component
            );
        }

        let artifact_locations = image
            .required_artifacts
            .iter()
            .map(|artifact| artifact.location)
            .collect::<BTreeSet<_>>();
        if artifact_locations.len() != image.required_artifacts.len() {
            bail!(
                "{} image policy contains duplicate required artifact locations",
                image.component
            );
        }
        for artifact in image.required_artifacts {
            if artifact.location.trim().is_empty() || artifact.media_type.trim().is_empty() {
                bail!(
                    "{} image policy artifact location and media type must not be empty",
                    image.component
                );
            }
        }

        let required_labels = image
            .required_labels
            .iter()
            .map(|label| label.label)
            .collect::<BTreeSet<_>>();
        if required_labels.len() != image.required_labels.len() {
            bail!(
                "{} image policy contains duplicate required labels",
                image.component
            );
        }
        for label in image.required_labels {
            if label.label.trim().is_empty() || label.value.trim().is_empty() {
                bail!(
                    "{} required image-label name and value must not be empty",
                    image.component
                );
            }
        }

        if let Some(label) = image.build_extension_label {
            if [label.label, label.extension, label.field]
                .iter()
                .any(|value| value.trim().is_empty())
            {
                bail!(
                    "{} build-extension label policy fields must not be empty",
                    image.component
                );
            }
        }
    }

    let artifact_copies = policy
        .artifact_copies
        .iter()
        .map(|copy| {
            (
                copy.source_component,
                copy.source_location,
                copy.destination_component,
                copy.destination_location,
            )
        })
        .collect::<BTreeSet<_>>();
    if artifact_copies.len() != policy.artifact_copies.len() {
        bail!("{} policy contains duplicate artifact copies", policy.name);
    }
    for copy in policy.artifact_copies {
        for (field, value) in [
            ("source component", copy.source_component),
            ("source location", copy.source_location),
            ("destination component", copy.destination_component),
            ("destination location", copy.destination_location),
        ] {
            if value.trim().is_empty() {
                bail!("{} artifact-copy {field} must not be empty", policy.name);
            }
        }
        for (direction, component, location) in [
            ("source", copy.source_component, copy.source_location),
            (
                "destination",
                copy.destination_component,
                copy.destination_location,
            ),
        ] {
            let image = policy
                .images
                .iter()
                .find(|image| image.component == component)
                .with_context(|| {
                    format!(
                        "{} artifact-copy {direction} component `{component}` is not a materialized image",
                        policy.name
                    )
                })?;
            if !image
                .required_artifacts
                .iter()
                .any(|artifact| artifact.location == location)
            {
                bail!(
                    "{} artifact-copy {direction} `{component}` location `{location}` is not a required artifact",
                    policy.name
                );
            }
        }
    }

    Ok(ValidatedPolicy {
        selected_components,
        image_components,
    })
}

fn validate_materialization(
    profile: &Profile,
    source_profile_bytes: &[u8],
    materialization: &ScenarioProfileMaterialization,
    policy: &ScenarioProfilePolicy,
    validated_policy: &ValidatedPolicy,
) -> Result<()> {
    if materialization.format != policy.materialization_format {
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
    for id in &validated_policy.selected_components {
        if !components.contains_key(id) {
            bail!(
                "source profile omits required {} component `{id}`",
                policy.name
            );
        }
    }

    let mut observed = BTreeMap::new();
    for image in &materialization.images {
        if observed.insert(image.component.as_str(), image).is_some() {
            bail!("duplicate image observation for `{}`", image.component);
        }
    }
    let observed_images = observed.keys().copied().collect::<BTreeSet<_>>();
    if observed_images != validated_policy.image_components {
        bail!(
            "image observations must be exactly: {}",
            validated_policy
                .image_components
                .iter()
                .copied()
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    for image_policy in policy.images {
        let component_id = image_policy.component;
        let component = components
            .get(component_id)
            .with_context(|| format!("source profile omits `{component_id}`"))?;
        let image = observed
            .get(component_id)
            .with_context(|| format!("materialization omits `{component_id}`"))?;
        validate_image(profile, component, image, image_policy)?;
    }
    validate_artifact_copies(&observed, policy)?;
    require_consistent_compose_version(&materialization.images, policy.name)?;
    Ok(())
}

fn validate_artifact_copies(
    observations: &BTreeMap<&str, &ImageObservation>,
    policy: &ScenarioProfilePolicy,
) -> Result<()> {
    for copy in policy.artifact_copies {
        let source = observed_artifact(observations, copy.source_component, copy.source_location)?;
        let destination = observed_artifact(
            observations,
            copy.destination_component,
            copy.destination_location,
        )?;
        if source.digest != destination.digest
            || source.bytes != destination.bytes
            || source.media_type != destination.media_type
        {
            bail!(
                "artifact copied from `{}` `{}` to `{}` `{}` must be byte-identical",
                copy.source_component,
                copy.source_location,
                copy.destination_component,
                copy.destination_location
            );
        }
    }
    Ok(())
}

fn observed_artifact<'a>(
    observations: &'a BTreeMap<&str, &ImageObservation>,
    component: &str,
    location: &str,
) -> Result<&'a ArtifactReference> {
    observations
        .get(component)
        .with_context(|| format!("materialization omits `{component}`"))?
        .embedded_artifacts
        .iter()
        .find(|artifact| artifact.location == location)
        .with_context(|| format!("image `{component}` omits required artifact `{location}`"))
}

fn validate_image(
    profile: &Profile,
    component: &catalog_bench_common::contract::Component,
    image: &ImageObservation,
    policy: &ImagePolicy,
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
    require_label(image, "com.docker.compose.service", policy.compose_service)?;
    require_non_empty_label(image, "com.docker.compose.version")?;
    let source = component
        .source
        .as_ref()
        .with_context(|| format!("component `{}` has no source revision", component.id))?;
    require_label(image, "org.opencontainers.image.revision", &source.revision)?;

    for label in policy.required_labels {
        require_label(image, label.label, label.value)?;
    }

    if let Some(label_policy) = policy.build_extension_label {
        let helper_revision = component
            .build
            .as_ref()
            .and_then(|build| build.extensions.get(label_policy.extension))
            .and_then(|extension| extension.get(label_policy.field))
            .and_then(Value::as_str)
            .with_context(|| {
                format!(
                    "component `{}` build extension `{}` omits string field `{}`",
                    component.id, label_policy.extension, label_policy.field
                )
            })?;
        require_label(image, label_policy.label, helper_revision)?;
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
    for artifact_policy in policy.required_artifacts {
        let artifact = artifacts.get(artifact_policy.location).with_context(|| {
            format!(
                "image `{}` omits required artifact `{}`",
                component.id, artifact_policy.location
            )
        })?;
        require_sha256(&artifact.digest, "embedded artifact digest")?;
        if artifact.media_type != artifact_policy.media_type {
            bail!(
                "embedded artifact `{}` must use media type `{}`",
                artifact_policy.location,
                artifact_policy.media_type
            );
        }
        if artifact.bytes == Some(0) || artifact.bytes.is_none() {
            bail!(
                "embedded artifact `{}` must record a positive byte size",
                artifact_policy.location
            );
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

fn require_consistent_compose_version(
    images: &[ImageObservation],
    profile_name: &str,
) -> Result<()> {
    let mut versions = images
        .iter()
        .map(|image| require_non_empty_label(image, "com.docker.compose.version"));
    let expected = versions.next().transpose()?.with_context(|| {
        format!("{profile_name} materialization contains no image observations")
    })?;
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

fn docker_architecture(profile_architecture: &str) -> Result<&str> {
    match profile_architecture {
        "aarch64" => Ok("arm64"),
        "x86_64" => Ok("amd64"),
        "armv7" | "armv7l" => Ok("arm"),
        "amd64" | "arm64" | "arm" | "ppc64le" | "s390x" | "riscv64" => Ok(profile_architecture),
        other => bail!("unsupported Docker architecture mapping for `{other}`"),
    }
}
