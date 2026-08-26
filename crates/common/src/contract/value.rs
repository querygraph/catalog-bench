use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{child_path, indexed_path, require_non_empty, Validate, ValidationIssue};

/// Deliberate extension points. Ordinary document fields remain closed.
pub type Extensions = BTreeMap<String, Value>;

macro_rules! string_id {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(
            Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
        )]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }

        impl Validate for $name {
            fn collect_issues(&self, path: &str, issues: &mut Vec<ValidationIssue>) {
                require_non_empty(&self.0, path, issues);
            }
        }
    };
}

string_id!(ScenarioId, "Stable identifier for a versioned scenario.");
string_id!(ProfileId, "Stable identifier for an execution profile.");
string_id!(ResultId, "Unique identifier for one result record.");
string_id!(BundleId, "Unique identifier for one result bundle.");
string_id!(ComponentId, "Identifier for a component within a profile.");
string_id!(CapabilityId, "Stable identifier for a scenario capability.");
string_id!(StepId, "Identifier for a step within a scenario.");
string_id!(
    AssertionId,
    "Identifier for an assertion within a scenario."
);
string_id!(
    EvidenceId,
    "Identifier for an evidence item within a result."
);

/// A content hash. Hash algorithms are explicit so a future contract can add one
/// without changing the meaning of existing values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Digest {
    pub algorithm: DigestAlgorithm,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum DigestAlgorithm {
    Sha256,
}

impl Validate for Digest {
    fn collect_issues(&self, path: &str, issues: &mut Vec<ValidationIssue>) {
        let expected_length = match self.algorithm {
            DigestAlgorithm::Sha256 => 64,
        };
        if self.value.len() != expected_length
            || !self.value.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            issues.push(ValidationIssue::new(
                child_path(path, "value"),
                format!(
                    "must contain exactly {expected_length} hexadecimal characters for {:?}",
                    self.algorithm
                ),
            ));
        }
    }
}

/// A file or URI together with the digest that makes the reference immutable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReference {
    /// Bundle-relative path or public URI.
    pub location: String,
    pub media_type: String,
    pub digest: Digest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: Extensions,
}

impl Validate for ArtifactReference {
    fn collect_issues(&self, path: &str, issues: &mut Vec<ValidationIssue>) {
        require_non_empty(&self.location, child_path(path, "location"), issues);
        require_non_empty(&self.media_type, child_path(path, "media_type"), issues);
        self.digest
            .collect_issues(&child_path(path, "digest"), issues);
    }
}

/// Immutable source identity for a component built from source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceRevision {
    pub repository: String,
    pub revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: Extensions,
}

impl Validate for SourceRevision {
    fn collect_issues(&self, path: &str, issues: &mut Vec<ValidationIssue>) {
        require_non_empty(&self.repository, child_path(path, "repository"), issues);
        require_non_empty(&self.revision, child_path(path, "revision"), issues);
    }
}

/// Reproducible build settings that materially affect a native executable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BuildConfiguration {
    pub toolchain: String,
    pub target: String,
    pub profile: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub compiler_flags: Vec<String>,
    pub locked: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: Extensions,
}

impl Validate for BuildConfiguration {
    fn collect_issues(&self, path: &str, issues: &mut Vec<ValidationIssue>) {
        require_non_empty(&self.toolchain, child_path(path, "toolchain"), issues);
        require_non_empty(&self.target, child_path(path, "target"), issues);
        require_non_empty(&self.profile, child_path(path, "profile"), issues);
    }
}

/// The deployable artifact used for a component.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum RuntimeArtifact {
    ContainerImage {
        reference: String,
        digest_scope: ImageDigestScope,
        digest: Digest,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        platform_digest: Option<Digest>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        embedded_artifacts: Vec<ArtifactReference>,
    },
    SourceBuild {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        executable: Option<Box<ArtifactReference>>,
    },
    Package {
        ecosystem: String,
        package: String,
        version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        digest: Option<Digest>,
    },
}

/// What an image digest addresses. Registry indexes, platform manifests, and
/// local Docker image IDs are different objects and must not be conflated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ImageDigestScope {
    Index,
    PlatformManifest,
    LocalImage,
}

impl Validate for RuntimeArtifact {
    fn collect_issues(&self, path: &str, issues: &mut Vec<ValidationIssue>) {
        match self {
            Self::ContainerImage {
                reference,
                digest_scope,
                digest,
                platform_digest,
                embedded_artifacts,
            } => {
                require_non_empty(reference, child_path(path, "reference"), issues);
                digest.collect_issues(&child_path(path, "digest"), issues);
                if let Some(digest) = platform_digest {
                    digest.collect_issues(&child_path(path, "platform_digest"), issues);
                    if *digest_scope != ImageDigestScope::Index {
                        issues.push(ValidationIssue::new(
                            child_path(path, "platform_digest"),
                            "is only meaningful when digest_scope is `index`",
                        ));
                    }
                }
                validate_artifacts(
                    embedded_artifacts,
                    &child_path(path, "embedded_artifacts"),
                    issues,
                );
            }
            Self::SourceBuild { executable } => {
                if let Some(artifact) = executable {
                    artifact.collect_issues(&child_path(path, "executable"), issues);
                }
            }
            Self::Package {
                ecosystem,
                package,
                version,
                digest,
            } => {
                require_non_empty(ecosystem, child_path(path, "ecosystem"), issues);
                require_non_empty(package, child_path(path, "package"), issues);
                require_non_empty(version, child_path(path, "version"), issues);
                if let Some(digest) = digest {
                    digest.collect_issues(&child_path(path, "digest"), issues);
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ComponentKind {
    Catalog,
    Client,
    Connector,
    Engine,
    ObjectStore,
    StateStore,
    BenchmarkHarness,
    ContainerRuntime,
    Validator,
    Converter,
    Other,
}

/// Exact identity of a catalog, client, engine, service, or tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Component {
    pub id: ComponentId,
    pub kind: ComponentKind,
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<Box<SourceRevision>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build: Option<Box<BuildConfiguration>>,
    pub artifact: RuntimeArtifact,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: Extensions,
}

impl Validate for Component {
    fn collect_issues(&self, path: &str, issues: &mut Vec<ValidationIssue>) {
        self.id.collect_issues(&child_path(path, "id"), issues);
        require_non_empty(&self.name, child_path(path, "name"), issues);
        require_non_empty(&self.version, child_path(path, "version"), issues);
        if let Some(source) = &self.source {
            source.collect_issues(&child_path(path, "source"), issues);
        }
        if let Some(build) = &self.build {
            build.collect_issues(&child_path(path, "build"), issues);
            if self.source.is_none() {
                issues.push(ValidationIssue::new(
                    child_path(path, "build"),
                    "requires an immutable source revision",
                ));
            }
        }
        if matches!(&self.artifact, RuntimeArtifact::SourceBuild { .. }) {
            if self.source.is_none() {
                issues.push(ValidationIssue::new(
                    child_path(path, "source"),
                    "is required for a source-build artifact",
                ));
            }
            if self.build.is_none() {
                issues.push(ValidationIssue::new(
                    child_path(path, "build"),
                    "is required for a source-build artifact",
                ));
            }
        }
        self.artifact
            .collect_issues(&child_path(path, "artifact"), issues);
    }
}

pub(crate) fn require_unique<T: Ord + Display>(
    values: impl IntoIterator<Item = T>,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value.to_string()) {
            issues.push(ValidationIssue::new(
                path,
                format!("contains duplicate identifier `{value}`"),
            ));
        }
    }
}

pub(crate) fn reject_secret_like_keys<'a>(
    keys: impl IntoIterator<Item = &'a str>,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    for key in keys {
        let normalized = key.to_ascii_lowercase();
        if ["password", "secret", "token", "private_key", "access_key"]
            .iter()
            .any(|needle| normalized.contains(needle))
        {
            issues.push(ValidationIssue::new(
                path,
                format!("secret-like setting key `{key}` is forbidden"),
            ));
        }
    }
}

pub(crate) fn validate_artifacts(
    artifacts: &[ArtifactReference],
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    for (index, artifact) in artifacts.iter().enumerate() {
        artifact.collect_issues(&indexed_path(path, index), issues);
    }
}
