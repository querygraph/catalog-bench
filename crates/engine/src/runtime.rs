use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use catalog_bench_common::contract::ComponentId;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::{InteroperabilityPlan, RuntimeArtifactExpectation, RuntimePlatformExpectation};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimePlatformObservation {
    pub expected_operating_system: String,
    pub observed_operating_system: String,
    pub operating_system_matches: bool,
    pub expected_architecture: String,
    pub observed_architecture: String,
    pub architecture_matches: bool,
}

impl RuntimePlatformObservation {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.operating_system_matches && self.architecture_matches
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeArtifactUnavailableKind {
    Missing,
    NotAFile,
    Open,
    Read,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum RuntimeArtifactOutcome {
    Match {
        observed_bytes: u64,
        observed_sha256: String,
    },
    Mismatch {
        observed_bytes: u64,
        observed_sha256: String,
        bytes_match: bool,
        sha256_matches: bool,
    },
    Unavailable {
        kind: RuntimeArtifactUnavailableKind,
    },
}

impl RuntimeArtifactOutcome {
    #[must_use]
    pub fn passed(&self) -> bool {
        matches!(self, Self::Match { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeArtifactObservation {
    pub location: String,
    pub media_type: String,
    pub components: Vec<ComponentId>,
    pub expected_bytes: u64,
    pub expected_sha256: String,
    pub outcome: RuntimeArtifactOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeVerification {
    pub platform: RuntimePlatformObservation,
    pub artifacts: Vec<RuntimeArtifactObservation>,
}

impl RuntimeVerification {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.platform.passed()
            && self
                .artifacts
                .iter()
                .all(|artifact| artifact.outcome.passed())
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeVerifier {
    root: PathBuf,
    operating_system: String,
    architecture: String,
}

impl RuntimeVerifier {
    #[must_use]
    pub fn host() -> Self {
        Self {
            root: PathBuf::from("/"),
            operating_system: std::env::consts::OS.to_owned(),
            architecture: std::env::consts::ARCH.to_owned(),
        }
    }

    #[must_use]
    pub fn for_observation(
        root: impl Into<PathBuf>,
        operating_system: impl Into<String>,
        architecture: impl Into<String>,
    ) -> Self {
        Self {
            root: root.into(),
            operating_system: operating_system.into(),
            architecture: architecture.into(),
        }
    }

    #[must_use]
    pub fn verify(&self, plan: &InteroperabilityPlan) -> RuntimeVerification {
        self.verify_expectations(plan.runtime_platform(), plan.runtime_artifacts())
    }

    #[must_use]
    pub fn verify_expectations(
        &self,
        expected: &RuntimePlatformExpectation,
        expectations: &[RuntimeArtifactExpectation],
    ) -> RuntimeVerification {
        let platform = RuntimePlatformObservation {
            expected_operating_system: expected.operating_system.clone(),
            observed_operating_system: self.operating_system.clone(),
            operating_system_matches: operating_system_matches(
                &expected.operating_system,
                &self.operating_system,
            ),
            expected_architecture: expected.architecture.clone(),
            observed_architecture: self.architecture.clone(),
            architecture_matches: architecture_matches(&expected.architecture, &self.architecture),
        };
        let artifacts = expectations
            .iter()
            .map(|expectation| self.verify_artifact(expectation))
            .collect();
        RuntimeVerification {
            platform,
            artifacts,
        }
    }

    pub(crate) fn artifact_path(&self, location: &str) -> PathBuf {
        self.root.join(location.trim_start_matches('/'))
    }

    fn verify_artifact(
        &self,
        expectation: &RuntimeArtifactExpectation,
    ) -> RuntimeArtifactObservation {
        let path = self.artifact_path(&expectation.location);
        let outcome = observe_file(&path, expectation);
        RuntimeArtifactObservation {
            location: expectation.location.clone(),
            media_type: expectation.media_type.clone(),
            components: expectation.components.clone(),
            expected_bytes: expectation.bytes,
            expected_sha256: expectation.sha256.clone(),
            outcome,
        }
    }
}

fn observe_file(path: &Path, expectation: &RuntimeArtifactExpectation) -> RuntimeArtifactOutcome {
    let metadata = match path.metadata() {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return RuntimeArtifactOutcome::Unavailable {
                kind: RuntimeArtifactUnavailableKind::Missing,
            };
        }
        Err(_) => {
            return RuntimeArtifactOutcome::Unavailable {
                kind: RuntimeArtifactUnavailableKind::Open,
            };
        }
    };
    if !metadata.is_file() {
        return RuntimeArtifactOutcome::Unavailable {
            kind: RuntimeArtifactUnavailableKind::NotAFile,
        };
    }
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(_) => {
            return RuntimeArtifactOutcome::Unavailable {
                kind: RuntimeArtifactUnavailableKind::Open,
            };
        }
    };
    let Some((observed_bytes, observed_sha256)) = digest_reader(&mut file) else {
        return RuntimeArtifactOutcome::Unavailable {
            kind: RuntimeArtifactUnavailableKind::Read,
        };
    };
    let bytes_match = observed_bytes == expectation.bytes;
    let sha256_matches = observed_sha256 == expectation.sha256;
    if bytes_match && sha256_matches {
        RuntimeArtifactOutcome::Match {
            observed_bytes,
            observed_sha256,
        }
    } else {
        RuntimeArtifactOutcome::Mismatch {
            observed_bytes,
            observed_sha256,
            bytes_match,
            sha256_matches,
        }
    }
}

fn digest_reader(reader: &mut impl Read) -> Option<(u64, String)> {
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).ok()?;
        if read == 0 {
            break;
        }
        bytes = bytes.checked_add(read as u64)?;
        digest.update(&buffer[..read]);
    }
    Some((bytes, hex(&digest.finalize())))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|byte| {
            [
                DIGITS[(byte >> 4) as usize] as char,
                DIGITS[(byte & 0x0f) as usize] as char,
            ]
        })
        .collect()
}

pub(crate) fn operating_system_matches(expected: &str, observed: &str) -> bool {
    normalize_operating_system(expected) == normalize_operating_system(observed)
}

fn normalize_operating_system(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "macos" | "darwin" => "macos".to_owned(),
        other => other.to_owned(),
    }
}

pub(crate) fn architecture_matches(expected: &str, observed: &str) -> bool {
    normalize_architecture(expected) == normalize_architecture(observed)
}

fn normalize_architecture(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "aarch64" | "arm64" => "arm64".to_owned(),
        "x86_64" | "amd64" => "amd64".to_owned(),
        other => other.to_owned(),
    }
}
