use std::fs;

use catalog_bench_common::contract::ComponentId;
use catalog_bench_engine::{
    RuntimeArtifactExpectation, RuntimeArtifactOutcome, RuntimeArtifactUnavailableKind,
    RuntimePlatformExpectation, RuntimeVerifier,
};
use sha2::{Digest as _, Sha256};
use tempfile::tempdir;

#[test]
fn verifies_every_expected_file_and_standard_platform_alias() {
    let root = tempdir().unwrap();
    let mut artifacts = expectations();
    for (index, artifact) in artifacts.iter_mut().enumerate() {
        let bytes = format!("runtime artifact {index}").into_bytes();
        artifact.bytes = bytes.len() as u64;
        artifact.sha256 = hex(&Sha256::digest(&bytes));
        let path = root.path().join(artifact.location.trim_start_matches('/'));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }

    let verification = RuntimeVerifier::for_observation(root.path(), "linux", "arm64")
        .verify_expectations(&platform(), &artifacts);
    assert!(verification.passed());
    assert!(verification
        .artifacts
        .iter()
        .all(|artifact| matches!(artifact.outcome, RuntimeArtifactOutcome::Match { .. })));
}

#[test]
fn records_mismatch_missing_and_platform_drift_without_raw_io_errors() {
    let root = tempdir().unwrap();
    let artifacts = expectations();
    let first = &artifacts[0];
    let path = root.path().join(first.location.trim_start_matches('/'));
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, b"wrong bytes").unwrap();

    let verification = RuntimeVerifier::for_observation(root.path(), "macos", "x86_64")
        .verify_expectations(&platform(), &artifacts);
    assert!(!verification.passed());
    assert!(!verification.platform.operating_system_matches);
    assert!(!verification.platform.architecture_matches);
    assert!(matches!(
        verification.artifacts[0].outcome,
        RuntimeArtifactOutcome::Mismatch { .. }
    ));
    assert!(verification.artifacts[1..].iter().all(|artifact| matches!(
        artifact.outcome,
        RuntimeArtifactOutcome::Unavailable {
            kind: RuntimeArtifactUnavailableKind::Missing
        }
    )));
    let encoded = serde_json::to_string(&verification).unwrap();
    assert!(!encoded.contains("No such file"));
    assert!(!encoded.contains(root.path().to_str().unwrap()));
}

fn platform() -> RuntimePlatformExpectation {
    RuntimePlatformExpectation {
        operating_system: "Linux".to_owned(),
        architecture: "aarch64".to_owned(),
    }
}

fn expectations() -> Vec<RuntimeArtifactExpectation> {
    ["/opt/runtime/engine", "/opt/runtime/connector"]
        .into_iter()
        .map(|location| RuntimeArtifactExpectation {
            location: location.to_owned(),
            media_type: "application/octet-stream".to_owned(),
            sha256: "0".repeat(64),
            bytes: 1,
            components: vec![ComponentId::from("engine")],
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
