//! Shared, effect-free helpers for deterministic publication materializers.

use std::fs;
use std::path::Path;

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{
    ArtifactReference, ContractDocument, Digest, DigestAlgorithm, Extensions, Profile, Scenario,
};
use serde::Serialize;

use crate::sha256_hex;

pub(crate) fn parse_profile(bytes: &[u8]) -> Result<Profile> {
    match catalog_bench_common::contract::parse_contract(bytes)? {
        ContractDocument::Profile(profile) => Ok(profile),
        document => bail!("expected profile, found {}", document.kind()),
    }
}

pub(crate) fn parse_scenario(bytes: &[u8]) -> Result<Scenario> {
    match catalog_bench_common::contract::parse_contract(bytes)? {
        ContractDocument::Scenario(scenario) => Ok(scenario),
        document => bail!("expected scenario, found {}", document.kind()),
    }
}

pub(crate) fn read_hashed(path: &Path, expected_sha256: &str) -> Result<Vec<u8>> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let actual = sha256_hex(&bytes);
    if actual != expected_sha256 {
        bail!(
            "{} hash mismatch: expected {expected_sha256}, got {actual}",
            path.display()
        );
    }
    Ok(bytes)
}

pub(crate) fn artifact(
    location: &str,
    media_type: &str,
    bytes: &[u8],
    description: &str,
) -> ArtifactReference {
    ArtifactReference {
        location: location.to_owned(),
        media_type: media_type.to_owned(),
        digest: Digest {
            algorithm: DigestAlgorithm::Sha256,
            value: sha256_hex(bytes),
        },
        bytes: Some(bytes.len() as u64),
        description: Some(description.to_owned()),
        extensions: Extensions::new(),
    }
}

pub(crate) fn pretty_json(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}
