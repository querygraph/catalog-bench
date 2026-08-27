//! Shared, effect-free helpers for deterministic publication materializers.

use std::fs;
use std::path::Path;

use anyhow::{bail, Context, Result};
use catalog_bench_common::contract::{
    ArtifactReference, ContractDocument, Digest, DigestAlgorithm, Extensions, Profile, Scenario,
};
use serde::{Deserialize, Serialize};

use crate::sha256_hex;

/// Parsed canonical UTC instant used to order reviewed evidence timestamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct UtcTimestamp {
    year: u32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    nanosecond: u32,
}

/// Human-reviewed bundle metadata shared by publication importers.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReviewBundle {
    pub(crate) id: String,
    pub(crate) title: String,
    pub(crate) output_directory: String,
    pub(crate) created_at: String,
}

/// Exact identity of one repository source named by a publication review.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReviewSource {
    pub(crate) location: String,
    pub(crate) sha256: String,
    pub(crate) bytes: u64,
}

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

pub(crate) fn require_text(value: &str, name: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(())
}

/// Parse `YYYY-MM-DDTHH:MM:SS[.fraction]Z` without accepting timezone drift.
pub(crate) fn parse_utc_timestamp(value: &str, name: &str) -> Result<UtcTimestamp> {
    let bytes = value.as_bytes();
    let fixed_shape = bytes.len() >= 20
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[13] == b':'
        && bytes[16] == b':'
        && bytes.last() == Some(&b'Z');
    if !fixed_shape {
        bail!("{name} must be a canonical UTC RFC 3339 timestamp");
    }

    let year = decimal(bytes, 0, 4, name)?;
    let month = decimal(bytes, 5, 7, name)?;
    let day = decimal(bytes, 8, 10, name)?;
    let hour = decimal(bytes, 11, 13, name)?;
    let minute = decimal(bytes, 14, 16, name)?;
    let second = decimal(bytes, 17, 19, name)?;
    let nanosecond = match bytes.len() {
        20 => 0,
        22..=30 if bytes[19] == b'.' => {
            let digits = &bytes[20..bytes.len() - 1];
            if !digits.iter().all(u8::is_ascii_digit) {
                bail!("{name} must be a canonical UTC RFC 3339 timestamp");
            }
            let mut nanos = digits
                .iter()
                .fold(0_u32, |value, digit| value * 10 + u32::from(digit - b'0'));
            for _ in digits.len()..9 {
                nanos *= 10;
            }
            nanos
        }
        _ => bail!("{name} must be a canonical UTC RFC 3339 timestamp"),
    };

    if year == 0
        || !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        bail!("{name} contains an invalid UTC calendar value");
    }

    Ok(UtcTimestamp {
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanosecond,
    })
}

fn decimal(bytes: &[u8], start: usize, end: usize, name: &str) -> Result<u32> {
    let digits = &bytes[start..end];
    if !digits.iter().all(u8::is_ascii_digit) {
        bail!("{name} must be a canonical UTC RFC 3339 timestamp");
    }
    Ok(digits
        .iter()
        .fold(0_u32, |value, digit| value * 10 + u32::from(digit - b'0')))
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(400) || (year.is_multiple_of(4) && !year.is_multiple_of(100)) => {
            29
        }
        2 => 28,
        _ => 0,
    }
}
