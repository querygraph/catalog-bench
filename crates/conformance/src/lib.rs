//! Catalog-neutral conformance probes that consume checked-in adapter profiles.
//!
//! Probes preserve protocol behavior and emit sanitized evidence. They do not
//! classify smoke output as publishable benchmark results; immutable result
//! bundles remain the responsibility of the final execution pipeline.

mod commit;
mod config;
mod evidence;
mod iceberg;
mod idempotency;
mod namespace;
mod operation;
mod routing;
mod sanitize;
mod spec;
mod table;
mod table_protocol;
mod target;
mod transport;

use anyhow::Result;
use serde::Serialize;
use sha2::{Digest as _, Sha256};

pub use commit::*;
pub use config::*;
pub use evidence::*;
pub use namespace::*;
pub use spec::*;
pub use table::*;

/// Serialize evidence deterministically as pretty JSON with one final newline.
pub fn encode_evidence<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Lowercase SHA-256 for exact evidence bytes.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    Sha256::digest(bytes)
        .iter()
        .flat_map(|byte| {
            [
                HEX[(byte >> 4) as usize] as char,
                HEX[(byte & 0x0f) as usize] as char,
            ]
        })
        .collect()
}
