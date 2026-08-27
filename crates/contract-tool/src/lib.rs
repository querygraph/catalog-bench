//! File-level validation, historical import, and generated reporting for
//! `catalog-bench/v1` evidence bundles.

mod bundle;
mod contention_bundle;
mod contention_profile;
mod legacy_commit;
mod matrix;
mod publication;

use sha2::{Digest as _, Sha256};

pub use bundle::{load_bundle, ValidatedBundle, ValidatedResult, ValidatedScenario};
pub use contention_bundle::{check_contention_result_bundle, write_contention_result_bundle};
pub use contention_profile::{
    check_contention_profile, render_contention_profile, write_contention_profile,
};
pub use legacy_commit::{check_historical_commit_bundle, write_historical_commit_bundle};
pub use matrix::render_commit_matrix;

fn sha256_hex(bytes: &[u8]) -> String {
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
