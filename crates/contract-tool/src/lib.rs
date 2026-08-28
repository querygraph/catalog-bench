//! File-level validation, historical import, and generated reporting for
//! `catalog-bench/v1` evidence bundles.

mod bundle;
mod contention_bundle;
mod contention_profile;
mod engine_bundle;
mod engine_evidence;
mod engine_matrix;
mod engine_review;
mod flink_profile;
mod legacy_commit;
mod matrix;
mod profile_materialization;
mod profile_runtime_policy;
mod publication;
mod spark_profile;

use sha2::{Digest as _, Sha256};

pub use bundle::{load_bundle, ValidatedBundle, ValidatedResult, ValidatedScenario};
pub use contention_bundle::{check_contention_result_bundle, write_contention_result_bundle};
pub use contention_profile::{
    check_contention_profile, render_contention_profile, write_contention_profile,
};
pub use engine_bundle::{check_engine_result_bundle, write_engine_result_bundle};
pub use engine_evidence::{
    validate_engine_evidence_set, EngineEvidenceSummary, ValidatedEngineEvidenceSet,
    ValidatedEngineTranscript,
};
pub use engine_matrix::render_engine_matrix;
pub use engine_review::{validate_engine_result_review, ValidatedEngineResultReview};
pub use flink_profile::{check_flink_profile, render_flink_profile, write_flink_profile};
pub use legacy_commit::{check_historical_commit_bundle, write_historical_commit_bundle};
pub use matrix::{render_commit_matrix, render_matrix};
pub use profile_materialization::{
    check_scenario_profile, render_scenario_profile, write_scenario_profile, ArtifactCopyPolicy,
    ArtifactPolicy, BuildExtensionLabelPolicy, ImagePolicy, RequiredLabelPolicy,
    ScenarioProfilePolicy,
};
pub use spark_profile::{check_spark_profile, render_spark_profile, write_spark_profile};

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
