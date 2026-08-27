//! Contention-specific policy for the shared scenario-profile materializer.

use std::path::Path;

use anyhow::Result;
use catalog_bench_common::contract::ProfilePurpose;

use crate::profile_materialization::{
    check_scenario_profile, render_scenario_profile, write_scenario_profile, ArtifactPolicy,
    ImagePolicy, ScenarioProfilePolicy,
};
use crate::profile_runtime_policy::{LAKECAT_IMAGE, MINIO_IMAGE};

const MATERIALIZED_COMPONENTS: &[&str] = &[
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

const BENCH_ARTIFACTS: &[ArtifactPolicy] = &[ArtifactPolicy {
    location: "image:/usr/local/bin/catalog-bench-commit",
    media_type: "application/vnd.elf",
}];
const MATERIALIZED_IMAGES: &[ImagePolicy] = &[
    ImagePolicy {
        component: "catalog-bench-commit",
        compose_service: "bench",
        required_artifacts: BENCH_ARTIFACTS,
        required_labels: &[],
        build_extension_label: None,
    },
    MINIO_IMAGE,
    LAKECAT_IMAGE,
];

const POLICY: ScenarioProfilePolicy = ScenarioProfilePolicy {
    name: "contention",
    materialization_format: "catalog-bench/contention-profile-materialization/v1",
    scope: "iceberg-rest.commit.same-table-contention/v2",
    purpose: ProfilePurpose::Performance,
    selected_components: MATERIALIZED_COMPONENTS,
    images: MATERIALIZED_IMAGES,
};

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
    render_scenario_profile(source_profile_bytes, materialization_bytes, &POLICY)
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
    write_scenario_profile(source_profile, materialization, output, &POLICY)
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
    check_scenario_profile(
        source_profile,
        materialization,
        output,
        &POLICY,
        "catalog-bench-contract profile materialize-contention",
    )
}
