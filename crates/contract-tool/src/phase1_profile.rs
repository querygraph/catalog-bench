//! Phase 1 behavioral/publication policy for the shared profile materializer.

use std::path::Path;

use anyhow::Result;
use catalog_bench_common::contract::ProfilePurpose;

use crate::profile_materialization::{
    check_scenario_profile, render_scenario_profile, write_scenario_profile, ArtifactPolicy,
    ImagePolicy, ScenarioProfilePolicy,
};
use crate::profile_runtime_policy::{LAKECAT_IMAGE, MINIO_IMAGE};

const COMPONENTS: &[&str] = &[
    "rust-runner",
    "catalog-bench-conformance",
    "minio",
    "lakecat",
    "lakecat-turso",
    "polaris",
    "gravitino",
    "lakekeeper",
    "nessie",
    "postgresql",
    "cpython",
    "pyiceberg",
    "pyarrow",
    "s3fs",
];

const CONFORMANCE_ARTIFACTS: &[ArtifactPolicy] = &[ArtifactPolicy {
    location: "image:/usr/local/bin/catalog-bench-conformance",
    media_type: "application/vnd.elf",
}];

const IMAGES: &[ImagePolicy] = &[
    ImagePolicy {
        component: "catalog-bench-conformance",
        compose_service: "bench",
        required_artifacts: CONFORMANCE_ARTIFACTS,
        required_labels: &[],
        build_extension_label: None,
    },
    MINIO_IMAGE,
    LAKECAT_IMAGE,
];

const POLICY: ScenarioProfilePolicy = ScenarioProfilePolicy {
    name: "phase1",
    materialization_format: "catalog-bench/phase1-profile-materialization/v1",
    scope: "phase1-behavioral-publication/v1",
    purpose: ProfilePurpose::Conformance,
    selected_components: COMPONENTS,
    images: IMAGES,
    artifact_copies: &[],
};

pub fn render_phase1_profile(source: &[u8], materialization: &[u8]) -> Result<Vec<u8>> {
    render_scenario_profile(source, materialization, &POLICY)
}

pub fn write_phase1_profile(source: &Path, materialization: &Path, output: &Path) -> Result<()> {
    write_scenario_profile(source, materialization, output, &POLICY)
}

pub fn check_phase1_profile(source: &Path, materialization: &Path, output: &Path) -> Result<()> {
    check_scenario_profile(
        source,
        materialization,
        output,
        &POLICY,
        "catalog-bench-contract profile materialize-phase1",
    )
}
