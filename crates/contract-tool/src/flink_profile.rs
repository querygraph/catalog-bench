//! Flink-specific policy for the shared scenario-profile materializer.

use std::path::Path;

use anyhow::Result;
use catalog_bench_common::contract::ProfilePurpose;

use crate::profile_materialization::{
    check_scenario_profile, render_scenario_profile, write_scenario_profile, ArtifactCopyPolicy,
    ArtifactPolicy, ImagePolicy, RequiredLabelPolicy, ScenarioProfilePolicy,
};
use crate::profile_runtime_policy::{LAKECAT_IMAGE, MINIO_IMAGE};

const FLINK_BASE_PLATFORM_DIGEST: &str =
    "sha256:99a499ed147b28d358486066ab8308e351b232b2ac81aff69157fdb349c84e18";
const ICEBERG_SOURCE_REVISION: &str = "6976e020b894f6a6777704df2b8c4458cb291ae9";
const ENGINE_RUNNER_SOURCE_REVISION: &str = "df3a68da787de82ae83d1a5034228b731f3bc588";

const MATERIALIZED_COMPONENTS: &[&str] = &[
    "rust-runner",
    "catalog-bench-engine",
    "minio",
    "lakecat",
    "lakecat-turso",
    "polaris",
    "gravitino",
    "lakekeeper",
    "postgresql",
    "iceberg-java",
    "flink",
];

const ENGINE_RUNNER_ARTIFACTS: &[ArtifactPolicy] = &[
    ArtifactPolicy {
        location: "image:/usr/local/bin/catalog-bench-engine",
        media_type: "application/vnd.elf",
    },
    ArtifactPolicy {
        location: "image:/opt/catalog-bench/catalog-bench-flink-runner.jar",
        media_type: "application/java-archive",
    },
];

const ICEBERG_ARTIFACTS: &[ArtifactPolicy] = &[
    ArtifactPolicy {
        location: "image:/opt/iceberg/iceberg-flink-runtime-2.1-1.11.0.jar",
        media_type: "application/java-archive",
    },
    ArtifactPolicy {
        location: "image:/opt/iceberg/iceberg-aws-bundle-1.11.0.jar",
        media_type: "application/java-archive",
    },
];

const FLINK_ARTIFACTS: &[ArtifactPolicy] = &[
    ArtifactPolicy {
        location: "image:/opt/flink/bin/flink",
        media_type: "application/x-shellscript",
    },
    ArtifactPolicy {
        location: "image:/opt/flink/lib/flink-dist-2.1.3.jar",
        media_type: "application/java-archive",
    },
    ArtifactPolicy {
        location: "image:/opt/flink/lib/iceberg-flink-runtime-2.1-1.11.0.jar",
        media_type: "application/java-archive",
    },
    ArtifactPolicy {
        location: "image:/opt/flink/lib/iceberg-aws-bundle-1.11.0.jar",
        media_type: "application/java-archive",
    },
    ArtifactPolicy {
        location: "image:/usr/local/bin/catalog-bench-engine",
        media_type: "application/vnd.elf",
    },
    ArtifactPolicy {
        location: "image:/opt/catalog-bench/catalog-bench-flink-runner.jar",
        media_type: "application/java-archive",
    },
];

const ARTIFACT_COPIES: &[ArtifactCopyPolicy] = &[
    ArtifactCopyPolicy {
        source_component: "iceberg-java",
        source_location: "image:/opt/iceberg/iceberg-flink-runtime-2.1-1.11.0.jar",
        destination_component: "flink",
        destination_location: "image:/opt/flink/lib/iceberg-flink-runtime-2.1-1.11.0.jar",
    },
    ArtifactCopyPolicy {
        source_component: "iceberg-java",
        source_location: "image:/opt/iceberg/iceberg-aws-bundle-1.11.0.jar",
        destination_component: "flink",
        destination_location: "image:/opt/flink/lib/iceberg-aws-bundle-1.11.0.jar",
    },
    ArtifactCopyPolicy {
        source_component: "catalog-bench-engine",
        source_location: "image:/usr/local/bin/catalog-bench-engine",
        destination_component: "flink",
        destination_location: "image:/usr/local/bin/catalog-bench-engine",
    },
    ArtifactCopyPolicy {
        source_component: "catalog-bench-engine",
        source_location: "image:/opt/catalog-bench/catalog-bench-flink-runner.jar",
        destination_component: "flink",
        destination_location: "image:/opt/catalog-bench/catalog-bench-flink-runner.jar",
    },
];

const RUNNER_LABELS: &[RequiredLabelPolicy] = &[RequiredLabelPolicy {
    label: "io.querygraph.catalog-bench.runner-source-revision",
    value: ENGINE_RUNNER_SOURCE_REVISION,
}];

const ICEBERG_LABELS: &[RequiredLabelPolicy] = &[
    RequiredLabelPolicy {
        label: "org.opencontainers.image.version",
        value: "1.11.0",
    },
    RequiredLabelPolicy {
        label: "io.querygraph.catalog-bench.iceberg-flink-runtime-coordinate",
        value: "org.apache.iceberg:iceberg-flink-runtime-2.1:1.11.0",
    },
    RequiredLabelPolicy {
        label: "io.querygraph.catalog-bench.iceberg-aws-bundle-coordinate",
        value: "org.apache.iceberg:iceberg-aws-bundle:1.11.0",
    },
];

const FLINK_LABELS: &[RequiredLabelPolicy] = &[
    RequiredLabelPolicy {
        label: "org.opencontainers.image.version",
        value: "2.1.3",
    },
    RequiredLabelPolicy {
        label: "org.opencontainers.image.base.digest",
        value: FLINK_BASE_PLATFORM_DIGEST,
    },
    RequiredLabelPolicy {
        label: "io.querygraph.catalog-bench.iceberg-source-revision",
        value: ICEBERG_SOURCE_REVISION,
    },
    RequiredLabelPolicy {
        label: "io.querygraph.catalog-bench.runner-source-revision",
        value: ENGINE_RUNNER_SOURCE_REVISION,
    },
];

const MATERIALIZED_IMAGES: &[ImagePolicy] = &[
    MINIO_IMAGE,
    LAKECAT_IMAGE,
    ImagePolicy {
        component: "catalog-bench-engine",
        compose_service: "flink-runner-image",
        required_artifacts: ENGINE_RUNNER_ARTIFACTS,
        required_labels: RUNNER_LABELS,
        build_extension_label: None,
    },
    ImagePolicy {
        component: "iceberg-java",
        compose_service: "iceberg-flink-runtime",
        required_artifacts: ICEBERG_ARTIFACTS,
        required_labels: ICEBERG_LABELS,
        build_extension_label: None,
    },
    ImagePolicy {
        component: "flink",
        compose_service: "flink",
        required_artifacts: FLINK_ARTIFACTS,
        required_labels: FLINK_LABELS,
        build_extension_label: None,
    },
];

const POLICY: ScenarioProfilePolicy = ScenarioProfilePolicy {
    name: "Flink interoperability",
    materialization_format: "catalog-bench/flink-profile-materialization/v1",
    scope: "engine.iceberg.write-read-evolution/v2",
    purpose: ProfilePurpose::Conformance,
    selected_components: MATERIALIZED_COMPONENTS,
    images: MATERIALIZED_IMAGES,
    artifact_copies: ARTIFACT_COPIES,
};

/// Render the runnable Flink interoperability profile.
///
/// # Errors
///
/// Returns an error when source, image, connector, topology, or output evidence
/// violates the Flink scenario policy or common profile contract.
pub fn render_flink_profile(
    source_profile_bytes: &[u8],
    materialization_bytes: &[u8],
) -> Result<Vec<u8>> {
    render_scenario_profile(source_profile_bytes, materialization_bytes, &POLICY)
}

/// Write the deterministically rendered Flink interoperability profile.
///
/// # Errors
///
/// Returns an error when an input cannot be read or validated or output cannot
/// be written.
pub fn write_flink_profile(
    source_profile: &Path,
    materialization: &Path,
    output: &Path,
) -> Result<()> {
    write_scenario_profile(source_profile, materialization, output, &POLICY)
}

/// Check a Flink interoperability profile against its authoritative inputs.
///
/// # Errors
///
/// Returns an error when an input is invalid or the checked-in output has
/// drifted by even one byte.
pub fn check_flink_profile(
    source_profile: &Path,
    materialization: &Path,
    output: &Path,
) -> Result<()> {
    check_scenario_profile(
        source_profile,
        materialization,
        output,
        &POLICY,
        "catalog-bench-contract profile materialize-flink",
    )
}
