//! Spark-specific policy for the shared scenario-profile materializer.

use std::path::Path;

use anyhow::{bail, Result};
use catalog_bench_common::contract::ProfilePurpose;

use crate::profile_materialization::{
    check_scenario_profile, render_scenario_profile, write_scenario_profile, ArtifactCopyPolicy,
    ArtifactPolicy, ImagePolicy, RequiredLabelPolicy, ScenarioProfilePolicy,
};
use crate::profile_runtime_policy::{LAKECAT_IMAGE, MINIO_IMAGE};

const SPARK_BASE_PLATFORM_DIGEST: &str =
    "sha256:f6831c619d0f6f07fe41912a5be499f6a7c0c1e9f18322d0c703ff21d2f30cd1";
const ICEBERG_SOURCE_REVISION: &str = "6976e020b894f6a6777704df2b8c4458cb291ae9";
const ENGINE_RUNNER_SOURCE_REVISION_V1: &str = "5e10f36e7e99815df273c7b567e466749f04d4be";
const ENGINE_RUNNER_SOURCE_REVISION_V2: &str = "59840b95c33e753919f5c984d10d6df45c834243";
const ENGINE_V2_SOURCE_PROFILE_ID: &str =
    "catalog-community-engine-v2-source-2026-08-28-linux-arm64";
const ENGINE_V2_LAKECAT_SOURCE_PROFILE_ID: &str =
    "catalog-community-engine-v2-lakecat-5d62f1c4-source-2026-08-28-linux-arm64";
const ENGINE_V2_MULTIPART_SOURCE_PROFILE_ID: &str =
    "catalog-community-engine-v2-lakecat-65f0a4c3-source-2026-08-28-linux-arm64";

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
    "spark-4.1",
];

const ENGINE_RUNNER_ARTIFACTS: &[ArtifactPolicy] = &[ArtifactPolicy {
    location: "image:/usr/local/bin/catalog-bench-engine",
    media_type: "application/vnd.elf",
}];

const ICEBERG_ARTIFACTS: &[ArtifactPolicy] = &[
    ArtifactPolicy {
        location: "image:/opt/iceberg/iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
        media_type: "application/java-archive",
    },
    ArtifactPolicy {
        location: "image:/opt/iceberg/iceberg-aws-bundle-1.11.0.jar",
        media_type: "application/java-archive",
    },
];

const SPARK_ARTIFACTS: &[ArtifactPolicy] = &[
    ArtifactPolicy {
        location: "image:/opt/spark/bin/spark-submit",
        media_type: "application/x-shellscript",
    },
    ArtifactPolicy {
        location: "image:/opt/spark/jars/spark-sql_2.13-4.1.3.jar",
        media_type: "application/java-archive",
    },
    ArtifactPolicy {
        location: "image:/opt/spark/jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
        media_type: "application/java-archive",
    },
    ArtifactPolicy {
        location: "image:/opt/spark/jars/iceberg-aws-bundle-1.11.0.jar",
        media_type: "application/java-archive",
    },
    ArtifactPolicy {
        location: "image:/usr/local/bin/catalog-bench-engine",
        media_type: "application/vnd.elf",
    },
];

const ARTIFACT_COPIES: &[ArtifactCopyPolicy] = &[
    ArtifactCopyPolicy {
        source_component: "iceberg-java",
        source_location: "image:/opt/iceberg/iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
        destination_component: "spark-4.1",
        destination_location: "image:/opt/spark/jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
    },
    ArtifactCopyPolicy {
        source_component: "iceberg-java",
        source_location: "image:/opt/iceberg/iceberg-aws-bundle-1.11.0.jar",
        destination_component: "spark-4.1",
        destination_location: "image:/opt/spark/jars/iceberg-aws-bundle-1.11.0.jar",
    },
    ArtifactCopyPolicy {
        source_component: "catalog-bench-engine",
        source_location: "image:/usr/local/bin/catalog-bench-engine",
        destination_component: "spark-4.1",
        destination_location: "image:/usr/local/bin/catalog-bench-engine",
    },
];

const ICEBERG_LABELS: &[RequiredLabelPolicy] = &[
    RequiredLabelPolicy {
        label: "org.opencontainers.image.version",
        value: "1.11.0",
    },
    RequiredLabelPolicy {
        label: "io.querygraph.catalog-bench.iceberg-spark-runtime-coordinate",
        value: "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
    },
    RequiredLabelPolicy {
        label: "io.querygraph.catalog-bench.iceberg-aws-bundle-coordinate",
        value: "org.apache.iceberg:iceberg-aws-bundle:1.11.0",
    },
];

const SPARK_LABELS_V1: &[RequiredLabelPolicy] = &[
    RequiredLabelPolicy {
        label: "org.opencontainers.image.version",
        value: "4.1.3",
    },
    RequiredLabelPolicy {
        label: "org.opencontainers.image.base.digest",
        value: SPARK_BASE_PLATFORM_DIGEST,
    },
    RequiredLabelPolicy {
        label: "io.querygraph.catalog-bench.iceberg-source-revision",
        value: ICEBERG_SOURCE_REVISION,
    },
    RequiredLabelPolicy {
        label: "io.querygraph.catalog-bench.runner-source-revision",
        value: ENGINE_RUNNER_SOURCE_REVISION_V1,
    },
];

const SPARK_LABELS_V2: &[RequiredLabelPolicy] = &[
    RequiredLabelPolicy {
        label: "org.opencontainers.image.version",
        value: "4.1.3",
    },
    RequiredLabelPolicy {
        label: "org.opencontainers.image.base.digest",
        value: SPARK_BASE_PLATFORM_DIGEST,
    },
    RequiredLabelPolicy {
        label: "io.querygraph.catalog-bench.iceberg-source-revision",
        value: ICEBERG_SOURCE_REVISION,
    },
    RequiredLabelPolicy {
        label: "io.querygraph.catalog-bench.runner-source-revision",
        value: ENGINE_RUNNER_SOURCE_REVISION_V2,
    },
];

const MATERIALIZED_IMAGES_V1: &[ImagePolicy] = &[
    MINIO_IMAGE,
    LAKECAT_IMAGE,
    ImagePolicy {
        component: "catalog-bench-engine",
        compose_service: "engine-runner-image",
        required_artifacts: ENGINE_RUNNER_ARTIFACTS,
        required_labels: &[],
        build_extension_label: None,
    },
    ImagePolicy {
        component: "iceberg-java",
        compose_service: "iceberg-spark-runtime",
        required_artifacts: ICEBERG_ARTIFACTS,
        required_labels: ICEBERG_LABELS,
        build_extension_label: None,
    },
    ImagePolicy {
        component: "spark-4.1",
        compose_service: "spark",
        required_artifacts: SPARK_ARTIFACTS,
        required_labels: SPARK_LABELS_V1,
        build_extension_label: None,
    },
];

const MATERIALIZED_IMAGES_V2: &[ImagePolicy] = &[
    MINIO_IMAGE,
    LAKECAT_IMAGE,
    ImagePolicy {
        component: "catalog-bench-engine",
        compose_service: "engine-runner-image",
        required_artifacts: ENGINE_RUNNER_ARTIFACTS,
        required_labels: &[],
        build_extension_label: None,
    },
    ImagePolicy {
        component: "iceberg-java",
        compose_service: "iceberg-spark-runtime",
        required_artifacts: ICEBERG_ARTIFACTS,
        required_labels: ICEBERG_LABELS,
        build_extension_label: None,
    },
    ImagePolicy {
        component: "spark-4.1",
        compose_service: "spark",
        required_artifacts: SPARK_ARTIFACTS,
        required_labels: SPARK_LABELS_V2,
        build_extension_label: None,
    },
];

const POLICY_V1: ScenarioProfilePolicy = ScenarioProfilePolicy {
    name: "Spark interoperability",
    materialization_format: "catalog-bench/spark-profile-materialization/v1",
    scope: "engine.iceberg.write-read-evolution/v1",
    purpose: ProfilePurpose::Conformance,
    selected_components: MATERIALIZED_COMPONENTS,
    images: MATERIALIZED_IMAGES_V1,
    artifact_copies: ARTIFACT_COPIES,
};

const POLICY_V2: ScenarioProfilePolicy = ScenarioProfilePolicy {
    name: "Spark interoperability v2",
    materialization_format: "catalog-bench/spark-profile-materialization/v1",
    scope: "engine.iceberg.write-read-evolution/v2",
    purpose: ProfilePurpose::Conformance,
    selected_components: MATERIALIZED_COMPONENTS,
    images: MATERIALIZED_IMAGES_V2,
    artifact_copies: ARTIFACT_COPIES,
};

/// Render the runnable Spark interoperability profile.
///
/// # Errors
///
/// Returns an error when source, image, connector, topology, or output evidence
/// violates the Spark scenario policy or common profile contract.
pub fn render_spark_profile(
    source_profile_bytes: &[u8],
    materialization_bytes: &[u8],
) -> Result<Vec<u8>> {
    render_scenario_profile(
        source_profile_bytes,
        materialization_bytes,
        policy_for_source(source_profile_bytes)?,
    )
}

/// Write the deterministically rendered Spark interoperability profile.
///
/// # Errors
///
/// Returns an error when an input cannot be read or validated or output cannot
/// be written.
pub fn write_spark_profile(
    source_profile: &Path,
    materialization: &Path,
    output: &Path,
) -> Result<()> {
    let source = std::fs::read(source_profile)?;
    write_scenario_profile(
        source_profile,
        materialization,
        output,
        policy_for_source(&source)?,
    )
}

/// Check a Spark interoperability profile against its authoritative inputs.
///
/// # Errors
///
/// Returns an error when an input is invalid or the checked-in output has
/// drifted by even one byte.
pub fn check_spark_profile(
    source_profile: &Path,
    materialization: &Path,
    output: &Path,
) -> Result<()> {
    let source = std::fs::read(source_profile)?;
    check_scenario_profile(
        source_profile,
        materialization,
        output,
        policy_for_source(&source)?,
        "catalog-bench-contract profile materialize-spark",
    )
}

fn policy_for_source(source: &[u8]) -> Result<&'static ScenarioProfilePolicy> {
    let document: serde_json::Value = serde_json::from_slice(source)?;
    match document.get("id").and_then(serde_json::Value::as_str) {
        Some(
            ENGINE_V2_SOURCE_PROFILE_ID
            | ENGINE_V2_LAKECAT_SOURCE_PROFILE_ID
            | ENGINE_V2_MULTIPART_SOURCE_PROFILE_ID,
        ) => Ok(&POLICY_V2),
        Some("catalog-community-current-2026-08-27-linux-arm64") => Ok(&POLICY_V1),
        _ => bail!("unsupported Spark source profile identity"),
    }
}
