//! Trino-specific policy for the shared scenario-profile materializer.

use std::path::Path;

use anyhow::Result;
use catalog_bench_common::contract::ProfilePurpose;

use crate::profile_materialization::{
    check_scenario_profile, render_scenario_profile, write_scenario_profile, ArtifactCopyPolicy,
    ArtifactPolicy, ImagePolicy, RequiredLabelPolicy, ScenarioProfilePolicy,
};
use crate::profile_runtime_policy::{LAKECAT_IMAGE, MINIO_IMAGE};

const TRINO_BASE_DIGEST: &str =
    "sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534";
const ENGINE_RUNNER_SOURCE_REVISION: &str = "6ea0f803e4c99a0eeb90c9303c038c97567698db";

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
    "trino",
];

const RUNNER_ARTIFACTS: &[ArtifactPolicy] = &[ArtifactPolicy {
    location: "image:/usr/local/bin/catalog-bench-engine",
    media_type: "application/vnd.elf",
}];

const ICEBERG_ARTIFACTS: &[ArtifactPolicy] = &[
    ArtifactPolicy {
        location: "image:/usr/lib/trino/plugin/iceberg/org.apache.iceberg_iceberg-core-1.11.0.jar",
        media_type: "application/java-archive",
    },
    ArtifactPolicy {
        location: "image:/usr/lib/trino/plugin/iceberg/org.apache.iceberg_iceberg-aws-1.11.0.jar",
        media_type: "application/java-archive",
    },
];

const TRINO_ARTIFACTS: &[ArtifactPolicy] = &[
    ArtifactPolicy {
        location: "image:/usr/lib/trino/bin/run-trino",
        media_type: "application/x-shellscript",
    },
    ArtifactPolicy {
        location: "image:/usr/lib/trino/bin/launcher",
        media_type: "application/x-shellscript",
    },
    ArtifactPolicy {
        location: "image:/usr/lib/trino/bin/linux-arm64/launcher",
        media_type: "application/vnd.elf",
    },
    ArtifactPolicy {
        location: "image:/usr/bin/trino",
        media_type: "application/java-archive",
    },
    ArtifactPolicy {
        location: "image:/usr/lib/trino/plugin/iceberg/org.apache.iceberg_iceberg-core-1.11.0.jar",
        media_type: "application/java-archive",
    },
    ArtifactPolicy {
        location: "image:/usr/lib/trino/plugin/iceberg/org.apache.iceberg_iceberg-aws-1.11.0.jar",
        media_type: "application/java-archive",
    },
    ArtifactPolicy {
        location: "image:/usr/local/bin/catalog-bench-engine",
        media_type: "application/vnd.elf",
    },
];

const ARTIFACT_COPIES: &[ArtifactCopyPolicy] = &[
    ArtifactCopyPolicy {
        source_component: "catalog-bench-engine",
        source_location: "image:/usr/local/bin/catalog-bench-engine",
        destination_component: "trino",
        destination_location: "image:/usr/local/bin/catalog-bench-engine",
    },
    ArtifactCopyPolicy {
        source_component: "iceberg-java",
        source_location:
            "image:/usr/lib/trino/plugin/iceberg/org.apache.iceberg_iceberg-core-1.11.0.jar",
        destination_component: "trino",
        destination_location:
            "image:/usr/lib/trino/plugin/iceberg/org.apache.iceberg_iceberg-core-1.11.0.jar",
    },
    ArtifactCopyPolicy {
        source_component: "iceberg-java",
        source_location:
            "image:/usr/lib/trino/plugin/iceberg/org.apache.iceberg_iceberg-aws-1.11.0.jar",
        destination_component: "trino",
        destination_location:
            "image:/usr/lib/trino/plugin/iceberg/org.apache.iceberg_iceberg-aws-1.11.0.jar",
    },
];

const RUNNER_LABELS: &[RequiredLabelPolicy] = &[RequiredLabelPolicy {
    label: "org.opencontainers.image.revision",
    value: ENGINE_RUNNER_SOURCE_REVISION,
}];

const ICEBERG_LABELS: &[RequiredLabelPolicy] = &[RequiredLabelPolicy {
    label: "org.opencontainers.image.version",
    value: "1.11.0",
}];

const TRINO_LABELS: &[RequiredLabelPolicy] = &[
    RequiredLabelPolicy {
        label: "org.opencontainers.image.version",
        value: "483",
    },
    RequiredLabelPolicy {
        label: "org.opencontainers.image.base.digest",
        value: TRINO_BASE_DIGEST,
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
        compose_service: "trino-engine-runner-base",
        required_artifacts: RUNNER_ARTIFACTS,
        required_labels: RUNNER_LABELS,
        build_extension_label: None,
    },
    ImagePolicy {
        component: "iceberg-java",
        compose_service: "trino-iceberg-runtime",
        required_artifacts: ICEBERG_ARTIFACTS,
        required_labels: ICEBERG_LABELS,
        build_extension_label: None,
    },
    ImagePolicy {
        component: "trino",
        compose_service: "trino",
        required_artifacts: TRINO_ARTIFACTS,
        required_labels: TRINO_LABELS,
        build_extension_label: None,
    },
];

const POLICY: ScenarioProfilePolicy = ScenarioProfilePolicy {
    name: "Trino interoperability",
    materialization_format: "catalog-bench/trino-profile-materialization/v1",
    scope: "engine.iceberg.write-read-evolution/v2",
    purpose: ProfilePurpose::Conformance,
    selected_components: MATERIALIZED_COMPONENTS,
    images: MATERIALIZED_IMAGES,
    artifact_copies: ARTIFACT_COPIES,
};

pub fn render_trino_profile(
    source_profile_bytes: &[u8],
    materialization_bytes: &[u8],
) -> Result<Vec<u8>> {
    render_scenario_profile(source_profile_bytes, materialization_bytes, &POLICY)
}

pub fn write_trino_profile(
    source_profile: &Path,
    materialization: &Path,
    output: &Path,
) -> Result<()> {
    write_scenario_profile(source_profile, materialization, output, &POLICY)
}

pub fn check_trino_profile(
    source_profile: &Path,
    materialization: &Path,
    output: &Path,
) -> Result<()> {
    check_scenario_profile(
        source_profile,
        materialization,
        output,
        &POLICY,
        "catalog-bench-contract profile materialize-trino",
    )
}
