//! Shared image policies for scenario profiles that reuse the catalog stack.

use crate::profile_materialization::{ArtifactPolicy, BuildExtensionLabelPolicy, ImagePolicy};

const MINIO_ARTIFACTS: &[ArtifactPolicy] = &[
    ArtifactPolicy {
        location: "image:/usr/local/bin/minio",
        media_type: "application/vnd.elf",
    },
    ArtifactPolicy {
        location: "image:/usr/local/bin/ensure-bucket",
        media_type: "application/vnd.elf",
    },
    ArtifactPolicy {
        location: "image:/usr/local/bin/healthcheck",
        media_type: "application/vnd.elf",
    },
    ArtifactPolicy {
        location: "image:/usr/local/bin/lakekeeper-setup",
        media_type: "application/vnd.elf",
    },
    ArtifactPolicy {
        location: "image:/usr/local/bin/polaris-setup",
        media_type: "application/vnd.elf",
    },
    ArtifactPolicy {
        location: "image:/usr/local/bin/wait-http",
        media_type: "application/vnd.elf",
    },
];

const LAKECAT_ARTIFACTS: &[ArtifactPolicy] = &[ArtifactPolicy {
    location: "image:/usr/local/bin/lakecat-service",
    media_type: "application/vnd.elf",
}];

pub(crate) const MINIO_IMAGE: ImagePolicy = ImagePolicy {
    component: "minio",
    compose_service: "minio",
    required_artifacts: MINIO_ARTIFACTS,
    required_labels: &[],
    build_extension_label: Some(BuildExtensionLabelPolicy {
        label: "io.querygraph.catalog-bench.helper-source-revision",
        extension: "querygraph/helper-source",
        field: "revision",
    }),
};

pub(crate) const LAKECAT_IMAGE: ImagePolicy = ImagePolicy {
    component: "lakecat",
    compose_service: "lakecat",
    required_artifacts: LAKECAT_ARTIFACTS,
    required_labels: &[],
    build_extension_label: None,
};
