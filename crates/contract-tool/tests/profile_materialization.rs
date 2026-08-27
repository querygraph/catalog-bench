use std::collections::BTreeSet;

use anyhow::{Context, Result};
use catalog_bench_common::contract::{
    parse_contract, ContractDocument, ImageDigestScope, ProfilePurpose, ProfileReadiness,
    RuntimeArtifact,
};
use catalog_bench_contract::{
    render_scenario_profile, ArtifactPolicy, BuildExtensionLabelPolicy, ImagePolicy,
    RequiredLabelPolicy, ScenarioProfilePolicy,
};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};

const SOURCE_PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-26.json");
const CONTENTION_MATERIALIZATION: &[u8] =
    include_bytes!("../../../materializations/v1/contention-2026-08-27.json");

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
const IMAGES: &[ImagePolicy] = &[
    ImagePolicy {
        component: "minio",
        compose_service: "minio",
        required_artifacts: MINIO_ARTIFACTS,
        required_labels: &[],
        build_extension_label: Some(BuildExtensionLabelPolicy {
            label: "io.querygraph.catalog-bench.helper-source-revision",
            extension: "querygraph/helper-source",
            field: "revision",
        }),
    },
    ImagePolicy {
        component: "lakecat",
        compose_service: "lakecat",
        required_artifacts: LAKECAT_ARTIFACTS,
        required_labels: &[],
        build_extension_label: None,
    },
];
const COMPONENTS: &[&str] = &["rust-runner", "minio", "lakecat", "lakecat-turso"];
const POLICY: ScenarioProfilePolicy = ScenarioProfilePolicy {
    name: "fixture",
    materialization_format: "catalog-bench/test-profile-materialization/v1",
    scope: "engine.iceberg.fixture/v1",
    purpose: ProfilePurpose::Conformance,
    selected_components: COMPONENTS,
    images: IMAGES,
};

fn fixture_materialization() -> Result<Vec<u8>> {
    let mut value: Value = serde_json::from_slice(CONTENTION_MATERIALIZATION)?;
    value["format"] = json!(POLICY.materialization_format);
    value["output_profile"] = json!({
        "id": "catalog-community-test-profile-linux-arm64",
        "title": "Test scenario profile",
        "description": "Synthetic projection proving the reusable materialization core.",
        "resolved_at": "2026-08-27T00:00:00Z"
    });
    value["images"]
        .as_array_mut()
        .context("contention materialization images must be an array")?
        .retain(|image| matches!(image["component"].as_str(), Some("minio" | "lakecat")));
    Ok(serde_json::to_vec(&value)?)
}

#[test]
fn reusable_policy_narrows_components_services_and_catalog_adapters() -> Result<()> {
    let materialization = fixture_materialization()?;
    let rendered = render_scenario_profile(SOURCE_PROFILE, &materialization, &POLICY)?;
    let ContractDocument::Profile(profile) = parse_contract(&rendered)? else {
        panic!("materializer must produce a profile");
    };

    assert_eq!(profile.purpose, ProfilePurpose::Conformance);
    assert_eq!(profile.readiness, ProfileReadiness::Runnable);
    assert_eq!(
        profile
            .components
            .iter()
            .map(|component| component.id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["lakecat", "lakecat-turso", "minio", "rust-runner"])
    );
    assert_eq!(
        profile
            .services
            .iter()
            .map(|service| service.component.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["lakecat", "minio"])
    );
    assert_eq!(profile.catalog_adapters.len(), 1);
    assert_eq!(profile.catalog_adapters[0].catalog.as_str(), "lakecat");
    assert_eq!(
        profile.extensions["querygraph/materialization"]["scope"],
        POLICY.scope
    );

    for component_id in ["minio", "lakecat"] {
        let component = profile
            .components
            .iter()
            .find(|component| component.id.as_str() == component_id)
            .unwrap();
        let RuntimeArtifact::ContainerImage { digest_scope, .. } = component.artifact else {
            panic!("{component_id} must be materialized as a local image");
        };
        assert_eq!(digest_scope, ImageDigestScope::LocalImage);
    }
    Ok(())
}

#[test]
fn policy_rejects_duplicate_unselected_or_ambiguous_entries() -> Result<()> {
    const DUPLICATE_COMPONENTS: &[&str] = &["rust-runner", "minio", "minio"];
    const DUPLICATE_POLICY: ScenarioProfilePolicy = ScenarioProfilePolicy {
        selected_components: DUPLICATE_COMPONENTS,
        ..POLICY
    };
    let materialization = fixture_materialization()?;
    let error = render_scenario_profile(SOURCE_PROFILE, &materialization, &DUPLICATE_POLICY)
        .expect_err("duplicate component policy must fail closed");
    assert!(error.to_string().contains("duplicate selected components"));

    const MINIO_ONLY: &[&str] = &["rust-runner", "minio"];
    const UNSELECTED_POLICY: ScenarioProfilePolicy = ScenarioProfilePolicy {
        selected_components: MINIO_ONLY,
        ..POLICY
    };
    let error = render_scenario_profile(SOURCE_PROFILE, &materialization, &UNSELECTED_POLICY)
        .expect_err("an image for an unselected component must fail closed");
    assert!(error
        .to_string()
        .contains("image policy targets unselected component `lakecat`"));

    const DUPLICATE_ARTIFACTS: &[ArtifactPolicy] = &[
        ArtifactPolicy {
            location: "image:/usr/local/bin/minio",
            media_type: "application/vnd.elf",
        },
        ArtifactPolicy {
            location: "image:/usr/local/bin/minio",
            media_type: "application/vnd.elf",
        },
    ];
    const DUPLICATE_ARTIFACT_IMAGES: &[ImagePolicy] = &[ImagePolicy {
        component: "minio",
        compose_service: "minio",
        required_artifacts: DUPLICATE_ARTIFACTS,
        required_labels: &[],
        build_extension_label: None,
    }];
    const DUPLICATE_ARTIFACT_POLICY: ScenarioProfilePolicy = ScenarioProfilePolicy {
        images: DUPLICATE_ARTIFACT_IMAGES,
        ..POLICY
    };
    let error =
        render_scenario_profile(SOURCE_PROFILE, &materialization, &DUPLICATE_ARTIFACT_POLICY)
            .expect_err("duplicate artifact requirements must fail closed");
    assert!(error
        .to_string()
        .contains("duplicate required artifact locations"));

    const DUPLICATE_LABELS: &[RequiredLabelPolicy] = &[
        RequiredLabelPolicy {
            label: "example.label",
            value: "one",
        },
        RequiredLabelPolicy {
            label: "example.label",
            value: "two",
        },
    ];
    const DUPLICATE_LABEL_IMAGES: &[ImagePolicy] = &[ImagePolicy {
        component: "minio",
        compose_service: "minio",
        required_artifacts: MINIO_ARTIFACTS,
        required_labels: DUPLICATE_LABELS,
        build_extension_label: None,
    }];
    const DUPLICATE_LABEL_POLICY: ScenarioProfilePolicy = ScenarioProfilePolicy {
        images: DUPLICATE_LABEL_IMAGES,
        ..POLICY
    };
    let error = render_scenario_profile(SOURCE_PROFILE, &materialization, &DUPLICATE_LABEL_POLICY)
        .expect_err("duplicate required labels must fail closed");
    assert!(error.to_string().contains("duplicate required labels"));
    Ok(())
}

#[test]
fn standard_host_and_docker_architecture_names_are_reconciled() -> Result<()> {
    let mut source: Value = serde_json::from_slice(SOURCE_PROFILE)?;
    source["platform"]["architecture"] = json!("x86_64");
    let source = serde_json::to_vec(&source)?;

    let mut materialization: Value = serde_json::from_slice(&fixture_materialization()?)?;
    materialization["source_profile"]["digest"]["value"] = json!(sha256_hex(&source));
    for image in materialization["images"]
        .as_array_mut()
        .expect("fixture images must be an array")
    {
        image["architecture"] = json!("amd64");
    }

    render_scenario_profile(&source, &serde_json::to_vec(&materialization)?, &POLICY)?;
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
