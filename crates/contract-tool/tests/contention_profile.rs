use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{Context, Result};
use catalog_bench_common::contract::{
    parse_contract, ContractDocument, ImageDigestScope, ProfilePurpose, ProfileReadiness,
    RuntimeArtifact,
};
use catalog_bench_contract::{check_contention_profile, render_contention_profile};
use serde_json::Value;

const SOURCE_PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-26.json");
const MATERIALIZATION: &[u8] =
    include_bytes!("../../../materializations/v1/contention-2026-08-27.json");
const RUNNABLE_PROFILE: &[u8] = include_bytes!("../../../profiles/v1/contention-2026-08-27.json");

#[test]
fn checked_in_profile_exactly_matches_its_materialization_inputs() -> Result<()> {
    let root = repository_root();
    check_contention_profile(
        &root.join("profiles/v1/current-2026-08-26.json"),
        &root.join("materializations/v1/contention-2026-08-27.json"),
        &root.join("profiles/v1/contention-2026-08-27.json"),
    )
}

#[test]
fn materialized_profile_is_runnable_and_scenario_scoped() -> Result<()> {
    let rendered = render_contention_profile(SOURCE_PROFILE, MATERIALIZATION)?;
    assert_eq!(rendered, RUNNABLE_PROFILE);

    let ContractDocument::Profile(profile) = parse_contract(&rendered)? else {
        panic!("materializer must produce a profile document");
    };
    assert_eq!(
        profile.id.as_str(),
        "catalog-community-contention-2026-08-27-linux-arm64"
    );
    assert_eq!(profile.purpose, ProfilePurpose::Performance);
    assert_eq!(profile.readiness, ProfileReadiness::Runnable);

    let expected_components = BTreeSet::from([
        "catalog-bench-commit",
        "gravitino",
        "lakecat",
        "lakecat-turso",
        "lakekeeper",
        "minio",
        "nessie",
        "polaris",
        "postgresql",
        "rust-runner",
    ]);
    let actual_components = profile
        .components
        .iter()
        .map(|component| component.id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_components, expected_components);

    let expected_catalogs =
        BTreeSet::from(["gravitino", "lakecat", "lakekeeper", "nessie", "polaris"]);
    let actual_catalogs = profile
        .catalog_adapters
        .iter()
        .map(|adapter| adapter.catalog.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_catalogs, expected_catalogs);

    for component_id in ["catalog-bench-commit", "minio", "lakecat"] {
        let component = profile
            .components
            .iter()
            .find(|component| component.id.as_str() == component_id)
            .with_context(|| format!("profile omits {component_id}"))?;
        let RuntimeArtifact::ContainerImage {
            digest_scope,
            platform_digest,
            embedded_artifacts,
            ..
        } = &component.artifact
        else {
            panic!("{component_id} must be represented by its observed image");
        };
        assert_eq!(*digest_scope, ImageDigestScope::LocalImage);
        assert!(platform_digest.is_none());
        assert!(!embedded_artifacts.is_empty());
    }
    Ok(())
}

#[test]
fn source_profile_byte_drift_is_rejected() {
    let mut drifted_source = SOURCE_PROFILE.to_vec();
    drifted_source.push(b'\n');

    let error = render_contention_profile(&drifted_source, MATERIALIZATION)
        .expect_err("source bytes outside the observed digest must fail closed");
    assert!(format!("{error:#}").contains("source profile digest mismatch"));
}

#[test]
fn incomplete_or_misattributed_image_evidence_is_rejected() -> Result<()> {
    let mut missing_artifact: Value = serde_json::from_slice(MATERIALIZATION)?;
    let artifacts = image_mut(&mut missing_artifact, "minio")?["embedded_artifacts"]
        .as_array_mut()
        .context("MinIO embedded_artifacts must be an array")?;
    let index = artifacts
        .iter()
        .position(|artifact| artifact["location"] == "image:/usr/local/bin/wait-http")
        .context("fixture must contain wait-http")?;
    artifacts.remove(index);
    let error = render_contention_profile(SOURCE_PROFILE, &serde_json::to_vec(&missing_artifact)?)
        .expect_err("a required image executable must not be omitted");
    assert!(format!("{error:#}")
        .contains("image `minio` omits required artifact `image:/usr/local/bin/wait-http`"));

    let mut wrong_source: Value = serde_json::from_slice(MATERIALIZATION)?;
    image_mut(&mut wrong_source, "lakecat")?["labels"]["org.opencontainers.image.revision"] =
        Value::String("0".repeat(40));
    let error = render_contention_profile(SOURCE_PROFILE, &serde_json::to_vec(&wrong_source)?)
        .expect_err("an image from a different source revision must fail closed");
    let message = format!("{error:#}");
    assert!(message.contains("image `lakecat` label `org.opencontainers.image.revision`"));
    assert!(message.contains("expected `962f43cb2d2f345addf188e63be0cf6059bc26b0`"));
    Ok(())
}

fn image_mut<'a>(materialization: &'a mut Value, component: &str) -> Result<&'a mut Value> {
    materialization["images"]
        .as_array_mut()
        .context("materialization images must be an array")?
        .iter_mut()
        .find(|image| image["component"] == component)
        .with_context(|| format!("materialization omits {component}"))
}

fn repository_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("contract-tool crate is nested two levels below the repository root")
}
