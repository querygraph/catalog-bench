use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{Context, Result};
use catalog_bench_common::contract::{
    parse_contract, ArtifactReference, ContractDocument, ImageDigestScope, ProfilePurpose,
    ProfileReadiness, RuntimeArtifact,
};
use catalog_bench_contract::{check_spark_profile, render_spark_profile};
use serde_json::{json, Value};

const SOURCE_PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-27.json");
const MATERIALIZATION: &[u8] =
    include_bytes!("../../../materializations/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const RUNNABLE_PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");

#[test]
fn checked_in_spark_profile_exactly_matches_its_inputs() -> Result<()> {
    let root = repository_root();
    check_spark_profile(
        &root.join("profiles/v1/current-2026-08-27.json"),
        &root.join("materializations/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json"),
        &root.join("profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json"),
    )
}

#[test]
fn spark_profile_is_runnable_common_and_connector_complete() -> Result<()> {
    let rendered = render_spark_profile(SOURCE_PROFILE, MATERIALIZATION)?;
    assert_eq!(rendered, RUNNABLE_PROFILE);

    let ContractDocument::Profile(profile) = parse_contract(&rendered)? else {
        panic!("Spark materializer must produce a profile");
    };
    assert_eq!(profile.purpose, ProfilePurpose::Conformance);
    assert_eq!(profile.readiness, ProfileReadiness::Runnable);
    assert_eq!(
        profile
            .components
            .iter()
            .map(|component| component.id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "gravitino",
            "iceberg-java",
            "catalog-bench-engine",
            "lakecat",
            "lakecat-turso",
            "lakekeeper",
            "minio",
            "polaris",
            "postgresql",
            "rust-runner",
            "spark-4.1",
        ])
    );
    assert_eq!(
        profile
            .catalog_adapters
            .iter()
            .map(|adapter| adapter.catalog.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["gravitino", "lakecat", "lakekeeper", "polaris"])
    );
    assert_eq!(
        profile.extensions["querygraph/materialization"]["scope"],
        "engine.iceberg.write-read-evolution/v1"
    );

    let artifacts = profile
        .components
        .iter()
        .filter(|component| {
            matches!(
                component.id.as_str(),
                "catalog-bench-engine" | "iceberg-java" | "spark-4.1"
            )
        })
        .map(|component| {
            let RuntimeArtifact::ContainerImage {
                digest_scope,
                embedded_artifacts,
                ..
            } = &component.artifact
            else {
                panic!("{} must be a materialized image", component.id);
            };
            assert_eq!(*digest_scope, ImageDigestScope::LocalImage);
            (
                component.id.as_str(),
                embedded_artifacts
                    .iter()
                    .map(|artifact| (artifact.location.as_str(), artifact))
                    .collect::<BTreeMap<_, _>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let connector = artifacts
        .get("iceberg-java")
        .context("profile omits Iceberg connector artifacts")?;
    let runner = artifacts
        .get("catalog-bench-engine")
        .context("profile omits engine-runner artifacts")?;
    let spark = artifacts
        .get("spark-4.1")
        .context("profile omits Spark runtime artifacts")?;
    for (connector_location, spark_location) in [
        (
            "image:/opt/iceberg/iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
            "image:/opt/spark/jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
        ),
        (
            "image:/opt/iceberg/iceberg-aws-bundle-1.11.0.jar",
            "image:/opt/spark/jars/iceberg-aws-bundle-1.11.0.jar",
        ),
    ] {
        let connector_artifact = required_artifact(connector, connector_location)?;
        let spark_artifact = required_artifact(spark, spark_location)?;
        assert_eq!(connector_artifact.digest, spark_artifact.digest);
        assert_eq!(connector_artifact.bytes, spark_artifact.bytes);
    }
    let runner_artifact = required_artifact(runner, "image:/usr/local/bin/catalog-bench-engine")?;
    let spark_runner = required_artifact(spark, "image:/usr/local/bin/catalog-bench-engine")?;
    assert_eq!(runner_artifact.digest, spark_runner.digest);
    assert_eq!(runner_artifact.bytes, spark_runner.bytes);
    Ok(())
}

#[test]
fn base_identity_or_required_jar_drift_fails_closed() -> Result<()> {
    let mut wrong_base: Value = serde_json::from_slice(MATERIALIZATION)?;
    image_mut(&mut wrong_base, "spark-4.1")?["labels"]["org.opencontainers.image.base.digest"] =
        json!(format!("sha256:{}", "0".repeat(64)));
    let error = render_spark_profile(SOURCE_PROFILE, &serde_json::to_vec(&wrong_base)?)
        .expect_err("wrong Spark base digest must fail closed");
    assert!(format!("{error:#}").contains("org.opencontainers.image.base.digest"));

    let mut missing_aws: Value = serde_json::from_slice(MATERIALIZATION)?;
    let artifacts = image_mut(&mut missing_aws, "iceberg-java")?["embedded_artifacts"]
        .as_array_mut()
        .context("Iceberg artifacts must be an array")?;
    artifacts.retain(|artifact| {
        artifact["location"] != "image:/opt/iceberg/iceberg-aws-bundle-1.11.0.jar"
    });
    let error = render_spark_profile(SOURCE_PROFILE, &serde_json::to_vec(&missing_aws)?)
        .expect_err("missing object-store bundle must fail closed");
    assert!(format!("{error:#}").contains("iceberg-aws-bundle-1.11.0.jar"));

    let mut drifted_runner: Value = serde_json::from_slice(MATERIALIZATION)?;
    artifact_mut(
        image_mut(&mut drifted_runner, "catalog-bench-engine")?,
        "image:/usr/local/bin/catalog-bench-engine",
    )?["digest"]["value"] = json!("3".repeat(64));
    let error = render_spark_profile(SOURCE_PROFILE, &serde_json::to_vec(&drifted_runner)?)
        .expect_err("runner copy drift must fail closed");
    assert!(format!("{error:#}").contains("must be byte-identical"));

    let mut wrong_runner_label: Value = serde_json::from_slice(MATERIALIZATION)?;
    image_mut(&mut wrong_runner_label, "spark-4.1")?["labels"]
        ["io.querygraph.catalog-bench.runner-source-revision"] = json!("4".repeat(40));
    let error = render_spark_profile(SOURCE_PROFILE, &serde_json::to_vec(&wrong_runner_label)?)
        .expect_err("wrong embedded runner source label must fail closed");
    assert!(format!("{error:#}").contains("io.querygraph.catalog-bench.runner-source-revision"));
    Ok(())
}

fn required_artifact<'a>(
    artifacts: &'a BTreeMap<&str, &ArtifactReference>,
    location: &str,
) -> Result<&'a ArtifactReference> {
    artifacts
        .get(location)
        .copied()
        .with_context(|| format!("profile omits required artifact `{location}`"))
}

fn image_mut<'a>(materialization: &'a mut Value, component: &str) -> Result<&'a mut Value> {
    materialization["images"]
        .as_array_mut()
        .context("materialization images must be an array")?
        .iter_mut()
        .find(|image| image["component"] == component)
        .with_context(|| format!("materialization omits {component}"))
}

fn artifact_mut<'a>(image: &'a mut Value, location: &str) -> Result<&'a mut Value> {
    image["embedded_artifacts"]
        .as_array_mut()
        .context("embedded artifacts must be an array")?
        .iter_mut()
        .find(|artifact| artifact["location"] == location)
        .with_context(|| format!("image omits artifact {location}"))
}

fn repository_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("contract-tool crate is nested two levels below the repository root")
}
