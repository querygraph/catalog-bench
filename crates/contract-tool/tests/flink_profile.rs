use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use catalog_bench_common::contract::{
    parse_contract, ArtifactReference, ContractDocument, ProfilePurpose, ProfileReadiness,
    RuntimeArtifact,
};
use catalog_bench_contract::render_flink_profile;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};

const BROAD_SOURCE_PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-27.json");
const FLINK_SOURCE_PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/flink-candidate-2.1.3-2026-08-27.json");
const SPARK_MATERIALIZATION: &[u8] =
    include_bytes!("../../../materializations/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const RUNNER_REVISION: &str = "df3a68da787de82ae83d1a5034228b731f3bc588";

#[test]
fn checked_in_candidate_advances_only_identity_and_runner_source() -> Result<()> {
    assert_eq!(
        sha256(FLINK_SOURCE_PROFILE),
        "d27bc0c59f65aa1dc57b70a80ea6a5d3de86a8984a8b95a5992ac1e2b6e5603a"
    );
    let mut expected: Value = serde_json::from_slice(BROAD_SOURCE_PROFILE)?;
    expected["id"] = json!("catalog-community-flink-candidate-2.1.3-2026-08-27-linux-arm64");
    expected["title"] = json!("Catalog community Flink 2.1.3 source-bound candidate pinset");
    expected["description"] = json!(
        "Immutable versions and production build recipes selected for the Linux ARM64 same-Docker Flink 2.1.3 interoperability run. This draft preserves the broad stock-engine candidate while advancing only the catalog-bench engine runner to its source-bound Flink image revision; it is an input contract, not benchmark evidence."
    );
    component_mut(&mut expected, "catalog-bench-engine")?["version"] = json!(RUNNER_REVISION);
    component_mut(&mut expected, "catalog-bench-engine")?["source"]["revision"] =
        json!(RUNNER_REVISION);

    let actual: Value = serde_json::from_slice(FLINK_SOURCE_PROFILE)?;
    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn flink_profile_is_runnable_common_and_source_bound() -> Result<()> {
    let (source, materialization) = fixtures()?;
    let rendered = render_flink_profile(&source, &materialization)?;
    let ContractDocument::Profile(profile) = parse_contract(&rendered)? else {
        panic!("Flink materializer must produce a profile");
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
            "catalog-bench-engine",
            "flink",
            "gravitino",
            "iceberg-java",
            "lakecat",
            "lakecat-turso",
            "lakekeeper",
            "minio",
            "polaris",
            "postgresql",
            "rust-runner",
        ])
    );
    assert_eq!(
        profile.extensions["querygraph/materialization"]["scope"],
        "engine.iceberg.write-read-evolution/v2"
    );
    assert!(profile.services.iter().any(|service| {
        service.role == "stock-engine" && service.component.as_str() == "flink"
    }));

    let artifacts = profile
        .components
        .iter()
        .filter(|component| {
            matches!(
                component.id.as_str(),
                "catalog-bench-engine" | "iceberg-java" | "flink"
            )
        })
        .map(|component| {
            let RuntimeArtifact::ContainerImage {
                embedded_artifacts, ..
            } = &component.artifact
            else {
                panic!("{} must be a materialized image", component.id);
            };
            (
                component.id.as_str(),
                embedded_artifacts
                    .iter()
                    .map(|artifact| (artifact.location.as_str(), artifact))
                    .collect::<BTreeMap<_, _>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for (source_component, source_location, destination_location) in [
        (
            "iceberg-java",
            "image:/opt/iceberg/iceberg-flink-runtime-2.1-1.11.0.jar",
            "image:/opt/flink/lib/iceberg-flink-runtime-2.1-1.11.0.jar",
        ),
        (
            "iceberg-java",
            "image:/opt/iceberg/iceberg-aws-bundle-1.11.0.jar",
            "image:/opt/flink/lib/iceberg-aws-bundle-1.11.0.jar",
        ),
        (
            "catalog-bench-engine",
            "image:/usr/local/bin/catalog-bench-engine",
            "image:/usr/local/bin/catalog-bench-engine",
        ),
        (
            "catalog-bench-engine",
            "image:/opt/catalog-bench/catalog-bench-flink-runner.jar",
            "image:/opt/catalog-bench/catalog-bench-flink-runner.jar",
        ),
    ] {
        let source = required_artifact(&artifacts[source_component], source_location)?;
        let destination = required_artifact(&artifacts["flink"], destination_location)?;
        assert_eq!(source.digest, destination.digest);
        assert_eq!(source.bytes, destination.bytes);
    }
    Ok(())
}

#[test]
fn base_runner_and_connector_drift_fail_closed() -> Result<()> {
    let (source, materialization) = fixtures()?;
    let mut wrong_base: Value = serde_json::from_slice(&materialization)?;
    image_mut(&mut wrong_base, "flink")?["labels"]["org.opencontainers.image.base.digest"] =
        json!(format!("sha256:{}", "0".repeat(64)));
    let error = render_flink_profile(&source, &serde_json::to_vec(&wrong_base)?)
        .expect_err("wrong Flink child digest must fail closed");
    assert!(format!("{error:#}").contains("org.opencontainers.image.base.digest"));

    let mut drifted_runner: Value = serde_json::from_slice(&materialization)?;
    artifact_mut(
        image_mut(&mut drifted_runner, "flink")?,
        "image:/opt/catalog-bench/catalog-bench-flink-runner.jar",
    )?["digest"]["value"] = json!("7".repeat(64));
    let error = render_flink_profile(&source, &serde_json::to_vec(&drifted_runner)?)
        .expect_err("drifted Java child copy must fail closed");
    assert!(format!("{error:#}").contains("must be byte-identical"));

    let mut missing_connector: Value = serde_json::from_slice(&materialization)?;
    image_mut(&mut missing_connector, "iceberg-java")?["embedded_artifacts"]
        .as_array_mut()
        .context("connector artifacts must be an array")?
        .retain(|artifact| {
            artifact["location"] != "image:/opt/iceberg/iceberg-flink-runtime-2.1-1.11.0.jar"
        });
    let error = render_flink_profile(&source, &serde_json::to_vec(&missing_connector)?)
        .expect_err("missing Flink connector must fail closed");
    assert!(format!("{error:#}").contains("iceberg-flink-runtime-2.1-1.11.0.jar"));
    Ok(())
}

fn fixtures() -> Result<(Vec<u8>, Vec<u8>)> {
    let source = FLINK_SOURCE_PROFILE.to_vec();

    let spark: Value = serde_json::from_slice(SPARK_MATERIALIZATION)?;
    let mut images = Vec::new();
    for component in ["minio", "lakecat"] {
        images.push(image(&spark, component)?.clone());
    }
    images.push(runner_image());
    images.push(connector_image());
    images.push(flink_image());
    let materialization = json!({
        "format": "catalog-bench/flink-profile-materialization/v1",
        "source_profile": {
            "id": "catalog-community-flink-candidate-2.1.3-2026-08-27-linux-arm64",
            "digest": {"algorithm": "sha256", "value": sha256(&source)},
        },
        "output_profile": {
            "id": "catalog-community-flink-2.1.3-iceberg-1.11.0-test-linux-arm64",
            "title": "Synthetic Flink materialization test",
            "description": "Synthetic observation for pure profile-policy tests.",
            "resolved_at": "2026-08-27T00:00:00Z",
        },
        "images": images,
    });
    Ok((source, serde_json::to_vec(&materialization)?))
}

fn runner_image() -> Value {
    image_observation(
        "catalog-bench-engine",
        "catalog-bench/flink-runner:synthetic",
        "flink-runner-image",
        RUNNER_REVISION,
        json!({"io.querygraph.catalog-bench.runner-source-revision": RUNNER_REVISION}),
        vec![
            artifact(
                "image:/usr/local/bin/catalog-bench-engine",
                "application/vnd.elf",
                '1',
            ),
            artifact(
                "image:/opt/catalog-bench/catalog-bench-flink-runner.jar",
                "application/java-archive",
                '2',
            ),
        ],
    )
}

fn connector_image() -> Value {
    image_observation(
        "iceberg-java",
        "catalog-bench/iceberg-flink-runtime:synthetic",
        "iceberg-flink-runtime",
        "6976e020b894f6a6777704df2b8c4458cb291ae9",
        json!({
            "org.opencontainers.image.version": "1.11.0",
            "io.querygraph.catalog-bench.iceberg-flink-runtime-coordinate":
                "org.apache.iceberg:iceberg-flink-runtime-2.1:1.11.0",
            "io.querygraph.catalog-bench.iceberg-aws-bundle-coordinate":
                "org.apache.iceberg:iceberg-aws-bundle:1.11.0",
        }),
        vec![
            artifact(
                "image:/opt/iceberg/iceberg-flink-runtime-2.1-1.11.0.jar",
                "application/java-archive",
                '3',
            ),
            artifact(
                "image:/opt/iceberg/iceberg-aws-bundle-1.11.0.jar",
                "application/java-archive",
                '4',
            ),
        ],
    )
}

fn flink_image() -> Value {
    image_observation(
        "flink",
        "catalog-bench/flink:synthetic",
        "flink",
        "6cda56b084d5c337b36d2f8ed464bc92093b0a34",
        json!({
            "org.opencontainers.image.version": "2.1.3",
            "org.opencontainers.image.base.digest":
                "sha256:99a499ed147b28d358486066ab8308e351b232b2ac81aff69157fdb349c84e18",
            "io.querygraph.catalog-bench.iceberg-source-revision":
                "6976e020b894f6a6777704df2b8c4458cb291ae9",
            "io.querygraph.catalog-bench.runner-source-revision": RUNNER_REVISION,
        }),
        vec![
            artifact(
                "image:/opt/flink/bin/flink",
                "application/x-shellscript",
                '5',
            ),
            artifact(
                "image:/opt/flink/lib/flink-dist-2.1.3.jar",
                "application/java-archive",
                '6',
            ),
            artifact(
                "image:/opt/flink/lib/iceberg-flink-runtime-2.1-1.11.0.jar",
                "application/java-archive",
                '3',
            ),
            artifact(
                "image:/opt/flink/lib/iceberg-aws-bundle-1.11.0.jar",
                "application/java-archive",
                '4',
            ),
            artifact(
                "image:/usr/local/bin/catalog-bench-engine",
                "application/vnd.elf",
                '1',
            ),
            artifact(
                "image:/opt/catalog-bench/catalog-bench-flink-runner.jar",
                "application/java-archive",
                '2',
            ),
        ],
    )
}

fn image_observation(
    component: &str,
    reference: &str,
    compose_service: &str,
    revision: &str,
    extra_labels: Value,
    artifacts: Vec<Value>,
) -> Value {
    let mut labels = serde_json::Map::from_iter([
        (
            "com.docker.compose.project".to_owned(),
            json!("catalog-bench"),
        ),
        (
            "com.docker.compose.service".to_owned(),
            json!(compose_service),
        ),
        ("com.docker.compose.version".to_owned(), json!("5.1.3")),
        (
            "org.opencontainers.image.revision".to_owned(),
            json!(revision),
        ),
    ]);
    labels.extend(extra_labels.as_object().cloned().unwrap_or_default());
    json!({
        "component": component,
        "reference": reference,
        "image_id": {"algorithm": "sha256", "value": "a".repeat(64)},
        "operating_system": "linux",
        "architecture": "arm64",
        "labels": labels,
        "embedded_artifacts": artifacts,
    })
}

fn artifact(location: &str, media_type: &str, digest: char) -> Value {
    json!({
        "location": location,
        "media_type": media_type,
        "digest": {"algorithm": "sha256", "value": digest.to_string().repeat(64)},
        "bytes": 12345,
        "description": "Synthetic profile-policy artifact.",
    })
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

fn image<'a>(materialization: &'a Value, component: &str) -> Result<&'a Value> {
    materialization["images"]
        .as_array()
        .context("materialization images must be an array")?
        .iter()
        .find(|image| image["component"] == component)
        .with_context(|| format!("materialization omits {component}"))
}

fn image_mut<'a>(materialization: &'a mut Value, component: &str) -> Result<&'a mut Value> {
    materialization["images"]
        .as_array_mut()
        .context("materialization images must be an array")?
        .iter_mut()
        .find(|image| image["component"] == component)
        .with_context(|| format!("materialization omits {component}"))
}

fn component_mut<'a>(profile: &'a mut Value, component: &str) -> Result<&'a mut Value> {
    profile["components"]
        .as_array_mut()
        .context("profile components must be an array")?
        .iter_mut()
        .find(|candidate| candidate["id"] == component)
        .with_context(|| format!("profile omits {component}"))
}

fn artifact_mut<'a>(image: &'a mut Value, location: &str) -> Result<&'a mut Value> {
    image["embedded_artifacts"]
        .as_array_mut()
        .context("embedded artifacts must be an array")?
        .iter_mut()
        .find(|artifact| artifact["location"] == location)
        .with_context(|| format!("image omits artifact {location}"))
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
