use std::collections::BTreeMap;

use catalog_bench_common::contract::{
    parse_contract, AdapterRequestHandling, ArtifactReference, ComponentId, ContractDocument,
    Profile, RuntimeArtifact, Scenario,
};
use catalog_bench_engine::{
    CatalogCredentialSource, EngineExecutionPlan, EngineRuntimeObservation, InteroperabilityPlan,
    SparkAuthentication, ENGINE_RUNNER_COMPONENT_ID, ENGINE_RUNNER_LOCATION, ENGINE_RUNNER_ROLE,
    ENGINE_TRANSCRIPT_FORMAT, ICEBERG_CONNECTOR_VERSION, SPARK_COMPONENT_VERSION,
    SPARK_PLAN_FORMAT,
};
use serde_json::json;

mod support;

use support::{remove_engine_runner, RUNNER_REVISION};

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.v2.json");
const CANDIDATE_PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-27.json");
const RUNNER_DIGEST: &str = "44e0aad6f2519678d335d6a437073da9674bb5a378df4b6d92fe88dfae038f5b";

#[test]
fn derives_one_secret_free_stock_spark_plan_for_each_selected_catalog() {
    let (profile, scenario) = contracts();

    for catalog in ["lakecat", "polaris", "gravitino", "lakekeeper"] {
        let plan = InteroperabilityPlan::from_contracts(
            &profile,
            &scenario,
            &ComponentId::from(catalog),
            "policy01",
        )
        .unwrap();
        let spark = plan.spark().unwrap();
        assert_eq!(plan.engine().version, SPARK_COMPONENT_VERSION);
        assert_eq!(plan.connector().version, ICEBERG_CONNECTOR_VERSION);
        assert_eq!(spark.format, SPARK_PLAN_FORMAT);
        assert_eq!(spark.scenario.transcript_format, ENGINE_TRANSCRIPT_FORMAT);
        assert_eq!(spark.catalog.name, "bench");
        assert_eq!(spark.file_io.endpoint, "http://minio:9000");
        assert_eq!(spark.file_io.bucket, "warehouse");
        assert_eq!(plan.object_store().bucket, "warehouse");
        assert!(plan.object_store().path_style_access);
        assert_eq!(
            spark.fixture.namespace,
            format!("cb_c201_{}_policy01", catalog.replace('-', "_"))
        );
        assert_eq!(spark.fixture.table, "events");

        let encoded = serde_json::to_string(spark).unwrap();
        for forbidden in [
            "CATALOG_BENCH_POLARIS_CLIENT_ID",
            "CATALOG_BENCH_POLARIS_CLIENT_SECRET",
            "CATALOG_BENCH_S3_ACCESS_KEY_ID",
            "CATALOG_BENCH_S3_SECRET_ACCESS_KEY",
        ] {
            assert!(!encoded.contains(forbidden));
        }
    }
}

#[test]
fn execution_plan_exposes_engine_neutral_scenario_and_fixture_views() {
    let (profile, scenario) = contracts();
    let plan = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "neutral01",
    )
    .unwrap();
    let EngineExecutionPlan::Spark(spark) = plan.execution();

    assert_eq!(plan.fixture(), &spark.fixture);
    assert_eq!(plan.scenario(), &spark.scenario);
    assert_eq!(plan.spark(), Some(spark));
}

#[test]
fn execution_plan_matches_only_its_exact_runtime_identity() {
    let (profile, scenario) = contracts();
    let plan = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "identity01",
    )
    .unwrap();
    let runtime = EngineRuntimeObservation {
        engine_version: "4.1.3".to_owned(),
        dependencies: BTreeMap::from([
            ("java".to_owned(), "21.0.11".to_owned()),
            ("scala".to_owned(), "2.13.17".to_owned()),
        ]),
        operating_system: "Linux".to_owned(),
        architecture: "aarch64".to_owned(),
    };

    assert!(plan.execution().runtime_identity_matches(&runtime));
    for drifted in [
        EngineRuntimeObservation {
            engine_version: "4.1.4".to_owned(),
            ..runtime.clone()
        },
        EngineRuntimeObservation {
            dependencies: BTreeMap::from([("java".to_owned(), "21.0.11".to_owned())]),
            ..runtime.clone()
        },
        EngineRuntimeObservation {
            dependencies: runtime
                .dependencies
                .iter()
                .map(|(name, version)| (name.clone(), version.clone()))
                .chain([("python".to_owned(), "3.13".to_owned())])
                .collect(),
            ..runtime.clone()
        },
    ] {
        assert!(!plan.execution().runtime_identity_matches(&drifted));
    }
}

#[test]
fn explicit_engine_selection_is_role_bound_and_unambiguous() {
    let (mut profile, scenario) = contracts();
    let ContractDocument::Profile(candidate) = parse_contract(CANDIDATE_PROFILE).unwrap() else {
        panic!("candidate fixture must be a profile");
    };
    profile.components.push(
        candidate
            .components
            .iter()
            .find(|component| component.id.as_str() == "flink")
            .unwrap()
            .clone(),
    );
    profile.services.push(
        candidate
            .services
            .iter()
            .find(|service| service.component.as_str() == "flink")
            .unwrap()
            .clone(),
    );

    let singular = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "select01",
    )
    .unwrap_err();
    assert!(singular
        .to_string()
        .contains("exactly one `stock-engine` service"));

    let selected = InteroperabilityPlan::from_contracts_for_engine(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        &ComponentId::from("spark-4.1"),
        "select01",
    )
    .unwrap();
    assert_eq!(selected.engine().id.as_str(), "spark-4.1");

    let unsupported = InteroperabilityPlan::from_contracts_for_engine(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        &ComponentId::from("flink"),
        "select01",
    )
    .unwrap_err();
    assert!(unsupported
        .to_string()
        .contains("Spark renderer supports Apache Spark 4.1.3"));

    let not_selected = InteroperabilityPlan::from_contracts_for_engine(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        &ComponentId::from("lakecat"),
        "select01",
    )
    .unwrap_err();
    assert!(not_selected
        .to_string()
        .contains("through exactly one `stock-engine` service"));
}

#[test]
fn derives_only_profile_routing_and_standard_authentication_data() {
    let (profile, scenario) = contracts();
    let lakecat = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "routes01",
    )
    .unwrap();
    let lakecat_spark = lakecat.spark().unwrap();
    assert_eq!(lakecat_spark.catalog.warehouse, None);
    assert_eq!(lakecat_spark.catalog.prefix, None);
    assert_eq!(
        lakecat_spark.fixture.requested_location.as_deref(),
        Some("s3://warehouse/lakecat/cb_c201_lakecat_routes01/events")
    );
    assert!(matches!(
        lakecat_spark.catalog.authentication,
        SparkAuthentication::Anonymous
    ));
    assert!(matches!(
        lakecat.credential_source(),
        CatalogCredentialSource::Anonymous
    ));

    let polaris = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("polaris"),
        "routes01",
    )
    .unwrap();
    let polaris_spark = polaris.spark().unwrap();
    assert_eq!(polaris_spark.catalog.warehouse.as_deref(), Some("bench"));
    assert_eq!(polaris_spark.catalog.prefix.as_deref(), Some("bench"));
    assert_eq!(polaris_spark.fixture.requested_location, None);
    assert!(matches!(
        &polaris_spark.catalog.authentication,
        SparkAuthentication::OAuth2ClientCredentials {
            oauth2_server_uri,
            scope
        } if oauth2_server_uri == "http://polaris:8181/api/catalog/v1/oauth/tokens"
            && scope == "PRINCIPAL_ROLE:ALL"
    ));
    assert!(matches!(
        polaris.credential_source(),
        CatalogCredentialSource::OAuth2ClientCredentials { .. }
    ));
    assert_eq!(
        serde_json::to_value(&polaris_spark.catalog.authentication).unwrap(),
        json!({
            "kind": "oauth2-client-credentials",
            "oauth2_server_uri": "http://polaris:8181/api/catalog/v1/oauth/tokens",
            "scope": "PRINCIPAL_ROLE:ALL"
        })
    );

    let lakekeeper = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakekeeper"),
        "routes01",
    )
    .unwrap();
    let lakekeeper_spark = lakekeeper.spark().unwrap();
    assert_eq!(lakekeeper_spark.catalog.warehouse.as_deref(), Some("bench"));
    assert_eq!(lakekeeper_spark.catalog.prefix, None);
}

#[test]
fn runtime_policy_requires_connector_bytes_inside_the_executed_engine() {
    let (profile, scenario) = contracts();
    let plan = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "runtime01",
    )
    .unwrap();
    assert_eq!(plan.runtime_artifacts().len(), 5);
    let shared = plan
        .runtime_artifacts()
        .iter()
        .filter(|artifact| artifact.location.starts_with("/opt/spark/jars/iceberg-"))
        .collect::<Vec<_>>();
    assert_eq!(shared.len(), 2);
    assert!(shared.iter().all(|artifact| {
        artifact.location.starts_with("/opt/spark/jars/iceberg-")
            && artifact
                .components
                .iter()
                .any(|id| id.as_str() == "spark-4.1")
            && artifact
                .components
                .iter()
                .any(|id| id.as_str() == "iceberg-java")
    }));

    let mut drifted = profile.clone();
    let connector = drifted
        .components
        .iter_mut()
        .find(|component| component.id.as_str() == "iceberg-java")
        .unwrap();
    let catalog_bench_common::contract::RuntimeArtifact::ContainerImage {
        embedded_artifacts, ..
    } = &mut connector.artifact
    else {
        panic!("connector fixture must be an image");
    };
    embedded_artifacts[0].digest.value = "0".repeat(64);
    let error = InteroperabilityPlan::from_contracts(
        &drifted,
        &scenario,
        &ComponentId::from("lakecat"),
        "runtime01",
    )
    .unwrap_err();
    assert!(error.to_string().contains("byte-identical copy"));

    let mut duplicated = profile.clone();
    let connector = duplicated
        .components
        .iter_mut()
        .find(|component| component.id.as_str() == "iceberg-java")
        .unwrap();
    let catalog_bench_common::contract::RuntimeArtifact::ContainerImage {
        embedded_artifacts, ..
    } = &mut connector.artifact
    else {
        panic!("connector fixture must be an image");
    };
    embedded_artifacts[1] = embedded_artifacts[0].clone();
    let error = InteroperabilityPlan::from_contracts(
        &duplicated,
        &scenario,
        &ComponentId::from("lakecat"),
        "runtime01",
    )
    .unwrap_err();
    assert!(error.to_string().contains("duplicates"));

    let mut traversing = profile.clone();
    let engine = traversing
        .components
        .iter_mut()
        .find(|component| component.id.as_str() == "spark-4.1")
        .unwrap();
    let catalog_bench_common::contract::RuntimeArtifact::ContainerImage {
        embedded_artifacts, ..
    } = &mut engine.artifact
    else {
        panic!("engine fixture must be an image");
    };
    embedded_artifacts[0].location = "image:/opt/spark/../private".to_owned();
    let error = InteroperabilityPlan::from_contracts(
        &traversing,
        &scenario,
        &ComponentId::from("lakecat"),
        "runtime01",
    )
    .unwrap_err();
    assert!(error.to_string().contains("traversal-free"));

    let mut moved_submit = profile.clone();
    let engine = moved_submit
        .components
        .iter_mut()
        .find(|component| component.id.as_str() == "spark-4.1")
        .unwrap();
    let catalog_bench_common::contract::RuntimeArtifact::ContainerImage {
        embedded_artifacts, ..
    } = &mut engine.artifact
    else {
        panic!("engine fixture must be an image");
    };
    embedded_artifacts[0].location = "image:/opt/alternate/spark-submit".to_owned();
    let error = InteroperabilityPlan::from_contracts(
        &moved_submit,
        &scenario,
        &ComponentId::from("lakecat"),
        "runtime01",
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("exactly one `/opt/spark/bin/spark-submit`"));
}

#[test]
fn runner_role_correlates_one_source_bound_elf_inside_spark() {
    let (profile, scenario) = contracts();

    let plan = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "runner01",
    )
    .unwrap();
    let runner = plan.runner().unwrap();
    assert_eq!(runner.id.as_str(), ENGINE_RUNNER_COMPONENT_ID);
    assert_eq!(runner.source_revision.as_deref(), Some(RUNNER_REVISION));
    let artifact = plan
        .runtime_artifacts()
        .iter()
        .find(|artifact| artifact.location == ENGINE_RUNNER_LOCATION)
        .unwrap();
    assert_eq!(artifact.media_type, "application/vnd.elf");
    assert_eq!(artifact.sha256, RUNNER_DIGEST);
    assert_eq!(artifact.bytes, 4_986_064);
    assert_eq!(
        artifact
            .components
            .iter()
            .map(ComponentId::as_str)
            .collect::<Vec<_>>(),
        [ENGINE_RUNNER_COMPONENT_ID, "spark-4.1"]
    );

    let mut drifted = profile.clone();
    runner_artifact_mut(&mut drifted).digest.value = "3".repeat(64);
    assert!(plan_error(&drifted, &scenario).contains("byte-identical copy"));

    let mut moved = profile.clone();
    runner_artifact_mut(&mut moved).location = "image:/opt/alternate/runner".to_owned();
    engine_runner_copy_mut(&mut moved).location = "image:/opt/alternate/runner".to_owned();
    assert!(plan_error(&moved, &scenario).contains(ENGINE_RUNNER_LOCATION));

    let mut duplicate_role = profile.clone();
    let service = duplicate_role
        .services
        .iter()
        .find(|service| service.role == ENGINE_RUNNER_ROLE)
        .unwrap()
        .clone();
    duplicate_role.services.push(service);
    assert!(plan_error(&duplicate_role, &scenario).contains("at most one `engine-runner`"));

    let mut wrong_source = profile;
    let runner = wrong_source
        .components
        .iter_mut()
        .find(|component| component.id.as_str() == ENGINE_RUNNER_COMPONENT_ID)
        .unwrap();
    runner.version = "not-a-revision".to_owned();
    assert!(plan_error(&wrong_source, &scenario).contains("40-character Git revision"));
}

#[test]
fn legacy_profile_without_runner_role_retains_toolchain_fallback() {
    let (mut profile, scenario) = contracts();
    remove_engine_runner(&mut profile);

    let plan = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "legacy_runner01",
    )
    .unwrap();
    assert!(plan.runner().is_none());
    assert_eq!(plan.runtime_artifacts().len(), 4);
}

#[test]
fn rejects_contract_drift_shims_unsafe_fixtures_and_ambiguous_roles() {
    let (profile, scenario) = contracts();

    let mut drifted_scenario = scenario.clone();
    drifted_scenario
        .parameters
        .insert("transcript_format".to_owned(), json!("drifted"));
    assert!(InteroperabilityPlan::from_contracts(
        &profile,
        &drifted_scenario,
        &ComponentId::from("lakecat"),
        "policy02",
    )
    .unwrap_err()
    .to_string()
    .contains("canonical"));

    let mut shimmed = profile.clone();
    let adapter = shimmed
        .catalog_adapters
        .iter_mut()
        .find(|adapter| adapter.catalog.as_str() == "lakecat")
        .unwrap();
    adapter.request_handling = AdapterRequestHandling::BehaviorChangingShim {
        component: ComponentId::from("rust-runner"),
        description: "test shim".to_owned(),
    };
    assert!(InteroperabilityPlan::from_contracts(
        &shimmed,
        &scenario,
        &ComponentId::from("lakecat"),
        "policy02",
    )
    .unwrap_err()
    .to_string()
    .contains("behavior-changing shim"));

    assert!(InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "unsafe-value",
    )
    .unwrap_err()
    .to_string()
    .contains("fixture ID"));

    let mut ambiguous = profile.clone();
    let engine_service = ambiguous
        .services
        .iter()
        .find(|service| service.role == "stock-engine")
        .unwrap()
        .clone();
    ambiguous.services.push(engine_service);
    assert!(InteroperabilityPlan::from_contracts(
        &ambiguous,
        &scenario,
        &ComponentId::from("lakecat"),
        "policy02",
    )
    .unwrap_err()
    .to_string()
    .contains("exactly one `stock-engine`"));
}

#[test]
fn rejects_shared_store_and_supported_runtime_drift() {
    let (profile, scenario) = contracts();

    let mut bad_store = profile.clone();
    bad_store
        .services
        .iter_mut()
        .find(|service| service.role == "shared-object-store")
        .unwrap()
        .settings
        .insert("path_style_access".to_owned(), json!("true"));
    assert!(InteroperabilityPlan::from_contracts(
        &bad_store,
        &scenario,
        &ComponentId::from("lakecat"),
        "policy03",
    )
    .unwrap_err()
    .to_string()
    .contains("must be a boolean"));

    let mut newer_spark = profile.clone();
    newer_spark
        .components
        .iter_mut()
        .find(|component| component.id.as_str() == "spark-4.1")
        .unwrap()
        .version = "4.2.0".to_owned();
    assert!(InteroperabilityPlan::from_contracts(
        &newer_spark,
        &scenario,
        &ComponentId::from("lakecat"),
        "policy03",
    )
    .unwrap_err()
    .to_string()
    .contains("supports Apache Spark 4.1.3"));
}

fn contracts() -> (Profile, Scenario) {
    let ContractDocument::Profile(profile) = parse_contract(PROFILE).unwrap() else {
        panic!("profile fixture must be a profile");
    };
    let ContractDocument::Scenario(scenario) = parse_contract(SCENARIO).unwrap() else {
        panic!("scenario fixture must be a scenario");
    };
    (profile, scenario)
}

fn runner_artifact_mut(profile: &mut Profile) -> &mut ArtifactReference {
    let runner = profile
        .components
        .iter_mut()
        .find(|component| component.id.as_str() == ENGINE_RUNNER_COMPONENT_ID)
        .unwrap();
    let RuntimeArtifact::ContainerImage {
        embedded_artifacts, ..
    } = &mut runner.artifact
    else {
        panic!("runner fixture must be an image");
    };
    &mut embedded_artifacts[0]
}

fn engine_runner_copy_mut(profile: &mut Profile) -> &mut ArtifactReference {
    let engine = profile
        .components
        .iter_mut()
        .find(|component| component.id.as_str() == "spark-4.1")
        .unwrap();
    let RuntimeArtifact::ContainerImage {
        embedded_artifacts, ..
    } = &mut engine.artifact
    else {
        panic!("engine fixture must be an image");
    };
    embedded_artifacts
        .iter_mut()
        .find(|artifact| artifact.location.strip_prefix("image:") == Some(ENGINE_RUNNER_LOCATION))
        .unwrap()
}

fn plan_error(profile: &Profile, scenario: &Scenario) -> String {
    InteroperabilityPlan::from_contracts(
        profile,
        scenario,
        &ComponentId::from("lakecat"),
        "runner01",
    )
    .unwrap_err()
    .to_string()
}
