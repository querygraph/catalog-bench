use catalog_bench_common::contract::{
    parse_contract, AdapterRequestHandling, ComponentId, ContractDocument, Profile, Scenario,
};
use catalog_bench_engine::{
    CatalogCredentialSource, InteroperabilityPlan, SparkAuthentication, ENGINE_TRANSCRIPT_FORMAT,
    ICEBERG_CONNECTOR_VERSION, SPARK_COMPONENT_VERSION, SPARK_PLAN_FORMAT,
};
use serde_json::json;

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.json");

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
        assert_eq!(plan.engine().version, SPARK_COMPONENT_VERSION);
        assert_eq!(plan.connector().version, ICEBERG_CONNECTOR_VERSION);
        assert_eq!(plan.spark().format, SPARK_PLAN_FORMAT);
        assert_eq!(
            plan.spark().scenario.transcript_format,
            ENGINE_TRANSCRIPT_FORMAT
        );
        assert_eq!(plan.spark().catalog.name, "bench");
        assert_eq!(plan.spark().file_io.endpoint, "http://minio:9000");
        assert_eq!(plan.object_store().bucket, "warehouse");
        assert!(plan.object_store().path_style_access);
        assert_eq!(
            plan.spark().fixture.namespace,
            format!("cb_c201_{}_policy01", catalog.replace('-', "_"))
        );
        assert_eq!(plan.spark().fixture.table, "events");

        let encoded = serde_json::to_string(plan.spark()).unwrap();
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
fn derives_only_profile_routing_and_standard_authentication_data() {
    let (profile, scenario) = contracts();
    let lakecat = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "routes01",
    )
    .unwrap();
    assert_eq!(lakecat.spark().catalog.warehouse, None);
    assert_eq!(lakecat.spark().catalog.prefix, None);
    assert_eq!(
        lakecat.spark().fixture.requested_location.as_deref(),
        Some("s3://warehouse/lakecat/cb_c201_lakecat_routes01/events")
    );
    assert!(matches!(
        lakecat.spark().catalog.authentication,
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
    assert_eq!(polaris.spark().catalog.warehouse.as_deref(), Some("bench"));
    assert_eq!(polaris.spark().catalog.prefix.as_deref(), Some("bench"));
    assert_eq!(polaris.spark().fixture.requested_location, None);
    assert!(matches!(
        &polaris.spark().catalog.authentication,
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
        serde_json::to_value(&polaris.spark().catalog.authentication).unwrap(),
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
    assert_eq!(
        lakekeeper.spark().catalog.warehouse.as_deref(),
        Some("bench")
    );
    assert_eq!(lakekeeper.spark().catalog.prefix, None);
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
    assert_eq!(plan.runtime_artifacts().len(), 4);
    let shared = plan
        .runtime_artifacts()
        .iter()
        .filter(|artifact| artifact.components.len() == 2)
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
