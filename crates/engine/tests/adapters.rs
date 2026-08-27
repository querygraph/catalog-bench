use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use catalog_bench_commit::store::ObjectStoreFailureKind;
use catalog_bench_common::contract::{
    parse_contract, ComponentId, ContractDocument, Profile, Scenario,
};
use catalog_bench_engine::{
    run_engine_workflow, EngineCatalogConnection, EngineCatalogConnectionFailureKind,
    EngineCatalogConnector, EngineObjectStoreConnector, InteroperabilityPlan,
    RestEngineCatalogConnector, RuntimeVerifier, SecretRead, SecretSource,
    SharedObjectStoreConnector, SparkProcessExecutor, SparkProcessOutcome, StockSparkRunner,
};
use serde_json::json;
use tempfile::tempdir;

#[allow(dead_code)]
#[path = "../../conformance/tests/support/mod.rs"]
mod support;

use support::{MockResponse, MockServer};

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.json");

#[tokio::test]
async fn runtime_rejection_keeps_every_production_connector_closed() {
    let (profile, scenario) = contracts();
    let profile = Arc::new(profile);
    let plan = interoperability_plan(&profile, &scenario, "polaris", "adapters01");
    let secrets = Arc::new(RecordingSecrets::with_values([(
        "CATALOG_BENCH_POLARIS_CLIENT_ID",
        "adapter-oauth-client-id",
    )]));
    let runtime_root = tempdir().unwrap();
    let runner = StockSparkRunner::from_parts(
        SparkProcessExecutor::default(),
        RuntimeVerifier::for_observation(runtime_root.path(), "linux", "arm64"),
        Arc::clone(&secrets),
    );

    let execution = run_engine_workflow(
        &plan,
        runner,
        RestEngineCatalogConnector::new(Arc::clone(&profile), Arc::clone(&secrets)),
        SharedObjectStoreConnector::new(Arc::clone(&secrets)),
    )
    .await;

    assert!(matches!(
        execution.process.outcome,
        SparkProcessOutcome::RuntimeRejected
    ));
    assert!(secrets.reads().is_empty());
    assert!(!execution.passed());
}

#[tokio::test]
async fn stock_rest_connector_negotiates_the_selected_profile_adapter() {
    let private_key = "dynamic-private-key-sentinel";
    let private_value = "dynamic-private-value-sentinel";
    let server = MockServer::start(vec![MockResponse::json(json!({
        "defaults": {(private_key): private_value},
        "overrides": {}
    }))]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());
    let profile = Arc::new(profile);
    let plan = interoperability_plan(&profile, &scenario, "lakecat", "adapters02");
    let secrets = Arc::new(RecordingSecrets::default());
    let connection = RestEngineCatalogConnector::new(profile, Arc::clone(&secrets))
        .connect(&plan)
        .await;

    let EngineCatalogConnection::Ready { negotiation, .. } = connection else {
        panic!("anonymous adapter should negotiate");
    };
    assert_eq!(negotiation.adapter.catalog, ComponentId::from("lakecat"));
    let serialized = serde_json::to_string(&negotiation).unwrap();
    assert!(!serialized.contains(private_key));
    assert!(!serialized.contains(private_value));
    assert!(secrets.reads().is_empty());
    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].target, "/catalog/v1/config");
}

#[tokio::test]
async fn connector_failures_keep_only_closed_evidence_categories() {
    let (profile, scenario) = contracts();
    let plan = interoperability_plan(&profile, &scenario, "polaris", "adapters03");
    let secrets = Arc::new(RecordingSecrets::with_values([(
        "CATALOG_BENCH_POLARIS_CLIENT_ID",
        "adapter-oauth-client-id",
    )]));
    let connection = RestEngineCatalogConnector::new(Arc::new(profile), Arc::clone(&secrets))
        .connect(&plan)
        .await;
    let EngineCatalogConnection::Failed {
        negotiation: Some(negotiation),
        failure,
    } = connection
    else {
        panic!("missing OAuth credentials should fail negotiation");
    };
    assert_eq!(
        failure.kind,
        EngineCatalogConnectionFailureKind::Authentication
    );
    assert_eq!(
        secrets.reads(),
        [
            "CATALOG_BENCH_POLARIS_CLIENT_ID",
            "CATALOG_BENCH_POLARIS_CLIENT_SECRET"
        ]
    );
    assert!(!serde_json::to_string(&negotiation)
        .unwrap()
        .contains("adapter-oauth-client-id"));

    let (profile, scenario) = contracts();
    let plan = interoperability_plan(&profile, &scenario, "lakecat", "adapters04");
    let mut missing_adapter = profile;
    missing_adapter
        .catalog_adapters
        .retain(|adapter| adapter.catalog.as_str() != "lakecat");
    let connection = RestEngineCatalogConnector::new(
        Arc::new(missing_adapter),
        Arc::new(RecordingSecrets::default()),
    )
    .connect(&plan)
    .await;
    let EngineCatalogConnection::Failed {
        negotiation: None,
        failure,
    } = connection
    else {
        panic!("invalid adapter setup should not retain backend detail");
    };
    assert_eq!(failure.kind, EngineCatalogConnectionFailureKind::Setup);
}

#[test]
fn shared_object_store_connector_reads_only_the_profile_credentials() {
    let (profile, scenario) = contracts();
    let plan = interoperability_plan(&profile, &scenario, "lakecat", "adapters05");
    let secrets = Arc::new(RecordingSecrets::with_values([
        ("CATALOG_BENCH_S3_ACCESS_KEY_ID", "adapter-access"),
        ("CATALOG_BENCH_S3_SECRET_ACCESS_KEY", "adapter-secret"),
    ]));
    let connector = SharedObjectStoreConnector::new(Arc::clone(&secrets));

    assert!(connector.connect(&plan).is_ok());
    assert_eq!(
        secrets.reads(),
        [
            "CATALOG_BENCH_S3_ACCESS_KEY_ID",
            "CATALOG_BENCH_S3_SECRET_ACCESS_KEY"
        ]
    );

    let missing = SharedObjectStoreConnector::new(Arc::new(RecordingSecrets::default()))
        .connect(&plan)
        .unwrap_err();
    assert_eq!(missing.kind, ObjectStoreFailureKind::Authentication);
    assert!(!missing.detail.contains("adapter-access"));
    assert!(!missing.detail.contains("adapter-secret"));
}

#[derive(Default)]
struct RecordingSecrets {
    values: BTreeMap<String, String>,
    reads: Mutex<Vec<String>>,
}

impl RecordingSecrets {
    fn with_values<const N: usize>(values: [(&str, &str); N]) -> Self {
        Self {
            values: values
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
            reads: Mutex::default(),
        }
    }

    fn reads(&self) -> Vec<String> {
        self.reads.lock().unwrap().clone()
    }
}

impl SecretSource for RecordingSecrets {
    fn read_secret(&self, name: &str) -> SecretRead {
        self.reads.lock().unwrap().push(name.to_owned());
        self.values
            .get(name)
            .cloned()
            .map(SecretRead::Value)
            .unwrap_or(SecretRead::Missing)
    }
}

fn interoperability_plan(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &str,
    fixture: &str,
) -> InteroperabilityPlan {
    InteroperabilityPlan::from_contracts(profile, scenario, &ComponentId::from(catalog), fixture)
        .unwrap()
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

fn adapter_mut<'a>(
    profile: &'a mut Profile,
    catalog: &str,
) -> &'a mut catalog_bench_common::contract::CatalogAdapter {
    profile
        .catalog_adapters
        .iter_mut()
        .find(|adapter| adapter.catalog.as_str() == catalog)
        .unwrap()
}
