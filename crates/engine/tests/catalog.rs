use catalog_bench_common::contract::{
    parse_contract, ComponentId, ContractDocument, Profile, Scenario,
};
use catalog_bench_conformance::{
    connect_catalog_adapter, CatalogConnectionOutcome, CATALOG_RESPONSE_LIMIT_BYTES,
};
use catalog_bench_engine::{
    EngineCatalog, EngineCatalogFailureKind, EnginePropertyObservation, EngineTableLoad,
    InteroperabilityPlan, RestEngineCatalog,
};
use serde_json::json;

#[path = "../../conformance/tests/support/mod.rs"]
mod support;

use support::{MockResponse, MockServer};

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.v2.json");

#[tokio::test]
async fn stock_rest_load_projects_full_state_and_cleanup_never_purges() {
    let private = "unknown-private-response-value";
    let server = MockServer::start(vec![
        MockResponse::json(json!({"defaults": {}, "overrides": {}})),
        MockResponse::json(table_response(private)),
        MockResponse::empty(204),
        MockResponse::empty(404),
        MockResponse::empty(204),
        MockResponse::empty(404),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());
    let plan = interoperability_plan(&profile, &scenario, "catalog01");
    let catalog = RestEngineCatalog::from_plan(session(&profile, "lakecat").await, &plan).unwrap();

    let load = catalog.load_table().await.unwrap();
    let EngineTableLoad::Present {
        http_status,
        response_bytes,
        state,
    } = &load
    else {
        panic!("table fixture must be present");
    };
    assert_eq!(*http_status, 200);
    assert!(*response_bytes > 0);
    assert_eq!(state.current_schema_id, 1);
    assert_eq!(state.table.format_version, 2);
    assert_eq!(state.table.last_column_id, 4);
    assert_eq!(state.table.schema.len(), 4);
    assert_eq!(state.table.schema[3].name, "note");
    assert_eq!(state.table.snapshots, 2);
    assert_eq!(
        state.table.properties.keys().collect::<Vec<_>>(),
        plan.scenario().table.properties.keys().collect::<Vec<_>>()
    );
    assert!(state
        .table
        .properties
        .values()
        .all(|outcome| *outcome == EnginePropertyObservation::Match));
    assert!(!serde_json::to_string(&load).unwrap().contains(private));

    let table_cleanup = catalog.drop_table_without_purge().await.unwrap();
    assert_eq!(table_cleanup.http_status, 204);
    assert!(!table_cleanup.already_absent);
    assert!(catalog.table_presence().await.unwrap().is_absent());
    let namespace_cleanup = catalog.drop_namespace().await.unwrap();
    assert_eq!(namespace_cleanup.http_status, 204);
    assert!(catalog.namespace_presence().await.unwrap().is_absent());

    let requests = server.finish();
    assert_eq!(requests[0].target, "/catalog/v1/config");
    assert_eq!(requests[1].method, "GET");
    assert!(requests[1].target.ends_with("/tables/events"));
    assert!(requests[1].body.is_empty());
    assert!(!requests[1].headers.contains_key("idempotency-key"));
    assert_eq!(requests[2].method, "DELETE");
    assert!(requests[2].target.ends_with("?purgeRequested=false"));
    assert_eq!(requests[3].method, "GET");
    assert!(requests[3].target.ends_with("/tables/events"));
    assert_eq!(requests[4].method, "DELETE");
    assert!(!requests[4].target.contains("purge"));
    assert_eq!(requests[5].method, "GET");
    assert!(!requests[5].target.contains("/tables/"));
}

#[tokio::test]
async fn projection_replaces_wrong_owned_properties_and_discards_unknown_values() {
    let private = "property-secret-sentinel";
    let mut response = table_response(private);
    response["metadata"]["properties"]["catalog-bench.owner"] = json!(private);
    let server = MockServer::start(vec![
        MockResponse::json(json!({"defaults": {}, "overrides": {}})),
        MockResponse::json(response),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());
    let plan = interoperability_plan(&profile, &scenario, "catalog02");
    let catalog = RestEngineCatalog::from_plan(session(&profile, "lakecat").await, &plan).unwrap();

    let load = catalog.load_table().await.unwrap();
    let owner = &load.state().unwrap().table.properties["catalog-bench.owner"];
    assert_eq!(*owner, EnginePropertyObservation::Mismatch);
    assert!(!serde_json::to_string(&load).unwrap().contains(private));
    assert_eq!(server.finish().len(), 2);
}

#[tokio::test]
async fn malformed_or_absent_rest_state_has_fixed_secret_free_classification() {
    let private = "field-secret-sentinel";
    let mut response = table_response("unknown");
    response["metadata"]["schemas"][1]["fields"][3]["name"] = json!(private);
    let server = MockServer::start(vec![
        MockResponse::json(json!({"defaults": {}, "overrides": {}})),
        MockResponse::json(response),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());
    let plan = interoperability_plan(&profile, &scenario, "catalog03");
    let catalog = RestEngineCatalog::from_plan(session(&profile, "lakecat").await, &plan).unwrap();
    let failure = catalog.load_table().await.unwrap_err();
    assert_eq!(failure.kind, EngineCatalogFailureKind::MalformedResponse);
    assert_eq!(failure.http_status, Some(200));
    assert!(!serde_json::to_string(&failure).unwrap().contains(private));
    assert_eq!(server.finish().len(), 2);

    let server = MockServer::start(vec![
        MockResponse::json(json!({"defaults": {}, "overrides": {}})),
        MockResponse::empty(404),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());
    let plan = interoperability_plan(&profile, &scenario, "catalog04");
    let load = RestEngineCatalog::from_plan(session(&profile, "lakecat").await, &plan)
        .unwrap()
        .load_table()
        .await
        .unwrap();
    assert!(matches!(
        load,
        EngineTableLoad::Absent {
            http_status: 404,
            ..
        }
    ));
    assert_eq!(server.finish().len(), 2);

    let server = MockServer::start(vec![
        MockResponse::json(json!({"defaults": {}, "overrides": {}})),
        MockResponse::oversized(CATALOG_RESPONSE_LIMIT_BYTES + 1),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());
    let plan = interoperability_plan(&profile, &scenario, "catalog05");
    let failure = RestEngineCatalog::from_plan(session(&profile, "lakecat").await, &plan)
        .unwrap()
        .load_table()
        .await
        .unwrap_err();
    assert_eq!(failure.kind, EngineCatalogFailureKind::ResponseTooLarge);
    assert_eq!(failure.http_status, Some(200));
    assert_eq!(server.finish().len(), 2);
}

#[tokio::test]
async fn cleanup_distinguishes_absence_and_discards_unexpected_response_bodies() {
    let private = "cleanup-private-sentinel";
    let server = MockServer::start(vec![
        MockResponse::json(json!({"defaults": {}, "overrides": {}})),
        MockResponse::empty(404),
        MockResponse::status_json(409, json!({"error": private})),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());
    let plan = interoperability_plan(&profile, &scenario, "catalog06");
    let catalog = RestEngineCatalog::from_plan(session(&profile, "lakecat").await, &plan).unwrap();

    let table_cleanup = catalog.drop_table_without_purge().await.unwrap();
    assert_eq!(table_cleanup.http_status, 404);
    assert_eq!(table_cleanup.response_bytes, 0);
    assert!(table_cleanup.already_absent);

    let failure = catalog.drop_namespace().await.unwrap_err();
    assert_eq!(failure.kind, EngineCatalogFailureKind::UnexpectedHttp);
    assert_eq!(failure.http_status, Some(409));
    assert!(!failure.to_string().contains(private));
    assert!(!serde_json::to_string(&failure).unwrap().contains(private));

    let requests = server.finish();
    assert_eq!(requests[1].method, "DELETE");
    assert!(requests[1].target.ends_with("?purgeRequested=false"));
    assert_eq!(requests[2].method, "DELETE");
}

async fn session(profile: &Profile, catalog: &str) -> catalog_bench_conformance::CatalogSession {
    let attempt = connect_catalog_adapter(
        profile,
        &ComponentId::from(catalog),
        30_000,
        CATALOG_RESPONSE_LIMIT_BYTES,
        |_| None,
    )
    .await
    .unwrap();
    let CatalogConnectionOutcome::Ready(session) = attempt.outcome else {
        panic!("test config must negotiate");
    };
    session
}

fn interoperability_plan(
    profile: &Profile,
    scenario: &Scenario,
    fixture: &str,
) -> InteroperabilityPlan {
    InteroperabilityPlan::from_contracts(profile, scenario, &ComponentId::from("lakecat"), fixture)
        .unwrap()
}

fn table_response(private: &str) -> serde_json::Value {
    json!({
        "metadata-location": "s3://warehouse/lakecat/table/metadata/v3.metadata.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000001",
            "location": "s3://warehouse/lakecat/table",
            "last-column-id": 4,
            "schemas": [
                {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "id", "required": true, "type": "long"},
                        {"id": 2, "name": "category", "required": false, "type": "string"},
                        {"id": 3, "name": "amount_cents", "required": true, "type": "long"}
                    ]
                },
                {
                    "type": "struct",
                    "schema-id": 1,
                    "fields": [
                        {"id": 1, "name": "id", "required": true, "type": "long"},
                        {"id": 2, "name": "category", "required": false, "type": "string"},
                        {"id": 3, "name": "amount_cents", "required": true, "type": "long"},
                        {"id": 4, "name": "note", "required": false, "type": "string"}
                    ]
                }
            ],
            "current-schema-id": 1,
            "snapshots": [
                {"snapshot-id": 1001},
                {"snapshot-id": 1002}
            ],
            "properties": {
                "catalog-bench.owner": "catalog-bench",
                "write.metadata.delete-after-commit.enabled": "false",
                "write.metadata.previous-versions-max": "100000",
                "unretained.private": private
            },
            "unretained-private": private
        },
        "config": {"unretained-private": private}
    })
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
