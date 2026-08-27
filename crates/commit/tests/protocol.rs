use catalog_bench_commit::model::{RequestIdentity, RequestOutcome};
use catalog_bench_commit::policy::ContentionFixture;
use catalog_bench_commit::protocol::{
    CatalogFailureKind, CatalogPort, ResourcePresence, RestCatalog,
};
use catalog_bench_common::contract::{
    parse_contract, ComponentId, ContractDocument, Profile, Scenario,
};
use catalog_bench_conformance::{
    connect_catalog, CatalogConnectionOutcome, CATALOG_RESPONSE_LIMIT_BYTES,
};
use serde_json::{json, Value};

#[path = "../../conformance/tests/support/mod.rs"]
mod support;

use support::{MockResponse, MockServer};

const PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-26.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json");

#[tokio::test]
async fn rest_fixture_uses_standard_routes_requirements_and_nonpurging_cleanup() {
    let fixture = ContentionFixture {
        id: "test".to_owned(),
        namespace: "cb_c108_lakecat_test_r01".to_owned(),
        table: "same_table_contention".to_owned(),
    };
    let location = format!(
        "s3://warehouse/lakecat/{}/{}",
        fixture.namespace, fixture.table
    );
    let initial = table_response(
        &location,
        "00000",
        json!({"catalog-bench.owner": "catalog-bench"}),
    );
    let final_property = "lakecat/test/1/sequential/0";
    let final_table = table_response(
        &location,
        "00001",
        json!({
            "catalog-bench.owner": "catalog-bench",
            "catalog-bench.contention.request-id": final_property
        }),
    );
    let server = MockServer::start(vec![
        MockResponse::json(json!({"defaults": {}, "overrides": {}})),
        MockResponse::empty(404),
        MockResponse::empty(200),
        MockResponse::json(initial),
        MockResponse::empty(200),
        MockResponse::empty(409),
        MockResponse::json(final_table),
        MockResponse::empty(204),
        MockResponse::empty(404),
        MockResponse::empty(204),
        MockResponse::empty(404),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());
    let session = session(&profile, &scenario, "lakecat").await;
    let catalog = RestCatalog::new(session, Some("s3://warehouse/lakecat"))
        .unwrap()
        .bind(&fixture)
        .unwrap();

    assert_eq!(
        catalog.namespace_presence().await.unwrap().presence,
        ResourcePresence::Absent
    );
    assert_eq!(catalog.create_namespace().await.unwrap().http_status, 200);
    let created = catalog.create_table().await.unwrap();
    assert_eq!(created.format_version, 2);
    assert_eq!(created.location, location);
    assert_eq!(catalog.requested_location(), Some(location.as_str()));

    let accepted = RequestIdentity::new(final_property).unwrap();
    assert!(matches!(
        catalog
            .commit(
                &created.table_uuid,
                "catalog-bench.contention.request-id",
                &accepted,
            )
            .await,
        RequestOutcome::Accepted
    ));
    let conflict = RequestIdentity::new("lakecat/test/1/concurrent/0/0").unwrap();
    assert!(matches!(
        catalog
            .commit(
                &created.table_uuid,
                "catalog-bench.contention.request-id",
                &conflict,
            )
            .await,
        RequestOutcome::Conflict
    ));
    let loaded = catalog.load_table().await.unwrap();
    assert_eq!(
        loaded.properties["catalog-bench.contention.request-id"],
        final_property
    );
    assert_eq!(
        catalog
            .drop_table_without_purge()
            .await
            .unwrap()
            .http_status,
        204
    );
    assert_eq!(
        catalog.table_presence().await.unwrap().presence,
        ResourcePresence::Absent
    );
    assert_eq!(catalog.drop_namespace().await.unwrap().http_status, 204);
    assert_eq!(
        catalog.namespace_presence().await.unwrap().presence,
        ResourcePresence::Absent
    );

    let requests = server.finish();
    assert_eq!(requests.len(), 11);
    assert_eq!(requests[1].method, "GET");
    assert_eq!(
        requests[1].target,
        "/catalog/v1/namespaces/cb_c108_lakecat_test_r01"
    );
    assert_eq!(requests[2].method, "POST");
    assert_eq!(
        serde_json::from_str::<Value>(&requests[2].body).unwrap(),
        json!({"namespace": [fixture.namespace], "properties": {}})
    );
    let create_body: Value = serde_json::from_str(&requests[3].body).unwrap();
    assert_eq!(create_body["stage-create"], false);
    assert_eq!(create_body["location"], location);
    assert_eq!(create_body["properties"]["format-version"], "2");
    let commit_body: Value = serde_json::from_str(&requests[4].body).unwrap();
    assert_eq!(
        commit_body["requirements"],
        json!([{"type": "assert-table-uuid", "uuid": "table-uuid"}])
    );
    assert_eq!(
        commit_body["updates"][0]["updates"]["catalog-bench.contention.request-id"],
        final_property
    );
    assert!(!requests[4].headers.contains_key("idempotency-key"));
    assert_eq!(requests[7].method, "DELETE");
    assert!(requests[7].target.ends_with("?purgeRequested=false"));
}

#[tokio::test]
async fn rest_fixture_rejects_non_v2_or_location_drifted_create_responses() {
    let fixture = ContentionFixture {
        id: "test".to_owned(),
        namespace: "cb_c108_lakecat_test_r01".to_owned(),
        table: "same_table_contention".to_owned(),
    };
    let mut response = table_response("s3://warehouse/unexpected", "00000", json!({}));
    response["metadata"]["format-version"] = json!(1);
    let server = MockServer::start(vec![
        MockResponse::json(json!({"defaults": {}, "overrides": {}})),
        MockResponse::json(response),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());
    let catalog = RestCatalog::new(
        session(&profile, &scenario, "lakecat").await,
        Some("s3://warehouse/lakecat"),
    )
    .unwrap()
    .bind(&fixture)
    .unwrap();

    let failure = catalog.create_table().await.unwrap_err();
    assert_eq!(failure.kind, CatalogFailureKind::MalformedResponse);
    assert_eq!(failure.http_status, Some(200));

    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].method, "POST");
    assert!(!requests[1].body.is_empty());
}

#[tokio::test]
async fn rest_fixture_classifies_oversized_create_responses_without_reading_raw_bodies() {
    let fixture = ContentionFixture {
        id: "test".to_owned(),
        namespace: "cb_c108_lakecat_test_r01".to_owned(),
        table: "same_table_contention".to_owned(),
    };
    let server = MockServer::start(vec![
        MockResponse::json(json!({"defaults": {}, "overrides": {}})),
        MockResponse::oversized(CATALOG_RESPONSE_LIMIT_BYTES + 1),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());
    let catalog = RestCatalog::new(
        session(&profile, &scenario, "lakecat").await,
        Some("s3://warehouse/lakecat"),
    )
    .unwrap()
    .bind(&fixture)
    .unwrap();

    let failure = catalog.create_table().await.unwrap_err();
    assert_eq!(failure.kind, CatalogFailureKind::ResponseTooLarge);
    assert_eq!(failure.http_status, Some(200));

    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].method, "POST");
    assert!(!requests[1].body.is_empty());
}

async fn session(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &str,
) -> catalog_bench_conformance::CatalogSession {
    let attempt = connect_catalog(
        profile,
        scenario,
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

fn table_response(location: &str, sequence: &str, properties: Value) -> Value {
    json!({
        "metadata-location": format!("{location}/metadata/{sequence}.metadata.json"),
        "metadata": {
            "format-version": 2,
            "table-uuid": "table-uuid",
            "location": location,
            "properties": properties
        }
    })
}

fn contracts() -> (Profile, Scenario) {
    let ContractDocument::Profile(profile) = parse_contract(PROFILE).unwrap() else {
        panic!("fixture profile must be a profile");
    };
    let ContractDocument::Scenario(scenario) = parse_contract(SCENARIO).unwrap() else {
        panic!("fixture scenario must be a scenario");
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
