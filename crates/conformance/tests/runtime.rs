use catalog_bench_common::contract::{
    parse_contract, ComponentId, ContractDocument, Profile, Scenario,
};
use catalog_bench_conformance::{
    connect_catalog, CatalogConnectionOutcome, CatalogNegotiationFailureStage,
    CatalogRequestFailureKind, NamespaceIdentifier, ResponseCapture, CATALOG_RESPONSE_LIMIT_BYTES,
};
use reqwest::Method;
use serde_json::json;

mod support;

use support::{MockResponse, MockServer};

const PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-26.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json");

#[tokio::test]
async fn anonymous_runtime_reuses_negotiated_routing_and_keeps_bodies_private() {
    let private = "private-response-sentinel";
    let config_secret = "config-secret-sentinel";
    let server = MockServer::start(vec![
        MockResponse::json(json!({
            "defaults": {
                "prefix": "warehouse-id",
                "s3.secret-access-key": config_secret
            },
            "overrides": {}
        })),
        MockResponse::json(json!({"private": private})),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakekeeper").endpoint.base_url = format!("{}/catalog", server.url());

    let attempt = connect_catalog(
        &profile,
        &scenario,
        &ComponentId::from("lakekeeper"),
        30_000,
        CATALOG_RESPONSE_LIMIT_BYTES,
        |_| None,
    )
    .await
    .unwrap();
    let evidence = serde_json::to_string(&attempt.evidence).unwrap();
    assert!(!evidence.contains(config_secret));
    let CatalogConnectionOutcome::Ready(session) = attempt.outcome else {
        panic!("valid anonymous config must produce a session");
    };
    let namespace = NamespaceIdentifier::single("fixture".to_owned()).unwrap();
    let response = session
        .request_json(
            Method::GET,
            session.namespace_url(&namespace).unwrap(),
            None,
            ResponseCapture::Json,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert!(response.body_bytes() > 0);
    assert_eq!(
        response.json::<serde_json::Value>().unwrap()["private"],
        private
    );
    assert!(!format!("{response:?}").contains(private));

    let requests = server.finish();
    assert_eq!(requests[0].target, "/catalog/v1/config?warehouse=bench");
    assert_eq!(
        requests[1].target,
        "/catalog/v1/warehouse-id/namespaces/fixture"
    );
    assert_eq!(requests[1].method, "GET");
    assert!(requests[1].body.is_empty());
    assert!(!requests[1].headers.contains_key("authorization"));
    assert!(!requests[1].headers.contains_key("idempotency-key"));
}

#[tokio::test]
async fn oauth_runtime_uses_bearer_auth_without_exposing_credentials_or_extra_headers() {
    let client_id = "runtime-client-id-sentinel";
    let client_secret = "runtime-client-secret-sentinel";
    let bearer_token = "runtime-bearer-token-sentinel";
    let server = MockServer::start(vec![
        MockResponse::json(json!({
            "access_token": bearer_token,
            "token_type": "Bearer"
        })),
        MockResponse::json(json!({"defaults": {}, "overrides": {}})),
        MockResponse::empty(204),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "polaris").endpoint.base_url = format!("{}/catalog", server.url());

    let attempt = connect_catalog(
        &profile,
        &scenario,
        &ComponentId::from("polaris"),
        30_000,
        CATALOG_RESPONSE_LIMIT_BYTES,
        |name| match name {
            "CATALOG_BENCH_POLARIS_CLIENT_ID" => Some(client_id.to_owned()),
            "CATALOG_BENCH_POLARIS_CLIENT_SECRET" => Some(client_secret.to_owned()),
            _ => None,
        },
    )
    .await
    .unwrap();
    let evidence = serde_json::to_string(&attempt.evidence).unwrap();
    for sensitive in [client_id, client_secret, bearer_token] {
        assert!(!evidence.contains(sensitive));
    }
    let CatalogConnectionOutcome::Ready(session) = attempt.outcome else {
        panic!("valid OAuth config must produce a session");
    };
    let namespace = NamespaceIdentifier::single("fixture".to_owned()).unwrap();
    let response = session
        .request_json(
            Method::POST,
            session.table_collection_url(&namespace).unwrap(),
            Some(&json!({"name": "table"})),
            ResponseCapture::Discard,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 204);
    assert!(response.json::<serde_json::Value>().is_err());

    let requests = server.finish();
    assert_eq!(requests[0].target, "/catalog/v1/oauth/tokens");
    assert_eq!(requests[1].target, "/catalog/v1/config?warehouse=bench");
    assert_eq!(
        requests[2].target,
        "/catalog/v1/bench/namespaces/fixture/tables"
    );
    assert_eq!(requests[2].method, "POST");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&requests[2].body).unwrap(),
        json!({"name": "table"})
    );
    assert_eq!(
        requests[2].headers.get("authorization").map(String::as_str),
        Some("Bearer runtime-bearer-token-sentinel")
    );
    assert!(!requests[2].headers.contains_key("idempotency-key"));
}

#[tokio::test]
async fn runtime_fails_closed_on_bad_config_and_oversized_operation_responses() {
    let bad_config_server = MockServer::start(vec![MockResponse::empty(500)]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url =
        format!("{}/catalog", bad_config_server.url());
    let attempt = connect_catalog(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        30_000,
        CATALOG_RESPONSE_LIMIT_BYTES,
        |_| None,
    )
    .await
    .unwrap();
    assert!(matches!(
        attempt.outcome,
        CatalogConnectionOutcome::Failed(ref failure)
            if failure.stage == CatalogNegotiationFailureStage::Config
    ));
    assert_eq!(bad_config_server.finish().len(), 1);

    let oversized_server = MockServer::start(vec![
        MockResponse::json(json!({"defaults": {}, "overrides": {}})),
        MockResponse::oversized(CATALOG_RESPONSE_LIMIT_BYTES + 1),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url =
        format!("{}/catalog", oversized_server.url());
    let attempt = connect_catalog(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        30_000,
        CATALOG_RESPONSE_LIMIT_BYTES,
        |_| None,
    )
    .await
    .unwrap();
    let CatalogConnectionOutcome::Ready(session) = attempt.outcome else {
        panic!("valid config must produce a session");
    };
    let namespace = NamespaceIdentifier::single("fixture".to_owned()).unwrap();
    let failure = session
        .request_json(
            Method::GET,
            session.namespace_url(&namespace).unwrap(),
            None,
            ResponseCapture::Discard,
        )
        .await
        .unwrap_err();
    assert_eq!(failure.kind, CatalogRequestFailureKind::ResponseTooLarge);
    assert_eq!(failure.http_status, Some(200));
    assert_eq!(oversized_server.finish().len(), 2);
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
