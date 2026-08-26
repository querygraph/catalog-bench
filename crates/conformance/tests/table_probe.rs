use catalog_bench_common::contract::{
    parse_contract, AdapterCapabilityCoverage, AssertionOutcome, CapabilityId,
    CapabilityLimitationSource, ComponentId, ContractDocument, Profile, Scenario,
    UnsupportedAdapterCapability,
};
use catalog_bench_conformance::{
    encode_evidence, run_table_probe, ContractDigests, ProbeClassification,
    TableOperationExecution, TablePaginationTranscript, TableTranscript,
};
use serde_json::{json, Value};

mod support;

use support::{MockResponse, MockServer, RecordedRequest};

const PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-26.json");
const SCENARIO: &[u8] = include_bytes!("../../../scenarios/v1/iceberg-rest.table.behavior.json");
const FIXTURE_ID: &str = "test";

#[tokio::test]
async fn anonymous_probe_covers_full_table_lifecycle_and_cleanup() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let server = MockServer::start(happy_responses(&names));
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(transcript.passed());
    assert!(transcript
        .assertions
        .iter()
        .all(|assertion| matches!(assertion.outcome, AssertionOutcome::Pass)));
    assert_eq!(
        transcript.pagination,
        TablePaginationTranscript::Paginated {
            pages: 2,
            unique_tables: 2,
        }
    );
    assert!(!transcript.sanitization.raw_secrets_persisted);
    assert!(!transcript.sanitization.raw_response_body_persisted);

    let requests = server.finish();
    assert_eq!(requests.len(), 32);
    assert_eq!(requests[0].target, "/catalog/v1/config");
    assert_eq!(
        requests[0].headers.get("accept").map(String::as_str),
        Some("application/json")
    );
    assert_eq!(requests[2].target, "/catalog/v1/namespaces");
    assert_eq!(
        request_json(&requests[2])["namespace"],
        json!([names.namespace])
    );
    assert_eq!(requests[3].method, "POST");
    assert!(requests[3].target.ends_with("/tables"));
    assert_eq!(request_json(&requests[3])["name"], "primary");
    assert_eq!(
        request_json(&requests[3])["location"],
        table_location(&names, "primary")
    );
    assert_eq!(
        request_json(&requests[4])["location"],
        table_location(&names, "sibling")
    );
    assert_eq!(
        request_json(&requests[3])["schema"]["fields"][0],
        json!({"id": 1, "name": "value", "required": false, "type": "long"})
    );
    assert!(requests[8].target.contains("pageToken="));
    assert!(requests[8].target.contains("pageSize=1"));
    assert!(requests[9]
        .target
        .contains("pageToken=page-two-secret-token"));
    assert!(requests[14].target.contains(&names.missing_namespace));
    assert_eq!(requests[15].target, "/catalog/v1/tables/rename");
    assert_eq!(requests[18].method, "DELETE");
    assert!(requests[18]
        .target
        .ends_with("sibling?purgeRequested=false"));
    assert!(requests[20].target.ends_with("/register"));
    assert_eq!(
        request_json(&requests[20])["metadata-location"],
        sibling_location()
    );
    assert_eq!(request_json(&requests[20])["overwrite"], false);
    assert!(requests[22..26]
        .iter()
        .all(|request| request.method == "DELETE"));
    assert!(requests[22..26]
        .iter()
        .all(|request| request.target.ends_with("purgeRequested=false")));
}

#[tokio::test]
async fn create_omits_location_when_the_adapter_selects_its_catalog_default() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let server = MockServer::start(happy_responses(&names));
    let (mut profile, scenario) = contracts();
    let adapter = adapter_mut(&mut profile, "lakecat");
    adapter.endpoint.base_url = format!("{}/catalog", server.url());
    adapter.endpoint.create_table_location = None;

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(transcript.passed());
    let requests = server.finish();
    for index in [3, 4, 12] {
        assert!(request_json(&requests[index]).get("location").is_none());
    }
}

#[tokio::test]
async fn optional_rename_and_register_failures_remain_visible_without_failing_required_behavior() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses[15] = error_response(406, "UnsupportedOperationException");
    responses[16] = updated_table_response(
        primary_updated_location(),
        &table_location(&names, "primary"),
        "primary-uuid",
    );
    responses[17] = error_response(404, "NoSuchTableException");
    responses[20] = error_response(406, "UnsupportedOperationException");
    responses[21] = error_response(404, "NoSuchTableException");
    responses[22] = MockResponse::empty(204);
    responses[23] = error_response(404, "NoSuchTableException");
    responses[25] = error_response(404, "NoSuchTableException");
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(transcript.passed());
    for id in ["table-rename-round-trip", "table-register-round-trip"] {
        let assertion = assertion(&transcript, id);
        assert!(!assertion.required);
        assert!(matches!(
            assertion.outcome,
            AssertionOutcome::Fail { ref explanation } if explanation.contains("HTTP 406")
        ));
    }
    assert!(transcript
        .assertions
        .iter()
        .filter(|assertion| assertion.required)
        .all(|assertion| matches!(assertion.outcome, AssertionOutcome::Pass)));
    assert_eq!(server.finish().len(), 32);
}

#[tokio::test]
async fn missing_namespace_success_response_fails_required_error_shape_but_still_cleans_up() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses[14] = list_response(Vec::new(), None);
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        transcript.classification,
        ProbeClassification::Fail { .. }
    ));
    assert!(matches!(
        assertion(&transcript, "missing-namespace-error-spec-shaped").outcome,
        AssertionOutcome::Fail { ref explanation } if explanation.contains("HTTP 200")
    ));
    assert!(matches!(
        assertion(&transcript, "table-fixture-clean").outcome,
        AssertionOutcome::Pass
    ));
    assert_eq!(server.finish().len(), 32);
}

#[tokio::test]
async fn pagination_rejects_duplicate_tables_and_cleanup_still_runs() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses[9] = list_response(vec![names.identifier("primary")], None);
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        transcript.pagination,
        TablePaginationTranscript::Failed { ref explanation }
            if explanation.contains("duplicate table")
    ));
    assert!(matches!(
        assertion(&transcript, "table-pagination-complete").outcome,
        AssertionOutcome::Fail { .. }
    ));
    assert!(matches!(
        assertion(&transcript, "table-fixture-clean").outcome,
        AssertionOutcome::Pass
    ));
    assert_eq!(server.finish().len(), 32);
}

#[tokio::test]
async fn isolated_namespace_listing_rejects_unexpected_tables() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses[5] = list_response(
        vec![
            names.identifier("primary"),
            names.identifier("sibling"),
            names.identifier("unexpected"),
        ],
        None,
    );
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        assertion(&transcript, "table-list-visible").outcome,
        AssertionOutcome::Fail { ref explanation } if explanation.contains("expected exactly")
    ));
    assert!(matches!(
        assertion(&transcript, "table-fixture-clean").outcome,
        AssertionOutcome::Pass
    ));
    assert_eq!(server.finish().len(), 32);
}

#[tokio::test]
async fn wrong_duplicate_error_type_fails_without_short_circuiting_cleanup() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses[12] = error_response(409, "ConflictException");
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        assertion(&transcript, "duplicate-table-error-spec-shaped").outcome,
        AssertionOutcome::Fail { ref explanation }
            if explanation.contains("ConflictException")
                && explanation.contains("AlreadyExistsException")
    ));
    assert!(matches!(
        assertion(&transcript, "table-fixture-clean").outcome,
        AssertionOutcome::Pass
    ));
    assert_eq!(server.finish().len(), 32);
}

#[tokio::test]
async fn preflight_collision_prevents_every_mutation() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let server = MockServer::start(vec![
        config_response(),
        namespace_response(&names.namespace),
        error_response(404, "NoSuchNamespaceException"),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        assertion(&transcript, "fixture-isolated").outcome,
        AssertionOutcome::Fail { .. }
    ));
    assert!(matches!(
        assertion(&transcript, "table-fixture-clean").outcome,
        AssertionOutcome::NotEvaluated { .. }
    ));
    assert!(transcript.operations.iter().all(|operation| {
        operation.id == "preflight-namespace"
            || operation.id == "list-tables-missing-namespace"
            || matches!(
                operation.execution,
                TableOperationExecution::NotAttempted { .. }
            )
    }));
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|request| request.method == "GET"));
}

#[tokio::test]
async fn unpaginated_fallback_is_explicit_and_permitted() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses[8] = list_response(
        vec![names.identifier("primary"), names.identifier("sibling")],
        None,
    );
    responses.remove(9);
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(transcript.passed());
    assert_eq!(
        transcript.pagination,
        TablePaginationTranscript::UnpaginatedFallback { unique_tables: 2 }
    );
    assert_eq!(server.finish().len(), 31);
}

#[tokio::test]
async fn metadata_location_must_advance_and_cleanup_still_runs() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses[10] = updated_table_response(
        primary_location(),
        &table_location(&names, "primary"),
        "primary-uuid",
    );
    responses[11] = updated_table_response(
        primary_location(),
        &table_location(&names, "primary"),
        "primary-uuid",
    );
    responses.drain(15..18);
    responses[19] = MockResponse::empty(204);
    responses[20] = error_response(404, "NoSuchTableException");
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        assertion(&transcript, "table-update-round-trip").outcome,
        AssertionOutcome::Fail { ref explanation }
            if explanation.contains("did not advance the metadata location")
    ));
    assert!(matches!(
        assertion(&transcript, "table-rename-round-trip").outcome,
        AssertionOutcome::NotEvaluated { .. }
    ));
    assert!(matches!(
        assertion(&transcript, "table-fixture-clean").outcome,
        AssertionOutcome::Pass
    ));
    assert_eq!(server.finish().len(), 29);
}

#[tokio::test]
async fn requested_table_location_must_be_preserved_and_cleanup_still_runs() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let server = MockServer::start(vec![
        config_response(),
        error_response(404, "NoSuchNamespaceException"),
        namespace_response(&names.namespace),
        initial_table_response(
            primary_location(),
            "s3://warehouse/not-the-requested-location",
            "primary-uuid",
        ),
        initial_table_response(
            sibling_location(),
            &table_location(&names, "sibling"),
            "sibling-uuid",
        ),
        initial_table_response(
            sibling_location(),
            &table_location(&names, "sibling"),
            "sibling-uuid",
        ),
        error_response(404, "NoSuchTableException"),
        error_response(404, "NoSuchNamespaceException"),
        MockResponse::empty(204),
        error_response(404, "NoSuchTableException"),
        MockResponse::empty(204),
        error_response(404, "NoSuchTableException"),
        error_response(404, "NoSuchTableException"),
        error_response(404, "NoSuchTableException"),
        error_response(404, "NoSuchTableException"),
        error_response(404, "NoSuchTableException"),
        MockResponse::empty(204),
        error_response(404, "NoSuchNamespaceException"),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        assertion(&transcript, "table-create-round-trip").outcome,
        AssertionOutcome::Fail { ref explanation }
            if explanation.contains("does not preserve requested table location")
    ));
    assert!(matches!(
        assertion(&transcript, "table-fixture-clean").outcome,
        AssertionOutcome::Pass
    ));
}

#[tokio::test]
async fn invalid_fixture_and_scenario_drift_are_rejected_before_transport() {
    let (profile, scenario) = contracts();
    let invalid_fixture = run_table_probe(
        &profile,
        &scenario,
        &ComponentId::new("lakecat"),
        "NOT_VALID",
        digests(),
        |_| None,
    )
    .await
    .expect_err("invalid fixture should be rejected");
    assert!(invalid_fixture.to_string().contains("only lowercase ASCII"));

    let mut drifted = scenario;
    drifted.parameters.insert("page_size".to_owned(), json!(2));
    let policy_error = run_table_probe(
        &profile,
        &drifted,
        &ComponentId::new("lakecat"),
        FIXTURE_ID,
        digests(),
        |_| None,
    )
    .await
    .expect_err("scenario drift should be rejected");
    assert!(policy_error
        .to_string()
        .contains("scenario parameters drifted"));
}

#[tokio::test]
async fn page_tokens_are_redacted_from_serialized_evidence() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let server = MockServer::start(happy_responses(&names));
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;
    let evidence = encode_evidence(&transcript).expect("evidence should serialize");
    let text = String::from_utf8(evidence).expect("evidence should be UTF-8");

    assert!(!text.contains("page-two-secret-token"));
    assert!(text.contains("<redacted>"));
    assert!(transcript
        .sanitization
        .redactions
        .iter()
        .any(|path| path.contains("next-page-token")));
    assert_eq!(server.finish().len(), 32);
}

#[tokio::test]
async fn oversized_preflight_response_is_bounded_without_risking_cleanup() {
    let server = MockServer::start(vec![
        config_response(),
        MockResponse::oversized(2 * 1024 * 1024),
        error_response(404, "NoSuchNamespaceException"),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        assertion(&transcript, "fixture-isolated").outcome,
        AssertionOutcome::Fail { .. }
    ));
    assert!(matches!(
        assertion(&transcript, "table-fixture-clean").outcome,
        AssertionOutcome::NotEvaluated { .. }
    ));
    let operation = transcript
        .operations
        .iter()
        .find(|operation| operation.id == "preflight-namespace")
        .expect("preflight operation should exist");
    let TableOperationExecution::Attempted {
        response: Some(response),
        failure: Some(_),
        ..
    } = &operation.execution
    else {
        panic!("oversized response should be captured as a bounded failure");
    };
    assert_eq!(response.body_bytes_observed, 2 * 1024 * 1024);
    assert_eq!(server.finish().len(), 3);
}

#[tokio::test]
async fn oauth_credentials_bearer_and_page_tokens_never_persist() {
    let client_id = "table-oauth-client-id-sentinel";
    let client_secret = "table-oauth-client-secret-sentinel";
    let bearer_token = "table-oauth-bearer-token-sentinel";
    let names = FixtureNames::new("polaris", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses.insert(
        0,
        MockResponse::json(json!({
            "access_token": bearer_token,
            "token_type": "Bearer"
        })),
    );
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "polaris").endpoint.base_url =
        format!("{}/api/catalog", server.url());

    let transcript = probe(
        &profile,
        &scenario,
        "polaris",
        FIXTURE_ID,
        |name| match name {
            "CATALOG_BENCH_POLARIS_CLIENT_ID" => Some(client_id.to_owned()),
            "CATALOG_BENCH_POLARIS_CLIENT_SECRET" => Some(client_secret.to_owned()),
            _ => None,
        },
    )
    .await;

    assert!(transcript.passed());
    let evidence = String::from_utf8(encode_evidence(&transcript).expect("evidence encodes"))
        .expect("evidence is UTF-8");
    for secret in [
        client_id,
        client_secret,
        bearer_token,
        "page-two-secret-token",
    ] {
        assert!(!evidence.contains(secret));
    }
    assert!(transcript
        .sanitization
        .redactions
        .iter()
        .any(|path| path.contains("authorization")));
    let requests = server.finish();
    assert_eq!(requests.len(), 33);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].target, "/api/catalog/v1/oauth/tokens");
    assert!(requests[1..].iter().all(|request| request
        .headers
        .get("authorization")
        .is_some_and(|value| value == "Bearer table-oauth-bearer-token-sentinel")));
}

#[tokio::test]
async fn declared_required_limitation_skips_transport() {
    let server = MockServer::start(Vec::new());
    let (mut profile, scenario) = contracts();
    declare_unsupported(&mut profile, "lakecat", &["iceberg-rest.table.create"]);

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        transcript.classification,
        ProbeClassification::Unsupported { ref capability, .. }
            if capability.as_str() == "iceberg-rest.table.create"
    ));
    assert!(transcript.operations.is_empty());
    assert!(transcript
        .assertions
        .iter()
        .all(|assertion| matches!(assertion.outcome, AssertionOutcome::NotEvaluated { .. })));
    assert!(server.finish().is_empty());
}

#[tokio::test]
async fn declared_optional_limitations_are_not_attempted_or_relabelled_as_failures() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses.drain(15..18);
    responses.drain(17..19);
    responses[17] = MockResponse::empty(204);
    responses[18] = error_response(404, "NoSuchTableException");
    responses[20] = error_response(404, "NoSuchTableException");
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());
    declare_unsupported(
        &mut profile,
        "lakecat",
        &["iceberg-rest.table.rename", "iceberg-rest.table.register"],
    );

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(transcript.passed());
    for id in ["table-rename-round-trip", "table-register-round-trip"] {
        assert!(matches!(
            assertion(&transcript, id).outcome,
            AssertionOutcome::NotEvaluated { ref reason }
                if reason.contains("profile declares") && reason.contains("unsupported")
        ));
    }
    for id in ["rename-primary", "register-sibling-metadata"] {
        let operation = transcript
            .operations
            .iter()
            .find(|operation| operation.id == id)
            .expect("optional operation should be classified");
        assert!(matches!(
            operation.execution,
            TableOperationExecution::NotAttempted { .. }
        ));
    }
    assert_eq!(server.finish().len(), 27);
}

async fn probe<F>(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &str,
    fixture_id: &str,
    getenv: F,
) -> TableTranscript
where
    F: Fn(&str) -> Option<String>,
{
    run_table_probe(
        profile,
        scenario,
        &ComponentId::new(catalog),
        fixture_id,
        digests(),
        getenv,
    )
    .await
    .expect("probe should produce evidence")
}

fn contracts() -> (Profile, Scenario) {
    let profile = match parse_contract(PROFILE).expect("checked-in profile should validate") {
        ContractDocument::Profile(profile) => profile,
        document => panic!("expected profile, found {}", document.kind()),
    };
    let scenario = match parse_contract(SCENARIO).expect("checked-in scenario should validate") {
        ContractDocument::Scenario(scenario) => scenario,
        document => panic!("expected scenario, found {}", document.kind()),
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
        .expect("profile should contain requested adapter")
}

fn declare_unsupported(profile: &mut Profile, catalog: &str, unsupported: &[&str]) {
    let exercise = profile
        .catalog_capabilities
        .iter()
        .map(|capability| capability.id.clone())
        .filter(|capability| !unsupported.contains(&capability.as_str()))
        .collect();
    let unsupported = unsupported
        .iter()
        .map(|capability| UnsupportedAdapterCapability {
            capability: CapabilityId::new(*capability),
            attributed_to: CapabilityLimitationSource::Catalog,
            explanation: "fixture catalog does not implement this optional route".to_owned(),
            upstream_reference: None,
        })
        .collect();
    adapter_mut(profile, catalog).capabilities = AdapterCapabilityCoverage::Explicit {
        exercise,
        unsupported,
    };
}

fn assertion<'a>(
    transcript: &'a TableTranscript,
    id: &str,
) -> &'a catalog_bench_conformance::ProbeAssertion {
    transcript
        .assertions
        .iter()
        .find(|assertion| assertion.assertion.as_str() == id)
        .expect("assertion should exist")
}

fn request_json(request: &RecordedRequest) -> Value {
    serde_json::from_str(&request.body).expect("request body should be JSON")
}

fn digests() -> ContractDigests {
    ContractDigests {
        profile_sha256: "0".repeat(64),
        scenario_sha256: "1".repeat(64),
    }
}

struct FixtureNames {
    namespace: String,
    missing_namespace: String,
}

impl FixtureNames {
    fn new(catalog: &str, id: &str) -> Self {
        let namespace = format!("cb_c105_{}_{}", catalog.replace('-', "_"), id);
        Self {
            missing_namespace: format!("{namespace}_missing"),
            namespace,
        }
    }

    fn identifier(&self, name: &str) -> Value {
        json!({"namespace": [self.namespace], "name": name})
    }
}

fn happy_responses(names: &FixtureNames) -> Vec<MockResponse> {
    vec![
        config_response(),
        error_response(404, "NoSuchNamespaceException"),
        namespace_response(&names.namespace),
        initial_table_response(
            primary_location(),
            &table_location(names, "primary"),
            "primary-uuid",
        ),
        initial_table_response(
            sibling_location(),
            &table_location(names, "sibling"),
            "sibling-uuid",
        ),
        list_response(
            vec![names.identifier("primary"), names.identifier("sibling")],
            None,
        ),
        initial_table_response(
            primary_location(),
            &table_location(names, "primary"),
            "primary-uuid",
        ),
        initial_table_response(
            sibling_location(),
            &table_location(names, "sibling"),
            "sibling-uuid",
        ),
        list_response(
            vec![names.identifier("primary")],
            Some("page-two-secret-token"),
        ),
        list_response(vec![names.identifier("sibling")], None),
        updated_table_response(
            primary_updated_location(),
            &table_location(names, "primary"),
            "primary-uuid",
        ),
        updated_table_response(
            primary_updated_location(),
            &table_location(names, "primary"),
            "primary-uuid",
        ),
        error_response(409, "AlreadyExistsException"),
        error_response(404, "NoSuchTableException"),
        error_response(404, "NoSuchNamespaceException"),
        MockResponse::empty(204),
        error_response(404, "NoSuchTableException"),
        updated_table_response(
            primary_updated_location(),
            &table_location(names, "primary"),
            "primary-uuid",
        ),
        MockResponse::empty(204),
        error_response(404, "NoSuchTableException"),
        initial_table_response(
            sibling_location(),
            &table_location(names, "sibling"),
            "sibling-uuid",
        ),
        initial_table_response(
            sibling_location(),
            &table_location(names, "sibling"),
            "sibling-uuid",
        ),
        error_response(404, "NoSuchTableException"),
        MockResponse::empty(204),
        error_response(404, "NoSuchTableException"),
        MockResponse::empty(204),
        error_response(404, "NoSuchTableException"),
        error_response(404, "NoSuchTableException"),
        error_response(404, "NoSuchTableException"),
        error_response(404, "NoSuchTableException"),
        MockResponse::empty(204),
        error_response(404, "NoSuchNamespaceException"),
    ]
}

fn config_response() -> MockResponse {
    MockResponse::json(json!({
        "defaults": {"namespace-separator": "%1F"},
        "overrides": {}
    }))
}

fn namespace_response(namespace: &str) -> MockResponse {
    MockResponse::json(json!({
        "namespace": [namespace],
        "properties": {"catalog-bench.owner": "catalog-bench"}
    }))
}

fn initial_table_response(
    metadata_location: &str,
    table_location: &str,
    uuid: &str,
) -> MockResponse {
    table_response(
        metadata_location,
        table_location,
        uuid,
        json!({
            "catalog-bench.owner": "catalog-bench",
            "c1-05.remove": "before",
            "c1-05.state": "before"
        }),
    )
}

fn updated_table_response(
    metadata_location: &str,
    table_location: &str,
    uuid: &str,
) -> MockResponse {
    table_response(
        metadata_location,
        table_location,
        uuid,
        json!({
            "catalog-bench.owner": "catalog-bench",
            "c1-05.state": "after"
        }),
    )
}

fn table_response(
    metadata_location: &str,
    table_location: &str,
    uuid: &str,
    properties: Value,
) -> MockResponse {
    MockResponse::json(json!({
        "metadata-location": metadata_location,
        "metadata": {
            "format-version": 2,
            "table-uuid": uuid,
            "location": table_location,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "value", "required": false, "type": "long"}
                ]
            }],
            "properties": properties
        },
        "config": {}
    }))
}

fn list_response(identifiers: Vec<Value>, next_page_token: Option<&str>) -> MockResponse {
    let mut response = json!({"identifiers": identifiers});
    if let Some(token) = next_page_token {
        response["next-page-token"] = Value::from(token);
    }
    MockResponse::json(response)
}

fn error_response(status: u16, error_type: &str) -> MockResponse {
    MockResponse::status_json(
        status,
        json!({
            "error": {
                "message": format!("fixture {error_type}"),
                "type": error_type,
                "code": status
            }
        }),
    )
}

fn primary_location() -> &'static str {
    "s3://warehouse/primary/metadata/00000.json"
}

fn primary_updated_location() -> &'static str {
    "s3://warehouse/primary/metadata/00001.json"
}

fn sibling_location() -> &'static str {
    "s3://warehouse/sibling/metadata/00000.json"
}

fn table_location(names: &FixtureNames, table: &str) -> String {
    format!("s3://warehouse/lakecat/{}/{table}", names.namespace)
}
