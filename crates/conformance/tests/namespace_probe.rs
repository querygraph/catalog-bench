use catalog_bench_common::contract::{
    parse_contract, AdapterCapabilityCoverage, AssertionCheck, AssertionOutcome,
    CapabilityLimitationSource, ComponentId, ContractDocument, Profile, Scenario,
    UnsupportedAdapterCapability,
};
use catalog_bench_conformance::{
    encode_evidence, run_namespace_probe, ContractDigests, NamespaceOperationExecution,
    NamespaceTranscript, PaginationTranscript, ProbeClassification, SanitizedResponseBody,
};
use serde_json::{json, Value};

mod support;

use support::{MockResponse, MockServer, RecordedRequest};

const PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-26.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/iceberg-rest.namespace.behavior.json");
const FIXTURE_ID: &str = "test";

#[tokio::test]
async fn anonymous_probe_covers_lifecycle_hierarchy_pagination_and_cleanup() {
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
        PaginationTranscript::Paginated {
            pages: 2,
            unique_namespaces: 2,
        }
    );
    assert!(!transcript.sanitization.raw_secrets_persisted);
    assert!(!transcript.sanitization.raw_response_body_persisted);

    let requests = server.finish();
    assert_eq!(requests.len(), 22);
    assert_eq!(requests[0].target, "/catalog/v1/config");
    assert_eq!(requests[4].method, "POST");
    assert_eq!(requests[4].target, "/catalog/v1/namespaces");
    assert_eq!(
        request_json(&requests[4])["namespace"],
        json!([names.primary])
    );
    assert_eq!(
        request_json(&requests[6])["namespace"],
        json!([names.primary, "child"])
    );
    assert!(requests[12]
        .target
        .contains(&format!("parent={}", names.primary)));
    assert!(requests[13].target.contains("pageToken="));
    assert!(requests[13].target.contains("pageSize=1"));
    assert_eq!(requests[16].method, "DELETE");
    assert!(requests[16]
        .target
        .to_ascii_uppercase()
        .contains("%1FCHILD"));
    assert_eq!(requests[17].method, "DELETE");
    assert!(requests[17].target.ends_with(&names.sibling));
    assert_eq!(requests[18].method, "DELETE");
    assert!(requests[18].target.ends_with(&names.primary));
}

#[tokio::test]
async fn specification_permitted_unpaginated_fallback_passes() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses[13] = list_response(vec![names.primary_parts(), names.sibling_parts()], None);
    responses.remove(14);
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(transcript.passed());
    assert_eq!(
        transcript.pagination,
        PaginationTranscript::UnpaginatedFallback {
            unique_namespaces: 2,
        }
    );
    assert_eq!(server.finish().len(), 21);
}

#[tokio::test]
async fn unsupported_optional_property_update_is_visible_without_failing_conformance() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses[9] = error_response(406, "NotSupportedException");
    responses[10] = namespace_response(
        names.primary_parts(),
        json!({"owner": "catalog-bench", "c1-04.remove": "before"}),
    );
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(transcript.passed());
    let assertion = assertion(&transcript, "namespace-properties-updated");
    assert!(!assertion.required);
    assert!(matches!(
        assertion.outcome,
        AssertionOutcome::Fail { ref explanation } if explanation.contains("HTTP 406")
    ));
    assert_eq!(server.finish().len(), 22);
}

#[tokio::test]
async fn wrong_duplicate_error_type_fails_but_cleanup_still_runs() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses[11] = error_response(409, "ConflictException");
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        transcript.classification,
        ProbeClassification::Fail { .. }
    ));
    assert!(matches!(
        assertion(&transcript, "duplicate-error-spec-shaped").outcome,
        AssertionOutcome::Fail { ref explanation }
            if explanation.contains("ConflictException")
    ));
    assert!(matches!(
        assertion(&transcript, "namespace-drop-clean").outcome,
        AssertionOutcome::Pass
    ));
    let requests = server.finish();
    assert_eq!(requests.len(), 22);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "DELETE")
            .count(),
        3
    );
}

#[tokio::test]
async fn missing_parent_success_response_is_rejected() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses[15] = list_response(Vec::new(), None);
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        assertion(&transcript, "missing-parent-error-spec-shaped").outcome,
        AssertionOutcome::Fail { ref explanation } if explanation.contains("HTTP 200")
    ));
    assert!(matches!(
        assertion(&transcript, "namespace-drop-clean").outcome,
        AssertionOutcome::Pass
    ));
    assert_eq!(server.finish().len(), 22);
}

#[tokio::test]
async fn flattened_hierarchy_is_rejected() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses[12] = list_response(vec![vec![format!("{}_child", names.primary)]], None);
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        assertion(&transcript, "namespace-hierarchy-preserved").outcome,
        AssertionOutcome::Fail { ref explanation } if explanation.contains("expected only")
    ));
    assert_eq!(server.finish().len(), 22);
}

#[tokio::test]
async fn pagination_rejects_duplicate_namespaces_across_pages() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names);
    responses[14] = list_response(vec![names.primary_parts()], None);
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        transcript.pagination,
        PaginationTranscript::Failed { ref explanation }
            if explanation.contains("duplicate namespace")
    ));
    assert!(matches!(
        assertion(&transcript, "namespace-pagination-complete").outcome,
        AssertionOutcome::Fail { .. }
    ));
    assert!(matches!(
        assertion(&transcript, "namespace-drop-clean").outcome,
        AssertionOutcome::Pass
    ));
    assert_eq!(server.finish().len(), 22);
}

#[tokio::test]
async fn preflight_collision_prevents_all_mutation() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let server = MockServer::start(vec![
        config_response(),
        namespace_response(names.primary_parts(), json!({})),
        error_response(404, "NoSuchNamespaceException"),
        error_response(404, "NoSuchNamespaceException"),
        error_response(404, "NoSuchNamespaceException"),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        assertion(&transcript, "fixture-isolated").outcome,
        AssertionOutcome::Fail { .. }
    ));
    assert!(transcript.operations.iter().any(|operation| matches!(
        operation.execution,
        NamespaceOperationExecution::NotAttempted { .. }
    )));
    let requests = server.finish();
    assert_eq!(requests.len(), 5);
    assert!(requests
        .iter()
        .all(|request| request.method != "POST" && request.method != "DELETE"));
}

#[tokio::test]
async fn oversized_namespace_response_is_bounded_and_cleanup_is_not_risked() {
    let server = MockServer::start(vec![
        config_response(),
        MockResponse::oversized((1 << 20) + 1),
        error_response(404, "NoSuchNamespaceException"),
        error_response(404, "NoSuchNamespaceException"),
        error_response(404, "NoSuchNamespaceException"),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    let preflight = transcript
        .operations
        .iter()
        .find(|operation| operation.id == "preflight-primary")
        .expect("primary preflight should be recorded");
    let NamespaceOperationExecution::Attempted {
        response: Some(response),
        ..
    } = &preflight.execution
    else {
        panic!("oversized response metadata should be retained")
    };
    assert_eq!(response.body_bytes_observed, (1 << 20) + 1);
    assert!(matches!(
        response.body,
        SanitizedResponseBody::Omitted { ref reason }
            if reason.contains("maximum capture size")
    ));
    assert_eq!(server.finish().len(), 5);
}

#[tokio::test]
async fn oauth_credentials_tokens_and_page_tokens_are_never_persisted() {
    let client_id = "namespace-oauth-client-id-sentinel";
    let client_secret = "namespace-oauth-client-secret-sentinel";
    let bearer_token = "namespace-oauth-bearer-token-sentinel";
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
    assert_eq!(requests.len(), 23);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].target, "/api/catalog/v1/oauth/tokens");
    assert!(requests[1..].iter().all(|request| request
        .headers
        .get("authorization")
        .is_some_and(|value| value == "Bearer namespace-oauth-bearer-token-sentinel")));
    assert!(requests[5]
        .target
        .starts_with("/api/catalog/v1/bench/namespaces"));
}

#[tokio::test]
async fn declared_required_limitation_skips_transport() {
    let server = MockServer::start(Vec::new());
    let (mut profile, scenario) = contracts();
    let unsupported = "iceberg-rest.namespace.create";
    let exercise = profile
        .catalog_capabilities
        .iter()
        .map(|capability| capability.id.clone())
        .filter(|capability| capability.as_str() != unsupported)
        .collect();
    let adapter = adapter_mut(&mut profile, "lakecat");
    adapter.endpoint.base_url = format!("{}/catalog", server.url());
    adapter.capabilities = AdapterCapabilityCoverage::Explicit {
        exercise,
        unsupported: vec![UnsupportedAdapterCapability {
            capability: unsupported.into(),
            attributed_to: CapabilityLimitationSource::Catalog,
            explanation: "fixture catalog cannot create namespaces".to_owned(),
            upstream_reference: None,
        }],
    };

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        transcript.classification,
        ProbeClassification::Unsupported { ref capability, .. }
            if capability.as_str() == unsupported
    ));
    assert!(transcript.operations.is_empty());
    assert!(server.finish().is_empty());
}

#[tokio::test]
async fn invalid_fixture_and_scenario_drift_are_rejected_before_transport() {
    let (profile, scenario) = contracts();
    let fixture_error = run_namespace_probe(
        &profile,
        &scenario,
        &ComponentId::new("lakecat"),
        "../unsafe",
        digests(),
        |_| None,
    )
    .await
    .expect_err("unsafe fixture id must be rejected");
    assert!(fixture_error.to_string().contains("fixture id"));

    let mut drifted = scenario;
    drifted
        .parameters
        .insert("page_size".to_owned(), Value::from(2));
    let policy_error = run_namespace_probe(
        &profile,
        &drifted,
        &ComponentId::new("lakecat"),
        FIXTURE_ID,
        digests(),
        |_| None,
    )
    .await
    .expect_err("unimplemented scenario policy must be rejected");
    assert!(policy_error
        .to_string()
        .contains("scenario parameters drifted"));
}

#[tokio::test]
async fn assertion_and_timeout_drift_are_rejected() {
    let (profile, mut assertion_drift) = contracts();
    let assertion = assertion_drift
        .assertions
        .iter_mut()
        .find(|assertion| assertion.id.as_str() == "namespace-pagination-complete")
        .expect("pagination assertion exists");
    let AssertionCheck::Custom { configuration, .. } = &mut assertion.check else {
        panic!("pagination assertion should be custom")
    };
    configuration["page_size"] = json!(2);
    let error = run_namespace_probe(
        &profile,
        &assertion_drift,
        &ComponentId::new("lakecat"),
        FIXTURE_ID,
        digests(),
        |_| None,
    )
    .await
    .expect_err("assertion drift must be rejected");
    assert!(error
        .to_string()
        .contains("assertion policy drifted at `namespace-pagination-complete`"));

    let (_, mut timeout_drift) = contracts();
    timeout_drift
        .steps
        .iter_mut()
        .find(|step| step.id.as_str() == "traverse-pagination")
        .expect("pagination step exists")
        .timeout_ms = Some(1);
    let error = run_namespace_probe(
        &profile,
        &timeout_drift,
        &ComponentId::new("lakecat"),
        FIXTURE_ID,
        digests(),
        |_| None,
    )
    .await
    .expect_err("timeout drift must be rejected");
    assert!(error
        .to_string()
        .contains("step policy drifted at `traverse-pagination`"));
}

async fn probe<F>(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &str,
    fixture_id: &str,
    getenv: F,
) -> NamespaceTranscript
where
    F: Fn(&str) -> Option<String>,
{
    run_namespace_probe(
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

fn assertion<'a>(
    transcript: &'a NamespaceTranscript,
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
    primary: String,
    sibling: String,
}

impl FixtureNames {
    fn new(catalog: &str, id: &str) -> Self {
        let stem = format!("cb_c104_{}_{}", catalog.replace('-', "_"), id);
        Self {
            primary: format!("{stem}_a"),
            sibling: format!("{stem}_b"),
        }
    }

    fn primary_parts(&self) -> Vec<String> {
        vec![self.primary.clone()]
    }

    fn sibling_parts(&self) -> Vec<String> {
        vec![self.sibling.clone()]
    }

    fn child_parts(&self) -> Vec<String> {
        vec![self.primary.clone(), "child".to_owned()]
    }
}

fn happy_responses(names: &FixtureNames) -> Vec<MockResponse> {
    vec![
        config_response(),
        error_response(404, "NoSuchNamespaceException"),
        error_response(404, "NoSuchNamespaceException"),
        error_response(404, "NoSuchNamespaceException"),
        namespace_response(
            names.primary_parts(),
            json!({"owner": "catalog-bench", "c1-04.remove": "before"}),
        ),
        namespace_response(names.sibling_parts(), json!({})),
        namespace_response(names.child_parts(), json!({})),
        list_response(vec![names.primary_parts(), names.sibling_parts()], None),
        namespace_response(
            names.primary_parts(),
            json!({"owner": "catalog-bench", "c1-04.remove": "before"}),
        ),
        MockResponse::json(json!({
            "updated": ["c1-04.state"],
            "removed": ["c1-04.remove"],
            "missing": []
        })),
        namespace_response(
            names.primary_parts(),
            json!({"owner": "catalog-bench", "c1-04.state": "after"}),
        ),
        error_response(409, "AlreadyExistsException"),
        list_response(vec![names.child_parts()], None),
        list_response(vec![names.primary_parts()], Some("page-two-secret-token")),
        list_response(vec![names.sibling_parts()], None),
        error_response(404, "NoSuchNamespaceException"),
        MockResponse::empty(204),
        MockResponse::empty(204),
        MockResponse::empty(204),
        error_response(404, "NoSuchNamespaceException"),
        error_response(404, "NoSuchNamespaceException"),
        error_response(404, "NoSuchNamespaceException"),
    ]
}

fn config_response() -> MockResponse {
    MockResponse::json(json!({
        "defaults": {"namespace-separator": "%1F"},
        "overrides": {}
    }))
}

fn namespace_response(namespace: Vec<String>, properties: Value) -> MockResponse {
    MockResponse::json(json!({
        "namespace": namespace,
        "properties": properties
    }))
}

fn list_response(namespaces: Vec<Vec<String>>, next_page_token: Option<&str>) -> MockResponse {
    let mut response = json!({"namespaces": namespaces});
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
