use catalog_bench_common::contract::{
    parse_contract, AdapterCapabilityCoverage, AssertionOutcome, CapabilityId,
    CapabilityLimitationSource, ComponentId, ContractDocument, Profile, Scenario,
    UnsupportedAdapterCapability,
};
use catalog_bench_conformance::{
    encode_evidence, run_commit_probe, CommitOperationExecution, CommitTranscript, ContractDigests,
    IdempotencyAdvertisement, ProbeClassification,
};
use serde_json::{json, Value};

mod support;

use support::{MockResponse, MockServer, RecordedRequest};

const PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-26.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/iceberg-rest.commit.correctness.json");
const FIXTURE_ID: &str = "test";

#[tokio::test]
async fn advertised_probe_proves_required_and_idempotent_commit_safety() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let handler_names = names.clone();
    let mut index = 0;
    let mut observed_key = None;
    let server = MockServer::start_handler(22, move |request| {
        let response = advertised_response(index, &handler_names, request, &mut observed_key);
        index += 1;
        response
    });
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(transcript.passed());
    assert!(transcript
        .assertions
        .iter()
        .all(|assertion| matches!(assertion.outcome, AssertionOutcome::Pass)));
    assert_eq!(
        transcript.idempotency,
        IdempotencyAdvertisement::Advertised {
            source: "/overrides/idempotency-key-lifetime".to_owned(),
            lifetime: "PT30M".to_owned(),
        }
    );
    assert_eq!(transcript.operations.len(), 21);
    assert!(!transcript.sanitization.raw_secrets_persisted);

    let requests = server.finish();
    assert_eq!(requests.len(), 22);
    assert_commit_request_contract(&requests, &names);
    let key = requests[12]
        .headers
        .get("idempotency-key")
        .expect("first retry commit should carry an idempotency key");
    let parsed = uuid::Uuid::parse_str(key).expect("idempotency key should be a UUID");
    assert_eq!(parsed.as_bytes()[6] >> 4, 7);
    assert_eq!(requests[14].headers.get("idempotency-key"), Some(key));
    assert_eq!(requests[16].headers.get("idempotency-key"), Some(key));
    assert_eq!(requests[12].body, requests[14].body);
    assert_ne!(requests[14].body, requests[16].body);

    let evidence = evidence_text(&transcript);
    assert!(!evidence.contains(key));
    assert!(evidence.contains("<redacted>"));
    assert!(transcript
        .sanitization
        .redactions
        .iter()
        .any(|path| path.contains("idempotency-key")));
    assert!(transcript
        .sanitization
        .redactions
        .iter()
        .any(|path| path.contains("response.body/echo")));
}

#[tokio::test]
async fn absent_advertisement_skips_every_optional_request_without_failing_required_behavior() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let server = MockServer::start(required_and_cleanup_responses(
        &names,
        config_response(None),
    ));
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(transcript.passed());
    assert_eq!(
        transcript.idempotency,
        IdempotencyAdvertisement::NotAdvertised
    );
    for assertion_id in [
        "idempotency-support-advertised",
        "exact-request-replayed-once",
        "idempotency-content-drift-rejected",
    ] {
        assert!(matches!(
            assertion(&transcript, assertion_id).outcome,
            AssertionOutcome::NotEvaluated { .. }
        ));
    }
    for operation_id in optional_operation_ids() {
        assert!(matches!(
            operation(&transcript, operation_id).execution,
            CommitOperationExecution::NotAttempted { .. }
        ));
    }
    let requests = server.finish();
    assert_eq!(requests.len(), 16);
    assert!(requests
        .iter()
        .all(|request| !request.headers.contains_key("idempotency-key")));
}

#[tokio::test]
async fn malformed_advertisement_is_visible_but_cannot_change_required_classification() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let server = MockServer::start(required_and_cleanup_responses(
        &names,
        malformed_config_response(),
    ));
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(transcript.passed());
    assert!(matches!(
        transcript.idempotency,
        IdempotencyAdvertisement::Malformed { ref source, .. }
            if source == "/overrides/idempotency-key-lifetime"
    ));
    assert!(matches!(
        assertion(&transcript, "idempotency-support-advertised").outcome,
        AssertionOutcome::Fail { ref explanation } if explanation.contains("nonempty string")
    ));
    assert!(transcript
        .assertions
        .iter()
        .filter(|assertion| assertion.required)
        .all(|assertion| matches!(assertion.outcome, AssertionOutcome::Pass)));
    assert_eq!(server.finish().len(), 16);
}

#[tokio::test]
async fn standard_top_level_and_defaults_advertisement_locations_are_both_resolved() {
    for (expected_source, config) in [
        (
            "/idempotency-key-lifetime",
            config_response_at(Some("PT5M"), None, Some("PT1M")),
        ),
        (
            "/defaults/idempotency-key-lifetime",
            config_response_at(None, None, Some("PT1M")),
        ),
    ] {
        let names = FixtureNames::new("lakecat", FIXTURE_ID);
        let server = MockServer::start(required_and_cleanup_responses(&names, config));
        let (mut profile, scenario) = contracts();
        adapter_mut(&mut profile, "lakecat").endpoint.base_url =
            format!("{}/catalog", server.url());
        declare_unsupported(
            &mut profile,
            "lakecat",
            &[
                "iceberg-rest.table.commit.exact-retry",
                "iceberg-rest.idempotency-key.content-binding",
            ],
        );

        let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

        assert!(transcript.passed());
        assert!(matches!(
            transcript.idempotency,
            IdempotencyAdvertisement::Advertised { ref source, .. }
                if source == expected_source
        ));
        assert_eq!(server.finish().len(), 16);
    }
}

#[tokio::test]
async fn stale_success_and_mutation_fail_atomicity_while_cleanup_still_runs() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = required_responses(&names, config_response(Some("PT30M")));
    responses[9] = stale_mutated_table_response(&names, metadata_location(3));
    responses[10] = stale_mutated_table_response(&names, metadata_location(3));
    responses[11] = stale_mutated_table_response(&names, metadata_location(3));
    responses.extend(cleanup_responses());
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        transcript.classification,
        ProbeClassification::Fail { .. }
    ));
    assert!(matches!(
        assertion(&transcript, "stale-requirement-rejected-atomically").outcome,
        AssertionOutcome::Fail { ref explanation } if explanation.contains("HTTP 200")
    ));
    assert!(matches!(
        assertion(&transcript, "required-final-state-exact").outcome,
        AssertionOutcome::Fail { .. }
    ));
    assert!(matches!(
        assertion(&transcript, "commit-fixture-clean").outcome,
        AssertionOutcome::Pass
    ));
    assert_eq!(server.finish().len(), 16);
}

#[tokio::test]
async fn stale_conflict_with_the_wrong_error_type_fails_without_short_circuiting_cleanup() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = required_and_cleanup_responses(&names, config_response(Some("PT30M")));
    responses[9] = error_response(409, "ValidationException");
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        assertion(&transcript, "stale-requirement-rejected-atomically").outcome,
        AssertionOutcome::Fail { ref explanation } if explanation.contains("ValidationException")
    ));
    assert!(matches!(
        assertion(&transcript, "required-final-state-exact").outcome,
        AssertionOutcome::Pass
    ));
    assert!(matches!(
        assertion(&transcript, "commit-fixture-clean").outcome,
        AssertionOutcome::Pass
    ));
    assert_eq!(server.finish().len(), 16);
}

#[tokio::test]
async fn stale_conflict_cannot_hide_a_pointer_or_property_change() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = required_and_cleanup_responses(&names, config_response(Some("PT30M")));
    responses[10] = stale_mutated_table_response(&names, metadata_location(3));
    responses[11] = stale_mutated_table_response(&names, metadata_location(3));
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        assertion(&transcript, "stale-requirement-rejected-atomically").outcome,
        AssertionOutcome::Fail { ref explanation } if explanation.contains("snapshot differs")
    ));
    assert!(matches!(
        assertion(&transcript, "commit-fixture-clean").outcome,
        AssertionOutcome::Pass
    ));
    assert_eq!(server.finish().len(), 16);
}

#[tokio::test]
async fn exact_retry_that_advances_twice_is_detected_and_drift_is_not_risked() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = required_responses(&names, config_response(Some("PT30M")));
    responses.extend([
        retry_table_response(&names, metadata_location(3), "accepted-once"),
        retry_table_response(&names, metadata_location(3), "accepted-once"),
        retry_table_response(&names, metadata_location(4), "accepted-once"),
        retry_table_response(&names, metadata_location(4), "accepted-once"),
    ]);
    responses.extend(cleanup_responses());
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(transcript.passed());
    assert!(matches!(
        assertion(&transcript, "exact-request-replayed-once").outcome,
        AssertionOutcome::Fail { ref explanation } if explanation.contains("snapshot differs")
    ));
    assert!(matches!(
        assertion(&transcript, "idempotency-content-drift-rejected").outcome,
        AssertionOutcome::NotEvaluated { .. }
    ));
    assert!(matches!(
        operation(&transcript, "commit-idempotency-drift").execution,
        CommitOperationExecution::NotAttempted { .. }
    ));
    assert_eq!(server.finish().len(), 20);
}

#[tokio::test]
async fn accepted_idempotency_content_drift_and_mutation_are_detected() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let mut responses = happy_responses(&names, config_response(Some("PT30M")));
    responses[16] = retry_table_response(&names, metadata_location(4), "drifted-must-not-apply");
    responses[17] = retry_table_response(&names, metadata_location(4), "drifted-must-not-apply");
    let server = MockServer::start(responses);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(transcript.passed());
    assert!(matches!(
        assertion(&transcript, "idempotency-content-drift-rejected").outcome,
        AssertionOutcome::Fail { ref explanation } if explanation.contains("HTTP 200")
    ));
    assert!(matches!(
        assertion(&transcript, "commit-fixture-clean").outcome,
        AssertionOutcome::Pass
    ));
    assert_eq!(server.finish().len(), 22);
}

#[tokio::test]
async fn oauth_credentials_bearer_token_and_uuidv7_key_never_persist() {
    let client_id = "commit-oauth-client-id-sentinel";
    let client_secret = "commit-oauth-client-secret-sentinel";
    let bearer_token = "commit-oauth-bearer-token-sentinel";
    let names = FixtureNames::new("polaris", FIXTURE_ID);
    let mut responses = happy_responses(&names, config_response(Some("PT30M")));
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
    let requests = server.finish();
    let key = requests[13]
        .headers
        .get("idempotency-key")
        .expect("idempotent request should carry a key");
    let evidence = evidence_text(&transcript);
    for secret in [client_id, client_secret, bearer_token, key] {
        assert!(!evidence.contains(secret));
    }
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].target, "/api/catalog/v1/oauth/tokens");
    assert!(requests[1..].iter().all(|request| request
        .headers
        .get("authorization")
        .is_some_and(|value| value == "Bearer commit-oauth-bearer-token-sentinel")));
}

#[tokio::test]
async fn preflight_collision_prevents_all_mutation_and_cleanup() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let server = MockServer::start(vec![
        config_response(Some("PT30M")),
        namespace_response(&names.namespace),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        transcript.classification,
        ProbeClassification::Fail { .. }
    ));
    assert!(matches!(
        assertion(&transcript, "fixture-isolated").outcome,
        AssertionOutcome::Fail { ref explanation } if explanation.contains("HTTP 200")
    ));
    assert!(matches!(
        assertion(&transcript, "commit-fixture-clean").outcome,
        AssertionOutcome::NotEvaluated { .. }
    ));
    assert!(transcript.operations[1..].iter().all(|operation| matches!(
        operation.execution,
        CommitOperationExecution::NotAttempted { .. }
    )));
    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.method == "GET"));
}

#[tokio::test]
async fn oversized_preflight_is_bounded_without_risking_cleanup() {
    let server = MockServer::start(vec![
        config_response(Some("PT30M")),
        MockResponse::oversized(2 * 1024 * 1024),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    let CommitOperationExecution::Attempted {
        response: Some(response),
        failure: Some(_),
        ..
    } = &operation(&transcript, "preflight-namespace").execution
    else {
        panic!("oversized response should become bounded failure evidence");
    };
    assert_eq!(response.body_bytes_observed, 2 * 1024 * 1024);
    assert!(matches!(
        operation(&transcript, "cleanup-drop-table").execution,
        CommitOperationExecution::NotAttempted { .. }
    ));
    assert_eq!(server.finish().len(), 2);
}

#[tokio::test]
async fn requested_create_location_is_sent_and_must_be_preserved() {
    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let wrong_location = "s3://warehouse/not-the-requested-location";
    let server = MockServer::start(vec![
        config_response(None),
        error_response(404, "NoSuchNamespaceException"),
        namespace_response(&names.namespace),
        initial_table_response(wrong_location),
        initial_table_response(wrong_location),
        MockResponse::empty(204),
        error_response(404, "NoSuchTableException"),
        MockResponse::empty(204),
        error_response(404, "NoSuchNamespaceException"),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;

    assert!(matches!(
        assertion(&transcript, "commit-fixture-ready").outcome,
        AssertionOutcome::Fail { ref explanation } if explanation.contains("does not preserve")
    ));
    assert!(matches!(
        assertion(&transcript, "commit-fixture-clean").outcome,
        AssertionOutcome::Pass
    ));
    let requests = server.finish();
    assert_eq!(request_json(&requests[3])["location"], names.table_location);
}

#[tokio::test]
async fn policy_drift_and_invalid_fixture_are_rejected_before_transport() {
    let (profile, scenario) = contracts();
    let invalid_fixture = run_commit_probe(
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

    let mut parameter_drift = scenario.clone();
    parameter_drift
        .parameters
        .insert("request_timeout_ms".to_owned(), json!(1));
    assert_policy_error(&profile, &parameter_drift, "parameters drifted").await;

    let mut timeout_drift = scenario.clone();
    timeout_drift.steps[1].timeout_ms = Some(1);
    assert_policy_error(&profile, &timeout_drift, "step policy drifted").await;

    let mut assertion_drift = scenario;
    assertion_drift.assertions[8].check =
        catalog_bench_common::contract::AssertionCheck::NoRequestErrors;
    assert_policy_error(&profile, &assertion_drift, "assertion policy drifted").await;
}

#[tokio::test]
async fn required_limitations_skip_transport_and_optional_limitations_skip_only_headers() {
    let empty_server = MockServer::start(Vec::new());
    let (mut unsupported_profile, scenario) = contracts();
    declare_unsupported(
        &mut unsupported_profile,
        "lakecat",
        &["iceberg-rest.table.create"],
    );
    let unsupported = probe(
        &unsupported_profile,
        &scenario,
        "lakecat",
        FIXTURE_ID,
        |_| None,
    )
    .await;
    assert!(matches!(
        unsupported.classification,
        ProbeClassification::Unsupported { ref capability, .. }
            if capability.as_str() == "iceberg-rest.table.create"
    ));
    assert!(unsupported.operations.is_empty());
    assert!(empty_server.finish().is_empty());

    let names = FixtureNames::new("lakecat", FIXTURE_ID);
    let server = MockServer::start(required_and_cleanup_responses(
        &names,
        config_response(Some("PT30M")),
    ));
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());
    declare_unsupported(
        &mut profile,
        "lakecat",
        &[
            "iceberg-rest.table.commit.exact-retry",
            "iceberg-rest.idempotency-key.content-binding",
        ],
    );
    let transcript = probe(&profile, &scenario, "lakecat", FIXTURE_ID, |_| None).await;
    assert!(transcript.passed());
    assert!(matches!(
        assertion(&transcript, "idempotency-support-advertised").outcome,
        AssertionOutcome::Pass
    ));
    assert!(matches!(
        assertion(&transcript, "exact-request-replayed-once").outcome,
        AssertionOutcome::NotEvaluated { ref reason } if reason.contains("profile declares")
    ));
    let requests = server.finish();
    assert!(requests
        .iter()
        .all(|request| !request.headers.contains_key("idempotency-key")));
}

async fn probe<F>(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &str,
    fixture_id: &str,
    getenv: F,
) -> CommitTranscript
where
    F: Fn(&str) -> Option<String>,
{
    run_commit_probe(
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

async fn assert_policy_error(profile: &Profile, scenario: &Scenario, expected: &str) {
    let error = run_commit_probe(
        profile,
        scenario,
        &ComponentId::new("lakecat"),
        FIXTURE_ID,
        digests(),
        |_| None,
    )
    .await
    .expect_err("policy drift should be rejected");
    assert!(error.to_string().contains(expected), "{error:#}");
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
            explanation: "fixture catalog does not implement this capability".to_owned(),
            upstream_reference: None,
        })
        .collect();
    adapter_mut(profile, catalog).capabilities = AdapterCapabilityCoverage::Explicit {
        exercise,
        unsupported,
    };
}

fn assertion<'a>(
    transcript: &'a CommitTranscript,
    id: &str,
) -> &'a catalog_bench_conformance::ProbeAssertion {
    transcript
        .assertions
        .iter()
        .find(|assertion| assertion.assertion.as_str() == id)
        .expect("assertion should exist")
}

fn operation<'a>(
    transcript: &'a CommitTranscript,
    id: &str,
) -> &'a catalog_bench_conformance::CommitOperationTranscript {
    transcript
        .operations
        .iter()
        .find(|operation| operation.id == id)
        .expect("operation should exist")
}

fn request_json(request: &RecordedRequest) -> Value {
    serde_json::from_str(&request.body).expect("request body should be JSON")
}

fn evidence_text(transcript: &CommitTranscript) -> String {
    String::from_utf8(encode_evidence(transcript).expect("evidence should encode"))
        .expect("evidence should be UTF-8")
}

fn digests() -> ContractDigests {
    ContractDigests {
        profile_sha256: "0".repeat(64),
        scenario_sha256: "1".repeat(64),
    }
}

#[derive(Clone)]
struct FixtureNames {
    namespace: String,
    table_location: String,
}

impl FixtureNames {
    fn new(catalog: &str, id: &str) -> Self {
        let namespace = format!("cb_c106_{}_{}", catalog.replace('-', "_"), id);
        let table_location = format!("s3://warehouse/{catalog}/{namespace}/commit_correctness");
        Self {
            namespace,
            table_location,
        }
    }
}

fn happy_responses(names: &FixtureNames, config: MockResponse) -> Vec<MockResponse> {
    let mut responses = required_responses(names, config);
    responses.extend([
        retry_table_response(names, metadata_location(3), "accepted-once"),
        retry_table_response(names, metadata_location(3), "accepted-once"),
        retry_table_response(names, metadata_location(3), "accepted-once"),
        retry_table_response(names, metadata_location(3), "accepted-once"),
        error_response(409, "IdempotencyKeyConflictException"),
        retry_table_response(names, metadata_location(3), "accepted-once"),
    ]);
    responses.extend(cleanup_responses());
    responses
}

fn required_and_cleanup_responses(names: &FixtureNames, config: MockResponse) -> Vec<MockResponse> {
    let mut responses = required_responses(names, config);
    responses.extend(cleanup_responses());
    responses
}

fn required_responses(names: &FixtureNames, config: MockResponse) -> Vec<MockResponse> {
    vec![
        config,
        error_response(404, "NoSuchNamespaceException"),
        namespace_response(&names.namespace),
        initial_table_response(&names.table_location),
        initial_table_response(&names.table_location),
        current_table_response(names, metadata_location(1)),
        current_table_response(names, metadata_location(1)),
        schema_table_response(names, metadata_location(2)),
        schema_table_response(names, metadata_location(2)),
        error_response(409, "CommitFailedException"),
        schema_table_response(names, metadata_location(2)),
        schema_table_response(names, metadata_location(2)),
    ]
}

fn cleanup_responses() -> Vec<MockResponse> {
    vec![
        MockResponse::empty(204),
        error_response(404, "NoSuchTableException"),
        MockResponse::empty(204),
        error_response(404, "NoSuchNamespaceException"),
    ]
}

fn advertised_response(
    index: usize,
    names: &FixtureNames,
    request: &RecordedRequest,
    observed_key: &mut Option<String>,
) -> MockResponse {
    match index {
        0 => config_response(Some("PT30M")),
        1 => error_response(404, "NoSuchNamespaceException"),
        2 => namespace_response(&names.namespace),
        3 | 4 => initial_table_response(&names.table_location),
        5 | 6 => current_table_response(names, metadata_location(1)),
        7 | 8 => schema_table_response(names, metadata_location(2)),
        9 => error_response(409, "CommitFailedException"),
        10 | 11 => schema_table_response(names, metadata_location(2)),
        12 => {
            let key = request
                .headers
                .get("idempotency-key")
                .expect("idempotent commit should carry a key")
                .clone();
            *observed_key = Some(key.clone());
            retry_table_response_with_echo(names, metadata_location(3), "accepted-once", &key)
        }
        13 => retry_table_response(names, metadata_location(3), "accepted-once"),
        14 => retry_table_response_with_echo(
            names,
            metadata_location(3),
            "accepted-once",
            observed_key
                .as_deref()
                .expect("first key should be observed"),
        ),
        15 => retry_table_response(names, metadata_location(3), "accepted-once"),
        16 => MockResponse::status_json(
            409,
            json!({
                "error": {
                    "message": format!(
                        "key {} was reused with different content",
                        observed_key.as_deref().expect("first key should be observed")
                    ),
                    "type": "IdempotencyKeyConflictException",
                    "code": 409
                }
            }),
        ),
        17 => retry_table_response(names, metadata_location(3), "accepted-once"),
        18 | 20 => MockResponse::empty(204),
        19 => error_response(404, "NoSuchTableException"),
        21 => error_response(404, "NoSuchNamespaceException"),
        _ => panic!("unexpected request index {index}"),
    }
}

fn assert_commit_request_contract(requests: &[RecordedRequest], names: &FixtureNames) {
    assert_eq!(requests[0].target, "/catalog/v1/config");
    assert_eq!(requests[2].method, "POST");
    assert_eq!(
        request_json(&requests[2])["namespace"],
        json!([names.namespace])
    );
    assert_eq!(request_json(&requests[3])["name"], "commit_correctness");
    assert_eq!(request_json(&requests[3])["location"], names.table_location);
    assert_eq!(
        request_json(&requests[5])["requirements"],
        json!([
            {"type": "assert-table-uuid", "uuid": "commit-table-uuid"},
            {"type": "assert-current-schema-id", "current-schema-id": 0}
        ])
    );
    assert_eq!(
        request_json(&requests[7])["requirements"][2],
        json!({
            "type": "assert-last-assigned-field-id",
            "last-assigned-field-id": 1
        })
    );
    assert_eq!(
        request_json(&requests[9])["requirements"][1]["current-schema-id"],
        0
    );
    assert_eq!(requests[18].method, "DELETE");
    assert!(requests[18].target.ends_with("purgeRequested=false"));
}

fn config_response(idempotency_lifetime: Option<&str>) -> MockResponse {
    config_response_at(None, idempotency_lifetime, None)
}

fn config_response_at(
    top_level: Option<&str>,
    override_value: Option<&str>,
    default_value: Option<&str>,
) -> MockResponse {
    let mut response = json!({
        "defaults": {"namespace-separator": "%1F"},
        "overrides": {}
    });
    if let Some(lifetime) = top_level {
        response["idempotency-key-lifetime"] = Value::from(lifetime);
    }
    if let Some(lifetime) = override_value {
        response["overrides"]["idempotency-key-lifetime"] = Value::from(lifetime);
    }
    if let Some(lifetime) = default_value {
        response["defaults"]["idempotency-key-lifetime"] = Value::from(lifetime);
    }
    MockResponse::json(response)
}

fn malformed_config_response() -> MockResponse {
    MockResponse::json(json!({
        "defaults": {"namespace-separator": "%1F"},
        "overrides": {"idempotency-key-lifetime": 30}
    }))
}

fn namespace_response(namespace: &str) -> MockResponse {
    MockResponse::json(json!({
        "namespace": [namespace],
        "properties": {"catalog-bench.owner": "catalog-bench"}
    }))
}

fn initial_table_response(table_location: &str) -> MockResponse {
    table_response(
        metadata_location(0),
        table_location,
        0,
        1,
        json!({
            "catalog-bench.owner": "catalog-bench",
            "c1-06.state": "initial",
            "catalog.internal.revision": "0"
        }),
        None,
    )
}

fn current_table_response(names: &FixtureNames, metadata_location: &str) -> MockResponse {
    table_response(
        metadata_location,
        &names.table_location,
        0,
        1,
        json!({
            "catalog-bench.owner": "catalog-bench",
            "c1-06.state": "requirement-accepted",
            "catalog.internal.revision": "1"
        }),
        None,
    )
}

fn schema_table_response(names: &FixtureNames, metadata_location: &str) -> MockResponse {
    table_response(
        metadata_location,
        &names.table_location,
        1,
        2,
        json!({
            "catalog-bench.owner": "catalog-bench",
            "c1-06.state": "requirement-accepted",
            "catalog.internal.revision": "2"
        }),
        None,
    )
}

fn stale_mutated_table_response(names: &FixtureNames, metadata_location: &str) -> MockResponse {
    table_response(
        metadata_location,
        &names.table_location,
        1,
        2,
        json!({
            "catalog-bench.owner": "catalog-bench",
            "c1-06.state": "requirement-accepted",
            "c1-06.stale": "must-not-apply",
            "catalog.internal.revision": "3"
        }),
        None,
    )
}

fn retry_table_response(
    names: &FixtureNames,
    metadata_location: &str,
    retry_value: &str,
) -> MockResponse {
    retry_table_response_with_extra(names, metadata_location, retry_value, None)
}

fn retry_table_response_with_echo(
    names: &FixtureNames,
    metadata_location: &str,
    retry_value: &str,
    echo: &str,
) -> MockResponse {
    retry_table_response_with_extra(names, metadata_location, retry_value, Some(echo))
}

fn retry_table_response_with_extra(
    names: &FixtureNames,
    metadata_location: &str,
    retry_value: &str,
    echo: Option<&str>,
) -> MockResponse {
    let revision = metadata_location
        .rsplit_once("0000")
        .and_then(|(_, suffix)| suffix.strip_suffix(".json"))
        .unwrap_or("unknown");
    let properties = json!({
        "catalog-bench.owner": "catalog-bench",
        "c1-06.state": "requirement-accepted",
        "c1-06.retry": retry_value,
        "catalog.internal.revision": revision
    });
    table_response(
        metadata_location,
        &names.table_location,
        1,
        2,
        properties,
        echo,
    )
}

fn table_response(
    metadata_location: &str,
    table_location: &str,
    schema_id: i32,
    last_column_id: i32,
    properties: Value,
    echo: Option<&str>,
) -> MockResponse {
    let fields = if schema_id == 0 {
        json!([
            {"id": 1, "name": "value", "required": false, "type": "long"}
        ])
    } else {
        json!([
            {"id": 1, "name": "value", "required": false, "type": "long"},
            {"id": 2, "name": "note", "required": false, "type": "string"}
        ])
    };
    let mut response = json!({
        "metadata-location": metadata_location,
        "metadata": {
            "format-version": 2,
            "table-uuid": "commit-table-uuid",
            "location": table_location,
            "last-column-id": last_column_id,
            "current-schema-id": schema_id,
            "schemas": [{
                "type": "struct",
                "schema-id": schema_id,
                "fields": fields
            }],
            "properties": properties
        },
        "config": {}
    });
    if let Some(echo) = echo {
        response["echo"] = Value::from(echo);
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

fn metadata_location(sequence: u8) -> &'static str {
    match sequence {
        0 => "s3://warehouse/commit/metadata/00000.json",
        1 => "s3://warehouse/commit/metadata/00001.json",
        2 => "s3://warehouse/commit/metadata/00002.json",
        3 => "s3://warehouse/commit/metadata/00003.json",
        4 => "s3://warehouse/commit/metadata/00004.json",
        _ => panic!("unsupported metadata sequence"),
    }
}

fn optional_operation_ids() -> [&'static str; 6] {
    [
        "commit-idempotent-first",
        "reload-after-idempotent-first",
        "commit-idempotent-replay",
        "reload-after-idempotent-replay",
        "commit-idempotency-drift",
        "reload-after-idempotency-drift",
    ]
}
