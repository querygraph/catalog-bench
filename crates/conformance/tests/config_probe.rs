use catalog_bench_common::contract::{
    parse_contract, AdapterCapabilityCoverage, AssertionCheck, AssertionOutcome,
    CapabilityLimitationSource, ComponentId, ContractDocument, Profile, Scenario,
    UnsupportedAdapterCapability,
};
use catalog_bench_conformance::{
    encode_evidence, run_config_probe, AuthenticationOutcome, ContractDigests,
    EndpointAdvertisement, PrefixResolution, ProbeClassification, SanitizedResponseBody,
};
use serde_json::json;

mod support;

use support::{MockResponse, MockServer};

const PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-26.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/iceberg-rest.config.negotiation.json");

#[tokio::test]
async fn anonymous_probe_negotiates_prefix_and_redacts_response_secrets() {
    let secret = "anonymous-response-secret-sentinel";
    let server = MockServer::start(vec![MockResponse::json(json!({
        "defaults": {
            "prefix": "warehouse-id",
            "client_id": "abc",
            "s3.secret-access-key": secret,
            "remote-uri": format!("https://example.test/catalog?token={secret}"),
            "fragment-uri": "https://example.test/catalog#private-fragment"
        },
        "overrides": {},
        "endpoints": [
            "GET /v1/{prefix}/namespaces",
            "POST /v1/{prefix}/namespaces"
        ]
    }))]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakekeeper").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new("lakekeeper"),
        digests(),
        |_| None,
    )
    .await
    .expect("anonymous probe should produce evidence");

    assert!(transcript.passed());
    assert_eq!(
        transcript.prefix,
        PrefixResolution::Negotiated {
            json_pointer: "/defaults/prefix".to_owned(),
            value: "warehouse-id".to_owned(),
        }
    );
    assert!(matches!(
        transcript.endpoints,
        EndpointAdvertisement::Explicit { .. }
    ));
    assert_eq!(
        transcript.authentication.outcome,
        AuthenticationOutcome::Ready
    );
    assert!(!transcript.sanitization.raw_secrets_persisted);
    assert!(!transcript.sanitization.raw_response_body_persisted);
    assert_eq!(
        transcript.sanitization.redactions,
        vec![
            "response.body/defaults/client_id".to_owned(),
            "response.body/defaults/fragment-uri".to_owned(),
            "response.body/defaults/remote-uri".to_owned(),
            "response.body/defaults/s3.secret-access-key".to_owned(),
        ]
    );

    let response = transcript
        .response
        .as_ref()
        .expect("config response should be captured");
    let SanitizedResponseBody::Json { value } = &response.body else {
        panic!("config response should contain sanitized JSON")
    };
    assert_eq!(
        value.pointer("/defaults/client_id"),
        Some(&json!("<redacted>"))
    );
    assert_eq!(
        value.pointer("/defaults/s3.secret-access-key"),
        Some(&json!("<redacted>"))
    );
    assert_eq!(
        value.pointer("/defaults/remote-uri"),
        Some(&json!("https://example.test/catalog?token=%3Credacted%3E"))
    );
    assert_eq!(
        value.pointer("/defaults/fragment-uri"),
        Some(&json!("https://example.test/catalog"))
    );
    assert_eq!(response.raw_body_sha256, None);
    let evidence = encode_evidence(&transcript).expect("transcript should serialize");
    assert!(!String::from_utf8_lossy(&evidence).contains(secret));

    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].target, "/catalog/v1/config?warehouse=bench");
    assert_eq!(
        requests[0].headers.get("accept").map(String::as_str),
        Some("application/json")
    );
}

#[tokio::test]
async fn oauth_probe_uses_bearer_token_without_persisting_credentials() {
    let client_id = "polaris-client-id-sentinel";
    let client_secret = "polaris-client-secret-sentinel";
    let bearer_token = "polaris-bearer-token-sentinel";
    let server = MockServer::start(vec![
        MockResponse::json(json!({
            "access_token": bearer_token,
            "token_type": "Bearer"
        })),
        MockResponse::json(json!({
            "defaults": {},
            "overrides": {}
        })),
    ]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "polaris").endpoint.base_url =
        format!("{}/api/catalog", server.url());

    let transcript = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new("polaris"),
        digests(),
        |name| match name {
            "CATALOG_BENCH_POLARIS_CLIENT_ID" => Some(client_id.to_owned()),
            "CATALOG_BENCH_POLARIS_CLIENT_SECRET" => Some(client_secret.to_owned()),
            _ => None,
        },
    )
    .await
    .expect("OAuth probe should produce evidence");

    assert!(transcript.passed());
    assert_eq!(
        transcript.authentication.outcome,
        AuthenticationOutcome::Ready
    );
    assert_eq!(transcript.authentication.http_status, Some(200));
    assert_eq!(
        transcript.request.headers.get("authorization"),
        Some(&"<redacted>".to_owned())
    );
    assert!(matches!(
        transcript.prefix,
        PrefixResolution::Static { ref value } if value == "bench"
    ));
    assert!(matches!(
        transcript.endpoints,
        EndpointAdvertisement::Omitted
    ));
    assert!(transcript
        .response
        .as_ref()
        .is_some_and(|response| response.raw_body_sha256.is_some()));

    let evidence = encode_evidence(&transcript).expect("transcript should serialize");
    let evidence = String::from_utf8(evidence).expect("evidence is UTF-8 JSON");
    for sensitive in [client_id, client_secret, bearer_token] {
        assert!(!evidence.contains(sensitive));
    }

    let requests = server.finish();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].target, "/api/catalog/v1/oauth/tokens");
    assert!(requests[0].body.contains("grant_type=client_credentials"));
    assert!(requests[0].body.contains(client_id));
    assert!(requests[0].body.contains(client_secret));
    assert!(requests[0].body.contains("scope=PRINCIPAL_ROLE%3AALL"));
    assert_eq!(requests[1].method, "GET");
    assert_eq!(requests[1].target, "/api/catalog/v1/config?warehouse=bench");
    assert_eq!(
        requests[1].headers.get("authorization").map(String::as_str),
        Some("Bearer polaris-bearer-token-sentinel")
    );
}

#[tokio::test]
async fn nonstandard_advertised_endpoint_fails_required_assertion() {
    let server = MockServer::start(vec![MockResponse::json(json!({
        "defaults": {},
        "overrides": {},
        "endpoints": ["GET /catalog/v1/namespaces"]
    }))]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new("lakecat"),
        digests(),
        |_| None,
    )
    .await
    .expect("failed assertions should still produce evidence");

    assert!(matches!(
        transcript.classification,
        ProbeClassification::Fail { .. }
    ));
    assert!(matches!(
        transcript.endpoints,
        EndpointAdvertisement::Invalid { ref explanation }
            if explanation.contains("not an Apache Iceberg 1.11.0 REST endpoint")
    ));
    let endpoint_assertion = transcript
        .assertions
        .iter()
        .find(|assertion| assertion.assertion.as_str() == "endpoint-advertisement-valid")
        .expect("endpoint assertion should be evaluated");
    assert!(matches!(
        endpoint_assertion.outcome,
        AssertionOutcome::Fail { .. }
    ));

    let requests = server.finish();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].target, "/catalog/v1/config");
}

#[tokio::test]
async fn non_string_config_property_fails_map_shape_assertion() {
    let server = MockServer::start(vec![MockResponse::json(json!({
        "defaults": {"clients": 4},
        "overrides": {}
    }))]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new("lakecat"),
        digests(),
        |_| None,
    )
    .await
    .expect("shape failure should still produce evidence");

    assert!(matches!(
        transcript.classification,
        ProbeClassification::Fail { .. }
    ));
    let shape_assertion = transcript
        .assertions
        .iter()
        .find(|assertion| assertion.assertion.as_str() == "config-map-shape")
        .expect("shape assertion should be evaluated");
    assert!(matches!(
        shape_assertion.outcome,
        AssertionOutcome::Fail { ref explanation }
            if explanation.contains("defaults.clients")
    ));
    assert_eq!(server.finish().len(), 1);
}

#[tokio::test]
async fn missing_required_config_map_fails_map_shape_assertion() {
    let server = MockServer::start(vec![MockResponse::json(json!({
        "defaults": {}
    }))]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new("lakecat"),
        digests(),
        |_| None,
    )
    .await
    .expect("missing required map should produce failed evidence");

    assert!(matches!(
        transcript.classification,
        ProbeClassification::Fail { .. }
    ));
    assert!(transcript.assertions.iter().any(|assertion| {
        assertion.assertion.as_str() == "config-map-shape"
            && matches!(
                &assertion.outcome,
                AssertionOutcome::Fail { explanation }
                    if explanation.contains("missing required `overrides`")
            )
    }));
    assert_eq!(server.finish().len(), 1);
}

#[tokio::test]
async fn non_json_media_type_fails_its_required_assertion() {
    let server = MockServer::start(vec![MockResponse::json(json!({
        "defaults": {},
        "overrides": {}
    }))
    .with_content_type("text/plain")]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new("lakecat"),
        digests(),
        |_| None,
    )
    .await
    .expect("wrong media type should produce failed evidence");

    assert!(matches!(
        transcript.classification,
        ProbeClassification::Fail { .. }
    ));
    assert!(transcript.assertions.iter().any(|assertion| {
        assertion.assertion.as_str() == "config-media-type"
            && matches!(
                &assertion.outcome,
                AssertionOutcome::Fail { explanation }
                    if explanation.contains("text/plain")
            )
    }));
    assert_eq!(server.finish().len(), 1);
}

#[tokio::test]
async fn empty_explicit_endpoint_array_is_valid_openapi_shape() {
    let server = MockServer::start(vec![MockResponse::json(json!({
        "defaults": {},
        "overrides": {},
        "endpoints": []
    }))]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new("lakecat"),
        digests(),
        |_| None,
    )
    .await
    .expect("empty endpoint array should produce evidence");

    assert!(transcript.passed());
    assert!(matches!(
        transcript.endpoints,
        EndpointAdvertisement::Explicit { ref endpoints } if endpoints.is_empty()
    ));
    assert_eq!(server.finish().len(), 1);
}

#[tokio::test]
async fn empty_oauth_environment_value_fails_before_transport() {
    let server = MockServer::start(Vec::new());
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "polaris").endpoint.base_url =
        format!("{}/api/catalog", server.url());

    let transcript = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new("polaris"),
        digests(),
        |name| match name {
            "CATALOG_BENCH_POLARIS_CLIENT_ID" => Some(String::new()),
            "CATALOG_BENCH_POLARIS_CLIENT_SECRET" => Some("secret".to_owned()),
            _ => None,
        },
    )
    .await
    .expect("runtime authentication failure should produce evidence");

    assert!(matches!(
        transcript.classification,
        ProbeClassification::Fail { .. }
    ));
    assert_eq!(
        transcript.authentication.outcome,
        AuthenticationOutcome::Failed
    );
    assert!(transcript.response.is_none());
    assert!(transcript
        .failure
        .as_ref()
        .is_some_and(|failure| failure.explanation.contains("not set or is empty")));
    assert!(server.finish().is_empty());
}

#[tokio::test]
async fn oversized_config_response_is_bounded_and_recorded_as_failure() {
    let server = MockServer::start(vec![MockResponse::oversized((1 << 20) + 1)]);
    let (mut profile, scenario) = contracts();
    adapter_mut(&mut profile, "lakecat").endpoint.base_url = format!("{}/catalog", server.url());

    let transcript = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new("lakecat"),
        digests(),
        |_| None,
    )
    .await
    .expect("oversized response should produce bounded failed evidence");

    assert!(matches!(
        transcript.classification,
        ProbeClassification::Fail { .. }
    ));
    let response = transcript
        .response
        .expect("response metadata should be retained");
    assert_eq!(response.body_bytes_observed, (1 << 20) + 1);
    assert_eq!(response.raw_body_sha256, None);
    assert!(matches!(
        response.body,
        SanitizedResponseBody::Omitted { ref reason } if reason.contains("maximum capture size")
    ));
    assert_eq!(server.finish().len(), 1);
}

#[tokio::test]
async fn declared_unsupported_capability_skips_transport() {
    let server = MockServer::start(Vec::new());
    let (mut profile, scenario) = contracts();
    let unsupported = "iceberg-rest.config.read";
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
            explanation: "fixture catalog does not expose config".to_owned(),
            upstream_reference: None,
        }],
    };

    let transcript = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new("lakecat"),
        digests(),
        |_| None,
    )
    .await
    .expect("declared limitation should produce unsupported evidence");

    assert!(matches!(
        transcript.classification,
        ProbeClassification::Unsupported { ref capability, .. }
            if capability.as_str() == unsupported
    ));
    assert!(transcript.response.is_none());
    assert!(server.finish().is_empty());
}

#[tokio::test]
async fn scenario_policy_drift_is_rejected_before_transport() {
    let (profile, mut scenario) = contracts();
    scenario.parameters.insert(
        "maximum_response_bytes".to_owned(),
        serde_json::Value::from(1024),
    );

    let error = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new("lakecat"),
        digests(),
        |_| None,
    )
    .await
    .expect_err("runner must reject a policy it does not implement");

    assert!(error.to_string().contains("scenario parameters drifted"));
}

#[tokio::test]
async fn unknown_scenario_parameter_is_rejected_before_transport() {
    let (profile, mut scenario) = contracts();
    scenario
        .parameters
        .insert("ignored_policy".to_owned(), json!(true));

    let error = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new("lakecat"),
        digests(),
        |_| None,
    )
    .await
    .expect_err("runner must reject unimplemented scenario parameters");

    assert!(error.to_string().contains("scenario parameters drifted"));
}

#[tokio::test]
async fn assertion_configuration_drift_is_rejected_before_transport() {
    let (profile, mut scenario) = contracts();
    let endpoint_assertion = scenario
        .assertions
        .iter_mut()
        .find(|assertion| assertion.id.as_str() == "endpoint-advertisement-valid")
        .expect("checked-in scenario should have endpoint assertion");
    let AssertionCheck::Custom { configuration, .. } = &mut endpoint_assertion.check else {
        panic!("endpoint assertion should use a custom check")
    };
    configuration["path_prefix"] = json!("/catalog/v1/");

    let error = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new("lakecat"),
        digests(),
        |_| None,
    )
    .await
    .expect_err("runner must reject an assertion policy it does not implement");

    assert!(error
        .to_string()
        .contains("assertion policy drifted at `endpoint-advertisement-valid`"));
}

#[tokio::test]
async fn request_timeout_drift_is_rejected_before_transport() {
    let (profile, mut scenario) = contracts();
    scenario
        .steps
        .iter_mut()
        .find(|step| step.id.as_str() == "request-config")
        .expect("checked-in scenario should have request step")
        .timeout_ms = Some(1);

    let error = run_config_probe(
        &profile,
        &scenario,
        &ComponentId::new("lakecat"),
        digests(),
        |_| None,
    )
    .await
    .expect_err("runner must reject a timeout it does not implement");

    assert!(error
        .to_string()
        .contains("step policy drifted at `request-config`"));
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

fn digests() -> ContractDigests {
    ContractDigests {
        profile_sha256: "0".repeat(64),
        scenario_sha256: "1".repeat(64),
    }
}
