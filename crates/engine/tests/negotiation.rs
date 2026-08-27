use catalog_bench_conformance::{CatalogNegotiationEvidence, ProbeFailureStage};
use catalog_bench_engine::{
    EngineAuthenticationMode, EngineCatalogNegotiationEvidence, EngineRoutingResolution,
};
use serde_json::json;

const PRIVATE: &str = "negotiation-private-sentinel";

#[test]
fn projection_discards_dynamic_config_and_failure_values() {
    let evidence: CatalogNegotiationEvidence = serde_json::from_value(json!({
        "adapter": {
            "catalog": "polaris",
            "name": "Apache Polaris",
            "version": "1.7.0",
            "protocol": "iceberg-rest-v1",
            "request_handling": {"kind": "protocol-native"}
        },
        "authentication": {
            "mode": "oauth2-client-credentials",
            "outcome": "failed",
            "token_url": format!("http://polaris/{PRIVATE}"),
            "scope": PRIVATE,
            "http_status": 401
        },
        "config": {
            "request": {
                "method": "GET",
                "url": format!("http://polaris/{PRIVATE}/v1/config"),
                "headers": {"x-private": PRIVATE}
            },
            "response": {
                "status": 409,
                "headers": {"x-private": PRIVATE},
                "body_bytes_observed": 123,
                "body": {
                    "kind": "json",
                    "value": {(PRIVATE): PRIVATE}
                }
            },
            "prefix": {
                "mode": "negotiated",
                "json_pointer": PRIVATE,
                "value": PRIVATE
            },
            "namespace_separator": {
                "mode": "configured",
                "json_pointer": PRIVATE,
                "encoded": PRIVATE
            },
            "failure": {"stage": "response", "explanation": PRIVATE}
        },
        "redactions": [PRIVATE]
    }))
    .unwrap();

    let projected = EngineCatalogNegotiationEvidence::try_from(evidence).unwrap();
    assert_eq!(
        projected.authentication.mode,
        EngineAuthenticationMode::OAuth2ClientCredentials
    );
    assert_eq!(projected.authentication.http_status, Some(401));
    assert_eq!(projected.config.http_status, Some(409));
    assert_eq!(projected.config.response_bytes, Some(123));
    assert_eq!(projected.config.prefix, EngineRoutingResolution::Negotiated);
    assert_eq!(
        projected.config.namespace_separator,
        EngineRoutingResolution::Configured
    );
    assert_eq!(
        projected.config.failure_stage,
        Some(ProbeFailureStage::Response)
    );
    assert_eq!(projected.redactions_observed, 1);
    assert!(!serde_json::to_string(&projected).unwrap().contains(PRIVATE));
}

#[test]
fn projection_rejects_authentication_vocabulary_drift() {
    let evidence: CatalogNegotiationEvidence = serde_json::from_value(json!({
        "adapter": {
            "catalog": "lakecat",
            "name": "LakeCat",
            "version": "test",
            "protocol": "iceberg-rest-v1",
            "request_handling": {"kind": "protocol-native"}
        },
        "authentication": {"mode": PRIVATE, "outcome": "not-attempted"},
        "config": {
            "request": {
                "method": "GET",
                "url": "http://lakecat/catalog/v1/config",
                "headers": {"accept": "application/json"}
            },
            "prefix": {"mode": "not-evaluated", "reason": PRIVATE},
            "namespace_separator": {"mode": "not-evaluated", "reason": PRIVATE}
        },
        "redactions": []
    }))
    .unwrap();

    assert!(EngineCatalogNegotiationEvidence::try_from(evidence).is_err());
}
