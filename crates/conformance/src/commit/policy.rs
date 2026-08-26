use std::collections::BTreeMap;

use anyhow::{bail, Result};
use catalog_bench_common::contract::{ActorRole, ComponentId, Profile, RequirementLevel, Scenario};
use serde_json::{json, Value};

use super::{
    fixture::FIXTURE_PREFIX, CommitFacts, COMMIT_SCENARIO_ID, COMMIT_SCENARIO_VERSION,
    COMMIT_TRANSCRIPT_FORMAT, IDEMPOTENCY_POINTERS,
};
use crate::evidence::ProbeAssertion;
use crate::iceberg::DEFAULT_NAMESPACE_SEPARATOR;
use crate::spec::{ICEBERG_REST_OPENAPI_SHA256, ICEBERG_REST_OPENAPI_SOURCE};
use crate::target::ProbeTarget;
use crate::transport::MAXIMUM_RESPONSE_BYTES;

pub(super) fn validate_invocation(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &ComponentId,
) -> Result<()> {
    if scenario.id.as_str() != COMMIT_SCENARIO_ID {
        bail!(
            "commit probe requires scenario `{COMMIT_SCENARIO_ID}`, found `{}`",
            scenario.id
        );
    }
    if scenario.version != COMMIT_SCENARIO_VERSION {
        bail!(
            "commit probe supports scenario version {COMMIT_SCENARIO_VERSION}, found {}",
            scenario.version
        );
    }
    validate_scenario_policy(scenario)?;
    ProbeTarget::resolve(profile, scenario, catalog)?;
    Ok(())
}

fn validate_scenario_policy(scenario: &Scenario) -> Result<()> {
    validate_parameters(scenario)?;
    validate_capabilities(scenario)?;
    validate_steps(scenario)?;
    validate_assertions(scenario)?;
    if !scenario.extensions.is_empty() {
        bail!("commit probe scenario v{COMMIT_SCENARIO_VERSION} does not accept extensions");
    }
    Ok(())
}

fn validate_parameters(scenario: &Scenario) -> Result<()> {
    let expected = BTreeMap::from([
        (
            "accepted_set_properties".to_owned(),
            json!({"c1-06.state": "requirement-accepted"}),
        ),
        (
            "commit_conflict".to_owned(),
            json!({"code": 409, "type": "CommitFailedException"}),
        ),
        (
            "drifted_set_properties".to_owned(),
            json!({"c1-06.retry": "drifted-must-not-apply"}),
        ),
        (
            "exact_retry_set_properties".to_owned(),
            json!({"c1-06.retry": "accepted-once"}),
        ),
        ("fixture_prefix".to_owned(), Value::from(FIXTURE_PREFIX)),
        (
            "iceberg_openapi_sha256".to_owned(),
            Value::from(ICEBERG_REST_OPENAPI_SHA256),
        ),
        (
            "iceberg_openapi_source".to_owned(),
            Value::from(ICEBERG_REST_OPENAPI_SOURCE),
        ),
        (
            "idempotency_advertisement_pointers".to_owned(),
            json!(IDEMPOTENCY_POINTERS),
        ),
        (
            "idempotency_header".to_owned(),
            Value::from("Idempotency-Key"),
        ),
        ("idempotency_key_format".to_owned(), Value::from("uuid-v7")),
        (
            "initial_properties".to_owned(),
            json!({
                "c1-06.state": "initial",
                "catalog-bench.owner": "catalog-bench"
            }),
        ),
        (
            "initial_schema".to_owned(),
            json!({
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "value", "required": false, "type": "long"}
                ]
            }),
        ),
        (
            "maximum_response_bytes".to_owned(),
            Value::from(MAXIMUM_RESPONSE_BYTES as u64),
        ),
        (
            "namespace_separator_default".to_owned(),
            Value::from(DEFAULT_NAMESPACE_SEPARATOR),
        ),
        ("purge_requested".to_owned(), Value::from(false)),
        ("request_timeout_ms".to_owned(), Value::from(30_000)),
        (
            "schema_transition".to_owned(),
            json!({
                "last-column-id": 2,
                "schema": {
                    "type": "struct",
                    "schema-id": 1,
                    "fields": [
                        {"id": 1, "name": "value", "required": false, "type": "long"},
                        {"id": 2, "name": "note", "required": false, "type": "string"}
                    ]
                }
            }),
        ),
        (
            "stale_set_properties".to_owned(),
            json!({"c1-06.stale": "must-not-apply"}),
        ),
        (
            "transcript_format".to_owned(),
            Value::from(COMMIT_TRANSCRIPT_FORMAT),
        ),
    ]);
    if scenario.parameters != expected {
        bail!(
            "commit probe scenario parameters drifted from the implemented v{COMMIT_SCENARIO_VERSION} policy"
        );
    }
    Ok(())
}

fn validate_capabilities(scenario: &Scenario) -> Result<()> {
    let expected = [
        ("iceberg-rest.namespace.create", RequirementLevel::Required),
        ("iceberg-rest.namespace.load", RequirementLevel::Required),
        ("iceberg-rest.namespace.drop", RequirementLevel::Required),
        ("iceberg-rest.table.create", RequirementLevel::Required),
        ("iceberg-rest.table.load", RequirementLevel::Required),
        ("iceberg-rest.table.update", RequirementLevel::Required),
        ("iceberg-rest.table.drop", RequirementLevel::Required),
        ("iceberg-rest.error.spec-shape", RequirementLevel::Required),
        (
            "iceberg-rest.table.commit.set-properties",
            RequirementLevel::Required,
        ),
        (
            "iceberg-rest.table.commit.requirements",
            RequirementLevel::Required,
        ),
        (
            "iceberg-rest.table.commit.stale-pointer-rejection",
            RequirementLevel::Required,
        ),
        (
            "iceberg-rest.table.commit.exact-retry",
            RequirementLevel::Optional,
        ),
        (
            "iceberg-rest.idempotency-key.content-binding",
            RequirementLevel::Optional,
        ),
    ];
    if scenario.capabilities.len() != expected.len() {
        bail!(
            "commit probe scenario must declare exactly {} capabilities",
            expected.len()
        );
    }
    for (actual, (capability, level)) in scenario.capabilities.iter().zip(expected) {
        if actual.capability.as_str() != capability
            || actual.level != level
            || actual.specification.as_deref() != Some(ICEBERG_REST_OPENAPI_SOURCE)
        {
            bail!("commit probe scenario capability policy drifted at `{capability}`");
        }
    }
    Ok(())
}

fn validate_steps(scenario: &Scenario) -> Result<()> {
    let expected = [
        (
            "negotiate-authentication",
            ActorRole::Harness,
            "authentication.negotiate",
            &[][..],
            None,
        ),
        (
            "negotiate-config",
            ActorRole::Client,
            "config.get-routing-and-idempotency",
            &["negotiate-authentication"][..],
            Some(30_000),
        ),
        (
            "preflight-fixture",
            ActorRole::Harness,
            "commit.verify-fixture-absence",
            &["negotiate-config"][..],
            None,
        ),
        (
            "create-fixture",
            ActorRole::Client,
            "commit.create-and-load-table",
            &["preflight-fixture"][..],
            Some(30_000),
        ),
        (
            "accept-current-requirements",
            ActorRole::Client,
            "table.commit.set-properties",
            &["create-fixture"][..],
            Some(30_000),
        ),
        (
            "advance-schema",
            ActorRole::Client,
            "table.commit.advance-schema",
            &["accept-current-requirements"][..],
            Some(30_000),
        ),
        (
            "reject-stale-requirement",
            ActorRole::Client,
            "table.commit.reject-stale-schema",
            &["advance-schema"][..],
            Some(30_000),
        ),
        (
            "inspect-idempotency-advertisement",
            ActorRole::Harness,
            "config.inspect-idempotency-key-lifetime",
            &["negotiate-config"][..],
            None,
        ),
        (
            "retry-exact-request",
            ActorRole::Client,
            "table.commit.retry-exact",
            &[
                "reject-stale-requirement",
                "inspect-idempotency-advertisement",
            ][..],
            Some(30_000),
        ),
        (
            "reject-idempotency-drift",
            ActorRole::Client,
            "table.commit.reject-idempotency-content-drift",
            &["retry-exact-request"][..],
            Some(30_000),
        ),
        (
            "verify-required-state",
            ActorRole::Client,
            "table.load-required-final-state",
            &["reject-stale-requirement"][..],
            Some(30_000),
        ),
        (
            "cleanup-fixture",
            ActorRole::Harness,
            "commit.reconcile-drop-and-verify",
            &["reject-idempotency-drift", "verify-required-state"][..],
            Some(30_000),
        ),
        (
            "sanitize-transcript",
            ActorRole::Harness,
            "evidence.sanitize-commit-transcript",
            &["cleanup-fixture"][..],
            None,
        ),
    ];
    if scenario.steps.len() != expected.len() {
        bail!(
            "commit probe scenario must declare exactly {} steps",
            expected.len()
        );
    }
    for (actual, (id, actor, operation, dependencies, timeout_ms)) in
        scenario.steps.iter().zip(expected)
    {
        let actual_dependencies = actual
            .depends_on
            .iter()
            .map(|dependency| dependency.as_str())
            .collect::<Vec<_>>();
        if actual.id.as_str() != id
            || actual.actor != actor
            || actual.operation != operation
            || actual_dependencies != dependencies
            || actual.timeout_ms != timeout_ms
            || !actual.parameters.is_empty()
        {
            bail!("commit probe scenario step policy drifted at `{id}`");
        }
    }
    Ok(())
}

fn validate_assertions(scenario: &Scenario) -> Result<()> {
    let expected = [
        (
            "authentication-ready",
            "negotiate-authentication",
            true,
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/authentication-ready-v1",
                "configuration": {"persist_token": false}
            }),
        ),
        (
            "config-routing-ready",
            "negotiate-config",
            true,
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/commit-config-routing-v1",
                "configuration": {"default_namespace_separator": "%1F"}
            }),
        ),
        (
            "fixture-isolated",
            "preflight-fixture",
            true,
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/commit-fixture-isolation-v1",
                "configuration": {"code": 404, "type": "NoSuchNamespaceException"}
            }),
        ),
        (
            "commit-fixture-ready",
            "create-fixture",
            true,
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/commit-fixture-v1",
                "configuration": {"create_http_status": 200, "load_http_status": 200}
            }),
        ),
        (
            "current-requirements-admitted",
            "accept-current-requirements",
            true,
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/commit-requirements-v1",
                "configuration": {
                    "expected_http_status": 200,
                    "require_new_metadata_location": true
                }
            }),
        ),
        (
            "schema-transition-admitted",
            "advance-schema",
            true,
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/commit-schema-transition-v1",
                "configuration": {"current_schema_id": 1, "last_column_id": 2}
            }),
        ),
        (
            "stale-requirement-rejected-atomically",
            "reject-stale-requirement",
            true,
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/commit-stale-rejection-v1",
                "configuration": {"code": 409, "type": "CommitFailedException"}
            }),
        ),
        (
            "idempotency-support-advertised",
            "inspect-idempotency-advertisement",
            false,
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/idempotency-advertisement-v1",
                "configuration": {"accepted_pointers": IDEMPOTENCY_POINTERS}
            }),
        ),
        (
            "exact-request-replayed-once",
            "retry-exact-request",
            false,
            json!({"kind": "exact-replay"}),
        ),
        (
            "idempotency-content-drift-rejected",
            "reject-idempotency-drift",
            false,
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/idempotency-content-binding-v1",
                "configuration": {
                    "expected_http_status": 409,
                    "require_unchanged_metadata_location": true
                }
            }),
        ),
        (
            "required-final-state-exact",
            "verify-required-state",
            true,
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/commit-final-state-v1",
                "configuration": {
                    "current_schema_id": 1,
                    "forbidden_property": "c1-06.stale"
                }
            }),
        ),
        (
            "commit-fixture-clean",
            "cleanup-fixture",
            true,
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/commit-cleanup-v1",
                "configuration": {"final_http_status": 404, "purge_requested": false}
            }),
        ),
        (
            "transcript-sanitized",
            "sanitize-transcript",
            true,
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/sanitized-commit-transcript-v1",
                "configuration": {
                    "idempotency_header_policy": "redact",
                    "request_header_policy": "allowlist-and-redact-authorization",
                    "response_header_policy": "allowlist",
                    "response_json_policy": "recursive-secret-key-redaction"
                }
            }),
        ),
    ];
    if scenario.assertions.len() != expected.len() {
        bail!(
            "commit probe scenario must declare exactly {} assertions",
            expected.len()
        );
    }
    for (actual, (id, step, required, check)) in scenario.assertions.iter().zip(expected) {
        if actual.id.as_str() != id
            || actual.step.as_str() != step
            || actual.required != required
            || serde_json::to_value(&actual.check)? != check
        {
            bail!("commit probe scenario assertion policy drifted at `{id}`");
        }
    }
    Ok(())
}

pub(super) fn evaluate_assertions(scenario: &Scenario, facts: &CommitFacts) -> Vec<ProbeAssertion> {
    scenario
        .assertions
        .iter()
        .map(|assertion| ProbeAssertion {
            assertion: assertion.id.clone(),
            required: assertion.required,
            outcome: facts.for_assertion(&assertion.check).outcome(),
        })
        .collect()
}
