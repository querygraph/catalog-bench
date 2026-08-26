use std::collections::BTreeMap;

use anyhow::{bail, Result};
use catalog_bench_common::contract::{ActorRole, ComponentId, Profile, RequirementLevel, Scenario};
use serde_json::{json, Value};

use super::{
    NamespaceFacts, NAMESPACE_SCENARIO_ID, NAMESPACE_SCENARIO_VERSION, NAMESPACE_TRANSCRIPT_FORMAT,
};
use crate::evidence::ProbeAssertion;
use crate::spec::{ICEBERG_REST_OPENAPI_SHA256, ICEBERG_REST_OPENAPI_SOURCE};
use crate::target::ProbeTarget;
use crate::transport::MAXIMUM_RESPONSE_BYTES;

pub(super) fn validate_invocation(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &ComponentId,
) -> Result<()> {
    if scenario.id.as_str() != NAMESPACE_SCENARIO_ID {
        bail!(
            "namespace probe requires scenario `{NAMESPACE_SCENARIO_ID}`, found `{}`",
            scenario.id
        );
    }
    if scenario.version != NAMESPACE_SCENARIO_VERSION {
        bail!(
            "namespace probe supports scenario version {NAMESPACE_SCENARIO_VERSION}, found {}",
            scenario.version
        );
    }
    validate_scenario_policy(scenario)?;
    ProbeTarget::resolve(profile, scenario, catalog)?;
    Ok(())
}

fn validate_scenario_policy(scenario: &Scenario) -> Result<()> {
    let expected_parameters = BTreeMap::from([
        ("fixture_prefix".to_owned(), Value::from("cb_c104")),
        (
            "iceberg_openapi_sha256".to_owned(),
            Value::from(ICEBERG_REST_OPENAPI_SHA256),
        ),
        (
            "iceberg_openapi_source".to_owned(),
            Value::from(ICEBERG_REST_OPENAPI_SOURCE),
        ),
        ("maximum_pages".to_owned(), Value::from(64)),
        (
            "maximum_response_bytes".to_owned(),
            Value::from(MAXIMUM_RESPONSE_BYTES as u64),
        ),
        (
            "namespace_separator_default".to_owned(),
            Value::from(super::routes::DEFAULT_SEPARATOR),
        ),
        ("page_size".to_owned(), Value::from(1)),
        ("request_timeout_ms".to_owned(), Value::from(30_000)),
        (
            "transcript_format".to_owned(),
            Value::from(NAMESPACE_TRANSCRIPT_FORMAT),
        ),
    ]);
    if scenario.parameters != expected_parameters {
        bail!(
            "namespace probe scenario parameters drifted from the implemented v{NAMESPACE_SCENARIO_VERSION} policy"
        );
    }

    let expected_capabilities = [
        "iceberg-rest.namespace.create",
        "iceberg-rest.namespace.list",
        "iceberg-rest.namespace.load",
        "iceberg-rest.namespace.update-properties",
        "iceberg-rest.namespace.drop",
        "iceberg-rest.namespace.hierarchy",
        "iceberg-rest.namespace.pagination",
        "iceberg-rest.namespace.error.duplicate",
        "iceberg-rest.namespace.error.missing-parent",
        "iceberg-rest.error.spec-shape",
    ];
    if scenario.capabilities.len() != expected_capabilities.len() {
        bail!(
            "namespace probe scenario must declare exactly {} capabilities",
            expected_capabilities.len()
        );
    }
    for (requirement, expected) in scenario.capabilities.iter().zip(expected_capabilities) {
        let expected_level = if expected == "iceberg-rest.namespace.update-properties" {
            RequirementLevel::Optional
        } else {
            RequirementLevel::Required
        };
        if requirement.capability.as_str() != expected
            || requirement.level != expected_level
            || requirement.specification.as_deref() != Some(ICEBERG_REST_OPENAPI_SOURCE)
        {
            bail!("namespace probe scenario capability policy drifted at `{expected}`");
        }
    }

    let expected_steps = [
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
            "config.get-routing",
            &["negotiate-authentication"][..],
            Some(30_000),
        ),
        (
            "preflight-fixture",
            ActorRole::Harness,
            "namespace.verify-fixture-absence",
            &["negotiate-config"][..],
            None,
        ),
        (
            "create-namespaces",
            ActorRole::Client,
            "namespace.create",
            &["preflight-fixture"][..],
            Some(30_000),
        ),
        (
            "inspect-namespace",
            ActorRole::Client,
            "namespace.list-and-load",
            &["create-namespaces"][..],
            Some(30_000),
        ),
        (
            "update-namespace",
            ActorRole::Client,
            "namespace.update-properties",
            &["inspect-namespace"][..],
            Some(30_000),
        ),
        (
            "reject-duplicate",
            ActorRole::Client,
            "namespace.create-duplicate",
            &["create-namespaces"][..],
            Some(30_000),
        ),
        (
            "inspect-hierarchy",
            ActorRole::Client,
            "namespace.list-under-parent",
            &["create-namespaces"][..],
            Some(30_000),
        ),
        (
            "traverse-pagination",
            ActorRole::Client,
            "namespace.list-pages",
            &["create-namespaces"][..],
            Some(30_000),
        ),
        (
            "reject-missing-parent",
            ActorRole::Client,
            "namespace.list-under-missing-parent",
            &["negotiate-config"][..],
            Some(30_000),
        ),
        (
            "cleanup-namespaces",
            ActorRole::Harness,
            "namespace.drop-and-verify",
            &[
                "inspect-namespace",
                "update-namespace",
                "reject-duplicate",
                "inspect-hierarchy",
                "traverse-pagination",
                "reject-missing-parent",
            ][..],
            Some(30_000),
        ),
        (
            "sanitize-transcript",
            ActorRole::Harness,
            "evidence.sanitize-http-transcript",
            &["cleanup-namespaces"][..],
            None,
        ),
    ];
    if scenario.steps.len() != expected_steps.len() {
        bail!(
            "namespace probe scenario must declare exactly {} steps",
            expected_steps.len()
        );
    }
    for (step, (id, actor, operation, dependencies, timeout_ms)) in
        scenario.steps.iter().zip(expected_steps)
    {
        let actual_dependencies = step
            .depends_on
            .iter()
            .map(|dependency| dependency.as_str())
            .collect::<Vec<_>>();
        if step.id.as_str() != id
            || step.actor != actor
            || step.operation != operation
            || actual_dependencies != dependencies
            || step.timeout_ms != timeout_ms
            || !step.parameters.is_empty()
        {
            bail!("namespace probe scenario step policy drifted at `{id}`");
        }
    }

    let expected_assertions = [
        (
            "authentication-ready",
            "negotiate-authentication",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/authentication-ready-v1",
                "configuration": {"persist_token": false}
            }),
        ),
        (
            "config-routing-ready",
            "negotiate-config",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/namespace-config-routing-v1",
                "configuration": {"default_namespace_separator": "%1F"}
            }),
        ),
        (
            "fixture-isolated",
            "preflight-fixture",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/namespace-fixture-isolation-v1",
                "configuration": {"expected_http_status": 404}
            }),
        ),
        (
            "namespace-create-round-trip",
            "create-namespaces",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/namespace-create-v1",
                "configuration": {"expected_http_status": 200}
            }),
        ),
        (
            "namespace-list-visible",
            "inspect-namespace",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/namespace-list-v1",
                "configuration": {"duplicate_policy": "reject"}
            }),
        ),
        (
            "namespace-load-round-trip",
            "inspect-namespace",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/namespace-load-v1",
                "configuration": {"expected_http_status": 200}
            }),
        ),
        (
            "namespace-properties-updated",
            "update-namespace",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/namespace-properties-v1",
                "configuration": {"preserve_unmentioned": true}
            }),
        ),
        (
            "duplicate-error-spec-shaped",
            "reject-duplicate",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/namespace-duplicate-error-v1",
                "configuration": {"code": 409, "type": "AlreadyExistsException"}
            }),
        ),
        (
            "namespace-hierarchy-preserved",
            "inspect-hierarchy",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/namespace-hierarchy-v1",
                "configuration": {"scope": "immediate-children"}
            }),
        ),
        (
            "namespace-pagination-complete",
            "traverse-pagination",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/namespace-pagination-v1",
                "configuration": {
                    "accept_unpaginated_fallback": true,
                    "maximum_pages": 64,
                    "page_size": 1
                }
            }),
        ),
        (
            "missing-parent-error-spec-shaped",
            "reject-missing-parent",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/namespace-missing-parent-error-v1",
                "configuration": {"code": 404, "type": "NoSuchNamespaceException"}
            }),
        ),
        (
            "namespace-drop-clean",
            "cleanup-namespaces",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/namespace-cleanup-v1",
                "configuration": {"final_http_status": 404}
            }),
        ),
        (
            "transcript-sanitized",
            "sanitize-transcript",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/sanitized-http-transcript-v1",
                "configuration": {
                    "request_header_policy": "allowlist-and-redact-authorization",
                    "response_header_policy": "allowlist",
                    "response_json_policy": "recursive-secret-key-redaction"
                }
            }),
        ),
    ];
    if scenario.assertions.len() != expected_assertions.len() {
        bail!(
            "namespace probe scenario must declare exactly {} assertions",
            expected_assertions.len()
        );
    }
    for (assertion, (id, step, check)) in scenario.assertions.iter().zip(expected_assertions) {
        let expected_required = id != "namespace-properties-updated";
        if assertion.id.as_str() != id
            || assertion.step.as_str() != step
            || assertion.required != expected_required
            || serde_json::to_value(&assertion.check)? != check
        {
            bail!("namespace probe scenario assertion policy drifted at `{id}`");
        }
    }

    if !scenario.extensions.is_empty() {
        bail!("namespace probe scenario v{NAMESPACE_SCENARIO_VERSION} does not accept extensions");
    }
    Ok(())
}

pub(super) fn evaluate_assertions(
    scenario: &Scenario,
    facts: &NamespaceFacts,
) -> Vec<ProbeAssertion> {
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
