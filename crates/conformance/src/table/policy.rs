use std::collections::BTreeMap;

use anyhow::{bail, Result};
use catalog_bench_common::contract::{ActorRole, ComponentId, Profile, RequirementLevel, Scenario};
use serde_json::{json, Value};

use super::{
    fixture::FIXTURE_PREFIX, TableFacts, TABLE_SCENARIO_ID, TABLE_SCENARIO_VERSION,
    TABLE_TRANSCRIPT_FORMAT,
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
    if scenario.id.as_str() != TABLE_SCENARIO_ID {
        bail!(
            "table probe requires scenario `{TABLE_SCENARIO_ID}`, found `{}`",
            scenario.id
        );
    }
    if scenario.version != TABLE_SCENARIO_VERSION {
        bail!(
            "table probe supports scenario version {TABLE_SCENARIO_VERSION}, found {}",
            scenario.version
        );
    }
    validate_scenario_policy(scenario)?;
    ProbeTarget::resolve(profile, scenario, catalog)?;
    Ok(())
}

fn validate_scenario_policy(scenario: &Scenario) -> Result<()> {
    let expected_parameters = BTreeMap::from([
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
            "initial_properties".to_owned(),
            json!({
                "c1-05.remove": "before",
                "c1-05.state": "before",
                "catalog-bench.owner": "catalog-bench"
            }),
        ),
        ("maximum_pages".to_owned(), Value::from(64)),
        (
            "maximum_response_bytes".to_owned(),
            Value::from(MAXIMUM_RESPONSE_BYTES as u64),
        ),
        (
            "namespace_separator_default".to_owned(),
            Value::from(DEFAULT_NAMESPACE_SEPARATOR),
        ),
        ("page_size".to_owned(), Value::from(1)),
        ("purge_requested".to_owned(), Value::from(false)),
        ("register_overwrite".to_owned(), Value::from(false)),
        ("remove_properties".to_owned(), json!(["c1-05.remove"])),
        ("request_timeout_ms".to_owned(), Value::from(30_000)),
        ("set_properties".to_owned(), json!({"c1-05.state": "after"})),
        (
            "table_schema".to_owned(),
            json!({
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "value", "required": false, "type": "long"}
                ]
            }),
        ),
        (
            "transcript_format".to_owned(),
            Value::from(TABLE_TRANSCRIPT_FORMAT),
        ),
    ]);
    if scenario.parameters != expected_parameters {
        bail!(
            "table probe scenario parameters drifted from the implemented v{TABLE_SCENARIO_VERSION} policy"
        );
    }

    let expected_capabilities = [
        "iceberg-rest.namespace.create",
        "iceberg-rest.namespace.load",
        "iceberg-rest.namespace.drop",
        "iceberg-rest.table.create",
        "iceberg-rest.table.list",
        "iceberg-rest.table.load",
        "iceberg-rest.table.register",
        "iceberg-rest.table.rename",
        "iceberg-rest.table.update",
        "iceberg-rest.table.drop",
        "iceberg-rest.error.spec-shape",
    ];
    if scenario.capabilities.len() != expected_capabilities.len() {
        bail!(
            "table probe scenario must declare exactly {} capabilities",
            expected_capabilities.len()
        );
    }
    for (requirement, expected) in scenario.capabilities.iter().zip(expected_capabilities) {
        let expected_level = if matches!(
            expected,
            "iceberg-rest.table.register" | "iceberg-rest.table.rename"
        ) {
            RequirementLevel::Optional
        } else {
            RequirementLevel::Required
        };
        if requirement.capability.as_str() != expected
            || requirement.level != expected_level
            || requirement.specification.as_deref() != Some(ICEBERG_REST_OPENAPI_SOURCE)
        {
            bail!("table probe scenario capability policy drifted at `{expected}`");
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
            "table.verify-fixture-absence",
            &["negotiate-config"][..],
            None,
        ),
        (
            "create-namespace",
            ActorRole::Client,
            "namespace.create-table-fixture",
            &["preflight-fixture"][..],
            Some(30_000),
        ),
        (
            "create-tables",
            ActorRole::Client,
            "table.create",
            &["create-namespace"][..],
            Some(30_000),
        ),
        (
            "inspect-tables",
            ActorRole::Client,
            "table.list-and-load",
            &["create-tables"][..],
            Some(30_000),
        ),
        (
            "traverse-pagination",
            ActorRole::Client,
            "table.list-pages",
            &["create-tables"][..],
            Some(30_000),
        ),
        (
            "update-table",
            ActorRole::Client,
            "table.update-properties",
            &["inspect-tables"][..],
            Some(30_000),
        ),
        (
            "reject-duplicate",
            ActorRole::Client,
            "table.create-duplicate",
            &["create-tables"][..],
            Some(30_000),
        ),
        (
            "reject-missing-table",
            ActorRole::Client,
            "table.load-missing",
            &["create-namespace"][..],
            Some(30_000),
        ),
        (
            "reject-missing-namespace",
            ActorRole::Client,
            "table.list-missing-namespace",
            &["negotiate-config"][..],
            Some(30_000),
        ),
        (
            "rename-table",
            ActorRole::Client,
            "table.rename-same-namespace",
            &["update-table"][..],
            Some(30_000),
        ),
        (
            "drop-table",
            ActorRole::Client,
            "table.drop-without-purge",
            &["inspect-tables"][..],
            Some(30_000),
        ),
        (
            "register-table",
            ActorRole::Client,
            "table.register-metadata",
            &["drop-table"][..],
            Some(30_000),
        ),
        (
            "cleanup-fixture",
            ActorRole::Harness,
            "table.reconcile-drop-and-verify",
            &[
                "traverse-pagination",
                "reject-duplicate",
                "reject-missing-table",
                "reject-missing-namespace",
                "rename-table",
                "register-table",
            ][..],
            Some(30_000),
        ),
        (
            "sanitize-transcript",
            ActorRole::Harness,
            "evidence.sanitize-http-transcript",
            &["cleanup-fixture"][..],
            None,
        ),
    ];
    if scenario.steps.len() != expected_steps.len() {
        bail!(
            "table probe scenario must declare exactly {} steps",
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
            bail!("table probe scenario step policy drifted at `{id}`");
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
                "name": "querygraph/catalog-bench/table-config-routing-v1",
                "configuration": {"default_namespace_separator": "%1F"}
            }),
        ),
        (
            "fixture-isolated",
            "preflight-fixture",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/table-fixture-isolation-v1",
                "configuration": {"code": 404, "type": "NoSuchNamespaceException"}
            }),
        ),
        (
            "fixture-namespace-created",
            "create-namespace",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/table-namespace-create-v1",
                "configuration": {"expected_http_status": 200}
            }),
        ),
        (
            "table-create-round-trip",
            "create-tables",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/table-create-v1",
                "configuration": {"expected_http_status": 200, "stage_create": false}
            }),
        ),
        (
            "table-list-visible",
            "inspect-tables",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/table-list-v1",
                "configuration": {"duplicate_policy": "reject"}
            }),
        ),
        (
            "table-load-round-trip",
            "inspect-tables",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/table-load-v1",
                "configuration": {"expected_http_status": 200}
            }),
        ),
        (
            "table-pagination-complete",
            "traverse-pagination",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/table-pagination-v1",
                "configuration": {
                    "accept_unpaginated_fallback": true,
                    "maximum_pages": 64,
                    "page_size": 1
                }
            }),
        ),
        (
            "table-update-round-trip",
            "update-table",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/table-update-v1",
                "configuration": {
                    "preserve_unmentioned": true,
                    "require_new_metadata_location": true
                }
            }),
        ),
        (
            "duplicate-table-error-spec-shaped",
            "reject-duplicate",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/table-duplicate-error-v1",
                "configuration": {"code": 409, "type": "AlreadyExistsException"}
            }),
        ),
        (
            "missing-table-error-spec-shaped",
            "reject-missing-table",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/table-missing-error-v1",
                "configuration": {"code": 404, "type": "NoSuchTableException"}
            }),
        ),
        (
            "missing-namespace-error-spec-shaped",
            "reject-missing-namespace",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/table-missing-namespace-error-v1",
                "configuration": {"code": 404, "type": "NoSuchNamespaceException"}
            }),
        ),
        (
            "table-rename-round-trip",
            "rename-table",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/table-rename-v1",
                "configuration": {"expected_http_status": 204, "scope": "same-namespace"}
            }),
        ),
        (
            "table-drop-clean",
            "drop-table",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/table-drop-v1",
                "configuration": {"final_http_status": 404, "purge_requested": false}
            }),
        ),
        (
            "table-register-round-trip",
            "register-table",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/table-register-v1",
                "configuration": {"expected_http_status": 200, "overwrite": false}
            }),
        ),
        (
            "table-fixture-clean",
            "cleanup-fixture",
            json!({
                "kind": "custom",
                "name": "querygraph/catalog-bench/table-cleanup-v1",
                "configuration": {"final_http_status": 404, "purge_requested": false}
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
            "table probe scenario must declare exactly {} assertions",
            expected_assertions.len()
        );
    }
    for (assertion, (id, step, check)) in scenario.assertions.iter().zip(expected_assertions) {
        let expected_required =
            !matches!(id, "table-rename-round-trip" | "table-register-round-trip");
        if assertion.id.as_str() != id
            || assertion.step.as_str() != step
            || assertion.required != expected_required
            || serde_json::to_value(&assertion.check)? != check
        {
            bail!("table probe scenario assertion policy drifted at `{id}`");
        }
    }

    if !scenario.extensions.is_empty() {
        bail!("table probe scenario v{TABLE_SCENARIO_VERSION} does not accept extensions");
    }
    Ok(())
}

pub(super) fn evaluate_assertions(scenario: &Scenario, facts: &TableFacts) -> Vec<ProbeAssertion> {
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
