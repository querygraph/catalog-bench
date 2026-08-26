use std::collections::BTreeMap;

use anyhow::Result;
use reqwest::Method;
use serde_json::{json, Value};
use url::Url;

use super::{
    CommitFacts, CommitFixture, CommitIdempotency, CONTENT_BINDING_CAPABILITY, CREATE_CAPABILITY,
    DROP_CAPABILITY, EXACT_RETRY_CAPABILITY, LOAD_CAPABILITY, UPDATE_CAPABILITY,
};
use crate::iceberg::CatalogRoutes;
use crate::idempotency::IdempotencyKey;
use crate::operation::{
    all_results, validate_error_response, validate_spec_error_response, validate_status, Fact,
    Observation, OperationRecorder,
};
use crate::table_protocol::{
    committed_table_request, parse_table_snapshot, validate_namespace_response,
    TableCreateLocations, TableSchema, TableSnapshot,
};

const NAMESPACE_CREATE_CAPABILITY: &str = "iceberg-rest.namespace.create";
const NAMESPACE_LOAD_CAPABILITY: &str = "iceberg-rest.namespace.load";
const NAMESPACE_DROP_CAPABILITY: &str = "iceberg-rest.namespace.drop";

const OWNER_PROPERTY: &str = "catalog-bench.owner";
const STATE_PROPERTY: &str = "c1-06.state";
const STALE_PROPERTY: &str = "c1-06.stale";
const RETRY_PROPERTY: &str = "c1-06.retry";

pub(super) async fn execute_commit_workflow(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &CommitFixture,
    create_locations: &TableCreateLocations,
    idempotency: CommitIdempotency<'_>,
    facts: &mut CommitFacts,
) -> Result<()> {
    let preflight = recorder
        .attempt(
            "preflight-namespace",
            Some(NAMESPACE_LOAD_CAPABILITY),
            Method::GET,
            routes.namespace(&fixture.namespace)?,
            None,
        )
        .await;
    facts.fixture_isolated = Fact::from_result(validate_error_response(
        &preflight,
        404,
        "NoSuchNamespaceException",
    ));
    if !facts.fixture_isolated.passed() {
        let reason = facts
            .fixture_isolated
            .explanation("fixture preflight did not pass");
        skip_after_preflight(recorder, &reason);
        facts.skip_after_preflight(&reason);
        return Ok(());
    }

    let namespace_create = recorder
        .attempt(
            "create-namespace",
            Some(NAMESPACE_CREATE_CAPABILITY),
            Method::POST,
            routes.namespace_collection()?,
            Some(json!({
                "namespace": fixture.namespace.parts(),
                "properties": {(OWNER_PROPERTY): "catalog-bench"}
            })),
        )
        .await;
    let namespace_created = validate_namespace_response(&namespace_create, &fixture.namespace);

    let initial_snapshot = if namespace_created.is_ok() {
        create_and_load_table(recorder, routes, fixture, create_locations).await?
    } else {
        let reason = "fixture namespace creation did not pass";
        recorder.skip("create-table", Some(CREATE_CAPABILITY), reason);
        recorder.skip("load-initial-table", Some(LOAD_CAPABILITY), reason);
        Err(namespace_created.expect_err("branch requires failed namespace validation"))
    };
    facts.fixture_ready =
        Fact::from_result(initial_snapshot.as_ref().map(|_| ()).map_err(Clone::clone));

    let Some(initial_snapshot) = initial_snapshot.ok() else {
        let reason = facts
            .fixture_ready
            .explanation("commit fixture creation did not pass");
        skip_required_commits(recorder, &reason);
        skip_optional_commits(recorder, &reason);
        facts.skip_after_fixture(&reason);
        facts.cleanup = Fact::from_result(cleanup_fixture(recorder, routes, fixture).await?);
        return Ok(());
    };

    let current_snapshot =
        commit_current_requirements(recorder, routes, fixture, &initial_snapshot).await?;
    facts.current_requirements =
        Fact::from_result(current_snapshot.as_ref().map(|_| ()).map_err(Clone::clone));
    let Some(current_snapshot) = current_snapshot.ok() else {
        let reason = facts
            .current_requirements
            .explanation("current requirement commit did not pass");
        skip_after_current_commit(recorder, &reason);
        facts.schema_transition = Fact::NotEvaluated(reason.clone());
        facts.stale_rejection = Fact::NotEvaluated(reason.clone());
        facts.required_final = Fact::NotEvaluated(reason.clone());
        facts.exact_replay = Fact::NotEvaluated(reason.clone());
        facts.content_binding = Fact::NotEvaluated(reason);
        facts.cleanup = Fact::from_result(cleanup_fixture(recorder, routes, fixture).await?);
        return Ok(());
    };

    let schema_snapshot =
        commit_schema_transition(recorder, routes, fixture, &current_snapshot).await?;
    facts.schema_transition =
        Fact::from_result(schema_snapshot.as_ref().map(|_| ()).map_err(Clone::clone));
    let Some(schema_snapshot) = schema_snapshot.ok() else {
        let reason = facts
            .schema_transition
            .explanation("schema transition did not pass");
        skip_after_schema_commit(recorder, &reason);
        facts.stale_rejection = Fact::NotEvaluated(reason.clone());
        facts.required_final = Fact::NotEvaluated(reason.clone());
        facts.exact_replay = Fact::NotEvaluated(reason.clone());
        facts.content_binding = Fact::NotEvaluated(reason);
        facts.cleanup = Fact::from_result(cleanup_fixture(recorder, routes, fixture).await?);
        return Ok(());
    };

    facts.stale_rejection = Fact::from_result(
        reject_stale_requirement(recorder, routes, fixture, &schema_snapshot).await?,
    );

    // This required proof intentionally precedes optional idempotency mutation.
    // It therefore remains comparable even when the optional branch is absent
    // or behaves incorrectly.
    facts.required_final = Fact::from_result(
        verify_required_final_state(recorder, routes, fixture, &schema_snapshot).await?,
    );

    execute_optional_idempotency(
        recorder,
        routes,
        fixture,
        &schema_snapshot,
        idempotency,
        facts,
    )
    .await?;

    facts.cleanup = Fact::from_result(cleanup_fixture(recorder, routes, fixture).await?);
    Ok(())
}

async fn create_and_load_table(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &CommitFixture,
    create_locations: &TableCreateLocations,
) -> Result<std::result::Result<TableSnapshot, String>> {
    let requested_location = create_locations.for_table(&fixture.namespace, &fixture.table)?;
    let create = recorder
        .attempt(
            "create-table",
            Some(CREATE_CAPABILITY),
            Method::POST,
            routes.table_collection(&fixture.namespace)?,
            Some(committed_table_request(
                &fixture.table,
                requested_location.as_deref(),
                initial_schema(),
                BTreeMap::from([
                    (OWNER_PROPERTY.to_owned(), "catalog-bench".to_owned()),
                    (STATE_PROPERTY.to_owned(), "initial".to_owned()),
                ]),
            )),
        )
        .await;
    let load = recorder
        .attempt(
            "load-initial-table",
            Some(LOAD_CAPABILITY),
            Method::GET,
            routes.table(&fixture.namespace, &fixture.table)?,
            None,
        )
        .await;

    let created = parse_table_snapshot(&create, 200)
        .and_then(|snapshot| validate_initial_snapshot(snapshot, requested_location.as_deref()));
    let loaded = parse_table_snapshot(&load, 200);
    Ok(created.and_then(|created| {
        loaded.and_then(|loaded| {
            validate_exact_snapshot(&created, &loaded)?;
            Ok(loaded)
        })
    }))
}

async fn commit_current_requirements(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &CommitFixture,
    before: &TableSnapshot,
) -> Result<std::result::Result<TableSnapshot, String>> {
    let body = property_commit_body(&before.uuid, 0, STATE_PROPERTY, "requirement-accepted");
    let commit = recorder
        .attempt(
            "commit-current-requirements",
            Some(UPDATE_CAPABILITY),
            Method::POST,
            routes.table(&fixture.namespace, &fixture.table)?,
            Some(body),
        )
        .await;
    let reload = recorder
        .attempt(
            "reload-after-current-requirements",
            Some(LOAD_CAPABILITY),
            Method::GET,
            routes.table(&fixture.namespace, &fixture.table)?,
            None,
        )
        .await;
    Ok(validate_property_transition(
        before,
        &commit,
        &reload,
        STATE_PROPERTY,
        "requirement-accepted",
    ))
}

async fn commit_schema_transition(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &CommitFixture,
    before: &TableSnapshot,
) -> Result<std::result::Result<TableSnapshot, String>> {
    let commit = recorder
        .attempt(
            "commit-schema-transition",
            Some(UPDATE_CAPABILITY),
            Method::POST,
            routes.table(&fixture.namespace, &fixture.table)?,
            Some(json!({
                "requirements": [
                    {"type": "assert-table-uuid", "uuid": before.uuid},
                    {"type": "assert-current-schema-id", "current-schema-id": 0},
                    {
                        "type": "assert-last-assigned-field-id",
                        "last-assigned-field-id": 1
                    }
                ],
                "updates": [
                    {
                        "action": "add-schema",
                        "schema": transitioned_schema(),
                        "last-column-id": 2
                    },
                    {"action": "set-current-schema", "schema-id": 1}
                ]
            })),
        )
        .await;
    let reload = recorder
        .attempt(
            "reload-after-schema-transition",
            Some(LOAD_CAPABILITY),
            Method::GET,
            routes.table(&fixture.namespace, &fixture.table)?,
            None,
        )
        .await;

    Ok(parse_table_snapshot(&commit, 200).and_then(|committed| {
        validate_identity_and_pointer_transition(before, &committed)?;
        validate_schema_one(&committed)?;
        if scenario_properties(&committed.properties) != scenario_properties(&before.properties) {
            return Err("schema transition changed scenario-owned table properties".to_owned());
        }
        parse_table_snapshot(&reload, 200).and_then(|loaded| {
            validate_exact_snapshot(&committed, &loaded)?;
            Ok(loaded)
        })
    }))
}

async fn reject_stale_requirement(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &CommitFixture,
    before: &TableSnapshot,
) -> Result<std::result::Result<(), String>> {
    let commit = recorder
        .attempt(
            "commit-stale-requirement",
            Some(UPDATE_CAPABILITY),
            Method::POST,
            routes.table(&fixture.namespace, &fixture.table)?,
            Some(property_commit_body(
                &before.uuid,
                0,
                STALE_PROPERTY,
                "must-not-apply",
            )),
        )
        .await;
    let reload = recorder
        .attempt(
            "reload-after-stale-requirement",
            Some(LOAD_CAPABILITY),
            Method::GET,
            routes.table(&fixture.namespace, &fixture.table)?,
            None,
        )
        .await;
    let conflict = validate_error_response(&commit, 409, "CommitFailedException");
    let unchanged = parse_table_snapshot(&reload, 200)
        .and_then(|loaded| validate_exact_snapshot(before, &loaded));
    Ok(all_results([&conflict, &unchanged]))
}

async fn verify_required_final_state(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &CommitFixture,
    expected: &TableSnapshot,
) -> Result<std::result::Result<(), String>> {
    let load = recorder
        .attempt(
            "load-required-final-state",
            Some(LOAD_CAPABILITY),
            Method::GET,
            routes.table(&fixture.namespace, &fixture.table)?,
            None,
        )
        .await;
    Ok(parse_table_snapshot(&load, 200).and_then(|loaded| {
        validate_exact_snapshot(expected, &loaded)?;
        validate_schema_one(&loaded)?;
        if loaded.properties.get(STATE_PROPERTY).map(String::as_str) != Some("requirement-accepted")
        {
            return Err(format!(
                "required final state does not retain `{STATE_PROPERTY}`"
            ));
        }
        if loaded.properties.contains_key(STALE_PROPERTY) {
            return Err(format!(
                "required final state contains rejected property `{STALE_PROPERTY}`"
            ));
        }
        Ok(())
    }))
}

async fn execute_optional_idempotency(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &CommitFixture,
    before: &TableSnapshot,
    idempotency: CommitIdempotency<'_>,
    facts: &mut CommitFacts,
) -> Result<()> {
    if !facts.stale_rejection.passed() {
        let reason = facts
            .stale_rejection
            .explanation("stale requirement rejection did not pass");
        skip_optional_commits(recorder, &reason);
        facts.exact_replay = Fact::NotEvaluated(reason.clone());
        facts.content_binding = Fact::NotEvaluated(reason);
        return Ok(());
    }
    if !idempotency.advertisement.advertised() {
        let reason = idempotency.advertisement.unavailable_reason();
        skip_optional_commits(recorder, &reason);
        facts.exact_replay = Fact::NotEvaluated(reason.clone());
        facts.content_binding = Fact::NotEvaluated(reason);
        return Ok(());
    }
    if let Some(reason) = &idempotency.operations.exact_retry_limitation {
        skip_optional_commits(recorder, reason);
        facts.exact_replay = Fact::NotEvaluated(reason.clone());
        facts.content_binding =
            Fact::NotEvaluated(format!("content binding depends on exact retry: {reason}"));
        return Ok(());
    }

    let Some(key) = idempotency.key else {
        let reason = "idempotency key was not generated for an advertised exact-retry check";
        skip_optional_commits(recorder, reason);
        facts.exact_replay = Fact::Fail(reason.to_owned());
        facts.content_binding = Fact::NotEvaluated(reason.to_owned());
        return Ok(());
    };
    let exact_snapshot = attempt_exact_replay(recorder, routes, fixture, before, key).await?;
    facts.exact_replay =
        Fact::from_result(exact_snapshot.as_ref().map(|_| ()).map_err(Clone::clone));

    if let Some(reason) = &idempotency.operations.content_binding_limitation {
        recorder.skip(
            "commit-idempotency-drift",
            Some(CONTENT_BINDING_CAPABILITY),
            reason,
        );
        recorder.skip(
            "reload-after-idempotency-drift",
            Some(CONTENT_BINDING_CAPABILITY),
            reason,
        );
        facts.content_binding = Fact::NotEvaluated(reason.clone());
    } else if let Ok(exact_snapshot) = exact_snapshot {
        facts.content_binding = Fact::from_result(
            reject_idempotency_drift(recorder, routes, fixture, &exact_snapshot, key).await?,
        );
    } else {
        let reason = facts
            .exact_replay
            .explanation("exact idempotency replay did not pass");
        recorder.skip(
            "commit-idempotency-drift",
            Some(CONTENT_BINDING_CAPABILITY),
            &reason,
        );
        recorder.skip(
            "reload-after-idempotency-drift",
            Some(CONTENT_BINDING_CAPABILITY),
            &reason,
        );
        facts.content_binding = Fact::NotEvaluated(reason);
    }
    Ok(())
}

async fn attempt_exact_replay(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &CommitFixture,
    before: &TableSnapshot,
    key: &IdempotencyKey,
) -> Result<std::result::Result<TableSnapshot, String>> {
    let body = property_commit_body(&before.uuid, 1, RETRY_PROPERTY, "accepted-once");
    let first = recorder
        .attempt_idempotent(
            "commit-idempotent-first",
            Some(EXACT_RETRY_CAPABILITY),
            Method::POST,
            routes.table(&fixture.namespace, &fixture.table)?,
            Some(body.clone()),
            key,
        )
        .await;
    let first_reload = recorder
        .attempt(
            "reload-after-idempotent-first",
            Some(EXACT_RETRY_CAPABILITY),
            Method::GET,
            routes.table(&fixture.namespace, &fixture.table)?,
            None,
        )
        .await;
    let replay = recorder
        .attempt_idempotent(
            "commit-idempotent-replay",
            Some(EXACT_RETRY_CAPABILITY),
            Method::POST,
            routes.table(&fixture.namespace, &fixture.table)?,
            Some(body),
            key,
        )
        .await;
    let replay_reload = recorder
        .attempt(
            "reload-after-idempotent-replay",
            Some(EXACT_RETRY_CAPABILITY),
            Method::GET,
            routes.table(&fixture.namespace, &fixture.table)?,
            None,
        )
        .await;

    Ok(parse_table_snapshot(&first, 200).and_then(|committed| {
        validate_identity_and_pointer_transition(before, &committed)?;
        let mut expected_properties = scenario_properties(&before.properties);
        expected_properties.insert(RETRY_PROPERTY.to_owned(), "accepted-once".to_owned());
        if committed.schema != before.schema
            || committed.last_column_id != before.last_column_id
            || scenario_properties(&committed.properties) != expected_properties
        {
            return Err(
                "first idempotent commit did not apply exactly one property update".to_owned(),
            );
        }
        let first_loaded = parse_table_snapshot(&first_reload, 200)?;
        validate_exact_snapshot(&committed, &first_loaded)?;
        let replayed = parse_table_snapshot(&replay, 200)?;
        validate_exact_snapshot(&committed, &replayed)?;
        let replay_loaded = parse_table_snapshot(&replay_reload, 200)?;
        validate_exact_snapshot(&committed, &replay_loaded)?;
        Ok(replay_loaded)
    }))
}

async fn reject_idempotency_drift(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &CommitFixture,
    before: &TableSnapshot,
    key: &IdempotencyKey,
) -> Result<std::result::Result<(), String>> {
    let drift = recorder
        .attempt_idempotent(
            "commit-idempotency-drift",
            Some(CONTENT_BINDING_CAPABILITY),
            Method::POST,
            routes.table(&fixture.namespace, &fixture.table)?,
            Some(property_commit_body(
                &before.uuid,
                1,
                RETRY_PROPERTY,
                "drifted-must-not-apply",
            )),
            key,
        )
        .await;
    let reload = recorder
        .attempt(
            "reload-after-idempotency-drift",
            Some(CONTENT_BINDING_CAPABILITY),
            Method::GET,
            routes.table(&fixture.namespace, &fixture.table)?,
            None,
        )
        .await;
    let conflict = validate_spec_error_response(&drift, 409).map(|_| ());
    let unchanged = parse_table_snapshot(&reload, 200)
        .and_then(|loaded| validate_exact_snapshot(before, &loaded));
    Ok(all_results([&conflict, &unchanged]))
}

async fn cleanup_fixture(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &CommitFixture,
) -> Result<std::result::Result<(), String>> {
    let drop_table = recorder
        .attempt(
            "cleanup-drop-table",
            Some(DROP_CAPABILITY),
            Method::DELETE,
            table_drop_url(routes, fixture)?,
            None,
        )
        .await;
    let load_table = recorder
        .attempt(
            "cleanup-verify-table-absent",
            Some(LOAD_CAPABILITY),
            Method::GET,
            routes.table(&fixture.namespace, &fixture.table)?,
            None,
        )
        .await;
    let drop_namespace = recorder
        .attempt(
            "cleanup-drop-namespace",
            Some(NAMESPACE_DROP_CAPABILITY),
            Method::DELETE,
            routes.namespace(&fixture.namespace)?,
            None,
        )
        .await;
    let load_namespace = recorder
        .attempt(
            "cleanup-verify-namespace-absent",
            Some(NAMESPACE_LOAD_CAPABILITY),
            Method::GET,
            routes.namespace(&fixture.namespace)?,
            None,
        )
        .await;
    let drop_table = validate_status(&drop_table, &[204, 404]);
    let load_table = validate_error_response(&load_table, 404, "NoSuchTableException");
    let drop_namespace = validate_status(&drop_namespace, &[204, 404]);
    let load_namespace = validate_error_response(&load_namespace, 404, "NoSuchNamespaceException");
    Ok(all_results([
        &drop_table,
        &load_table,
        &drop_namespace,
        &load_namespace,
    ]))
}

fn validate_initial_snapshot(
    snapshot: TableSnapshot,
    requested_location: Option<&str>,
) -> std::result::Result<TableSnapshot, String> {
    validate_schema_zero(&snapshot)?;
    if snapshot.last_column_id != Some(1) {
        return Err(format!(
            "initial last-column-id {:?} does not match 1",
            snapshot.last_column_id
        ));
    }
    if let Some(requested_location) = requested_location {
        if snapshot.location != requested_location {
            return Err(format!(
                "created table location `{}` does not preserve `{requested_location}`",
                snapshot.location
            ));
        }
    }
    for (key, expected) in [
        (OWNER_PROPERTY, "catalog-bench"),
        (STATE_PROPERTY, "initial"),
    ] {
        if snapshot.properties.get(key).map(String::as_str) != Some(expected) {
            return Err(format!(
                "initial metadata does not preserve property `{key}`"
            ));
        }
    }
    Ok(snapshot)
}

fn validate_property_transition(
    before: &TableSnapshot,
    commit: &Observation,
    reload: &Observation,
    property: &str,
    value: &str,
) -> std::result::Result<TableSnapshot, String> {
    let committed = parse_table_snapshot(commit, 200)?;
    validate_identity_and_pointer_transition(before, &committed)?;
    if committed.schema != before.schema || committed.last_column_id != before.last_column_id {
        return Err("property commit changed schema metadata".to_owned());
    }
    let mut expected_properties = scenario_properties(&before.properties);
    expected_properties.insert(property.to_owned(), value.to_owned());
    if scenario_properties(&committed.properties) != expected_properties {
        return Err(format!(
            "property commit did not produce the exact `{property}` transition"
        ));
    }
    let loaded = parse_table_snapshot(reload, 200)?;
    validate_exact_snapshot(&committed, &loaded)?;
    Ok(loaded)
}

fn scenario_properties(properties: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    properties
        .iter()
        .filter(|(key, _)| key.starts_with("catalog-bench.") || key.starts_with("c1-06."))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn validate_identity_and_pointer_transition(
    before: &TableSnapshot,
    after: &TableSnapshot,
) -> std::result::Result<(), String> {
    if after.uuid != before.uuid {
        return Err("commit changed the table UUID".to_owned());
    }
    if after.location != before.location {
        return Err("commit changed the table location".to_owned());
    }
    if after.metadata_location == before.metadata_location {
        return Err("commit did not advance the metadata location".to_owned());
    }
    Ok(())
}

fn validate_exact_snapshot(
    expected: &TableSnapshot,
    actual: &TableSnapshot,
) -> std::result::Result<(), String> {
    if actual == expected {
        Ok(())
    } else {
        Err("table snapshot differs from the exact expected metadata state".to_owned())
    }
}

fn validate_schema_zero(snapshot: &TableSnapshot) -> std::result::Result<(), String> {
    validate_schema(&snapshot.schema, 0, &[(1, "value", false, json!("long"))])
}

fn validate_schema_one(snapshot: &TableSnapshot) -> std::result::Result<(), String> {
    if snapshot.last_column_id != Some(2) {
        return Err(format!(
            "transitioned last-column-id {:?} does not match 2",
            snapshot.last_column_id
        ));
    }
    validate_schema(
        &snapshot.schema,
        1,
        &[
            (1, "value", false, json!("long")),
            (2, "note", false, json!("string")),
        ],
    )
}

fn validate_schema(
    schema: &TableSchema,
    schema_id: i32,
    expected_fields: &[(i32, &str, bool, Value)],
) -> std::result::Result<(), String> {
    if schema.r#type != "struct" || schema.schema_id != Some(schema_id) {
        return Err(format!("current schema is not struct schema {schema_id}"));
    }
    if schema.fields.len() != expected_fields.len() {
        return Err(format!(
            "schema {schema_id} has {} fields instead of {}",
            schema.fields.len(),
            expected_fields.len()
        ));
    }
    for (actual, (id, name, required, field_type)) in schema.fields.iter().zip(expected_fields) {
        if actual.id != *id
            || actual.name != *name
            || actual.required != *required
            || actual.field_type != *field_type
        {
            return Err(format!(
                "schema {schema_id} does not preserve field `{name}`"
            ));
        }
    }
    Ok(())
}

fn property_commit_body(uuid: &str, schema_id: i32, property: &str, value: &str) -> Value {
    json!({
        "requirements": [
            {"type": "assert-table-uuid", "uuid": uuid},
            {"type": "assert-current-schema-id", "current-schema-id": schema_id}
        ],
        "updates": [{
            "action": "set-properties",
            "updates": {(property): value}
        }]
    })
}

fn initial_schema() -> Value {
    json!({
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {"id": 1, "name": "value", "required": false, "type": "long"}
        ]
    })
}

fn transitioned_schema() -> Value {
    json!({
        "type": "struct",
        "schema-id": 1,
        "fields": [
            {"id": 1, "name": "value", "required": false, "type": "long"},
            {"id": 2, "name": "note", "required": false, "type": "string"}
        ]
    })
}

fn table_drop_url(routes: &CatalogRoutes, fixture: &CommitFixture) -> Result<Url> {
    let mut url = routes.table(&fixture.namespace, &fixture.table)?;
    url.query_pairs_mut().append_pair("purgeRequested", "false");
    Ok(url)
}

fn skip_after_preflight(recorder: &mut OperationRecorder<'_>, reason: &str) {
    recorder.skip(
        "create-namespace",
        Some(NAMESPACE_CREATE_CAPABILITY),
        reason,
    );
    recorder.skip("create-table", Some(CREATE_CAPABILITY), reason);
    recorder.skip("load-initial-table", Some(LOAD_CAPABILITY), reason);
    skip_required_commits(recorder, reason);
    skip_optional_commits(recorder, reason);
    skip_cleanup(recorder, reason);
}

fn skip_required_commits(recorder: &mut OperationRecorder<'_>, reason: &str) {
    for (id, capability) in [
        ("commit-current-requirements", UPDATE_CAPABILITY),
        ("reload-after-current-requirements", LOAD_CAPABILITY),
        ("commit-schema-transition", UPDATE_CAPABILITY),
        ("reload-after-schema-transition", LOAD_CAPABILITY),
        ("commit-stale-requirement", UPDATE_CAPABILITY),
        ("reload-after-stale-requirement", LOAD_CAPABILITY),
        ("load-required-final-state", LOAD_CAPABILITY),
    ] {
        recorder.skip(id, Some(capability), reason);
    }
}

fn skip_after_current_commit(recorder: &mut OperationRecorder<'_>, reason: &str) {
    for (id, capability) in [
        ("commit-schema-transition", UPDATE_CAPABILITY),
        ("reload-after-schema-transition", LOAD_CAPABILITY),
        ("commit-stale-requirement", UPDATE_CAPABILITY),
        ("reload-after-stale-requirement", LOAD_CAPABILITY),
        ("load-required-final-state", LOAD_CAPABILITY),
    ] {
        recorder.skip(id, Some(capability), reason);
    }
    skip_optional_commits(recorder, reason);
}

fn skip_after_schema_commit(recorder: &mut OperationRecorder<'_>, reason: &str) {
    for (id, capability) in [
        ("commit-stale-requirement", UPDATE_CAPABILITY),
        ("reload-after-stale-requirement", LOAD_CAPABILITY),
        ("load-required-final-state", LOAD_CAPABILITY),
    ] {
        recorder.skip(id, Some(capability), reason);
    }
    skip_optional_commits(recorder, reason);
}

fn skip_optional_commits(recorder: &mut OperationRecorder<'_>, reason: &str) {
    for (id, capability) in [
        ("commit-idempotent-first", EXACT_RETRY_CAPABILITY),
        ("reload-after-idempotent-first", EXACT_RETRY_CAPABILITY),
        ("commit-idempotent-replay", EXACT_RETRY_CAPABILITY),
        ("reload-after-idempotent-replay", EXACT_RETRY_CAPABILITY),
        ("commit-idempotency-drift", CONTENT_BINDING_CAPABILITY),
        ("reload-after-idempotency-drift", CONTENT_BINDING_CAPABILITY),
    ] {
        recorder.skip(id, Some(capability), reason);
    }
}

fn skip_cleanup(recorder: &mut OperationRecorder<'_>, reason: &str) {
    for (id, capability) in [
        ("cleanup-drop-table", DROP_CAPABILITY),
        ("cleanup-verify-table-absent", LOAD_CAPABILITY),
        ("cleanup-drop-namespace", NAMESPACE_DROP_CAPABILITY),
        ("cleanup-verify-namespace-absent", NAMESPACE_LOAD_CAPABILITY),
    ] {
        recorder.skip(id, Some(capability), reason);
    }
}
