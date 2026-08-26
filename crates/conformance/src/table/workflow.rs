use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use reqwest::Method;
use serde::Deserialize;
use serde_json::{json, Value};
use url::Url;

use super::{
    OptionalTableOperations, TableFacts, TableFixture, TableIdentifier, TablePaginationTranscript,
    CREATE_CAPABILITY, DROP_CAPABILITY, LIST_CAPABILITY, LOAD_CAPABILITY, REGISTER_CAPABILITY,
    RENAME_CAPABILITY, UPDATE_CAPABILITY,
};
use crate::iceberg::{CatalogRoutes, NamespaceIdentifier};
use crate::operation::{
    all_results, parse_json_response, validate_error_response, validate_status, Fact, Observation,
    OperationRecorder,
};

const NAMESPACE_CREATE_CAPABILITY: &str = "iceberg-rest.namespace.create";
const NAMESPACE_LOAD_CAPABILITY: &str = "iceberg-rest.namespace.load";
const NAMESPACE_DROP_CAPABILITY: &str = "iceberg-rest.namespace.drop";
const ERROR_SHAPE_CAPABILITY: &str = "iceberg-rest.error.spec-shape";
const PAGE_SIZE: usize = 1;
const MAXIMUM_PAGES: usize = 64;

const OWNER_PROPERTY: &str = "catalog-bench.owner";
const REMOVE_PROPERTY: &str = "c1-05.remove";
const STATE_PROPERTY: &str = "c1-05.state";

pub(super) async fn execute_table_workflow(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &TableFixture,
    optional_operations: &OptionalTableOperations,
    facts: &mut TableFacts,
) -> Result<TablePaginationTranscript> {
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
        skip_mutating_workflow(recorder, &reason);
        facts.skip_mutating_behavior(&reason);
        facts.missing_namespace = Fact::from_result(
            probe_missing_namespace(recorder, routes, &fixture.missing_namespace).await?,
        );
        return Ok(TablePaginationTranscript::NotEvaluated { reason });
    }

    let namespace_create = recorder
        .attempt(
            "create-namespace",
            Some(NAMESPACE_CREATE_CAPABILITY),
            Method::POST,
            routes.namespace_collection()?,
            Some(json!({
                "namespace": fixture.namespace.parts(),
                "properties": {"catalog-bench.owner": "catalog-bench"}
            })),
        )
        .await;
    let namespace_created = validate_namespace_response(&namespace_create, &fixture.namespace);
    facts.fixture_namespace = Fact::from_result(namespace_created.clone());

    if namespace_created.is_err() {
        let reason = "fixture namespace creation did not pass";
        skip_table_operations(recorder, reason);
        facts.skip_table_behavior(reason);
        facts.missing_table =
            Fact::from_result(probe_missing_table(recorder, routes, &fixture.missing).await?);
        facts.missing_namespace = Fact::from_result(
            probe_missing_namespace(recorder, routes, &fixture.missing_namespace).await?,
        );
        facts.cleanup = Fact::from_result(cleanup_fixture(recorder, routes, fixture).await?);
        return Ok(TablePaginationTranscript::NotEvaluated {
            reason: reason.to_owned(),
        });
    }

    let primary_create = create_table(recorder, routes, &fixture.primary, "create-primary").await?;
    let sibling_create = create_table(recorder, routes, &fixture.sibling, "create-sibling").await?;
    let primary_snapshot = validate_initial_snapshot(&primary_create);
    let sibling_snapshot = validate_initial_snapshot(&sibling_create);
    let both_created = primary_snapshot.is_ok() && sibling_snapshot.is_ok();
    let create_result = all_results([&primary_snapshot, &sibling_snapshot]).and_then(|()| {
        if primary_snapshot.as_ref().expect("validated above").uuid
            == sibling_snapshot.as_ref().expect("validated above").uuid
        {
            Err("created tables returned the same table UUID".to_owned())
        } else {
            Ok(())
        }
    });
    facts.create = Fact::from_result(create_result);

    if both_created {
        let listing = recorder
            .attempt(
                "list-tables",
                Some(LIST_CAPABILITY),
                Method::GET,
                routes.table_collection(&fixture.namespace)?,
                None,
            )
            .await;
        facts.list = Fact::from_result(validate_table_listing(
            &listing,
            [&fixture.primary, &fixture.sibling],
        ));
    } else {
        let reason = "both table creates must pass before ordinary listing";
        recorder.skip("list-tables", Some(LIST_CAPABILITY), reason);
        facts.list = Fact::NotEvaluated(reason.to_owned());
    }

    let primary_load = load_created_table(
        recorder,
        routes,
        &fixture.primary,
        "load-primary",
        primary_snapshot.as_ref().ok(),
    )
    .await?;
    let sibling_load = load_created_table(
        recorder,
        routes,
        &fixture.sibling,
        "load-sibling",
        sibling_snapshot.as_ref().ok(),
    )
    .await?;
    let both_loaded = primary_load.is_ok() && sibling_load.is_ok();
    facts.load = if both_created {
        Fact::from_result(all_results([&primary_load, &sibling_load]))
    } else {
        Fact::NotEvaluated("both table creates must pass before load round-trip".to_owned())
    };

    let pagination = if both_created {
        let (pagination, result) = traverse_pages(recorder, routes, fixture).await?;
        facts.pagination = Fact::from_result(result);
        pagination
    } else {
        let reason = "both table creates must pass before pagination".to_owned();
        recorder.skip("list-page-001", Some(LIST_CAPABILITY), &reason);
        facts.pagination = Fact::NotEvaluated(reason.clone());
        TablePaginationTranscript::NotEvaluated { reason }
    };

    let updated_snapshot = if both_loaded {
        let before = primary_load
            .as_ref()
            .expect("both_loaded guarantees a primary snapshot");
        let update = recorder
            .attempt(
                "update-primary-properties",
                Some(UPDATE_CAPABILITY),
                Method::POST,
                routes.table(&fixture.namespace, &fixture.primary.name)?,
                Some(json!({
                    "requirements": [],
                    "updates": [
                        {
                            "action": "set-properties",
                            "updates": {(STATE_PROPERTY): "after"}
                        },
                        {
                            "action": "remove-properties",
                            "removals": [REMOVE_PROPERTY]
                        }
                    ]
                })),
            )
            .await;
        let reload = recorder
            .attempt(
                "reload-primary-after-update",
                Some(UPDATE_CAPABILITY),
                Method::GET,
                routes.table(&fixture.namespace, &fixture.primary.name)?,
                None,
            )
            .await;
        match validate_update(before, &update, &reload) {
            Ok(snapshot) => {
                facts.update = Fact::Pass;
                Some(snapshot)
            }
            Err(error) => {
                facts.update = Fact::Fail(error);
                None
            }
        }
    } else {
        let reason = "both table loads must pass before update";
        recorder.skip("update-primary-properties", Some(UPDATE_CAPABILITY), reason);
        recorder.skip(
            "reload-primary-after-update",
            Some(UPDATE_CAPABILITY),
            reason,
        );
        facts.update = Fact::NotEvaluated(reason.to_owned());
        None
    };

    if both_created {
        let duplicate = create_table(
            recorder,
            routes,
            &fixture.primary,
            "create-primary-duplicate",
        )
        .await?;
        facts.duplicate = Fact::from_result(validate_error_response(
            &duplicate,
            409,
            "AlreadyExistsException",
        ));
    } else {
        let reason = "primary table creation did not pass";
        recorder.skip("create-primary-duplicate", Some(CREATE_CAPABILITY), reason);
        facts.duplicate = Fact::NotEvaluated(reason.to_owned());
    }

    facts.missing_table =
        Fact::from_result(probe_missing_table(recorder, routes, &fixture.missing).await?);
    facts.missing_namespace = Fact::from_result(
        probe_missing_namespace(recorder, routes, &fixture.missing_namespace).await?,
    );

    facts.rename = if let Some(reason) = &optional_operations.rename_limitation {
        for id in [
            "rename-primary",
            "load-primary-after-rename",
            "load-renamed-after-rename",
        ] {
            recorder.skip(id, Some(RENAME_CAPABILITY), reason);
        }
        Fact::NotEvaluated(reason.clone())
    } else if let Some(before) = updated_snapshot.as_ref() {
        Fact::from_result(attempt_rename(recorder, routes, fixture, before).await?)
    } else {
        let reason = "primary table update did not pass";
        for id in [
            "rename-primary",
            "load-primary-after-rename",
            "load-renamed-after-rename",
        ] {
            recorder.skip(id, Some(RENAME_CAPABILITY), reason);
        }
        Fact::NotEvaluated(reason.to_owned())
    };

    let sibling_dropped = if both_loaded {
        let snapshot = sibling_load
            .as_ref()
            .expect("both_loaded guarantees a sibling snapshot");
        let result = drop_sibling(recorder, routes, &fixture.sibling).await?;
        let passed = result.is_ok();
        facts.drop_table = Fact::from_result(result);
        passed.then_some(snapshot)
    } else {
        let reason = "both table loads must pass before drop";
        recorder.skip("drop-sibling-without-purge", Some(DROP_CAPABILITY), reason);
        recorder.skip("load-sibling-after-drop", Some(DROP_CAPABILITY), reason);
        facts.drop_table = Fact::NotEvaluated(reason.to_owned());
        None
    };

    facts.register = if let Some(reason) = &optional_operations.register_limitation {
        recorder.skip(
            "register-sibling-metadata",
            Some(REGISTER_CAPABILITY),
            reason,
        );
        recorder.skip(
            "load-registered-after-register",
            Some(REGISTER_CAPABILITY),
            reason,
        );
        Fact::NotEvaluated(reason.clone())
    } else if let Some(source) = sibling_dropped {
        Fact::from_result(attempt_register(recorder, routes, &fixture.registered, source).await?)
    } else {
        let reason = "sibling drop did not pass";
        recorder.skip(
            "register-sibling-metadata",
            Some(REGISTER_CAPABILITY),
            reason,
        );
        recorder.skip(
            "load-registered-after-register",
            Some(REGISTER_CAPABILITY),
            reason,
        );
        Fact::NotEvaluated(reason.to_owned())
    };

    facts.cleanup = Fact::from_result(cleanup_fixture(recorder, routes, fixture).await?);
    Ok(pagination)
}

async fn create_table(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    identifier: &TableIdentifier,
    operation_id: &str,
) -> Result<Observation> {
    Ok(recorder
        .attempt(
            operation_id,
            Some(CREATE_CAPABILITY),
            Method::POST,
            routes.table_collection(&identifier.namespace)?,
            Some(create_table_request(&identifier.name)),
        )
        .await)
}

fn create_table_request(name: &str) -> Value {
    json!({
        "name": name,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "value", "required": false, "type": "long"}
            ]
        },
        "stage-create": false,
        "properties": {
            (OWNER_PROPERTY): "catalog-bench",
            (REMOVE_PROPERTY): "before",
            (STATE_PROPERTY): "before"
        }
    })
}

async fn load_created_table(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    identifier: &TableIdentifier,
    operation_id: &str,
    created: Option<&TableSnapshot>,
) -> Result<std::result::Result<TableSnapshot, String>> {
    let Some(created) = created else {
        let reason = format!("{} table creation did not pass", identifier.name);
        recorder.skip(operation_id, Some(LOAD_CAPABILITY), &reason);
        return Ok(Err(reason));
    };
    let observation = recorder
        .attempt(
            operation_id,
            Some(LOAD_CAPABILITY),
            Method::GET,
            routes.table(&identifier.namespace, &identifier.name)?,
            None,
        )
        .await;
    Ok(parse_snapshot(&observation, 200)
        .and_then(|loaded| validate_same_snapshot(created, &loaded).map(|()| loaded)))
}

async fn probe_missing_table(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    identifier: &TableIdentifier,
) -> Result<std::result::Result<(), String>> {
    let observation = recorder
        .attempt(
            "load-missing-table",
            Some(ERROR_SHAPE_CAPABILITY),
            Method::GET,
            routes.table(&identifier.namespace, &identifier.name)?,
            None,
        )
        .await;
    Ok(validate_error_response(
        &observation,
        404,
        "NoSuchTableException",
    ))
}

async fn probe_missing_namespace(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    namespace: &NamespaceIdentifier,
) -> Result<std::result::Result<(), String>> {
    let observation = recorder
        .attempt(
            "list-tables-missing-namespace",
            Some(ERROR_SHAPE_CAPABILITY),
            Method::GET,
            routes.table_collection(namespace)?,
            None,
        )
        .await;
    Ok(validate_error_response(
        &observation,
        404,
        "NoSuchNamespaceException",
    ))
}

async fn attempt_rename(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &TableFixture,
    before: &TableSnapshot,
) -> Result<std::result::Result<(), String>> {
    let rename = recorder
        .attempt(
            "rename-primary",
            Some(RENAME_CAPABILITY),
            Method::POST,
            routes.table_rename()?,
            Some(json!({
                "source": fixture.primary,
                "destination": fixture.renamed
            })),
        )
        .await;
    let source = recorder
        .attempt(
            "load-primary-after-rename",
            Some(RENAME_CAPABILITY),
            Method::GET,
            routes.table(&fixture.namespace, &fixture.primary.name)?,
            None,
        )
        .await;
    let destination = recorder
        .attempt(
            "load-renamed-after-rename",
            Some(RENAME_CAPABILITY),
            Method::GET,
            routes.table(&fixture.namespace, &fixture.renamed.name)?,
            None,
        )
        .await;

    Ok(validate_status(&rename, &[204])
        .and_then(|()| validate_error_response(&source, 404, "NoSuchTableException"))
        .and_then(|()| parse_snapshot(&destination, 200))
        .and_then(|renamed| validate_same_snapshot(before, &renamed)))
}

async fn drop_sibling(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    sibling: &TableIdentifier,
) -> Result<std::result::Result<(), String>> {
    let drop = recorder
        .attempt(
            "drop-sibling-without-purge",
            Some(DROP_CAPABILITY),
            Method::DELETE,
            table_drop_url(routes, sibling)?,
            None,
        )
        .await;
    let load = recorder
        .attempt(
            "load-sibling-after-drop",
            Some(DROP_CAPABILITY),
            Method::GET,
            routes.table(&sibling.namespace, &sibling.name)?,
            None,
        )
        .await;
    Ok(validate_status(&drop, &[204])
        .and_then(|()| validate_error_response(&load, 404, "NoSuchTableException")))
}

async fn attempt_register(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    registered: &TableIdentifier,
    source: &TableSnapshot,
) -> Result<std::result::Result<(), String>> {
    let register = recorder
        .attempt(
            "register-sibling-metadata",
            Some(REGISTER_CAPABILITY),
            Method::POST,
            routes.table_register(&registered.namespace)?,
            Some(json!({
                "name": registered.name,
                "metadata-location": source.metadata_location,
                "overwrite": false
            })),
        )
        .await;
    let load = recorder
        .attempt(
            "load-registered-after-register",
            Some(REGISTER_CAPABILITY),
            Method::GET,
            routes.table(&registered.namespace, &registered.name)?,
            None,
        )
        .await;
    Ok(parse_snapshot(&register, 200)
        .and_then(|snapshot| validate_same_snapshot(source, &snapshot))
        .and_then(|()| parse_snapshot(&load, 200))
        .and_then(|snapshot| validate_same_snapshot(source, &snapshot)))
}

async fn cleanup_fixture(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &TableFixture,
) -> Result<std::result::Result<(), String>> {
    let mut results = Vec::new();
    for identifier in fixture.candidates() {
        let drop_id = format!("cleanup-drop-{}", identifier.name);
        let observation = recorder
            .attempt(
                drop_id,
                Some(DROP_CAPABILITY),
                Method::DELETE,
                table_drop_url(routes, identifier)?,
                None,
            )
            .await;
        results.push(validate_status(&observation, &[204, 404]));
    }
    for identifier in fixture.candidates() {
        let load_id = format!("cleanup-verify-{}-absent", identifier.name);
        let observation = recorder
            .attempt(
                load_id,
                Some(DROP_CAPABILITY),
                Method::GET,
                routes.table(&identifier.namespace, &identifier.name)?,
                None,
            )
            .await;
        results.push(validate_error_response(
            &observation,
            404,
            "NoSuchTableException",
        ));
    }
    let drop_namespace = recorder
        .attempt(
            "cleanup-drop-namespace",
            Some(NAMESPACE_DROP_CAPABILITY),
            Method::DELETE,
            routes.namespace(&fixture.namespace)?,
            None,
        )
        .await;
    results.push(validate_status(&drop_namespace, &[204, 404]));
    let load_namespace = recorder
        .attempt(
            "cleanup-verify-namespace-absent",
            Some(NAMESPACE_LOAD_CAPABILITY),
            Method::GET,
            routes.namespace(&fixture.namespace)?,
            None,
        )
        .await;
    results.push(validate_error_response(
        &load_namespace,
        404,
        "NoSuchNamespaceException",
    ));
    Ok(all_results(results.iter()))
}

fn table_drop_url(routes: &CatalogRoutes, identifier: &TableIdentifier) -> Result<Url> {
    let mut url = routes.table(&identifier.namespace, &identifier.name)?;
    url.query_pairs_mut().append_pair("purgeRequested", "false");
    Ok(url)
}

async fn traverse_pages(
    recorder: &mut OperationRecorder<'_>,
    routes: &CatalogRoutes,
    fixture: &TableFixture,
) -> Result<(TablePaginationTranscript, std::result::Result<(), String>)> {
    let mut token = String::new();
    let mut tokens = BTreeSet::new();
    let mut identifiers = BTreeSet::new();

    for page_index in 1..=MAXIMUM_PAGES {
        let observation = recorder
            .attempt(
                format!("list-page-{page_index:03}"),
                Some(LIST_CAPABILITY),
                Method::GET,
                routes.table_page(&fixture.namespace, &token, PAGE_SIZE)?,
                None,
            )
            .await;
        let page = match parse_list_response(&observation) {
            Ok(page) => page,
            Err(error) => return Ok(pagination_failure(error)),
        };
        let page_identifiers = match parse_identifiers(page.identifiers) {
            Ok(identifiers) => identifiers,
            Err(error) => return Ok(pagination_failure(error)),
        };
        if page_identifiers.len() > PAGE_SIZE {
            if page_index == 1 && page.next_page_token.is_none() {
                let result = validate_complete_table_set(
                    &page_identifiers,
                    [&fixture.primary, &fixture.sibling],
                );
                return Ok((
                    TablePaginationTranscript::UnpaginatedFallback {
                        unique_tables: page_identifiers.len(),
                    },
                    result,
                ));
            }
            return Ok(pagination_failure(format!(
                "page {page_index} returned {} tables above pageSize {PAGE_SIZE}",
                page_identifiers.len()
            )));
        }
        for identifier in page_identifiers {
            if !identifiers.insert(identifier.clone()) {
                return Ok(pagination_failure(format!(
                    "pagination returned duplicate table {identifier:?}"
                )));
            }
        }
        match page.next_page_token {
            Some(next) if next.is_empty() => {
                return Ok(pagination_failure(
                    "pagination returned an empty next-page-token".to_owned(),
                ));
            }
            Some(next) if !tokens.insert(next.clone()) => {
                return Ok(pagination_failure(
                    "pagination repeated a next-page-token".to_owned(),
                ));
            }
            Some(next) => token = next,
            None => {
                let result =
                    validate_complete_table_set(&identifiers, [&fixture.primary, &fixture.sibling]);
                return Ok((
                    TablePaginationTranscript::Paginated {
                        pages: page_index,
                        unique_tables: identifiers.len(),
                    },
                    result,
                ));
            }
        }
    }
    Ok(pagination_failure(format!(
        "pagination exceeded the {MAXIMUM_PAGES}-page safety bound"
    )))
}

fn pagination_failure(
    explanation: String,
) -> (TablePaginationTranscript, std::result::Result<(), String>) {
    (
        TablePaginationTranscript::Failed {
            explanation: explanation.clone(),
        },
        Err(explanation),
    )
}

fn validate_namespace_response(
    observation: &Observation,
    expected: &NamespaceIdentifier,
) -> std::result::Result<(), String> {
    let response: NamespaceResponse = parse_json_response(observation, 200)?;
    let actual =
        NamespaceIdentifier::from_parts(response.namespace).map_err(|error| error.to_string())?;
    if actual == *expected {
        Ok(())
    } else {
        Err(format!(
            "response namespace {:?} does not match {:?}",
            actual.parts(),
            expected.parts()
        ))
    }
}

fn validate_initial_snapshot(
    observation: &Observation,
) -> std::result::Result<TableSnapshot, String> {
    let snapshot = parse_snapshot(observation, 200)?;
    for (key, expected) in [
        (OWNER_PROPERTY, "catalog-bench"),
        (REMOVE_PROPERTY, "before"),
        (STATE_PROPERTY, "before"),
    ] {
        if snapshot.properties.get(key).map(String::as_str) != Some(expected) {
            return Err(format!(
                "created metadata did not preserve requested property `{key}`"
            ));
        }
    }
    Ok(snapshot)
}

fn validate_update(
    before: &TableSnapshot,
    update: &Observation,
    reload: &Observation,
) -> std::result::Result<TableSnapshot, String> {
    let committed = parse_snapshot(update, 200)?;
    if committed.uuid != before.uuid {
        return Err("table update changed the table UUID".to_owned());
    }
    if committed.schema != before.schema {
        return Err("property-only update changed the current schema".to_owned());
    }
    if committed.metadata_location == before.metadata_location {
        return Err("table update did not advance the metadata location".to_owned());
    }
    if committed.properties.get(OWNER_PROPERTY).map(String::as_str) != Some("catalog-bench") {
        return Err(format!(
            "update did not preserve unmentioned property `{OWNER_PROPERTY}`"
        ));
    }
    if committed.properties.get(STATE_PROPERTY).map(String::as_str) != Some("after") {
        return Err(format!(
            "update did not set property `{STATE_PROPERTY}` to `after`"
        ));
    }
    if committed.properties.contains_key(REMOVE_PROPERTY) {
        return Err(format!(
            "update did not remove property `{REMOVE_PROPERTY}`"
        ));
    }
    let loaded = parse_snapshot(reload, 200)?;
    validate_same_snapshot(&committed, &loaded)?;
    if loaded.properties != committed.properties {
        return Err("reload property map differs from commit response".to_owned());
    }
    Ok(loaded)
}

fn validate_same_snapshot(
    expected: &TableSnapshot,
    actual: &TableSnapshot,
) -> std::result::Result<(), String> {
    if actual.uuid != expected.uuid {
        return Err(format!(
            "table UUID `{}` does not match `{}`",
            actual.uuid, expected.uuid
        ));
    }
    if actual.metadata_location != expected.metadata_location {
        return Err(format!(
            "metadata location `{}` does not match `{}`",
            actual.metadata_location, expected.metadata_location
        ));
    }
    if actual.schema != expected.schema {
        return Err("current table schema changed unexpectedly".to_owned());
    }
    Ok(())
}

fn parse_snapshot(
    observation: &Observation,
    status: u16,
) -> std::result::Result<TableSnapshot, String> {
    let response: LoadTableResult = parse_json_response(observation, status)?;
    let metadata_location = response
        .metadata_location
        .filter(|location| !location.trim().is_empty())
        .ok_or_else(|| {
            "committed table response omitted a nonempty metadata-location".to_owned()
        })?;
    if !(1..=3).contains(&response.metadata.format_version) {
        return Err(format!(
            "table format version {} is outside 1..=3",
            response.metadata.format_version
        ));
    }
    if response.metadata.table_uuid.trim().is_empty() {
        return Err("table metadata returned an empty table-uuid".to_owned());
    }
    let schema = current_schema(&response.metadata)?;
    validate_requested_schema(&schema)?;
    Ok(TableSnapshot {
        metadata_location,
        uuid: response.metadata.table_uuid,
        schema,
        properties: response.metadata.properties,
    })
}

fn current_schema(metadata: &TableMetadata) -> std::result::Result<TableSchema, String> {
    match metadata.current_schema_id {
        Some(current) => metadata
            .schemas
            .iter()
            .find(|schema| schema.schema_id == Some(current))
            .cloned()
            .ok_or_else(|| format!("current-schema-id {current} has no matching schema")),
        None if metadata.schemas.len() == 1 => Ok(metadata.schemas[0].clone()),
        None => Err("table metadata omitted an unambiguous current schema".to_owned()),
    }
}

fn validate_requested_schema(schema: &TableSchema) -> std::result::Result<(), String> {
    if schema.r#type != "struct" {
        return Err(format!(
            "current schema type `{}` is not `struct`",
            schema.r#type
        ));
    }
    let [field] = schema.fields.as_slice() else {
        return Err("current schema does not contain exactly one requested field".to_owned());
    };
    if field.id != 1
        || field.name != "value"
        || field.required
        || field.field_type != Value::String("long".to_owned())
    {
        return Err(
            "current schema does not preserve optional long field `value` (id 1)".to_owned(),
        );
    }
    Ok(())
}

fn validate_table_listing<'a>(
    observation: &Observation,
    expected: impl IntoIterator<Item = &'a TableIdentifier>,
) -> std::result::Result<(), String> {
    let response = parse_list_response(observation)?;
    let identifiers = parse_identifiers(response.identifiers)?;
    validate_complete_table_set(&identifiers, expected)
}

fn validate_complete_table_set<'a>(
    identifiers: &BTreeSet<TableIdentifier>,
    expected: impl IntoIterator<Item = &'a TableIdentifier>,
) -> std::result::Result<(), String> {
    let expected = expected.into_iter().cloned().collect::<BTreeSet<_>>();
    if identifiers == &expected {
        Ok(())
    } else {
        Err(format!(
            "listing returned {identifiers:?}, expected exactly {expected:?}"
        ))
    }
}

fn parse_list_response(
    observation: &Observation,
) -> std::result::Result<ListTablesResponse, String> {
    parse_json_response(observation, 200)
}

fn parse_identifiers(
    identifiers: Vec<WireTableIdentifier>,
) -> std::result::Result<BTreeSet<TableIdentifier>, String> {
    let input_count = identifiers.len();
    let parsed = identifiers
        .into_iter()
        .map(|identifier| {
            NamespaceIdentifier::from_parts(identifier.namespace)
                .map_err(|error| error.to_string())
                .and_then(|namespace| {
                    TableIdentifier::new(namespace, identifier.name)
                        .map_err(|error| error.to_string())
                })
        })
        .collect::<std::result::Result<BTreeSet<_>, _>>()?;
    if parsed.len() != input_count {
        return Err("listing contains duplicate table identifiers".to_owned());
    }
    Ok(parsed)
}

fn skip_mutating_workflow(recorder: &mut OperationRecorder<'_>, reason: &str) {
    recorder.skip(
        "create-namespace",
        Some(NAMESPACE_CREATE_CAPABILITY),
        reason,
    );
    skip_table_operations(recorder, reason);
    for identifier in ["primary", "renamed", "sibling", "registered"] {
        recorder.skip(
            format!("cleanup-drop-{identifier}"),
            Some(DROP_CAPABILITY),
            reason,
        );
    }
    for identifier in ["primary", "renamed", "sibling", "registered"] {
        recorder.skip(
            format!("cleanup-verify-{identifier}-absent"),
            Some(DROP_CAPABILITY),
            reason,
        );
    }
    recorder.skip(
        "cleanup-drop-namespace",
        Some(NAMESPACE_DROP_CAPABILITY),
        reason,
    );
    recorder.skip(
        "cleanup-verify-namespace-absent",
        Some(NAMESPACE_LOAD_CAPABILITY),
        reason,
    );
}

fn skip_table_operations(recorder: &mut OperationRecorder<'_>, reason: &str) {
    for (id, capability) in [
        ("create-primary", CREATE_CAPABILITY),
        ("create-sibling", CREATE_CAPABILITY),
        ("list-tables", LIST_CAPABILITY),
        ("load-primary", LOAD_CAPABILITY),
        ("load-sibling", LOAD_CAPABILITY),
        ("list-page-001", LIST_CAPABILITY),
        ("update-primary-properties", UPDATE_CAPABILITY),
        ("reload-primary-after-update", UPDATE_CAPABILITY),
        ("create-primary-duplicate", CREATE_CAPABILITY),
        ("rename-primary", RENAME_CAPABILITY),
        ("load-primary-after-rename", RENAME_CAPABILITY),
        ("load-renamed-after-rename", RENAME_CAPABILITY),
        ("drop-sibling-without-purge", DROP_CAPABILITY),
        ("load-sibling-after-drop", DROP_CAPABILITY),
        ("register-sibling-metadata", REGISTER_CAPABILITY),
        ("load-registered-after-register", REGISTER_CAPABILITY),
    ] {
        recorder.skip(id, Some(capability), reason);
    }
}

#[derive(Debug, Deserialize)]
struct NamespaceResponse {
    namespace: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct LoadTableResult {
    #[serde(rename = "metadata-location")]
    metadata_location: Option<String>,
    metadata: TableMetadata,
}

#[derive(Debug, Deserialize)]
struct TableMetadata {
    #[serde(rename = "format-version")]
    format_version: u8,
    #[serde(rename = "table-uuid")]
    table_uuid: String,
    #[serde(default)]
    properties: BTreeMap<String, String>,
    #[serde(default)]
    schemas: Vec<TableSchema>,
    #[serde(rename = "current-schema-id", default)]
    current_schema_id: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct TableSchema {
    r#type: String,
    #[serde(rename = "schema-id", default)]
    schema_id: Option<i32>,
    fields: Vec<TableField>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct TableField {
    id: i32,
    name: String,
    required: bool,
    #[serde(rename = "type")]
    field_type: Value,
}

#[derive(Clone)]
struct TableSnapshot {
    metadata_location: String,
    uuid: String,
    schema: TableSchema,
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct ListTablesResponse {
    #[serde(rename = "next-page-token", default)]
    next_page_token: Option<String>,
    #[serde(default)]
    identifiers: Vec<WireTableIdentifier>,
}

#[derive(Debug, Deserialize)]
struct WireTableIdentifier {
    namespace: Vec<String>,
    name: String,
}
