use std::collections::BTreeMap;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use url::Url;

use crate::iceberg::NamespaceIdentifier;
use crate::operation::{parse_json_response, Observation};

pub(crate) struct TableCreateLocations(Option<Url>);

impl TableCreateLocations {
    pub(crate) fn new(root: Option<&str>) -> Result<Self> {
        let root = root
            .map(Url::parse)
            .transpose()
            .context("parse adapter create-table location root")?;
        if root.as_ref().is_some_and(Url::cannot_be_a_base) {
            return Err(anyhow!(
                "adapter create-table location root cannot contain child table locations"
            ));
        }
        Ok(Self(root))
    }

    pub(crate) fn for_table(
        &self,
        namespace: &NamespaceIdentifier,
        name: &str,
    ) -> Result<Option<String>> {
        let Some(root) = &self.0 else {
            return Ok(None);
        };
        let mut location = root.clone();
        {
            let mut segments = location.path_segments_mut().map_err(|()| {
                anyhow!("adapter create-table location root cannot contain path segments")
            })?;
            segments.pop_if_empty();
            for part in namespace.parts() {
                segments.push(part);
            }
            segments.push(name);
        }
        Ok(Some(location.to_string()))
    }
}

pub(crate) fn committed_table_request(
    name: &str,
    location: Option<&str>,
    schema: Value,
    properties: BTreeMap<String, String>,
) -> Value {
    let mut request = json!({
        "name": name,
        "schema": schema,
        "stage-create": false,
        "properties": properties,
    });
    if let Some(location) = location {
        request["location"] = Value::String(location.to_owned());
    }
    request
}

pub(crate) fn validate_namespace_response(
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

pub(crate) fn parse_table_snapshot(
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
    if response.metadata.location.trim().is_empty() {
        return Err("table metadata returned an empty location".to_owned());
    }
    let schema = current_schema(&response.metadata)?;
    Ok(TableSnapshot {
        metadata_location,
        location: response.metadata.location,
        uuid: response.metadata.table_uuid,
        schema,
        last_column_id: response.metadata.last_column_id,
        properties: response.metadata.properties,
    })
}

pub(crate) fn validate_same_table_snapshot(
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
    if actual.location != expected.location {
        return Err(format!(
            "table location `{}` does not match `{}`",
            actual.location, expected.location
        ));
    }
    if actual.schema != expected.schema {
        return Err("current table schema changed unexpectedly".to_owned());
    }
    Ok(())
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TableSnapshot {
    pub(crate) metadata_location: String,
    pub(crate) location: String,
    pub(crate) uuid: String,
    pub(crate) schema: TableSchema,
    pub(crate) last_column_id: Option<i32>,
    pub(crate) properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct TableSchema {
    pub(crate) r#type: String,
    #[serde(rename = "schema-id", default)]
    pub(crate) schema_id: Option<i32>,
    pub(crate) fields: Vec<TableField>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct TableField {
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) required: bool,
    #[serde(rename = "type")]
    pub(crate) field_type: Value,
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
    location: String,
    #[serde(rename = "last-column-id", default)]
    last_column_id: Option<i32>,
    #[serde(default)]
    properties: BTreeMap<String, String>,
    #[serde(default)]
    schemas: Vec<TableSchema>,
    #[serde(rename = "current-schema-id", default)]
    current_schema_id: Option<i32>,
}
