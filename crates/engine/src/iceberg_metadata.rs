//! Bounded projection of an Iceberg v2 metadata object into engine evidence.

use std::fmt::{Display, Formatter};

use serde_json::Value;
use url::Url;
use uuid::Uuid;

use crate::strict_json::decode_strict_json;
use crate::{
    EngineFieldObservation, EnginePropertyObservation, EngineTableObservation, IcebergField,
    IcebergPrimitiveType, TrinoFixtureTarget, TrinoObservationPolicy,
};

const MAXIMUM_ICEBERG_METADATA_BYTES: usize = 4 * 1024 * 1024;
const MAXIMUM_METADATA_TEXT_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergMetadataError {
    TooLarge,
    Malformed,
    UnsupportedVersion,
    InvalidIdentity,
    InvalidLocation,
    InvalidSchema,
    InvalidSnapshots,
    InvalidProperties,
}

impl Display for IcebergMetadataError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::TooLarge => "Iceberg metadata exceeds its byte limit",
            Self::Malformed => "Iceberg metadata is malformed",
            Self::UnsupportedVersion => "Iceberg metadata is not format version 2",
            Self::InvalidIdentity => "Iceberg metadata has an invalid table identity",
            Self::InvalidLocation => "Iceberg metadata has an invalid object-store location",
            Self::InvalidSchema => "Iceberg metadata has an invalid scenario schema",
            Self::InvalidSnapshots => "Iceberg metadata has an invalid snapshots array",
            Self::InvalidProperties => "Iceberg metadata has invalid scenario properties",
        })
    }
}

impl std::error::Error for IcebergMetadataError {}

pub fn decode_iceberg_table_metadata(
    bytes: &[u8],
    metadata_location: &str,
    fixture: &TrinoFixtureTarget,
    policy: &TrinoObservationPolicy,
) -> Result<EngineTableObservation, IcebergMetadataError> {
    if bytes.len() > MAXIMUM_ICEBERG_METADATA_BYTES {
        return Err(IcebergMetadataError::TooLarge);
    }
    let document = decode_strict_json(bytes).map_err(|_| IcebergMetadataError::Malformed)?;
    let object = document
        .as_object()
        .ok_or(IcebergMetadataError::Malformed)?;
    let format_version = required_u64(object, "format-version")?;
    if format_version != u64::from(policy.format_version) || format_version != 2 {
        return Err(IcebergMetadataError::UnsupportedVersion);
    }
    let table_uuid = required_text(object, "table-uuid")?;
    let parsed_uuid =
        Uuid::parse_str(table_uuid).map_err(|_| IcebergMetadataError::InvalidIdentity)?;
    if parsed_uuid.hyphenated().to_string() != table_uuid {
        return Err(IcebergMetadataError::InvalidIdentity);
    }
    let location = required_text(object, "location")?;
    validate_s3_location(location, &fixture.bucket, false)?;
    validate_s3_location(metadata_location, &fixture.bucket, true)?;
    let metadata_prefix = format!("{}/metadata/", location.trim_end_matches('/'));
    if !metadata_location.starts_with(&metadata_prefix) {
        return Err(IcebergMetadataError::InvalidLocation);
    }
    if let Some(expected) = &fixture.requested_location {
        if location.trim_end_matches('/') != expected.trim_end_matches('/') {
            return Err(IcebergMetadataError::InvalidLocation);
        }
    }
    let last_column_id = i32::try_from(required_u64(object, "last-column-id")?)
        .map_err(|_| IcebergMetadataError::InvalidSchema)?;
    let current_schema_id = required_u64(object, "current-schema-id")?;
    let schemas = object
        .get("schemas")
        .and_then(Value::as_array)
        .ok_or(IcebergMetadataError::InvalidSchema)?;
    let current = schemas
        .iter()
        .filter_map(Value::as_object)
        .find(|schema| required_u64(schema, "schema-id") == Ok(current_schema_id))
        .ok_or(IcebergMetadataError::InvalidSchema)?;
    if current.get("type").and_then(Value::as_str) != Some("struct") {
        return Err(IcebergMetadataError::InvalidSchema);
    }
    let fields = current
        .get("fields")
        .and_then(Value::as_array)
        .ok_or(IcebergMetadataError::InvalidSchema)?;
    let schema = fields
        .iter()
        .map(decode_field)
        .collect::<Result<Vec<_>, _>>()?;
    validate_scenario_schema(&schema, last_column_id, policy)?;
    let snapshots = match object.get("snapshots") {
        None | Some(Value::Null) => 0,
        Some(Value::Array(snapshots)) => {
            u64::try_from(snapshots.len()).map_err(|_| IcebergMetadataError::InvalidSnapshots)?
        }
        Some(_) => return Err(IcebergMetadataError::InvalidSnapshots),
    };
    let metadata_properties = object
        .get("properties")
        .and_then(Value::as_object)
        .ok_or(IcebergMetadataError::InvalidProperties)?;
    if metadata_properties.values().any(|value| {
        value
            .as_str()
            .is_none_or(|text| text.len() > MAXIMUM_METADATA_TEXT_BYTES)
    }) {
        return Err(IcebergMetadataError::InvalidProperties);
    }
    let properties = policy
        .properties
        .iter()
        .map(|(key, expected)| {
            let observation = if metadata_properties.get(key).and_then(Value::as_str)
                == Some(expected.as_str())
            {
                EnginePropertyObservation::Match
            } else {
                EnginePropertyObservation::Mismatch
            };
            (key.clone(), observation)
        })
        .collect();

    Ok(EngineTableObservation {
        table_uuid: table_uuid.to_owned(),
        metadata_location: metadata_location.to_owned(),
        location: location.to_owned(),
        format_version: 2,
        last_column_id,
        schema,
        snapshots,
        properties,
    })
}

fn decode_field(value: &Value) -> Result<EngineFieldObservation, IcebergMetadataError> {
    let field = value
        .as_object()
        .ok_or(IcebergMetadataError::InvalidSchema)?;
    let id = i32::try_from(required_u64(field, "id")?)
        .map_err(|_| IcebergMetadataError::InvalidSchema)?;
    if id <= 0 {
        return Err(IcebergMetadataError::InvalidSchema);
    }
    let name = required_text(field, "name")?;
    if name.is_empty() || name.len() > 256 {
        return Err(IcebergMetadataError::InvalidSchema);
    }
    let required = field
        .get("required")
        .and_then(Value::as_bool)
        .ok_or(IcebergMetadataError::InvalidSchema)?;
    let field_type = match required_text(field, "type")? {
        "long" => IcebergPrimitiveType::Long,
        "string" => IcebergPrimitiveType::String,
        _ => return Err(IcebergMetadataError::InvalidSchema),
    };
    Ok(EngineFieldObservation {
        id,
        name: name.to_owned(),
        required,
        field_type,
    })
}

fn validate_scenario_schema(
    observed: &[EngineFieldObservation],
    last_column_id: i32,
    policy: &TrinoObservationPolicy,
) -> Result<(), IcebergMetadataError> {
    let initial = policy
        .initial_schema
        .iter()
        .map(observation_from_field)
        .collect::<Vec<_>>();
    if observed == initial
        && last_column_id == initial.iter().map(|field| field.id).max().unwrap_or(0)
    {
        return Ok(());
    }
    let mut evolved = initial;
    let evolved_id = evolved
        .iter()
        .map(|field| field.id)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(IcebergMetadataError::InvalidSchema)?;
    evolved.push(EngineFieldObservation {
        id: evolved_id,
        name: policy.evolved_field.name.clone(),
        required: policy.evolved_field.required,
        field_type: policy.evolved_field.field_type,
    });
    if observed == evolved && last_column_id == evolved_id {
        Ok(())
    } else {
        Err(IcebergMetadataError::InvalidSchema)
    }
}

fn observation_from_field(field: &IcebergField) -> EngineFieldObservation {
    EngineFieldObservation {
        id: field.id,
        name: field.name.clone(),
        required: field.required,
        field_type: field.field_type,
    }
}

fn required_text<'a>(
    object: &'a serde_json::Map<String, Value>,
    key: &str,
) -> Result<&'a str, IcebergMetadataError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| value.len() <= MAXIMUM_METADATA_TEXT_BYTES)
        .ok_or(IcebergMetadataError::Malformed)
}

fn required_u64(
    object: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<u64, IcebergMetadataError> {
    object
        .get(key)
        .and_then(Value::as_u64)
        .ok_or(IcebergMetadataError::Malformed)
}

fn validate_s3_location(
    value: &str,
    bucket: &str,
    require_metadata_json: bool,
) -> Result<(), IcebergMetadataError> {
    let url = Url::parse(value).map_err(|_| IcebergMetadataError::InvalidLocation)?;
    if url.scheme() != "s3"
        || url.host_str() != Some(bucket)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || (require_metadata_json
            && (!url.path().contains("/metadata/") || !url.path().ends_with(".json")))
    {
        return Err(IcebergMetadataError::InvalidLocation);
    }
    Ok(())
}
