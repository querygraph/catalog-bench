use std::fmt::{Display, Formatter};

use schemars::generate::SchemaSettings;
use schemars::{JsonSchema, Schema};
use serde_json::Value;

use super::{Profile, ResultBundleManifest, ResultRecord, Scenario, Validate, ValidationErrors};

pub const SCENARIO_SCHEMA_FILE: &str = "scenario.schema.json";
pub const RESULT_SCHEMA_FILE: &str = "result.schema.json";
pub const PROFILE_SCHEMA_FILE: &str = "profile.schema.json";
pub const MANIFEST_SCHEMA_FILE: &str = "manifest.schema.json";

const SCHEMA_BASE_URI: &str = "https://adversari.al/catalog-bench/schemas/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentKind {
    Scenario,
    Result,
    Profile,
    Manifest,
}

impl Display for DocumentKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Scenario => "scenario",
            Self::Result => "result",
            Self::Profile => "profile",
            Self::Manifest => "manifest",
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ContractDocument {
    Scenario(Scenario),
    Result(Box<ResultRecord>),
    Profile(Profile),
    Manifest(ResultBundleManifest),
}

impl ContractDocument {
    #[must_use]
    pub fn kind(&self) -> DocumentKind {
        match self {
            Self::Scenario(_) => DocumentKind::Scenario,
            Self::Result(_) => DocumentKind::Result,
            Self::Profile(_) => DocumentKind::Profile,
            Self::Manifest(_) => DocumentKind::Manifest,
        }
    }
}

#[derive(Debug)]
pub enum ParseContractError {
    Json(serde_json::Error),
    MissingKind,
    UnknownKind(String),
    Semantic(ValidationErrors),
}

impl Display for ParseContractError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(error) => write!(formatter, "invalid contract JSON: {error}"),
            Self::MissingKind => formatter.write_str("contract document is missing string `kind`"),
            Self::UnknownKind(kind) => write!(formatter, "unknown contract document kind `{kind}`"),
            Self::Semantic(errors) => write!(formatter, "contract validation failed: {errors}"),
        }
    }
}

impl std::error::Error for ParseContractError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Json(error) => Some(error),
            Self::Semantic(errors) => Some(errors),
            Self::MissingKind | Self::UnknownKind(_) => None,
        }
    }
}

/// Deserialize and semantically validate any v1 contract document.
pub fn parse_contract(bytes: &[u8]) -> Result<ContractDocument, ParseContractError> {
    let value: Value = serde_json::from_slice(bytes).map_err(ParseContractError::Json)?;
    let kind = value
        .get("kind")
        .and_then(Value::as_str)
        .ok_or(ParseContractError::MissingKind)?;

    match kind {
        "scenario" => parse_typed(value).map(ContractDocument::Scenario),
        "result" => parse_typed(value)
            .map(Box::new)
            .map(ContractDocument::Result),
        "profile" => parse_typed(value).map(ContractDocument::Profile),
        "manifest" => parse_typed(value).map(ContractDocument::Manifest),
        other => Err(ParseContractError::UnknownKind(other.to_owned())),
    }
}

fn parse_typed<T>(value: Value) -> Result<T, ParseContractError>
where
    T: serde::de::DeserializeOwned + Validate,
{
    let document: T = serde_json::from_value(value).map_err(ParseContractError::Json)?;
    document.validate().map_err(ParseContractError::Semantic)?;
    Ok(document)
}

/// A checked-in schema and its stable public filename.
#[derive(Debug, Clone, PartialEq)]
pub struct GeneratedSchema {
    pub kind: DocumentKind,
    pub file_name: &'static str,
    pub document: Value,
}

/// Generate all v1 schemas with explicit Draft 2020-12 settings.
pub fn generated_schemas() -> Result<Vec<GeneratedSchema>, serde_json::Error> {
    Ok(vec![
        generate::<Scenario>(DocumentKind::Scenario, SCENARIO_SCHEMA_FILE)?,
        generate::<ResultRecord>(DocumentKind::Result, RESULT_SCHEMA_FILE)?,
        generate::<Profile>(DocumentKind::Profile, PROFILE_SCHEMA_FILE)?,
        generate::<ResultBundleManifest>(DocumentKind::Manifest, MANIFEST_SCHEMA_FILE)?,
    ])
}

fn generate<T: JsonSchema>(
    kind: DocumentKind,
    file_name: &'static str,
) -> Result<GeneratedSchema, serde_json::Error> {
    let schema: Schema = SchemaSettings::draft2020_12()
        .for_deserialize()
        .into_generator()
        .into_root_schema_for::<T>();
    let mut document = serde_json::to_value(schema)?;
    if let Some(object) = document.as_object_mut() {
        object.insert(
            "$id".to_owned(),
            Value::String(format!("{SCHEMA_BASE_URI}/{file_name}")),
        );
    }
    Ok(GeneratedSchema {
        kind,
        file_name,
        document,
    })
}
