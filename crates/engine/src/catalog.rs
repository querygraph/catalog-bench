use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;

use catalog_bench_commit::store::{ObjectStoreConnectionPolicy, ObjectStoreFailure, TableRoot};
use catalog_bench_conformance::{
    CatalogRequestFailure, CatalogRequestFailureKind, CatalogSession, NamespaceIdentifier,
    ResponseCapture,
};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use crate::{
    EngineFieldObservation, EnginePropertyObservation, EngineTableObservation,
    IcebergPrimitiveType, InteroperabilityPlan, ObjectStorePlan,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EngineCatalogFailureKind {
    Route,
    Timeout,
    Transport,
    ResponseRead,
    ResponseTooLarge,
    UnexpectedHttp,
    MalformedResponse,
    Harness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineCatalogFailure {
    pub kind: EngineCatalogFailureKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
}

impl EngineCatalogFailure {
    fn route() -> Self {
        Self {
            kind: EngineCatalogFailureKind::Route,
            http_status: None,
        }
    }

    fn unexpected_http(http_status: u16) -> Self {
        Self {
            kind: EngineCatalogFailureKind::UnexpectedHttp,
            http_status: Some(http_status),
        }
    }

    fn malformed(http_status: u16) -> Self {
        Self {
            kind: EngineCatalogFailureKind::MalformedResponse,
            http_status: Some(http_status),
        }
    }
}

impl Display for EngineCatalogFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let category = match self.kind {
            EngineCatalogFailureKind::Route => "route construction failed",
            EngineCatalogFailureKind::Timeout => "request timed out",
            EngineCatalogFailureKind::Transport => "request transport failed",
            EngineCatalogFailureKind::ResponseRead => "response read failed",
            EngineCatalogFailureKind::ResponseTooLarge => "response exceeded its size limit",
            EngineCatalogFailureKind::UnexpectedHttp => "response used an unexpected HTTP status",
            EngineCatalogFailureKind::MalformedResponse => "response violated the expected schema",
            EngineCatalogFailureKind::Harness => "harness invariant failed",
        };
        match self.http_status {
            Some(status) => write!(formatter, "{category} (HTTP {status})"),
            None => formatter.write_str(category),
        }
    }
}

impl Error for EngineCatalogFailure {}

impl From<CatalogRequestFailure> for EngineCatalogFailure {
    fn from(failure: CatalogRequestFailure) -> Self {
        let kind = match failure.kind {
            CatalogRequestFailureKind::Timeout => EngineCatalogFailureKind::Timeout,
            CatalogRequestFailureKind::Transport => EngineCatalogFailureKind::Transport,
            CatalogRequestFailureKind::ResponseRead => EngineCatalogFailureKind::ResponseRead,
            CatalogRequestFailureKind::ResponseTooLarge => {
                EngineCatalogFailureKind::ResponseTooLarge
            }
            CatalogRequestFailureKind::BodyNotRetained => EngineCatalogFailureKind::Harness,
            CatalogRequestFailureKind::MalformedJson => EngineCatalogFailureKind::MalformedResponse,
        };
        Self {
            kind,
            http_status: failure.http_status,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineCatalogTable {
    pub current_schema_id: i32,
    pub table: EngineTableObservation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum EngineTableLoad {
    Present {
        http_status: u16,
        response_bytes: u64,
        state: EngineCatalogTable,
    },
    Absent {
        http_status: u16,
        response_bytes: u64,
    },
}

impl EngineTableLoad {
    #[must_use]
    pub fn state(&self) -> Option<&EngineCatalogTable> {
        match self {
            Self::Present { state, .. } => Some(state),
            Self::Absent { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineCleanupReceipt {
    pub http_status: u16,
    pub response_bytes: u64,
    pub already_absent: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum EngineResourcePresence {
    Present {
        http_status: u16,
        response_bytes: u64,
    },
    Absent {
        http_status: u16,
        response_bytes: u64,
    },
}

impl EngineResourcePresence {
    #[must_use]
    pub fn is_absent(self) -> bool {
        matches!(self, Self::Absent { .. })
    }
}

pub trait EngineCatalog: Clone + Send + Sync + 'static {
    fn load_table(
        &self,
    ) -> impl Future<Output = Result<EngineTableLoad, EngineCatalogFailure>> + Send;

    fn drop_table_without_purge(
        &self,
    ) -> impl Future<Output = Result<EngineCleanupReceipt, EngineCatalogFailure>> + Send;

    fn drop_namespace(
        &self,
    ) -> impl Future<Output = Result<EngineCleanupReceipt, EngineCatalogFailure>> + Send;

    fn table_presence(
        &self,
    ) -> impl Future<Output = Result<EngineResourcePresence, EngineCatalogFailure>> + Send;

    fn namespace_presence(
        &self,
    ) -> impl Future<Output = Result<EngineResourcePresence, EngineCatalogFailure>> + Send;
}

#[derive(Clone)]
pub struct RestEngineCatalog {
    session: CatalogSession,
    namespace: String,
    table: String,
    table_url: Url,
    drop_table_url: Url,
    namespace_url: Url,
    projection: TableProjection,
}

impl RestEngineCatalog {
    pub fn from_plan(
        session: CatalogSession,
        plan: &InteroperabilityPlan,
    ) -> Result<Self, EngineCatalogFailure> {
        let fixture = plan.fixture();
        let namespace = NamespaceIdentifier::single(fixture.namespace.clone())
            .map_err(|_| EngineCatalogFailure::route())?;
        let table_url = session
            .table_url(&namespace, &fixture.table)
            .map_err(|_| EngineCatalogFailure::route())?;
        let mut drop_table_url = table_url.clone();
        drop_table_url
            .query_pairs_mut()
            .append_pair("purgeRequested", "false");
        let namespace_url = session
            .namespace_url(&namespace)
            .map_err(|_| EngineCatalogFailure::route())?;
        Ok(Self {
            session,
            namespace: fixture.namespace.clone(),
            table: fixture.table.clone(),
            table_url,
            drop_table_url,
            namespace_url,
            projection: TableProjection::from_plan(plan),
        })
    }

    async fn cleanup(&self, url: Url) -> Result<EngineCleanupReceipt, EngineCatalogFailure> {
        let response = self
            .session
            .request_json(Method::DELETE, url, None, ResponseCapture::Discard)
            .await
            .map_err(EngineCatalogFailure::from)?;
        if ![204, 404].contains(&response.status()) {
            return Err(EngineCatalogFailure::unexpected_http(response.status()));
        }
        Ok(EngineCleanupReceipt {
            http_status: response.status(),
            response_bytes: response.body_bytes(),
            already_absent: response.status() == 404,
        })
    }

    async fn presence(&self, url: Url) -> Result<EngineResourcePresence, EngineCatalogFailure> {
        let response = self
            .session
            .request_json(Method::GET, url, None, ResponseCapture::Discard)
            .await
            .map_err(EngineCatalogFailure::from)?;
        match response.status() {
            200 => Ok(EngineResourcePresence::Present {
                http_status: response.status(),
                response_bytes: response.body_bytes(),
            }),
            404 => Ok(EngineResourcePresence::Absent {
                http_status: response.status(),
                response_bytes: response.body_bytes(),
            }),
            status => Err(EngineCatalogFailure::unexpected_http(status)),
        }
    }
}

impl EngineCatalog for RestEngineCatalog {
    async fn load_table(&self) -> Result<EngineTableLoad, EngineCatalogFailure> {
        let response = self
            .session
            .request_json(
                Method::GET,
                self.table_url.clone(),
                None,
                ResponseCapture::Json,
            )
            .await
            .map_err(EngineCatalogFailure::from)?;
        match response.status() {
            404 => Ok(EngineTableLoad::Absent {
                http_status: response.status(),
                response_bytes: response.body_bytes(),
            }),
            200 => {
                let decoded = response
                    .json::<LoadTableResponse>()
                    .map_err(EngineCatalogFailure::from)?;
                let state = self.projection.project(decoded, response.status())?;
                Ok(EngineTableLoad::Present {
                    http_status: response.status(),
                    response_bytes: response.body_bytes(),
                    state,
                })
            }
            status => Err(EngineCatalogFailure::unexpected_http(status)),
        }
    }

    fn drop_table_without_purge(
        &self,
    ) -> impl Future<Output = Result<EngineCleanupReceipt, EngineCatalogFailure>> + Send {
        self.cleanup(self.drop_table_url.clone())
    }

    fn drop_namespace(
        &self,
    ) -> impl Future<Output = Result<EngineCleanupReceipt, EngineCatalogFailure>> + Send {
        self.cleanup(self.namespace_url.clone())
    }

    fn table_presence(
        &self,
    ) -> impl Future<Output = Result<EngineResourcePresence, EngineCatalogFailure>> + Send {
        self.presence(self.table_url.clone())
    }

    fn namespace_presence(
        &self,
    ) -> impl Future<Output = Result<EngineResourcePresence, EngineCatalogFailure>> + Send {
        self.presence(self.namespace_url.clone())
    }
}

impl ObjectStoreConnectionPolicy for ObjectStorePlan {
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn bucket(&self) -> &str {
        &self.bucket
    }

    fn region(&self) -> &str {
        &self.region
    }

    fn allow_http(&self) -> bool {
        self.allow_http
    }

    fn path_style_access(&self) -> bool {
        self.path_style_access
    }

    fn access_key_env(&self) -> &str {
        &self.access_key_env
    }

    fn secret_key_env(&self) -> &str {
        &self.secret_key_env
    }
}

impl Debug for RestEngineCatalog {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RestEngineCatalog")
            .field("session", &"<private>")
            .field("namespace", &self.namespace)
            .field("table", &self.table)
            .field("table_url", &self.table_url)
            .field("drop_table_url", &self.drop_table_url)
            .field("namespace_url", &self.namespace_url)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct TableProjection {
    bucket: String,
    allowed_fields: BTreeSet<String>,
    expected_properties: BTreeMap<String, String>,
}

impl TableProjection {
    fn from_plan(plan: &InteroperabilityPlan) -> Self {
        let scenario = plan.scenario();
        let mut allowed_fields = scenario
            .table
            .schema
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect::<BTreeSet<_>>();
        allowed_fields.insert(scenario.schema_evolution.field.name.clone());
        Self {
            bucket: plan.object_store().bucket.clone(),
            allowed_fields,
            expected_properties: scenario.table.properties.clone(),
        }
    }

    fn project(
        &self,
        response: LoadTableResponse,
        http_status: u16,
    ) -> Result<EngineCatalogTable, EngineCatalogFailure> {
        let metadata_location = response
            .metadata_location
            .filter(|location| !location.is_empty())
            .ok_or_else(|| EngineCatalogFailure::malformed(http_status))?;
        let metadata = response.metadata;
        let table_uuid = Uuid::parse_str(&metadata.table_uuid)
            .map_err(|_| EngineCatalogFailure::malformed(http_status))?;
        if table_uuid.is_nil()
            || metadata.format_version != 2
            || metadata.last_column_id <= 0
            || metadata.current_schema_id < 0
        {
            return Err(EngineCatalogFailure::malformed(http_status));
        }
        TableRoot::new(&metadata.location, &metadata_location, &self.bucket)
            .map_err(|_: ObjectStoreFailure| EngineCatalogFailure::malformed(http_status))?;

        let schema_ids = metadata
            .schemas
            .iter()
            .map(|schema| schema.schema_id)
            .collect::<BTreeSet<_>>();
        if schema_ids.len() != metadata.schemas.len()
            || schema_ids.iter().any(|schema_id| *schema_id < 0)
            || metadata
                .schemas
                .iter()
                .flat_map(|schema| schema.fields.iter())
                .map(|field| field.id)
                .max()
                != Some(metadata.last_column_id)
        {
            return Err(EngineCatalogFailure::malformed(http_status));
        }
        let current = metadata
            .schemas
            .iter()
            .filter(|schema| schema.schema_id == metadata.current_schema_id)
            .collect::<Vec<_>>();
        let [current] = current.as_slice() else {
            return Err(EngineCatalogFailure::malformed(http_status));
        };
        if current.schema_type != "struct" || current.fields.is_empty() {
            return Err(EngineCatalogFailure::malformed(http_status));
        }

        let mut field_ids = BTreeSet::new();
        let mut field_names = BTreeSet::new();
        let mut fields = Vec::with_capacity(current.fields.len());
        for field in &current.fields {
            if field.id <= 0
                || !valid_identifier(&field.name)
                || !self.allowed_fields.contains(&field.name)
                || !field_ids.insert(field.id)
                || !field_names.insert(field.name.as_str())
            {
                return Err(EngineCatalogFailure::malformed(http_status));
            }
            let field_type = match field.field_type.as_str() {
                Some("long") => IcebergPrimitiveType::Long,
                Some("string") => IcebergPrimitiveType::String,
                _ => return Err(EngineCatalogFailure::malformed(http_status)),
            };
            fields.push(EngineFieldObservation {
                id: field.id,
                name: field.name.clone(),
                required: field.required,
                field_type,
            });
        }
        let snapshot_ids = metadata
            .snapshots
            .iter()
            .map(|snapshot| snapshot.snapshot_id)
            .collect::<BTreeSet<_>>();
        if snapshot_ids.len() != metadata.snapshots.len() {
            return Err(EngineCatalogFailure::malformed(http_status));
        }
        let snapshots = u64::try_from(metadata.snapshots.len())
            .map_err(|_| EngineCatalogFailure::malformed(http_status))?;
        let properties = self
            .expected_properties
            .iter()
            .filter_map(|(key, expected)| {
                metadata.properties.get(key).map(|observed| {
                    let outcome = if observed == expected {
                        EnginePropertyObservation::Match
                    } else {
                        EnginePropertyObservation::Mismatch
                    };
                    (key.clone(), outcome)
                })
            })
            .collect();
        Ok(EngineCatalogTable {
            current_schema_id: metadata.current_schema_id,
            table: EngineTableObservation {
                table_uuid: table_uuid.to_string(),
                metadata_location,
                location: metadata.location,
                format_version: metadata.format_version,
                last_column_id: metadata.last_column_id,
                schema: fields,
                snapshots,
                properties,
            },
        })
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

#[derive(Debug, Deserialize)]
struct LoadTableResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: Option<String>,
    metadata: RestTableMetadata,
}

#[derive(Debug, Deserialize)]
struct RestTableMetadata {
    #[serde(rename = "format-version")]
    format_version: u8,
    #[serde(rename = "table-uuid")]
    table_uuid: String,
    location: String,
    #[serde(rename = "last-column-id")]
    last_column_id: i32,
    schemas: Vec<RestSchema>,
    #[serde(rename = "current-schema-id")]
    current_schema_id: i32,
    #[serde(default)]
    snapshots: Vec<RestSnapshot>,
    #[serde(default)]
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct RestSchema {
    #[serde(rename = "schema-id")]
    schema_id: i32,
    #[serde(rename = "type")]
    schema_type: String,
    fields: Vec<RestField>,
}

#[derive(Debug, Deserialize)]
struct RestField {
    id: i32,
    name: String,
    required: bool,
    #[serde(rename = "type")]
    field_type: Value,
}

#[derive(Debug, Deserialize)]
struct RestSnapshot {
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
}
