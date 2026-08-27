use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::Future;

use anyhow::{anyhow, Context, Result};
use catalog_bench_conformance::{
    CatalogRequestFailure, CatalogRequestFailureKind, CatalogSession, NamespaceIdentifier,
    ResponseCapture,
};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use url::Url;

use crate::model::{RequestErrorKind, RequestIdentity, RequestOutcome, SanitizedRequestError};
use crate::policy::ContentionFixture;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSnapshot {
    pub format_version: u8,
    pub table_uuid: String,
    pub location: String,
    pub metadata_location: String,
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResourcePresence {
    Present,
    Absent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PresenceObservation {
    pub http_status: u16,
    pub presence: ResourcePresence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationReceipt {
    pub http_status: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CatalogFailureKind {
    Timeout,
    Transport,
    ResponseRead,
    ResponseTooLarge,
    UnexpectedHttp,
    MalformedResponse,
    Harness,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogFailure {
    pub kind: CatalogFailureKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    pub detail: String,
}

impl CatalogFailure {
    fn unexpected_http(status: u16, operation: &str, allowed: &[u16]) -> Self {
        Self {
            kind: CatalogFailureKind::UnexpectedHttp,
            http_status: Some(status),
            detail: format!("{operation} returned HTTP {status}; expected {allowed:?}"),
        }
    }

    fn malformed(status: u16, operation: &str, detail: impl Display) -> Self {
        Self {
            kind: CatalogFailureKind::MalformedResponse,
            http_status: Some(status),
            detail: format!("{operation} response is invalid: {detail}"),
        }
    }

    #[must_use]
    pub fn request_error(&self) -> SanitizedRequestError {
        let kind = match self.kind {
            CatalogFailureKind::Timeout => RequestErrorKind::Timeout,
            CatalogFailureKind::Transport | CatalogFailureKind::ResponseRead => {
                RequestErrorKind::Transport
            }
            CatalogFailureKind::ResponseTooLarge => RequestErrorKind::ResponseTooLarge,
            CatalogFailureKind::UnexpectedHttp => RequestErrorKind::UnexpectedHttp,
            CatalogFailureKind::MalformedResponse => RequestErrorKind::MalformedResponse,
            CatalogFailureKind::Harness => RequestErrorKind::Harness,
        };
        SanitizedRequestError {
            kind,
            http_status: self.http_status,
        }
    }
}

impl Display for CatalogFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl Error for CatalogFailure {}

impl From<CatalogRequestFailure> for CatalogFailure {
    fn from(failure: CatalogRequestFailure) -> Self {
        let kind = match failure.kind {
            CatalogRequestFailureKind::Timeout => CatalogFailureKind::Timeout,
            CatalogRequestFailureKind::Transport => CatalogFailureKind::Transport,
            CatalogRequestFailureKind::ResponseRead => CatalogFailureKind::ResponseRead,
            CatalogRequestFailureKind::ResponseTooLarge => CatalogFailureKind::ResponseTooLarge,
            CatalogRequestFailureKind::MalformedJson => CatalogFailureKind::MalformedResponse,
            CatalogRequestFailureKind::BodyNotRetained => CatalogFailureKind::Harness,
        };
        Self {
            kind,
            http_status: failure.http_status,
            detail: failure.detail,
        }
    }
}

/// Catalog effects required by one contention fixture. The fixture binding
/// keeps route construction out of measured commit latency.
pub trait CatalogPort: Clone + Send + Sync + 'static {
    fn namespace_presence(
        &self,
    ) -> impl Future<Output = std::result::Result<PresenceObservation, CatalogFailure>> + Send;

    fn table_presence(
        &self,
    ) -> impl Future<Output = std::result::Result<PresenceObservation, CatalogFailure>> + Send;

    fn create_namespace(
        &self,
    ) -> impl Future<Output = std::result::Result<MutationReceipt, CatalogFailure>> + Send;

    fn create_table(
        &self,
    ) -> impl Future<Output = std::result::Result<TableSnapshot, CatalogFailure>> + Send;

    fn load_table(
        &self,
    ) -> impl Future<Output = std::result::Result<TableSnapshot, CatalogFailure>> + Send;

    fn commit(
        &self,
        table_uuid: &str,
        property: &str,
        request_identity: &RequestIdentity,
    ) -> impl Future<Output = RequestOutcome> + Send;

    fn drop_table_without_purge(
        &self,
    ) -> impl Future<Output = std::result::Result<MutationReceipt, CatalogFailure>> + Send;

    fn drop_namespace(
        &self,
    ) -> impl Future<Output = std::result::Result<MutationReceipt, CatalogFailure>> + Send;
}

#[derive(Clone)]
pub struct RestCatalog {
    session: CatalogSession,
    create_table_location_root: Option<Url>,
}

impl RestCatalog {
    pub fn new(session: CatalogSession, create_table_location_root: Option<&str>) -> Result<Self> {
        let create_table_location_root = create_table_location_root
            .map(Url::parse)
            .transpose()
            .context("parse adapter create-table location root")?;
        if create_table_location_root
            .as_ref()
            .is_some_and(Url::cannot_be_a_base)
        {
            return Err(anyhow!(
                "adapter create-table location root cannot contain child table locations"
            ));
        }
        Ok(Self {
            session,
            create_table_location_root,
        })
    }

    pub fn bind(&self, fixture: &ContentionFixture) -> Result<RestCatalogFixture> {
        let namespace = NamespaceIdentifier::single(fixture.namespace.clone())?;
        let namespace_url = self.session.namespace_url(&namespace)?;
        let table_collection_url = self.session.table_collection_url(&namespace)?;
        let table_url = self.session.table_url(&namespace, &fixture.table)?;
        let mut drop_table_url = table_url.clone();
        drop_table_url
            .query_pairs_mut()
            .append_pair("purgeRequested", "false");
        let requested_location = self
            .create_table_location_root
            .as_ref()
            .map(|root| table_location(root, fixture))
            .transpose()?;
        Ok(RestCatalogFixture {
            session: self.session.clone(),
            namespace: fixture.namespace.clone(),
            table: fixture.table.clone(),
            namespace_url,
            namespace_collection_url: self.session.namespace_collection_url()?,
            table_collection_url,
            table_url,
            drop_table_url,
            requested_location,
        })
    }
}

#[derive(Clone)]
pub struct RestCatalogFixture {
    session: CatalogSession,
    namespace: String,
    table: String,
    namespace_url: Url,
    namespace_collection_url: Url,
    table_collection_url: Url,
    table_url: Url,
    drop_table_url: Url,
    requested_location: Option<String>,
}

impl RestCatalogFixture {
    #[must_use]
    pub fn requested_location(&self) -> Option<&str> {
        self.requested_location.as_deref()
    }

    async fn presence(
        &self,
        url: Url,
        operation: &str,
    ) -> std::result::Result<PresenceObservation, CatalogFailure> {
        let response = self
            .session
            .request_json(Method::GET, url, None, ResponseCapture::Discard)
            .await
            .map_err(CatalogFailure::from)?;
        let presence = match response.status() {
            200 => ResourcePresence::Present,
            404 => ResourcePresence::Absent,
            status => {
                return Err(CatalogFailure::unexpected_http(
                    status,
                    operation,
                    &[200, 404],
                ));
            }
        };
        Ok(PresenceObservation {
            http_status: response.status(),
            presence,
        })
    }

    async fn mutation(
        &self,
        method: Method,
        url: Url,
        body: Option<&Value>,
        operation: &str,
        allowed: &[u16],
    ) -> std::result::Result<MutationReceipt, CatalogFailure> {
        let response = self
            .session
            .request_json(method, url, body, ResponseCapture::Discard)
            .await
            .map_err(CatalogFailure::from)?;
        if !allowed.contains(&response.status()) {
            return Err(CatalogFailure::unexpected_http(
                response.status(),
                operation,
                allowed,
            ));
        }
        Ok(MutationReceipt {
            http_status: response.status(),
        })
    }
}

impl CatalogPort for RestCatalogFixture {
    fn namespace_presence(
        &self,
    ) -> impl Future<Output = std::result::Result<PresenceObservation, CatalogFailure>> + Send {
        self.presence(self.namespace_url.clone(), "load namespace")
    }

    fn table_presence(
        &self,
    ) -> impl Future<Output = std::result::Result<PresenceObservation, CatalogFailure>> + Send {
        self.presence(self.table_url.clone(), "load table")
    }

    fn create_namespace(
        &self,
    ) -> impl Future<Output = std::result::Result<MutationReceipt, CatalogFailure>> + Send {
        let body = json!({"namespace": [self.namespace], "properties": {}});
        async move {
            self.mutation(
                Method::POST,
                self.namespace_collection_url.clone(),
                Some(&body),
                "create namespace",
                &[200],
            )
            .await
        }
    }

    fn create_table(
        &self,
    ) -> impl Future<Output = std::result::Result<TableSnapshot, CatalogFailure>> + Send {
        let body = create_table_request(&self.table, self.requested_location.as_deref());
        async move {
            let response = self
                .session
                .request_json(
                    Method::POST,
                    self.table_collection_url.clone(),
                    Some(&body),
                    ResponseCapture::Json,
                )
                .await
                .map_err(CatalogFailure::from)?;
            if ![200, 201].contains(&response.status()) {
                return Err(CatalogFailure::unexpected_http(
                    response.status(),
                    "create table",
                    &[200, 201],
                ));
            }
            let snapshot = response
                .json::<LoadTableResult>()
                .map_err(CatalogFailure::from)
                .and_then(|value| parse_snapshot(value, response.status(), "create table"))?;
            validate_created_snapshot(
                &snapshot,
                response.status(),
                self.requested_location.as_deref(),
            )?;
            Ok(snapshot)
        }
    }

    async fn load_table(&self) -> std::result::Result<TableSnapshot, CatalogFailure> {
        let response = self
            .session
            .request_json(
                Method::GET,
                self.table_url.clone(),
                None,
                ResponseCapture::Json,
            )
            .await
            .map_err(CatalogFailure::from)?;
        if response.status() != 200 {
            return Err(CatalogFailure::unexpected_http(
                response.status(),
                "load table",
                &[200],
            ));
        }
        response
            .json::<LoadTableResult>()
            .map_err(CatalogFailure::from)
            .and_then(|value| parse_snapshot(value, response.status(), "load table"))
    }

    fn commit(
        &self,
        table_uuid: &str,
        property: &str,
        request_identity: &RequestIdentity,
    ) -> impl Future<Output = RequestOutcome> + Send {
        let body = commit_request(table_uuid, property, request_identity.expose_for_request());
        async move {
            match self
                .session
                .request_json(
                    Method::POST,
                    self.table_url.clone(),
                    Some(&body),
                    ResponseCapture::Discard,
                )
                .await
            {
                Ok(response) if response.status() == 200 => RequestOutcome::Accepted,
                Ok(response) if response.status() == 409 => RequestOutcome::Conflict,
                Ok(response) => RequestOutcome::Error(SanitizedRequestError {
                    kind: RequestErrorKind::UnexpectedHttp,
                    http_status: Some(response.status()),
                }),
                Err(failure) => {
                    RequestOutcome::Error(CatalogFailure::from(failure).request_error())
                }
            }
        }
    }

    fn drop_table_without_purge(
        &self,
    ) -> impl Future<Output = std::result::Result<MutationReceipt, CatalogFailure>> + Send {
        self.mutation(
            Method::DELETE,
            self.drop_table_url.clone(),
            None,
            "drop table without purge",
            &[204, 404],
        )
    }

    fn drop_namespace(
        &self,
    ) -> impl Future<Output = std::result::Result<MutationReceipt, CatalogFailure>> + Send {
        self.mutation(
            Method::DELETE,
            self.namespace_url.clone(),
            None,
            "drop namespace",
            &[204, 404],
        )
    }
}

fn table_location(root: &Url, fixture: &ContentionFixture) -> Result<String> {
    let mut location = root.clone();
    {
        let mut segments = location
            .path_segments_mut()
            .map_err(|()| anyhow!("create-table location root cannot contain path segments"))?;
        segments.pop_if_empty();
        segments.push(&fixture.namespace);
        segments.push(&fixture.table);
    }
    Ok(location.to_string())
}

fn create_table_request(name: &str, location: Option<&str>) -> Value {
    let mut body = json!({
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
            "format-version": "2",
            "catalog-bench.owner": "catalog-bench"
        }
    });
    if let Some(location) = location {
        body["location"] = Value::String(location.to_owned());
    }
    body
}

fn commit_request(table_uuid: &str, property: &str, request_identity: &str) -> Value {
    json!({
        "requirements": [{"type": "assert-table-uuid", "uuid": table_uuid}],
        "updates": [{
            "action": "set-properties",
            "updates": {(property): request_identity}
        }]
    })
}

fn parse_snapshot(
    response: LoadTableResult,
    status: u16,
    operation: &str,
) -> std::result::Result<TableSnapshot, CatalogFailure> {
    let metadata_location = response
        .metadata_location
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| CatalogFailure::malformed(status, operation, "missing metadata-location"))?;
    if response.metadata.table_uuid.trim().is_empty()
        || response.metadata.location.trim().is_empty()
    {
        return Err(CatalogFailure::malformed(
            status,
            operation,
            "table UUID and location must be nonempty",
        ));
    }
    if response.metadata.format_version != 2 {
        return Err(CatalogFailure::malformed(
            status,
            operation,
            format!(
                "format-version is {}, expected 2",
                response.metadata.format_version
            ),
        ));
    }
    Ok(TableSnapshot {
        format_version: response.metadata.format_version,
        table_uuid: response.metadata.table_uuid,
        location: response.metadata.location,
        metadata_location,
        properties: response.metadata.properties,
    })
}

fn validate_created_snapshot(
    snapshot: &TableSnapshot,
    status: u16,
    requested_location: Option<&str>,
) -> std::result::Result<(), CatalogFailure> {
    if requested_location.is_some_and(|location| snapshot.location != location) {
        return Err(CatalogFailure::malformed(
            status,
            "create table",
            "returned location differs from the requested shared-store location",
        ));
    }
    Ok(())
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
    #[serde(default)]
    properties: BTreeMap<String, String>,
}
