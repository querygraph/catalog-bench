use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::sync::Arc;

use futures::TryStreamExt as _;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::policy::ObjectStorePolicy;
use crate::protocol::TableSnapshot;

const MAXIMUM_FAILURE_DETAIL_CHARACTERS: usize = 512;

#[derive(Clone, PartialEq, Eq)]
pub struct TableRoot {
    location: String,
    bucket: String,
    path: Path,
}

impl TableRoot {
    pub fn new(
        location: &str,
        metadata_location: &str,
        expected_bucket: &str,
    ) -> Result<Self, ObjectStoreFailure> {
        let (bucket, path) = parse_s3_location(location, "table location")?;
        if bucket != expected_bucket {
            return Err(ObjectStoreFailure::configuration(format!(
                "table location uses bucket `{bucket}` instead of `{expected_bucket}`"
            )));
        }
        if path.as_ref().is_empty() {
            return Err(ObjectStoreFailure::configuration(
                "table location must identify a run-owned path",
            ));
        }
        let root = Self {
            location: location.to_owned(),
            bucket,
            path,
        };
        root.metadata_path(metadata_location)?;
        Ok(root)
    }

    pub fn from_snapshot(
        snapshot: &TableSnapshot,
        expected_bucket: &str,
    ) -> Result<Self, ObjectStoreFailure> {
        Self::new(
            &snapshot.location,
            &snapshot.metadata_location,
            expected_bucket,
        )
    }

    #[must_use]
    pub fn location(&self) -> &str {
        &self.location
    }

    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn contains_metadata_location(&self, location: &str) -> bool {
        self.metadata_path(location).is_ok()
    }

    fn metadata_path(&self, location: &str) -> Result<Path, ObjectStoreFailure> {
        let (bucket, path) = parse_s3_location(location, "metadata location")?;
        if bucket != self.bucket || !path.prefix_matches(&self.path) || path == self.path {
            return Err(ObjectStoreFailure::configuration(
                "metadata location is outside the returned table root",
            ));
        }
        if !path.as_ref().ends_with(".metadata.json") {
            return Err(ObjectStoreFailure::configuration(
                "metadata location does not end in `.metadata.json`",
            ));
        }
        Ok(path)
    }
}

impl Debug for TableRoot {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TableRoot")
            .field("location", &self.location)
            .field("bucket", &self.bucket)
            .field("path", &self.path.as_ref())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectAuditSnapshot {
    pub table_root: String,
    pub metadata_objects: u64,
    pub metadata_bytes: u64,
    pub referenced_metadata_location: String,
    pub referenced_metadata_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableObjectAuditSnapshot {
    pub table_root: String,
    pub metadata_objects: u64,
    pub metadata_bytes: u64,
    pub parquet_objects: u64,
    pub parquet_bytes: u64,
    pub referenced_metadata_location: String,
    pub referenced_metadata_exists: bool,
}

impl From<TableObjectAuditSnapshot> for ObjectAuditSnapshot {
    fn from(audit: TableObjectAuditSnapshot) -> Self {
        Self {
            table_root: audit.table_root,
            metadata_objects: audit.metadata_objects,
            metadata_bytes: audit.metadata_bytes,
            referenced_metadata_location: audit.referenced_metadata_location,
            referenced_metadata_exists: audit.referenced_metadata_exists,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectStoreFailureKind {
    Configuration,
    Authentication,
    List,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectStoreFailure {
    pub kind: ObjectStoreFailureKind,
    pub detail: String,
}

impl ObjectStoreFailure {
    fn configuration(detail: impl Into<String>) -> Self {
        Self {
            kind: ObjectStoreFailureKind::Configuration,
            detail: detail.into(),
        }
    }

    fn authentication(detail: impl Into<String>) -> Self {
        Self {
            kind: ObjectStoreFailureKind::Authentication,
            detail: detail.into(),
        }
    }

    fn listing(detail: impl Into<String>) -> Self {
        Self {
            kind: ObjectStoreFailureKind::List,
            detail: detail.into(),
        }
    }
}

impl Display for ObjectStoreFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl Error for ObjectStoreFailure {}

pub trait MetadataStore: Clone + Send + Sync + 'static {
    fn audit(
        &self,
        root: &TableRoot,
        metadata_location: &str,
    ) -> impl Future<Output = Result<ObjectAuditSnapshot, ObjectStoreFailure>> + Send;
}

pub trait TableObjectStore: Clone + Send + Sync + 'static {
    fn audit_table(
        &self,
        root: &TableRoot,
        metadata_location: &str,
    ) -> impl Future<Output = Result<TableObjectAuditSnapshot, ObjectStoreFailure>> + Send;
}

pub trait ObjectStoreConnectionPolicy {
    fn endpoint(&self) -> &str;
    fn bucket(&self) -> &str;
    fn region(&self) -> &str;
    fn allow_http(&self) -> bool;
    fn path_style_access(&self) -> bool;
    fn access_key_env(&self) -> &str;
    fn secret_key_env(&self) -> &str;
}

impl ObjectStoreConnectionPolicy for ObjectStorePolicy {
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

#[derive(Clone)]
pub struct ObjectStoreAuditor {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    sensitive_values: Arc<Vec<String>>,
}

impl ObjectStoreAuditor {
    pub fn from_policy<F>(policy: &ObjectStorePolicy, getenv: F) -> Result<Self, ObjectStoreFailure>
    where
        F: Fn(&str) -> Option<String>,
    {
        Self::from_connection(policy, getenv)
    }

    pub fn from_connection<P, F>(policy: &P, getenv: F) -> Result<Self, ObjectStoreFailure>
    where
        P: ObjectStoreConnectionPolicy,
        F: Fn(&str) -> Option<String>,
    {
        let access_key = getenv(policy.access_key_env())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                ObjectStoreFailure::authentication(format!(
                    "environment variable `{}` is not set or is empty",
                    policy.access_key_env()
                ))
            })?;
        let secret_key = getenv(policy.secret_key_env())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                ObjectStoreFailure::authentication(format!(
                    "environment variable `{}` is not set or is empty",
                    policy.secret_key_env()
                ))
            })?;
        let store = AmazonS3Builder::new()
            .with_endpoint(policy.endpoint())
            .with_access_key_id(&access_key)
            .with_secret_access_key(&secret_key)
            .with_region(policy.region())
            .with_bucket_name(policy.bucket())
            .with_allow_http(policy.allow_http())
            .with_virtual_hosted_style_request(!policy.path_style_access())
            .build()
            .map_err(|error| {
                ObjectStoreFailure::configuration(redact_and_bound(
                    &format!("failed to build shared object-store client: {error}"),
                    &[access_key.clone(), secret_key.clone()],
                ))
            })?;
        Ok(Self {
            store: Arc::new(store),
            bucket: policy.bucket().to_owned(),
            sensitive_values: Arc::new(vec![access_key, secret_key]),
        })
    }

    pub fn for_store(store: Arc<dyn ObjectStore>, bucket: impl Into<String>) -> Self {
        Self {
            store,
            bucket: bucket.into(),
            sensitive_values: Arc::new(Vec::new()),
        }
    }
}

impl MetadataStore for ObjectStoreAuditor {
    fn audit(
        &self,
        root: &TableRoot,
        metadata_location: &str,
    ) -> impl Future<Output = Result<ObjectAuditSnapshot, ObjectStoreFailure>> + Send {
        let root = root.clone();
        let metadata_location = metadata_location.to_owned();
        async move {
            self.audit_table(&root, &metadata_location)
                .await
                .map(Into::into)
        }
    }
}

impl TableObjectStore for ObjectStoreAuditor {
    fn audit_table(
        &self,
        root: &TableRoot,
        metadata_location: &str,
    ) -> impl Future<Output = Result<TableObjectAuditSnapshot, ObjectStoreFailure>> + Send {
        let root = root.clone();
        let metadata_location = metadata_location.to_owned();
        async move {
            if root.bucket != self.bucket {
                return Err(ObjectStoreFailure::configuration(format!(
                    "table root bucket `{}` does not match auditor bucket `{}`",
                    root.bucket, self.bucket
                )));
            }
            let referenced = root.metadata_path(&metadata_location)?;
            let mut objects = self.store.list(Some(&root.path));
            let mut metadata_objects = 0_u64;
            let mut metadata_bytes = 0_u64;
            let mut parquet_objects = 0_u64;
            let mut parquet_bytes = 0_u64;
            let mut referenced_metadata_exists = false;
            while let Some(object) = objects.try_next().await.map_err(|error| {
                ObjectStoreFailure::listing(redact_and_bound(
                    &format!("failed to list table objects: {error}"),
                    &self.sensitive_values,
                ))
            })? {
                let size = u64::try_from(object.size).map_err(|_| {
                    ObjectStoreFailure::listing("object byte count cannot be represented as u64")
                })?;
                if object.location.as_ref().ends_with(".metadata.json") {
                    metadata_objects = metadata_objects.checked_add(1).ok_or_else(|| {
                        ObjectStoreFailure::listing("metadata object count overflowed u64")
                    })?;
                    metadata_bytes = metadata_bytes.checked_add(size).ok_or_else(|| {
                        ObjectStoreFailure::listing("metadata byte count overflowed u64")
                    })?;
                    referenced_metadata_exists |= object.location == referenced;
                } else if object.location.as_ref().ends_with(".parquet") {
                    parquet_objects = parquet_objects.checked_add(1).ok_or_else(|| {
                        ObjectStoreFailure::listing("Parquet object count overflowed u64")
                    })?;
                    parquet_bytes = parquet_bytes.checked_add(size).ok_or_else(|| {
                        ObjectStoreFailure::listing("Parquet byte count overflowed u64")
                    })?;
                }
            }
            Ok(TableObjectAuditSnapshot {
                table_root: root.location,
                metadata_objects,
                metadata_bytes,
                parquet_objects,
                parquet_bytes,
                referenced_metadata_location: metadata_location,
                referenced_metadata_exists,
            })
        }
    }
}

impl Debug for ObjectStoreAuditor {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreAuditor")
            .field("store", &"<private>")
            .field("bucket", &self.bucket)
            .field("credentials", &"<redacted>")
            .finish()
    }
}

fn parse_s3_location(location: &str, label: &str) -> Result<(String, Path), ObjectStoreFailure> {
    let url = Url::parse(location)
        .map_err(|error| ObjectStoreFailure::configuration(format!("invalid {label}: {error}")))?;
    if url.scheme() != "s3"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ObjectStoreFailure::configuration(format!(
            "{label} must be a credential-free absolute s3 URI"
        )));
    }
    let path = Path::from_url_path(url.path()).map_err(|error| {
        ObjectStoreFailure::configuration(format!("invalid {label} path: {error}"))
    })?;
    Ok((
        url.host_str()
            .expect("validated host must exist")
            .to_owned(),
        path,
    ))
}

fn redact_and_bound(detail: &str, sensitive_values: &[String]) -> String {
    let redacted = sensitive_values
        .iter()
        .filter(|value| value.len() >= 4)
        .fold(detail.to_owned(), |text, value| {
            text.replace(value, "<redacted>")
        });
    let mut characters = redacted.chars();
    let bounded = characters
        .by_ref()
        .take(MAXIMUM_FAILURE_DETAIL_CHARACTERS)
        .collect::<String>();
    if characters.next().is_some() {
        format!("{bounded}…")
    } else {
        bounded
    }
}
