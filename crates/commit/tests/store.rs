use std::collections::BTreeMap;
use std::sync::Arc;

use catalog_bench_commit::protocol::TableSnapshot;
use catalog_bench_commit::store::{
    MetadataStore, ObjectStoreAuditor, ObjectStoreConnectionPolicy, ObjectStoreFailureKind,
    TableObjectStore, TableRoot,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};

#[tokio::test]
async fn recursive_table_root_audit_counts_metadata_and_parquet_and_finds_exact_pointer() {
    let store = Arc::new(InMemory::new());
    for (path, payload) in [
        ("warehouse/table/metadata/00000.metadata.json", &b"one"[..]),
        (
            "warehouse/table/metadata/nested/00001.metadata.json",
            &b"two-two"[..],
        ),
        ("warehouse/table/metadata/manifest.avro", &b"manifest"[..]),
        ("warehouse/table/data/part-00000.parquet", &b"data"[..]),
        (
            "warehouse/table/data/nested/part-00001.parquet",
            &b"more-data"[..],
        ),
        (
            "warehouse/table-sibling/metadata/99999.metadata.json",
            &b"sibling"[..],
        ),
        (
            "warehouse/table-sibling/data/part-99999.parquet",
            &b"sibling-data"[..],
        ),
    ] {
        store
            .put(
                &Path::parse(path).unwrap(),
                PutPayload::from(payload.to_vec()),
            )
            .await
            .unwrap();
    }
    let snapshot = snapshot(
        "s3://warehouse/warehouse/table",
        "s3://warehouse/warehouse/table/metadata/nested/00001.metadata.json",
    );
    let root = TableRoot::from_snapshot(&snapshot, "warehouse").unwrap();
    let auditor = ObjectStoreAuditor::for_store(store, "warehouse");

    let audit = auditor
        .audit_table(&root, &snapshot.metadata_location)
        .await
        .unwrap();
    assert_eq!(audit.metadata_objects, 2);
    assert_eq!(audit.metadata_bytes, 10);
    assert_eq!(audit.parquet_objects, 2);
    assert_eq!(audit.parquet_bytes, 13);
    assert!(audit.referenced_metadata_exists);
    assert_eq!(audit.table_root, snapshot.location);
}

#[tokio::test]
async fn audit_reports_missing_pointer_without_fabricating_an_object() {
    let store = Arc::new(InMemory::new());
    store
        .put(
            &Path::parse("root/table/metadata/00000.metadata.json").unwrap(),
            PutPayload::from_static(b"metadata"),
        )
        .await
        .unwrap();
    let snapshot = snapshot(
        "s3://warehouse/root/table",
        "s3://warehouse/root/table/metadata/missing.metadata.json",
    );
    let root = TableRoot::from_snapshot(&snapshot, "warehouse").unwrap();
    let audit = ObjectStoreAuditor::for_store(store, "warehouse")
        .audit(&root, &snapshot.metadata_location)
        .await
        .unwrap();

    assert_eq!(audit.metadata_objects, 1);
    assert!(!audit.referenced_metadata_exists);
}

#[tokio::test]
async fn metadata_read_is_bounded_and_confined_to_the_validated_table_root() {
    let store = Arc::new(InMemory::new());
    let path = Path::parse("root/table/metadata/00000.metadata.json").unwrap();
    store
        .put(&path, PutPayload::from_static(b"metadata"))
        .await
        .unwrap();
    let root = TableRoot::new(
        "s3://warehouse/root/table",
        "s3://warehouse/root/table/metadata/00000.metadata.json",
        "warehouse",
    )
    .unwrap();
    let auditor = ObjectStoreAuditor::for_store(store, "warehouse");

    assert_eq!(
        auditor
            .read_metadata(
                &root,
                "s3://warehouse/root/table/metadata/00000.metadata.json",
                8,
            )
            .await
            .unwrap(),
        b"metadata"
    );
    assert_eq!(
        auditor
            .read_metadata(
                &root,
                "s3://warehouse/root/table/metadata/00000.metadata.json",
                7,
            )
            .await
            .unwrap_err()
            .kind,
        ObjectStoreFailureKind::Read
    );
    assert!(auditor
        .read_metadata(
            &root,
            "s3://warehouse/root/sibling/metadata/00000.metadata.json",
            8,
        )
        .await
        .is_err());
}

#[test]
fn table_root_rejects_bucket_drift_escape_and_nonmetadata_pointers() {
    let wrong_bucket = snapshot(
        "s3://other/root/table",
        "s3://other/root/table/metadata/00000.metadata.json",
    );
    assert_eq!(
        TableRoot::from_snapshot(&wrong_bucket, "warehouse")
            .unwrap_err()
            .kind,
        ObjectStoreFailureKind::Configuration
    );

    let escaped = snapshot(
        "s3://warehouse/root/table",
        "s3://warehouse/root/sibling/metadata/00000.metadata.json",
    );
    assert!(TableRoot::from_snapshot(&escaped, "warehouse").is_err());

    let manifest = snapshot(
        "s3://warehouse/root/table",
        "s3://warehouse/root/table/metadata/manifest.avro",
    );
    assert!(TableRoot::from_snapshot(&manifest, "warehouse").is_err());

    let traversal = snapshot(
        "s3://warehouse/root/table",
        "s3://warehouse/root/table/%2E%2E/escape.metadata.json",
    );
    assert!(TableRoot::from_snapshot(&traversal, "warehouse").is_err());
}

#[test]
fn generic_table_root_constructor_matches_snapshot_adapter() {
    let snapshot = snapshot(
        "s3://warehouse/root/table",
        "s3://warehouse/root/table/metadata/00000.metadata.json",
    );
    assert_eq!(
        TableRoot::new(&snapshot.location, &snapshot.metadata_location, "warehouse").unwrap(),
        TableRoot::from_snapshot(&snapshot, "warehouse").unwrap()
    );
}

#[test]
fn generic_connection_policy_builds_without_exposing_credentials() {
    let auditor = ObjectStoreAuditor::from_connection(&TestConnection, |name| match name {
        "TEST_ACCESS" => Some("example-access-value".to_owned()),
        "TEST_SECRET" => Some("example-secret-value".to_owned()),
        _ => None,
    })
    .unwrap();
    let debug = format!("{auditor:?}");
    assert!(debug.contains("warehouse"));
    assert!(!debug.contains("example-access-value"));
    assert!(!debug.contains("example-secret-value"));
}

struct TestConnection;

impl ObjectStoreConnectionPolicy for TestConnection {
    fn endpoint(&self) -> &str {
        "http://minio:9000"
    }

    fn bucket(&self) -> &str {
        "warehouse"
    }

    fn region(&self) -> &str {
        "us-east-1"
    }

    fn allow_http(&self) -> bool {
        true
    }

    fn path_style_access(&self) -> bool {
        true
    }

    fn access_key_env(&self) -> &str {
        "TEST_ACCESS"
    }

    fn secret_key_env(&self) -> &str {
        "TEST_SECRET"
    }
}

fn snapshot(location: &str, metadata_location: &str) -> TableSnapshot {
    TableSnapshot {
        format_version: 2,
        table_uuid: "uuid".to_owned(),
        location: location.to_owned(),
        metadata_location: metadata_location.to_owned(),
        properties: BTreeMap::new(),
    }
}
