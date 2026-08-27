use std::collections::BTreeMap;
use std::sync::Arc;

use catalog_bench_commit::protocol::TableSnapshot;
use catalog_bench_commit::store::{
    MetadataStore, ObjectStoreAuditor, ObjectStoreFailureKind, TableRoot,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};

#[tokio::test]
async fn recursive_table_root_audit_counts_only_metadata_and_finds_exact_pointer() {
    let store = Arc::new(InMemory::new());
    for (path, payload) in [
        ("warehouse/table/metadata/00000.metadata.json", &b"one"[..]),
        (
            "warehouse/table/metadata/nested/00001.metadata.json",
            &b"two-two"[..],
        ),
        ("warehouse/table/metadata/manifest.avro", &b"manifest"[..]),
        (
            "warehouse/table-sibling/metadata/99999.metadata.json",
            &b"sibling"[..],
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
        .audit(&root, &snapshot.metadata_location)
        .await
        .unwrap();
    assert_eq!(audit.metadata_objects, 2);
    assert_eq!(audit.metadata_bytes, 10);
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

fn snapshot(location: &str, metadata_location: &str) -> TableSnapshot {
    TableSnapshot {
        format_version: 2,
        table_uuid: "uuid".to_owned(),
        location: location.to_owned(),
        metadata_location: metadata_location.to_owned(),
        properties: BTreeMap::new(),
    }
}
