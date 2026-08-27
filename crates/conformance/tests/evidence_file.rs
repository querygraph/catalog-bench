use std::fs;
use std::sync::{Arc, Barrier};

use catalog_bench_conformance::{write_new_evidence, EvidenceWriteFailureKind};

#[test]
fn publication_creates_parents_and_preserves_existing_evidence() {
    let root = tempfile::tempdir().unwrap();
    let output = root.path().join("nested/evidence.json");
    let original = b"{\"complete\":true}\n";

    write_new_evidence(&output, original).unwrap();
    let failure = write_new_evidence(&output, b"replacement").unwrap_err();

    assert_eq!(failure.kind(), EvidenceWriteFailureKind::Publish);
    assert_eq!(fs::read(&output).unwrap(), original);
    assert_eq!(fs::read_dir(output.parent().unwrap()).unwrap().count(), 1);
}

#[test]
fn concurrent_publication_exposes_one_complete_payload() {
    const WRITERS: usize = 16;

    let root = tempfile::tempdir().unwrap();
    let output = Arc::new(root.path().join("evidence.json"));
    let barrier = Arc::new(Barrier::new(WRITERS));
    let writers = (0..WRITERS)
        .map(|writer| {
            let output = Arc::clone(&output);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let payload = format!("{{\"writer\":{writer},\"complete\":true}}\n").into_bytes();
                barrier.wait();
                let result = write_new_evidence(&output, &payload);
                (payload, result)
            })
        })
        .collect::<Vec<_>>();
    let outcomes = writers
        .into_iter()
        .map(|writer| writer.join().unwrap())
        .collect::<Vec<_>>();
    let successful = outcomes
        .iter()
        .filter_map(|(payload, result)| result.is_ok().then_some(payload))
        .collect::<Vec<_>>();

    assert_eq!(successful.len(), 1);
    assert_eq!(fs::read(output.as_ref()).unwrap(), *successful[0]);
    assert!(outcomes
        .iter()
        .filter(|(_, result)| result.is_err())
        .all(
            |(_, result)| result.as_ref().unwrap_err().kind() == EvidenceWriteFailureKind::Publish
        ));
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
}
