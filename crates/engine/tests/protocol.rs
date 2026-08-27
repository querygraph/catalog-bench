use catalog_bench_engine::{
    EngineEvent, EngineEventDecoder, EngineFailureCategory, EngineProtocolFailureKind, EngineStage,
    ENGINE_EVENT_PREFIX, MAXIMUM_ENGINE_EVENT_BYTES, MAXIMUM_ENGINE_STDOUT_BYTES,
};
use serde_json::json;

#[test]
fn accepts_complete_ordered_stream_and_grants_cleanup_only_after_absence() {
    let events = successful_events();
    let mut decoder = EngineEventDecoder::new();
    decoder.push(b"ordinary Spark log that is ignored\n");
    let encoded = encoded_events(&events);
    for chunk in encoded.chunks(7) {
        decoder.push(chunk);
    }
    let capture = decoder.finish();

    assert_eq!(capture.events, events);
    assert!(capture.failure.is_none());
    assert!(capture.cleanup_authorized());
    assert!(capture.completed());
    assert!(!capture.fixture_collision());
}

#[test]
fn collision_is_terminal_and_never_grants_cleanup_authority() {
    let events = successful_events()[..2]
        .iter()
        .cloned()
        .chain([EngineEvent::FixturePreflight { absent: false }])
        .collect::<Vec<_>>();
    let mut decoder = EngineEventDecoder::new();
    decoder.push(&encoded_events(&events));
    let capture = decoder.finish();

    assert!(capture.failure.is_none());
    assert!(capture.fixture_collision());
    assert!(!capture.cleanup_authorized());
    assert!(!capture.completed());
}

#[test]
fn ordered_failure_after_absence_preserves_cleanup_authority_without_raw_detail() {
    let events = successful_events()[..3]
        .iter()
        .cloned()
        .chain([EngineEvent::Failed {
            stage: EngineStage::CreateNamespace,
            category: EngineFailureCategory::Catalog,
        }])
        .collect::<Vec<_>>();
    let mut decoder = EngineEventDecoder::new();
    decoder.push(&encoded_events(&events));
    let capture = decoder.finish();

    assert!(capture.failure.is_none());
    assert!(capture.cleanup_authorized());
    assert_eq!(
        capture.engine_failure(),
        Some((EngineStage::CreateNamespace, EngineFailureCategory::Catalog))
    );
    assert!(!serde_json::to_string(&capture)
        .unwrap()
        .contains("exception"));
}

#[test]
fn rejects_malformed_oversized_out_of_order_and_post_terminal_events() {
    let cases = [
        (
            format!(
                "{}{{not-json}}\n",
                std::str::from_utf8(ENGINE_EVENT_PREFIX).unwrap()
            )
            .into_bytes(),
            EngineProtocolFailureKind::MalformedEvent,
        ),
        (
            vec![b'x'; MAXIMUM_ENGINE_EVENT_BYTES + 1],
            EngineProtocolFailureKind::EventLineTooLarge,
        ),
        (
            encoded_events(&[EngineEvent::CatalogReady]),
            EngineProtocolFailureKind::OutOfOrder,
        ),
        (
            encoded_events(
                &successful_events()[..2]
                    .iter()
                    .cloned()
                    .chain([
                        EngineEvent::FixturePreflight { absent: false },
                        EngineEvent::CatalogReady,
                    ])
                    .collect::<Vec<_>>(),
            ),
            EngineProtocolFailureKind::PostTerminal,
        ),
    ];
    for (bytes, expected) in cases {
        let mut decoder = EngineEventDecoder::new();
        decoder.push(&bytes);
        let capture = decoder.finish();
        assert_eq!(capture.failure.unwrap().kind, expected);
    }

    let mut decoder = EngineEventDecoder::new();
    decoder.push(&vec![b'x'; MAXIMUM_ENGINE_STDOUT_BYTES + 1]);
    assert_eq!(
        decoder.finish().failure.unwrap().kind,
        EngineProtocolFailureKind::StdoutTooLarge
    );
}

#[test]
fn malformed_data_after_trusted_absence_cannot_revoke_cleanup_ownership() {
    let mut decoder = EngineEventDecoder::new();
    decoder.push(&encoded_events(&successful_events()[..3]));
    decoder.push(
        format!(
            "{}{}\n",
            std::str::from_utf8(ENGINE_EVENT_PREFIX).unwrap(),
            json!({"event": "unknown"})
        )
        .as_bytes(),
    );
    let capture = decoder.finish();

    assert_eq!(
        capture.failure.as_ref().unwrap().kind,
        EngineProtocolFailureKind::MalformedEvent
    );
    assert!(capture.cleanup_authorized());
    assert!(!capture.completed());
}

#[test]
fn property_values_outside_the_closed_observation_adt_are_not_retained() {
    let private = "catalog-controlled-private-value";
    let mut decoder = EngineEventDecoder::new();
    decoder.push(&encoded_events(&successful_events()[..4]));
    let event = json!({
        "event": "table-ready",
        "table": {
            "table_uuid": "00000000-0000-0000-0000-000000000001",
            "metadata_location": "s3://warehouse/table/metadata/v1.metadata.json",
            "location": "s3://warehouse/table",
            "format_version": 2,
            "last_column_id": 3,
            "schema": [],
            "snapshots": 0,
            "properties": {"catalog-bench.owner": private}
        }
    });
    decoder.push(
        format!(
            "{}{}\n",
            std::str::from_utf8(ENGINE_EVENT_PREFIX).unwrap(),
            event
        )
        .as_bytes(),
    );
    let capture = decoder.finish();

    assert_eq!(
        capture.failure.as_ref().unwrap().kind,
        EngineProtocolFailureKind::MalformedEvent
    );
    assert!(capture.cleanup_authorized());
    assert!(!serde_json::to_string(&capture).unwrap().contains(private));
}

fn encoded_events(events: &[EngineEvent]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for event in events {
        bytes.extend_from_slice(ENGINE_EVENT_PREFIX);
        serde_json::to_writer(&mut bytes, event).unwrap();
        bytes.push(b'\n');
    }
    bytes
}

fn successful_events() -> Vec<EngineEvent> {
    use catalog_bench_engine::{
        EngineRuntimeObservation, EngineTableObservation, RowReadObservation,
    };

    let table = EngineTableObservation {
        table_uuid: "00000000-0000-0000-0000-000000000001".to_owned(),
        metadata_location: "s3://warehouse/table/metadata/00001.metadata.json".to_owned(),
        location: "s3://warehouse/table".to_owned(),
        format_version: 2,
        last_column_id: 3,
        schema: Vec::new(),
        snapshots: 0,
        properties: Default::default(),
    };
    vec![
        EngineEvent::RuntimeReady {
            runtime: EngineRuntimeObservation {
                spark_version: "4.1.3".to_owned(),
                scala_version: "2.13.17".to_owned(),
                java_version: "21.0.11".to_owned(),
                operating_system: "Linux".to_owned(),
                architecture: "aarch64".to_owned(),
            },
        },
        EngineEvent::CatalogReady,
        EngineEvent::FixturePreflight { absent: true },
        EngineEvent::NamespaceReady {
            listed_exactly: true,
        },
        EngineEvent::TableReady {
            table: table.clone(),
        },
        EngineEvent::InitialAppended { snapshots: 1 },
        EngineEvent::InitialRead {
            read: RowReadObservation {
                rows: 16,
                bytes: 346,
                sha256: "e".repeat(64),
            },
        },
        EngineEvent::SchemaEvolved {
            table: table.clone(),
        },
        EngineEvent::EvolvedAppended { snapshots: 2 },
        EngineEvent::EvolvedRead {
            read: RowReadObservation {
                rows: 20,
                bytes: 570,
                sha256: "b".repeat(64),
            },
        },
        EngineEvent::FinalTable { table },
        EngineEvent::Completed,
    ]
}
