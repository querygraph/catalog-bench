use std::collections::BTreeMap;

use catalog_bench_common::contract::{parse_contract, ComponentId, ContractDocument};
use catalog_bench_engine::{
    run_trino_child, EngineEvent, EngineEventDecoder, EngineFailureCategory,
    EngineFieldObservation, EnginePropertyObservation, EngineRuntimeObservation, EngineStage,
    EngineTableObservation, InteroperabilityPlan, RowReadObservation, TrinoCatalogSetup,
    TrinoEffectFailure, TrinoEffects, TrinoFixtureTarget, TrinoObservationPolicy, TrinoOperation,
    TrinoRenderedProgram, TrinoServerConfiguration, ENGINE_EVENT_PREFIX, TRINO_FAILURE_EXIT,
    TRINO_FIXTURE_COLLISION_EXIT, TRINO_SUCCESS_EXIT,
};

mod support;

use support::select_synthetic_materialized_trino;

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const CANDIDATE_PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.v2.json");

#[test]
fn complete_run_emits_the_closed_event_order() {
    let program = program();
    let mut effects = FakeEffects::default();
    let run = run_trino_child(&program, &mut effects);

    assert_eq!(run.exit_code, TRINO_SUCCESS_EXIT);
    assert!(decode(&run.events).failure.is_none());
    assert!(matches!(run.events.last(), Some(EngineEvent::Completed)));
    assert_eq!(run.events.len(), 12);
    assert!(matches!(
        run.events.as_slice(),
        [
            EngineEvent::RuntimeReady { .. },
            EngineEvent::CatalogReady,
            EngineEvent::FixturePreflight { absent: true },
            EngineEvent::NamespaceReady {
                listed_exactly: true
            },
            EngineEvent::TableReady { .. },
            EngineEvent::InitialAppended { snapshots: 1 },
            EngineEvent::InitialRead { .. },
            EngineEvent::SchemaEvolved { .. },
            EngineEvent::EvolvedAppended { snapshots: 2 },
            EngineEvent::EvolvedRead { .. },
            EngineEvent::FinalTable { .. },
            EngineEvent::Completed,
        ]
    ));
    assert_eq!(
        effects.calls,
        [
            "runtime",
            "initialize",
            "preflight",
            "create-namespace",
            "list-namespace",
            "create-table",
            "observe",
            "initial-append",
            "snapshots",
            "initial-read",
            "add-column",
            "observe",
            "evolved-append",
            "snapshots",
            "evolved-read",
            "observe",
        ]
    );
}

#[test]
fn collision_is_terminal_before_any_mutation() {
    let program = program();
    let mut effects = FakeEffects {
        absent: false,
        ..FakeEffects::default()
    };
    let run = run_trino_child(&program, &mut effects);

    assert_eq!(run.exit_code, TRINO_FIXTURE_COLLISION_EXIT);
    assert!(decode(&run.events).failure.is_none());
    assert!(matches!(
        run.events.last(),
        Some(EngineEvent::FixturePreflight { absent: false })
    ));
    assert_eq!(effects.calls, ["runtime", "initialize", "preflight"]);
}

#[test]
fn read_mismatch_fails_closed_before_evolution() {
    let program = program();
    let mut effects = FakeEffects {
        mismatch_read: true,
        ..FakeEffects::default()
    };
    let run = run_trino_child(&program, &mut effects);

    assert_eq!(run.exit_code, TRINO_FAILURE_EXIT);
    assert!(decode(&run.events).failure.is_none());
    assert!(matches!(
        run.events.last(),
        Some(EngineEvent::Failed {
            stage: EngineStage::ReadInitial,
            category: EngineFailureCategory::Data,
        })
    ));
    assert!(!effects.calls.contains(&"add-column"));
}

#[test]
fn malformed_operation_order_and_effect_failure_never_panic_or_leak_detail() {
    let mut malformed = program();
    malformed.operations.swap(0, 1);
    let mut effects = FakeEffects::default();
    let run = run_trino_child(&malformed, &mut effects);
    assert_eq!(run.exit_code, TRINO_FAILURE_EXIT);
    assert!(decode(&run.events).failure.is_none());
    assert_eq!(effects.calls, ["runtime"]);

    let program = program();
    let mut effects = FakeEffects {
        fail_at: Some("list-namespace"),
        ..FakeEffects::default()
    };
    let run = run_trino_child(&program, &mut effects);
    assert!(matches!(
        run.events.last(),
        Some(EngineEvent::Failed {
            stage: EngineStage::CreateNamespace,
            category: EngineFailureCategory::Catalog,
        })
    ));
    let encoded = serde_json::to_string(&run.events).unwrap();
    assert!(!encoded.contains("list-namespace"));
}

struct FakeEffects {
    calls: Vec<&'static str>,
    absent: bool,
    mismatch_read: bool,
    fail_at: Option<&'static str>,
    snapshots: u64,
}

impl Default for FakeEffects {
    fn default() -> Self {
        Self {
            calls: Vec::new(),
            absent: true,
            mismatch_read: false,
            fail_at: None,
            snapshots: 0,
        }
    }
}

impl FakeEffects {
    fn call(&mut self, name: &'static str) -> Result<(), TrinoEffectFailure> {
        self.calls.push(name);
        if self.fail_at == Some(name) {
            Err(TrinoEffectFailure)
        } else {
            Ok(())
        }
    }
}

impl TrinoEffects for FakeEffects {
    fn runtime_observation(&mut self) -> Result<EngineRuntimeObservation, TrinoEffectFailure> {
        self.call("runtime")?;
        Ok(EngineRuntimeObservation {
            engine_version: "483".to_owned(),
            dependencies: BTreeMap::from([("java".to_owned(), "25.0.3".to_owned())]),
            operating_system: "Linux".to_owned(),
            architecture: "aarch64".to_owned(),
        })
    }

    fn initialize_catalog(
        &mut self,
        _: &TrinoCatalogSetup,
        _: &TrinoServerConfiguration,
    ) -> Result<(), TrinoEffectFailure> {
        self.call("initialize")
    }

    fn fixture_absent(&mut self, _: &TrinoFixtureTarget) -> Result<bool, TrinoEffectFailure> {
        self.call("preflight")?;
        Ok(self.absent)
    }

    fn execute(&mut self, operation: &TrinoOperation) -> Result<(), TrinoEffectFailure> {
        let name = match operation {
            TrinoOperation::CreateNamespace { .. } => "create-namespace",
            TrinoOperation::CreateTable { .. } => "create-table",
            TrinoOperation::InitialAppend { .. } => "initial-append",
            TrinoOperation::AddColumn { .. } => "add-column",
            TrinoOperation::EvolvedAppend { .. } => "evolved-append",
            TrinoOperation::InitialRead { .. }
            | TrinoOperation::EvolvedRead { .. }
            | TrinoOperation::SnapshotRead { .. } => return Err(TrinoEffectFailure),
        };
        self.call(name)
    }

    fn namespace_listed_exactly(
        &mut self,
        _: &TrinoFixtureTarget,
    ) -> Result<bool, TrinoEffectFailure> {
        self.call("list-namespace")?;
        Ok(true)
    }

    fn observe_table(
        &mut self,
        _: &TrinoFixtureTarget,
        _: &TrinoObservationPolicy,
    ) -> Result<EngineTableObservation, TrinoEffectFailure> {
        self.call("observe")?;
        Ok(table())
    }

    fn read(
        &mut self,
        operation: &TrinoOperation,
    ) -> Result<RowReadObservation, TrinoEffectFailure> {
        let (name, expected) = match operation {
            TrinoOperation::InitialRead { expected, .. } => ("initial-read", expected),
            TrinoOperation::EvolvedRead { expected, .. } => ("evolved-read", expected),
            _ => return Err(TrinoEffectFailure),
        };
        self.call(name)?;
        Ok(RowReadObservation {
            rows: expected.rows,
            bytes: expected.bytes,
            sha256: if self.mismatch_read {
                "0".repeat(64)
            } else {
                expected.sha256.clone()
            },
        })
    }

    fn snapshot_count(&mut self, _: &TrinoOperation) -> Result<u64, TrinoEffectFailure> {
        self.call("snapshots")?;
        self.snapshots += 1;
        Ok(self.snapshots)
    }
}

fn table() -> EngineTableObservation {
    EngineTableObservation {
        table_uuid: "00000000-0000-7000-8000-000000000001".to_owned(),
        metadata_location: "s3://warehouse/table/metadata/v1.json".to_owned(),
        location: "s3://warehouse/table".to_owned(),
        format_version: 2,
        last_column_id: 4,
        schema: vec![EngineFieldObservation {
            id: 1,
            name: "id".to_owned(),
            required: true,
            field_type: catalog_bench_engine::IcebergPrimitiveType::Long,
        }],
        snapshots: 2,
        properties: BTreeMap::from([(
            "catalog-bench.owner".to_owned(),
            EnginePropertyObservation::Match,
        )]),
    }
}

fn program() -> TrinoRenderedProgram {
    let ContractDocument::Profile(mut profile) = parse_contract(PROFILE).unwrap() else {
        panic!("profile fixture must be a profile");
    };
    let ContractDocument::Profile(candidate) = parse_contract(CANDIDATE_PROFILE).unwrap() else {
        panic!("candidate fixture must be a profile");
    };
    let ContractDocument::Scenario(scenario) = parse_contract(SCENARIO).unwrap() else {
        panic!("scenario fixture must be a scenario");
    };
    select_synthetic_materialized_trino(&mut profile, &candidate);
    let plan = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "state01",
    )
    .unwrap();
    TrinoRenderedProgram::render(plan.trino().unwrap()).unwrap()
}

fn decode(events: &[EngineEvent]) -> catalog_bench_engine::EngineEventCapture {
    let mut decoder = EngineEventDecoder::new();
    for event in events {
        let mut line = ENGINE_EVENT_PREFIX.to_vec();
        line.extend(serde_json::to_vec(event).unwrap());
        line.push(b'\n');
        decoder.push(&line);
    }
    decoder.finish()
}
