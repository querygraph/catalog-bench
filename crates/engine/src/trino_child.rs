//! Deterministic orchestration for the stock Trino child effects.

use crate::{
    EngineEvent, EngineFailureCategory, EngineRuntimeObservation, EngineStage,
    EngineTableObservation, RowReadObservation, TrinoCatalogSetup, TrinoFixtureTarget,
    TrinoObservationPolicy, TrinoOperation, TrinoOperationPurpose, TrinoRenderedProgram,
    TrinoServerConfiguration,
};

pub const TRINO_SUCCESS_EXIT: i32 = 0;
pub const TRINO_FAILURE_EXIT: i32 = 2;
pub const TRINO_FIXTURE_COLLISION_EXIT: i32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrinoEffectFailure;

pub trait TrinoEffects {
    fn runtime_observation(&mut self) -> Result<EngineRuntimeObservation, TrinoEffectFailure>;
    fn initialize_catalog(
        &mut self,
        catalog: &TrinoCatalogSetup,
        configuration: &TrinoServerConfiguration,
    ) -> Result<(), TrinoEffectFailure>;
    fn fixture_absent(&mut self, fixture: &TrinoFixtureTarget) -> Result<bool, TrinoEffectFailure>;
    fn execute(&mut self, operation: &TrinoOperation) -> Result<(), TrinoEffectFailure>;
    fn namespace_listed_exactly(
        &mut self,
        fixture: &TrinoFixtureTarget,
    ) -> Result<bool, TrinoEffectFailure>;
    fn observe_table(
        &mut self,
        fixture: &TrinoFixtureTarget,
        policy: &TrinoObservationPolicy,
    ) -> Result<EngineTableObservation, TrinoEffectFailure>;
    fn read(
        &mut self,
        operation: &TrinoOperation,
    ) -> Result<RowReadObservation, TrinoEffectFailure>;
    fn snapshot_count(&mut self, operation: &TrinoOperation) -> Result<u64, TrinoEffectFailure>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrinoChildRun {
    pub exit_code: i32,
    pub events: Vec<EngineEvent>,
}

pub fn run_trino_child(
    program: &TrinoRenderedProgram,
    effects: &mut impl TrinoEffects,
) -> TrinoChildRun {
    let mut events = Vec::with_capacity(13);
    let runtime = match effects.runtime_observation() {
        Ok(runtime) => runtime,
        Err(_) => {
            return failed(
                events,
                EngineStage::VerifyRuntime,
                EngineFailureCategory::Runtime,
            )
        }
    };
    events.push(EngineEvent::RuntimeReady { runtime });
    if !valid_operation_order(&program.operations) {
        return failed(
            events,
            EngineStage::InitializeCatalog,
            EngineFailureCategory::Connector,
        );
    }
    let configuration = match TrinoServerConfiguration::render(program) {
        Ok(configuration) => configuration,
        Err(_) => {
            return failed(
                events,
                EngineStage::InitializeCatalog,
                EngineFailureCategory::Connector,
            )
        }
    };
    if effects
        .initialize_catalog(&program.catalog, &configuration)
        .is_err()
    {
        return failed(
            events,
            EngineStage::InitializeCatalog,
            EngineFailureCategory::Connector,
        );
    }
    events.push(EngineEvent::CatalogReady);
    let absent = match effects.fixture_absent(&program.fixture) {
        Ok(absent) => absent,
        Err(_) => {
            return failed(
                events,
                EngineStage::PreflightFixture,
                EngineFailureCategory::Catalog,
            )
        }
    };
    events.push(EngineEvent::FixturePreflight { absent });
    if !absent {
        return TrinoChildRun {
            exit_code: TRINO_FIXTURE_COLLISION_EXIT,
            events,
        };
    }

    if effects.execute(&program.operations[0]).is_err()
        || !matches!(effects.namespace_listed_exactly(&program.fixture), Ok(true))
    {
        return failed(
            events,
            EngineStage::CreateNamespace,
            EngineFailureCategory::Catalog,
        );
    }
    events.push(EngineEvent::NamespaceReady {
        listed_exactly: true,
    });
    if effects.execute(&program.operations[1]).is_err() {
        return failed(
            events,
            EngineStage::CreateTable,
            EngineFailureCategory::Connector,
        );
    }
    let table = match effects.observe_table(&program.fixture, &program.observation) {
        Ok(table) => table,
        Err(_) => {
            return failed(
                events,
                EngineStage::CreateTable,
                EngineFailureCategory::Connector,
            )
        }
    };
    events.push(EngineEvent::TableReady { table });
    if effects.execute(&program.operations[2]).is_err() {
        return failed(
            events,
            EngineStage::AppendInitial,
            EngineFailureCategory::Data,
        );
    }
    let snapshots = match effects.snapshot_count(&program.operations[7]) {
        Ok(snapshots) => snapshots,
        Err(_) => {
            return failed(
                events,
                EngineStage::AppendInitial,
                EngineFailureCategory::Data,
            )
        }
    };
    events.push(EngineEvent::InitialAppended { snapshots });
    let expected = expected_read(&program.operations[3]);
    if !matches!(effects.read(&program.operations[3]), Ok(ref read) if read == &expected) {
        return failed(
            events,
            EngineStage::ReadInitial,
            EngineFailureCategory::Data,
        );
    }
    events.push(EngineEvent::InitialRead { read: expected });
    if effects.execute(&program.operations[4]).is_err() {
        return failed(
            events,
            EngineStage::EvolveSchema,
            EngineFailureCategory::Connector,
        );
    }
    let table = match effects.observe_table(&program.fixture, &program.observation) {
        Ok(table) => table,
        Err(_) => {
            return failed(
                events,
                EngineStage::EvolveSchema,
                EngineFailureCategory::Connector,
            )
        }
    };
    events.push(EngineEvent::SchemaEvolved { table });
    if effects.execute(&program.operations[5]).is_err() {
        return failed(
            events,
            EngineStage::AppendEvolved,
            EngineFailureCategory::Data,
        );
    }
    let snapshots = match effects.snapshot_count(&program.operations[7]) {
        Ok(snapshots) => snapshots,
        Err(_) => {
            return failed(
                events,
                EngineStage::AppendEvolved,
                EngineFailureCategory::Data,
            )
        }
    };
    events.push(EngineEvent::EvolvedAppended { snapshots });
    let expected = expected_read(&program.operations[6]);
    if !matches!(effects.read(&program.operations[6]), Ok(ref read) if read == &expected) {
        return failed(
            events,
            EngineStage::ReadEvolved,
            EngineFailureCategory::Data,
        );
    }
    events.push(EngineEvent::EvolvedRead { read: expected });
    let table = match effects.observe_table(&program.fixture, &program.observation) {
        Ok(table) => table,
        Err(_) => {
            return failed(
                events,
                EngineStage::ObserveFinalTable,
                EngineFailureCategory::Connector,
            )
        }
    };
    events.push(EngineEvent::FinalTable { table });
    events.push(EngineEvent::Completed);
    TrinoChildRun {
        exit_code: TRINO_SUCCESS_EXIT,
        events,
    }
}

fn valid_operation_order(operations: &[TrinoOperation]) -> bool {
    operations.iter().map(TrinoOperation::purpose).eq([
        TrinoOperationPurpose::CreateNamespace,
        TrinoOperationPurpose::CreateTable,
        TrinoOperationPurpose::InitialAppend,
        TrinoOperationPurpose::InitialRead,
        TrinoOperationPurpose::AddColumn,
        TrinoOperationPurpose::EvolvedAppend,
        TrinoOperationPurpose::EvolvedRead,
        TrinoOperationPurpose::SnapshotRead,
    ])
}

fn expected_read(operation: &TrinoOperation) -> RowReadObservation {
    let expected = match operation {
        TrinoOperation::InitialRead { expected, .. }
        | TrinoOperation::EvolvedRead { expected, .. } => expected,
        _ => unreachable!("renderer fixes read operations at indices three and six"),
    };
    RowReadObservation {
        rows: expected.rows,
        bytes: expected.bytes,
        sha256: expected.sha256.clone(),
    }
}

fn failed(
    mut events: Vec<EngineEvent>,
    stage: EngineStage,
    category: EngineFailureCategory,
) -> TrinoChildRun {
    events.push(EngineEvent::Failed { stage, category });
    TrinoChildRun {
        exit_code: TRINO_FAILURE_EXIT,
        events,
    }
}
