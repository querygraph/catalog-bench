use std::collections::BTreeMap;

use catalog_bench_commit::store::{TableObjectAuditSnapshot, TableRoot};
use uuid::Uuid;

use crate::runtime::{architecture_matches, operating_system_matches};
use crate::workflow::{EngineBehaviorChecks, EngineExecution, EngineOperationEvidence};
use crate::{
    EngineCatalogTable, EngineEvent, EngineFieldObservation, EnginePropertyObservation,
    EngineRuntimeObservation, EngineTableLoad, EngineTableObservation, IcebergField,
    InteroperabilityPlan, RowReadObservation, RuntimeArtifactOutcome, RuntimeVerification,
    SparkProcessExecution, SPARK_JAVA_VERSION, SPARK_SCALA_VERSION,
};

#[derive(Default)]
struct EventObservations<'a> {
    runtime: Option<&'a EngineRuntimeObservation>,
    catalog_ready: bool,
    fixture_absent: bool,
    namespace_listed_exactly: bool,
    table_ready: Option<&'a EngineTableObservation>,
    initial_snapshots: Option<u64>,
    initial_read: Option<&'a RowReadObservation>,
    schema_evolved: Option<&'a EngineTableObservation>,
    evolved_snapshots: Option<u64>,
    evolved_read: Option<&'a RowReadObservation>,
    final_table: Option<&'a EngineTableObservation>,
}

impl<'a> EventObservations<'a> {
    fn from_execution(execution: &'a SparkProcessExecution) -> Self {
        let mut observations = Self::default();
        let Some(capture) = &execution.capture else {
            return observations;
        };
        for event in &capture.events {
            match event {
                EngineEvent::RuntimeReady { runtime } => observations.runtime = Some(runtime),
                EngineEvent::CatalogReady => observations.catalog_ready = true,
                EngineEvent::FixturePreflight { absent } => {
                    observations.fixture_absent = *absent;
                }
                EngineEvent::NamespaceReady { listed_exactly } => {
                    observations.namespace_listed_exactly = *listed_exactly;
                }
                EngineEvent::TableReady { table } => observations.table_ready = Some(table),
                EngineEvent::InitialAppended { snapshots } => {
                    observations.initial_snapshots = Some(*snapshots);
                }
                EngineEvent::InitialRead { read } => observations.initial_read = Some(read),
                EngineEvent::SchemaEvolved { table } => {
                    observations.schema_evolved = Some(table);
                }
                EngineEvent::EvolvedAppended { snapshots } => {
                    observations.evolved_snapshots = Some(*snapshots);
                }
                EngineEvent::EvolvedRead { read } => observations.evolved_read = Some(read),
                EngineEvent::FinalTable { table } => observations.final_table = Some(table),
                EngineEvent::Completed | EngineEvent::Failed { .. } => {}
            }
        }
        observations
    }
}

pub(super) fn evaluate_checks(
    plan: &InteroperabilityPlan,
    execution: &EngineExecution,
) -> EngineBehaviorChecks {
    let events = EventObservations::from_execution(&execution.process);
    let initial_fields = expected_initial_fields(plan);
    let evolved_fields = expected_evolved_fields(plan);
    let initial_last_id = initial_fields.iter().map(|field| field.id).max();
    let evolved_last_id = evolved_fields
        .as_ref()
        .and_then(|fields| fields.iter().map(|field| field.id).max());
    let table_round_trip = events.table_ready.is_some_and(|table| {
        initial_last_id
            .is_some_and(|last_id| table_matches_shape(plan, table, &initial_fields, last_id, 0))
    });
    let schema_evolved = events.schema_evolved.is_some_and(|table| {
        evolved_fields.as_ref().is_some_and(|fields| {
            evolved_last_id.is_some_and(|last_id| {
                table_matches_shape(plan, table, fields, last_id, 1)
                    && events
                        .table_ready
                        .is_some_and(|initial| table_identity_preserved(initial, table))
            })
        })
    });
    let final_table_matches = events.final_table.is_some_and(|table| {
        evolved_fields.as_ref().is_some_and(|fields| {
            evolved_last_id.is_some_and(|last_id| {
                table_matches_shape(plan, table, fields, last_id, 2)
                    && events.schema_evolved.is_some_and(|evolved| {
                        table_identity_preserved(evolved, table)
                            && evolved.metadata_location != table.metadata_location
                    })
            })
        })
    });
    let catalog_state_correlated = final_table_matches
        && matches!(
            (events.final_table, execution.catalog_state.output()),
            (
                Some(final_table),
                Some(EngineTableLoad::Present {
                    state: EngineCatalogTable {
                        current_schema_id: 1,
                        table,
                    },
                    ..
                })
            ) if table == final_table
        );
    let shared_object_evidence_complete = catalog_state_correlated
        && object_evidence_complete(
            plan,
            execution.catalog_state.output(),
            &execution.object_state,
        );

    EngineBehaviorChecks {
        engine_runtime_pinned: runtime_verification_matches(plan, &execution.process.runtime)
            && events
                .runtime
                .is_some_and(|runtime| runtime_matches(plan, runtime)),
        stock_rest_catalog_ready: events.catalog_ready,
        fixture_isolated: events.fixture_absent,
        namespace_round_trip: events.namespace_listed_exactly,
        table_round_trip,
        initial_append_committed: events.initial_snapshots == Some(1),
        initial_read_exact: events
            .initial_read
            .is_some_and(|read| read_matches(read, &plan.spark().scenario.canonical_reads.initial)),
        schema_evolved,
        evolved_append_committed: events.evolved_snapshots == Some(2),
        evolved_read_exact: events.evolved_read.is_some_and(|read| {
            read_matches(read, &plan.spark().scenario.canonical_reads.after_evolution)
        }),
        catalog_state_correlated,
        shared_object_evidence_complete,
        fixture_clean: execution.cleanup.passed(),
    }
}

fn runtime_matches(plan: &InteroperabilityPlan, runtime: &EngineRuntimeObservation) -> bool {
    runtime.spark_version == plan.engine().version
        && runtime.scala_version == SPARK_SCALA_VERSION
        && runtime.java_version == SPARK_JAVA_VERSION
        && operating_system_matches(
            &plan.runtime_platform().operating_system,
            &runtime.operating_system,
        )
        && architecture_matches(&plan.runtime_platform().architecture, &runtime.architecture)
}

fn runtime_verification_matches(
    plan: &InteroperabilityPlan,
    verification: &RuntimeVerification,
) -> bool {
    let expected_platform = plan.runtime_platform();
    verification.platform.expected_operating_system == expected_platform.operating_system
        && verification.platform.expected_architecture == expected_platform.architecture
        && verification.platform.operating_system_matches
        && verification.platform.architecture_matches
        && verification.artifacts.len() == plan.runtime_artifacts().len()
        && verification
            .artifacts
            .iter()
            .zip(plan.runtime_artifacts())
            .all(|(observed, expected)| {
                observed.location == expected.location
                    && observed.media_type == expected.media_type
                    && observed.components == expected.components
                    && observed.expected_bytes == expected.bytes
                    && observed.expected_sha256 == expected.sha256
                    && matches!(
                        &observed.outcome,
                        RuntimeArtifactOutcome::Match {
                            observed_bytes,
                            observed_sha256,
                        } if *observed_bytes == expected.bytes
                            && observed_sha256 == &expected.sha256
                    )
            })
}

fn expected_initial_fields(plan: &InteroperabilityPlan) -> Vec<EngineFieldObservation> {
    plan.spark()
        .scenario
        .table
        .schema
        .fields
        .iter()
        .map(engine_field)
        .collect()
}

fn expected_evolved_fields(plan: &InteroperabilityPlan) -> Option<Vec<EngineFieldObservation>> {
    let mut fields = expected_initial_fields(plan);
    let id = fields.iter().map(|field| field.id).max()?.checked_add(1)?;
    let evolved = &plan.spark().scenario.schema_evolution.field;
    fields.push(EngineFieldObservation {
        id,
        name: evolved.name.clone(),
        required: evolved.required,
        field_type: evolved.field_type,
    });
    Some(fields)
}

fn engine_field(field: &IcebergField) -> EngineFieldObservation {
    EngineFieldObservation {
        id: field.id,
        name: field.name.clone(),
        required: field.required,
        field_type: field.field_type,
    }
}

fn table_matches_shape(
    plan: &InteroperabilityPlan,
    table: &EngineTableObservation,
    fields: &[EngineFieldObservation],
    last_column_id: i32,
    snapshots: u64,
) -> bool {
    let expected_properties = plan
        .spark()
        .scenario
        .table
        .properties
        .keys()
        .map(|key| (key.clone(), EnginePropertyObservation::Match))
        .collect::<BTreeMap<_, _>>();
    table.format_version == 2
        && table.last_column_id == last_column_id
        && table.schema == fields
        && table.snapshots == snapshots
        && table.properties == expected_properties
        && Uuid::parse_str(&table.table_uuid).is_ok_and(|uuid| !uuid.is_nil())
        && TableRoot::new(
            &table.location,
            &table.metadata_location,
            &plan.object_store().bucket,
        )
        .is_ok()
        && plan
            .spark()
            .fixture
            .requested_location
            .as_ref()
            .is_none_or(|location| location == &table.location)
}

fn table_identity_preserved(
    previous: &EngineTableObservation,
    current: &EngineTableObservation,
) -> bool {
    previous.table_uuid == current.table_uuid
        && previous.location == current.location
        && previous.metadata_location != current.metadata_location
}

fn read_matches(read: &RowReadObservation, expected: &crate::CanonicalRead) -> bool {
    read.rows == expected.rows && read.bytes == expected.bytes && read.sha256 == expected.sha256
}

fn object_evidence_complete(
    plan: &InteroperabilityPlan,
    catalog_state: Option<&EngineTableLoad>,
    evidence: &EngineOperationEvidence<TableObjectAuditSnapshot>,
) -> bool {
    let (Some(EngineTableLoad::Present { state, .. }), Some(audit)) =
        (catalog_state, evidence.output())
    else {
        return false;
    };
    let policy = &plan.spark().scenario.object_audit;
    audit.table_root == state.table.location
        && audit.referenced_metadata_location == state.table.metadata_location
        && audit.referenced_metadata_exists
        && audit.metadata_objects >= policy.minimum_metadata_objects
        && audit.metadata_bytes > 0
        && audit.parquet_objects >= policy.minimum_parquet_objects
        && audit.parquet_bytes > 0
}
