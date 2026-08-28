use catalog_bench_common::contract::{parse_contract, ComponentId, ContractDocument};
use catalog_bench_engine::{
    decode_iceberg_table_metadata, EnginePropertyObservation, IcebergMetadataError,
    InteroperabilityPlan, TrinoRenderedProgram,
};
use serde_json::{json, Value};

mod support;

use support::select_synthetic_materialized_trino;

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const CANDIDATE_PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.v2.json");
const METADATA_LOCATION: &str =
    "s3://warehouse/lakecat/cb_c201_lakecat_metadata01/events/metadata/00001-test.metadata.json";

#[test]
fn projects_initial_and_evolved_v2_metadata_into_closed_evidence() {
    let program = program();
    let initial = metadata(&program, false, 0);
    let observed = decode(&program, &initial).unwrap();
    assert_eq!(observed.table_uuid, "00000000-0000-7000-8000-000000000001");
    assert_eq!(
        observed.location,
        program.fixture.requested_location.as_deref().unwrap()
    );
    assert_eq!(observed.last_column_id, 3);
    assert_eq!(observed.schema.len(), 3);
    assert_eq!(observed.snapshots, 0);
    assert!(observed
        .properties
        .values()
        .all(|value| *value == EnginePropertyObservation::Match));

    let evolved = metadata(&program, true, 2);
    let observed = decode(&program, &evolved).unwrap();
    assert_eq!(observed.last_column_id, 4);
    assert_eq!(observed.schema[3].name, "note");
    assert!(!observed.schema[3].required);
    assert_eq!(observed.snapshots, 2);
}

#[test]
fn reports_only_match_or_mismatch_for_scenario_owned_properties() {
    let program = program();
    let mut metadata = metadata(&program, false, 0);
    metadata["properties"]["catalog-bench.owner"] = json!("someone-else");
    metadata["properties"]["untrusted.secret"] = json!("must-not-be-projected");
    let observed = decode(&program, &metadata).unwrap();
    assert_eq!(
        observed.properties["catalog-bench.owner"],
        EnginePropertyObservation::Mismatch
    );
    assert!(!observed.properties.contains_key("untrusted.secret"));
}

#[test]
fn rejects_identity_location_schema_snapshot_and_property_drift() {
    let program = program();
    let mut cases = Vec::new();

    let mut uuid = metadata(&program, false, 0);
    uuid["table-uuid"] = json!("not-a-uuid");
    cases.push((uuid, IcebergMetadataError::InvalidIdentity));
    let mut location = metadata(&program, false, 0);
    location["location"] = json!("s3://another-bucket/table");
    cases.push((location, IcebergMetadataError::InvalidLocation));
    let mut schema = metadata(&program, false, 0);
    schema["schemas"][0]["fields"][0]["id"] = json!(99);
    cases.push((schema, IcebergMetadataError::InvalidSchema));
    let mut snapshots = metadata(&program, false, 0);
    snapshots["snapshots"] = json!({});
    cases.push((snapshots, IcebergMetadataError::InvalidSnapshots));
    let mut properties = metadata(&program, false, 0);
    properties["properties"]["catalog-bench.owner"] = json!(["not", "text"]);
    cases.push((properties, IcebergMetadataError::InvalidProperties));

    for (metadata, expected) in cases {
        assert_eq!(decode(&program, &metadata), Err(expected));
    }
    assert_eq!(
        decode_iceberg_table_metadata(
            &serde_json::to_vec(&metadata(&program, false, 0)).unwrap(),
            "s3://warehouse/not-metadata/data.parquet",
            &program.fixture,
            &program.observation,
        ),
        Err(IcebergMetadataError::InvalidLocation)
    );
}

#[test]
fn rejects_duplicate_keys_malformed_json_and_the_metadata_byte_limit() {
    let program = program();
    let encoded = serde_json::to_string(&metadata(&program, false, 0)).unwrap();
    let duplicate = encoded.replacen(
        '{',
        "{\"table-uuid\":\"00000000-0000-7000-8000-000000000099\",",
        1,
    );
    assert_eq!(
        decode_iceberg_table_metadata(
            duplicate.as_bytes(),
            METADATA_LOCATION,
            &program.fixture,
            &program.observation,
        ),
        Err(IcebergMetadataError::Malformed)
    );
    assert_eq!(
        decode_iceberg_table_metadata(
            b"{not-json}",
            METADATA_LOCATION,
            &program.fixture,
            &program.observation,
        ),
        Err(IcebergMetadataError::Malformed)
    );
    let oversized = vec![b' '; 4 * 1024 * 1024 + 1];
    assert_eq!(
        decode_iceberg_table_metadata(
            &oversized,
            METADATA_LOCATION,
            &program.fixture,
            &program.observation,
        ),
        Err(IcebergMetadataError::TooLarge)
    );
}

fn decode(
    program: &TrinoRenderedProgram,
    metadata: &Value,
) -> Result<catalog_bench_engine::EngineTableObservation, IcebergMetadataError> {
    decode_iceberg_table_metadata(
        &serde_json::to_vec(metadata).unwrap(),
        METADATA_LOCATION,
        &program.fixture,
        &program.observation,
    )
}

fn metadata(program: &TrinoRenderedProgram, evolved: bool, snapshots: usize) -> Value {
    let mut fields = program
        .observation
        .initial_schema
        .iter()
        .map(|field| {
            json!({
                "id": field.id,
                "name": field.name,
                "required": field.required,
                "type": match field.field_type {
                    catalog_bench_engine::IcebergPrimitiveType::Long => "long",
                    catalog_bench_engine::IcebergPrimitiveType::String => "string",
                },
            })
        })
        .collect::<Vec<_>>();
    if evolved {
        fields.push(json!({
            "id": 4,
            "name": program.observation.evolved_field.name,
            "required": program.observation.evolved_field.required,
            "type": "string",
        }));
    }
    let mut document = json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-7000-8000-000000000001",
        "location": program.fixture.requested_location,
        "last-column-id": if evolved { 4 } else { 3 },
        "current-schema-id": if evolved { 1 } else { 0 },
        "schemas": [{
            "schema-id": if evolved { 1 } else { 0 },
            "type": "struct",
            "fields": fields,
        }],
        "properties": program.observation.properties,
    });
    if snapshots > 0 {
        document["snapshots"] = Value::Array((0..snapshots).map(|_| json!({})).collect());
    }
    document
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
        "metadata01",
    )
    .unwrap();
    TrinoRenderedProgram::render(plan.trino().unwrap()).unwrap()
}
