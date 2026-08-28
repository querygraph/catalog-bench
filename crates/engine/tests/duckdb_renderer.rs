use catalog_bench_common::contract::{parse_contract, ComponentId, ContractDocument};
use catalog_bench_engine::{
    DuckDbExecutionPlan, DuckDbOperation, DuckDbOperationPurpose, DuckDbRenderedProgram,
    InteroperabilityPlan, DUCKDB_PLAN_FORMAT,
};

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.v2.json");
const RENDERER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/duckdb.rs");

#[test]
fn renders_the_complete_catalog_neutral_duckdb_program() {
    let ContractDocument::Profile(profile) = parse_contract(PROFILE).unwrap() else {
        panic!()
    };
    let ContractDocument::Scenario(scenario) = parse_contract(SCENARIO).unwrap() else {
        panic!()
    };
    let source = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "duck01",
    )
    .unwrap();
    let spark = source.spark().unwrap();
    let program = DuckDbRenderedProgram::render(&DuckDbExecutionPlan {
        format: DUCKDB_PLAN_FORMAT.to_owned(),
        catalog: spark.catalog.clone(),
        file_io: spark.file_io.clone(),
        fixture: spark.fixture.clone(),
        scenario: spark.scenario.clone(),
    })
    .unwrap();
    assert_eq!(program.catalog_name, "bench");
    assert_eq!(
        program
            .operations
            .iter()
            .map(DuckDbOperation::purpose)
            .collect::<Vec<_>>(),
        [
            DuckDbOperationPurpose::CreateNamespace,
            DuckDbOperationPurpose::CreateTable,
            DuckDbOperationPurpose::InitialAppend,
            DuckDbOperationPurpose::InitialRead,
            DuckDbOperationPurpose::AddColumn,
            DuckDbOperationPurpose::EvolvedAppend,
            DuckDbOperationPurpose::EvolvedRead,
            DuckDbOperationPurpose::SnapshotRead,
        ]
    );
    let create = statement(&program, DuckDbOperationPurpose::CreateTable);
    assert!(create.contains("'format-version' = 2"));
    assert!(create.contains("'catalog-bench.owner'"));
    assert!(statement(&program, DuckDbOperationPurpose::AddColumn)
        .contains("ADD COLUMN \"note\" VARCHAR"));
    assert!(matches!(
        program.operations.last(),
        Some(DuckDbOperation::SnapshotRead)
    ));
    let encoded = serde_json::to_string(&program).unwrap();
    assert!(!encoded.contains("SECRET_ACCESS_KEY"));
    assert!(!encoded.contains("CLIENT_SECRET"));
}

#[test]
fn renderer_has_no_catalog_specific_or_rest_client_branches() {
    let source = std::fs::read_to_string(RENDERER).unwrap();
    for forbidden in [
        "polaris",
        "gravitino",
        "lakekeeper",
        "reqwest",
        "namespaces/",
        "tables/",
    ] {
        assert!(!source.to_ascii_lowercase().contains(forbidden));
    }
}

fn statement(program: &DuckDbRenderedProgram, purpose: DuckDbOperationPurpose) -> &str {
    program
        .operations
        .iter()
        .find(|operation| operation.purpose() == purpose)
        .and_then(DuckDbOperation::sql)
        .unwrap()
}
