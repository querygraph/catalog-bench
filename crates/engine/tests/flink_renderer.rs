use std::fs;

use catalog_bench_common::contract::{
    parse_contract, ComponentId, ContractDocument, Profile, Scenario,
};
use catalog_bench_engine::{
    EngineCatalogAuthentication, FlinkRenderedProgram, FlinkStatementPurpose, InteroperabilityPlan,
};

mod support;

use support::select_synthetic_materialized_flink;

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const CANDIDATE_PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.v2.json");
const RENDERER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/flink.rs");

#[test]
fn renders_the_complete_stock_flink_program_for_every_catalog() {
    let (profile, scenario) = contracts();
    for catalog in ["lakecat", "polaris", "gravitino", "lakekeeper"] {
        let plan = InteroperabilityPlan::from_contracts(
            &profile,
            &scenario,
            &ComponentId::from(catalog),
            "render01",
        )
        .unwrap();
        let program = FlinkRenderedProgram::render(plan.flink().unwrap()).unwrap();

        assert_eq!(program.parallelism, 1);
        assert_eq!(program.catalog.properties["type"], "iceberg");
        assert_eq!(program.catalog.properties["catalog-type"], "rest");
        assert_eq!(
            program.catalog.properties["io-impl"],
            "org.apache.iceberg.aws.s3.S3FileIO"
        );
        assert_eq!(
            program
                .statements
                .iter()
                .map(|statement| statement.purpose)
                .collect::<Vec<_>>(),
            [
                FlinkStatementPurpose::CreateNamespace,
                FlinkStatementPurpose::CreateTable,
                FlinkStatementPurpose::InitialAppend,
                FlinkStatementPurpose::InitialRead,
                FlinkStatementPurpose::AddColumn,
                FlinkStatementPurpose::EvolvedAppend,
                FlinkStatementPurpose::EvolvedRead,
                FlinkStatementPurpose::SnapshotRead,
            ]
        );
        let add_column = statement(&program, FlinkStatementPurpose::AddColumn);
        assert!(add_column.ends_with("ADD `note` STRING"));
        assert!(!add_column.contains("NOT NULL"));
        let initial_append = statement(&program, FlinkStatementPurpose::InitialAppend);
        assert_eq!(initial_append.matches("), (").count(), 15);
        assert!(initial_append.contains("(0, 'category-0', 7)"));
        assert!(initial_append.contains("(15, 'category-3', 1507)"));
        let evolved_append = statement(&program, FlinkStatementPurpose::EvolvedAppend);
        assert_eq!(evolved_append.matches("), (").count(), 3);
        assert!(evolved_append.contains("(16, 'category-0', 1607, 'evolved-16')"));
        assert!(evolved_append.contains("(19, 'category-3', 1907, 'evolved-19')"));

        let encoded = serde_json::to_string(&program).unwrap();
        for forbidden in [
            "CATALOG_BENCH_ENGINE_CLIENT_SECRET",
            "CATALOG_BENCH_S3_SECRET_ACCESS_KEY",
            "client-secret",
            "access-key-id",
            "secret-access-key",
        ] {
            assert!(!encoded.contains(forbidden));
        }
    }
}

#[test]
fn preserves_standard_authentication_as_secret_free_typed_setup() {
    let (profile, scenario) = contracts();
    let anonymous = render(&profile, &scenario, "lakecat", "auth01");
    assert!(matches!(
        anonymous.catalog.authentication,
        EngineCatalogAuthentication::Anonymous
    ));

    let oauth = render(&profile, &scenario, "polaris", "auth01");
    assert!(matches!(
        oauth.catalog.authentication,
        EngineCatalogAuthentication::OAuth2ClientCredentials { ref scope, .. }
            if scope == "PRINCIPAL_ROLE:ALL"
    ));
    assert!(!oauth
        .catalog
        .properties
        .keys()
        .any(|key| key.contains("credential") || key.contains("token")));
}

#[test]
fn escapes_literals_and_rejects_identifier_or_generator_drift() {
    let (profile, scenario) = contracts();
    let plan = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "safe01",
    )
    .unwrap();
    let mut flink = plan.flink().unwrap().clone();
    flink
        .scenario
        .table
        .properties
        .insert("owner".to_owned(), "O'Reilly".to_owned());
    let program = FlinkRenderedProgram::render(&flink).unwrap();
    assert!(statement(&program, FlinkStatementPurpose::CreateTable).contains("'O''Reilly'"));

    flink.fixture.table = "unsafe-name".to_owned();
    assert!(FlinkRenderedProgram::render(&flink)
        .unwrap_err()
        .to_string()
        .contains("identifier"));

    let mut invalid_generator = plan.flink().unwrap().clone();
    invalid_generator.scenario.row_generator.category =
        catalog_bench_engine::CategoryGenerator::ModuloLabel {
            modulus: 0,
            prefix: "category-".to_owned(),
        };
    assert!(FlinkRenderedProgram::render(&invalid_generator)
        .unwrap_err()
        .to_string()
        .contains("modulus"));
}

#[test]
fn rejects_plan_execution_policy_and_file_io_drift() {
    let (profile, scenario) = contracts();
    let plan = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "drift01",
    )
    .unwrap();
    let original = plan.flink().unwrap();

    let mut format = original.clone();
    format.format = "catalog-bench/flink-engine-plan/v2".to_owned();
    let mut parallelism = original.clone();
    parallelism.execution.parallelism = 2;
    let mut shim = original.clone();
    shim.scenario.engine_policy.catalog_specific_shims =
        catalog_bench_engine::ForbiddenPolicy::Forbidden;
    shim.scenario.transcript_format = "catalog-bench/unknown".to_owned();
    let mut file_io = original.clone();
    file_io.file_io.path_style_access = false;
    let mut namespace = original.clone();
    namespace.fixture.namespace = "outside_drift01".to_owned();

    for (name, drifted) in [
        ("format", format),
        ("parallelism", parallelism),
        ("scenario", shim),
        ("file IO", file_io),
        ("namespace", namespace),
    ] {
        assert!(
            FlinkRenderedProgram::render(&drifted).is_err(),
            "renderer accepted {name} drift"
        );
    }
}

#[test]
fn renderer_contains_no_catalog_branch_or_transport_substitute() {
    let source = fs::read_to_string(RENDERER).unwrap();
    for catalog in ["lakecat", "polaris", "gravitino", "lakekeeper", "nessie"] {
        assert!(!source.to_ascii_lowercase().contains(catalog));
    }
    for forbidden in ["reqwest", "hyper::", "/v1/", "namespaces/", "tables/"] {
        assert!(!source.contains(forbidden));
    }
    assert!(source.contains("ALTER TABLE {qualified_table} ADD"));
    assert!(source.contains("catalog-type"));
    assert!(source.contains("iceberg"));
}

fn contracts() -> (Profile, Scenario) {
    let ContractDocument::Profile(mut profile) = parse_contract(PROFILE).unwrap() else {
        panic!("profile fixture must be a profile");
    };
    let ContractDocument::Profile(candidate) = parse_contract(CANDIDATE_PROFILE).unwrap() else {
        panic!("candidate fixture must be a profile");
    };
    let ContractDocument::Scenario(scenario) = parse_contract(SCENARIO).unwrap() else {
        panic!("scenario fixture must be a scenario");
    };
    select_synthetic_materialized_flink(&mut profile, &candidate);
    (profile, scenario)
}

fn render(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &str,
    fixture: &str,
) -> FlinkRenderedProgram {
    let plan = InteroperabilityPlan::from_contracts(
        profile,
        scenario,
        &ComponentId::from(catalog),
        fixture,
    )
    .unwrap();
    FlinkRenderedProgram::render(plan.flink().unwrap()).unwrap()
}

fn statement(program: &FlinkRenderedProgram, purpose: FlinkStatementPurpose) -> &str {
    &program
        .statements
        .iter()
        .find(|statement| statement.purpose == purpose)
        .unwrap()
        .sql
}
