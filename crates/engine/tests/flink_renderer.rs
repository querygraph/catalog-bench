use std::fs;

use catalog_bench_common::contract::{
    parse_contract, ComponentId, ContractDocument, Profile, Scenario,
};
use catalog_bench_engine::{
    EngineCatalogAuthentication, FlinkOperation, FlinkOperationPurpose, FlinkRenderedProgram,
    InteroperabilityPlan,
};

mod support;

use support::select_synthetic_materialized_flink;

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const CANDIDATE_PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.v2.json");
const RENDERER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/flink.rs");
const CHILD_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/flink-runner");

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
                .operations
                .iter()
                .map(catalog_bench_engine::FlinkOperation::purpose)
                .collect::<Vec<_>>(),
            [
                FlinkOperationPurpose::CreateNamespace,
                FlinkOperationPurpose::CreateTable,
                FlinkOperationPurpose::InitialAppend,
                FlinkOperationPurpose::InitialRead,
                FlinkOperationPurpose::AddColumn,
                FlinkOperationPurpose::EvolvedAppend,
                FlinkOperationPurpose::EvolvedRead,
                FlinkOperationPurpose::SnapshotRead,
            ]
        );
        assert_eq!(program.fixture.table, "events");
        assert_eq!(program.fixture.bucket, "warehouse");
        assert_eq!(program.observation.initial_schema.len(), 3);
        assert_eq!(program.observation.evolved_field.name, "note");
        let add_column = statement(&program, FlinkOperationPurpose::AddColumn);
        assert!(add_column.ends_with("ADD `note` STRING"));
        assert!(!add_column.contains("NOT NULL"));
        let initial_append = statement(&program, FlinkOperationPurpose::InitialAppend);
        assert_eq!(initial_append.matches("), (").count(), 15);
        assert!(initial_append.contains("(0, 'category-0', 7)"));
        assert!(initial_append.contains("(15, 'category-3', 1507)"));
        let evolved_append = statement(&program, FlinkOperationPurpose::EvolvedAppend);
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
fn rendered_wire_envelope_carries_closed_read_and_observation_oracles() {
    let (profile, scenario) = contracts();
    let program = render(&profile, &scenario, "lakecat", "wire01");
    let initial = program
        .operations
        .iter()
        .find_map(|operation| match operation {
            FlinkOperation::InitialRead { expected, .. } => Some(expected),
            _ => None,
        })
        .unwrap();
    assert_eq!(initial.rows, 16);
    assert_eq!(initial.bytes, 346);
    assert_eq!(
        initial.sha256,
        "e78b526d7e757090a9a90c80802c2a543cbf8166cfac6d6ed48c618926e85a15"
    );
    assert_eq!(program.observation.format_version, 2);
    assert_eq!(
        program.observation.properties["catalog-bench.owner"],
        "catalog-bench"
    );

    let encoded = serde_json::to_vec(&program).unwrap();
    assert_eq!(
        serde_json::from_slice::<FlinkRenderedProgram>(&encoded).unwrap(),
        program
    );
    let mut unknown = serde_json::to_value(&program).unwrap();
    unknown["operations"][0]["untrusted"] = serde_json::json!(true);
    assert!(serde_json::from_value::<FlinkRenderedProgram>(unknown).is_err());
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
    assert!(statement(&program, FlinkOperationPurpose::CreateTable).contains("'O''Reilly'"));

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

#[test]
fn child_decoder_is_java17_pinned_bounded_and_transport_free() {
    let pom = fs::read_to_string(format!("{CHILD_ROOT}/pom.xml")).unwrap();
    for pin in [
        "<maven.compiler.release>17</maven.compiler.release>",
        "<jackson.version>2.18.2</jackson.version>",
        "<junit.version>5.11.4</junit.version>",
        "<project.build.outputTimestamp>2026-08-27T00:00:00Z</project.build.outputTimestamp>",
        "<minimizeJar>true</minimizeJar>",
    ] {
        assert!(pom.contains(pin), "child POM lost `{pin}`");
    }
    let model = fs::read_to_string(format!(
        "{CHILD_ROOT}/src/main/java/org/querygraph/catalogbench/flink/Program.java"
    ))
    .unwrap();
    let codec = fs::read_to_string(format!(
        "{CHILD_ROOT}/src/main/java/org/querygraph/catalogbench/flink/ProgramCodec.java"
    ))
    .unwrap();
    let events = fs::read_to_string(format!(
        "{CHILD_ROOT}/src/main/java/org/querygraph/catalogbench/flink/ChildEvent.java"
    ))
    .unwrap();
    let effects = fs::read_to_string(format!(
        "{CHILD_ROOT}/src/main/java/org/querygraph/catalogbench/flink/EngineEffects.java"
    ))
    .unwrap();
    let sink = fs::read_to_string(format!(
        "{CHILD_ROOT}/src/main/java/org/querygraph/catalogbench/flink/EventSink.java"
    ))
    .unwrap();
    let runner = fs::read_to_string(format!(
        "{CHILD_ROOT}/src/main/java/org/querygraph/catalogbench/flink/ProgramRunner.java"
    ))
    .unwrap();
    for required in [
        "STRICT_DUPLICATE_DETECTION",
        "FAIL_ON_UNKNOWN_PROPERTIES",
        "FAIL_ON_TRAILING_TOKENS",
        "MAX_PROGRAM_BYTES = 256 * 1024",
        "LinkOption.NOFOLLOW_LINKS",
        "OPERATION_ORDER",
    ] {
        assert!(codec.contains(required), "child decoder lost `{required}`");
    }
    for required in ["CATALOG_BENCH_EVENT ", "MAX_EVENT_BYTES = 16 * 1024"] {
        assert!(
            sink.contains(required),
            "child event sink lost `{required}`"
        );
    }
    for required in [
        "FIXTURE_COLLISION_EXIT = 3",
        "effects.fixtureAbsent(program.fixture())",
        "if (!expected.equals(observed))",
    ] {
        assert!(runner.contains(required), "child runner lost `{required}`");
    }
    for source in [&model, &codec, &events, &effects, &sink, &runner] {
        for catalog in ["lakecat", "polaris", "gravitino", "lakekeeper", "nessie"] {
            assert!(!source.to_ascii_lowercase().contains(catalog));
        }
        for transport in ["java.net.http", "okhttp", "apache.http", "HttpClient"] {
            assert!(!source.contains(transport));
        }
    }
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

fn statement(program: &FlinkRenderedProgram, purpose: FlinkOperationPurpose) -> &str {
    program
        .operations
        .iter()
        .find(|operation| operation.purpose() == purpose)
        .unwrap()
        .sql()
}
