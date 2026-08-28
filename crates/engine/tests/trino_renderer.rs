use std::fs;

use catalog_bench_common::contract::{
    parse_contract, ComponentId, ContractDocument, Profile, Scenario,
};
use catalog_bench_engine::{
    EngineCatalogAuthentication, InteroperabilityPlan, TrinoOperation, TrinoOperationPurpose,
    TrinoRenderedProgram, TrinoServerConfiguration,
};

mod support;

use support::select_synthetic_materialized_trino;

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const CANDIDATE_PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.v2.json");
const RENDERER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/trino.rs");

#[test]
fn renders_the_complete_stock_trino_program_for_every_catalog() {
    let (profile, scenario) = contracts();
    for catalog in ["lakecat", "polaris", "gravitino", "lakekeeper"] {
        let program = render(&profile, &scenario, catalog, "render01");

        assert_eq!(program.task_concurrency, 1);
        assert_eq!(program.catalog.properties["connector.name"], "iceberg");
        assert_eq!(program.catalog.properties["iceberg.catalog.type"], "rest");
        assert_eq!(program.catalog.properties["fs.s3.enabled"], "true");
        assert_eq!(
            program.catalog.properties["s3.endpoint"],
            "http://minio:9000"
        );
        assert_eq!(
            program
                .operations
                .iter()
                .map(TrinoOperation::purpose)
                .collect::<Vec<_>>(),
            [
                TrinoOperationPurpose::CreateNamespace,
                TrinoOperationPurpose::CreateTable,
                TrinoOperationPurpose::InitialAppend,
                TrinoOperationPurpose::InitialRead,
                TrinoOperationPurpose::AddColumn,
                TrinoOperationPurpose::EvolvedAppend,
                TrinoOperationPurpose::EvolvedRead,
                TrinoOperationPurpose::SnapshotRead,
            ]
        );
        assert_eq!(program.fixture.bucket, "warehouse");
        let create = statement(&program, TrinoOperationPurpose::CreateTable);
        assert!(create.starts_with("CREATE TABLE \"bench\"."));
        assert!(create.contains("\"id\" BIGINT NOT NULL"));
        assert!(create.contains("\"category\" VARCHAR"));
        assert!(create.contains("format_version = 2"));
        assert!(create.contains("extra_properties = MAP(ARRAY["));
        assert_eq!(
            program
                .catalog
                .properties
                .get("iceberg.allowed-extra-properties")
                .map(String::as_str),
            Some(
                "catalog-bench.owner,write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max"
            )
        );
        let initial = statement(&program, TrinoOperationPurpose::InitialAppend);
        assert_eq!(initial.matches("), (").count(), 15);
        assert!(initial.contains("(0, 'category-0', 7)"));
        let evolved = statement(&program, TrinoOperationPurpose::EvolvedAppend);
        assert_eq!(evolved.matches("), (").count(), 3);
        assert!(evolved.contains("(19, 'category-3', 1907, 'evolved-19')"));
        assert!(statement(&program, TrinoOperationPurpose::AddColumn)
            .contains(" ADD COLUMN \"note\" VARCHAR"));
        assert!(statement(&program, TrinoOperationPurpose::SnapshotRead)
            .contains("\"events$snapshots\""));

        let encoded = serde_json::to_string(&program).unwrap();
        for forbidden in [
            "CATALOG_BENCH_ENGINE_CLIENT_SECRET",
            "CATALOG_BENCH_S3_SECRET_ACCESS_KEY",
            "oauth2.credential",
            "s3.aws-secret-key",
        ] {
            assert!(!encoded.contains(forbidden));
        }
    }
}

#[test]
fn preserves_typed_authentication_without_serializing_credentials() {
    let (profile, scenario) = contracts();
    let anonymous = render(&profile, &scenario, "lakecat", "auth01");
    assert!(matches!(
        anonymous.catalog.authentication,
        EngineCatalogAuthentication::Anonymous
    ));
    assert_eq!(
        anonymous.catalog.properties["iceberg.rest-catalog.security"],
        "NONE"
    );

    let oauth = render(&profile, &scenario, "polaris", "auth01");
    assert!(matches!(
        oauth.catalog.authentication,
        EngineCatalogAuthentication::OAuth2ClientCredentials { ref scope, .. }
            if scope == "PRINCIPAL_ROLE:ALL"
    ));
    assert_eq!(
        oauth.catalog.properties["iceberg.rest-catalog.security"],
        "OAUTH2"
    );
    assert!(!oauth
        .catalog
        .properties
        .keys()
        .any(|key| key.ends_with("credential") || key.ends_with("token")));
}

#[test]
fn wire_envelope_carries_closed_read_and_observation_oracles() {
    let (profile, scenario) = contracts();
    let program = render(&profile, &scenario, "lakecat", "wire01");
    let TrinoOperation::InitialRead { expected, .. } = &program.operations[3] else {
        panic!("fourth operation must be the initial read");
    };
    assert_eq!(expected.rows, 16);
    assert_eq!(expected.bytes, 346);
    assert_eq!(program.observation.format_version, 2);
    assert_eq!(program.observation.evolved_field.name, "note");

    let encoded = serde_json::to_vec(&program).unwrap();
    assert_eq!(
        serde_json::from_slice::<TrinoRenderedProgram>(&encoded).unwrap(),
        program
    );
    let mut unknown = serde_json::to_value(&program).unwrap();
    unknown["operations"][0]["untrusted"] = serde_json::json!(true);
    assert!(serde_json::from_value::<TrinoRenderedProgram>(unknown).is_err());
}

#[test]
fn renders_a_complete_secret_reference_only_server_configuration() {
    let (profile, scenario) = contracts();
    let anonymous = render(&profile, &scenario, "lakecat", "config01");
    let configuration = TrinoServerConfiguration::render(&anonymous).unwrap();
    assert_eq!(
        configuration
            .files()
            .iter()
            .map(|file| file.relative_path)
            .collect::<Vec<_>>(),
        [
            "catalog/bench.properties",
            "config.properties",
            "jvm.config",
            "log.properties",
            "node.properties",
        ]
    );
    let catalog = file(&configuration, "catalog/bench.properties");
    assert!(catalog.contains("s3.aws-access-key=${ENV:CATALOG_BENCH_S3_ACCESS_KEY_ID}\n"));
    assert!(catalog.contains("s3.aws-secret-key=${ENV:CATALOG_BENCH_S3_SECRET_ACCESS_KEY}\n"));
    assert!(!catalog.contains("oauth2.credential"));
    assert!(file(&configuration, "config.properties").contains("task.concurrency=1\n"));
    assert!(file(&configuration, "node.properties")
        .contains("node.data-dir=${ENV:CATALOG_BENCH_TRINO_DATA_DIR}\n"));
    assert!(file(&configuration, "node.properties")
        .contains("node.id=${ENV:CATALOG_BENCH_TRINO_NODE_ID}\n"));
    for required in [
        "-agentpath:/usr/lib/trino/bin/libjvmkill.so",
        "-XX:InitialRAMPercentage=80",
        "-XX:MaxRAMPercentage=80",
        "-Djdk.nio.maxCachedBufferSize=2000000",
    ] {
        assert!(file(&configuration, "jvm.config").contains(required));
    }

    let oauth = render(&profile, &scenario, "polaris", "config01");
    let configuration = TrinoServerConfiguration::render(&oauth).unwrap();
    assert!(file(&configuration, "catalog/bench.properties").contains(
        "iceberg.rest-catalog.oauth2.credential=${ENV:CATALOG_BENCH_ENGINE_OAUTH_CREDENTIAL}\n"
    ));

    let encoded = configuration
        .files()
        .iter()
        .map(|file| file.contents.as_str())
        .collect::<String>();
    for secret in ["actual-client-secret", "actual-s3-secret"] {
        assert!(!encoded.contains(secret));
    }
}

#[test]
fn server_configuration_rejects_property_injection() {
    let (profile, scenario) = contracts();
    let mut program = render(&profile, &scenario, "lakecat", "config02");
    program
        .catalog
        .properties
        .insert("unsafe\nproperty".to_owned(), "value".to_owned());
    assert!(TrinoServerConfiguration::render(&program)
        .unwrap_err()
        .to_string()
        .contains("unsafe property"));
}

#[test]
fn rejects_plan_policy_file_io_identifier_and_generator_drift() {
    let (profile, scenario) = contracts();
    let plan = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "drift01",
    )
    .unwrap();
    let original = plan.trino().unwrap();

    let mut format = original.clone();
    format.format = "catalog-bench/trino-engine-plan/v2".to_owned();
    let mut concurrency = original.clone();
    concurrency.execution.task_concurrency = 2;
    let mut file_io = original.clone();
    file_io.file_io.enabled = false;
    let mut namespace = original.clone();
    namespace.fixture.namespace = "outside_drift01".to_owned();
    let mut identifier = original.clone();
    identifier.fixture.table = "unsafe-name".to_owned();
    let mut generator = original.clone();
    generator.scenario.row_generator.category =
        catalog_bench_engine::CategoryGenerator::ModuloLabel {
            modulus: 0,
            prefix: "category-".to_owned(),
        };

    for (name, drifted) in [
        ("format", format),
        ("concurrency", concurrency),
        ("file IO", file_io),
        ("namespace", namespace),
        ("identifier", identifier),
        ("generator", generator),
    ] {
        assert!(
            TrinoRenderedProgram::render(&drifted).is_err(),
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
    for required in [
        "CREATE SCHEMA IF NOT EXISTS",
        "ALTER TABLE {qualified_table} ADD",
        "iceberg.rest-catalog.uri",
        "fs.s3.enabled",
    ] {
        assert!(source.contains(required), "renderer lost `{required}`");
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
    select_synthetic_materialized_trino(&mut profile, &candidate);
    (profile, scenario)
}

fn render(
    profile: &Profile,
    scenario: &Scenario,
    catalog: &str,
    fixture: &str,
) -> TrinoRenderedProgram {
    let plan = InteroperabilityPlan::from_contracts(
        profile,
        scenario,
        &ComponentId::from(catalog),
        fixture,
    )
    .unwrap();
    TrinoRenderedProgram::render(plan.trino().unwrap()).unwrap()
}

fn statement(program: &TrinoRenderedProgram, purpose: TrinoOperationPurpose) -> &str {
    program
        .operations
        .iter()
        .find(|operation| operation.purpose() == purpose)
        .map(TrinoOperation::sql)
        .unwrap()
}

fn file<'a>(configuration: &'a TrinoServerConfiguration, path: &str) -> &'a str {
    configuration
        .files()
        .iter()
        .find(|file| file.relative_path == path)
        .map(|file| file.contents.as_str())
        .unwrap()
}
