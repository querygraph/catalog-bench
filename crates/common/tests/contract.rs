use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use catalog_bench_common::contract::*;

fn digest(character: char) -> Digest {
    Digest {
        algorithm: DigestAlgorithm::Sha256,
        value: character.to_string().repeat(64),
    }
}

fn artifact(location: &str, character: char) -> ArtifactReference {
    ArtifactReference {
        location: location.to_owned(),
        media_type: "application/json".to_owned(),
        digest: digest(character),
        bytes: Some(42),
        description: None,
        extensions: BTreeMap::new(),
    }
}

fn package_component(id: &str, kind: ComponentKind, name: &str) -> Component {
    Component {
        id: id.into(),
        kind,
        name: name.to_owned(),
        version: "1.2.3".to_owned(),
        source: None,
        build: None,
        artifact: RuntimeArtifact::Package {
            ecosystem: "fixture".to_owned(),
            package: name.to_owned(),
            version: "1.2.3".to_owned(),
            digest: Some(digest('c')),
        },
        extensions: BTreeMap::new(),
    }
}

fn scenario() -> Scenario {
    Scenario {
        contract_version: ContractVersion::V1,
        kind: ScenarioDocumentKind::Scenario,
        id: "iceberg-rest.fixture".into(),
        version: 1,
        title: "Fixture scenario".to_owned(),
        description: "Exercises one neutral operation.".to_owned(),
        family: ScenarioFamily::IcebergRest,
        classification: ClassificationPolicy::StrictV1,
        tags: vec!["fixture".to_owned()],
        capabilities: vec![CapabilityRequirement {
            capability: "iceberg-rest".to_owned(),
            level: RequirementLevel::Required,
            description: "Iceberg REST is required.".to_owned(),
            specification: Some("https://iceberg.apache.org/".to_owned()),
        }],
        parameters: BTreeMap::new(),
        steps: vec![ScenarioStep {
            id: "load-table".into(),
            actor: ActorRole::Client,
            operation: "table.load".to_owned(),
            description: "Load a table through Iceberg REST.".to_owned(),
            depends_on: Vec::new(),
            parameters: BTreeMap::new(),
            timeout_ms: Some(5_000),
        }],
        assertions: vec![AssertionSpec {
            id: "http-ok".into(),
            step: "load-table".into(),
            required: true,
            description: "The catalog returns a success status.".to_owned(),
            check: AssertionCheck::HttpStatus { allowed: vec![200] },
        }],
        extensions: BTreeMap::new(),
    }
}

fn profile() -> Profile {
    Profile {
        contract_version: ContractVersion::V1,
        kind: ProfileDocumentKind::Profile,
        id: "fixture".into(),
        title: "Fixture profile".to_owned(),
        description: "Pins a catalog and shared object store.".to_owned(),
        resolved_at: "2026-08-26T00:00:00Z".to_owned(),
        purpose: ProfilePurpose::Conformance,
        platform: ExecutionPlatform {
            operating_system: "Linux".to_owned(),
            architecture: "aarch64".to_owned(),
            mode: ExecutionMode::DockerCompose,
            container_runtime: None,
            network: "fixture-net".to_owned(),
            shared_object_store: "minio".into(),
            warehouse_uri: "s3://warehouse".to_owned(),
            extensions: BTreeMap::new(),
        },
        components: vec![
            package_component("catalog", ComponentKind::Catalog, "Catalog"),
            package_component("minio", ComponentKind::ObjectStore, "MinIO"),
        ],
        services: vec![ServiceBinding {
            component: "catalog".into(),
            role: "catalog".to_owned(),
            endpoint: Some("http://catalog:8181".to_owned()),
            private_state: None,
            settings: BTreeMap::new(),
            extensions: BTreeMap::new(),
        }],
        extensions: BTreeMap::new(),
    }
}

fn result_record() -> ResultRecord {
    ResultRecord {
        contract_version: ContractVersion::V1,
        kind: ResultDocumentKind::Result,
        id: "fixture-run-result".into(),
        scenario: ScenarioReference {
            id: "iceberg-rest.fixture".into(),
            version: 1,
            digest: digest('a'),
        },
        profile: ProfileReference {
            id: "fixture".into(),
            digest: digest('b'),
        },
        catalog: ExecutedComponent {
            profile_component: "catalog".into(),
            name: "Catalog".to_owned(),
            version: "1.2.3".to_owned(),
        },
        client: None,
        adapters: Vec::new(),
        run: RunIdentity {
            id: "fixture-run".to_owned(),
            started_at: "2026-08-26T00:00:00Z".to_owned(),
            finished_at: "2026-08-26T00:00:01Z".to_owned(),
            repetition: 1,
            random_seed: Some(7),
        },
        outcome: ResultOutcome::Pass {
            summary: Some("Required behavior passed.".to_owned()),
        },
        environment: EnvironmentManifest {
            operating_system: "Linux".to_owned(),
            architecture: "aarch64".to_owned(),
            cpu_model: "fixture".to_owned(),
            logical_cpus: 4,
            memory_bytes: 8 * 1024 * 1024 * 1024,
            cpu_limit: Some(4.0),
            memory_limit_bytes: Some(4 * 1024 * 1024 * 1024),
            network: "fixture-net".to_owned(),
            container_runtime: "Docker fixture".to_owned(),
            runtime_flags: BTreeMap::new(),
            extensions: BTreeMap::new(),
        },
        assertions: vec![AssertionEvaluation {
            assertion: "http-ok".into(),
            required: true,
            outcome: AssertionOutcome::Pass,
            evidence: vec!["response".into()],
        }],
        measurements: vec![MeasuredPhase {
            name: "load".to_owned(),
            elapsed_ms: 1.0,
            operations: Some(1),
            latency_ms: Some(Distribution {
                samples: 1,
                minimum: 1.0,
                maximum: 1.0,
                mean: Some(1.0),
                standard_deviation: Some(0.0),
                quantiles: BTreeMap::from([("p50".to_owned(), 1.0)]),
            }),
            metrics: Vec::new(),
        }],
        evidence: vec![Evidence {
            id: "response".into(),
            kind: EvidenceKind::HttpTranscript,
            artifact: artifact("evidence/response.json", 'd'),
            sanitized: true,
            redactions: vec!["authorization header".to_owned()],
            extensions: BTreeMap::new(),
        }],
        artifacts: Vec::new(),
        extensions: BTreeMap::new(),
    }
}

#[test]
fn every_document_round_trips_through_the_dispatch_parser() {
    let manifest = ResultBundleManifest {
        contract_version: ContractVersion::V1,
        kind: ManifestDocumentKind::Manifest,
        id: "fixture-bundle".into(),
        title: "Fixture bundle".to_owned(),
        created_at: "2026-08-26T00:00:02Z".to_owned(),
        provenance: Provenance::Fixture {
            explanation: "Contract test fixture.".to_owned(),
        },
        profile: artifact("profiles/fixture.json", 'b'),
        scenarios: vec![artifact("scenarios/fixture.json", 'a')],
        results: vec![artifact("results/fixture.json", 'e')],
        source_evidence: Vec::new(),
        redaction: RedactionStatement {
            reviewed: true,
            policy: "Synthetic fixture contains no secrets.".to_owned(),
            removed_fields: Vec::new(),
        },
        extensions: BTreeMap::new(),
    };
    let documents = [
        serde_json::to_vec(&scenario()).unwrap(),
        serde_json::to_vec(&profile()).unwrap(),
        serde_json::to_vec(&result_record()).unwrap(),
        serde_json::to_vec(&manifest).unwrap(),
    ];

    for bytes in documents {
        parse_contract(&bytes).unwrap();
    }
}

#[test]
fn checked_in_schemas_exactly_match_rust_types() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");

    for schema in generated_schemas().unwrap() {
        let path = root.join("schemas/v1").join(schema.file_name);
        let checked_in: serde_json::Value =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert_eq!(checked_in, schema.document, "{} drifted", schema.file_name);
        assert_eq!(
            checked_in["$schema"],
            "https://json-schema.org/draft/2020-12/schema"
        );
    }
}

#[test]
fn unknown_fields_are_rejected_outside_extensions() {
    let mut value = serde_json::to_value(scenario()).unwrap();
    value["typo"] = serde_json::json!(true);

    let error = parse_contract(&serde_json::to_vec(&value).unwrap()).unwrap_err();

    assert!(error.to_string().contains("unknown field `typo`"));
}

#[test]
fn pass_cannot_hide_a_failed_required_assertion() {
    let mut result = result_record();
    result.assertions[0].outcome = AssertionOutcome::Fail {
        explanation: "server returned 500".to_owned(),
    };

    let error = result.validate().unwrap_err();

    assert!(error.to_string().contains("required assertion `http-ok`"));
}

#[test]
fn unsanitized_evidence_is_not_publishable() {
    let mut result = result_record();
    result.evidence[0].sanitized = false;

    let error = result.validate().unwrap_err();

    assert!(error
        .to_string()
        .contains("must be true for a publishable result"));
}

#[test]
fn profile_rejects_secret_shaped_settings() {
    let mut profile = profile();
    profile.services[0]
        .settings
        .insert("client_secret".to_owned(), serde_json::json!("redacted"));

    let error = profile.validate().unwrap_err();

    assert!(error.to_string().contains("secret-like setting key"));
}

#[test]
fn source_builds_require_source_and_build_provenance() {
    let mut component = package_component("catalog", ComponentKind::Catalog, "Catalog");
    component.artifact = RuntimeArtifact::SourceBuild { executable: None };

    let error = component.validate().unwrap_err();

    assert!(error.to_string().contains("source: is required"));
    assert!(error.to_string().contains("build: is required"));
}

#[test]
fn image_digest_scope_cannot_be_ambiguous() {
    let mut component = package_component("catalog", ComponentKind::Catalog, "Catalog");
    component.artifact = RuntimeArtifact::ContainerImage {
        reference: "catalog:fixture".to_owned(),
        digest_scope: ImageDigestScope::LocalImage,
        digest: digest('a'),
        platform_digest: Some(digest('b')),
        embedded_artifacts: Vec::new(),
    };

    let error = component.validate().unwrap_err();

    assert!(error
        .to_string()
        .contains("only meaningful when digest_scope is `index`"));
}

#[test]
fn distributions_require_monotonic_quantiles() {
    let mut result = result_record();
    let distribution = result.measurements[0].latency_ms.as_mut().unwrap();
    distribution.minimum = 1.0;
    distribution.maximum = 10.0;
    distribution.quantiles = BTreeMap::from([("p50".to_owned(), 8.0), ("p99".to_owned(), 2.0)]);

    let error = result.validate().unwrap_err();

    assert!(error.to_string().contains("p50 must not exceed p99"));
}
