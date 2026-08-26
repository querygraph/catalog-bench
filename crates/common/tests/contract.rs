use std::collections::{BTreeMap, BTreeSet};

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
            capability: "iceberg-rest".into(),
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
        readiness: ProfileReadiness::Runnable,
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
            role: "iceberg-rest-catalog".to_owned(),
            endpoint: Some("http://catalog:8181".to_owned()),
            private_state: None,
            settings: BTreeMap::new(),
            extensions: BTreeMap::new(),
        }],
        catalog_capabilities: vec![CatalogCapability {
            id: "iceberg-rest".into(),
            description: "Exercise the fixture Iceberg REST operation.".to_owned(),
            specification: Some("https://iceberg.apache.org/".to_owned()),
        }],
        catalog_adapters: vec![CatalogAdapter {
            catalog: "catalog".into(),
            protocol: CatalogProtocol::IcebergRestV1,
            endpoint: CatalogEndpoint {
                base_url: "http://catalog:8181".to_owned(),
                config: CatalogConfigRequest {
                    path: "/v1/config".to_owned(),
                    query: BTreeMap::new(),
                },
                route_prefix: CatalogRoutePrefix::Unprefixed,
                create_table_location: None,
            },
            authentication: CatalogAuthentication::Anonymous,
            request_handling: AdapterRequestHandling::ProtocolNative,
            capabilities: AdapterCapabilityCoverage::ExerciseAll,
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
        run: RunIdentity::Single {
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
            cpu_model: Captured::Exact {
                value: "fixture".to_owned(),
            },
            logical_cpus: Captured::Exact { value: 4 },
            memory_bytes: Captured::Exact {
                value: 8 * 1024 * 1024 * 1024,
            },
            cpu_limit: Some(4.0),
            memory_limit_bytes: Some(4 * 1024 * 1024 * 1024),
            network: "fixture-net".to_owned(),
            container_runtime: Captured::Exact {
                value: "Docker fixture".to_owned(),
            },
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
    for schema in generated_schemas().unwrap() {
        let bytes = match schema.file_name {
            "manifest.schema.json" => {
                include_bytes!("../../../schemas/v1/manifest.schema.json").as_slice()
            }
            "profile.schema.json" => {
                include_bytes!("../../../schemas/v1/profile.schema.json").as_slice()
            }
            "result.schema.json" => {
                include_bytes!("../../../schemas/v1/result.schema.json").as_slice()
            }
            "scenario.schema.json" => {
                include_bytes!("../../../schemas/v1/scenario.schema.json").as_slice()
            }
            unexpected => panic!("unexpected generated schema {unexpected}"),
        };
        let checked_in: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(checked_in, schema.document, "{} drifted", schema.file_name);
        assert_eq!(
            checked_in["$schema"],
            "https://json-schema.org/draft/2020-12/schema"
        );
    }
}

#[test]
fn checked_in_profiles_scenario_and_phase_one_adapters_validate() {
    let bytes = [
        include_bytes!("../../../profiles/v1/reproduction-2026-08-08.json").as_slice(),
        include_bytes!("../../../profiles/v1/current-2026-08-26.json").as_slice(),
        include_bytes!("../../../scenarios/v1/iceberg-rest.commit.same-table-contention.json")
            .as_slice(),
        include_bytes!("../../../scenarios/v1/iceberg-rest.config.negotiation.json").as_slice(),
    ];
    let documents = bytes.map(|document| parse_contract(document).unwrap());

    let ContractDocument::Profile(historical) = &documents[0] else {
        panic!("first document must be the historical profile");
    };
    assert!(matches!(historical.readiness, ProfileReadiness::Runnable));

    let ContractDocument::Profile(current) = &documents[1] else {
        panic!("second document must be the current profile");
    };
    let ProfileReadiness::Draft {
        unresolved_artifacts,
        ..
    } = &current.readiness
    else {
        panic!("current profile must stay draft until artifacts are built");
    };
    assert_eq!(unresolved_artifacts.len(), 6);
    assert_eq!(current.catalog_adapters.len(), 5);
    assert_eq!(current.catalog_capabilities.len(), 27);
    assert!(current.components.iter().any(|component| {
        component.id.as_str() == "catalog-bench-conformance"
            && component.kind == ComponentKind::BenchmarkHarness
    }));
    assert!(current.services.iter().any(|service| {
        service.component.as_str() == "catalog-bench-conformance"
            && service.role == "conformance-runner"
    }));

    let catalog_ids = current
        .catalog_adapters
        .iter()
        .map(|adapter| adapter.catalog.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        catalog_ids,
        BTreeSet::from(["gravitino", "lakecat", "lakekeeper", "nessie", "polaris"])
    );
    assert!(current.catalog_adapters.iter().all(|adapter| matches!(
        adapter.request_handling,
        AdapterRequestHandling::ProtocolNative
    )));
    assert!(current
        .catalog_adapters
        .iter()
        .all(|adapter| matches!(adapter.capabilities, AdapterCapabilityCoverage::ExerciseAll)));

    let endpoints = current
        .catalog_adapters
        .iter()
        .map(|adapter| (adapter.catalog.as_str(), adapter.endpoint.base_url.as_str()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        endpoints,
        BTreeMap::from([
            ("gravitino", "http://gravitino:9001/iceberg"),
            ("lakecat", "http://lakecat:8181/catalog"),
            ("lakekeeper", "http://lakekeeper:8181/catalog"),
            ("nessie", "http://nessie:19120/iceberg"),
            ("polaris", "http://polaris:8181/api/catalog"),
        ])
    );

    assert!(documents[2..]
        .iter()
        .all(|document| matches!(document, ContractDocument::Scenario(_))));
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
fn historical_environment_uncertainty_is_explicit() {
    let mut result = result_record();
    result.environment.cpu_model = Captured::Unknown {
        explanation: "The historical runner did not record the CPU model.".to_owned(),
    };
    result.environment.memory_bytes = Captured::Approximate {
        value: 8_375_186_227,
        explanation: "Converted from the rounded report value 7.8 GiB.".to_owned(),
    };

    result.validate().unwrap();

    result.environment.container_runtime = Captured::Unknown {
        explanation: String::new(),
    };
    assert!(result
        .validate()
        .unwrap_err()
        .to_string()
        .contains("container_runtime.explanation: must not be empty"));
}

#[test]
fn aggregate_results_disclose_round_selection() {
    let mut result = result_record();
    result.run = RunIdentity::Aggregate {
        id: "five-round-median".to_owned(),
        period: "2026-08-08".to_owned(),
        included_repetitions: vec![2, 3, 4, 5, 6],
        excluded_repetitions: vec![1],
        aggregation: "median, with min-max range retained".to_owned(),
    };

    result.validate().unwrap();

    let RunIdentity::Aggregate {
        included_repetitions,
        excluded_repetitions,
        ..
    } = &mut result.run
    else {
        unreachable!();
    };
    excluded_repetitions.push(included_repetitions[0]);
    assert!(result
        .validate()
        .unwrap_err()
        .to_string()
        .contains("cannot be both included and excluded"));
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
fn non_historical_profiles_require_one_adapter_per_catalog() {
    let mut profile = profile();
    profile.catalog_adapters.clear();

    let error = profile.validate().unwrap_err();

    assert!(error
        .to_string()
        .contains("catalog component `catalog` has no adapter"));
}

#[test]
fn adapters_reject_service_endpoint_drift_and_incomplete_capabilities() {
    let mut profile = profile();
    profile.catalog_adapters[0].endpoint.base_url = "http://other:8181".to_owned();
    profile.catalog_adapters[0].capabilities = AdapterCapabilityCoverage::Explicit {
        exercise: Vec::new(),
        unsupported: Vec::new(),
    };

    let error = profile.validate().unwrap_err().to_string();

    assert!(error.contains("must exactly match the catalog service endpoint"));
    assert!(error.contains("does not classify capability `iceberg-rest`"));
}

#[test]
fn adapters_reject_undefined_or_overlapping_capabilities() {
    let mut profile = profile();
    profile.catalog_adapters[0].capabilities = AdapterCapabilityCoverage::Explicit {
        exercise: vec!["iceberg-rest".into(), "fixture.undefined".into()],
        unsupported: vec![UnsupportedAdapterCapability {
            capability: "iceberg-rest".into(),
            attributed_to: CapabilityLimitationSource::Catalog,
            explanation: "Fixture limitation.".to_owned(),
            upstream_reference: None,
        }],
    };

    let error = profile.validate().unwrap_err().to_string();

    assert!(error.contains("classifies undefined capability `fixture.undefined`"));
    assert!(error.contains("cannot be both exercised and unsupported"));
}

#[test]
fn adapter_routes_reject_credentials_and_secret_query_keys() {
    let mut profile = profile();
    profile.catalog_adapters[0].endpoint.base_url = "http://user:password@catalog:8181".to_owned();
    profile.catalog_adapters[0]
        .endpoint
        .config
        .query
        .insert("access_token".to_owned(), "redacted".to_owned());

    let error = profile.validate().unwrap_err().to_string();

    assert!(error.contains("without credentials"));
    assert!(error.contains("secret-like setting key `access_token` is forbidden"));
}

#[test]
fn adapter_base_url_rejects_a_root_trailing_slash() {
    let mut profile = profile();
    profile.catalog_adapters[0].endpoint.base_url = "http://catalog/".to_owned();
    profile.services[0].endpoint = Some("http://catalog/".to_owned());

    let error = profile.validate().unwrap_err().to_string();

    assert!(error.contains("without credentials, query, fragment, or trailing slash"));
}

#[test]
fn oauth_adapter_rejects_nonportable_or_reused_environment_names() {
    let mut profile = profile();
    profile.catalog_adapters[0].authentication = CatalogAuthentication::OAuth2ClientCredentials {
        token_path: "/v1/oauth/tokens".to_owned(),
        scope: "fixture:all".to_owned(),
        client_id_env: "9INVALID".to_owned(),
        client_secret_env: "9INVALID".to_owned(),
    };

    let error = profile.validate().unwrap_err().to_string();

    assert!(error.contains("must be a portable environment-variable name"));
    assert!(error.contains("must differ from the client-id environment variable"));
}

#[test]
fn behavior_changing_shims_require_a_disclosed_connector_component() {
    let mut profile = profile();
    profile.catalog_adapters[0].request_handling = AdapterRequestHandling::BehaviorChangingShim {
        component: "minio".into(),
        description: "Fixture response rewrite.".to_owned(),
    };

    let error = profile.validate().unwrap_err().to_string();
    assert!(error.contains("must be a connector component"));

    profile.components.push(package_component(
        "shim",
        ComponentKind::Connector,
        "Fixture shim",
    ));
    profile.catalog_adapters[0].request_handling = AdapterRequestHandling::BehaviorChangingShim {
        component: "shim".into(),
        description: "Fixture response rewrite.".to_owned(),
    };
    profile.validate().unwrap();
}

#[test]
fn runnable_profiles_reject_unresolved_artifacts() {
    let mut profile = profile();
    let RuntimeArtifact::Package { digest, .. } = &mut profile.components[0].artifact else {
        unreachable!();
    };
    *digest = None;

    let error = profile.validate().unwrap_err();

    assert!(error
        .to_string()
        .contains("runnable profile has unresolved artifacts: catalog"));
}

#[test]
fn draft_profiles_name_every_unresolved_artifact() {
    let mut profile = profile();
    let RuntimeArtifact::Package { digest, .. } = &mut profile.components[0].artifact else {
        unreachable!();
    };
    *digest = None;
    profile.readiness = ProfileReadiness::Draft {
        unresolved_artifacts: vec!["catalog".into()],
        explanation: "The production package has not been built yet.".to_owned(),
    };

    profile.validate().unwrap();
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
