use std::collections::BTreeMap;

use catalog_bench_common::contract::{
    ArtifactReference, Component, ComponentId, ComponentKind, Digest, DigestAlgorithm,
    ImageDigestScope, Profile, RuntimeArtifact, ServiceBinding, SourceRevision,
};
use catalog_bench_engine::{
    ENGINE_RUNNER_COMPONENT_ID, ENGINE_RUNNER_LOCATION, ENGINE_RUNNER_ROLE,
};

pub(crate) const RUNNER_REVISION: &str = "1111111111111111111111111111111111111111";
pub(crate) const RUNNER_DIGEST: &str =
    "2222222222222222222222222222222222222222222222222222222222222222";

pub(crate) fn add_engine_runner(profile: &mut Profile) {
    let artifact = ArtifactReference {
        location: format!("image:{ENGINE_RUNNER_LOCATION}"),
        media_type: "application/vnd.elf".to_owned(),
        digest: Digest {
            algorithm: DigestAlgorithm::Sha256,
            value: RUNNER_DIGEST.to_owned(),
        },
        bytes: Some(4_986_064),
        description: Some("test optimized engine runner".to_owned()),
        extensions: BTreeMap::new(),
    };
    profile.components.push(Component {
        id: ComponentId::from(ENGINE_RUNNER_COMPONENT_ID),
        kind: ComponentKind::BenchmarkHarness,
        name: "catalog-bench stock-engine runner".to_owned(),
        version: RUNNER_REVISION.to_owned(),
        source: Some(Box::new(SourceRevision {
            repository: "https://github.com/querygraph/catalog-bench.git".to_owned(),
            revision: RUNNER_REVISION.to_owned(),
            tag: None,
            extensions: BTreeMap::new(),
        })),
        build: None,
        artifact: RuntimeArtifact::ContainerImage {
            reference: "catalog-bench-engine:test".to_owned(),
            digest_scope: ImageDigestScope::LocalImage,
            digest: Digest {
                algorithm: DigestAlgorithm::Sha256,
                value: "4".repeat(64),
            },
            platform_digest: None,
            embedded_artifacts: vec![artifact.clone()],
        },
        extensions: BTreeMap::new(),
    });
    profile.services.push(ServiceBinding {
        component: ComponentId::from(ENGINE_RUNNER_COMPONENT_ID),
        role: ENGINE_RUNNER_ROLE.to_owned(),
        endpoint: None,
        private_state: None,
        settings: BTreeMap::new(),
        extensions: BTreeMap::new(),
    });
    let engine = profile
        .components
        .iter_mut()
        .find(|component| component.id.as_str() == "spark-4.1")
        .expect("engine fixture must contain Spark");
    let RuntimeArtifact::ContainerImage {
        embedded_artifacts, ..
    } = &mut engine.artifact
    else {
        panic!("engine fixture must be an image");
    };
    embedded_artifacts.push(artifact);
}
