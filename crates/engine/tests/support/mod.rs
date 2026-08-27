use catalog_bench_common::contract::{Profile, RuntimeArtifact};
use catalog_bench_engine::{
    ENGINE_RUNNER_COMPONENT_ID, ENGINE_RUNNER_LOCATION, ENGINE_RUNNER_ROLE,
};

pub(crate) const RUNNER_REVISION: &str = "5e10f36e7e99815df273c7b567e466749f04d4be";

pub(crate) fn remove_engine_runner(profile: &mut Profile) {
    profile
        .components
        .retain(|component| component.id.as_str() != ENGINE_RUNNER_COMPONENT_ID);
    profile.services.retain(|service| {
        service.component.as_str() != ENGINE_RUNNER_COMPONENT_ID
            && service.role != ENGINE_RUNNER_ROLE
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
    embedded_artifacts.retain(|artifact| {
        artifact.location.strip_prefix("image:") != Some(ENGINE_RUNNER_LOCATION)
    });
}
