use catalog_bench_common::contract::{Profile, RuntimeArtifact};
use catalog_bench_engine::{
    ENGINE_RUNNER_COMPONENT_ID, ENGINE_RUNNER_LOCATION, ENGINE_RUNNER_ROLE, FLINK_RUNNER_LOCATION,
    TRINO_CLI_LOCATION, TRINO_LAUNCHER_LOCATION, TRINO_NATIVE_LAUNCHER_LOCATION,
    TRINO_SERVER_LOCATION,
};

#[allow(dead_code)]
pub(crate) const RUNNER_REVISION: &str = "5e10f36e7e99815df273c7b567e466749f04d4be";

#[allow(dead_code)]
pub(crate) fn remove_engine_runner(profile: &mut Profile) {
    let engine_id = profile
        .services
        .iter()
        .find(|service| service.role == "stock-engine")
        .expect("engine fixture must bind one stock engine")
        .component
        .clone();
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
        .find(|component| component.id == engine_id)
        .expect("engine fixture must contain its selected engine");
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

#[allow(dead_code)]
pub(crate) fn select_synthetic_materialized_flink(profile: &mut Profile, candidate: &Profile) {
    let mut flink = candidate
        .components
        .iter()
        .find(|component| component.id.as_str() == "flink")
        .expect("candidate fixture must contain Flink")
        .clone();
    let spark_index = profile
        .components
        .iter()
        .position(|component| component.id.as_str() == "spark-4.1")
        .expect("materialized fixture must contain Spark");
    flink.artifact = profile.components.remove(spark_index).artifact;
    let RuntimeArtifact::ContainerImage {
        embedded_artifacts, ..
    } = &mut flink.artifact
    else {
        panic!("materialized engine fixture must be an image");
    };
    embedded_artifacts.retain_mut(|artifact| {
        if artifact.location == "image:/opt/spark/bin/spark-submit" {
            artifact.location = "image:/opt/flink/bin/flink".to_owned();
            true
        } else if artifact.location.contains("iceberg-spark-runtime-4.1_2.13") {
            artifact.location =
                "image:/opt/flink/lib/iceberg-flink-runtime-2.1-1.11.0.jar".to_owned();
            true
        } else if artifact.location.contains("iceberg-aws-bundle") {
            artifact.location = "image:/opt/flink/lib/iceberg-aws-bundle-1.11.0.jar".to_owned();
            true
        } else {
            artifact.location.strip_prefix("image:") == Some(ENGINE_RUNNER_LOCATION)
        }
    });
    let mut runner_jar = embedded_artifacts
        .iter()
        .find(|artifact| artifact.media_type == "application/java-archive")
        .expect("synthetic Flink fixture needs one JAR identity")
        .clone();
    runner_jar.location = format!("image:{FLINK_RUNNER_LOCATION}");
    runner_jar.digest.value = "a".repeat(64);
    runner_jar.bytes = Some(12_345);
    runner_jar.description = Some("Synthetic source-bound Flink runner fixture.".to_owned());
    embedded_artifacts.push(runner_jar.clone());
    profile.components.push(flink);
    profile
        .services
        .retain(|service| service.component.as_str() != "spark-4.1");
    profile.services.push(
        candidate
            .services
            .iter()
            .find(|service| service.component.as_str() == "flink")
            .expect("candidate fixture must bind Flink")
            .clone(),
    );

    let runner = profile
        .components
        .iter_mut()
        .find(|component| component.id.as_str() == ENGINE_RUNNER_COMPONENT_ID)
        .expect("materialized fixture must contain the engine runner");
    let RuntimeArtifact::ContainerImage {
        embedded_artifacts, ..
    } = &mut runner.artifact
    else {
        panic!("runner fixture must be an image");
    };
    embedded_artifacts.push(runner_jar);

    let connector = profile
        .components
        .iter_mut()
        .find(|component| component.id.as_str() == "iceberg-java")
        .expect("materialized fixture must contain the Iceberg connector");
    let RuntimeArtifact::ContainerImage {
        embedded_artifacts, ..
    } = &mut connector.artifact
    else {
        panic!("connector fixture must be an image");
    };
    for artifact in embedded_artifacts {
        if artifact.location.contains("iceberg-spark-runtime-4.1_2.13") {
            artifact.location =
                "image:/opt/iceberg/iceberg-flink-runtime-2.1-1.11.0.jar".to_owned();
        }
    }
}

#[allow(dead_code)]
pub(crate) fn select_synthetic_materialized_trino(profile: &mut Profile, candidate: &Profile) {
    let mut trino = candidate
        .components
        .iter()
        .find(|component| component.id.as_str() == "trino")
        .expect("candidate fixture must contain Trino")
        .clone();
    let spark_index = profile
        .components
        .iter()
        .position(|component| component.id.as_str() == "spark-4.1")
        .expect("materialized fixture must contain Spark");
    trino.artifact = profile.components.remove(spark_index).artifact;
    let RuntimeArtifact::ContainerImage {
        embedded_artifacts, ..
    } = &mut trino.artifact
    else {
        panic!("materialized engine fixture must be an image");
    };
    for artifact in &mut *embedded_artifacts {
        if artifact.location == "image:/opt/spark/bin/spark-submit" {
            artifact.location = format!("image:{TRINO_SERVER_LOCATION}");
        } else if artifact.location.contains("iceberg-spark-runtime-4.1_2.13") {
            artifact.location =
                "image:/usr/lib/trino/plugin/iceberg/iceberg-core-1.11.0.jar".to_owned();
        } else if artifact.location.contains("iceberg-aws-bundle") {
            artifact.location =
                "image:/usr/lib/trino/plugin/iceberg/iceberg-aws-1.11.0.jar".to_owned();
        }
    }
    let mut cli = embedded_artifacts
        .iter()
        .find(|artifact| artifact.media_type == "application/java-archive")
        .expect("synthetic Trino fixture needs a Java archive identity")
        .clone();
    cli.location = format!("image:{TRINO_CLI_LOCATION}");
    cli.digest.value = "b".repeat(64);
    cli.bytes = Some(54_321);
    cli.description = Some("Synthetic stock Trino CLI fixture.".to_owned());
    embedded_artifacts.push(cli);
    let mut launcher = embedded_artifacts
        .iter()
        .find(|artifact| artifact.location == format!("image:{TRINO_SERVER_LOCATION}"))
        .expect("synthetic Trino fixture needs a server launcher identity")
        .clone();
    launcher.location = format!("image:{TRINO_LAUNCHER_LOCATION}");
    launcher.media_type = "application/x-shellscript".to_owned();
    launcher.digest.value = "c".repeat(64);
    launcher.bytes = Some(67_890);
    launcher.description = Some("Synthetic stock Trino launcher fixture.".to_owned());
    embedded_artifacts.push(launcher);
    let mut native_launcher = embedded_artifacts
        .last()
        .expect("synthetic Trino fixture needs a launcher identity")
        .clone();
    native_launcher.location = format!("image:{TRINO_NATIVE_LAUNCHER_LOCATION}");
    native_launcher.media_type = "application/vnd.elf".to_owned();
    native_launcher.digest.value = "d".repeat(64);
    native_launcher.bytes = Some(3_932_322);
    native_launcher.description = Some("Synthetic stock Trino native launcher fixture.".to_owned());
    embedded_artifacts.push(native_launcher);
    profile.components.push(trino);
    profile
        .services
        .retain(|service| service.component.as_str() != "spark-4.1");
    profile.services.push(
        candidate
            .services
            .iter()
            .find(|service| service.component.as_str() == "trino")
            .expect("candidate fixture must bind Trino")
            .clone(),
    );

    let connector = profile
        .components
        .iter_mut()
        .find(|component| component.id.as_str() == "iceberg-java")
        .expect("materialized fixture must contain the Iceberg connector");
    let RuntimeArtifact::ContainerImage {
        embedded_artifacts, ..
    } = &mut connector.artifact
    else {
        panic!("connector fixture must be an image");
    };
    for artifact in embedded_artifacts {
        if artifact.location.contains("iceberg-spark-runtime-4.1_2.13") {
            artifact.location = "image:/opt/iceberg/iceberg-core-1.11.0.jar".to_owned();
        } else if artifact.location.contains("iceberg-aws-bundle") {
            artifact.location = "image:/opt/iceberg/iceberg-aws-1.11.0.jar".to_owned();
        }
    }
}
