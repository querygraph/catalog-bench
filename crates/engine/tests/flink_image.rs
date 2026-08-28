use std::fs;

const DOCKERFILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docker/flink/Dockerfile");
const BUILD_SCRIPT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docker/build-flink-images.sh"
);
const COMPOSE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docker-compose.yml");
const SOURCE_REVISION: &str = "df38c8186cfe3c2400113ae3308250a01d05c800";

#[test]
fn flink_image_definition_is_checksum_locked_and_source_correlated() {
    let source = fs::read_to_string(DOCKERFILE).unwrap();
    for required in [
        "catalog-bench/flink-base:2.1.3-arm64-99a499ed",
        "eclipse-temurin:17-jdk-jammy@sha256:7d5ae9dfe05f32e2a93abd14966de3373961ad18022ee99a647cdbb8867d74e9 AS java-build",
        "sha256:80ffca22aed9e8b9713a232f3394fd81d7f20322df75efdb2b047dbd3e3a23bb",
        "831a8591fe20c8243b1dbe7d71e3244f31d1665b0804b2e825e38cbbe5ce0cafb8338851f90780735568773e0a6cd07bbec107cda0b896b008b861075358b6f6",
        "--strict-checksums",
        "sha256:39e658d876f253815b4b17c6676bebf6a24d517afe93a21298106d1d7fa97331",
        "sha256:38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f",
        "sha256:6443332781f26a7199009d9517cd1eb013fbf54ca1c9759a2a9e487542b1d52a",
        "sha256:bd20267b0555766ae84697dd888f074ca326d8e2ec3c0008928a6ac2966c67de",
        "sha256:99a499ed147b28d358486066ab8308e351b232b2ac81aff69157fdb349c84e18",
        "/opt/catalog-bench/catalog-bench-flink-runner.jar",
        "/usr/local/bin/catalog-bench-engine",
        "/opt/flink/lib/iceberg-flink-runtime-2.1-1.11.0.jar",
        "/opt/flink/lib/iceberg-aws-bundle-1.11.0.jar",
        "/opt/flink/lib/hadoop-client-api-3.4.3.jar",
        "/opt/flink/lib/hadoop-client-runtime-3.4.3.jar",
        "USER flink",
    ] {
        assert!(source.contains(required), "Flink image lost `{required}`");
    }
    assert_eq!(
        source
            .matches("= \"$CATALOG_BENCH_SOURCE_REVISION\"")
            .count(),
        4
    );
    for forbidden in [
        "curl ",
        "wget ",
        "apt-get",
        "COPY . ",
        "type=cache",
        ":latest",
    ] {
        assert!(
            !source.contains(forbidden),
            "Flink image gained `{forbidden}`"
        );
    }
}

#[test]
fn flink_compose_build_resolves_the_pinned_arm_child_and_one_runtime() {
    let build = fs::read_to_string(BUILD_SCRIPT).unwrap();
    let compose = fs::read_to_string(COMPOSE).unwrap();
    for required in [
        "flink:2.1.3-scala_2.12-java17@sha256:cc557bbe316d804e83195717a41788dc1ddb9a965887bd0ab83d148480a7802d",
        "flink@sha256:99a499ed147b28d358486066ab8308e351b232b2ac81aff69157fdb349c84e18",
        "docker buildx imagetools inspect",
        "docker pull --platform linux/arm64 \"$child_reference\"",
        "actual_descriptor",
        "COMPOSE_PROFILES=lakekeeper,polaris,gravitino,flink",
    ] {
        assert!(build.contains(required), "Flink build lost `{required}`");
    }
    for forbidden in ["prune", "volume rm", "system reset", "rm -rf"] {
        assert!(
            !build.contains(forbidden),
            "Flink build gained `{forbidden}`"
        );
    }
    assert_eq!(compose.matches(SOURCE_REVISION).count(), 4);
    for required in [
        "flink-engine-runner-base:",
        "iceberg-flink-runtime:",
        "flink-runner-image:",
        "flink-engine:",
        "catalog-bench-engine-runner: \"service:flink-engine-runner-base\"",
        "entrypoint: [\"/usr/local/bin/catalog-bench-engine\"]",
        "entrypoint: [\"/opt/flink/bin/flink\"]",
    ] {
        assert!(
            compose.contains(required),
            "Flink Compose lost `{required}`"
        );
    }
}
