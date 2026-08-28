use std::fs;

const DOCKERFILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docker/trino/Dockerfile");
const BUILD_SCRIPT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docker/build-trino-images.sh"
);
const COMPOSE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docker-compose.yml");
const SOURCE_REVISION: &str = "6131423fc4ea06cba0d651dd479b23d29495f5b5";
const TRINO_DIGEST: &str =
    "sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534";

#[test]
fn trino_image_is_exact_stock_runtime_plus_source_bound_runner() {
    let source = fs::read_to_string(DOCKERFILE).unwrap();
    for required in [
        TRINO_DIGEST,
        "FROM catalog-bench-engine-runner AS runner",
        "COPY --from=runner /usr/local/bin/catalog-bench-engine /usr/local/bin/catalog-bench-engine",
        "org.opencontainers.image.version=\"483\"",
        "io.querygraph.catalog-bench.runner-source-revision",
        "USER trino",
    ] {
        assert!(source.contains(required), "Trino image lost `{required}`");
    }
    for forbidden in ["curl ", "wget ", "apt-get", "COPY . ", ":latest"] {
        assert!(
            !source.contains(forbidden),
            "Trino image gained `{forbidden}`"
        );
    }
}

#[test]
fn trino_compose_verifies_arm64_child_and_runs_one_composite_image() {
    let build = fs::read_to_string(BUILD_SCRIPT).unwrap();
    let compose = fs::read_to_string(COMPOSE).unwrap();
    for required in [
        TRINO_DIGEST,
        "docker pull --platform linux/arm64",
        "actual_digest",
        "COMPOSE_PROFILES=lakekeeper,polaris,gravitino,trino",
    ] {
        assert!(build.contains(required), "Trino build lost `{required}`");
    }
    for forbidden in ["prune", "volume rm", "system reset", "rm -rf"] {
        assert!(
            !build.contains(forbidden),
            "Trino build gained `{forbidden}`"
        );
    }
    assert_eq!(compose.matches(SOURCE_REVISION).count(), 3);
    for required in [
        "trino-engine-runner-base:",
        "trino-engine:",
        "catalog-bench-engine-runner: \"service:trino-engine-runner-base\"",
        "catalog-bench/trino:483-runner-6131423fc4ea",
    ] {
        assert!(
            compose.contains(required),
            "Trino Compose lost `{required}`"
        );
    }
}
