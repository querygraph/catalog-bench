use std::fs;

const DOCKERFILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docker/duckdb/Dockerfile"
);
const BUILD_SCRIPT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docker/build-duckdb-images.sh"
);
const COMPOSE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docker-compose.yml");
const SOURCE_REVISION: &str = "14eca11bd9d4a0de2ea0f078be588a9c1c5b279c";

#[test]
fn duckdb_image_is_source_pinned_with_offline_signed_extensions() {
    let source = fs::read_to_string(DOCKERFILE).unwrap();
    for required in [
        "ENABLE_EXTENSION_AUTOINSTALL=OFF",
        "4d869567d4e23b86a78388faf5e1fc2c1b9197be66cf51231ab9193ee8e3b22c",
        "bde75aadc4ebf9edb4b9fecf4aac6025f35fdae56116b2673b0b4990242c3a02",
        "dcd28c0f227b714e524d6d7eae79db7d14a10029303faed6fce71cdcbb81e08d",
        "162a90f01983e0c680822aedc72e0612ac938c6debd9686653ac2f13bc3f0402",
        "69fae819202f4aea2dfd2dbe8a4ef468684ddd4cc990f2f5bb86c3e83476f36b",
        "d2e643e3408d553b9e2e6077dea548dc9ede6d85752ca6e8112c7fd50d1a0d40",
        "io.querygraph.catalog-bench.duckdb-iceberg-revision",
        "io.querygraph.catalog-bench.duckdb-httpfs-revision",
        "io.querygraph.catalog-bench.duckdb-avro-revision",
        "USER duckdb",
    ] {
        assert!(source.contains(required), "DuckDB image lost `{required}`");
    }
    for forbidden in ["INSTALL iceberg", "UPDATE EXTENSIONS", ":latest", "COPY . "] {
        assert!(
            !source.contains(forbidden),
            "DuckDB image gained `{forbidden}`"
        );
    }
}

#[test]
fn duckdb_build_verifies_version_extensions_and_platform() {
    let build = fs::read_to_string(BUILD_SCRIPT).unwrap();
    let compose = fs::read_to_string(COMPOSE).unwrap();
    for required in [
        "docker image inspect",
        "expected linux/arm64",
        "SELECT version()",
        "LOAD httpfs; LOAD iceberg",
        "httpfs|iceberg",
    ] {
        assert!(build.contains(required), "DuckDB build lost `{required}`");
    }
    assert_eq!(compose.matches(SOURCE_REVISION).count(), 2);
    assert!(compose.contains("duckdb-runtime-base:"));
    assert!(compose.contains("catalog-bench/duckdb:1.5.3-arm64"));
}
