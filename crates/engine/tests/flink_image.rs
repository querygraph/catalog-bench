use std::fs;

const DOCKERFILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docker/flink/Dockerfile");

#[test]
fn flink_image_definition_is_checksum_locked_and_source_correlated() {
    let source = fs::read_to_string(DOCKERFILE).unwrap();
    for required in [
        "catalog-bench/flink-base:2.1.3-arm64-99a499ed",
        "sha256:80ffca22aed9e8b9713a232f3394fd81d7f20322df75efdb2b047dbd3e3a23bb",
        "831a8591fe20c8243b1dbe7d71e3244f31d1665b0804b2e825e38cbbe5ce0cafb8338851f90780735568773e0a6cd07bbec107cda0b896b008b861075358b6f6",
        "--strict-checksums",
        "sha256:39e658d876f253815b4b17c6676bebf6a24d517afe93a21298106d1d7fa97331",
        "sha256:38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f",
        "sha256:99a499ed147b28d358486066ab8308e351b232b2ac81aff69157fdb349c84e18",
        "/opt/catalog-bench/catalog-bench-flink-runner.jar",
        "/usr/local/bin/catalog-bench-engine",
        "/opt/flink/lib/iceberg-flink-runtime-2.1-1.11.0.jar",
        "/opt/flink/lib/iceberg-aws-bundle-1.11.0.jar",
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
