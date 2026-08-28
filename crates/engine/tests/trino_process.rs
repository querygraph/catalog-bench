use std::path::Path;

use catalog_bench_engine::{
    TrinoCliInvocation, TrinoCliOutput, TrinoLauncherInvocation, TRINO_CLI_LOCATION,
    TRINO_LAUNCHER_LOCATION,
};

#[test]
fn launcher_uses_only_the_verified_stock_program_and_private_configuration() {
    let invocation = TrinoLauncherInvocation::new(
        Path::new(TRINO_LAUNCHER_LOCATION),
        Path::new("/run/catalog-bench/trino/etc"),
    )
    .unwrap();

    assert_eq!(invocation.executable(), Path::new(TRINO_LAUNCHER_LOCATION));
    assert_eq!(
        invocation.arguments(),
        ["run", "--etc-dir", "/run/catalog-bench/trino/etc"]
    );
}

#[test]
fn cli_has_one_closed_batch_shape_for_json_and_effect_queries() {
    for (output, expected_format) in [
        (TrinoCliOutput::Json, "JSON"),
        (TrinoCliOutput::Discard, "NULL"),
    ] {
        let invocation =
            TrinoCliInvocation::new(Path::new(TRINO_CLI_LOCATION), "SELECT 1 AS ready", output)
                .unwrap();
        assert_eq!(invocation.executable(), Path::new(TRINO_CLI_LOCATION));
        assert_eq!(
            invocation.arguments(),
            [
                "--server",
                "http://127.0.0.1:8080",
                "--user",
                "catalog_bench",
                "--catalog",
                "bench",
                "--source",
                "catalog-bench",
                "--no-progress",
                "--output-format",
                expected_format,
                "--execute",
                "SELECT 1 AS ready",
            ]
        );
    }
}

#[test]
fn invocation_rejects_relative_paths_controls_and_unbounded_sql() {
    assert!(TrinoLauncherInvocation::new(
        Path::new("launcher"),
        Path::new("/run/catalog-bench/trino/etc")
    )
    .is_err());
    assert!(TrinoCliInvocation::new(
        Path::new(TRINO_CLI_LOCATION),
        "SELECT '\0'",
        TrinoCliOutput::Json,
    )
    .is_err());
    assert!(TrinoCliInvocation::new(
        Path::new(TRINO_CLI_LOCATION),
        &"x".repeat(1024 * 1024 + 1),
        TrinoCliOutput::Json,
    )
    .is_err());
}
