use std::path::Path;
use std::time::Duration;

use catalog_bench_engine::{
    TrinoCliInvocation, TrinoCliOutput, TrinoCommandExecutor, TrinoCommandFailure,
    TrinoLauncherInvocation, TRINO_CLI_LOCATION, TRINO_LAUNCHER_LOCATION,
};
use tempfile::TempDir;

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

#[tokio::test]
async fn bounded_executor_uses_a_sanitized_working_environment() {
    let directory = TempDir::new().unwrap();
    let executable = script(
        &directory,
        "success.sh",
        "printf '%s\\n%s\\n' \"$HOME\" \"$TMPDIR\"",
    );
    let invocation =
        TrinoCliInvocation::new(&executable, "SELECT 1 AS ready", TrinoCliOutput::Json).unwrap();
    let output = TrinoCommandExecutor::new(Duration::from_secs(2))
        .unwrap()
        .execute_cli(&invocation, directory.path(), 4096)
        .await
        .unwrap();

    assert_eq!(
        output,
        format!(
            "{}\n{}\n",
            directory.path().display(),
            directory.path().display()
        )
        .as_bytes()
    );
}

#[tokio::test]
async fn bounded_executor_rejects_output_exit_timeout_and_invalid_limits() {
    let directory = TempDir::new().unwrap();
    let executor = TrinoCommandExecutor::new(Duration::from_secs(2)).unwrap();
    for (name, body, limit, expected) in [
        (
            "large.sh",
            "dd if=/dev/zero bs=1025 count=1 2>/dev/null",
            1024,
            TrinoCommandFailure::OutputTooLarge,
        ),
        ("exit.sh", "exit 7", 1, TrinoCommandFailure::Exit),
    ] {
        let executable = script(&directory, name, body);
        let invocation =
            TrinoCliInvocation::new(&executable, "SELECT 1", TrinoCliOutput::Json).unwrap();
        assert_eq!(
            executor
                .execute_cli(&invocation, directory.path(), limit)
                .await,
            Err(expected)
        );
    }
    let timeout_executor = TrinoCommandExecutor::new(Duration::from_millis(100)).unwrap();
    let executable = script(&directory, "timeout.sh", "sleep 30");
    let invocation =
        TrinoCliInvocation::new(&executable, "SELECT 1", TrinoCliOutput::Json).unwrap();
    assert_eq!(
        timeout_executor
            .execute_cli(&invocation, directory.path(), 1)
            .await,
        Err(TrinoCommandFailure::Timeout)
    );
    let executable = script(&directory, "unused.sh", "exit 99");
    let invocation =
        TrinoCliInvocation::new(&executable, "SELECT 1", TrinoCliOutput::Json).unwrap();
    assert_eq!(
        executor.execute_cli(&invocation, directory.path(), 0).await,
        Err(TrinoCommandFailure::InvalidLimit)
    );
    assert!(matches!(
        TrinoCommandExecutor::new(Duration::ZERO),
        Err(TrinoCommandFailure::InvalidTimeout)
    ));
}

#[cfg(unix)]
fn script(directory: &TempDir, name: &str, body: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt as _;

    let path = directory.path().join(name);
    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&path, permissions).unwrap();
    path
}
