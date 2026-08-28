use std::path::Path;
use std::time::Duration;

use catalog_bench_common::contract::{parse_contract, ComponentId, ContractDocument};
use catalog_bench_engine::{
    InteroperabilityPlan, StagedTrinoServer, TrinoCliInvocation, TrinoCliOutput,
    TrinoCommandExecutor, TrinoCommandFailure, TrinoLauncherInvocation, TrinoRenderedProgram,
    TrinoServerConfiguration, TRINO_CLI_LOCATION, TRINO_LAUNCHER_LOCATION,
};
use tempfile::TempDir;

mod support;

use support::select_synthetic_materialized_trino;

const PROFILE: &[u8] =
    include_bytes!("../../../profiles/v1/spark-4.1.3-iceberg-1.11.0-2026-08-27.json");
const CANDIDATE_PROFILE: &[u8] = include_bytes!("../../../profiles/v1/current-2026-08-27.json");
const SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.v2.json");

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

#[test]
fn stages_the_exact_private_configuration_and_removes_it_on_drop() {
    let configuration = configuration();
    let staged = StagedTrinoServer::create(&configuration).unwrap();
    let root = staged.root().to_owned();

    assert!(staged.configuration().starts_with(&root));
    assert!(staged.data().starts_with(&root));
    assert!(staged.data().is_dir());
    for file in configuration.files() {
        let path = staged.configuration().join(file.relative_path);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), file.contents);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        for directory in [staged.root(), staged.configuration(), staged.data()] {
            assert_eq!(
                std::fs::metadata(directory).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
    }
    let staged_text = configuration
        .files()
        .iter()
        .map(|file| file.contents.as_str())
        .collect::<String>();
    assert!(!staged_text.contains("secret-value"));
    assert!(staged_text.contains("${ENV:CATALOG_BENCH_S3_SECRET_ACCESS_KEY}"));

    drop(staged);
    assert!(!root.exists());
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

fn configuration() -> TrinoServerConfiguration {
    let ContractDocument::Profile(mut profile) = parse_contract(PROFILE).unwrap() else {
        panic!("profile fixture must be a profile");
    };
    let ContractDocument::Profile(candidate) = parse_contract(CANDIDATE_PROFILE).unwrap() else {
        panic!("candidate fixture must be a profile");
    };
    let ContractDocument::Scenario(scenario) = parse_contract(SCENARIO).unwrap() else {
        panic!("scenario fixture must be a scenario");
    };
    select_synthetic_materialized_trino(&mut profile, &candidate);
    let plan = InteroperabilityPlan::from_contracts(
        &profile,
        &scenario,
        &ComponentId::from("lakecat"),
        "stage01",
    )
    .unwrap();
    let program = TrinoRenderedProgram::render(plan.trino().unwrap()).unwrap();
    TrinoServerConfiguration::render(&program).unwrap()
}
