use std::path::Path;
use std::time::Duration;

use catalog_bench_common::contract::{parse_contract, ComponentId, ContractDocument};
use catalog_bench_engine::{
    InteroperabilityPlan, RunningTrinoServer, StagedTrinoServer, TrinoCliInvocation,
    TrinoCliOutput, TrinoCommandExecutor, TrinoCommandFailure, TrinoLauncherInvocation,
    TrinoRenderedProgram, TrinoServerConfiguration, TrinoServerEnvironment, TrinoServerFailure,
    TRINO_CLI_LOCATION, TRINO_LAUNCHER_LOCATION,
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
        ["--etc-dir", "/run/catalog-bench/trino/etc", "run"]
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

#[tokio::test]
async fn server_supervisor_waits_for_typed_readiness_and_stops_the_process_group() {
    let directory = TempDir::new().unwrap();
    let captured = directory.path().join("environment");
    let launcher = script(
        &directory,
        "launcher.sh",
        &format!(
            "printf '%s\\n%s\\n%s\\n%s\\n%s' \"$CATALOG_BENCH_TRINO_NODE_ID\" \"$CATALOG_BENCH_TRINO_DATA_DIR\" \"$CATALOG_BENCH_S3_ACCESS_KEY_ID\" \"$CATALOG_BENCH_S3_SECRET_ACCESS_KEY\" \"$CATALOG_BENCH_ENGINE_OAUTH_CREDENTIAL\" > '{}'; while :; do sleep 1; done",
            captured.display()
        ),
    );
    let counter = directory.path().join("counter");
    let cli = script(
        &directory,
        "cli.sh",
        &format!(
            "count=0; test ! -f '{0}' || count=$(cat '{0}'); count=$((count + 1)); printf '%s' \"$count\" > '{0}'; if test \"$count\" -lt 3; then exit 1; fi; printf '{{\"ready\":1}}\\n'",
            counter.display()
        ),
    );
    let invocation = TrinoLauncherInvocation::new(&launcher, directory.path()).unwrap();
    let environment = environment(&directory);
    let server = RunningTrinoServer::start(
        &invocation,
        &cli,
        directory.path(),
        &environment,
        Duration::from_secs(2),
        Duration::from_millis(20),
    )
    .await
    .unwrap();
    let pid = server.process_id().unwrap();
    assert!(
        std::fs::read_to_string(counter)
            .unwrap()
            .parse::<u64>()
            .unwrap()
            >= 3
    );
    assert_eq!(
        std::fs::read_to_string(captured).unwrap(),
        format!(
            "catalog-bench-test-node\n{}\naccess-value\nsecret-value\nclient-id:client-secret",
            directory.path().join("data").display()
        )
    );
    let debug = format!("{environment:?}");
    assert!(!debug.contains("access-value"));
    assert!(!debug.contains("secret-value"));
    assert!(!debug.contains("client-secret"));
    server.shutdown().await;
    #[cfg(unix)]
    assert_eq!(unsafe { libc::kill(i32::try_from(pid).unwrap(), 0) }, -1);
}

#[tokio::test]
async fn server_supervisor_classifies_exit_timeout_and_invalid_configuration() {
    let directory = TempDir::new().unwrap();
    let cli = script(&directory, "cli.sh", "exit 1");
    let exited = script(&directory, "exit.sh", "exit 7");
    let invocation = TrinoLauncherInvocation::new(&exited, directory.path()).unwrap();
    let environment = environment(&directory);
    assert_eq!(
        RunningTrinoServer::start(
            &invocation,
            &cli,
            directory.path(),
            &environment,
            Duration::from_secs(1),
            Duration::from_millis(10),
        )
        .await
        .unwrap_err(),
        TrinoServerFailure::Exited
    );
    let running = script(&directory, "running.sh", "while :; do sleep 1; done");
    let invocation = TrinoLauncherInvocation::new(&running, directory.path()).unwrap();
    assert_eq!(
        RunningTrinoServer::start(
            &invocation,
            &cli,
            directory.path(),
            &environment,
            Duration::from_millis(80),
            Duration::from_millis(10),
        )
        .await
        .unwrap_err(),
        TrinoServerFailure::Timeout
    );
    assert_eq!(
        RunningTrinoServer::start(
            &invocation,
            &cli,
            directory.path(),
            &environment,
            Duration::ZERO,
            Duration::from_millis(10),
        )
        .await
        .unwrap_err(),
        TrinoServerFailure::InvalidTimeout
    );
}

#[test]
fn server_environment_rejects_empty_secret_and_relative_data_paths() {
    assert!(TrinoServerEnvironment::new(
        "node".to_owned(),
        "relative".into(),
        "access".to_owned(),
        "secret".to_owned(),
        None,
    )
    .is_err());
    assert!(TrinoServerEnvironment::new(
        "node".to_owned(),
        "/data".into(),
        String::new(),
        "secret".to_owned(),
        None,
    )
    .is_err());
}

fn environment(directory: &TempDir) -> TrinoServerEnvironment {
    TrinoServerEnvironment::new(
        "catalog-bench-test-node".to_owned(),
        directory.path().join("data"),
        "access-value".to_owned(),
        "secret-value".to_owned(),
        Some("client-id:client-secret".to_owned()),
    )
    .unwrap()
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
