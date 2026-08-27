use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tempfile::{Builder as TempDirBuilder, TempDir};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStdout, Command};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use zeroize::Zeroize as _;

use crate::{
    CatalogCredentialSource, EngineEventCapture, EngineEventDecoder, EngineFailureCategory,
    EngineProtocolFailureKind, EngineStage, InteroperabilityPlan, RuntimeVerification,
    RuntimeVerifier, ENGINE_OAUTH_CLIENT_ID_ENV, ENGINE_OAUTH_CLIENT_SECRET_ENV,
    SPARK_SUBMIT_LOCATION,
};

const SPARK_RENDERER: &[u8] = include_bytes!("../spark/runner.py");
const DEFAULT_PROCESS_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const PROCESS_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const PROCESS_KILL_TIMEOUT: Duration = Duration::from_secs(5);
const SPARK_SUCCESS_EXIT: i32 = 0;
const SPARK_FAILURE_EXIT: i32 = 2;
const SPARK_COLLISION_EXIT: i32 = 3;
const FALLBACK_PATH: &str =
    "/opt/spark/bin:/opt/spark/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const PUBLIC_ENVIRONMENT_ALLOWLIST: &[&str] = &[
    "JAVA_HOME",
    "LANG",
    "LC_ALL",
    "PATH",
    "PYSPARK_DRIVER_PYTHON",
    "PYSPARK_PYTHON",
    "SPARK_HOME",
    "TZ",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SparkCredentialKind {
    ObjectStoreAccessKey,
    ObjectStoreSecretKey,
    CatalogClientId,
    CatalogClientSecret,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SparkCredentialFailureKind {
    Missing,
    Empty,
    Unreadable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SparkCredentialFailure {
    pub credential: SparkCredentialKind,
    pub kind: SparkCredentialFailureKind,
}

pub enum SecretRead {
    Missing,
    Unreadable,
    Value(String),
}

impl Debug for SecretRead {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing => formatter.write_str("Missing"),
            Self::Unreadable => formatter.write_str("Unreadable"),
            Self::Value(_) => formatter.write_str("Value(<redacted>)"),
        }
    }
}

pub trait SecretSource {
    fn read_secret(&self, name: &str) -> SecretRead;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessEnvironment;

impl SecretSource for ProcessEnvironment {
    fn read_secret(&self, name: &str) -> SecretRead {
        match std::env::var(name) {
            Ok(value) => SecretRead::Value(value),
            Err(std::env::VarError::NotPresent) => SecretRead::Missing,
            Err(std::env::VarError::NotUnicode(_)) => SecretRead::Unreadable,
        }
    }
}

struct SecretValue(String);

impl SecretValue {
    fn expose(&self) -> &str {
        &self.0
    }
}

impl Debug for SecretValue {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl Drop for SecretValue {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

struct SparkSecrets {
    object_store_access_key: SecretValue,
    object_store_secret_key: SecretValue,
    catalog_oauth: Option<(SecretValue, SecretValue)>,
}

impl SparkSecrets {
    fn load(
        plan: &InteroperabilityPlan,
        source: &(impl SecretSource + ?Sized),
    ) -> Result<Self, SparkCredentialFailure> {
        let object_store_access_key = read_required(
            source,
            &plan.object_store().access_key_env,
            SparkCredentialKind::ObjectStoreAccessKey,
        )?;
        let object_store_secret_key = read_required(
            source,
            &plan.object_store().secret_key_env,
            SparkCredentialKind::ObjectStoreSecretKey,
        )?;
        let catalog_oauth = match plan.credential_source() {
            CatalogCredentialSource::Anonymous => None,
            CatalogCredentialSource::OAuth2ClientCredentials {
                client_id_env,
                client_secret_env,
            } => Some((
                read_required(source, client_id_env, SparkCredentialKind::CatalogClientId)?,
                read_required(
                    source,
                    client_secret_env,
                    SparkCredentialKind::CatalogClientSecret,
                )?,
            )),
        };
        Ok(Self {
            object_store_access_key,
            object_store_secret_key,
            catalog_oauth,
        })
    }
}

impl Debug for SparkSecrets {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SparkSecrets")
            .field("object_store_access_key", &"<redacted>")
            .field("object_store_secret_key", &"<redacted>")
            .field(
                "catalog_oauth",
                &self.catalog_oauth.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

fn read_required(
    source: &(impl SecretSource + ?Sized),
    name: &str,
    credential: SparkCredentialKind,
) -> Result<SecretValue, SparkCredentialFailure> {
    match source.read_secret(name) {
        SecretRead::Missing => Err(SparkCredentialFailure {
            credential,
            kind: SparkCredentialFailureKind::Missing,
        }),
        SecretRead::Unreadable => Err(SparkCredentialFailure {
            credential,
            kind: SparkCredentialFailureKind::Unreadable,
        }),
        SecretRead::Value(value) => {
            let value = SecretValue(value);
            if value.expose().is_empty() {
                Err(SparkCredentialFailure {
                    credential,
                    kind: SparkCredentialFailureKind::Empty,
                })
            } else {
                Ok(value)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SparkProcessConfigurationError;

impl std::fmt::Display for SparkProcessConfigurationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Spark process timeout must be positive")
    }
}

impl std::error::Error for SparkProcessConfigurationError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SparkPreparationFailureKind {
    TemporaryDirectory,
    EncodePlan,
    WritePlan,
    WriteRenderer,
    CreateLocalDirectory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SparkProcessOutcome {
    RuntimeRejected,
    CredentialRejected {
        failure: SparkCredentialFailure,
    },
    PreparationFailed {
        kind: SparkPreparationFailureKind,
    },
    SpawnFailed,
    TimedOut,
    StdoutFailed,
    WaitFailed,
    ProtocolRejected {
        kind: EngineProtocolFailureKind,
    },
    ExitProtocolMismatch,
    Completed,
    FixtureCollision,
    EngineFailed {
        stage: EngineStage,
        category: EngineFailureCategory,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SparkProcessExecution {
    pub runtime: RuntimeVerification,
    pub outcome: SparkProcessOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture: Option<EngineEventCapture>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_elapsed_micros: Option<u64>,
}

impl SparkProcessExecution {
    #[must_use]
    pub fn passed(&self) -> bool {
        matches!(self.outcome, SparkProcessOutcome::Completed)
    }

    #[must_use]
    pub fn cleanup_authorized(&self) -> bool {
        self.capture
            .as_ref()
            .is_some_and(EngineEventCapture::cleanup_authorized)
    }

    #[must_use]
    pub fn fixture_collision(&self) -> bool {
        matches!(self.outcome, SparkProcessOutcome::FixtureCollision)
    }

    fn before_process(runtime: RuntimeVerification, outcome: SparkProcessOutcome) -> Self {
        Self {
            runtime,
            outcome,
            capture: None,
            exit_code: None,
            process_elapsed_micros: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SparkProcessExecutor {
    process_timeout: Duration,
}

impl SparkProcessExecutor {
    pub fn try_new(process_timeout: Duration) -> Result<Self, SparkProcessConfigurationError> {
        if process_timeout.is_zero() {
            return Err(SparkProcessConfigurationError);
        }
        Ok(Self { process_timeout })
    }

    pub async fn execute(
        &self,
        plan: &InteroperabilityPlan,
        verifier: &RuntimeVerifier,
    ) -> SparkProcessExecution {
        self.execute_with_source(plan, verifier, &ProcessEnvironment)
            .await
    }

    pub async fn execute_with_source(
        &self,
        plan: &InteroperabilityPlan,
        verifier: &RuntimeVerifier,
        secrets: &(impl SecretSource + ?Sized),
    ) -> SparkProcessExecution {
        let runtime = verifier.verify(plan);
        if !runtime.passed() {
            return SparkProcessExecution::before_process(
                runtime,
                SparkProcessOutcome::RuntimeRejected,
            );
        }

        let staged = match StagedSparkInput::create(plan) {
            Ok(staged) => staged,
            Err(kind) => {
                return SparkProcessExecution::before_process(
                    runtime,
                    SparkProcessOutcome::PreparationFailed { kind },
                );
            }
        };
        let secrets = match SparkSecrets::load(plan, secrets) {
            Ok(secrets) => secrets,
            Err(failure) => {
                return SparkProcessExecution::before_process(
                    runtime,
                    SparkProcessOutcome::CredentialRejected { failure },
                );
            }
        };

        let mut command = Command::new(verifier.artifact_path(SPARK_SUBMIT_LOCATION));
        configure_command(&mut command, plan, &staged, &secrets);
        let spawned = command.spawn();
        drop(command);
        drop(secrets);
        let mut child = match spawned {
            Ok(child) => child,
            Err(_) => {
                return SparkProcessExecution::before_process(
                    runtime,
                    SparkProcessOutcome::SpawnFailed,
                );
            }
        };
        let started = Instant::now();
        let Some(stdout) = child.stdout.take() else {
            terminate_child(&mut child).await;
            return SparkProcessExecution {
                runtime,
                outcome: SparkProcessOutcome::StdoutFailed,
                capture: None,
                exit_code: None,
                process_elapsed_micros: Some(elapsed_micros(started)),
            };
        };

        let decoder = Arc::new(Mutex::new(EngineEventDecoder::new()));
        let reader_signal = Arc::new(ReaderSignal::default());
        let reader = tokio::spawn(drain_stdout(
            stdout,
            Arc::clone(&decoder),
            Arc::clone(&reader_signal),
        ));
        let wait = tokio::select! {
            result = child.wait() => match result {
                Ok(status) => WaitObservation::Exited(status),
                Err(_) => WaitObservation::Failed,
            },
            issue = reader_signal.notified() => WaitObservation::ReaderStopped(issue),
            () = tokio::time::sleep(self.process_timeout) => WaitObservation::TimedOut,
        };
        if !matches!(wait, WaitObservation::Exited(_)) {
            terminate_child(&mut child).await;
        }
        let stdout_failed = finish_reader(reader).await;
        let capture = finish_decoder(decoder);
        let exit_code = match &wait {
            WaitObservation::Exited(status) => status.code(),
            WaitObservation::TimedOut
            | WaitObservation::Failed
            | WaitObservation::ReaderStopped(_) => None,
        };
        let outcome = classify_process(wait, stdout_failed, &capture);

        SparkProcessExecution {
            runtime,
            outcome,
            capture: Some(capture),
            exit_code,
            process_elapsed_micros: Some(elapsed_micros(started)),
        }
    }
}

impl Default for SparkProcessExecutor {
    fn default() -> Self {
        Self {
            process_timeout: DEFAULT_PROCESS_TIMEOUT,
        }
    }
}

struct StagedSparkInput {
    directory: TempDir,
    renderer: PathBuf,
    plan: PathBuf,
    local: PathBuf,
}

impl StagedSparkInput {
    fn create(plan: &InteroperabilityPlan) -> Result<Self, SparkPreparationFailureKind> {
        let directory = TempDirBuilder::new()
            .prefix("catalog-bench-spark-")
            .tempdir()
            .map_err(|_| SparkPreparationFailureKind::TemporaryDirectory)?;
        let renderer = directory.path().join("runner.py");
        let plan_path = directory.path().join("plan.json");
        let local = directory.path().join("local");
        let encoded = serde_json::to_vec(plan.spark())
            .map_err(|_| SparkPreparationFailureKind::EncodePlan)?;
        std::fs::write(&plan_path, encoded).map_err(|_| SparkPreparationFailureKind::WritePlan)?;
        std::fs::write(&renderer, SPARK_RENDERER)
            .map_err(|_| SparkPreparationFailureKind::WriteRenderer)?;
        std::fs::create_dir(&local)
            .map_err(|_| SparkPreparationFailureKind::CreateLocalDirectory)?;
        Ok(Self {
            directory,
            renderer,
            plan: plan_path,
            local,
        })
    }

    fn root(&self) -> &Path {
        self.directory.path()
    }
}

fn configure_command(
    command: &mut Command,
    plan: &InteroperabilityPlan,
    staged: &StagedSparkInput,
    secrets: &SparkSecrets,
) {
    command
        .arg(&staged.renderer)
        .arg("--plan")
        .arg(&staged.plan)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .env_clear()
        .current_dir(staged.root());
    #[cfg(unix)]
    command.process_group(0);
    for name in PUBLIC_ENVIRONMENT_ALLOWLIST {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    if std::env::var_os("PATH").is_none() {
        command.env("PATH", FALLBACK_PATH);
    }
    if std::env::var_os("SPARK_HOME").is_none() {
        command.env("SPARK_HOME", "/opt/spark");
    }
    command
        .env("HOME", staged.root())
        .env("TMPDIR", staged.root())
        .env("SPARK_LOCAL_DIRS", &staged.local)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .env(
            "AWS_ACCESS_KEY_ID",
            secrets.object_store_access_key.expose(),
        )
        .env(
            "AWS_SECRET_ACCESS_KEY",
            secrets.object_store_secret_key.expose(),
        )
        .env("AWS_REGION", &plan.object_store().region)
        .env("AWS_DEFAULT_REGION", &plan.object_store().region)
        .env("AWS_EC2_METADATA_DISABLED", "true");
    if let Some((client_id, client_secret)) = &secrets.catalog_oauth {
        command
            .env(ENGINE_OAUTH_CLIENT_ID_ENV, client_id.expose())
            .env(ENGINE_OAUTH_CLIENT_SECRET_ENV, client_secret.expose());
    }
}

enum WaitObservation {
    Exited(ExitStatus),
    TimedOut,
    Failed,
    ReaderStopped(ReaderIssue),
}

fn classify_process(
    wait: WaitObservation,
    stdout_failed: bool,
    capture: &EngineEventCapture,
) -> SparkProcessOutcome {
    match wait {
        WaitObservation::TimedOut => return SparkProcessOutcome::TimedOut,
        WaitObservation::Failed => return SparkProcessOutcome::WaitFailed,
        WaitObservation::ReaderStopped(ReaderIssue::Stdout) => {
            return SparkProcessOutcome::StdoutFailed;
        }
        WaitObservation::ReaderStopped(ReaderIssue::Protocol) => {
            return capture
                .failure
                .as_ref()
                .map(|failure| SparkProcessOutcome::ProtocolRejected { kind: failure.kind })
                .unwrap_or(SparkProcessOutcome::StdoutFailed);
        }
        WaitObservation::Exited(_) if stdout_failed => return SparkProcessOutcome::StdoutFailed,
        WaitObservation::Exited(_) => {}
    }
    if let Some(failure) = &capture.failure {
        return SparkProcessOutcome::ProtocolRejected { kind: failure.kind };
    }
    let WaitObservation::Exited(status) = wait else {
        unreachable!("non-exit process states returned above")
    };
    match status.code() {
        Some(SPARK_SUCCESS_EXIT) if capture.completed() => SparkProcessOutcome::Completed,
        Some(SPARK_COLLISION_EXIT) if capture.fixture_collision() => {
            SparkProcessOutcome::FixtureCollision
        }
        Some(SPARK_FAILURE_EXIT) => capture
            .engine_failure()
            .map(|(stage, category)| SparkProcessOutcome::EngineFailed { stage, category })
            .unwrap_or(SparkProcessOutcome::ExitProtocolMismatch),
        Some(_) | None => SparkProcessOutcome::ExitProtocolMismatch,
    }
}

async fn drain_stdout(
    mut stdout: ChildStdout,
    decoder: Arc<Mutex<EngineEventDecoder>>,
    signal: Arc<ReaderSignal>,
) -> bool {
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        match stdout.read(&mut buffer).await {
            Ok(0) => return false,
            Ok(bytes) => {
                let failed = match decoder.lock() {
                    Ok(mut decoder) => {
                        decoder.push(&buffer[..bytes]);
                        decoder.failed()
                    }
                    Err(_) => {
                        signal.report(ReaderIssue::Stdout);
                        return true;
                    }
                };
                if failed {
                    signal.report(ReaderIssue::Protocol);
                    return false;
                }
            }
            Err(_) => {
                signal.report(ReaderIssue::Stdout);
                return true;
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum ReaderIssue {
    Protocol = 1,
    Stdout = 2,
}

#[derive(Debug, Default)]
struct ReaderSignal {
    issue: AtomicU8,
    notification: Notify,
}

impl ReaderSignal {
    fn report(&self, issue: ReaderIssue) {
        if self
            .issue
            .compare_exchange(0, issue as u8, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            self.notification.notify_one();
        }
    }

    async fn notified(&self) -> ReaderIssue {
        self.notification.notified().await;
        match self.issue.load(Ordering::Acquire) {
            value if value == ReaderIssue::Protocol as u8 => ReaderIssue::Protocol,
            value if value == ReaderIssue::Stdout as u8 => ReaderIssue::Stdout,
            _ => unreachable!("reader notification requires a recorded issue"),
        }
    }
}

async fn finish_reader(mut reader: JoinHandle<bool>) -> bool {
    match timeout(PROCESS_DRAIN_TIMEOUT, &mut reader).await {
        Ok(Ok(failed)) => failed,
        Ok(Err(_)) => true,
        Err(_) => {
            reader.abort();
            let _ = reader.await;
            true
        }
    }
}

fn finish_decoder(decoder: Arc<Mutex<EngineEventDecoder>>) -> EngineEventCapture {
    let decoder = match Arc::try_unwrap(decoder) {
        Ok(decoder) => decoder,
        Err(_) => return EngineEventDecoder::new().finish(),
    };
    decoder
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .finish()
}

async fn terminate_child(child: &mut Child) {
    #[cfg(unix)]
    let group_killed = child.id().is_some_and(|id| {
        i32::try_from(id).is_ok_and(|id| {
            // The command starts a fresh process group. A negative PID targets
            // SparkSubmit and every local child (including its Python worker).
            // SAFETY: `kill` dereferences no Rust memory; `id` came from this
            // live child and is negated only to address its isolated group.
            unsafe { libc::kill(-id, libc::SIGKILL) == 0 }
        })
    });
    #[cfg(not(unix))]
    let group_killed = false;
    if !group_killed {
        let _ = child.start_kill();
    }
    let _ = timeout(PROCESS_KILL_TIMEOUT, child.wait()).await;
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}
