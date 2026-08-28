//! Closed subprocess grammar for the pinned stock Trino launcher and CLI.

use std::fmt::{Debug, Formatter};
use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use tempfile::{Builder as TempDirBuilder, TempDir};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;
use zeroize::Zeroize as _;

use crate::process::{configure_sanitized_process, terminate_child};
use crate::{TrinoServerConfiguration, S3_ACCESS_KEY_ENV, S3_SECRET_KEY_ENV, TRINO_CATALOG_NAME};

const TRINO_SERVER_URI: &str = "http://127.0.0.1:8080";
const TRINO_USER: &str = "catalog_bench";
const MAXIMUM_TRINO_SQL_BYTES: usize = 1024 * 1024;
const MAXIMUM_TRINO_CAPTURE_BYTES: usize = 16 * 1024 * 1024;
const TRINO_NODE_ID_ENV: &str = "CATALOG_BENCH_TRINO_NODE_ID";
const TRINO_DATA_DIR_ENV: &str = "CATALOG_BENCH_TRINO_DATA_DIR";
const TRINO_OAUTH_CREDENTIAL_ENV: &str = "CATALOG_BENCH_ENGINE_OAUTH_CREDENTIAL";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrinoInvocationError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrinoStageFailure {
    CreateRoot,
    CreateDirectory,
    InvalidPath,
    CreateFile,
    WriteFile,
}

#[derive(Debug)]
pub struct StagedTrinoServer {
    root: TempDir,
    configuration: PathBuf,
    data: PathBuf,
}

impl StagedTrinoServer {
    pub fn create(configuration: &TrinoServerConfiguration) -> Result<Self, TrinoStageFailure> {
        let root = TempDirBuilder::new()
            .prefix("catalog-bench-trino-")
            .tempdir()
            .map_err(|_| TrinoStageFailure::CreateRoot)?;
        restrict_directory(root.path())?;
        let configuration_root = root.path().join("etc");
        let catalog_root = configuration_root.join("catalog");
        let data = root.path().join("data");
        for directory in [&configuration_root, &catalog_root, &data] {
            std::fs::create_dir(directory).map_err(|_| TrinoStageFailure::CreateDirectory)?;
            restrict_directory(directory)?;
        }
        for file in configuration.files() {
            let relative = Path::new(file.relative_path);
            if relative.is_absolute()
                || relative
                    .components()
                    .any(|component| !matches!(component, std::path::Component::Normal(_)))
            {
                return Err(TrinoStageFailure::InvalidPath);
            }
            let path = configuration_root.join(relative);
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                options.mode(0o600);
            }
            let mut output = options
                .open(path)
                .map_err(|_| TrinoStageFailure::CreateFile)?;
            output
                .write_all(file.contents.as_bytes())
                .and_then(|()| output.sync_all())
                .map_err(|_| TrinoStageFailure::WriteFile)?;
        }
        Ok(Self {
            root,
            configuration: configuration_root,
            data,
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        self.root.path()
    }

    #[must_use]
    pub fn configuration(&self) -> &Path {
        &self.configuration
    }

    #[must_use]
    pub fn data(&self) -> &Path {
        &self.data
    }
}

fn restrict_directory(path: &Path) -> Result<(), TrinoStageFailure> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| TrinoStageFailure::CreateDirectory)?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrinoLauncherInvocation {
    executable: PathBuf,
    arguments: Vec<String>,
}

impl TrinoLauncherInvocation {
    pub fn new(executable: &Path, configuration: &Path) -> Result<Self, TrinoInvocationError> {
        if !executable.is_absolute() || !configuration.is_absolute() {
            return Err(TrinoInvocationError);
        }
        path_argument(executable)?;
        Ok(Self {
            executable: executable.to_owned(),
            arguments: vec![
                "--etc-dir".to_owned(),
                path_argument(configuration)?,
                "run".to_owned(),
            ],
        })
    }

    #[must_use]
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    #[must_use]
    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrinoCliOutput {
    Json,
    Discard,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrinoCliInvocation {
    executable: PathBuf,
    arguments: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrinoCommandFailure {
    InvalidTimeout,
    InvalidLimit,
    Spawn,
    MissingStdout,
    Read,
    OutputTooLarge,
    Timeout,
    Exit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrinoServerFailure {
    InvalidTimeout,
    Spawn,
    Exited,
    Probe,
    Timeout,
}

pub struct TrinoServerEnvironment {
    node_id: String,
    data_directory: PathBuf,
    object_store_access_key: String,
    object_store_secret_key: String,
    catalog_oauth_credential: Option<String>,
}

impl TrinoServerEnvironment {
    pub fn new(
        node_id: String,
        data_directory: PathBuf,
        object_store_access_key: String,
        object_store_secret_key: String,
        catalog_oauth_credential: Option<String>,
    ) -> Result<Self, TrinoInvocationError> {
        if !valid_environment_value(&node_id)
            || !data_directory.is_absolute()
            || path_argument(&data_directory).is_err()
            || !valid_environment_value(&object_store_access_key)
            || !valid_environment_value(&object_store_secret_key)
            || catalog_oauth_credential
                .as_deref()
                .is_some_and(|value| !valid_environment_value(value))
        {
            return Err(TrinoInvocationError);
        }
        Ok(Self {
            node_id,
            data_directory,
            object_store_access_key,
            object_store_secret_key,
            catalog_oauth_credential,
        })
    }

    fn apply(&self, command: &mut Command) {
        command
            .env(TRINO_NODE_ID_ENV, &self.node_id)
            .env(TRINO_DATA_DIR_ENV, &self.data_directory)
            .env(S3_ACCESS_KEY_ENV, &self.object_store_access_key)
            .env(S3_SECRET_KEY_ENV, &self.object_store_secret_key);
        if let Some(credential) = &self.catalog_oauth_credential {
            command.env(TRINO_OAUTH_CREDENTIAL_ENV, credential);
        }
    }
}

impl Debug for TrinoServerEnvironment {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TrinoServerEnvironment")
            .field("node_id", &self.node_id)
            .field("data_directory", &self.data_directory)
            .field("object_store_access_key", &"<redacted>")
            .field("object_store_secret_key", &"<redacted>")
            .field(
                "catalog_oauth_credential",
                &self.catalog_oauth_credential.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl Drop for TrinoServerEnvironment {
    fn drop(&mut self) {
        self.object_store_access_key.zeroize();
        self.object_store_secret_key.zeroize();
        self.catalog_oauth_credential.zeroize();
    }
}

#[derive(Debug)]
pub struct RunningTrinoServer {
    child: tokio::process::Child,
}

impl RunningTrinoServer {
    pub async fn start(
        launcher: &TrinoLauncherInvocation,
        cli_executable: &Path,
        root: &Path,
        environment: &TrinoServerEnvironment,
        startup_timeout: Duration,
        probe_interval: Duration,
    ) -> Result<Self, TrinoServerFailure> {
        if startup_timeout.is_zero() || probe_interval.is_zero() {
            return Err(TrinoServerFailure::InvalidTimeout);
        }
        let mut command = Command::new(launcher.executable());
        command.args(launcher.arguments());
        configure_sanitized_process(&mut command, root);
        environment.apply(&mut command);
        command.stdout(Stdio::null());
        let mut child = command.spawn().map_err(|_| TrinoServerFailure::Spawn)?;
        let probe =
            TrinoCliInvocation::new(cli_executable, "SELECT 1 AS ready", TrinoCliOutput::Json)
                .map_err(|_| TrinoServerFailure::Probe)?;
        let probe_timeout = probe_interval.min(Duration::from_secs(5));
        let executor = TrinoCommandExecutor::new(probe_timeout)
            .map_err(|_| TrinoServerFailure::InvalidTimeout)?;
        let started = Instant::now();
        loop {
            match child.try_wait() {
                Ok(Some(_)) | Err(_) => return Err(TrinoServerFailure::Exited),
                Ok(None) => {}
            }
            if let Ok(output) = executor.execute_cli(&probe, root, 1024).await {
                if crate::decode_trino_single_u64(&output, "ready") == Ok(1) {
                    return Ok(Self { child });
                }
            }
            if started.elapsed() >= startup_timeout {
                terminate_child(&mut child).await;
                return Err(TrinoServerFailure::Timeout);
            }
            tokio::time::sleep(
                probe_interval.min(startup_timeout.saturating_sub(started.elapsed())),
            )
            .await;
        }
    }

    pub async fn shutdown(mut self) {
        terminate_child(&mut self.child).await;
    }

    pub fn process_id(&self) -> Option<u32> {
        self.child.id()
    }
}

impl Drop for RunningTrinoServer {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(id) = self.child.id().and_then(|id| i32::try_from(id).ok()) {
            // SAFETY: the child was started in its own process group and no Rust
            // memory is dereferenced. A negative PID targets the isolated group.
            unsafe {
                libc::kill(-id, libc::SIGKILL);
            }
        }
        let _ = self.child.start_kill();
    }
}

#[derive(Debug, Clone)]
pub struct TrinoCommandExecutor {
    timeout: Duration,
}

impl TrinoCommandExecutor {
    pub fn new(timeout: Duration) -> Result<Self, TrinoCommandFailure> {
        if timeout.is_zero() {
            return Err(TrinoCommandFailure::InvalidTimeout);
        }
        Ok(Self { timeout })
    }

    pub async fn execute_cli(
        &self,
        invocation: &TrinoCliInvocation,
        root: &Path,
        maximum_stdout_bytes: usize,
    ) -> Result<Vec<u8>, TrinoCommandFailure> {
        if maximum_stdout_bytes == 0 || maximum_stdout_bytes > MAXIMUM_TRINO_CAPTURE_BYTES {
            return Err(TrinoCommandFailure::InvalidLimit);
        }
        let mut command = Command::new(invocation.executable());
        command.args(invocation.arguments());
        configure_sanitized_process(&mut command, root);
        let mut child = command.spawn().map_err(|_| TrinoCommandFailure::Spawn)?;
        let stdout = child
            .stdout
            .take()
            .ok_or(TrinoCommandFailure::MissingStdout)?;
        let limit = u64::try_from(maximum_stdout_bytes)
            .map_err(|_| TrinoCommandFailure::InvalidLimit)?
            .saturating_add(1);
        let mut reader = tokio::spawn(async move {
            let mut output = Vec::new();
            stdout
                .take(limit)
                .read_to_end(&mut output)
                .await
                .map(|_| output)
        });
        let started = Instant::now();
        enum First {
            Process(std::io::Result<std::process::ExitStatus>),
            Output(Result<std::io::Result<Vec<u8>>, tokio::task::JoinError>),
            Timeout,
        }
        let first = tokio::select! {
            status = child.wait() => First::Process(status),
            output = &mut reader => First::Output(output),
            () = tokio::time::sleep(self.timeout) => First::Timeout,
        };
        let (status, output) = match first {
            First::Process(status) => {
                let status = match status {
                    Ok(status) => status,
                    Err(_) => {
                        terminate_child(&mut child).await;
                        reader.abort();
                        return Err(TrinoCommandFailure::Exit);
                    }
                };
                let output = match timeout(remaining(self.timeout, started), &mut reader).await {
                    Ok(Ok(Ok(output))) => output,
                    Ok(Ok(Err(_))) | Ok(Err(_)) | Err(_) => {
                        terminate_child(&mut child).await;
                        reader.abort();
                        return Err(TrinoCommandFailure::Read);
                    }
                };
                (status, output)
            }
            First::Output(output) => {
                let output = match output {
                    Ok(Ok(output)) => output,
                    Ok(Err(_)) | Err(_) => {
                        terminate_child(&mut child).await;
                        return Err(TrinoCommandFailure::Read);
                    }
                };
                if output.len() > maximum_stdout_bytes {
                    terminate_child(&mut child).await;
                    return Err(TrinoCommandFailure::OutputTooLarge);
                }
                let status = match timeout(remaining(self.timeout, started), child.wait()).await {
                    Ok(Ok(status)) => status,
                    Ok(Err(_)) => return Err(TrinoCommandFailure::Exit),
                    Err(_) => {
                        terminate_child(&mut child).await;
                        return Err(TrinoCommandFailure::Timeout);
                    }
                };
                (status, output)
            }
            First::Timeout => {
                terminate_child(&mut child).await;
                reader.abort();
                return Err(TrinoCommandFailure::Timeout);
            }
        };
        if output.len() > maximum_stdout_bytes {
            return Err(TrinoCommandFailure::OutputTooLarge);
        }
        if !status.success() {
            return Err(TrinoCommandFailure::Exit);
        }
        Ok(output)
    }
}

fn remaining(limit: Duration, started: Instant) -> Duration {
    limit.saturating_sub(started.elapsed())
}

impl TrinoCliInvocation {
    pub fn new(
        executable: &Path,
        sql: &str,
        output: TrinoCliOutput,
    ) -> Result<Self, TrinoInvocationError> {
        if !executable.is_absolute() || !valid_sql(sql) {
            return Err(TrinoInvocationError);
        }
        path_argument(executable)?;
        Ok(Self {
            executable: executable.to_owned(),
            arguments: vec![
                "--server".to_owned(),
                TRINO_SERVER_URI.to_owned(),
                "--user".to_owned(),
                TRINO_USER.to_owned(),
                "--catalog".to_owned(),
                TRINO_CATALOG_NAME.to_owned(),
                "--source".to_owned(),
                "catalog-bench".to_owned(),
                "--no-progress".to_owned(),
                "--output-format".to_owned(),
                match output {
                    TrinoCliOutput::Json => "JSON",
                    TrinoCliOutput::Discard => "NULL",
                }
                .to_owned(),
                "--execute".to_owned(),
                sql.to_owned(),
            ],
        })
    }

    #[must_use]
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    #[must_use]
    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }
}

fn valid_sql(sql: &str) -> bool {
    !sql.is_empty() && sql.len() <= MAXIMUM_TRINO_SQL_BYTES && !sql.chars().any(char::is_control)
}

fn valid_environment_value(value: &str) -> bool {
    !value.is_empty() && !value.chars().any(|character| character == '\0')
}

fn path_argument(path: &Path) -> Result<String, TrinoInvocationError> {
    path.to_str()
        .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
        .map(str::to_owned)
        .ok_or(TrinoInvocationError)
}
