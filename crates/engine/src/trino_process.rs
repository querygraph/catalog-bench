//! Closed subprocess grammar for the pinned stock Trino launcher and CLI.

use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tempfile::{Builder as TempDirBuilder, TempDir};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;

use crate::process::{configure_sanitized_process, terminate_child};
use crate::{TrinoServerConfiguration, TRINO_CATALOG_NAME};

const TRINO_SERVER_URI: &str = "http://127.0.0.1:8080";
const TRINO_USER: &str = "catalog_bench";
const MAXIMUM_TRINO_SQL_BYTES: usize = 1024 * 1024;
const MAXIMUM_TRINO_CAPTURE_BYTES: usize = 16 * 1024 * 1024;

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
                "run".to_owned(),
                "--etc-dir".to_owned(),
                path_argument(configuration)?,
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

fn path_argument(path: &Path) -> Result<String, TrinoInvocationError> {
    path.to_str()
        .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
        .map(str::to_owned)
        .ok_or(TrinoInvocationError)
}
