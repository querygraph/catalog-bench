//! Closed subprocess grammar for the pinned stock Trino launcher and CLI.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;

use crate::process::{configure_sanitized_process, terminate_child};
use crate::TRINO_CATALOG_NAME;

const TRINO_SERVER_URI: &str = "http://127.0.0.1:8080";
const TRINO_USER: &str = "catalog_bench";
const MAXIMUM_TRINO_SQL_BYTES: usize = 1024 * 1024;
const MAXIMUM_TRINO_CAPTURE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrinoInvocationError;

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
