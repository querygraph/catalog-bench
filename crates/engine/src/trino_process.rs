//! Closed subprocess grammar for the pinned stock Trino launcher and CLI.

use std::path::{Path, PathBuf};

use crate::TRINO_CATALOG_NAME;

const TRINO_SERVER_URI: &str = "http://127.0.0.1:8080";
const TRINO_USER: &str = "catalog_bench";
const MAXIMUM_TRINO_SQL_BYTES: usize = 1024 * 1024;

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
