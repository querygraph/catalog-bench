//! Versioned, catalog-neutral contracts for interoperability scenarios and evidence.
//!
//! The benchmark binaries' compact [`crate::BenchReport`] remains the process-local
//! wire format. These contracts are the durable publication boundary: they pin a
//! scenario and environment, preserve evidence, and classify every attempted or
//! unattempted result without conflating unsupported behavior with failure.

mod manifest;
mod profile;
mod result;
mod scenario;
mod schema;
mod value;

use std::fmt::{Display, Formatter};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use manifest::*;
pub use profile::*;
pub use result::*;
pub use scenario::*;
pub use schema::*;
pub use value::*;

/// The only contract version accepted by these Rust types.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ContractVersion {
    /// The initial public contract.
    #[serde(rename = "catalog-bench/v1")]
    #[default]
    V1,
}

/// One semantic contract violation with a JSON-style field path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationIssue {
    pub path: String,
    pub message: String,
}

impl ValidationIssue {
    pub(crate) fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

/// All semantic violations found in a document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationErrors(Vec<ValidationIssue>);

impl ValidationErrors {
    #[must_use]
    pub fn issues(&self) -> &[ValidationIssue] {
        &self.0
    }
}

impl Display for ValidationErrors {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        for (index, issue) in self.0.iter().enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{}: {}", issue.path, issue.message)?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationErrors {}

/// Semantic validation beyond what JSON Schema can express clearly.
pub trait Validate {
    /// Add every violation to `issues`, prefixing paths with `path`.
    fn collect_issues(&self, path: &str, issues: &mut Vec<ValidationIssue>);

    /// Validate the complete value without failing at the first problem.
    fn validate(&self) -> Result<(), ValidationErrors> {
        let mut issues = Vec::new();
        self.collect_issues("$", &mut issues);
        if issues.is_empty() {
            Ok(())
        } else {
            Err(ValidationErrors(issues))
        }
    }
}

pub(crate) fn require_non_empty(
    value: &str,
    path: impl Into<String>,
    issues: &mut Vec<ValidationIssue>,
) {
    if value.trim().is_empty() {
        issues.push(ValidationIssue::new(path, "must not be empty"));
    }
}

pub(crate) fn require_finite_non_negative(
    value: f64,
    path: impl Into<String>,
    issues: &mut Vec<ValidationIssue>,
) {
    if !value.is_finite() || value < 0.0 {
        issues.push(ValidationIssue::new(
            path,
            "must be a finite, non-negative number",
        ));
    }
}

pub(crate) fn child_path(parent: &str, child: impl Display) -> String {
    format!("{parent}.{child}")
}

pub(crate) fn indexed_path(parent: &str, index: usize) -> String {
    format!("{parent}[{index}]")
}
