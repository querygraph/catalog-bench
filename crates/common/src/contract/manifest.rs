use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    child_path, require_non_empty, validate_artifacts, ArtifactReference, BundleId, ComponentId,
    ContractVersion, Extensions, Validate, ValidationIssue,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ManifestDocumentKind {
    #[serde(rename = "manifest")]
    Manifest,
}

/// How a result bundle came into existence. Historical imports are first-class
/// and cannot be mistaken for a fresh live run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Provenance {
    LiveRun {
        runner: ComponentId,
        sanitized_invocation: String,
        started_at: String,
        completed_at: String,
    },
    HistoricalImport {
        source_date: String,
        imported_at: String,
        explanation: String,
    },
    Fixture {
        explanation: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RedactionStatement {
    pub reviewed: bool,
    pub policy: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed_fields: Vec<String>,
}

/// Root index for one immutable result bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResultBundleManifest {
    pub contract_version: ContractVersion,
    pub kind: ManifestDocumentKind,
    pub id: BundleId,
    pub title: String,
    pub created_at: String,
    pub provenance: Provenance,
    pub profile: ArtifactReference,
    pub scenarios: Vec<ArtifactReference>,
    pub results: Vec<ArtifactReference>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_evidence: Vec<ArtifactReference>,
    pub redaction: RedactionStatement,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: Extensions,
}

impl Validate for ResultBundleManifest {
    fn collect_issues(&self, path: &str, issues: &mut Vec<ValidationIssue>) {
        self.id.collect_issues(&child_path(path, "id"), issues);
        require_non_empty(&self.title, child_path(path, "title"), issues);
        require_non_empty(&self.created_at, child_path(path, "created_at"), issues);
        validate_provenance(&self.provenance, &child_path(path, "provenance"), issues);
        self.profile
            .collect_issues(&child_path(path, "profile"), issues);
        if self.scenarios.is_empty() {
            issues.push(ValidationIssue::new(
                child_path(path, "scenarios"),
                "must contain at least one scenario",
            ));
        }
        if self.results.is_empty() {
            issues.push(ValidationIssue::new(
                child_path(path, "results"),
                "must contain at least one result",
            ));
        }
        validate_artifacts(&self.scenarios, &child_path(path, "scenarios"), issues);
        validate_artifacts(&self.results, &child_path(path, "results"), issues);
        validate_artifacts(
            &self.source_evidence,
            &child_path(path, "source_evidence"),
            issues,
        );
        if !self.redaction.reviewed {
            issues.push(ValidationIssue::new(
                child_path(path, "redaction.reviewed"),
                "must be true before publication",
            ));
        }
        require_non_empty(
            &self.redaction.policy,
            child_path(path, "redaction.policy"),
            issues,
        );
    }
}

fn validate_provenance(provenance: &Provenance, path: &str, issues: &mut Vec<ValidationIssue>) {
    match provenance {
        Provenance::LiveRun {
            runner,
            sanitized_invocation,
            started_at,
            completed_at,
        } => {
            runner.collect_issues(&child_path(path, "runner"), issues);
            require_non_empty(
                sanitized_invocation,
                child_path(path, "sanitized_invocation"),
                issues,
            );
            require_non_empty(started_at, child_path(path, "started_at"), issues);
            require_non_empty(completed_at, child_path(path, "completed_at"), issues);
        }
        Provenance::HistoricalImport {
            source_date,
            imported_at,
            explanation,
        } => {
            require_non_empty(source_date, child_path(path, "source_date"), issues);
            require_non_empty(imported_at, child_path(path, "imported_at"), issues);
            require_non_empty(explanation, child_path(path, "explanation"), issues);
        }
        Provenance::Fixture { explanation } => {
            require_non_empty(explanation, child_path(path, "explanation"), issues);
        }
    }
}
