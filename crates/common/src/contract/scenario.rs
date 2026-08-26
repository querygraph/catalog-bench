use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    child_path, indexed_path, require_non_empty, require_unique, AssertionId, ContractVersion,
    Extensions, ScenarioId, StepId, Validate, ValidationIssue,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ScenarioDocumentKind {
    #[serde(rename = "scenario")]
    Scenario,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ScenarioFamily {
    IcebergRest,
    ClientInteroperability,
    Concurrency,
    SecurityAndTenancy,
    Operations,
    MigrationAndFederation,
    SemanticInteroperability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum RequirementLevel {
    Required,
    Optional,
}

/// Capability prerequisite used to classify an unattempted operation as
/// `unsupported`, rather than as a failed assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CapabilityRequirement {
    pub capability: String,
    pub level: RequirementLevel,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub specification: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ActorRole {
    Harness,
    Catalog,
    Client,
    Engine,
    ObjectStore,
    StateStore,
    ExternalSink,
}

/// One neutral action. `operation` names behavior, not an implementation-specific
/// command; catalog adapters translate it without changing its semantics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScenarioStep {
    pub id: StepId,
    pub actor: ActorRole,
    pub operation: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<StepId>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

/// A portable assertion definition. `Custom` is namespaced and carries its full
/// configuration so a consumer never has to guess what an adapter checked.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum AssertionCheck {
    HttpStatus { allowed: Vec<u16> },
    JsonPointerEquals { pointer: String, expected: Value },
    ArtifactExists { role: String },
    ObjectCountDelta { minimum: u64 },
    NoRequestErrors,
    ExactReplay,
    DataRead { expectation: String },
    Custom { name: String, configuration: Value },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AssertionSpec {
    pub id: AssertionId,
    pub step: StepId,
    pub required: bool,
    pub description: String,
    pub check: AssertionCheck,
}

/// Fixed v1 classification semantics:
///
/// - pass: the scenario ran and every required assertion passed;
/// - fail: the scenario ran and behavior violated a required assertion;
/// - unsupported: a declared prerequisite capability is absent;
/// - not-tested: execution was not attempted for an environmental or procedural
///   reason, without making a capability claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ClassificationPolicy {
    #[serde(rename = "strict-v1")]
    StrictV1,
}

/// A complete, implementation-neutral scenario definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub contract_version: ContractVersion,
    pub kind: ScenarioDocumentKind,
    pub id: ScenarioId,
    /// Monotonically increasing scenario revision, independent of contract v1.
    pub version: u32,
    pub title: String,
    pub description: String,
    pub family: ScenarioFamily,
    pub classification: ClassificationPolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<CapabilityRequirement>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: BTreeMap<String, Value>,
    pub steps: Vec<ScenarioStep>,
    pub assertions: Vec<AssertionSpec>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: Extensions,
}

impl Validate for Scenario {
    fn collect_issues(&self, path: &str, issues: &mut Vec<ValidationIssue>) {
        self.id.collect_issues(&child_path(path, "id"), issues);
        if self.version == 0 {
            issues.push(ValidationIssue::new(
                child_path(path, "version"),
                "must be greater than zero",
            ));
        }
        require_non_empty(&self.title, child_path(path, "title"), issues);
        require_non_empty(&self.description, child_path(path, "description"), issues);
        if self.steps.is_empty() {
            issues.push(ValidationIssue::new(
                child_path(path, "steps"),
                "must contain at least one step",
            ));
        }
        if self.assertions.is_empty() {
            issues.push(ValidationIssue::new(
                child_path(path, "assertions"),
                "must contain at least one assertion",
            ));
        }

        require_unique(
            self.steps.iter().map(|step| step.id.clone()),
            &child_path(path, "steps"),
            issues,
        );
        require_unique(
            self.assertions.iter().map(|assertion| assertion.id.clone()),
            &child_path(path, "assertions"),
            issues,
        );
        require_unique(
            self.capabilities
                .iter()
                .map(|requirement| requirement.capability.as_str()),
            &child_path(path, "capabilities"),
            issues,
        );

        let step_positions: BTreeMap<&StepId, usize> = self
            .steps
            .iter()
            .enumerate()
            .map(|(index, step)| (&step.id, index))
            .collect();

        for (index, step) in self.steps.iter().enumerate() {
            let step_path = indexed_path(&child_path(path, "steps"), index);
            step.id
                .collect_issues(&child_path(&step_path, "id"), issues);
            require_non_empty(&step.operation, child_path(&step_path, "operation"), issues);
            require_non_empty(
                &step.description,
                child_path(&step_path, "description"),
                issues,
            );
            require_unique(
                step.depends_on.iter(),
                &child_path(&step_path, "depends_on"),
                issues,
            );
            for dependency in &step.depends_on {
                match step_positions.get(dependency) {
                    Some(dependency_index) if *dependency_index < index => {}
                    Some(_) => issues.push(ValidationIssue::new(
                        child_path(&step_path, "depends_on"),
                        format!("dependency `{dependency}` must precede this step"),
                    )),
                    None => issues.push(ValidationIssue::new(
                        child_path(&step_path, "depends_on"),
                        format!("references unknown step `{dependency}`"),
                    )),
                }
            }
        }

        let step_ids: BTreeSet<&StepId> = self.steps.iter().map(|step| &step.id).collect();
        for (index, assertion) in self.assertions.iter().enumerate() {
            let assertion_path = indexed_path(&child_path(path, "assertions"), index);
            assertion
                .id
                .collect_issues(&child_path(&assertion_path, "id"), issues);
            if !step_ids.contains(&assertion.step) {
                issues.push(ValidationIssue::new(
                    child_path(&assertion_path, "step"),
                    format!("references unknown step `{}`", assertion.step),
                ));
            }
            require_non_empty(
                &assertion.description,
                child_path(&assertion_path, "description"),
                issues,
            );
            validate_check(
                &assertion.check,
                &child_path(&assertion_path, "check"),
                issues,
            );
        }

        for (index, requirement) in self.capabilities.iter().enumerate() {
            let capability_path = indexed_path(&child_path(path, "capabilities"), index);
            require_non_empty(
                &requirement.capability,
                child_path(&capability_path, "capability"),
                issues,
            );
            require_non_empty(
                &requirement.description,
                child_path(&capability_path, "description"),
                issues,
            );
        }
    }
}

fn validate_check(check: &AssertionCheck, path: &str, issues: &mut Vec<ValidationIssue>) {
    match check {
        AssertionCheck::HttpStatus { allowed } if allowed.is_empty() => {
            issues.push(ValidationIssue::new(
                child_path(path, "allowed"),
                "must contain at least one status code",
            ));
        }
        AssertionCheck::HttpStatus { allowed } => {
            for status in allowed {
                if !(100..=599).contains(status) {
                    issues.push(ValidationIssue::new(
                        child_path(path, "allowed"),
                        format!("contains invalid HTTP status {status}"),
                    ));
                }
            }
        }
        AssertionCheck::JsonPointerEquals { pointer, .. } => {
            if !pointer.is_empty() && !pointer.starts_with('/') {
                issues.push(ValidationIssue::new(
                    child_path(path, "pointer"),
                    "must be empty or start with `/`",
                ));
            }
        }
        AssertionCheck::ArtifactExists { role } => {
            require_non_empty(role, child_path(path, "role"), issues);
        }
        AssertionCheck::DataRead { expectation } => {
            require_non_empty(expectation, child_path(path, "expectation"), issues);
        }
        AssertionCheck::Custom { name, .. } => {
            require_non_empty(name, child_path(path, "name"), issues);
            if !name.contains('/') {
                issues.push(ValidationIssue::new(
                    child_path(path, "name"),
                    "must be namespaced (for example `org.example/check`)",
                ));
            }
        }
        AssertionCheck::ObjectCountDelta { .. }
        | AssertionCheck::NoRequestErrors
        | AssertionCheck::ExactReplay => {}
    }
}
