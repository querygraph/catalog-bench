use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    child_path, indexed_path, require_finite_non_negative, require_non_empty, require_unique,
    validate_artifacts, ArtifactReference, AssertionId, CapabilityId, ComponentId, ContractVersion,
    Digest, EvidenceId, Extensions, ProfileId, ResultId, ScenarioId, Validate, ValidationIssue,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ResultDocumentKind {
    #[serde(rename = "result")]
    Result,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScenarioReference {
    pub id: ScenarioId,
    pub version: u32,
    pub digest: Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProfileReference {
    pub id: ProfileId,
    pub digest: Digest,
}

/// Component identity repeated at the result boundary for readable, standalone
/// evidence. The immutable profile carries its full artifact/source pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutedComponent {
    pub profile_component: ComponentId,
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum RunIdentity {
    Single {
        id: String,
        started_at: String,
        finished_at: String,
        repetition: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        random_seed: Option<u64>,
    },
    Aggregate {
        id: String,
        period: String,
        included_repetitions: Vec<u32>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        excluded_repetitions: Vec<u32>,
        aggregation: String,
    },
}

/// A captured environment value with explicit precision. Historical imports can
/// preserve rounded or missing metadata without fabricating exact values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "precision", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Captured<T> {
    Exact { value: T },
    Approximate { value: T, explanation: String },
    Unknown { explanation: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentManifest {
    pub operating_system: String,
    pub architecture: String,
    pub cpu_model: Captured<String>,
    pub logical_cpus: Captured<u32>,
    pub memory_bytes: Captured<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_limit: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_limit_bytes: Option<u64>,
    pub network: String,
    pub container_runtime: Captured<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub runtime_flags: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum FailureCategory {
    Assertion,
    Transport,
    Catalog,
    Client,
    Harness,
    Environment,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    pub category: FailureCategory,
    pub summary: String,
    pub detail: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<EvidenceId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UnsupportedCapability {
    pub capability: CapabilityId,
    pub explanation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_reference: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NotTestedReason {
    pub explanation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocking_dependency: Option<String>,
}

/// Exhaustive result classification. Details required by non-pass outcomes are
/// represented by the variant itself and cannot drift into unrelated fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ResultOutcome {
    Pass {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
    },
    Fail {
        failure: Failure,
    },
    Unsupported {
        limitation: UnsupportedCapability,
    },
    NotTested {
        reason: NotTestedReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum AssertionOutcome {
    Pass,
    Fail { explanation: String },
    NotEvaluated { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AssertionEvaluation {
    pub assertion: AssertionId,
    /// Copied from the immutable scenario for standalone readability; bundle
    /// validation verifies that it agrees with the scenario definition.
    pub required: bool,
    pub outcome: AssertionOutcome,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<EvidenceId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum EvidenceKind {
    HttpTranscript,
    Log,
    ObjectAudit,
    QueryOutput,
    CatalogState,
    Lineage,
    PolicyReceipt,
    GraphProof,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub id: EvidenceId,
    pub kind: EvidenceKind,
    pub artifact: ArtifactReference,
    /// Public result bundles require `true`; an internal unsanitized capture must
    /// be transformed before it enters this contract.
    pub sanitized: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub redactions: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: Extensions,
}

/// Distribution summary. Quantile keys use `p<number>` (for example `p50`,
/// `p95`, `p99`, or `p99.9`) and values use the surrounding measurement's unit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Distribution {
    pub samples: u64,
    pub minimum: f64,
    pub maximum: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mean: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standard_deviation: Option<f64>,
    pub quantiles: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum MetricValue {
    Scalar { value: f64 },
    Counter { value: u64 },
    Ratio { numerator: u64, denominator: u64 },
    Distribution { distribution: Distribution },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Metric {
    pub name: String,
    pub unit: String,
    pub value: MetricValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MeasuredPhase {
    pub name: String,
    pub elapsed_ms: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operations: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<Distribution>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<Metric>,
}

/// One independently auditable scenario execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResultRecord {
    pub contract_version: ContractVersion,
    pub kind: ResultDocumentKind,
    pub id: ResultId,
    pub scenario: ScenarioReference,
    pub profile: ProfileReference,
    pub catalog: ExecutedComponent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client: Option<ExecutedComponent>,
    /// Every shim or adapter that can affect behavior must be disclosed here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub adapters: Vec<ComponentId>,
    pub run: RunIdentity,
    pub outcome: ResultOutcome,
    pub environment: EnvironmentManifest,
    pub assertions: Vec<AssertionEvaluation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub measurements: Vec<MeasuredPhase>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<Evidence>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactReference>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: Extensions,
}

impl Validate for ResultRecord {
    fn collect_issues(&self, path: &str, issues: &mut Vec<ValidationIssue>) {
        self.id.collect_issues(&child_path(path, "id"), issues);
        validate_scenario_reference(&self.scenario, &child_path(path, "scenario"), issues);
        validate_profile_reference(&self.profile, &child_path(path, "profile"), issues);
        validate_executed_component(&self.catalog, &child_path(path, "catalog"), issues);
        if let Some(client) = &self.client {
            validate_executed_component(client, &child_path(path, "client"), issues);
        }
        validate_run(&self.run, &child_path(path, "run"), issues);
        validate_environment(&self.environment, &child_path(path, "environment"), issues);

        require_unique(
            self.assertions
                .iter()
                .map(|evaluation| evaluation.assertion.clone()),
            &child_path(path, "assertions"),
            issues,
        );
        require_unique(
            self.evidence.iter().map(|evidence| evidence.id.clone()),
            &child_path(path, "evidence"),
            issues,
        );

        let evidence_ids: BTreeSet<&EvidenceId> =
            self.evidence.iter().map(|evidence| &evidence.id).collect();
        for (index, evidence) in self.evidence.iter().enumerate() {
            let evidence_path = indexed_path(&child_path(path, "evidence"), index);
            evidence
                .id
                .collect_issues(&child_path(&evidence_path, "id"), issues);
            evidence
                .artifact
                .collect_issues(&child_path(&evidence_path, "artifact"), issues);
            if !evidence.sanitized {
                issues.push(ValidationIssue::new(
                    child_path(&evidence_path, "sanitized"),
                    "must be true for a publishable result",
                ));
            }
        }

        for (index, evaluation) in self.assertions.iter().enumerate() {
            let assertion_path = indexed_path(&child_path(path, "assertions"), index);
            evaluation
                .assertion
                .collect_issues(&child_path(&assertion_path, "assertion"), issues);
            validate_evidence_references(
                &evaluation.evidence,
                &evidence_ids,
                &child_path(&assertion_path, "evidence"),
                issues,
            );
            match &evaluation.outcome {
                AssertionOutcome::Pass => {}
                AssertionOutcome::Fail { explanation } => require_non_empty(
                    explanation,
                    child_path(&assertion_path, "outcome.explanation"),
                    issues,
                ),
                AssertionOutcome::NotEvaluated { reason } => require_non_empty(
                    reason,
                    child_path(&assertion_path, "outcome.reason"),
                    issues,
                ),
            }
        }

        validate_result_outcome(
            &self.outcome,
            &self.assertions,
            &evidence_ids,
            &child_path(path, "outcome"),
            issues,
        );

        for (index, phase) in self.measurements.iter().enumerate() {
            validate_phase(
                phase,
                &indexed_path(&child_path(path, "measurements"), index),
                issues,
            );
        }
        validate_artifacts(&self.artifacts, &child_path(path, "artifacts"), issues);
    }
}

fn validate_scenario_reference(
    reference: &ScenarioReference,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    reference.id.collect_issues(&child_path(path, "id"), issues);
    if reference.version == 0 {
        issues.push(ValidationIssue::new(
            child_path(path, "version"),
            "must be greater than zero",
        ));
    }
    reference
        .digest
        .collect_issues(&child_path(path, "digest"), issues);
}

fn validate_profile_reference(
    reference: &ProfileReference,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    reference.id.collect_issues(&child_path(path, "id"), issues);
    reference
        .digest
        .collect_issues(&child_path(path, "digest"), issues);
}

fn validate_executed_component(
    component: &ExecutedComponent,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    component
        .profile_component
        .collect_issues(&child_path(path, "profile_component"), issues);
    require_non_empty(&component.name, child_path(path, "name"), issues);
    require_non_empty(&component.version, child_path(path, "version"), issues);
}

fn validate_run(run: &RunIdentity, path: &str, issues: &mut Vec<ValidationIssue>) {
    match run {
        RunIdentity::Single {
            id,
            started_at,
            finished_at,
            repetition,
            ..
        } => {
            require_non_empty(id, child_path(path, "id"), issues);
            require_non_empty(started_at, child_path(path, "started_at"), issues);
            require_non_empty(finished_at, child_path(path, "finished_at"), issues);
            if *repetition == 0 {
                issues.push(ValidationIssue::new(
                    child_path(path, "repetition"),
                    "must be greater than zero",
                ));
            }
        }
        RunIdentity::Aggregate {
            id,
            period,
            included_repetitions,
            excluded_repetitions,
            aggregation,
        } => {
            require_non_empty(id, child_path(path, "id"), issues);
            require_non_empty(period, child_path(path, "period"), issues);
            require_non_empty(aggregation, child_path(path, "aggregation"), issues);
            if included_repetitions.is_empty() {
                issues.push(ValidationIssue::new(
                    child_path(path, "included_repetitions"),
                    "must contain at least one repetition",
                ));
            }
            require_unique(
                included_repetitions.iter(),
                &child_path(path, "included_repetitions"),
                issues,
            );
            require_unique(
                excluded_repetitions.iter(),
                &child_path(path, "excluded_repetitions"),
                issues,
            );
            for repetition in included_repetitions
                .iter()
                .chain(excluded_repetitions.iter())
            {
                if *repetition == 0 {
                    issues.push(ValidationIssue::new(
                        path,
                        "repetition numbers must be greater than zero",
                    ));
                }
            }
            for repetition in included_repetitions {
                if excluded_repetitions.contains(repetition) {
                    issues.push(ValidationIssue::new(
                        path,
                        format!("repetition {repetition} cannot be both included and excluded"),
                    ));
                }
            }
        }
    }
}

fn validate_environment(
    environment: &EnvironmentManifest,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    require_non_empty(
        &environment.operating_system,
        child_path(path, "operating_system"),
        issues,
    );
    require_non_empty(
        &environment.architecture,
        child_path(path, "architecture"),
        issues,
    );
    validate_captured_text(
        &environment.cpu_model,
        child_path(path, "cpu_model"),
        issues,
    );
    validate_captured_positive_u32(
        &environment.logical_cpus,
        &child_path(path, "logical_cpus"),
        issues,
    );
    validate_captured_positive_u64(
        &environment.memory_bytes,
        &child_path(path, "memory_bytes"),
        issues,
    );
    require_non_empty(&environment.network, child_path(path, "network"), issues);
    validate_captured_text(
        &environment.container_runtime,
        child_path(path, "container_runtime"),
        issues,
    );
    if let Some(cpu_limit) = environment.cpu_limit {
        require_finite_non_negative(cpu_limit, child_path(path, "cpu_limit"), issues);
        if cpu_limit == 0.0 {
            issues.push(ValidationIssue::new(
                child_path(path, "cpu_limit"),
                "must be greater than zero when present",
            ));
        }
    }
}

fn validate_captured_text(
    captured: &Captured<String>,
    path: impl Into<String>,
    issues: &mut Vec<ValidationIssue>,
) {
    let path = path.into();
    match captured {
        Captured::Exact { value } => require_non_empty(value, child_path(&path, "value"), issues),
        Captured::Approximate { value, explanation } => {
            require_non_empty(value, child_path(&path, "value"), issues);
            require_non_empty(explanation, child_path(&path, "explanation"), issues);
        }
        Captured::Unknown { explanation } => {
            require_non_empty(explanation, child_path(&path, "explanation"), issues)
        }
    }
}

fn validate_captured_positive_u32(
    captured: &Captured<u32>,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    match captured {
        Captured::Exact { value } if *value == 0 => issues.push(ValidationIssue::new(
            child_path(path, "value"),
            "must be greater than zero",
        )),
        Captured::Approximate { value, .. } if *value == 0 => issues.push(ValidationIssue::new(
            child_path(path, "value"),
            "must be greater than zero",
        )),
        Captured::Approximate { explanation, .. } => {
            require_non_empty(explanation, child_path(path, "explanation"), issues);
        }
        Captured::Unknown { explanation } => {
            require_non_empty(explanation, child_path(path, "explanation"), issues)
        }
        Captured::Exact { .. } => {}
    }
}

fn validate_captured_positive_u64(
    captured: &Captured<u64>,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    match captured {
        Captured::Exact { value } if *value == 0 => issues.push(ValidationIssue::new(
            child_path(path, "value"),
            "must be greater than zero",
        )),
        Captured::Approximate { value, .. } if *value == 0 => issues.push(ValidationIssue::new(
            child_path(path, "value"),
            "must be greater than zero",
        )),
        Captured::Approximate { explanation, .. } => {
            require_non_empty(explanation, child_path(path, "explanation"), issues);
        }
        Captured::Unknown { explanation } => {
            require_non_empty(explanation, child_path(path, "explanation"), issues)
        }
        Captured::Exact { .. } => {}
    }
}

fn validate_result_outcome(
    outcome: &ResultOutcome,
    assertions: &[AssertionEvaluation],
    evidence_ids: &BTreeSet<&EvidenceId>,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    match outcome {
        ResultOutcome::Pass { summary } => {
            if let Some(summary) = summary {
                require_non_empty(summary, child_path(path, "summary"), issues);
            }
            if assertions.is_empty() {
                issues.push(ValidationIssue::new(
                    child_path(path, "status"),
                    "pass requires at least one assertion evaluation",
                ));
            }
            for evaluation in assertions.iter().filter(|evaluation| evaluation.required) {
                if !matches!(evaluation.outcome, AssertionOutcome::Pass) {
                    issues.push(ValidationIssue::new(
                        child_path(path, "status"),
                        format!(
                            "pass requires required assertion `{}` to pass",
                            evaluation.assertion
                        ),
                    ));
                }
            }
        }
        ResultOutcome::Fail { failure } => {
            require_non_empty(
                &failure.summary,
                child_path(path, "failure.summary"),
                issues,
            );
            require_non_empty(&failure.detail, child_path(path, "failure.detail"), issues);
            validate_evidence_references(
                &failure.evidence,
                evidence_ids,
                &child_path(path, "failure.evidence"),
                issues,
            );
        }
        ResultOutcome::Unsupported { limitation } => {
            limitation
                .capability
                .collect_issues(&child_path(path, "limitation.capability"), issues);
            require_non_empty(
                &limitation.explanation,
                child_path(path, "limitation.explanation"),
                issues,
            );
        }
        ResultOutcome::NotTested { reason } => {
            require_non_empty(
                &reason.explanation,
                child_path(path, "reason.explanation"),
                issues,
            );
        }
    }
}

fn validate_evidence_references(
    references: &[EvidenceId],
    available: &BTreeSet<&EvidenceId>,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    require_unique(references, path, issues);
    for reference in references {
        if !available.contains(reference) {
            issues.push(ValidationIssue::new(
                path,
                format!("references unknown evidence `{reference}`"),
            ));
        }
    }
}

fn validate_phase(phase: &MeasuredPhase, path: &str, issues: &mut Vec<ValidationIssue>) {
    require_non_empty(&phase.name, child_path(path, "name"), issues);
    require_finite_non_negative(phase.elapsed_ms, child_path(path, "elapsed_ms"), issues);
    if let Some(distribution) = &phase.latency_ms {
        validate_distribution(distribution, &child_path(path, "latency_ms"), issues);
    }
    require_unique(
        phase.metrics.iter().map(|metric| metric.name.as_str()),
        &child_path(path, "metrics"),
        issues,
    );
    for (index, metric) in phase.metrics.iter().enumerate() {
        let metric_path = indexed_path(&child_path(path, "metrics"), index);
        require_non_empty(&metric.name, child_path(&metric_path, "name"), issues);
        require_non_empty(&metric.unit, child_path(&metric_path, "unit"), issues);
        match &metric.value {
            MetricValue::Scalar { value } => {
                require_finite_non_negative(*value, child_path(&metric_path, "value.value"), issues)
            }
            MetricValue::Counter { .. } => {}
            MetricValue::Ratio { denominator, .. } if *denominator == 0 => {
                issues.push(ValidationIssue::new(
                    child_path(&metric_path, "value.denominator"),
                    "must be greater than zero",
                ));
            }
            MetricValue::Ratio { .. } => {}
            MetricValue::Distribution { distribution } => validate_distribution(
                distribution,
                &child_path(&metric_path, "value.distribution"),
                issues,
            ),
        }
    }
}

fn validate_distribution(
    distribution: &Distribution,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    if distribution.samples == 0 {
        issues.push(ValidationIssue::new(
            child_path(path, "samples"),
            "must be greater than zero",
        ));
    }
    require_finite_non_negative(distribution.minimum, child_path(path, "minimum"), issues);
    require_finite_non_negative(distribution.maximum, child_path(path, "maximum"), issues);
    if distribution.minimum > distribution.maximum {
        issues.push(ValidationIssue::new(
            path,
            "minimum must not exceed maximum",
        ));
    }
    if let Some(mean) = distribution.mean {
        require_finite_non_negative(mean, child_path(path, "mean"), issues);
        if mean < distribution.minimum || mean > distribution.maximum {
            issues.push(ValidationIssue::new(
                child_path(path, "mean"),
                "must be between minimum and maximum",
            ));
        }
    }
    if let Some(deviation) = distribution.standard_deviation {
        require_finite_non_negative(deviation, child_path(path, "standard_deviation"), issues);
    }

    let mut parsed_quantiles = Vec::new();
    for (name, value) in &distribution.quantiles {
        let Some(percentile) = parse_percentile(name) else {
            issues.push(ValidationIssue::new(
                child_path(path, "quantiles"),
                format!("invalid quantile `{name}`; expected p0 through p100"),
            ));
            continue;
        };
        require_finite_non_negative(*value, format!("{}.quantiles.{name}", path), issues);
        if *value < distribution.minimum || *value > distribution.maximum {
            issues.push(ValidationIssue::new(
                format!("{}.quantiles.{name}", path),
                "must be between minimum and maximum",
            ));
        }
        parsed_quantiles.push((percentile, *value, name));
    }
    parsed_quantiles.sort_by(|left, right| left.0.total_cmp(&right.0));
    for pair in parsed_quantiles.windows(2) {
        if pair[0].1 > pair[1].1 {
            issues.push(ValidationIssue::new(
                child_path(path, "quantiles"),
                format!("{} must not exceed {}", pair[0].2, pair[1].2),
            ));
        }
    }
}

fn parse_percentile(name: &str) -> Option<f64> {
    let value = name.strip_prefix('p')?.parse::<f64>().ok()?;
    (value.is_finite() && (0.0..=100.0).contains(&value)).then_some(value)
}
