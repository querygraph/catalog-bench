use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    child_path, indexed_path, require_non_empty, require_unique, Component, ComponentId,
    ContractVersion, Extensions, ProfileId, Validate, ValidationIssue,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ProfileDocumentKind {
    #[serde(rename = "profile")]
    Profile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ProfilePurpose {
    HistoricalReproduction,
    CurrentCandidate,
    Conformance,
    Performance,
    FaultInjection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ExecutionMode {
    DockerCompose,
    Kubernetes,
    HostProcesses,
    Mixed,
}

/// Shared execution substrate. Actual per-run hardware is captured in each
/// result's environment manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlatform {
    pub operating_system: String,
    pub architecture: String,
    pub mode: ExecutionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_runtime: Option<ComponentId>,
    pub network: String,
    pub shared_object_store: ComponentId,
    pub warehouse_uri: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: Extensions,
}

/// A component's neutral role and endpoint within the profile topology.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ServiceBinding {
    pub component: ComponentId,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_state: Option<ComponentId>,
    /// Sanitized, behaviorally relevant settings only. Secrets are forbidden.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub settings: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: Extensions,
}

/// A version-pinned environment recipe shared by scenarios and result records.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub contract_version: ContractVersion,
    pub kind: ProfileDocumentKind,
    pub id: ProfileId,
    pub title: String,
    pub description: String,
    /// RFC 3339 date or timestamp at which moving dependencies were resolved.
    pub resolved_at: String,
    pub purpose: ProfilePurpose,
    pub platform: ExecutionPlatform,
    pub components: Vec<Component>,
    pub services: Vec<ServiceBinding>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: Extensions,
}

impl Validate for Profile {
    fn collect_issues(&self, path: &str, issues: &mut Vec<ValidationIssue>) {
        self.id.collect_issues(&child_path(path, "id"), issues);
        require_non_empty(&self.title, child_path(path, "title"), issues);
        require_non_empty(&self.description, child_path(path, "description"), issues);
        require_non_empty(&self.resolved_at, child_path(path, "resolved_at"), issues);
        if self.components.is_empty() {
            issues.push(ValidationIssue::new(
                child_path(path, "components"),
                "must contain at least one component",
            ));
        }

        require_unique(
            self.components.iter().map(|component| component.id.clone()),
            &child_path(path, "components"),
            issues,
        );

        let component_ids: BTreeSet<&ComponentId> = self
            .components
            .iter()
            .map(|component| &component.id)
            .collect();
        for (index, component) in self.components.iter().enumerate() {
            component.collect_issues(
                &indexed_path(&child_path(path, "components"), index),
                issues,
            );
        }

        let platform_path = child_path(path, "platform");
        require_non_empty(
            &self.platform.operating_system,
            child_path(&platform_path, "operating_system"),
            issues,
        );
        require_non_empty(
            &self.platform.architecture,
            child_path(&platform_path, "architecture"),
            issues,
        );
        require_non_empty(
            &self.platform.network,
            child_path(&platform_path, "network"),
            issues,
        );
        require_non_empty(
            &self.platform.warehouse_uri,
            child_path(&platform_path, "warehouse_uri"),
            issues,
        );
        validate_component_reference(
            &self.platform.shared_object_store,
            &component_ids,
            &child_path(&platform_path, "shared_object_store"),
            issues,
        );
        if let Some(runtime) = &self.platform.container_runtime {
            validate_component_reference(
                runtime,
                &component_ids,
                &child_path(&platform_path, "container_runtime"),
                issues,
            );
        }

        for (index, service) in self.services.iter().enumerate() {
            let service_path = indexed_path(&child_path(path, "services"), index);
            validate_component_reference(
                &service.component,
                &component_ids,
                &child_path(&service_path, "component"),
                issues,
            );
            require_non_empty(&service.role, child_path(&service_path, "role"), issues);
            if let Some(endpoint) = &service.endpoint {
                require_non_empty(endpoint, child_path(&service_path, "endpoint"), issues);
            }
            if let Some(state) = &service.private_state {
                validate_component_reference(
                    state,
                    &component_ids,
                    &child_path(&service_path, "private_state"),
                    issues,
                );
            }
            for key in service.settings.keys() {
                let normalized = key.to_ascii_lowercase();
                if ["password", "secret", "token", "private_key", "access_key"]
                    .iter()
                    .any(|needle| normalized.contains(needle))
                {
                    issues.push(ValidationIssue::new(
                        child_path(&service_path, "settings"),
                        format!("secret-like setting key `{key}` is forbidden"),
                    ));
                }
            }
        }
    }
}

fn validate_component_reference(
    component: &ComponentId,
    available: &BTreeSet<&ComponentId>,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    component.collect_issues(path, issues);
    if !available.contains(component) {
        issues.push(ValidationIssue::new(
            path,
            format!("references unknown component `{component}`"),
        ));
    }
}
