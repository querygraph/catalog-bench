use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    child_path, indexed_path, reject_secret_like_keys, require_non_empty, require_unique,
    AdapterCapabilityCoverage, CapabilityId, CatalogAdapter, CatalogCapability, Component,
    ComponentId, ComponentKind, ContractVersion, Extensions, ProfileId, RuntimeArtifact, Validate,
    ValidationIssue,
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

/// Whether all artifacts needed to execute the profile have immutable identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ProfileReadiness {
    Runnable,
    Draft {
        unresolved_artifacts: Vec<ComponentId>,
        explanation: String,
    },
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
    pub readiness: ProfileReadiness,
    pub platform: ExecutionPlatform,
    pub components: Vec<Component>,
    pub services: Vec<ServiceBinding>,
    /// Versioned operation vocabulary shared by all catalog adapters.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub catalog_capabilities: Vec<CatalogCapability>,
    /// Exact routing, authentication, shim, and capability coverage per catalog.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub catalog_adapters: Vec<CatalogAdapter>,
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
        validate_readiness(
            &self.readiness,
            &self.components,
            &component_ids,
            &child_path(path, "readiness"),
            issues,
        );

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
            reject_secret_like_keys(
                service.settings.keys().map(String::as_str),
                &child_path(&service_path, "settings"),
                issues,
            );
        }

        validate_catalog_adapters(self, &component_ids, path, issues);
    }
}

fn validate_catalog_adapters(
    profile: &Profile,
    component_ids: &BTreeSet<&ComponentId>,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    let catalog_components: BTreeSet<&ComponentId> = profile
        .components
        .iter()
        .filter(|component| component.kind == ComponentKind::Catalog)
        .map(|component| &component.id)
        .collect();
    let contract_is_present =
        !profile.catalog_capabilities.is_empty() || !profile.catalog_adapters.is_empty();
    if (catalog_components.is_empty()
        || matches!(profile.purpose, ProfilePurpose::HistoricalReproduction))
        && !contract_is_present
    {
        return;
    }

    let capabilities_path = child_path(path, "catalog_capabilities");
    if profile.catalog_capabilities.is_empty() {
        issues.push(ValidationIssue::new(
            &capabilities_path,
            "must define at least one capability when catalog adapters are required",
        ));
    }
    require_unique(
        profile
            .catalog_capabilities
            .iter()
            .map(|capability| &capability.id),
        &capabilities_path,
        issues,
    );
    for (index, capability) in profile.catalog_capabilities.iter().enumerate() {
        capability.collect_issues(&indexed_path(&capabilities_path, index), issues);
    }

    let adapters_path = child_path(path, "catalog_adapters");
    require_unique(
        profile
            .catalog_adapters
            .iter()
            .map(|adapter| &adapter.catalog),
        &adapters_path,
        issues,
    );

    let adapter_catalogs: BTreeSet<&ComponentId> = profile
        .catalog_adapters
        .iter()
        .map(|adapter| &adapter.catalog)
        .collect();
    for missing in catalog_components.difference(&adapter_catalogs) {
        issues.push(ValidationIssue::new(
            &adapters_path,
            format!("catalog component `{missing}` has no adapter"),
        ));
    }
    for extra in adapter_catalogs.difference(&catalog_components) {
        issues.push(ValidationIssue::new(
            &adapters_path,
            format!("adapter references non-catalog component `{extra}`"),
        ));
    }

    let defined_capabilities: BTreeSet<&CapabilityId> = profile
        .catalog_capabilities
        .iter()
        .map(|capability| &capability.id)
        .collect();
    for (index, adapter) in profile.catalog_adapters.iter().enumerate() {
        let adapter_path = indexed_path(&adapters_path, index);
        adapter.collect_issues(&adapter_path, issues);
        validate_component_reference(
            &adapter.catalog,
            component_ids,
            &child_path(&adapter_path, "catalog"),
            issues,
        );
        validate_adapter_service(profile, adapter, &adapter_path, issues);
        validate_adapter_shim(profile, adapter, &adapter_path, issues);
        validate_adapter_capabilities(adapter, &defined_capabilities, &adapter_path, issues);
    }
}

fn validate_adapter_service(
    profile: &Profile,
    adapter: &CatalogAdapter,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    let matching = profile
        .services
        .iter()
        .filter(|service| service.component == adapter.catalog)
        .collect::<Vec<_>>();
    if matching.len() != 1 {
        issues.push(ValidationIssue::new(
            child_path(path, "catalog"),
            format!(
                "catalog adapter requires exactly one service binding, found {}",
                matching.len()
            ),
        ));
        return;
    }

    let service = matching[0];
    if service.role != "iceberg-rest-catalog" {
        issues.push(ValidationIssue::new(
            child_path(path, "catalog"),
            "catalog adapter service role must be `iceberg-rest-catalog`",
        ));
    }
    if service.endpoint.as_deref() != Some(adapter.endpoint.base_url.as_str()) {
        issues.push(ValidationIssue::new(
            child_path(path, "endpoint.base_url"),
            "must exactly match the catalog service endpoint",
        ));
    }
}

fn validate_adapter_shim(
    profile: &Profile,
    adapter: &CatalogAdapter,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    let Some(shim) = adapter.request_handling.shim_component() else {
        return;
    };
    match profile
        .components
        .iter()
        .find(|component| component.id == *shim)
    {
        Some(component) if component.kind == ComponentKind::Connector => {}
        Some(_) => issues.push(ValidationIssue::new(
            child_path(path, "request_handling.component"),
            format!("behavior-changing shim `{shim}` must be a connector component"),
        )),
        None => issues.push(ValidationIssue::new(
            child_path(path, "request_handling.component"),
            format!("references unknown shim component `{shim}`"),
        )),
    }
}

fn validate_adapter_capabilities(
    adapter: &CatalogAdapter,
    defined: &BTreeSet<&CapabilityId>,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    let AdapterCapabilityCoverage::Explicit {
        exercise,
        unsupported,
    } = &adapter.capabilities
    else {
        return;
    };
    let declared: BTreeSet<&CapabilityId> = exercise
        .iter()
        .chain(unsupported.iter().map(|limitation| &limitation.capability))
        .collect();
    let capabilities_path = child_path(path, "capabilities");
    for missing in defined.difference(&declared) {
        issues.push(ValidationIssue::new(
            &capabilities_path,
            format!("does not classify capability `{missing}`"),
        ));
    }
    for unknown in declared.difference(defined) {
        issues.push(ValidationIssue::new(
            &capabilities_path,
            format!("classifies undefined capability `{unknown}`"),
        ));
    }
}

fn validate_readiness(
    readiness: &ProfileReadiness,
    components: &[Component],
    component_ids: &BTreeSet<&ComponentId>,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    let unresolved: BTreeSet<&ComponentId> = components
        .iter()
        .filter(|component| match &component.artifact {
            RuntimeArtifact::SourceBuild { executable } => executable.is_none(),
            RuntimeArtifact::Package { digest, .. } => digest.is_none(),
            RuntimeArtifact::ContainerImage { .. } => false,
        })
        .map(|component| &component.id)
        .collect();

    match readiness {
        ProfileReadiness::Runnable if !unresolved.is_empty() => {
            let names = unresolved
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            issues.push(ValidationIssue::new(
                path,
                format!("runnable profile has unresolved artifacts: {names}"),
            ));
        }
        ProfileReadiness::Runnable => {}
        ProfileReadiness::Draft {
            unresolved_artifacts,
            explanation,
        } => {
            require_non_empty(explanation, child_path(path, "explanation"), issues);
            require_unique(
                unresolved_artifacts.iter(),
                &child_path(path, "unresolved_artifacts"),
                issues,
            );
            let declared: BTreeSet<&ComponentId> = unresolved_artifacts.iter().collect();
            for component in unresolved_artifacts {
                validate_component_reference(
                    component,
                    component_ids,
                    &child_path(path, "unresolved_artifacts"),
                    issues,
                );
            }
            for missing in unresolved.difference(&declared) {
                issues.push(ValidationIssue::new(
                    child_path(path, "unresolved_artifacts"),
                    format!("must declare unresolved component `{missing}`"),
                ));
            }
            for resolved in declared.difference(&unresolved) {
                issues.push(ValidationIssue::new(
                    child_path(path, "unresolved_artifacts"),
                    format!("component `{resolved}` already has an immutable artifact"),
                ));
            }
            if unresolved.is_empty() {
                issues.push(ValidationIssue::new(
                    path,
                    "draft profile must have at least one unresolved artifact",
                ));
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
