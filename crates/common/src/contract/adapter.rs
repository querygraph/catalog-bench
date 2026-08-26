use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

use super::{
    child_path, indexed_path, reject_secret_like_keys, require_non_empty, require_unique,
    CapabilityId, ComponentId, Extensions, Validate, ValidationIssue,
};

/// One capability in the profile's immutable interoperability vocabulary.
///
/// Definitions live once at profile level. Each catalog adapter must exercise
/// the entire vocabulary or explicitly partition it into exercised and
/// unsupported capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CatalogCapability {
    pub id: CapabilityId,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub specification: Option<String>,
}

impl Validate for CatalogCapability {
    fn collect_issues(&self, path: &str, issues: &mut Vec<ValidationIssue>) {
        self.id.collect_issues(&child_path(path, "id"), issues);
        require_non_empty(&self.description, child_path(path, "description"), issues);
        if let Some(specification) = &self.specification {
            require_non_empty(specification, child_path(path, "specification"), issues);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum CatalogProtocol {
    IcebergRestV1,
}

/// The catalog's configuration request relative to its adapter base URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CatalogConfigRequest {
    pub path: String,
    /// Sanitized routing parameters only, such as Lakekeeper's warehouse name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub query: BTreeMap<String, String>,
}

/// How the standard `{prefix}` path segment is resolved for catalog requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CatalogRoutePrefix {
    Unprefixed,
    Static { value: String },
    Negotiated { config_json_pointer: String },
}

/// Exact, behaviorally relevant Iceberg REST routing for one catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CatalogEndpoint {
    /// Docker-network URL through the end of the deployment-specific mount path.
    pub base_url: String,
    pub config: CatalogConfigRequest,
    pub route_prefix: CatalogRoutePrefix,
    /// A standard optional `createTable.location`, never a rewritten request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_table_location: Option<String>,
}

/// Authentication performed by the harness before sending protocol requests.
/// Secret values never belong in a profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CatalogAuthentication {
    Anonymous,
    #[serde(rename = "oauth2-client-credentials")]
    OAuth2ClientCredentials {
        /// Token route relative to the catalog base URL.
        token_path: String,
        scope: String,
        /// Environment variable names only; secret values never enter profiles.
        client_id_env: String,
        client_secret_env: String,
    },
}

/// Whether an adapter preserves the request and response semantics under test.
///
/// A behavior-changing shim remains representable so experimental evidence can
/// disclose it, but it cannot masquerade as a protocol-native adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum AdapterRequestHandling {
    ProtocolNative,
    BehaviorChangingShim {
        component: ComponentId,
        description: String,
    },
}

impl AdapterRequestHandling {
    pub(crate) fn shim_component(&self) -> Option<&ComponentId> {
        match self {
            Self::ProtocolNative => None,
            Self::BehaviorChangingShim { component, .. } => Some(component),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum CapabilityLimitationSource {
    Catalog,
    Adapter,
}

/// A capability deliberately not exercised for one catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UnsupportedAdapterCapability {
    pub capability: CapabilityId,
    pub attributed_to: CapabilityLimitationSource,
    pub explanation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_reference: Option<String>,
}

/// An exhaustive disposition of the profile capability vocabulary.
///
/// `exercise-all` is the DRY representation when every capability should run.
/// `explicit` partitions the vocabulary when one or more capabilities are known
/// to be unsupported. Exercising a capability is not a claim that it will pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum AdapterCapabilityCoverage {
    ExerciseAll,
    Explicit {
        exercise: Vec<CapabilityId>,
        unsupported: Vec<UnsupportedAdapterCapability>,
    },
}

impl AdapterCapabilityCoverage {
    /// Whether the adapter should execute this capability and classify observed
    /// behavior with scenario assertions.
    #[must_use]
    pub fn exercises(&self, capability: &CapabilityId) -> bool {
        match self {
            Self::ExerciseAll => true,
            Self::Explicit { exercise, .. } => exercise.contains(capability),
        }
    }

    /// The pre-execution limitation for a capability, when one is declared.
    #[must_use]
    pub fn limitation(&self, capability: &CapabilityId) -> Option<&UnsupportedAdapterCapability> {
        match self {
            Self::ExerciseAll => None,
            Self::Explicit { unsupported, .. } => unsupported
                .iter()
                .find(|limitation| limitation.capability == *capability),
        }
    }
}

/// One catalog's complete, profile-pinned harness binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CatalogAdapter {
    pub catalog: ComponentId,
    pub protocol: CatalogProtocol,
    pub endpoint: CatalogEndpoint,
    pub authentication: CatalogAuthentication,
    pub request_handling: AdapterRequestHandling,
    pub capabilities: AdapterCapabilityCoverage,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: Extensions,
}

impl Validate for CatalogAdapter {
    fn collect_issues(&self, path: &str, issues: &mut Vec<ValidationIssue>) {
        self.catalog
            .collect_issues(&child_path(path, "catalog"), issues);
        validate_http_base_url(
            &self.endpoint.base_url,
            &child_path(path, "endpoint.base_url"),
            issues,
        );
        validate_relative_path(
            &self.endpoint.config.path,
            &child_path(path, "endpoint.config.path"),
            issues,
        );
        if self.endpoint.config.path != "/v1/config" {
            issues.push(ValidationIssue::new(
                child_path(path, "endpoint.config.path"),
                "Iceberg REST v1 config must use `/v1/config` relative to the base URL",
            ));
        }
        for (key, value) in &self.endpoint.config.query {
            require_non_empty(key, child_path(path, "endpoint.config.query"), issues);
            require_non_empty(value, child_path(path, "endpoint.config.query"), issues);
        }
        reject_secret_like_keys(
            self.endpoint.config.query.keys().map(String::as_str),
            &child_path(path, "endpoint.config.query"),
            issues,
        );
        validate_route_prefix(
            &self.endpoint.route_prefix,
            &child_path(path, "endpoint.route_prefix"),
            issues,
        );
        if let Some(location) = &self.endpoint.create_table_location {
            validate_warehouse_uri(
                location,
                &child_path(path, "endpoint.create_table_location"),
                issues,
            );
        }

        if let CatalogAuthentication::OAuth2ClientCredentials {
            token_path,
            scope,
            client_id_env,
            client_secret_env,
        } = &self.authentication
        {
            validate_relative_path(
                token_path,
                &child_path(path, "authentication.token_path"),
                issues,
            );
            require_non_empty(scope, child_path(path, "authentication.scope"), issues);
            validate_environment_variable(
                client_id_env,
                &child_path(path, "authentication.client_id_env"),
                issues,
            );
            validate_environment_variable(
                client_secret_env,
                &child_path(path, "authentication.client_secret_env"),
                issues,
            );
            if client_id_env == client_secret_env {
                issues.push(ValidationIssue::new(
                    child_path(path, "authentication.client_secret_env"),
                    "must differ from the client-id environment variable",
                ));
            }
        }

        if let AdapterRequestHandling::BehaviorChangingShim {
            component,
            description,
        } = &self.request_handling
        {
            component.collect_issues(&child_path(path, "request_handling.component"), issues);
            require_non_empty(
                description,
                child_path(path, "request_handling.description"),
                issues,
            );
        }

        validate_capability_coverage(
            &self.capabilities,
            &child_path(path, "capabilities"),
            issues,
        );
    }
}

fn validate_capability_coverage(
    coverage: &AdapterCapabilityCoverage,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    let AdapterCapabilityCoverage::Explicit {
        exercise,
        unsupported,
    } = coverage
    else {
        return;
    };
    require_unique(exercise.iter(), &child_path(path, "exercise"), issues);
    require_unique(
        unsupported.iter().map(|limitation| &limitation.capability),
        &child_path(path, "unsupported"),
        issues,
    );

    let exercised: BTreeSet<&CapabilityId> = exercise.iter().collect();
    for (index, limitation) in unsupported.iter().enumerate() {
        let limitation_path = indexed_path(&child_path(path, "unsupported"), index);
        limitation
            .capability
            .collect_issues(&child_path(&limitation_path, "capability"), issues);
        require_non_empty(
            &limitation.explanation,
            child_path(&limitation_path, "explanation"),
            issues,
        );
        if let Some(reference) = &limitation.upstream_reference {
            require_non_empty(
                reference,
                child_path(&limitation_path, "upstream_reference"),
                issues,
            );
        }
        if exercised.contains(&limitation.capability) {
            issues.push(ValidationIssue::new(
                child_path(&limitation_path, "capability"),
                format!(
                    "capability `{}` cannot be both exercised and unsupported",
                    limitation.capability
                ),
            ));
        }
    }
}

fn validate_route_prefix(
    prefix: &CatalogRoutePrefix,
    path: &str,
    issues: &mut Vec<ValidationIssue>,
) {
    match prefix {
        CatalogRoutePrefix::Unprefixed => {}
        CatalogRoutePrefix::Static { value } => {
            require_non_empty(value, child_path(path, "value"), issues);
            if value
                .chars()
                .any(|character| character.is_whitespace() || "/?#".contains(character))
            {
                issues.push(ValidationIssue::new(
                    child_path(path, "value"),
                    "must be one unescaped path segment",
                ));
            }
        }
        CatalogRoutePrefix::Negotiated {
            config_json_pointer,
        } => {
            require_non_empty(
                config_json_pointer,
                child_path(path, "config_json_pointer"),
                issues,
            );
            if !matches!(
                config_json_pointer.as_str(),
                "/defaults/prefix" | "/overrides/prefix"
            ) {
                issues.push(ValidationIssue::new(
                    child_path(path, "config_json_pointer"),
                    "must resolve the standard `prefix` property from config defaults or overrides",
                ));
            }
        }
    }
}

fn validate_relative_path(value: &str, path: &str, issues: &mut Vec<ValidationIssue>) {
    require_non_empty(value, path, issues);
    if !value.starts_with('/')
        || value.contains(['?', '#'])
        || value.chars().any(char::is_whitespace)
    {
        issues.push(ValidationIssue::new(
            path,
            "must be an absolute, query-free path relative to the adapter base URL",
        ));
    }
}

fn validate_http_base_url(value: &str, path: &str, issues: &mut Vec<ValidationIssue>) {
    match Url::parse(value) {
        Ok(url)
            if matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && !value.ends_with('/') => {}
        Ok(_) => issues.push(ValidationIssue::new(
            path,
            "must be an absolute HTTP(S) base URL without credentials, query, fragment, or trailing slash",
        )),
        Err(error) => issues.push(ValidationIssue::new(
            path,
            format!("must be a valid absolute URL: {error}"),
        )),
    }
}

fn validate_warehouse_uri(value: &str, path: &str, issues: &mut Vec<ValidationIssue>) {
    match Url::parse(value) {
        Ok(url)
            if url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none() => {}
        Ok(_) => issues.push(ValidationIssue::new(
            path,
            "must be an absolute warehouse URI without credentials, query, or fragment",
        )),
        Err(error) => issues.push(ValidationIssue::new(
            path,
            format!("must be a valid absolute warehouse URI: {error}"),
        )),
    }
}

fn validate_environment_variable(value: &str, path: &str, issues: &mut Vec<ValidationIssue>) {
    require_non_empty(value, path, issues);
    let mut characters = value.chars();
    let valid_start = characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic());
    if !valid_start
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        issues.push(ValidationIssue::new(
            path,
            "must be a portable environment-variable name",
        ));
    }
}
