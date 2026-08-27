use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

use catalog_bench_common::contract::{
    parse_contract, AdapterRequestHandling, CatalogProtocol, ComponentId, ComponentKind,
    ContractDocument, ExecutionMode, Profile, RequirementLevel, Scenario, ScenarioFamily,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

pub const CONTENTION_SCENARIO_ID: &str = "iceberg-rest.commit.same-table-contention";
pub const CONTENTION_SCENARIO_VERSION: u32 = 2;
pub const CONTENTION_TRANSCRIPT_FORMAT: &str = "catalog-bench/contention-transcript/v1";
pub const RUNNER_COMPONENT_ID: &str = "catalog-bench-commit";
pub const CONTENTION_METADATA_DELETE_AFTER_COMMIT: bool = false;
pub const CONTENTION_METADATA_PREVIOUS_VERSIONS_MAX: u32 = 100_000;

const CANONICAL_SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/iceberg-rest.commit.same-table-contention.v2.json");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyError(String);

impl PolicyError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl Display for PolicyError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for PolicyError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CatalogOrder {
    RotateLeft,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AggregatePolicy {
    MedianWithMinMax,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CommitRequirement {
    AssertTableUuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdempotencyPolicy {
    Omitted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoundPolicy {
    pub conditioning_rounds: u32,
    pub measured_rounds: u32,
    pub catalog_order: CatalogOrder,
    pub aggregate: AggregatePolicy,
    pub require_every_round_to_pass: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadPolicy {
    pub warmup_commits: u64,
    pub sequential_commits: u64,
    pub concurrent_writers: u32,
    pub concurrent_duration_ms: u64,
    pub commit_property: String,
    pub requirement: CommitRequirement,
    pub idempotency_key: IdempotencyPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataRetentionPolicy {
    pub delete_after_commit: bool,
    pub previous_versions_max: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectStorePolicy {
    pub component: ComponentId,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub allow_http: bool,
    pub path_style_access: bool,
    pub access_key_env: String,
    pub secret_key_env: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitPolicy {
    pub request_timeout_ms: u64,
    pub maximum_response_bytes: u64,
    pub maximum_fixture_id_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentionParameters {
    pub fixture_prefix: String,
    pub transcript_format: String,
    pub round_policy: RoundPolicy,
    pub metadata_retention: MetadataRetentionPolicy,
    pub workload: WorkloadPolicy,
    pub object_store: ObjectStorePolicy,
    pub limits: LimitPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogRun {
    pub catalog: ComponentId,
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RoundKind {
    Conditioning,
    Measured,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoundPlan {
    pub repetition: u32,
    pub kind: RoundKind,
    pub catalogs: Vec<CatalogRun>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentionPlan {
    parameters: ContentionParameters,
    rounds: Vec<RoundPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentionFixture {
    pub id: String,
    pub namespace: String,
    pub table: String,
}

impl ContentionPlan {
    pub fn from_contracts(profile: &Profile, scenario: &Scenario) -> Result<Self, PolicyError> {
        validate_scenario_shape(scenario)?;
        let parameters = decode_parameters(scenario)?;
        validate_parameters(&parameters)?;
        validate_canonical_scenario(scenario)?;
        let catalogs = validate_profile(profile, scenario, &parameters)?;
        let rounds = schedule_rounds(&catalogs, &parameters.round_policy)?;
        Ok(Self { parameters, rounds })
    }

    #[must_use]
    pub fn parameters(&self) -> &ContentionParameters {
        &self.parameters
    }

    #[must_use]
    pub fn rounds(&self) -> &[RoundPlan] {
        &self.rounds
    }

    pub fn fixture(
        &self,
        catalog: &ComponentId,
        fixture_id: &str,
        repetition: u32,
    ) -> Result<ContentionFixture, PolicyError> {
        validate_fixture_id(fixture_id, self.parameters.limits.maximum_fixture_id_bytes)?;
        if repetition == 0
            || !self.rounds.iter().any(|round| {
                round.repetition == repetition
                    && round.catalogs.iter().any(|entry| entry.catalog == *catalog)
            })
        {
            return Err(PolicyError::new(format!(
                "catalog `{catalog}` is not scheduled in repetition {repetition}"
            )));
        }
        let catalog_stem = catalog.as_str().replace('-', "_");
        if !catalog_stem
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(PolicyError::new(format!(
                "catalog identifier `{catalog}` is unsafe for an isolated fixture"
            )));
        }
        Ok(ContentionFixture {
            id: fixture_id.to_owned(),
            namespace: format!(
                "{}_{}_{}_r{repetition:02}",
                self.parameters.fixture_prefix, catalog_stem, fixture_id
            ),
            table: "same_table_contention".to_owned(),
        })
    }
}

fn validate_scenario_shape(scenario: &Scenario) -> Result<(), PolicyError> {
    if scenario.id.as_str() != CONTENTION_SCENARIO_ID {
        return Err(PolicyError::new(format!(
            "contention runner requires scenario `{CONTENTION_SCENARIO_ID}`, found `{}`",
            scenario.id
        )));
    }
    if scenario.version != CONTENTION_SCENARIO_VERSION {
        return Err(PolicyError::new(format!(
            "contention runner supports scenario version {CONTENTION_SCENARIO_VERSION}, found {}",
            scenario.version
        )));
    }
    if scenario.family != ScenarioFamily::Concurrency {
        return Err(PolicyError::new(
            "contention scenario family must be `concurrency`",
        ));
    }
    if scenario
        .capabilities
        .iter()
        .any(|requirement| requirement.level != RequirementLevel::Required)
    {
        return Err(PolicyError::new(
            "every contention capability must be required",
        ));
    }
    if scenario
        .assertions
        .iter()
        .any(|assertion| !assertion.required)
    {
        return Err(PolicyError::new(
            "every contention assertion must be required",
        ));
    }

    Ok(())
}

fn validate_canonical_scenario(scenario: &Scenario) -> Result<(), PolicyError> {
    let ContractDocument::Scenario(canonical) =
        parse_contract(CANONICAL_SCENARIO).map_err(|error| {
            PolicyError::new(format!("compiled contention scenario is invalid: {error}"))
        })?
    else {
        return Err(PolicyError::new(
            "compiled contention scenario is not a scenario document",
        ));
    };
    if *scenario != canonical {
        return Err(PolicyError::new(
            "contention scenario drifted from the implemented canonical v2 policy",
        ));
    }
    Ok(())
}

fn decode_parameters(scenario: &Scenario) -> Result<ContentionParameters, PolicyError> {
    let object = scenario
        .parameters
        .clone()
        .into_iter()
        .collect::<serde_json::Map<_, _>>();
    serde_json::from_value(Value::Object(object)).map_err(|error| {
        PolicyError::new(format!("invalid contention scenario parameters: {error}"))
    })
}

fn validate_parameters(parameters: &ContentionParameters) -> Result<(), PolicyError> {
    if parameters.transcript_format != CONTENTION_TRANSCRIPT_FORMAT {
        return Err(PolicyError::new(format!(
            "unsupported transcript format `{}`",
            parameters.transcript_format
        )));
    }
    if parameters.fixture_prefix.is_empty()
        || !parameters
            .fixture_prefix
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(PolicyError::new(
            "fixture prefix must contain lowercase ASCII letters, digits, or underscores",
        ));
    }
    let rounds = &parameters.round_policy;
    if rounds.conditioning_rounds == 0 || rounds.measured_rounds == 0 {
        return Err(PolicyError::new(
            "round policy requires conditioning and measured rounds",
        ));
    }
    rounds
        .conditioning_rounds
        .checked_add(rounds.measured_rounds)
        .ok_or_else(|| PolicyError::new("round count overflows u32"))?;
    if !rounds.require_every_round_to_pass {
        return Err(PolicyError::new(
            "strict contention ranking requires every round to pass",
        ));
    }

    let retention = &parameters.metadata_retention;
    if retention.delete_after_commit != CONTENTION_METADATA_DELETE_AFTER_COMMIT
        || retention.previous_versions_max != CONTENTION_METADATA_PREVIOUS_VERSIONS_MAX
    {
        return Err(PolicyError::new(format!(
            "contention metadata retention must set delete-after-commit={} and previous-versions-max={}",
            CONTENTION_METADATA_DELETE_AFTER_COMMIT,
            CONTENTION_METADATA_PREVIOUS_VERSIONS_MAX
        )));
    }

    let workload = &parameters.workload;
    if workload.warmup_commits == 0
        || workload.sequential_commits == 0
        || workload.concurrent_writers == 0
        || workload.concurrent_duration_ms == 0
    {
        return Err(PolicyError::new(
            "warmup, sequential, writer, and concurrent duration values must be positive",
        ));
    }
    if workload.commit_property.trim().is_empty() {
        return Err(PolicyError::new("commit property must not be empty"));
    }

    let object_store = &parameters.object_store;
    let endpoint = Url::parse(&object_store.endpoint)
        .map_err(|error| PolicyError::new(format!("invalid object-store endpoint: {error}")))?;
    if endpoint.host_str().is_none()
        || endpoint.username() != ""
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || !matches!(endpoint.scheme(), "http" | "https")
    {
        return Err(PolicyError::new(
            "object-store endpoint must be a credential-free absolute HTTP(S) URL",
        ));
    }
    if (endpoint.scheme() == "http") != object_store.allow_http {
        return Err(PolicyError::new(
            "object-store allow_http must match the endpoint scheme",
        ));
    }
    if object_store.bucket.trim().is_empty() || object_store.region.trim().is_empty() {
        return Err(PolicyError::new(
            "object-store bucket and region must not be empty",
        ));
    }
    if !object_store.path_style_access {
        return Err(PolicyError::new(
            "the shared MinIO workload requires path-style access",
        ));
    }
    for variable in [&object_store.access_key_env, &object_store.secret_key_env] {
        if !valid_environment_variable(variable) {
            return Err(PolicyError::new(format!(
                "invalid object-store credential environment variable `{variable}`"
            )));
        }
    }
    if object_store.access_key_env == object_store.secret_key_env {
        return Err(PolicyError::new(
            "object-store access-key and secret-key environment variables must differ",
        ));
    }

    let limits = &parameters.limits;
    if limits.request_timeout_ms == 0
        || limits.maximum_response_bytes == 0
        || limits.maximum_fixture_id_bytes == 0
    {
        return Err(PolicyError::new("contention limits must be positive"));
    }
    usize::try_from(limits.maximum_response_bytes)
        .map_err(|_| PolicyError::new("maximum response size does not fit this runner"))?;
    Ok(())
}

fn validate_profile(
    profile: &Profile,
    scenario: &Scenario,
    parameters: &ContentionParameters,
) -> Result<Vec<CatalogRun>, PolicyError> {
    if profile.platform.mode != ExecutionMode::DockerCompose {
        return Err(PolicyError::new(
            "contention profile must execute through Docker Compose",
        ));
    }
    if profile.platform.shared_object_store != parameters.object_store.component {
        return Err(PolicyError::new(
            "scenario object store does not match the profile shared object store",
        ));
    }
    if profile.platform.warehouse_uri != format!("s3://{}", parameters.object_store.bucket) {
        return Err(PolicyError::new(
            "scenario object-store bucket does not match the profile warehouse URI",
        ));
    }

    let runner = profile
        .components
        .iter()
        .find(|component| component.id.as_str() == RUNNER_COMPONENT_ID)
        .ok_or_else(|| PolicyError::new("profile omits the contention runner component"))?;
    if runner.kind != ComponentKind::BenchmarkHarness {
        return Err(PolicyError::new(
            "contention runner component is not a benchmark harness",
        ));
    }
    let store = profile
        .components
        .iter()
        .find(|component| component.id == parameters.object_store.component)
        .ok_or_else(|| PolicyError::new("profile omits the shared object-store component"))?;
    if store.kind != ComponentKind::ObjectStore {
        return Err(PolicyError::new(
            "profile shared object-store component has the wrong kind",
        ));
    }
    let service = profile
        .services
        .iter()
        .find(|service| service.component == parameters.object_store.component)
        .ok_or_else(|| PolicyError::new("profile omits the shared object-store service"))?;
    if service.endpoint.as_deref() != Some(parameters.object_store.endpoint.as_str())
        || service.settings.get("bucket").and_then(Value::as_str)
            != Some(parameters.object_store.bucket.as_str())
        || service.settings.get("region").and_then(Value::as_str)
            != Some(parameters.object_store.region.as_str())
        || service
            .settings
            .get("path_style_access")
            .and_then(Value::as_bool)
            != Some(parameters.object_store.path_style_access)
    {
        return Err(PolicyError::new(
            "profile object-store service does not match the contention policy",
        ));
    }

    let vocabulary = profile
        .catalog_capabilities
        .iter()
        .map(|capability| &capability.id)
        .collect::<BTreeSet<_>>();
    let components = profile
        .components
        .iter()
        .map(|component| (&component.id, component))
        .collect::<BTreeMap<_, _>>();
    if profile.catalog_adapters.is_empty() {
        return Err(PolicyError::new("profile has no catalog adapters"));
    }
    let mut catalogs = Vec::with_capacity(profile.catalog_adapters.len());
    for adapter in &profile.catalog_adapters {
        if adapter.protocol != CatalogProtocol::IcebergRestV1 {
            return Err(PolicyError::new(format!(
                "catalog `{}` does not use Iceberg REST v1",
                adapter.catalog
            )));
        }
        if !matches!(
            adapter.request_handling,
            AdapterRequestHandling::ProtocolNative
        ) {
            return Err(PolicyError::new(format!(
                "catalog `{}` uses a behavior-changing shim",
                adapter.catalog
            )));
        }
        for requirement in &scenario.capabilities {
            if !vocabulary.contains(&requirement.capability) {
                return Err(PolicyError::new(format!(
                    "profile vocabulary omits required capability `{}`",
                    requirement.capability
                )));
            }
            if !adapter.capabilities.exercises(&requirement.capability) {
                return Err(PolicyError::new(format!(
                    "catalog `{}` does not exercise required capability `{}`",
                    adapter.catalog, requirement.capability
                )));
            }
        }
        let component = components.get(&adapter.catalog).ok_or_else(|| {
            PolicyError::new(format!(
                "profile omits component for catalog `{}`",
                adapter.catalog
            ))
        })?;
        if component.kind != ComponentKind::Catalog {
            return Err(PolicyError::new(format!(
                "component `{}` is not classified as a catalog",
                adapter.catalog
            )));
        }
        catalogs.push(CatalogRun {
            catalog: adapter.catalog.clone(),
            name: component.name.clone(),
            version: component.version.clone(),
        });
    }
    if parameters.round_policy.measured_rounds as usize != catalogs.len() {
        return Err(PolicyError::new(format!(
            "balanced rotate-left ranking requires one measured round per catalog ({} catalogs, {} measured rounds)",
            catalogs.len(), parameters.round_policy.measured_rounds
        )));
    }
    Ok(catalogs)
}

fn schedule_rounds(
    catalogs: &[CatalogRun],
    policy: &RoundPolicy,
) -> Result<Vec<RoundPlan>, PolicyError> {
    if catalogs.is_empty() {
        return Err(PolicyError::new("cannot schedule an empty catalog set"));
    }
    let total = policy
        .conditioning_rounds
        .checked_add(policy.measured_rounds)
        .ok_or_else(|| PolicyError::new("round count overflows u32"))?;
    let mut rounds = Vec::with_capacity(total as usize);
    for index in 0..total {
        let offset = index as usize % catalogs.len();
        let rotated = catalogs[offset..]
            .iter()
            .chain(&catalogs[..offset])
            .cloned()
            .collect();
        rounds.push(RoundPlan {
            repetition: index + 1,
            kind: if index < policy.conditioning_rounds {
                RoundKind::Conditioning
            } else {
                RoundKind::Measured
            },
            catalogs: rotated,
        });
    }
    Ok(rounds)
}

fn validate_fixture_id(id: &str, maximum_bytes: usize) -> Result<(), PolicyError> {
    if id.is_empty() || id.len() > maximum_bytes {
        return Err(PolicyError::new(format!(
            "fixture id must contain 1 to {maximum_bytes} bytes"
        )));
    }
    if !id
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(PolicyError::new(
            "fixture id must contain only lowercase ASCII letters, digits, and underscores",
        ));
    }
    Ok(())
}

fn valid_environment_variable(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_uppercase() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}
