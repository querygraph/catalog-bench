use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::path::{Component as PathComponent, Path};

use catalog_bench_common::contract::{
    parse_contract, AdapterRequestHandling, ArtifactReference, CatalogAdapter,
    CatalogAuthentication, CatalogProtocol, CatalogRoutePrefix, Component, ComponentId,
    ComponentKind, ContractDocument, DigestAlgorithm, ExecutionMode, Profile, ProfileReadiness,
    RuntimeArtifact, Scenario, ScenarioFamily, ServiceBinding,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

pub const ENGINE_SCENARIO_ID: &str = "engine.iceberg.write-read-evolution";
pub const ENGINE_SCENARIO_VERSION: u32 = 1;
pub const ENGINE_TRANSCRIPT_FORMAT: &str = "catalog-bench/engine-interoperability-transcript/v1";
pub const ENGINE_RUNNER_COMPONENT_ID: &str = "catalog-bench-engine";
pub const ENGINE_RUNNER_ROLE: &str = "engine-runner";
pub const ENGINE_RUNNER_LOCATION: &str = "/usr/local/bin/catalog-bench-engine";
pub const SPARK_PLAN_FORMAT: &str = "catalog-bench/spark-engine-plan/v1";
pub const SPARK_CATALOG_NAME: &str = "bench";
pub const SPARK_COMPONENT_NAME: &str = "Apache Spark";
pub const SPARK_COMPONENT_VERSION: &str = "4.1.3";
pub const SPARK_SCALA_VERSION: &str = "2.13.17";
pub const SPARK_JAVA_VERSION: &str = "21.0.11";
pub const ICEBERG_CONNECTOR_NAME: &str = "Apache Iceberg Java engine runtimes";
pub const ICEBERG_CONNECTOR_VERSION: &str = "1.11.0";
pub const SPARK_SUBMIT_LOCATION: &str = "/opt/spark/bin/spark-submit";
pub const S3_ACCESS_KEY_ENV: &str = "CATALOG_BENCH_S3_ACCESS_KEY_ID";
pub const S3_SECRET_KEY_ENV: &str = "CATALOG_BENCH_S3_SECRET_ACCESS_KEY";
pub const ENGINE_OAUTH_CLIENT_ID_ENV: &str = "CATALOG_BENCH_ENGINE_CLIENT_ID";
pub const ENGINE_OAUTH_CLIENT_SECRET_ENV: &str = "CATALOG_BENCH_ENGINE_CLIENT_SECRET";

const FIXTURE_TABLE_NAME: &str = "events";
const CANONICAL_SCENARIO: &[u8] =
    include_bytes!("../../../scenarios/v1/engine.iceberg.write-read-evolution.json");

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
pub enum ForbiddenPolicy {
    Forbidden,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConnectorPolicy {
    StockProfileComponent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SyntaxRenderingPolicy {
    EngineSpecificButCatalogNeutral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UnsupportedPolicy {
    ClassifyBeforeMutationWithoutASubstituteRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnginePolicy {
    pub catalog_specific_branches: ForbiddenPolicy,
    pub catalog_specific_shims: ForbiddenPolicy,
    pub connector: ConnectorPolicy,
    pub syntax_rendering: SyntaxRenderingPolicy,
    pub unsupported: UnsupportedPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FileFormat {
    Parquet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IcebergPrimitiveType {
    Long,
    String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub field_type: IcebergPrimitiveType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergSchema {
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    #[serde(rename = "type")]
    pub schema_type: StructType,
    pub fields: Vec<IcebergField>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StructType {
    Struct,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePolicy {
    pub file_format: FileFormat,
    pub format_version: u8,
    pub properties: BTreeMap<String, String>,
    pub schema: IcebergSchema,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaEvolutionPolicy {
    pub field: EvolutionField,
    pub preserve_existing_field_ids: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvolutionField {
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub field_type: IcebergPrimitiveType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum IntegerGenerator {
    Affine { multiplier: u64, offset: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum CategoryGenerator {
    ModuloLabel { modulus: u64, prefix: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum NoteGenerator {
    IdLabel { prefix: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowGenerator {
    pub amount_cents: IntegerGenerator,
    pub category: CategoryGenerator,
    pub note: NoteGenerator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchPolicy {
    pub id_start: u64,
    pub rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchPolicies {
    pub initial: BatchPolicy,
    pub evolved: BatchPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CanonicalEncoding {
    CompactRfc8259JsonArrayPerRowUtf8Lf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalRead {
    pub bytes: u64,
    pub columns: Vec<String>,
    pub rows: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalReads {
    pub encoding: CanonicalEncoding,
    pub initial: CanonicalRead,
    pub order_by: Vec<String>,
    pub after_evolution: CanonicalRead,
    pub trailing_lf: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectAuditScope {
    ReturnedTableRootInProfileSharedObjectStore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectAuditPolicy {
    pub minimum_metadata_objects: u64,
    pub minimum_parquet_objects: u64,
    pub scope: ObjectAuditScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineScenarioParameters {
    pub catalog_protocol: CatalogProtocol,
    pub engine_policy: EnginePolicy,
    pub fixture_prefix: String,
    pub table: TablePolicy,
    pub schema_evolution: SchemaEvolutionPolicy,
    pub row_generator: RowGenerator,
    pub batches: BatchPolicies,
    pub canonical_reads: CanonicalReads,
    pub object_audit: ObjectAuditPolicy,
    pub transcript_format: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    pub namespace: String,
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_location: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum SparkAuthentication {
    Anonymous,
    #[serde(rename = "oauth2-client-credentials")]
    OAuth2ClientCredentials {
        oauth2_server_uri: String,
        scope: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SparkCatalogPlan {
    pub name: String,
    pub uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    pub authentication: SparkAuthentication,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SparkFileIoPlan {
    pub implementation: String,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub path_style_access: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SparkExecutionSettings {
    pub master: String,
    pub shuffle_partitions: u32,
    pub default_parallelism: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SparkExecutionPlan {
    pub format: String,
    pub execution: SparkExecutionSettings,
    pub catalog: SparkCatalogPlan,
    pub file_io: SparkFileIoPlan,
    pub fixture: Fixture,
    pub scenario: EngineScenarioParameters,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentIdentity {
    pub id: ComponentId,
    pub name: String,
    pub version: String,
    pub source_revision: Option<String>,
}

impl From<&Component> for ComponentIdentity {
    fn from(component: &Component) -> Self {
        Self {
            id: component.id.clone(),
            name: component.name.clone(),
            version: component.version.clone(),
            source_revision: component
                .source
                .as_ref()
                .map(|source| source.revision.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeArtifactExpectation {
    pub location: String,
    pub media_type: String,
    pub sha256: String,
    pub bytes: u64,
    pub components: Vec<ComponentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePlatformExpectation {
    pub operating_system: String,
    pub architecture: String,
}

#[derive(Clone, PartialEq, Eq)]
pub enum CatalogCredentialSource {
    Anonymous,
    OAuth2ClientCredentials {
        client_id_env: String,
        client_secret_env: String,
    },
}

impl std::fmt::Debug for CatalogCredentialSource {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Anonymous => formatter.write_str("Anonymous"),
            Self::OAuth2ClientCredentials { .. } => formatter
                .debug_struct("OAuth2ClientCredentials")
                .field("environment", &"<private>")
                .finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStorePlan {
    pub component: ComponentId,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub allow_http: bool,
    pub path_style_access: bool,
    pub access_key_env: String,
    pub secret_key_env: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InteroperabilityPlan {
    runner: Option<ComponentIdentity>,
    catalog: ComponentIdentity,
    engine: ComponentIdentity,
    connector: ComponentIdentity,
    credential_source: CatalogCredentialSource,
    object_store: ObjectStorePlan,
    runtime_platform: RuntimePlatformExpectation,
    runtime_artifacts: Vec<RuntimeArtifactExpectation>,
    spark: SparkExecutionPlan,
}

impl InteroperabilityPlan {
    pub fn from_contracts(
        profile: &Profile,
        scenario: &Scenario,
        catalog: &ComponentId,
        fixture_id: &str,
    ) -> Result<Self, PolicyError> {
        validate_canonical_scenario(scenario)?;
        let parameters = decode_parameters(scenario)?;
        validate_parameters(&parameters)?;
        validate_profile_platform(profile)?;

        let adapter = adapter(profile, catalog)?;
        if !matches!(
            adapter.request_handling,
            AdapterRequestHandling::ProtocolNative
        ) {
            return Err(PolicyError::new(format!(
                "catalog `{catalog}` uses a behavior-changing shim"
            )));
        }
        if adapter.protocol != parameters.catalog_protocol {
            return Err(PolicyError::new(format!(
                "catalog `{catalog}` protocol does not match the scenario"
            )));
        }
        let catalog_component = component(profile, catalog, ComponentKind::Catalog)?;
        let runner_component = optional_engine_runner(profile)?;
        let engine_component = role_component(profile, "stock-engine", ComponentKind::Engine)?;
        let connector_component =
            role_component(profile, "engine-connector", ComponentKind::Connector)?;
        validate_supported_runtime(engine_component, connector_component)?;

        let object_store = object_store_plan(profile)?;
        let fixture = fixture(
            &parameters.fixture_prefix,
            catalog,
            fixture_id,
            adapter,
            &object_store,
        )?;
        let (authentication, credential_source) = authentication(adapter)?;
        let spark = SparkExecutionPlan {
            format: SPARK_PLAN_FORMAT.to_owned(),
            execution: SparkExecutionSettings {
                master: "local[2]".to_owned(),
                shuffle_partitions: 1,
                default_parallelism: 1,
            },
            catalog: SparkCatalogPlan {
                name: SPARK_CATALOG_NAME.to_owned(),
                uri: adapter.endpoint.base_url.clone(),
                warehouse: config_warehouse(adapter)?,
                prefix: configured_prefix(adapter),
                authentication,
            },
            file_io: SparkFileIoPlan {
                implementation: "org.apache.iceberg.aws.s3.S3FileIO".to_owned(),
                endpoint: object_store.endpoint.clone(),
                bucket: object_store.bucket.clone(),
                region: object_store.region.clone(),
                path_style_access: object_store.path_style_access,
            },
            fixture,
            scenario: parameters,
        };
        let runtime_artifacts =
            runtime_artifacts(runner_component, engine_component, connector_component)?;
        validate_execution_artifact(&runtime_artifacts, &engine_component.id)?;
        if let Some(runner) = runner_component {
            validate_runner_artifact(&runtime_artifacts, &runner.id, &engine_component.id)?;
        }

        Ok(Self {
            runner: runner_component.map(ComponentIdentity::from),
            catalog: catalog_component.into(),
            engine: engine_component.into(),
            connector: connector_component.into(),
            credential_source,
            object_store,
            runtime_platform: RuntimePlatformExpectation {
                operating_system: profile.platform.operating_system.clone(),
                architecture: profile.platform.architecture.clone(),
            },
            runtime_artifacts,
            spark,
        })
    }

    #[must_use]
    pub fn runner(&self) -> Option<&ComponentIdentity> {
        self.runner.as_ref()
    }

    #[must_use]
    pub fn catalog(&self) -> &ComponentIdentity {
        &self.catalog
    }

    #[must_use]
    pub fn engine(&self) -> &ComponentIdentity {
        &self.engine
    }

    #[must_use]
    pub fn connector(&self) -> &ComponentIdentity {
        &self.connector
    }

    #[must_use]
    pub fn credential_source(&self) -> &CatalogCredentialSource {
        &self.credential_source
    }

    #[must_use]
    pub fn object_store(&self) -> &ObjectStorePlan {
        &self.object_store
    }

    #[must_use]
    pub fn runtime_platform(&self) -> &RuntimePlatformExpectation {
        &self.runtime_platform
    }

    #[must_use]
    pub fn runtime_artifacts(&self) -> &[RuntimeArtifactExpectation] {
        &self.runtime_artifacts
    }

    #[must_use]
    pub fn spark(&self) -> &SparkExecutionPlan {
        &self.spark
    }
}

fn validate_execution_artifact(
    artifacts: &[RuntimeArtifactExpectation],
    engine: &ComponentId,
) -> Result<(), PolicyError> {
    let matches = artifacts
        .iter()
        .filter(|artifact| artifact.location == SPARK_SUBMIT_LOCATION)
        .collect::<Vec<_>>();
    let [artifact] = matches.as_slice() else {
        return Err(PolicyError::new(format!(
            "engine runtime must contain exactly one `{SPARK_SUBMIT_LOCATION}` artifact"
        )));
    };
    if artifact.media_type != "application/x-shellscript"
        || artifact.bytes == 0
        || artifact.components.as_slice() != std::slice::from_ref(engine)
    {
        return Err(PolicyError::new(
            "Spark submission artifact must be a nonempty engine-owned shell script",
        ));
    }
    Ok(())
}

fn validate_canonical_scenario(scenario: &Scenario) -> Result<(), PolicyError> {
    if scenario.id.as_str() != ENGINE_SCENARIO_ID {
        return Err(PolicyError::new(format!(
            "engine runner requires scenario `{ENGINE_SCENARIO_ID}`, found `{}`",
            scenario.id
        )));
    }
    if scenario.version != ENGINE_SCENARIO_VERSION {
        return Err(PolicyError::new(format!(
            "engine runner supports scenario version {ENGINE_SCENARIO_VERSION}, found {}",
            scenario.version
        )));
    }
    if scenario.family != ScenarioFamily::ClientInteroperability {
        return Err(PolicyError::new(
            "engine scenario must use the client-interoperability family",
        ));
    }
    let ContractDocument::Scenario(canonical) =
        parse_contract(CANONICAL_SCENARIO).map_err(|error| {
            PolicyError::new(format!("canonical engine scenario is invalid: {error}"))
        })?
    else {
        return Err(PolicyError::new(
            "canonical engine scenario did not decode as a scenario",
        ));
    };
    if *scenario != canonical {
        return Err(PolicyError::new(
            "engine scenario differs from the canonical checked-in contract",
        ));
    }
    Ok(())
}

fn decode_parameters(scenario: &Scenario) -> Result<EngineScenarioParameters, PolicyError> {
    serde_json::from_value(Value::Object(
        scenario.parameters.clone().into_iter().collect(),
    ))
    .map_err(|error| PolicyError::new(format!("invalid engine scenario parameters: {error}")))
}

fn validate_parameters(parameters: &EngineScenarioParameters) -> Result<(), PolicyError> {
    if parameters.transcript_format != ENGINE_TRANSCRIPT_FORMAT {
        return Err(PolicyError::new("unexpected engine transcript format"));
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
    if parameters.table.format_version != 2
        || parameters.table.schema.schema_id != 0
        || parameters.table.schema.fields.is_empty()
    {
        return Err(PolicyError::new(
            "engine table policy must define a nonempty format-v2 schema zero",
        ));
    }
    let field_ids = parameters
        .table
        .schema
        .fields
        .iter()
        .map(|field| field.id)
        .collect::<BTreeSet<_>>();
    let field_names = parameters
        .table
        .schema
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<BTreeSet<_>>();
    if field_ids.len() != parameters.table.schema.fields.len()
        || field_names.len() != parameters.table.schema.fields.len()
        || field_ids.iter().any(|id| *id <= 0)
    {
        return Err(PolicyError::new(
            "engine table fields require unique positive IDs and unique names",
        ));
    }
    if parameters.batches.initial.rows == 0
        || parameters.batches.evolved.rows == 0
        || parameters
            .batches
            .initial
            .id_start
            .checked_add(parameters.batches.initial.rows)
            != Some(parameters.batches.evolved.id_start)
    {
        return Err(PolicyError::new(
            "engine row batches must be positive, contiguous, and non-overlapping",
        ));
    }
    if !parameters.canonical_reads.trailing_lf
        || parameters.canonical_reads.order_by != ["id"]
        || !valid_sha256(&parameters.canonical_reads.initial.sha256)
        || !valid_sha256(&parameters.canonical_reads.after_evolution.sha256)
    {
        return Err(PolicyError::new(
            "canonical reads require ID ordering, a final LF, and SHA-256 identities",
        ));
    }
    if parameters.object_audit.minimum_metadata_objects == 0
        || parameters.object_audit.minimum_parquet_objects == 0
    {
        return Err(PolicyError::new(
            "object-audit minimums must both be positive",
        ));
    }
    Ok(())
}

fn validate_profile_platform(profile: &Profile) -> Result<(), PolicyError> {
    if !matches!(profile.readiness, ProfileReadiness::Runnable) {
        return Err(PolicyError::new("engine profile must be runnable"));
    }
    if profile.platform.mode != ExecutionMode::DockerCompose
        || !profile
            .platform
            .operating_system
            .eq_ignore_ascii_case("linux")
        || !matches!(
            profile.platform.architecture.to_ascii_lowercase().as_str(),
            "aarch64" | "arm64"
        )
    {
        return Err(PolicyError::new(
            "engine profile must execute Linux ARM64 through Docker Compose",
        ));
    }
    Ok(())
}

fn adapter<'a>(
    profile: &'a Profile,
    catalog: &ComponentId,
) -> Result<&'a CatalogAdapter, PolicyError> {
    profile
        .catalog_adapters
        .iter()
        .find(|adapter| adapter.catalog == *catalog)
        .ok_or_else(|| PolicyError::new(format!("profile has no adapter for `{catalog}`")))
}

fn component<'a>(
    profile: &'a Profile,
    id: &ComponentId,
    kind: ComponentKind,
) -> Result<&'a Component, PolicyError> {
    let component = profile
        .components
        .iter()
        .find(|component| component.id == *id)
        .ok_or_else(|| PolicyError::new(format!("profile has no component `{id}`")))?;
    if component.kind != kind {
        return Err(PolicyError::new(format!(
            "profile component `{id}` has kind {:?}, expected {kind:?}",
            component.kind
        )));
    }
    Ok(component)
}

fn role_component<'a>(
    profile: &'a Profile,
    role: &str,
    kind: ComponentKind,
) -> Result<&'a Component, PolicyError> {
    let services = profile
        .services
        .iter()
        .filter(|service| service.role == role)
        .collect::<Vec<_>>();
    let [service] = services.as_slice() else {
        return Err(PolicyError::new(format!(
            "profile must contain exactly one `{role}` service"
        )));
    };
    component(profile, &service.component, kind)
}

fn optional_engine_runner(profile: &Profile) -> Result<Option<&Component>, PolicyError> {
    let services = profile
        .services
        .iter()
        .filter(|service| service.role == ENGINE_RUNNER_ROLE)
        .collect::<Vec<_>>();
    let service = match services.as_slice() {
        [] => return Ok(None),
        [service] => *service,
        _ => {
            return Err(PolicyError::new(format!(
                "profile must contain at most one `{ENGINE_RUNNER_ROLE}` service"
            )));
        }
    };
    if service.component.as_str() != ENGINE_RUNNER_COMPONENT_ID {
        return Err(PolicyError::new(format!(
            "`{ENGINE_RUNNER_ROLE}` must select `{ENGINE_RUNNER_COMPONENT_ID}`"
        )));
    }
    let runner = component(profile, &service.component, ComponentKind::BenchmarkHarness)?;
    let revision = runner
        .source
        .as_ref()
        .map(|source| source.revision.as_str())
        .ok_or_else(|| PolicyError::new("engine runner must declare its source revision"))?;
    if runner.version != revision || !valid_git_revision(revision) {
        return Err(PolicyError::new(
            "engine runner version and source must be one lowercase 40-character Git revision",
        ));
    }
    Ok(Some(runner))
}

fn validate_supported_runtime(
    engine: &Component,
    connector: &Component,
) -> Result<(), PolicyError> {
    if engine.name != SPARK_COMPONENT_NAME || engine.version != SPARK_COMPONENT_VERSION {
        return Err(PolicyError::new(format!(
            "Spark renderer supports {SPARK_COMPONENT_NAME} {SPARK_COMPONENT_VERSION}, found {} {}",
            engine.name, engine.version
        )));
    }
    if connector.name != ICEBERG_CONNECTOR_NAME || connector.version != ICEBERG_CONNECTOR_VERSION {
        return Err(PolicyError::new(format!(
            "Spark renderer supports {ICEBERG_CONNECTOR_NAME} {ICEBERG_CONNECTOR_VERSION}, found {} {}",
            connector.name, connector.version
        )));
    }
    Ok(())
}

fn object_store_plan(profile: &Profile) -> Result<ObjectStorePlan, PolicyError> {
    let component = component(
        profile,
        &profile.platform.shared_object_store,
        ComponentKind::ObjectStore,
    )?;
    let services = profile
        .services
        .iter()
        .filter(|service| {
            service.component == component.id && service.role == "shared-object-store"
        })
        .collect::<Vec<_>>();
    let [service] = services.as_slice() else {
        return Err(PolicyError::new(
            "profile must contain exactly one shared-object-store service for its platform component",
        ));
    };
    let endpoint = service
        .endpoint
        .as_deref()
        .ok_or_else(|| PolicyError::new("shared object store has no endpoint"))?;
    let parsed = Url::parse(endpoint)
        .map_err(|error| PolicyError::new(format!("invalid object-store endpoint: {error}")))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(PolicyError::new(
            "object-store endpoint must be a credential-free HTTP(S) URL",
        ));
    }
    let bucket = setting_text(service, "bucket")?;
    let region = setting_text(service, "region")?;
    let path_style_access = setting_bool(service, "path_style_access")?;
    let warehouse = Url::parse(&profile.platform.warehouse_uri)
        .map_err(|error| PolicyError::new(format!("invalid warehouse URI: {error}")))?;
    if warehouse.scheme() != "s3" || warehouse.host_str() != Some(bucket) {
        return Err(PolicyError::new(
            "profile warehouse URI must use the shared object-store bucket",
        ));
    }
    Ok(ObjectStorePlan {
        component: component.id.clone(),
        endpoint: endpoint.to_owned(),
        bucket: bucket.to_owned(),
        region: region.to_owned(),
        allow_http: parsed.scheme() == "http",
        path_style_access,
        access_key_env: S3_ACCESS_KEY_ENV.to_owned(),
        secret_key_env: S3_SECRET_KEY_ENV.to_owned(),
    })
}

fn setting_text<'a>(service: &'a ServiceBinding, key: &str) -> Result<&'a str, PolicyError> {
    service
        .settings
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            PolicyError::new(format!("service setting `{key}` must be a nonempty string"))
        })
}

fn setting_bool(service: &ServiceBinding, key: &str) -> Result<bool, PolicyError> {
    service
        .settings
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| PolicyError::new(format!("service setting `{key}` must be a boolean")))
}

fn fixture(
    prefix: &str,
    catalog: &ComponentId,
    fixture_id: &str,
    adapter: &CatalogAdapter,
    object_store: &ObjectStorePlan,
) -> Result<Fixture, PolicyError> {
    validate_fixture_id(fixture_id)?;
    let catalog_slug = catalog
        .as_str()
        .bytes()
        .map(|byte| match byte {
            b'a'..=b'z' | b'0'..=b'9' => Ok(byte as char),
            b'-' => Ok('_'),
            _ => Err(PolicyError::new(
                "catalog identifiers used in fixtures require lowercase ASCII letters, digits, or hyphens",
            )),
        })
        .collect::<Result<String, _>>()?;
    let namespace = format!("{prefix}_{catalog_slug}_{fixture_id}");
    let requested_location = adapter
        .endpoint
        .create_table_location
        .as_deref()
        .map(|root| child_table_location(root, &namespace, FIXTURE_TABLE_NAME, object_store))
        .transpose()?;
    Ok(Fixture {
        namespace,
        table: FIXTURE_TABLE_NAME.to_owned(),
        requested_location,
    })
}

fn validate_fixture_id(fixture_id: &str) -> Result<(), PolicyError> {
    if fixture_id.is_empty() || fixture_id.len() > 24 {
        return Err(PolicyError::new(
            "fixture ID must contain 1 to 24 characters",
        ));
    }
    if !fixture_id
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(PolicyError::new(
            "fixture ID must contain lowercase ASCII letters, digits, or underscores",
        ));
    }
    Ok(())
}

fn child_table_location(
    root: &str,
    namespace: &str,
    table: &str,
    object_store: &ObjectStorePlan,
) -> Result<String, PolicyError> {
    let mut location = Url::parse(root)
        .map_err(|error| PolicyError::new(format!("invalid create-table location: {error}")))?;
    if location.scheme() != "s3" || location.host_str() != Some(object_store.bucket.as_str()) {
        return Err(PolicyError::new(
            "create-table location must use the shared object-store bucket",
        ));
    }
    {
        let mut segments = location.path_segments_mut().map_err(|()| {
            PolicyError::new("create-table location cannot contain child path segments")
        })?;
        segments.pop_if_empty();
        segments.push(namespace);
        segments.push(table);
    }
    Ok(location.to_string())
}

fn authentication(
    adapter: &CatalogAdapter,
) -> Result<(SparkAuthentication, CatalogCredentialSource), PolicyError> {
    match &adapter.authentication {
        CatalogAuthentication::Anonymous => Ok((
            SparkAuthentication::Anonymous,
            CatalogCredentialSource::Anonymous,
        )),
        CatalogAuthentication::OAuth2ClientCredentials {
            token_path,
            scope,
            client_id_env,
            client_secret_env,
        } => {
            let token_url = endpoint_url(&adapter.endpoint.base_url, token_path)?;
            Ok((
                SparkAuthentication::OAuth2ClientCredentials {
                    oauth2_server_uri: token_url,
                    scope: scope.clone(),
                },
                CatalogCredentialSource::OAuth2ClientCredentials {
                    client_id_env: client_id_env.clone(),
                    client_secret_env: client_secret_env.clone(),
                },
            ))
        }
    }
}

fn endpoint_url(base_url: &str, path: &str) -> Result<String, PolicyError> {
    let value = format!("{}{path}", base_url.trim_end_matches('/'));
    let url = Url::parse(&value)
        .map_err(|error| PolicyError::new(format!("invalid catalog endpoint: {error}")))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(PolicyError::new(
            "catalog endpoint must be a credential-free HTTP(S) URL",
        ));
    }
    Ok(url.to_string())
}

fn config_warehouse(adapter: &CatalogAdapter) -> Result<Option<String>, PolicyError> {
    if adapter
        .endpoint
        .config
        .query
        .keys()
        .any(|key| key != "warehouse")
    {
        return Err(PolicyError::new(
            "Spark REST binding supports only the standard `warehouse` config query",
        ));
    }
    Ok(adapter.endpoint.config.query.get("warehouse").cloned())
}

fn configured_prefix(adapter: &CatalogAdapter) -> Option<String> {
    match &adapter.endpoint.route_prefix {
        CatalogRoutePrefix::Static { value } => Some(value.clone()),
        CatalogRoutePrefix::Unprefixed | CatalogRoutePrefix::Negotiated { .. } => None,
    }
}

fn runtime_artifacts(
    runner: Option<&Component>,
    engine: &Component,
    connector: &Component,
) -> Result<Vec<RuntimeArtifactExpectation>, PolicyError> {
    let engine_artifacts = embedded_artifacts(engine)?;
    let connector_artifacts = embedded_artifacts(connector)?;
    if engine_artifacts.is_empty() || connector_artifacts.is_empty() {
        return Err(PolicyError::new(
            "engine and connector images must declare embedded runtime artifacts",
        ));
    }

    let mut expectations = engine_artifacts
        .iter()
        .map(|artifact| expectation(artifact, vec![engine.id.clone()]))
        .collect::<Result<Vec<_>, _>>()?;
    let mut matched_engine_artifacts = BTreeSet::new();
    for connector_artifact in connector_artifacts {
        let connector_expectation = expectation(connector_artifact, vec![connector.id.clone()])?;
        let matches = expectations
            .iter()
            .enumerate()
            .filter_map(|(index, expectation)| {
                same_artifact(expectation, &connector_expectation).then_some(index)
            })
            .collect::<Vec<_>>();
        let [matched_index] = matches.as_slice() else {
            return Err(PolicyError::new(format!(
                "connector artifact `{}` must have exactly one byte-identical copy in the engine image",
                connector_artifact.location
            )));
        };
        if !matched_engine_artifacts.insert(*matched_index) {
            return Err(PolicyError::new(format!(
                "connector artifact `{}` duplicates another connector artifact identity",
                connector_artifact.location
            )));
        }
        let matched = &mut expectations[*matched_index];
        matched.components.push(connector.id.clone());
        matched.components.sort();
        matched.components.dedup();
    }
    if let Some(runner) = runner {
        let runner_artifacts = embedded_artifacts(runner)?;
        let [runner_artifact] = runner_artifacts else {
            return Err(PolicyError::new(
                "engine runner image must declare exactly one embedded executable",
            ));
        };
        let runner_expectation = expectation(runner_artifact, vec![runner.id.clone()])?;
        let matches = expectations
            .iter()
            .enumerate()
            .filter_map(|(index, expectation)| {
                (expectation.location == runner_expectation.location
                    && same_artifact(expectation, &runner_expectation))
                .then_some(index)
            })
            .collect::<Vec<_>>();
        let [matched_index] = matches.as_slice() else {
            return Err(PolicyError::new(
                "engine runner executable must have exactly one byte-identical copy at the same path in the engine image",
            ));
        };
        let matched = &mut expectations[*matched_index];
        matched.components.push(runner.id.clone());
        matched.components.sort();
        matched.components.dedup();
    }
    expectations.sort_by(|left, right| left.location.cmp(&right.location));
    Ok(expectations)
}

fn validate_runner_artifact(
    artifacts: &[RuntimeArtifactExpectation],
    runner: &ComponentId,
    engine: &ComponentId,
) -> Result<(), PolicyError> {
    let matches = artifacts
        .iter()
        .filter(|artifact| artifact.location == ENGINE_RUNNER_LOCATION)
        .collect::<Vec<_>>();
    let [artifact] = matches.as_slice() else {
        return Err(PolicyError::new(format!(
            "engine runtime must contain exactly one `{ENGINE_RUNNER_LOCATION}` artifact"
        )));
    };
    let mut expected_components = vec![runner.clone(), engine.clone()];
    expected_components.sort();
    if artifact.media_type != "application/vnd.elf"
        || artifact.bytes == 0
        || artifact.components != expected_components
    {
        return Err(PolicyError::new(
            "engine runner artifact must be one nonempty runner-and-engine-owned ELF",
        ));
    }
    Ok(())
}

fn embedded_artifacts(component: &Component) -> Result<&[ArtifactReference], PolicyError> {
    match &component.artifact {
        RuntimeArtifact::ContainerImage {
            embedded_artifacts, ..
        } => Ok(embedded_artifacts),
        RuntimeArtifact::SourceBuild { .. } | RuntimeArtifact::Package { .. } => {
            Err(PolicyError::new(format!(
                "component `{}` must use a container image",
                component.id
            )))
        }
    }
}

fn expectation(
    artifact: &ArtifactReference,
    components: Vec<ComponentId>,
) -> Result<RuntimeArtifactExpectation, PolicyError> {
    let location = artifact.location.strip_prefix("image:").ok_or_else(|| {
        PolicyError::new(format!(
            "runtime artifact `{}` must use an image path",
            artifact.location
        ))
    })?;
    if !location.starts_with('/') {
        return Err(PolicyError::new(
            "runtime artifact image path must be absolute",
        ));
    }
    let mut path_components = Path::new(location).components();
    if !matches!(path_components.next(), Some(PathComponent::RootDir))
        || !path_components.all(|component| matches!(component, PathComponent::Normal(_)))
    {
        return Err(PolicyError::new(
            "runtime artifact image path must be absolute and traversal-free",
        ));
    }
    if artifact.digest.algorithm != DigestAlgorithm::Sha256 || !valid_sha256(&artifact.digest.value)
    {
        return Err(PolicyError::new(
            "runtime artifacts require lowercase SHA-256 identities",
        ));
    }
    let bytes = artifact
        .bytes
        .ok_or_else(|| PolicyError::new("runtime artifact byte count is required"))?;
    Ok(RuntimeArtifactExpectation {
        location: location.to_owned(),
        media_type: artifact.media_type.clone(),
        sha256: artifact.digest.value.clone(),
        bytes,
        components,
    })
}

fn same_artifact(left: &RuntimeArtifactExpectation, right: &RuntimeArtifactExpectation) -> bool {
    left.media_type == right.media_type && left.sha256 == right.sha256 && left.bytes == right.bytes
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_git_revision(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
