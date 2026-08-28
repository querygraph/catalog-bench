use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use url::Url;

use crate::sql::{
    literal, render_insert, render_read, render_rows, valid_identifier, SqlGenerationError,
};
use crate::{
    CanonicalRead, ConnectorPolicy, EngineCatalogAuthentication, EvolutionField, ForbiddenPolicy,
    IcebergField, IcebergPrimitiveType, SyntaxRenderingPolicy, TrinoExecutionPlan,
    UnsupportedPolicy, ENGINE_TRANSCRIPT_FORMAT, TRINO_CATALOG_NAME, TRINO_PLAN_FORMAT,
};
use crate::{S3_ACCESS_KEY_ENV, S3_SECRET_KEY_ENV};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrinoRenderError(&'static str);

impl Display for TrinoRenderError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TrinoRenderError {}

impl From<SqlGenerationError> for TrinoRenderError {
    fn from(error: SqlGenerationError) -> Self {
        Self(error.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrinoCatalogSetup {
    pub name: String,
    pub properties: BTreeMap<String, String>,
    pub authentication: EngineCatalogAuthentication,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TrinoOperationPurpose {
    CreateNamespace,
    CreateTable,
    InitialAppend,
    InitialRead,
    AddColumn,
    EvolvedAppend,
    EvolvedRead,
    SnapshotRead,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrinoFixtureTarget {
    pub namespace: String,
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_location: Option<String>,
    pub bucket: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrinoObservationPolicy {
    pub format_version: u8,
    pub initial_schema: Vec<IcebergField>,
    pub evolved_field: EvolutionField,
    pub properties: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
pub enum TrinoOperation {
    CreateNamespace {
        sql: String,
    },
    CreateTable {
        sql: String,
    },
    InitialAppend {
        sql: String,
    },
    InitialRead {
        sql: String,
        expected: CanonicalRead,
    },
    AddColumn {
        sql: String,
    },
    EvolvedAppend {
        sql: String,
    },
    EvolvedRead {
        sql: String,
        expected: CanonicalRead,
    },
    SnapshotRead {
        sql: String,
    },
}

impl TrinoOperation {
    #[must_use]
    pub fn purpose(&self) -> TrinoOperationPurpose {
        match self {
            Self::CreateNamespace { .. } => TrinoOperationPurpose::CreateNamespace,
            Self::CreateTable { .. } => TrinoOperationPurpose::CreateTable,
            Self::InitialAppend { .. } => TrinoOperationPurpose::InitialAppend,
            Self::InitialRead { .. } => TrinoOperationPurpose::InitialRead,
            Self::AddColumn { .. } => TrinoOperationPurpose::AddColumn,
            Self::EvolvedAppend { .. } => TrinoOperationPurpose::EvolvedAppend,
            Self::EvolvedRead { .. } => TrinoOperationPurpose::EvolvedRead,
            Self::SnapshotRead { .. } => TrinoOperationPurpose::SnapshotRead,
        }
    }

    #[must_use]
    pub fn sql(&self) -> &str {
        match self {
            Self::CreateNamespace { sql }
            | Self::CreateTable { sql }
            | Self::InitialAppend { sql }
            | Self::InitialRead { sql, .. }
            | Self::AddColumn { sql }
            | Self::EvolvedAppend { sql }
            | Self::EvolvedRead { sql, .. }
            | Self::SnapshotRead { sql } => sql,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrinoRenderedProgram {
    pub task_concurrency: u32,
    pub catalog: TrinoCatalogSetup,
    pub fixture: TrinoFixtureTarget,
    pub observation: TrinoObservationPolicy,
    pub operations: Vec<TrinoOperation>,
}

impl TrinoRenderedProgram {
    pub fn render(plan: &TrinoExecutionPlan) -> Result<Self, TrinoRenderError> {
        validate_plan(plan)?;
        let namespace = identifier(&plan.fixture.namespace)?;
        let table = identifier(&plan.fixture.table)?;
        let catalog = identifier(&plan.catalog.name)?;
        let qualified_namespace = format!("{catalog}.{namespace}");
        let qualified_table = format!("{qualified_namespace}.{table}");
        let scenario = &plan.scenario;
        let schema = scenario
            .table
            .schema
            .fields
            .iter()
            .map(render_field)
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        let mut table_properties = vec![
            "format = 'PARQUET'".to_owned(),
            format!("format_version = {}", scenario.table.format_version),
        ];
        if let Some(location) = &plan.fixture.requested_location {
            table_properties.push(format!("location = {}", literal(location)));
        }
        if !scenario.table.properties.is_empty() {
            table_properties.push(format!(
                "extra_properties = MAP(ARRAY[{}], ARRAY[{}])",
                scenario
                    .table
                    .properties
                    .keys()
                    .map(|key| literal(key))
                    .collect::<Vec<_>>()
                    .join(", "),
                scenario
                    .table
                    .properties
                    .values()
                    .map(|value| literal(value))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        let create_table = format!(
            "CREATE TABLE {qualified_table} ({schema}) WITH ({})",
            table_properties.join(", ")
        );
        let initial_columns = scenario
            .table
            .schema
            .fields
            .iter()
            .map(|field| identifier(&field.name))
            .collect::<Result<Vec<_>, _>>()?;
        let evolved_column = identifier(&scenario.schema_evolution.field.name)?;
        let evolved_columns = initial_columns
            .iter()
            .cloned()
            .chain([evolved_column.clone()])
            .collect::<Vec<_>>();
        let initial_values = render_rows(scenario, false)?;
        let evolved_values = render_rows(scenario, true)?;
        let initial_read = render_read(
            &qualified_table,
            &scenario.canonical_reads.initial.columns,
            &scenario.canonical_reads.order_by,
            identifier,
        )?;
        let evolved_read = render_read(
            &qualified_table,
            &scenario.canonical_reads.after_evolution.columns,
            &scenario.canonical_reads.order_by,
            identifier,
        )?;
        let evolution = &scenario.schema_evolution.field;
        let add_column = format!(
            "ALTER TABLE {qualified_table} ADD {} {}{}",
            evolved_column,
            render_type(evolution.field_type),
            if evolution.required { " NOT NULL" } else { "" }
        );

        Ok(Self {
            task_concurrency: plan.execution.task_concurrency,
            catalog: TrinoCatalogSetup {
                name: plan.catalog.name.clone(),
                properties: catalog_properties(plan),
                authentication: plan.catalog.authentication.clone(),
            },
            fixture: TrinoFixtureTarget {
                namespace: plan.fixture.namespace.clone(),
                table: plan.fixture.table.clone(),
                requested_location: plan.fixture.requested_location.clone(),
                bucket: plan.file_io.bucket.clone(),
            },
            observation: TrinoObservationPolicy {
                format_version: scenario.table.format_version,
                initial_schema: scenario.table.schema.fields.clone(),
                evolved_field: scenario.schema_evolution.field.clone(),
                properties: scenario.table.properties.clone(),
            },
            operations: vec![
                TrinoOperation::CreateNamespace {
                    sql: format!("CREATE SCHEMA IF NOT EXISTS {qualified_namespace}"),
                },
                TrinoOperation::CreateTable { sql: create_table },
                TrinoOperation::InitialAppend {
                    sql: render_insert(&qualified_table, &initial_columns, &initial_values),
                },
                TrinoOperation::InitialRead {
                    sql: initial_read,
                    expected: scenario.canonical_reads.initial.clone(),
                },
                TrinoOperation::AddColumn { sql: add_column },
                TrinoOperation::EvolvedAppend {
                    sql: render_insert(&qualified_table, &evolved_columns, &evolved_values),
                },
                TrinoOperation::EvolvedRead {
                    sql: evolved_read,
                    expected: scenario.canonical_reads.after_evolution.clone(),
                },
                TrinoOperation::SnapshotRead {
                    sql: format!(
                        "SELECT * FROM {qualified_namespace}.\"{}$snapshots\"",
                        plan.fixture.table
                    ),
                },
            ],
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrinoConfigurationFile {
    pub relative_path: &'static str,
    pub contents: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrinoServerConfiguration {
    files: Vec<TrinoConfigurationFile>,
}

impl TrinoServerConfiguration {
    pub fn render(program: &TrinoRenderedProgram) -> Result<Self, TrinoRenderError> {
        if program.task_concurrency != 1 || program.catalog.name != TRINO_CATALOG_NAME {
            return Err(TrinoRenderError(
                "unsupported Trino server configuration input",
            ));
        }
        let mut catalog = program.catalog.properties.clone();
        catalog.insert(
            "s3.aws-access-key".to_owned(),
            environment_reference(S3_ACCESS_KEY_ENV),
        );
        catalog.insert(
            "s3.aws-secret-key".to_owned(),
            environment_reference(S3_SECRET_KEY_ENV),
        );
        if matches!(
            program.catalog.authentication,
            EngineCatalogAuthentication::OAuth2ClientCredentials { .. }
        ) {
            catalog.insert(
                "iceberg.rest-catalog.oauth2.credential".to_owned(),
                environment_reference("CATALOG_BENCH_ENGINE_OAUTH_CREDENTIAL"),
            );
        }
        let catalog = render_property_file(&catalog)?;
        let config = render_property_file(&BTreeMap::from([
            ("coordinator".to_owned(), "true".to_owned()),
            (
                "node-scheduler.include-coordinator".to_owned(),
                "true".to_owned(),
            ),
            (
                "discovery.uri".to_owned(),
                "http://127.0.0.1:8080".to_owned(),
            ),
            ("catalog.management".to_owned(), "static".to_owned()),
            (
                "task.concurrency".to_owned(),
                program.task_concurrency.to_string(),
            ),
        ]))?;
        let node = render_property_file(&BTreeMap::from([
            ("node.environment".to_owned(), "catalog_bench".to_owned()),
            (
                "node.id".to_owned(),
                environment_reference("CATALOG_BENCH_TRINO_NODE_ID"),
            ),
            (
                "node.data-dir".to_owned(),
                environment_reference("CATALOG_BENCH_TRINO_DATA_DIR"),
            ),
        ]))?;
        Ok(Self {
            files: vec![
                TrinoConfigurationFile {
                    relative_path: "catalog/bench.properties",
                    contents: catalog,
                },
                TrinoConfigurationFile {
                    relative_path: "config.properties",
                    contents: config,
                },
                TrinoConfigurationFile {
                    relative_path: "jvm.config",
                    contents: TRINO_JVM_CONFIG.to_owned(),
                },
                TrinoConfigurationFile {
                    relative_path: "log.properties",
                    contents: "io.trino=INFO\n".to_owned(),
                },
                TrinoConfigurationFile {
                    relative_path: "node.properties",
                    contents: node,
                },
            ],
        })
    }

    #[must_use]
    pub fn files(&self) -> &[TrinoConfigurationFile] {
        &self.files
    }
}

const TRINO_JVM_CONFIG: &str = concat!(
    "-server\n",
    "-agentpath:/usr/lib/trino/bin/libjvmkill.so\n",
    "-XX:InitialRAMPercentage=80\n",
    "-XX:MaxRAMPercentage=80\n",
    "-XX:G1HeapRegionSize=32M\n",
    "-XX:+ExplicitGCInvokesConcurrent\n",
    "-XX:+HeapDumpOnOutOfMemoryError\n",
    "-XX:+ExitOnOutOfMemoryError\n",
    "-XX:-OmitStackTraceInFastThrow\n",
    "-XX:ReservedCodeCacheSize=256M\n",
    "-XX:PerMethodRecompilationCutoff=10000\n",
    "-XX:PerBytecodeRecompilationCutoff=10000\n",
    "-Djdk.attach.allowAttachSelf=true\n",
    "-Djdk.nio.maxCachedBufferSize=2000000\n",
);

fn environment_reference(name: &str) -> String {
    format!("${{ENV:{name}}}")
}

fn render_property_file(properties: &BTreeMap<String, String>) -> Result<String, TrinoRenderError> {
    let mut rendered = String::new();
    for (key, value) in properties {
        if !valid_property_key(key) || !valid_property_value(value) {
            return Err(TrinoRenderError(
                "Trino configuration contains an unsafe property",
            ));
        }
        rendered.push_str(key);
        rendered.push('=');
        rendered.push_str(value);
        rendered.push('\n');
    }
    Ok(rendered)
}

fn valid_property_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

fn valid_property_value(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 4096
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
}

fn validate_plan(plan: &TrinoExecutionPlan) -> Result<(), TrinoRenderError> {
    if plan.format != TRINO_PLAN_FORMAT {
        return Err(TrinoRenderError("unsupported Trino plan format"));
    }
    if plan.execution.task_concurrency != 1 {
        return Err(TrinoRenderError("unsupported Trino execution settings"));
    }
    if plan.catalog.name != TRINO_CATALOG_NAME
        || !credential_free_http_url(&plan.catalog.uri)
        || !plan.file_io.enabled
        || !credential_free_http_url(&plan.file_io.endpoint)
        || plan.file_io.bucket.is_empty()
        || plan.file_io.region.is_empty()
        || !plan.file_io.path_style_access
    {
        return Err(TrinoRenderError(
            "unsupported Trino catalog or file IO policy",
        ));
    }
    if let Some(location) = &plan.fixture.requested_location {
        let Ok(location) = Url::parse(location) else {
            return Err(TrinoRenderError("invalid Trino table location"));
        };
        if location.scheme() != "s3"
            || location.host_str() != Some(plan.file_io.bucket.as_str())
            || !location.username().is_empty()
            || location.password().is_some()
            || location.query().is_some()
            || location.fragment().is_some()
        {
            return Err(TrinoRenderError("invalid Trino table location"));
        }
    }
    let policy = &plan.scenario.engine_policy;
    if policy.catalog_specific_branches != ForbiddenPolicy::Forbidden
        || policy.catalog_specific_shims != ForbiddenPolicy::Forbidden
        || policy.connector != ConnectorPolicy::StockProfileComponent
        || policy.syntax_rendering != SyntaxRenderingPolicy::EngineSpecificButCatalogNeutral
        || policy.unsupported != UnsupportedPolicy::ClassifyBeforeMutationWithoutASubstituteRequest
        || plan.scenario.transcript_format != ENGINE_TRANSCRIPT_FORMAT
        || !plan
            .fixture
            .namespace
            .starts_with(&format!("{}_", plan.scenario.fixture_prefix))
    {
        return Err(TrinoRenderError(
            "scenario policy does not authorize the Trino renderer",
        ));
    }
    Ok(())
}

fn credential_free_http_url(value: &str) -> bool {
    Url::parse(value).is_ok_and(|url| {
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
    })
}

fn catalog_properties(plan: &TrinoExecutionPlan) -> BTreeMap<String, String> {
    let mut properties = BTreeMap::from([
        ("connector.name".to_owned(), "iceberg".to_owned()),
        ("iceberg.catalog.type".to_owned(), "rest".to_owned()),
        (
            "iceberg.rest-catalog.uri".to_owned(),
            plan.catalog.uri.clone(),
        ),
        ("fs.s3.enabled".to_owned(), plan.file_io.enabled.to_string()),
        ("s3.endpoint".to_owned(), plan.file_io.endpoint.clone()),
        ("s3.region".to_owned(), plan.file_io.region.clone()),
        (
            "s3.path-style-access".to_owned(),
            plan.file_io.path_style_access.to_string(),
        ),
        (
            "iceberg.allowed-extra-properties".to_owned(),
            plan.scenario
                .table
                .properties
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(","),
        ),
    ]);
    if let Some(warehouse) = &plan.catalog.warehouse {
        properties.insert(
            "iceberg.rest-catalog.warehouse".to_owned(),
            warehouse.clone(),
        );
    }
    if let Some(prefix) = &plan.catalog.prefix {
        properties.insert("iceberg.rest-catalog.prefix".to_owned(), prefix.clone());
    }
    match &plan.catalog.authentication {
        EngineCatalogAuthentication::Anonymous => {
            properties.insert(
                "iceberg.rest-catalog.security".to_owned(),
                "NONE".to_owned(),
            );
        }
        EngineCatalogAuthentication::OAuth2ClientCredentials {
            oauth2_server_uri,
            scope,
        } => {
            properties.insert(
                "iceberg.rest-catalog.security".to_owned(),
                "OAUTH2".to_owned(),
            );
            properties.insert(
                "iceberg.rest-catalog.oauth2.server-uri".to_owned(),
                oauth2_server_uri.clone(),
            );
            properties.insert(
                "iceberg.rest-catalog.oauth2.scope".to_owned(),
                scope.clone(),
            );
        }
    }
    properties
}

fn render_field(field: &crate::IcebergField) -> Result<String, TrinoRenderError> {
    Ok(format!(
        "{} {}{}",
        identifier(&field.name)?,
        render_type(field.field_type),
        if field.required { " NOT NULL" } else { "" }
    ))
}

fn render_type(field_type: IcebergPrimitiveType) -> &'static str {
    match field_type {
        IcebergPrimitiveType::Long => "BIGINT",
        IcebergPrimitiveType::String => "VARCHAR",
    }
}

fn identifier(value: &str) -> Result<String, TrinoRenderError> {
    if !valid_identifier(value) {
        return Err(TrinoRenderError(
            "Trino identifier is outside the closed vocabulary",
        ));
    }
    Ok(format!("\"{value}\""))
}
