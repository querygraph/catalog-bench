use std::error::Error;
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use url::Url;

use crate::sql::{
    literal, render_insert, render_read, render_rows, valid_identifier, SqlGenerationError,
};
use crate::{
    CanonicalRead, ConnectorPolicy, DuckDbExecutionPlan, ForbiddenPolicy, IcebergField,
    IcebergPrimitiveType, SyntaxRenderingPolicy, UnsupportedPolicy, DUCKDB_CATALOG_NAME,
    DUCKDB_PLAN_FORMAT, ENGINE_TRANSCRIPT_FORMAT,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckDbRenderError(&'static str);

impl Display for DuckDbRenderError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}
impl Error for DuckDbRenderError {}
impl From<SqlGenerationError> for DuckDbRenderError {
    fn from(error: SqlGenerationError) -> Self {
        Self(error.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DuckDbOperationPurpose {
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
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
pub enum DuckDbOperation {
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
    SnapshotRead,
}
impl DuckDbOperation {
    #[must_use]
    pub fn purpose(&self) -> DuckDbOperationPurpose {
        match self {
            Self::CreateNamespace { .. } => DuckDbOperationPurpose::CreateNamespace,
            Self::CreateTable { .. } => DuckDbOperationPurpose::CreateTable,
            Self::InitialAppend { .. } => DuckDbOperationPurpose::InitialAppend,
            Self::InitialRead { .. } => DuckDbOperationPurpose::InitialRead,
            Self::AddColumn { .. } => DuckDbOperationPurpose::AddColumn,
            Self::EvolvedAppend { .. } => DuckDbOperationPurpose::EvolvedAppend,
            Self::EvolvedRead { .. } => DuckDbOperationPurpose::EvolvedRead,
            Self::SnapshotRead => DuckDbOperationPurpose::SnapshotRead,
        }
    }
    #[must_use]
    pub fn sql(&self) -> Option<&str> {
        match self {
            Self::CreateNamespace { sql }
            | Self::CreateTable { sql }
            | Self::InitialAppend { sql }
            | Self::InitialRead { sql, .. }
            | Self::AddColumn { sql }
            | Self::EvolvedAppend { sql }
            | Self::EvolvedRead { sql, .. } => Some(sql),
            Self::SnapshotRead => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckDbRenderedProgram {
    pub catalog_name: String,
    pub catalog_uri: String,
    pub warehouse: Option<String>,
    pub prefix: Option<String>,
    pub authentication: crate::EngineCatalogAuthentication,
    pub file_io: crate::S3FileIoPlan,
    pub fixture: crate::TrinoFixtureTarget,
    pub observation: crate::TrinoObservationPolicy,
    pub operations: Vec<DuckDbOperation>,
}

impl DuckDbRenderedProgram {
    pub fn render(plan: &DuckDbExecutionPlan) -> Result<Self, DuckDbRenderError> {
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
        let mut properties = vec![format!(
            "'format-version' = {}",
            scenario.table.format_version
        )];
        properties.extend(
            scenario
                .table
                .properties
                .iter()
                .map(|(key, value)| format!("{} = {}", literal(key), literal(value))),
        );
        if let Some(location) = &plan.fixture.requested_location {
            properties.push(format!("location = {}", literal(location)));
        }
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
        Ok(Self {
            catalog_name: plan.catalog.name.clone(),
            catalog_uri: plan.catalog.uri.clone(),
            warehouse: plan.catalog.warehouse.clone(),
            prefix: plan.catalog.prefix.clone(),
            authentication: plan.catalog.authentication.clone(),
            file_io: plan.file_io.clone(),
            fixture: crate::TrinoFixtureTarget {
                namespace: plan.fixture.namespace.clone(),
                table: plan.fixture.table.clone(),
                requested_location: plan.fixture.requested_location.clone(),
                bucket: plan.file_io.bucket.clone(),
            },
            observation: crate::TrinoObservationPolicy {
                format_version: scenario.table.format_version,
                initial_schema: scenario.table.schema.fields.clone(),
                evolved_field: scenario.schema_evolution.field.clone(),
                properties: scenario.table.properties.clone(),
            },
            operations: vec![
                DuckDbOperation::CreateNamespace {
                    sql: format!("CREATE SCHEMA IF NOT EXISTS {qualified_namespace}"),
                },
                DuckDbOperation::CreateTable {
                    sql: format!(
                        "CREATE TABLE {qualified_table} ({schema}) WITH ({})",
                        properties.join(", ")
                    ),
                },
                DuckDbOperation::InitialAppend {
                    sql: render_insert(&qualified_table, &initial_columns, &initial_values),
                },
                DuckDbOperation::InitialRead {
                    sql: render_read(
                        &qualified_table,
                        &scenario.canonical_reads.initial.columns,
                        &scenario.canonical_reads.order_by,
                        identifier,
                    )?,
                    expected: scenario.canonical_reads.initial.clone(),
                },
                DuckDbOperation::AddColumn {
                    sql: format!(
                        "ALTER TABLE {qualified_table} ADD COLUMN {evolved_column} {}{}",
                        render_type(scenario.schema_evolution.field.field_type),
                        if scenario.schema_evolution.field.required {
                            " NOT NULL"
                        } else {
                            ""
                        }
                    ),
                },
                DuckDbOperation::EvolvedAppend {
                    sql: render_insert(&qualified_table, &evolved_columns, &evolved_values),
                },
                DuckDbOperation::EvolvedRead {
                    sql: render_read(
                        &qualified_table,
                        &scenario.canonical_reads.after_evolution.columns,
                        &scenario.canonical_reads.order_by,
                        identifier,
                    )?,
                    expected: scenario.canonical_reads.after_evolution.clone(),
                },
                DuckDbOperation::SnapshotRead,
            ],
        })
    }
}

fn validate_plan(plan: &DuckDbExecutionPlan) -> Result<(), DuckDbRenderError> {
    let policy = &plan.scenario.engine_policy;
    if plan.format != DUCKDB_PLAN_FORMAT
        || plan.catalog.name != DUCKDB_CATALOG_NAME
        || !credential_free_http_url(&plan.catalog.uri)
        || !credential_free_http_url(&plan.file_io.endpoint)
        || plan.file_io.bucket.is_empty()
        || plan.file_io.region.is_empty()
        || !plan.file_io.path_style_access
        || policy.catalog_specific_branches != ForbiddenPolicy::Forbidden
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
        return Err(DuckDbRenderError("unsupported DuckDB plan policy"));
    }
    if let Some(location) = &plan.fixture.requested_location {
        let url =
            Url::parse(location).map_err(|_| DuckDbRenderError("invalid DuckDB table location"))?;
        if url.scheme() != "s3"
            || url.host_str() != Some(plan.file_io.bucket.as_str())
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(DuckDbRenderError("invalid DuckDB table location"));
        }
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
fn identifier(value: &str) -> Result<String, SqlGenerationError> {
    if valid_identifier(value) {
        Ok(format!("\"{value}\""))
    } else {
        Err(SqlGenerationError("invalid DuckDB identifier"))
    }
}
fn render_field(field: &IcebergField) -> Result<String, SqlGenerationError> {
    Ok(format!(
        "{} {}{}",
        identifier(&field.name)?,
        render_type(field.field_type),
        if field.required { " NOT NULL" } else { "" }
    ))
}
fn render_type(value: IcebergPrimitiveType) -> &'static str {
    match value {
        IcebergPrimitiveType::Long => "BIGINT",
        IcebergPrimitiveType::String => "VARCHAR",
    }
}
