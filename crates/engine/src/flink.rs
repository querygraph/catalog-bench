use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    CategoryGenerator, ConnectorPolicy, EngineCatalogAuthentication, FlinkExecutionPlan,
    ForbiddenPolicy, IcebergPrimitiveType, IntegerGenerator, NoteGenerator, SyntaxRenderingPolicy,
    UnsupportedPolicy, ENGINE_TRANSCRIPT_FORMAT, FLINK_CATALOG_NAME, FLINK_PLAN_FORMAT,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlinkRenderError(&'static str);

impl Display for FlinkRenderError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for FlinkRenderError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlinkCatalogSetup {
    pub name: String,
    pub properties: BTreeMap<String, String>,
    pub authentication: EngineCatalogAuthentication,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FlinkStatementPurpose {
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
pub struct FlinkStatement {
    pub purpose: FlinkStatementPurpose,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlinkRenderedProgram {
    pub parallelism: u32,
    pub catalog: FlinkCatalogSetup,
    pub statements: Vec<FlinkStatement>,
}

impl FlinkRenderedProgram {
    pub fn render(plan: &FlinkExecutionPlan) -> Result<Self, FlinkRenderError> {
        validate_plan(plan)?;
        let namespace = identifier(&plan.fixture.namespace)?;
        let table = identifier(&plan.fixture.table)?;
        let qualified_table = format!("{namespace}.{table}");
        let scenario = &plan.scenario;
        let schema = scenario
            .table
            .schema
            .fields
            .iter()
            .map(render_field)
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        let mut table_properties = scenario.table.properties.clone();
        table_properties.insert(
            "format-version".to_owned(),
            scenario.table.format_version.to_string(),
        );
        table_properties.insert(
            "write.format.default".to_owned(),
            match scenario.table.file_format {
                crate::FileFormat::Parquet => "parquet".to_owned(),
            },
        );
        if let Some(location) = &plan.fixture.requested_location {
            table_properties.insert("location".to_owned(), location.clone());
        }
        let create_table = format!(
            "CREATE TABLE {qualified_table} ({schema}) WITH ({})",
            render_properties(&table_properties)
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
        let initial_values = render_rows(plan, false)?;
        let evolved_values = render_rows(plan, true)?;
        let initial_read = render_read(
            &qualified_table,
            &scenario.canonical_reads.initial.columns,
            &scenario.canonical_reads.order_by,
        )?;
        let evolved_read = render_read(
            &qualified_table,
            &scenario.canonical_reads.after_evolution.columns,
            &scenario.canonical_reads.order_by,
        )?;
        let evolution = &scenario.schema_evolution.field;
        let add_column = format!(
            "ALTER TABLE {qualified_table} ADD {} {}{}",
            evolved_column,
            render_type(evolution.field_type),
            if evolution.required { " NOT NULL" } else { "" }
        );

        Ok(Self {
            parallelism: plan.execution.parallelism,
            catalog: FlinkCatalogSetup {
                name: plan.catalog.name.clone(),
                properties: catalog_properties(plan),
                authentication: plan.catalog.authentication.clone(),
            },
            statements: vec![
                statement(
                    FlinkStatementPurpose::CreateNamespace,
                    format!("CREATE DATABASE IF NOT EXISTS {namespace}"),
                ),
                statement(FlinkStatementPurpose::CreateTable, create_table),
                statement(
                    FlinkStatementPurpose::InitialAppend,
                    render_insert(&qualified_table, &initial_columns, &initial_values),
                ),
                statement(FlinkStatementPurpose::InitialRead, initial_read),
                statement(FlinkStatementPurpose::AddColumn, add_column),
                statement(
                    FlinkStatementPurpose::EvolvedAppend,
                    render_insert(&qualified_table, &evolved_columns, &evolved_values),
                ),
                statement(FlinkStatementPurpose::EvolvedRead, evolved_read),
                statement(
                    FlinkStatementPurpose::SnapshotRead,
                    format!(
                        "SELECT * FROM {namespace}.`{}$snapshots`",
                        plan.fixture.table
                    ),
                ),
            ],
        })
    }
}

fn validate_plan(plan: &FlinkExecutionPlan) -> Result<(), FlinkRenderError> {
    if plan.format != FLINK_PLAN_FORMAT {
        return Err(FlinkRenderError("unsupported Flink plan format"));
    }
    if plan.execution.parallelism != 1 {
        return Err(FlinkRenderError("unsupported Flink execution settings"));
    }
    if plan.catalog.name != FLINK_CATALOG_NAME
        || !credential_free_http_url(&plan.catalog.uri)
        || plan.file_io.implementation != "org.apache.iceberg.aws.s3.S3FileIO"
        || !credential_free_http_url(&plan.file_io.endpoint)
        || plan.file_io.bucket.is_empty()
        || plan.file_io.region.is_empty()
        || !plan.file_io.path_style_access
    {
        return Err(FlinkRenderError(
            "unsupported Flink catalog or file IO policy",
        ));
    }
    if let Some(location) = &plan.fixture.requested_location {
        let Ok(location) = Url::parse(location) else {
            return Err(FlinkRenderError("invalid Flink table location"));
        };
        if location.scheme() != "s3"
            || location.host_str() != Some(plan.file_io.bucket.as_str())
            || !location.username().is_empty()
            || location.password().is_some()
            || location.query().is_some()
            || location.fragment().is_some()
        {
            return Err(FlinkRenderError("invalid Flink table location"));
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
        return Err(FlinkRenderError(
            "scenario policy does not authorize the Flink renderer",
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

fn catalog_properties(plan: &FlinkExecutionPlan) -> BTreeMap<String, String> {
    let mut properties = BTreeMap::from([
        ("type".to_owned(), "iceberg".to_owned()),
        ("catalog-type".to_owned(), "rest".to_owned()),
        ("uri".to_owned(), plan.catalog.uri.clone()),
        ("io-impl".to_owned(), plan.file_io.implementation.clone()),
        ("s3.endpoint".to_owned(), plan.file_io.endpoint.clone()),
        ("s3.region".to_owned(), plan.file_io.region.clone()),
        (
            "s3.path-style-access".to_owned(),
            plan.file_io.path_style_access.to_string(),
        ),
    ]);
    if let Some(warehouse) = &plan.catalog.warehouse {
        properties.insert("warehouse".to_owned(), warehouse.clone());
    }
    if let Some(prefix) = &plan.catalog.prefix {
        properties.insert("prefix".to_owned(), prefix.clone());
    }
    properties
}

fn render_rows(plan: &FlinkExecutionPlan, evolved: bool) -> Result<Vec<String>, FlinkRenderError> {
    let scenario = &plan.scenario;
    let batch = if evolved {
        scenario.batches.evolved
    } else {
        scenario.batches.initial
    };
    let end = batch
        .id_start
        .checked_add(batch.rows)
        .ok_or(FlinkRenderError(
            "generated row range exceeds unsigned 64-bit range",
        ))?;
    (batch.id_start..end)
        .map(|id| {
            let IntegerGenerator::Affine { multiplier, offset } =
                &scenario.row_generator.amount_cents;
            let amount = multiplier
                .checked_mul(id)
                .and_then(|value| value.checked_add(*offset))
                .ok_or(FlinkRenderError(
                    "generated amount exceeds unsigned 64-bit range",
                ))?;
            let CategoryGenerator::ModuloLabel { modulus, prefix } =
                &scenario.row_generator.category;
            if *modulus == 0 {
                return Err(FlinkRenderError("category modulus must be positive"));
            }
            let mut values = vec![
                id.to_string(),
                literal(&format!("{prefix}{}", id % modulus)),
                amount.to_string(),
            ];
            if evolved {
                let NoteGenerator::IdLabel { prefix } = &scenario.row_generator.note;
                values.push(literal(&format!("{prefix}{id}")));
            }
            Ok(format!("({})", values.join(", ")))
        })
        .collect()
}

fn render_field(field: &crate::IcebergField) -> Result<String, FlinkRenderError> {
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
        IcebergPrimitiveType::String => "STRING",
    }
}

fn render_insert(table: &str, columns: &[String], values: &[String]) -> String {
    format!(
        "INSERT INTO {table} ({}) VALUES {}",
        columns.join(", "),
        values.join(", ")
    )
}

fn render_read(
    table: &str,
    columns: &[String],
    order_by: &[String],
) -> Result<String, FlinkRenderError> {
    let columns = columns
        .iter()
        .map(|column| identifier(column))
        .collect::<Result<Vec<_>, _>>()?;
    let order_by = order_by
        .iter()
        .map(|column| identifier(column))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!(
        "SELECT {} FROM {table} ORDER BY {}",
        columns.join(", "),
        order_by.join(", ")
    ))
}

fn render_properties(properties: &BTreeMap<String, String>) -> String {
    properties
        .iter()
        .map(|(key, value)| format!("{}={}", literal(key), literal(value)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn statement(purpose: FlinkStatementPurpose, sql: String) -> FlinkStatement {
    FlinkStatement { purpose, sql }
}

fn identifier(value: &str) -> Result<String, FlinkRenderError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(FlinkRenderError(
            "Flink identifier is outside the closed vocabulary",
        ));
    }
    Ok(format!("`{value}`"))
}

fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
