//! Engine-neutral generation of the scenario's deterministic SQL row values.

use std::error::Error;
use std::fmt::{Display, Formatter};

use crate::{CategoryGenerator, EngineScenarioParameters, IntegerGenerator, NoteGenerator};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SqlGenerationError(pub(crate) &'static str);

impl Display for SqlGenerationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for SqlGenerationError {}

pub(crate) fn render_rows(
    scenario: &EngineScenarioParameters,
    evolved: bool,
) -> Result<Vec<String>, SqlGenerationError> {
    let batch = if evolved {
        scenario.batches.evolved
    } else {
        scenario.batches.initial
    };
    let end = batch
        .id_start
        .checked_add(batch.rows)
        .ok_or(SqlGenerationError(
            "generated row range exceeds unsigned 64-bit range",
        ))?;
    (batch.id_start..end)
        .map(|id| {
            let IntegerGenerator::Affine { multiplier, offset } =
                &scenario.row_generator.amount_cents;
            let amount = multiplier
                .checked_mul(id)
                .and_then(|value| value.checked_add(*offset))
                .ok_or(SqlGenerationError(
                    "generated amount exceeds unsigned 64-bit range",
                ))?;
            let CategoryGenerator::ModuloLabel { modulus, prefix } =
                &scenario.row_generator.category;
            if *modulus == 0 {
                return Err(SqlGenerationError("category modulus must be positive"));
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

pub(crate) fn render_insert(table: &str, columns: &[String], values: &[String]) -> String {
    format!(
        "INSERT INTO {table} ({}) VALUES {}",
        columns.join(", "),
        values.join(", ")
    )
}

pub(crate) fn render_read<E>(
    table: &str,
    columns: &[String],
    order_by: &[String],
    identifier: fn(&str) -> Result<String, E>,
) -> Result<String, E> {
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

pub(crate) fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

pub(crate) fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
