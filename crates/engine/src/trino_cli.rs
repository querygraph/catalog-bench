//! Strict decoding of bounded stock Trino CLI JSON output.

use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};

use catalog_bench_conformance::sha256_hex;
use serde_json::Value;

use crate::sql::valid_identifier;
use crate::strict_json::{decode_strict_json, StrictJsonError};
use crate::{CanonicalRead, RowReadObservation};

const MAXIMUM_TRINO_CLI_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_TRINO_SCALAR_BYTES: usize = 64 * 1024;
const MAXIMUM_TRINO_SCALAR_TEXT_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrinoCliDecodeError {
    OutputTooLarge,
    MissingTrailingLf,
    MalformedRow,
    DuplicateColumn,
    UnexpectedColumns,
    UnsupportedValue,
    TooManyRows,
    CanonicalOutputTooLarge,
}

impl Display for TrinoCliDecodeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::OutputTooLarge => "Trino CLI output exceeds its byte limit",
            Self::MissingTrailingLf => "Trino CLI output lacks its final LF",
            Self::MalformedRow => "Trino CLI emitted a malformed JSON row",
            Self::DuplicateColumn => "Trino CLI row contains a duplicate column",
            Self::UnexpectedColumns => "Trino CLI row columns differ from the read oracle",
            Self::UnsupportedValue => "Trino CLI row contains a non-scalar value",
            Self::TooManyRows => "Trino CLI emitted more rows than the read oracle",
            Self::CanonicalOutputTooLarge => "canonical Trino read exceeds its byte limit",
        })
    }
}

impl std::error::Error for TrinoCliDecodeError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrinoCliScalarError {
    OutputTooLarge,
    InvalidShape,
    DuplicateColumn,
    InvalidValue,
}

impl Display for TrinoCliScalarError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::OutputTooLarge => "Trino CLI scalar output exceeds its byte limit",
            Self::InvalidShape => "Trino CLI scalar output has an invalid row shape",
            Self::DuplicateColumn => "Trino CLI scalar output contains a duplicate column",
            Self::InvalidValue => "Trino CLI scalar output has an invalid value",
        })
    }
}

impl std::error::Error for TrinoCliScalarError {}

pub fn decode_trino_single_u64(output: &[u8], column: &str) -> Result<u64, TrinoCliScalarError> {
    single_value(output, column)?
        .as_u64()
        .ok_or(TrinoCliScalarError::InvalidValue)
}

pub fn decode_trino_single_text(
    output: &[u8],
    column: &str,
) -> Result<String, TrinoCliScalarError> {
    let value = single_value(output, column)?;
    let text = value
        .as_str()
        .filter(|text| {
            !text.is_empty()
                && text.len() <= MAXIMUM_TRINO_SCALAR_TEXT_BYTES
                && !text.chars().any(char::is_control)
        })
        .ok_or(TrinoCliScalarError::InvalidValue)?;
    Ok(text.to_owned())
}

fn single_value(output: &[u8], column: &str) -> Result<Value, TrinoCliScalarError> {
    if output.len() > MAXIMUM_TRINO_SCALAR_BYTES {
        return Err(TrinoCliScalarError::OutputTooLarge);
    }
    if output.is_empty() || !output.ends_with(b"\n") || !valid_identifier(column) {
        return Err(TrinoCliScalarError::InvalidShape);
    }
    let line = output
        .strip_suffix(b"\n")
        .ok_or(TrinoCliScalarError::InvalidShape)?;
    if line.is_empty() || line.contains(&b'\n') {
        return Err(TrinoCliScalarError::InvalidShape);
    }
    let value = decode_strict_json(line).map_err(|error| match error {
        StrictJsonError::DuplicateKey => TrinoCliScalarError::DuplicateColumn,
        StrictJsonError::Malformed => TrinoCliScalarError::InvalidShape,
    })?;
    let Value::Object(mut object) = value else {
        return Err(TrinoCliScalarError::InvalidShape);
    };
    if object.len() != 1 || !object.contains_key(column) {
        return Err(TrinoCliScalarError::InvalidShape);
    }
    object
        .remove(column)
        .ok_or(TrinoCliScalarError::InvalidShape)
}

pub fn decode_trino_canonical_read(
    output: &[u8],
    expected: &CanonicalRead,
) -> Result<RowReadObservation, TrinoCliDecodeError> {
    if output.len() > MAXIMUM_TRINO_CLI_BYTES {
        return Err(TrinoCliDecodeError::OutputTooLarge);
    }
    if !output.is_empty() && !output.ends_with(b"\n") {
        return Err(TrinoCliDecodeError::MissingTrailingLf);
    }
    let expected_columns = expected.columns.iter().cloned().collect::<BTreeSet<_>>();
    if expected_columns.len() != expected.columns.len() {
        return Err(TrinoCliDecodeError::UnexpectedColumns);
    }

    let capacity = usize::try_from(expected.bytes)
        .unwrap_or(MAXIMUM_TRINO_CLI_BYTES)
        .min(MAXIMUM_TRINO_CLI_BYTES);
    let mut canonical = Vec::with_capacity(capacity);
    let mut rows = 0_u64;
    let body = output.strip_suffix(b"\n").unwrap_or(output);
    for line in body.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            if output.is_empty() {
                break;
            }
            return Err(TrinoCliDecodeError::MalformedRow);
        }
        if rows >= expected.rows {
            return Err(TrinoCliDecodeError::TooManyRows);
        }
        let row = decode_strict_json(line).map_err(|error| match error {
            StrictJsonError::DuplicateKey => TrinoCliDecodeError::DuplicateColumn,
            StrictJsonError::Malformed => TrinoCliDecodeError::MalformedRow,
        })?;
        let row = row.as_object().ok_or(TrinoCliDecodeError::MalformedRow)?;
        if row
            .values()
            .any(|value| matches!(value, Value::Array(_) | Value::Object(_)))
        {
            return Err(TrinoCliDecodeError::UnsupportedValue);
        }
        if row.keys().cloned().collect::<BTreeSet<_>>() != expected_columns {
            return Err(TrinoCliDecodeError::UnexpectedColumns);
        }
        let values = expected
            .columns
            .iter()
            .map(|column| row.get(column).expect("column set equality checked above"))
            .collect::<Vec<_>>();
        serde_json::to_writer(&mut canonical, &values)
            .map_err(|_| TrinoCliDecodeError::MalformedRow)?;
        canonical.push(b'\n');
        if canonical.len() > MAXIMUM_TRINO_CLI_BYTES {
            return Err(TrinoCliDecodeError::CanonicalOutputTooLarge);
        }
        rows += 1;
    }
    Ok(RowReadObservation {
        rows,
        bytes: u64::try_from(canonical.len()).unwrap_or(u64::MAX),
        sha256: sha256_hex(&canonical),
    })
}
