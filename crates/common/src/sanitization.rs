use std::error::Error;
use std::fmt::{Display, Formatter};

use serde::Serialize;
use serde_json::Value;

/// Fixed failure categories for a serialized evidence-value audit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializedValueAuditFailure {
    /// The supplied evidence could not be represented as JSON.
    Serialization,
    /// A serialized data value contained a configured secret.
    SensitiveValue,
    /// A serialized data value contained another forbidden runtime identity.
    ForbiddenValue,
}

impl Display for SerializedValueAuditFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialization => {
                formatter.write_str("value could not be serialized for evidence audit")
            }
            Self::SensitiveValue => {
                formatter.write_str("serialized evidence contains a sensitive runtime value")
            }
            Self::ForbiddenValue => {
                formatter.write_str("serialized evidence contains a forbidden runtime value")
            }
        }
    }
}

impl Error for SerializedValueAuditFailure {}

/// Audit data values in a serializable evidence tree.
///
/// JSON object keys are fixed schema vocabulary rather than captured runtime
/// data, so only values are inspected. Empty needles are ignored because every
/// string contains the empty string.
pub fn audit_serialized_values<T>(
    evidence: &T,
    sensitive_values: &[String],
    forbidden_values: &[String],
) -> Result<(), SerializedValueAuditFailure>
where
    T: Serialize,
{
    let value =
        serde_json::to_value(evidence).map_err(|_| SerializedValueAuditFailure::Serialization)?;
    inspect_value(&value, sensitive_values, forbidden_values)
}

fn inspect_value(
    value: &Value,
    sensitive_values: &[String],
    forbidden_values: &[String],
) -> Result<(), SerializedValueAuditFailure> {
    match value {
        Value::Array(values) => {
            for value in values {
                inspect_value(value, sensitive_values, forbidden_values)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                inspect_value(value, sensitive_values, forbidden_values)?;
            }
        }
        Value::String(value) => {
            if contains_any(value, sensitive_values) {
                return Err(SerializedValueAuditFailure::SensitiveValue);
            }
            if contains_any(value, forbidden_values) {
                return Err(SerializedValueAuditFailure::ForbiddenValue);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn contains_any(value: &str, needles: &[String]) -> bool {
    needles
        .iter()
        .filter(|needle| !needle.is_empty())
        .any(|needle| value.contains(needle))
}
