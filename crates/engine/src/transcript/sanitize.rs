use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};

use catalog_bench_common::sanitization::{audit_serialized_values, SerializedValueAuditFailure};
use zeroize::Zeroize as _;

use super::EngineTranscript;
use crate::{
    BatchPolicy, CategoryGenerator, IntegerGenerator, InteroperabilityPlan, NoteGenerator,
    SecretRead, SecretSource,
};

const RAW_BEARER_PREFIX: &str = "Bearer ";

pub(super) fn audit_with_plan(
    transcript: &EngineTranscript,
    plan: &InteroperabilityPlan,
    sensitive_values: &[String],
) -> Result<(), EngineSanitizationViolation> {
    audit_base_values(transcript, sensitive_values)?;
    let rows = canonical_row_values(plan).ok_or(EngineSanitizationViolation::ContractBinding)?;
    match audit_serialized_values(transcript, &[], &rows) {
        Ok(()) => Ok(()),
        Err(SerializedValueAuditFailure::Serialization) => {
            Err(EngineSanitizationViolation::Serialization)
        }
        Err(SerializedValueAuditFailure::SensitiveValue) => {
            Err(EngineSanitizationViolation::SensitiveRuntimeValue)
        }
        Err(SerializedValueAuditFailure::ForbiddenValue) => {
            Err(EngineSanitizationViolation::RawEngineRow)
        }
    }
}

pub(super) fn audit_base_values(
    transcript: &EngineTranscript,
    sensitive_values: &[String],
) -> Result<(), EngineSanitizationViolation> {
    audit_serialized_values(transcript, sensitive_values, &[])
        .map_err(EngineSanitizationViolation::from)?;
    match audit_serialized_values(transcript, &[], &[RAW_BEARER_PREFIX.to_owned()]) {
        Ok(()) => Ok(()),
        Err(SerializedValueAuditFailure::Serialization) => {
            Err(EngineSanitizationViolation::Serialization)
        }
        Err(SerializedValueAuditFailure::SensitiveValue) => {
            Err(EngineSanitizationViolation::SensitiveRuntimeValue)
        }
        Err(SerializedValueAuditFailure::ForbiddenValue) => {
            Err(EngineSanitizationViolation::RawCredentialForm)
        }
    }
}

fn canonical_row_values(plan: &InteroperabilityPlan) -> Option<Vec<String>> {
    let scenario = plan.scenario();
    let IntegerGenerator::Affine { multiplier, offset } = scenario.row_generator.amount_cents;
    let CategoryGenerator::ModuloLabel {
        modulus,
        ref prefix,
    } = scenario.row_generator.category;
    let NoteGenerator::IdLabel {
        prefix: ref note_prefix,
    } = scenario.row_generator.note;
    if modulus == 0 {
        return None;
    }

    let row_values = |id: u64| {
        let amount = id.checked_mul(multiplier)?.checked_add(offset)?;
        Some((format!("{prefix}{}", id % modulus), amount))
    };
    let mut rows = Vec::new();
    for id in batch_ids(scenario.batches.initial)? {
        let (category, amount) = row_values(id)?;
        rows.push(serde_json::to_string(&(id, &category, amount)).ok()?);
        rows.push(serde_json::to_string(&(id, category, amount, Option::<String>::None)).ok()?);
    }
    for id in batch_ids(scenario.batches.evolved)? {
        let (category, amount) = row_values(id)?;
        rows.push(
            serde_json::to_string(&(id, category, amount, format!("{note_prefix}{id}"))).ok()?,
        );
    }
    rows.sort();
    rows.dedup();
    Some(rows)
}

fn batch_ids(batch: BatchPolicy) -> Option<std::ops::Range<u64>> {
    Some(batch.id_start..batch.id_start.checked_add(batch.rows)?)
}

pub(super) struct ObservedSecretSource<S> {
    source: Arc<S>,
    sensitive_values: Mutex<Vec<String>>,
}

impl<S> ObservedSecretSource<S> {
    pub(super) fn new(source: Arc<S>) -> Self {
        Self {
            source,
            sensitive_values: Mutex::default(),
        }
    }

    pub(super) fn sensitive_values(&self) -> SensitiveValues {
        let mut values = self.sensitive_values.lock().unwrap().clone();
        values.sort();
        values.dedup();
        SensitiveValues(values)
    }
}

impl<S> SecretSource for ObservedSecretSource<S>
where
    S: SecretSource,
{
    fn read_secret(&self, name: &str) -> SecretRead {
        let secret = self.source.read_secret(name);
        if let SecretRead::Value(value) = &secret {
            if !value.is_empty() {
                self.sensitive_values.lock().unwrap().push(value.clone());
            }
        }
        secret
    }
}

impl<S> Drop for ObservedSecretSource<S> {
    fn drop(&mut self) {
        if let Ok(values) = self.sensitive_values.get_mut() {
            values.zeroize();
        }
    }
}

pub(super) struct SensitiveValues(Vec<String>);

impl SensitiveValues {
    pub(super) fn as_slice(&self) -> &[String] {
        &self.0
    }
}

impl Drop for SensitiveValues {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineSanitizationViolation {
    Serialization,
    SensitiveRuntimeValue,
    RawCredentialForm,
    RawEngineRow,
    ContractBinding,
}

impl Display for EngineSanitizationViolation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialization => formatter.write_str("engine transcript could not be serialized"),
            Self::SensitiveRuntimeValue => {
                formatter.write_str("engine transcript contains a sensitive runtime value")
            }
            Self::RawCredentialForm => {
                formatter.write_str("engine transcript contains an unredacted credential form")
            }
            Self::RawEngineRow => formatter.write_str("engine transcript contains a raw row"),
            Self::ContractBinding => {
                formatter.write_str("engine transcript cannot bind its sanitization policy")
            }
        }
    }
}

impl Error for EngineSanitizationViolation {}

impl From<SerializedValueAuditFailure> for EngineSanitizationViolation {
    fn from(failure: SerializedValueAuditFailure) -> Self {
        match failure {
            SerializedValueAuditFailure::Serialization => Self::Serialization,
            SerializedValueAuditFailure::SensitiveValue => Self::SensitiveRuntimeValue,
            SerializedValueAuditFailure::ForbiddenValue => Self::RawCredentialForm,
        }
    }
}
