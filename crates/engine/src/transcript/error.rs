use std::error::Error;
use std::fmt::{Display, Formatter};

use super::EngineSanitizationViolation;
use crate::PolicyError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineTranscriptValidationFailureKind {
    Format,
    Scenario,
    ContractDigests,
    Profile,
    Policy,
    Components,
    Fixture,
    Execution,
    Sanitization,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineTranscriptValidationFailure {
    pub kind: EngineTranscriptValidationFailureKind,
}

impl EngineTranscriptValidationFailure {
    pub(super) fn new(kind: EngineTranscriptValidationFailureKind) -> Self {
        Self { kind }
    }
}

impl Display for EngineTranscriptValidationFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "engine transcript validation failed: {:?}",
            self.kind
        )
    }
}

impl Error for EngineTranscriptValidationFailure {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineEvidenceErrorKind {
    ProfileContract,
    ScenarioContract,
    ProfileDocumentKind,
    ScenarioDocumentKind,
    Policy,
    ProfileComponent,
    Sanitization,
    Validation,
}

#[derive(Debug)]
pub struct EngineEvidenceError {
    kind: EngineEvidenceErrorKind,
    source: EngineEvidenceErrorSource,
}

#[derive(Debug)]
enum EngineEvidenceErrorSource {
    None,
    Policy(PolicyError),
    Sanitization(EngineSanitizationViolation),
    Validation(EngineTranscriptValidationFailure),
}

impl EngineEvidenceError {
    pub(super) fn fixed(kind: EngineEvidenceErrorKind) -> Self {
        Self {
            kind,
            source: EngineEvidenceErrorSource::None,
        }
    }

    pub(super) fn policy(error: PolicyError) -> Self {
        Self {
            kind: EngineEvidenceErrorKind::Policy,
            source: EngineEvidenceErrorSource::Policy(error),
        }
    }

    pub(super) fn sanitization(error: EngineSanitizationViolation) -> Self {
        Self {
            kind: EngineEvidenceErrorKind::Sanitization,
            source: EngineEvidenceErrorSource::Sanitization(error),
        }
    }

    pub(super) fn validation(error: EngineTranscriptValidationFailure) -> Self {
        Self {
            kind: EngineEvidenceErrorKind::Validation,
            source: EngineEvidenceErrorSource::Validation(error),
        }
    }

    #[must_use]
    pub fn kind(&self) -> EngineEvidenceErrorKind {
        self.kind
    }
}

impl Display for EngineEvidenceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.source {
            EngineEvidenceErrorSource::None => {
                write!(formatter, "engine evidence failed: {:?}", self.kind)
            }
            EngineEvidenceErrorSource::Policy(error) => Display::fmt(error, formatter),
            EngineEvidenceErrorSource::Sanitization(error) => Display::fmt(error, formatter),
            EngineEvidenceErrorSource::Validation(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for EngineEvidenceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.source {
            EngineEvidenceErrorSource::None => None,
            EngineEvidenceErrorSource::Policy(error) => Some(error),
            EngineEvidenceErrorSource::Sanitization(error) => Some(error),
            EngineEvidenceErrorSource::Validation(error) => Some(error),
        }
    }
}
