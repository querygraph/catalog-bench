//! Engine-neutral process evidence shared by stock runtime adapters.
//!
//! The `0`/`2`/`3` terminal mapping belongs to the closed harness event
//! protocol, not to Spark. Every stock-engine adapter must emit that same
//! protocol before the common reconciliation workflow can trust its outcome.

use serde::{Deserialize, Serialize};

use crate::protocol::{
    EngineEventCapture, EngineFailureCategory, EngineProtocolFailureKind, EngineStage,
};
use crate::runtime::RuntimeVerification;

const SUCCESS_EXIT: i32 = 0;
const FAILURE_EXIT: i32 = 2;
const FIXTURE_COLLISION_EXIT: i32 = 3;

/// Credential category whose value may be read but never serialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EngineCredentialKind {
    ObjectStoreAccessKey,
    ObjectStoreSecretKey,
    CatalogClientId,
    CatalogClientSecret,
}

/// Closed reason that a required credential could not be admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EngineCredentialFailureKind {
    Missing,
    Empty,
    Unreadable,
}

/// Value-free credential admission failure safe for transcript evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineCredentialFailure {
    pub credential: EngineCredentialKind,
    pub kind: EngineCredentialFailureKind,
}

/// Effect stage that prevented an engine process from being launched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EnginePreparationFailureKind {
    ExecutionPlanMismatch,
    RenderPlan,
    TemporaryDirectory,
    EncodePlan,
    WritePlan,
    WriteRenderer,
    CreateLocalDirectory,
}

/// Closed terminal classification shared by all stock-engine process adapters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "kebab-case", deny_unknown_fields)]
pub enum EngineProcessOutcome {
    RuntimeRejected {},
    CredentialRejected {
        failure: EngineCredentialFailure,
    },
    PreparationFailed {
        kind: EnginePreparationFailureKind,
    },
    SpawnFailed {},
    TimedOut {},
    StdoutFailed {},
    WaitFailed {},
    ProtocolRejected {
        kind: EngineProtocolFailureKind,
    },
    ExitProtocolMismatch {},
    Completed {},
    FixtureCollision {},
    EngineFailed {
        stage: EngineStage,
        category: EngineFailureCategory,
    },
}

impl EngineProcessOutcome {
    pub(crate) fn from_terminal(exit_code: Option<i32>, capture: &EngineEventCapture) -> Self {
        match exit_code {
            Some(SUCCESS_EXIT) if capture.completed() => Self::Completed {},
            Some(FIXTURE_COLLISION_EXIT) if capture.fixture_collision() => {
                Self::FixtureCollision {}
            }
            Some(FAILURE_EXIT) => capture
                .engine_failure()
                .map(|(stage, category)| Self::EngineFailed { stage, category })
                .unwrap_or(Self::ExitProtocolMismatch {}),
            Some(_) | None => Self::ExitProtocolMismatch {},
        }
    }
}

/// Runtime, protocol, and terminal evidence produced before reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineProcessExecution {
    pub runtime: RuntimeVerification,
    pub outcome: EngineProcessOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture: Option<EngineEventCapture>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_elapsed_micros: Option<u64>,
}

impl EngineProcessExecution {
    pub(crate) fn before_process(
        runtime: RuntimeVerification,
        outcome: EngineProcessOutcome,
    ) -> Self {
        Self {
            runtime,
            outcome,
            capture: None,
            exit_code: None,
            process_elapsed_micros: None,
        }
    }

    pub(crate) fn from_in_process_events(
        runtime: RuntimeVerification,
        exit_code: i32,
        events: Vec<crate::EngineEvent>,
        process_elapsed_micros: u64,
    ) -> Self {
        let capture = EngineEventCapture {
            events,
            failure: None,
            stdout_bytes_observed: 0,
        };
        let outcome = EngineProcessOutcome::from_terminal(Some(exit_code), &capture);
        Self {
            runtime,
            outcome,
            capture: Some(capture),
            exit_code: Some(exit_code),
            process_elapsed_micros: Some(process_elapsed_micros),
        }
    }

    #[must_use]
    pub fn passed(&self) -> bool {
        matches!(self.outcome, EngineProcessOutcome::Completed {})
            && self.exit_code == Some(SUCCESS_EXIT)
            && self
                .capture
                .as_ref()
                .is_some_and(EngineEventCapture::completed)
    }

    #[must_use]
    pub fn cleanup_authorized(&self) -> bool {
        !matches!(self.outcome, EngineProcessOutcome::FixtureCollision {})
            && self
                .capture
                .as_ref()
                .is_some_and(EngineEventCapture::cleanup_authorized)
    }

    #[must_use]
    pub fn fixture_collision(&self) -> bool {
        matches!(self.outcome, EngineProcessOutcome::FixtureCollision {})
            && self.exit_code == Some(FIXTURE_COLLISION_EXIT)
            && self
                .capture
                .as_ref()
                .is_some_and(EngineEventCapture::fixture_collision)
    }
}
