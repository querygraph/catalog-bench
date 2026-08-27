use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::IcebergPrimitiveType;

pub const ENGINE_EVENT_PREFIX: &[u8] = b"CATALOG_BENCH_EVENT ";
pub const MAXIMUM_ENGINE_EVENT_BYTES: usize = 16 * 1024;
pub const MAXIMUM_ENGINE_EVENTS: usize = 32;
pub const MAXIMUM_ENGINE_STDOUT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EngineStage {
    VerifyRuntime,
    InitializeCatalog,
    PreflightFixture,
    CreateNamespace,
    CreateTable,
    AppendInitial,
    ReadInitial,
    EvolveSchema,
    AppendEvolved,
    ReadEvolved,
    ObserveFinalTable,
}

impl EngineStage {
    const ORDER: [Self; 11] = [
        Self::VerifyRuntime,
        Self::InitializeCatalog,
        Self::PreflightFixture,
        Self::CreateNamespace,
        Self::CreateTable,
        Self::AppendInitial,
        Self::ReadInitial,
        Self::EvolveSchema,
        Self::AppendEvolved,
        Self::ReadEvolved,
        Self::ObserveFinalTable,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EngineFailureCategory {
    Runtime,
    Connector,
    Catalog,
    Data,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineRuntimeObservation {
    pub spark_version: String,
    pub scala_version: String,
    pub java_version: String,
    pub operating_system: String,
    pub architecture: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineFieldObservation {
    pub id: i32,
    pub name: String,
    pub required: bool,
    pub field_type: IcebergPrimitiveType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EnginePropertyObservation {
    Match,
    Mismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineTableObservation {
    pub table_uuid: String,
    pub metadata_location: String,
    pub location: String,
    pub format_version: u8,
    pub last_column_id: i32,
    pub schema: Vec<EngineFieldObservation>,
    pub snapshots: u64,
    pub properties: BTreeMap<String, EnginePropertyObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowReadObservation {
    pub rows: u64,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "kebab-case", deny_unknown_fields)]
pub enum EngineEvent {
    RuntimeReady {
        runtime: EngineRuntimeObservation,
    },
    CatalogReady,
    FixturePreflight {
        absent: bool,
    },
    NamespaceReady {
        listed_exactly: bool,
    },
    TableReady {
        table: EngineTableObservation,
    },
    InitialAppended {
        snapshots: u64,
    },
    InitialRead {
        read: RowReadObservation,
    },
    SchemaEvolved {
        table: EngineTableObservation,
    },
    EvolvedAppended {
        snapshots: u64,
    },
    EvolvedRead {
        read: RowReadObservation,
    },
    FinalTable {
        table: EngineTableObservation,
    },
    Completed,
    Failed {
        stage: EngineStage,
        category: EngineFailureCategory,
    },
}

impl EngineEvent {
    fn successful_stage(&self) -> Option<EngineStage> {
        match self {
            Self::RuntimeReady { .. } => Some(EngineStage::VerifyRuntime),
            Self::CatalogReady => Some(EngineStage::InitializeCatalog),
            Self::FixturePreflight { .. } => Some(EngineStage::PreflightFixture),
            Self::NamespaceReady { .. } => Some(EngineStage::CreateNamespace),
            Self::TableReady { .. } => Some(EngineStage::CreateTable),
            Self::InitialAppended { .. } => Some(EngineStage::AppendInitial),
            Self::InitialRead { .. } => Some(EngineStage::ReadInitial),
            Self::SchemaEvolved { .. } => Some(EngineStage::EvolveSchema),
            Self::EvolvedAppended { .. } => Some(EngineStage::AppendEvolved),
            Self::EvolvedRead { .. } => Some(EngineStage::ReadEvolved),
            Self::FinalTable { .. } => Some(EngineStage::ObserveFinalTable),
            Self::Completed | Self::Failed { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EngineProtocolFailureKind {
    StdoutTooLarge,
    EventLineTooLarge,
    MalformedEvent,
    TooManyEvents,
    OutOfOrder,
    PostTerminal,
    MissingTerminal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineProtocolFailure {
    pub kind: EngineProtocolFailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineEventCapture {
    pub events: Vec<EngineEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<EngineProtocolFailure>,
    pub stdout_bytes_observed: u64,
}

impl EngineEventCapture {
    #[must_use]
    pub fn cleanup_authorized(&self) -> bool {
        self.events
            .iter()
            .any(|event| matches!(event, EngineEvent::FixturePreflight { absent: true }))
    }

    #[must_use]
    pub fn fixture_collision(&self) -> bool {
        self.failure.is_none()
            && matches!(
                self.events.last(),
                Some(EngineEvent::FixturePreflight { absent: false })
            )
    }

    #[must_use]
    pub fn completed(&self) -> bool {
        self.failure.is_none() && matches!(self.events.last(), Some(EngineEvent::Completed))
    }

    #[must_use]
    pub fn engine_failure(&self) -> Option<(EngineStage, EngineFailureCategory)> {
        match self.events.last() {
            Some(EngineEvent::Failed { stage, category }) if self.failure.is_none() => {
                Some((*stage, *category))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
pub struct EngineEventDecoder {
    line: Vec<u8>,
    events: Vec<EngineEvent>,
    failure: Option<EngineProtocolFailure>,
    stdout_bytes_observed: u64,
    next_stage: usize,
    terminal: bool,
}

impl EngineEventDecoder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, bytes: &[u8]) {
        self.stdout_bytes_observed = self
            .stdout_bytes_observed
            .saturating_add(bytes.len() as u64);
        if self.stdout_bytes_observed > MAXIMUM_ENGINE_STDOUT_BYTES as u64 {
            self.fail(EngineProtocolFailureKind::StdoutTooLarge);
            return;
        }
        if self.failure.is_some() {
            return;
        }
        for byte in bytes {
            if *byte == b'\n' {
                self.finish_line();
                self.line.clear();
            } else if self.line.len() < MAXIMUM_ENGINE_EVENT_BYTES {
                self.line.push(*byte);
            } else {
                self.fail(EngineProtocolFailureKind::EventLineTooLarge);
                return;
            }
        }
    }

    pub(crate) fn failed(&self) -> bool {
        self.failure.is_some()
    }

    #[must_use]
    pub fn finish(mut self) -> EngineEventCapture {
        if self.failure.is_none() && !self.line.is_empty() {
            self.finish_line();
        }
        if self.failure.is_none() && !self.terminal {
            self.fail(EngineProtocolFailureKind::MissingTerminal);
        }
        EngineEventCapture {
            events: self.events,
            failure: self.failure,
            stdout_bytes_observed: self.stdout_bytes_observed,
        }
    }

    fn finish_line(&mut self) {
        if self.failure.is_some() || !self.line.starts_with(ENGINE_EVENT_PREFIX) {
            return;
        }
        let payload = &self.line[ENGINE_EVENT_PREFIX.len()..];
        let Ok(event) = serde_json::from_slice::<EngineEvent>(payload) else {
            self.fail(EngineProtocolFailureKind::MalformedEvent);
            return;
        };
        self.accept(event);
    }

    fn accept(&mut self, event: EngineEvent) {
        if self.terminal {
            self.fail(EngineProtocolFailureKind::PostTerminal);
            return;
        }
        if self.events.len() >= MAXIMUM_ENGINE_EVENTS {
            self.fail(EngineProtocolFailureKind::TooManyEvents);
            return;
        }
        match &event {
            EngineEvent::Failed { stage, .. } => {
                if EngineStage::ORDER.get(self.next_stage) != Some(stage) {
                    self.fail(EngineProtocolFailureKind::OutOfOrder);
                    return;
                }
                self.terminal = true;
            }
            EngineEvent::Completed => {
                if self.next_stage != EngineStage::ORDER.len() {
                    self.fail(EngineProtocolFailureKind::OutOfOrder);
                    return;
                }
                self.terminal = true;
            }
            EngineEvent::FixturePreflight { absent: false } => {
                if EngineStage::ORDER.get(self.next_stage) != Some(&EngineStage::PreflightFixture) {
                    self.fail(EngineProtocolFailureKind::OutOfOrder);
                    return;
                }
                self.next_stage += 1;
                self.terminal = true;
            }
            _ => {
                if event.successful_stage().as_ref() != EngineStage::ORDER.get(self.next_stage) {
                    self.fail(EngineProtocolFailureKind::OutOfOrder);
                    return;
                }
                self.next_stage += 1;
            }
        }
        self.events.push(event);
    }

    fn fail(&mut self, kind: EngineProtocolFailureKind) {
        if self.failure.is_none() {
            self.failure = Some(EngineProtocolFailure { kind });
        }
    }
}
