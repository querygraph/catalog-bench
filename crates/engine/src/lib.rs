//! Stock-engine interoperability policy, execution, and sanitized evidence.
//!
//! The checked-in scenario owns semantic behavior. Engine renderers receive a
//! closed, secret-free plan and may translate that behavior only into the
//! selected engine's stock public syntax.

mod adapters;
mod catalog;
mod execution;
mod flink;
mod iceberg_metadata;
mod negotiation;
mod policy;
mod process;
mod protocol;
mod reconcile;
mod runtime;
mod sql;
mod strict_json;
mod transcript;
mod trino;
mod trino_child;
mod trino_cli;
mod trino_process;
mod workflow;

pub use adapters::{
    RestEngineCatalogConnector, SharedObjectStoreConnector, StockFlinkRunner, StockSparkRunner,
};
pub use catalog::{
    EngineCatalog, EngineCatalogFailure, EngineCatalogFailureKind, EngineCatalogTable,
    EngineCleanupReceipt, EngineResourcePresence, EngineTableLoad, RestEngineCatalog,
};
pub use execution::*;
pub use flink::*;
pub use iceberg_metadata::*;
pub use negotiation::{
    EngineAuthenticationEvidence, EngineAuthenticationMode, EngineCatalogConfigEvidence,
    EngineCatalogNegotiationEvidence, EngineNegotiationProjectionFailure, EngineRoutingResolution,
};
pub use policy::*;
pub use process::*;
pub use protocol::*;
pub use runtime::*;
pub use transcript::{
    run_stock_spark_interoperability, EngineContracts, EngineEvidenceError,
    EngineEvidenceErrorKind, EngineSanitizationViolation, EngineTranscript,
    EngineTranscriptComponent, EngineTranscriptComponents, EngineTranscriptFixture,
    EngineTranscriptProfile, EngineTranscriptSanitization, EngineTranscriptValidationFailure,
    EngineTranscriptValidationFailureKind,
};
pub use trino::*;
pub use trino_child::*;
pub use trino_cli::*;
pub use trino_process::*;
pub use workflow::{
    run_engine_workflow, EngineBehaviorChecks, EngineBehaviorClassification,
    EngineCatalogConnection, EngineCatalogConnectionEvidence, EngineCatalogConnectionFailure,
    EngineCatalogConnectionFailureKind, EngineCatalogConnector, EngineCleanupEvidence,
    EngineExecution, EngineObjectStoreConnector, EngineOperationEvidence, EngineOperationFailure,
    EngineRunner, EngineSkipReason,
};
