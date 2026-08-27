//! Stock-engine interoperability policy, execution, and sanitized evidence.
//!
//! The checked-in scenario owns semantic behavior. Engine renderers receive a
//! closed, secret-free plan and may translate that behavior only into the
//! selected engine's stock public syntax.

mod adapters;
mod catalog;
mod negotiation;
mod policy;
mod process;
mod protocol;
mod reconcile;
mod runtime;
mod workflow;

pub use adapters::{RestEngineCatalogConnector, SharedObjectStoreConnector, StockSparkRunner};
pub use catalog::{
    EngineCatalog, EngineCatalogFailure, EngineCatalogFailureKind, EngineCatalogTable,
    EngineCleanupReceipt, EngineResourcePresence, EngineTableLoad, RestEngineCatalog,
};
pub use negotiation::{
    EngineAuthenticationEvidence, EngineAuthenticationMode, EngineCatalogConfigEvidence,
    EngineCatalogNegotiationEvidence, EngineNegotiationProjectionFailure, EngineRoutingResolution,
};
pub use policy::*;
pub use process::*;
pub use protocol::*;
pub use runtime::*;
pub use workflow::{
    run_engine_workflow, EngineBehaviorChecks, EngineBehaviorClassification,
    EngineCatalogConnection, EngineCatalogConnectionEvidence, EngineCatalogConnectionFailure,
    EngineCatalogConnectionFailureKind, EngineCatalogConnector, EngineCleanupEvidence,
    EngineExecution, EngineObjectStoreConnector, EngineOperationEvidence, EngineOperationFailure,
    EngineRunner, EngineSkipReason,
};
