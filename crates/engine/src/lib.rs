//! Stock-engine interoperability policy, execution, and sanitized evidence.
//!
//! The checked-in scenario owns semantic behavior. Engine renderers receive a
//! closed, secret-free plan and may translate that behavior only into the
//! selected engine's stock public syntax.

mod policy;
mod process;
mod protocol;
mod runtime;

pub use policy::*;
pub use process::*;
pub use protocol::*;
pub use runtime::*;
