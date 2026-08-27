//! Stock-engine interoperability policy, execution, and sanitized evidence.
//!
//! The checked-in scenario owns semantic behavior. Engine renderers receive a
//! closed, secret-free plan and may translate that behavior only into the
//! selected engine's stock public syntax.

mod policy;

pub use policy::*;
