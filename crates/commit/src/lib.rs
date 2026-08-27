//! Strict same-table contention benchmark building blocks.
//!
//! The executable owns network and object-store effects. This library keeps the
//! benchmark policy, schedule, accounting, and sanitization boundaries pure so
//! they can be exhaustively tested without a running catalog.

pub mod model;
pub mod policy;
pub mod protocol;
pub mod stats;
pub mod store;
