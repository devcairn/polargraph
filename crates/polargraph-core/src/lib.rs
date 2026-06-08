//! Core types for PolarGraph.
//!
//! This crate is dependency-free (no I/O, no async). Every other crate depends
//! on it. Keep it that way.

pub mod id;
pub mod schema;
pub mod temporal;
pub mod triple;
pub mod value;
pub mod view;

#[cfg(test)]
mod tests;

pub use id::NodeId;
pub use schema::{EdgeTypeDef, FieldDef, FieldKind, NodeTypeDef, StorageMode, VectorSpaceDef};
pub use temporal::Timestamp;
pub use triple::{Edge, Node, Predicate, Triple};
pub use value::Value;
pub use view::{View, ViewId};
