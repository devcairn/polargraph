//! Node and edge identifiers.
//!
//! We use UUID v7 (time-ordered) so that IDs sort chronologically and are
//! cluster-safe without a central sequence generator.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Opaque identifier for a node in the graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub Uuid);

impl NodeId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Fixed-width 16-byte representation for index keys.
    #[inline]
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Opaque identifier for an edge (relationship) instance.
///
/// Edges are first-class so they can carry properties and appear in views.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EdgeId(pub Uuid);

impl EdgeId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for EdgeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for EdgeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
