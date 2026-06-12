//! The fundamental graph primitives: Node, Edge, Predicate, Triple.
//!
//! A Triple is the atomic unit of storage: (subject, predicate, object).
//! Subject and object are NodeIds. Predicate is an interned string label.
//! Every triple carries a bitemporal envelope and an optional value (for
//! property triples where the "object" is a scalar rather than another node).

use crate::{
    id::{EdgeId, NodeId},
    temporal::BiTemporalRange,
    value::Value,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// An interned predicate/label. Stored as a string; the storage layer
/// maintains a predicate-id map for compact index keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Predicate(pub String);

impl Predicate {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl std::fmt::Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A node in the graph. Nodes have an ID, a type label, and a property bag.
/// Properties are themselves triples in the store; this struct is the
/// in-memory representation returned by queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    /// The ontological type of this node (e.g. "Person", "Project").
    pub node_type: String,
    /// Property bag — populated according to the caller's predicate mask.
    pub properties: HashMap<String, Value>,
}

impl Node {
    pub fn new(node_type: impl Into<String>) -> Self {
        Self {
            id: NodeId::new(),
            node_type: node_type.into(),
            properties: HashMap::new(),
        }
    }
}

/// A relationship edge between two nodes. Edges are first-class so they can:
/// - carry properties
/// - be included/excluded by views
/// - have their label remapped per-view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub id: EdgeId,
    pub from: NodeId,
    pub to: NodeId,
    /// The canonical predicate label stored in the triple index.
    pub predicate: Predicate,
    pub properties: HashMap<String, Value>,
    pub temporal: BiTemporalRange,
}

impl Edge {
    pub fn new(from: NodeId, to: NodeId, predicate: impl Into<String>) -> Self {
        Self {
            id: EdgeId::new(),
            from,
            to,
            predicate: Predicate::new(predicate),
            properties: HashMap::new(),
            temporal: BiTemporalRange::assert_now(crate::temporal::Timestamp::now()),
        }
    }
}

/// The atomic storage unit.
///
/// Four flavours:
/// - **Relationship triple**: subject → predicate → object (both are NodeIds)
/// - **Property triple**:    subject → predicate → value  (object is a scalar)
/// - **EdgeProperty**:       edge annotation → predicate → scalar value  (RDF-star)
/// - **EdgeRelation**:       edge annotation → predicate → object node   (RDF-star)
///
/// Relation and Property triples share the hexastore index; property triples
/// use a reserved sentinel for the object slot in the index key.
/// EdgeProperty and EdgeRelation triples are stored in the EPA and EPO column
/// families respectively, keyed by EdgeId rather than NodeId.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Triple {
    /// subject → predicate → object node
    Relation {
        subject: NodeId,
        predicate: Predicate,
        object: NodeId,
        edge_id: EdgeId,
        temporal: BiTemporalRange,
    },
    /// subject → predicate → scalar value
    Property {
        subject: NodeId,
        predicate: Predicate,
        value: Value,
        temporal: BiTemporalRange,
    },
    /// RDF-star: annotate a relation triple (by EdgeId) with a scalar value.
    ///
    /// Stored in the `EPA` column family: `[edge_id:16][pred_id:4][tt:8]`.
    EdgeProperty {
        edge: EdgeId,
        predicate: Predicate,
        value: Value,
        temporal: BiTemporalRange,
    },
    /// RDF-star: annotate a relation triple (by EdgeId) with a relation to a node.
    ///
    /// Stored in the `EPO` column family: `[edge_id:16][pred_id:4][obj_id:16][tt:8]`.
    EdgeRelation {
        edge: EdgeId,
        predicate: Predicate,
        object: NodeId,
        temporal: BiTemporalRange,
    },
}

impl Triple {
    /// Return the subject NodeId.
    ///
    /// For `EdgeProperty` and `EdgeRelation`, the EdgeId bytes are reinterpreted
    /// as a NodeId (both wrap `Uuid`). This is safe since the actual routing to
    /// EPA/EPO CFs is done before `subject()` would be used for storage writes.
    pub fn subject(&self) -> NodeId {
        match self {
            Triple::Relation { subject, .. } | Triple::Property { subject, .. } => *subject,
            Triple::EdgeProperty { edge, .. } | Triple::EdgeRelation { edge, .. } => NodeId(edge.0),
        }
    }

    pub fn predicate(&self) -> &Predicate {
        match self {
            Triple::Relation { predicate, .. }
            | Triple::Property { predicate, .. }
            | Triple::EdgeProperty { predicate, .. }
            | Triple::EdgeRelation { predicate, .. } => predicate,
        }
    }

    pub fn temporal(&self) -> &BiTemporalRange {
        match self {
            Triple::Relation { temporal, .. }
            | Triple::Property { temporal, .. }
            | Triple::EdgeProperty { temporal, .. }
            | Triple::EdgeRelation { temporal, .. } => temporal,
        }
    }
}
