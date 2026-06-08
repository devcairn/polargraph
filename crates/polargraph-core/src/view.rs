//! View definitions — lenses over the graph.
//!
//! A View is itself stored as nodes+triples in the graph (so it gets
//! temporal versioning and access control for free). This module defines
//! the in-memory struct that the query engine uses after deserialisation.

use crate::id::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Stable identifier for a named view.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ViewId(pub String);

impl ViewId {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl std::fmt::Display for ViewId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Which nodes belong to a view — expressed as a filter over node types,
/// explicit inclusions, or both.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeFilter {
    /// Include nodes whose `node_type` is in this set.
    /// Empty set = include all types (no type filtering).
    pub include_types: HashSet<String>,
    /// Always include these specific nodes regardless of type.
    pub explicit_nodes: HashSet<NodeId>,
}

impl NodeFilter {
    pub fn by_types(types: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            include_types: types.into_iter().map(|t| t.into()).collect(),
            ..Default::default()
        }
    }
}

/// Defines how a predicate's label should be presented in this view and
/// whether the display direction should be reversed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgePresentation {
    /// Display label (overrides the canonical predicate name).
    pub label: String,
    /// If true, render the arrow tip at the *source* node instead of the
    /// target — purely a rendering hint; the underlying edge is unchanged.
    pub reverse_direction: bool,
}

/// A complete view definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub id: ViewId,
    pub display_name: String,

    /// Which nodes are included. `None` = all nodes.
    pub node_filter: Option<NodeFilter>,

    /// Predicates that are visible in this view.
    /// Empty = show all predicates (no filtering).
    pub visible_predicates: HashSet<String>,

    /// Per-predicate presentation overrides (label rename, direction flip).
    pub edge_presentations: HashMap<String, EdgePresentation>,
}

impl View {
    pub fn new(id: impl Into<String>, display_name: impl Into<String>) -> Self {
        Self {
            id: ViewId::new(id),
            display_name: display_name.into(),
            node_filter: None,
            visible_predicates: HashSet::new(),
            edge_presentations: HashMap::new(),
        }
    }

    /// Returns the display label for a predicate in this view.
    /// Falls back to the canonical predicate name if no override is defined.
    pub fn edge_label<'a>(&'a self, predicate: &'a str) -> &'a str {
        self.edge_presentations
            .get(predicate)
            .map(|p| p.label.as_str())
            .unwrap_or(predicate)
    }

    /// Returns true if the edge arrow should be displayed reversed.
    pub fn is_reversed(&self, predicate: &str) -> bool {
        self.edge_presentations
            .get(predicate)
            .map(|p| p.reverse_direction)
            .unwrap_or(false)
    }

    /// Returns true if the predicate should be surfaced in this view.
    pub fn shows_predicate(&self, predicate: &str) -> bool {
        self.visible_predicates.is_empty() || self.visible_predicates.contains(predicate)
    }
}
