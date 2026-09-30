//! A triple together with the named graph it belongs to.

use serde::{Deserialize, Serialize};

use crate::{id::GraphId, triple::Triple};

/// A [`Triple`] in a named graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quad {
    pub triple: Triple,
    pub graph: GraphId,
}

impl Quad {
    pub fn new(triple: Triple, graph: GraphId) -> Self {
        Self { triple, graph }
    }

    /// A triple in the default graph.
    pub fn default_graph(triple: Triple) -> Self {
        Self::new(triple, GraphId::DEFAULT)
    }
}
