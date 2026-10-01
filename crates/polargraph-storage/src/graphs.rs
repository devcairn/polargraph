//! Named-graph management: metadata, drop, copy and move
//! (`docs/contxtbroker-platform-plan.md` §2.4, §2.6).
//!
//! - Graph metadata is stored as properties of the graph's IRI node in the
//!   system graph [`SYSTEM_GRAPH_IRI`].
//! - [`drop_graph`](TripleStore::drop_graph) is **bitemporal**: it writes a
//!   closing version (`vt_end = now`) for every live quad instead of deleting
//!   keys, so the graph's history stays queryable with time travel.
//! - [`copy_graph`](TripleStore::copy_graph) copies the live quads of one
//!   graph into another (SPARQL `COPY` when `clear_target`, `ADD` otherwise);
//!   [`move_graph`](TripleStore::move_graph) is copy + drop.
//!
//! Large operations commit in chunks of [`GRAPH_OP_CHUNK`] quads. While a
//! chunked copy runs, the target graph carries the metadata flag
//! [`COPY_IN_PROGRESS_PRED`] = `true`, removed when the copy completes.

use polargraph_core::{
    id::{GraphId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};

use crate::{changes::GraphOp, error::StorageError, mvcc::WriteMode, store::TripleStore};

/// The system graph holding graph metadata.
pub const SYSTEM_GRAPH_IRI: &str = "urn:pg:graph:meta";

/// Metadata predicate set on a copy target while a chunked copy is running.
pub const COPY_IN_PROGRESS_PRED: &str = "urn:pg:copyInProgress";

/// Quads per commit for drop / copy.
pub const GRAPH_OP_CHUNK: usize = 50_000;

/// Live-quad statistics for one graph.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphStats {
    /// Quads valid now.
    pub live_quads: u64,
    /// Latest transaction time among the live quads (µs), 0 if none.
    pub last_write_tt: i64,
}

impl TripleStore {
    /// The id of the system (metadata) graph, interning it on first use.
    pub fn system_graph(&self) -> Result<GraphId, StorageError> {
        self.intern_graph(SYSTEM_GRAPH_IRI)
    }

    /// Create (intern) a named graph and set its metadata properties.
    /// Idempotent; existing metadata predicates are replaced.
    pub fn create_graph(
        &self,
        iri: &str,
        metadata: &[(String, Value)],
    ) -> Result<GraphId, StorageError> {
        self.create_graph_by(iri, metadata, "")
    }

    /// [`Self::create_graph`], recording `author` in the change log (a new
    /// graph is logged as [`GraphOp::Created`]).
    pub fn create_graph_by(
        &self,
        iri: &str,
        metadata: &[(String, Value)],
        author: &str,
    ) -> Result<GraphId, StorageError> {
        let is_new = self.graph_id(iri).is_none();
        let g = self.intern_graph(iri)?;
        if is_new {
            let mut tx = self.begin();
            tx.set_author(author);
            tx.record_graph_op(GraphOp::Created(g));
            tx.commit()?;
        }
        if !metadata.is_empty() {
            self.set_graph_metadata(g, metadata)?;
        }
        Ok(g)
    }

    /// Replace metadata properties of graph `g` (one value per predicate).
    pub fn set_graph_metadata(
        &self,
        g: GraphId,
        metadata: &[(String, Value)],
    ) -> Result<(), StorageError> {
        let node = self.graph_node_or_err(g)?;
        let meta = self.system_graph()?;
        let mut tx = self.begin();
        for (predicate, value) in metadata {
            tx.insert_in(
                Triple::Property {
                    subject: node,
                    predicate: Predicate::new(predicate.as_str()),
                    value: value.clone(),
                    temporal: BiTemporalRange::assert_now(Timestamp::now()),
                },
                meta,
                WriteMode::Replace,
            );
        }
        tx.commit()?;
        Ok(())
    }

    /// Current metadata properties of graph `g`, sorted by predicate.
    pub fn graph_metadata(&self, g: GraphId) -> Result<Vec<(String, Value)>, StorageError> {
        let Some(node) = self.graph_node(g) else {
            return Ok(vec![]);
        };
        let Some(meta) = self.graph_id(SYSTEM_GRAPH_IRI) else {
            return Ok(vec![]);
        };
        let snap = self.snapshot(Timestamp(self.oracle_ts()));
        let mut out: Vec<(String, Value)> = snap
            .scan_by_subject_in_graph(meta, &node)?
            .into_iter()
            .filter_map(|t| match t {
                Triple::Property {
                    predicate, value, ..
                } => Some((predicate.0, value)),
                _ => None,
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// Live-quad count and latest write time of graph `g`.
    pub fn graph_stats(&self, g: GraphId) -> Result<GraphStats, StorageError> {
        let live = self.live_quads(g)?;
        Ok(GraphStats {
            live_quads: live.len() as u64,
            last_write_tt: live.iter().map(|t| t.temporal().tt.0).max().unwrap_or(0),
        })
    }

    /// Close every live quad of `g` at `now` (bitemporal drop). Returns the
    /// number of quads closed. The graph id and its metadata remain.
    pub fn drop_graph(&self, g: GraphId) -> Result<usize, StorageError> {
        self.drop_graph_by(g, "")
    }

    /// [`Self::drop_graph`], recording `author`; the last commit carries
    /// [`GraphOp::Dropped`].
    pub fn drop_graph_by(&self, g: GraphId, author: &str) -> Result<usize, StorageError> {
        let now = Timestamp::now();
        let live = self.live_quads(g)?;
        let chunks: Vec<&[Triple]> = live.chunks(GRAPH_OP_CHUNK).collect();
        for (i, chunk) in chunks.iter().enumerate() {
            let mut tx = self.begin();
            tx.set_author(author);
            for t in chunk.iter() {
                tx.insert_in(close_at(t.clone(), now), g, WriteMode::Add);
            }
            if i + 1 == chunks.len() {
                tx.record_graph_op(GraphOp::Dropped(g));
            }
            tx.commit()?;
        }
        if chunks.is_empty() {
            let mut tx = self.begin();
            tx.set_author(author);
            tx.record_graph_op(GraphOp::Dropped(g));
            tx.commit()?;
        }
        Ok(live.len())
    }

    /// Copy the live quads of `source` into `target` as new writes (the
    /// source's valid-time windows are kept). With `clear_target`, `target`
    /// is dropped first (SPARQL `COPY`); otherwise quads are added to it
    /// (SPARQL `ADD`). Returns the number of quads copied.
    pub fn copy_graph(
        &self,
        source: GraphId,
        target: GraphId,
        clear_target: bool,
    ) -> Result<usize, StorageError> {
        self.copy_graph_by(source, target, clear_target, "")
    }

    /// [`Self::copy_graph`], recording `author`; the last commit carries
    /// [`GraphOp::Copied`] (a cleared target also logs `Dropped`).
    pub fn copy_graph_by(
        &self,
        source: GraphId,
        target: GraphId,
        clear_target: bool,
        author: &str,
    ) -> Result<usize, StorageError> {
        if source == target {
            return Ok(0);
        }
        if clear_target {
            self.drop_graph_by(target, author)?;
        }
        let live = self.live_quads(source)?;
        let chunked = live.len() > GRAPH_OP_CHUNK;
        if chunked && target != GraphId::DEFAULT {
            self.set_graph_metadata(target, &[(COPY_IN_PROGRESS_PRED.into(), Value::Bool(true))])?;
        }
        let chunks: Vec<&[Triple]> = live.chunks(GRAPH_OP_CHUNK).collect();
        for (i, chunk) in chunks.iter().enumerate() {
            let mut tx = self.begin();
            tx.set_author(author);
            for t in chunk.iter() {
                tx.insert_in(t.clone(), target, WriteMode::Add);
            }
            if i + 1 == chunks.len() {
                tx.record_graph_op(GraphOp::Copied { source, target });
            }
            tx.commit()?;
        }
        if chunks.is_empty() {
            let mut tx = self.begin();
            tx.set_author(author);
            tx.record_graph_op(GraphOp::Copied { source, target });
            tx.commit()?;
        }
        if chunked && target != GraphId::DEFAULT {
            self.clear_graph_metadata(target, COPY_IN_PROGRESS_PRED)?;
        }
        Ok(live.len())
    }

    /// Copy `source` into `target` (replacing it), then drop `source`
    /// (SPARQL `MOVE`). Returns the number of quads moved.
    pub fn move_graph(&self, source: GraphId, target: GraphId) -> Result<usize, StorageError> {
        self.move_graph_by(source, target, "")
    }

    /// [`Self::move_graph`], recording `author` (logged as `Dropped(target)`,
    /// `Copied`, `Dropped(source)`).
    pub fn move_graph_by(
        &self,
        source: GraphId,
        target: GraphId,
        author: &str,
    ) -> Result<usize, StorageError> {
        if source == target {
            return Ok(0);
        }
        let n = self.copy_graph_by(source, target, true, author)?;
        self.drop_graph_by(source, author)?;
        Ok(n)
    }

    /// Close a metadata property of graph `g`.
    fn clear_graph_metadata(&self, g: GraphId, predicate: &str) -> Result<(), StorageError> {
        let (Some(node), Some(meta)) = (self.graph_node(g), self.graph_id(SYSTEM_GRAPH_IRI)) else {
            return Ok(());
        };
        let snap = self.snapshot(Timestamp(self.oracle_ts()));
        let now = Timestamp::now();
        let mut tx = self.begin();
        for t in snap.scan_by_subject_in_graph(meta, &node)? {
            if t.predicate().0 == predicate {
                tx.insert_in(close_at(t, now), meta, WriteMode::Add);
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Quads of `g` valid now, at the latest commit.
    fn live_quads(&self, g: GraphId) -> Result<Vec<Triple>, StorageError> {
        self.snapshot(Timestamp(self.oracle_ts())).scan_graph(g)
    }

    fn graph_node_or_err(&self, g: GraphId) -> Result<NodeId, StorageError> {
        self.graph_node(g).ok_or_else(|| {
            StorageError::Validation(format!(
                "{g} is not a named graph (the default graph has no metadata)"
            ))
        })
    }
}

/// `t` with its valid time closed at `now` (same `vt_start`, so it shadows
/// the open version) — write it to retract `t` bitemporally.
pub fn close_at(t: Triple, now: Timestamp) -> Triple {
    let close = |temporal: BiTemporalRange| BiTemporalRange {
        vt_end: now.max(temporal.vt_start),
        ..temporal
    };
    match t {
        Triple::Relation {
            subject,
            predicate,
            object,
            edge_id,
            temporal,
        } => Triple::Relation {
            subject,
            predicate,
            object,
            edge_id,
            temporal: close(temporal),
        },
        Triple::Property {
            subject,
            predicate,
            value,
            temporal,
        } => Triple::Property {
            subject,
            predicate,
            value,
            temporal: close(temporal),
        },
        other => other,
    }
}
