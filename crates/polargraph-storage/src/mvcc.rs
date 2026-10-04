//! MVCC layer: TimestampOracle, Transaction, Snapshot, ConflictError.
//!
//! # Concurrency model
//!
//! We use **optimistic concurrency control** (OCC):
//!
//! 1. `begin()` — snapshot the current committed timestamp as `read_ts`.
//! 2. Read through the snapshot (sees all commits with `tt <= read_ts`).
//! 3. Buffer writes in memory.
//! 4. `commit()` — acquire commit lock, check for write-write conflicts,
//!    stamp all buffered triples with `commit_ts`, flush to RocksDB.
//!
//! Conflicts: if a quad in the write buffer — or, for a `Replace` property
//! write, any value of its `(subject, predicate, graph)` — has a version in
//! storage with `read_ts < tt <= commit_ts`, another transaction beat us to
//! it. We abort rather than silently overwrite.
//!
//! # Timestamp oracle
//!
//! A single `AtomicI64` serves as the committed timestamp.  Read timestamps
//! snapshot it without touching it.  Commits acquire a `Mutex<()>` to
//! serialize, increment the counter, write all triples with the new value as
//! `tt`, then release the lock.  The counter persists to the META CF so
//! restarts pick up where they left off.

use crate::{
    cf,
    error::StorageError,
    keys::{self, Order},
    store::{PendingWrite, TripleStore},
};
use polargraph_core::{
    id::{GraphId, NodeId},
    temporal::Timestamp,
    triple::Triple,
    value::Value,
};
use rocksdb::WriteBatch;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::debug;

// ── META key for the oracle counter ──────────────────────────────────────────

pub(crate) const META_ORACLE_CTR: &[u8] = b"__oracle_ctr";

// ── TimestampOracle ───────────────────────────────────────────────────────────

/// Monotonically increasing transaction timestamp source.
///
/// Cheap to clone (Arc-backed). All clones share the same counter.
#[derive(Clone)]
pub struct TimestampOracle {
    inner: Arc<OracleInner>,
}

struct OracleInner {
    /// Current highest committed timestamp. Read transactions snapshot this;
    /// commit transactions increment it under `commit_lock`.
    committed: AtomicI64,
    /// Ensures no two transactions share a commit timestamp and that the
    /// conflict check + write are atomic.
    commit_lock: Mutex<()>,
}

impl TimestampOracle {
    /// Create a new oracle starting at `initial` (loaded from storage on open).
    pub fn new(initial: i64) -> Self {
        Self {
            inner: Arc::new(OracleInner {
                committed: AtomicI64::new(initial),
                commit_lock: Mutex::new(()),
            }),
        }
    }

    /// Snapshot the current committed timestamp for a new read transaction.
    pub fn read_ts(&self) -> Timestamp {
        Timestamp(self.inner.committed.load(Ordering::SeqCst))
    }

    /// Advance `committed` to at least `ts` if `ts` is larger.
    ///
    /// Used by `insert_at_ts` so that subsequent reads can see explicitly-
    /// timestamped triples without going through the full commit path.
    pub(crate) fn advance_to(&self, ts: Timestamp) {
        let mut current = self.inner.committed.load(Ordering::SeqCst);
        while ts.0 > current {
            match self.inner.committed.compare_exchange(
                current,
                ts.0,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(new_current) => current = new_current,
            }
        }
    }

    /// Acquire the commit lock and advance the counter.
    ///
    /// The commit timestamp is `max(committed + 1, now_µs)` so that `tt` is
    /// both monotonically increasing (MVCC correctness) and anchored to real
    /// wall-clock time (bitemporal retention). The guard must be held until the
    /// WriteBatch is flushed to RocksDB.
    pub(crate) fn begin_commit(&self) -> (Timestamp, MutexGuard<'_, ()>) {
        let guard = self.inner.commit_lock.lock().unwrap();
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        let prev = self.inner.committed.load(Ordering::SeqCst);
        let new_ts = std::cmp::max(prev + 1, wall);
        self.inner.committed.store(new_ts, Ordering::SeqCst);
        (Timestamp(new_ts), guard)
    }
}

// ── ConflictError ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct ConflictError {
    pub subject: NodeId,
    pub predicate: String,
}

impl std::fmt::Display for ConflictError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "write conflict on ({}, {}): committed by another transaction after read_ts",
            self.subject, self.predicate
        )
    }
}

impl std::error::Error for ConflictError {}

// ── WriteMode ─────────────────────────────────────────────────────────────────

/// How a property write treats existing values of the same
/// `(subject, predicate, graph)` (`docs/design/v3-key-layout.md` §6).
/// Relations and annotations ignore it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// `Replace` for an open-ended property (`vt_end` = end of time), `Add`
    /// otherwise — a closing write (DELETE) only touches its own value.
    #[default]
    Auto,
    /// Close every other open value of `(s, p, g)` at the new value's
    /// `vt_start`, then write it: the property ends up single-valued.
    Replace,
    /// Write alongside existing values (RDF semantics, multi-valued fields).
    Add,
}

impl WriteMode {
    /// The effective mode for `triple` (`Auto` resolved).
    pub fn resolve(self, triple: &Triple) -> WriteMode {
        match (self, triple) {
            (WriteMode::Auto, Triple::Property { temporal, .. })
                if temporal.vt_end == Timestamp::END_OF_TIME =>
            {
                WriteMode::Replace
            }
            (WriteMode::Auto, _) => WriteMode::Add,
            (mode, _) => mode,
        }
    }
}

// ── Transaction ───────────────────────────────────────────────────────────────

/// An in-progress read-write transaction.
///
/// Reads see all data committed at or before `read_ts`.
/// Writes are buffered until `commit()` or dropped on rollback.
pub struct Transaction {
    pub(crate) store: TripleStore,
    pub read_ts: Timestamp,
    write_buffer: Vec<Triple>,
    /// Graph and write mode of each entry in `write_buffer` (same order).
    write_meta: Vec<(GraphId, WriteMode)>,
    /// IRIs to record in the IRI dictionary on commit.
    iris: Vec<String>,
    /// Who is writing (a user id; empty for service calls) — recorded in the
    /// change log.
    author: String,
    /// Graph-level operations to record in the change log.
    graph_ops: Vec<crate::changes::GraphOp>,
}

impl Transaction {
    pub(crate) fn new(store: TripleStore, read_ts: Timestamp) -> Self {
        Self {
            store,
            read_ts,
            write_buffer: Vec::new(),
            write_meta: Vec::new(),
            iris: Vec::new(),
            author: String::new(),
            graph_ops: Vec::new(),
        }
    }

    /// Record `author` (the caller's user id) on this commit's change-log
    /// entry.
    pub fn set_author(&mut self, author: impl Into<String>) {
        self.author = author.into();
    }

    /// Record a graph-level operation on this commit's change-log entry.
    pub fn record_graph_op(&mut self, op: crate::changes::GraphOp) {
        self.graph_ops.push(op);
    }

    /// Record `iri` in the IRI dictionary when this transaction commits, so
    /// the node it names (`term::iri_to_node_id(iri)`) can be exported under
    /// its IRI. `urn:uuid:` IRIs are ignored (they carry their ID).
    pub fn bind_iri(&mut self, iri: impl Into<String>) {
        self.iris.push(iri.into());
    }

    /// Buffer a triple for insertion into the default graph at commit time,
    /// with [`WriteMode::Auto`].
    ///
    /// The triple's existing `tt` is ignored; the actual commit timestamp is
    /// assigned at `commit()`.
    pub fn insert(&mut self, triple: Triple) {
        self.insert_in(triple, GraphId::DEFAULT, WriteMode::Auto);
    }

    /// Buffer a triple for insertion into `graph` with an explicit write mode.
    pub fn insert_in(&mut self, triple: Triple, graph: GraphId, mode: WriteMode) {
        self.write_buffer.push(triple);
        self.write_meta.push((graph, mode));
    }

    /// Returns a view of triples buffered but not yet committed.
    ///
    /// Used by the query layer for write-your-own-reads within an open
    /// transaction: overlay these over storage scans at `read_ts`.
    pub fn pending_triples(&self) -> &[Triple] {
        &self.write_buffer
    }

    /// Snapshot reads — see state as of `read_ts`. ─────────────────────────
    pub fn scan_by_subject(&self, subject: &NodeId) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_subject_at(subject, &crate::store::ReadAt::latest(self.read_ts))
    }

    pub fn scan_by_subject_predicate(
        &self,
        subject: &NodeId,
        predicate: &str,
    ) -> Result<Vec<Triple>, StorageError> {
        self.store.scan_by_subject_predicate_at(
            subject,
            predicate,
            &crate::store::ReadAt::latest(self.read_ts),
        )
    }

    pub fn scan_by_predicate(&self, predicate: &str) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_predicate_at(predicate, &crate::store::ReadAt::latest(self.read_ts))
    }

    pub fn scan_by_predicate_object(
        &self,
        predicate: &str,
        object: &NodeId,
    ) -> Result<Vec<Triple>, StorageError> {
        self.store.scan_by_predicate_object_at(
            predicate,
            object,
            &crate::store::ReadAt::latest(self.read_ts),
        )
    }

    pub fn scan_by_object(&self, object: &NodeId) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_object_at(object, &crate::store::ReadAt::latest(self.read_ts))
    }

    pub fn scan_by_subject_object(
        &self,
        subject: &NodeId,
        object: &NodeId,
    ) -> Result<Vec<Triple>, StorageError> {
        self.store.scan_by_subject_object_at(
            subject,
            object,
            &crate::store::ReadAt::latest(self.read_ts),
        )
    }

    pub fn scan_all(&self) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_all_at(&crate::store::ReadAt::latest(self.read_ts))
    }

    /// Commit the transaction.
    ///
    /// Steps:
    ///   1. Acquire commit lock, get `commit_ts`.
    ///   2. For each buffered hexastore triple, check no conflicting write exists
    ///      in storage with `read_ts < tt <= commit_ts`. Edge annotations skip
    ///      conflict checking (additive-only semantics).
    ///   3. If clean, write all triples with `commit_ts` as their `tt`.
    ///   4. Persist updated oracle counter to META CF.
    ///   5. Release commit lock.
    pub fn commit(self) -> Result<Timestamp, StorageError> {
        let Transaction {
            store,
            read_ts,
            write_buffer,
            write_meta,
            iris,
            author,
            graph_ops,
        } = self;
        if write_buffer.is_empty() && iris.is_empty() && graph_ops.is_empty() {
            return Ok(read_ts);
        }

        let (commit_ts, _guard) = store.oracle().begin_commit();
        debug!("tx commit: read_ts={} commit_ts={}", read_ts.0, commit_ts.0);

        let writes: Vec<PendingWrite> = write_buffer
            .into_iter()
            .zip(write_meta)
            .map(|(triple, (graph, mode))| PendingWrite {
                triple,
                graph,
                mode,
            })
            .collect();

        // ── conflict check (quads only) ───────────────────────────────────────
        // Edge annotations don't participate; they use additive append-only
        // semantics. One raw iterator is reused for every check.
        {
            let gspo_cf = store.cf_handle(Order::Gspo.cf())?;
            let mut gspo = store.db_ref().raw_iterator_cf(&gspo_cf);
            for w in &writes {
                if let Some(conflict) = check_conflict(&store, &mut gspo, w, read_ts, commit_ts)? {
                    return Err(StorageError::WriteConflict(conflict));
                }
            }
        }

        // ── build WriteBatch ──────────────────────────────────────────────────
        let mut batch = WriteBatch::default();
        let mut staged = Vec::new();
        store.stage_writes(&mut batch, &writes, commit_ts, &mut staged)?;
        store.batch_change(&mut batch, commit_ts, &author, &graph_ops, &staged)?;

        // IRI dictionary entries — checked and written under the commit lock,
        // so concurrent bindings of one node are serialized.
        let mut pending_iris = HashMap::new();
        for iri in &iris {
            store.batch_iri(&mut batch, iri, &mut pending_iris)?;
        }

        // Persist oracle counter so restarts don't reuse timestamps.
        let meta_cf = store.cf_handle(cf::META)?;
        batch.put_cf(&meta_cf, META_ORACLE_CTR, commit_ts.0.to_be_bytes());

        store.db_write(batch)?;
        store.notify_commit(commit_ts);
        Ok(commit_ts)
    }
}

/// Returns `Some(ConflictError)` if a version committed in
/// `(read_ts, commit_ts]` exists for what `w` writes: the exact quad, or — for
/// a `Replace` property write — any value of its `(subject, predicate, graph)`.
fn check_conflict(
    store: &TripleStore,
    gspo: &mut rocksdb::DBRawIteratorWithThreadMode<'_, crate::store::DB>,
    w: &PendingWrite,
    read_ts: Timestamp,
    commit_ts: Timestamp,
) -> Result<Option<ConflictError>, StorageError> {
    let s = w.triple.subject();
    let pred_str = w.triple.predicate().0.as_str();
    let Some(p) = store.predicate_id(pred_str) else {
        return Ok(None); // predicate not yet in store → no conflict
    };
    let prefix = match &w.triple {
        Triple::Relation { object, .. } => {
            Order::Gspo.prefix(Some(&s), Some(p), Some(object), Some(w.graph))
        }
        Triple::Property { value, .. } => match w.mode.resolve(&w.triple) {
            WriteMode::Replace => Order::Gspo.prefix(Some(&s), Some(p), None, Some(w.graph)),
            _ => Order::Gspo.prefix(
                Some(&s),
                Some(p),
                Some(&keys::value_object(value)),
                Some(w.graph),
            ),
        },
        Triple::EdgeProperty { .. } | Triple::EdgeRelation { .. } => return Ok(None),
    };

    gspo.seek(prefix);
    while let Some(key) = gspo.key() {
        if !key.starts_with(&prefix) {
            break;
        }
        let tt = keys::key_tt(key);
        if tt > read_ts && tt <= commit_ts {
            return Ok(Some(ConflictError {
                subject: s,
                predicate: pred_str.to_owned(),
            }));
        }
        gspo.next();
    }
    gspo.status()?;
    Ok(None)
}

// ── Snapshot ──────────────────────────────────────────────────────────────────

/// A read-only point-in-time view of the graph.
///
/// All scans return only triples committed at or before `ts`, with each
/// (subject, predicate, object) deduplicated to its latest version.
///
/// `vt_as_of`: scans filter to triples whose valid-time window covers `t`
/// (`vt_start ≤ t < vt_end`). Defaults to "now" at construction time, so a
/// fresh `Snapshot` shows current state — triples closed by `DELETE` (which
/// works by closing their valid-time window, see
/// `polargraph_query::cypher::execute_write_ops`) are correctly hidden.
/// Call `with_vt_as_of` to pin the snapshot to a different point in valid
/// time for historical/time-travel queries.
pub struct Snapshot {
    store: TripleStore,
    pub ts: Timestamp,
    /// Valid-time point-in-time filter (unix µs). Always `Some` — defaults to
    /// "now" so closed (deleted) triples aren't visible unless a caller
    /// explicitly asks for a historical point via `with_vt_as_of`.
    pub vt_as_of: Option<i64>,
    /// Graphs this snapshot may read (`None` = every graph). Enforced in the
    /// scan itself, before values are decoded — see [`Self::with_readable_graphs`].
    readable: Option<std::sync::Arc<roaring::RoaringBitmap>>,
}

impl Snapshot {
    pub(crate) fn new(store: TripleStore, ts: Timestamp) -> Self {
        Self {
            store,
            ts,
            vt_as_of: Some(Timestamp::now().0),
            readable: None,
        }
    }

    /// Restrict every read of this snapshot to the graphs in `readable`
    /// (graph ids). Quads in other graphs are skipped inside the scan, so they
    /// never reach query evaluation, filters or projections.
    pub fn with_readable_graphs(
        mut self,
        readable: std::sync::Arc<roaring::RoaringBitmap>,
    ) -> Self {
        self.readable = Some(readable);
        self
    }

    /// This snapshot restricted to the graphs in `included` (a query's
    /// dataset). Composes with [`Self::with_readable_graphs`] (intersection).
    pub fn within_graphs(
        mut self,
        included: impl IntoIterator<Item = polargraph_core::id::GraphId>,
    ) -> Self {
        let included: roaring::RoaringBitmap = included.into_iter().map(|g| g.0).collect();
        let readable = match self.readable.take() {
            Some(r) => &*r & &included,
            None => included,
        };
        self.readable = Some(std::sync::Arc::new(readable));
        self
    }

    /// This snapshot without the graphs in `excluded` (e.g. the inferred
    /// graphs, for a request that opts out of inferred facts). Composes with
    /// [`Self::with_readable_graphs`].
    pub fn without_graphs(mut self, excluded: &roaring::RoaringBitmap) -> Self {
        if excluded.is_empty() {
            return self;
        }
        let mut readable = match self.readable.take() {
            Some(r) => (*r).clone(),
            None => {
                let mut all: roaring::RoaringBitmap =
                    self.store.list_graphs().iter().map(|(g, _)| g.0).collect();
                all.insert(polargraph_core::id::GraphId::DEFAULT.0);
                all
            }
        };
        readable -= excluded;
        self.readable = Some(std::sync::Arc::new(readable));
        self
    }

    /// The readable-graph restriction, if any.
    pub fn readable_graphs(&self) -> Option<&roaring::RoaringBitmap> {
        self.readable.as_deref()
    }

    /// Whether this snapshot may read graph `g`.
    pub fn can_read_graph(&self, g: GraphId) -> bool {
        crate::store::readable_admits(self.readable_graphs(), g)
    }

    fn read_at(&self) -> crate::store::ReadAt {
        crate::store::ReadAt {
            ts: self.ts,
            vt_as_of: self.vt_as_of,
            readable: self.readable.clone(),
        }
    }

    /// Pin the valid-time filter to a specific point in time (for historical /
    /// time-travel queries).
    ///
    /// All subsequent scans will restrict results to triples whose valid-time
    /// window `[vt_start, vt_end)` contains `vt` (i.e. `vt_start ≤ vt < vt_end`).
    pub fn with_vt_as_of(mut self, vt: i64) -> Self {
        self.vt_as_of = Some(vt);
        self
    }

    pub fn scan_by_subject(&self, subject: &NodeId) -> Result<Vec<Triple>, StorageError> {
        self.store.scan_by_subject_at(subject, &self.read_at())
    }

    pub fn scan_by_subject_predicate(
        &self,
        subject: &NodeId,
        predicate: &str,
    ) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_subject_predicate_at(subject, predicate, &self.read_at())
    }

    pub fn scan_by_predicate(&self, predicate: &str) -> Result<Vec<Triple>, StorageError> {
        self.store.scan_by_predicate_at(predicate, &self.read_at())
    }

    pub fn scan_by_predicate_object(
        &self,
        predicate: &str,
        object: &NodeId,
    ) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_predicate_object_at(predicate, object, &self.read_at())
    }

    pub fn scan_by_object(&self, object: &NodeId) -> Result<Vec<Triple>, StorageError> {
        self.store.scan_by_object_at(object, &self.read_at())
    }

    pub fn scan_by_subject_object(
        &self,
        subject: &NodeId,
        object: &NodeId,
    ) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_subject_object_at(subject, object, &self.read_at())
    }

    pub fn scan_all(&self) -> Result<Vec<Triple>, StorageError> {
        self.store.scan_all_at(&self.read_at())
    }

    /// Property triples of `predicate` whose value equals `value` — an index
    /// lookup on the value hash (any graph).
    pub fn scan_by_predicate_value(
        &self,
        predicate: &str,
        value: &Value,
    ) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_predicate_value_at(predicate, value, &self.read_at())
    }

    /// Triples matching the bound slots within `scope`, each with its graph
    /// (see [`crate::GraphScope`]). `object` may be a node or a property's
    /// value hash ([`crate::keys::value_object`]).
    pub fn scan_scoped(
        &self,
        subject: Option<&NodeId>,
        predicate: Option<&str>,
        object: Option<&NodeId>,
        scope: &crate::store::GraphScope,
    ) -> Result<Vec<(GraphId, Triple)>, StorageError> {
        self.store
            .scan_scoped_at(subject, predicate, object, scope, &self.read_at())
    }

    /// The store this snapshot reads (for graph ↔ node lookups).
    pub fn store(&self) -> &TripleStore {
        &self.store
    }

    /// Every triple in graph `g`.
    pub fn scan_graph(&self, g: GraphId) -> Result<Vec<Triple>, StorageError> {
        self.store.scan_graph_at(g, &self.read_at())
    }

    /// Triples of `subject` in graph `g`.
    pub fn scan_by_subject_in_graph(
        &self,
        g: GraphId,
        subject: &NodeId,
    ) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_subject_in_graph_at(g, subject, &self.read_at())
    }

    /// Return `NodeId`s confirmed to have a live text value for `predicate` containing `query`.
    /// Values longer than [`crate::store::TRIGRAM_MAX_TEXT_BYTES`] are not indexed and never match.
    pub fn text_search(
        &self,
        predicate: &str,
        query: &str,
    ) -> Result<Vec<polargraph_core::id::NodeId>, StorageError> {
        self.store.text_search_at(predicate, query, &self.read_at())
    }

    /// Return all annotations on `edge` as of this snapshot's timestamp.
    pub fn scan_edge_annotations(
        &self,
        edge: polargraph_core::id::EdgeId,
    ) -> Result<Vec<crate::store::EdgeAnnotation>, StorageError> {
        self.store
            .scan_edge_annotations_with(edge, self.ts, self.readable_graphs())
    }

    /// Return edge property annotations for `predicate` as `Triple::EdgeProperty` variants.
    pub fn scan_annotations_by_predicate(
        &self,
        predicate: &str,
    ) -> Result<Vec<polargraph_core::triple::Triple>, StorageError> {
        self.store
            .scan_annotations_by_predicate_with(predicate, self.ts, self.readable_graphs())
    }

    /// Return all annotations on `edge` as Triple variants.
    pub fn scan_edge_annotations_as_triples(
        &self,
        edge: polargraph_core::id::EdgeId,
    ) -> Result<Vec<polargraph_core::triple::Triple>, StorageError> {
        self.store
            .scan_edge_annotations_as_triples_with(edge, self.ts, self.readable_graphs())
    }
}
