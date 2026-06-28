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
//! Conflicts: if any (subject, predicate, object) in the write buffer has a
//! version in storage with `tt > read_ts`, another transaction beat us to it.
//! We abort rather than silently overwrite.
//!
//! # Timestamp oracle
//!
//! A single `AtomicI64` serves as the committed timestamp.  Read timestamps
//! snapshot it without touching it.  Commits acquire a `Mutex<()>` to
//! serialize, increment the counter, write all triples with the new value as
//! `tt`, then release the lock.  The counter persists to the META CF so
//! restarts pick up where they left off.

use crate::{
    cf, codec,
    error::StorageError,
    keys,
    store::{encode_epo_value, TripleStore},
};
use polargraph_core::{
    id::NodeId,
    temporal::{BiTemporalRange, Timestamp},
    triple::Triple,
    value::Value,
};
use rocksdb::{Direction, IteratorMode, WriteBatch};
use std::{
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

// ── Transaction ───────────────────────────────────────────────────────────────

/// An in-progress read-write transaction.
///
/// Reads see all data committed at or before `read_ts`.
/// Writes are buffered until `commit()` or dropped on rollback.
pub struct Transaction {
    pub(crate) store: TripleStore,
    pub read_ts: Timestamp,
    write_buffer: Vec<Triple>,
}

impl Transaction {
    pub(crate) fn new(store: TripleStore, read_ts: Timestamp) -> Self {
        Self {
            store,
            read_ts,
            write_buffer: Vec::new(),
        }
    }

    /// Buffer a triple for insertion at commit time.
    ///
    /// The triple's existing `tt` is ignored; the actual commit timestamp is
    /// assigned at `commit()`.
    pub fn insert(&mut self, triple: Triple) {
        self.write_buffer.push(triple);
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
        self.store.scan_by_subject_at(subject, self.read_ts, None)
    }

    pub fn scan_by_subject_predicate(
        &self,
        subject: &NodeId,
        predicate: &str,
    ) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_subject_predicate_at(subject, predicate, self.read_ts, None)
    }

    pub fn scan_by_predicate(&self, predicate: &str) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_predicate_at(predicate, self.read_ts, None)
    }

    pub fn scan_by_predicate_object(
        &self,
        predicate: &str,
        object: &NodeId,
    ) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_predicate_object_at(predicate, object, self.read_ts, None)
    }

    pub fn scan_by_object(&self, object: &NodeId) -> Result<Vec<Triple>, StorageError> {
        self.store.scan_by_object_at(object, self.read_ts, None)
    }

    pub fn scan_by_subject_object(
        &self,
        subject: &NodeId,
        object: &NodeId,
    ) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_subject_object_at(subject, object, self.read_ts, None)
    }

    pub fn scan_all(&self) -> Result<Vec<Triple>, StorageError> {
        self.store.scan_all_at(self.read_ts, None)
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
        if self.write_buffer.is_empty() {
            return Ok(self.read_ts);
        }

        let (commit_ts, _guard) = self.store.oracle().begin_commit();
        debug!(
            "tx commit: read_ts={} commit_ts={}",
            self.read_ts.0, commit_ts.0
        );

        // ── conflict check (hexastore triples only) ───────────────────────────
        for triple in &self.write_buffer {
            match triple {
                Triple::Relation { .. } | Triple::Property { .. } => {
                    if let Some(conflict) = self.check_conflict(triple, commit_ts)? {
                        return Err(StorageError::WriteConflict(conflict));
                    }
                }
                // Edge annotations don't participate in hexastore conflict
                // checking; they use additive append-only semantics.
                Triple::EdgeProperty { .. } | Triple::EdgeRelation { .. } => {}
            }
        }

        // ── build WriteBatch ──────────────────────────────────────────────────
        let mut batch = WriteBatch::default();

        for triple in &self.write_buffer {
            let temporal_stamped = stamp_temporal(triple.temporal(), commit_ts);
            let pred_id = self.store.intern_predicate(triple.predicate().0.as_str())?;

            match triple {
                Triple::Relation { .. } | Triple::Property { .. } => {
                    self.store.batch_triple(
                        &mut batch,
                        triple.subject(),
                        pred_id,
                        object_of(triple),
                        commit_ts,
                        &encode_value(triple, &temporal_stamped)?,
                    )?;
                    // Trigram index: write TRI CF entries for text property values.
                    if let Triple::Property {
                        value: Value::Text(text),
                        ..
                    } = triple
                    {
                        self.store.batch_text_trigrams(
                            &mut batch,
                            &triple.subject(),
                            pred_id,
                            text,
                        )?;
                    }
                }
                Triple::EdgeProperty { edge, value, .. } => {
                    let value_bytes = codec::encode_property(value, &temporal_stamped)?;
                    self.store
                        .batch_epa(&mut batch, *edge, pred_id, commit_ts, &value_bytes)?;
                }
                Triple::EdgeRelation { edge, object, .. } => {
                    let epo_val = encode_epo_value(&temporal_stamped);
                    self.store
                        .batch_epo(&mut batch, *edge, pred_id, *object, commit_ts, &epo_val)?;
                }
            }
        }

        // Persist oracle counter so restarts don't reuse timestamps.
        let meta_cf = self.store.cf_handle(cf::META)?;
        batch.put_cf(&meta_cf, META_ORACLE_CTR, commit_ts.0.to_be_bytes());

        self.store.db_write(batch)?;
        Ok(commit_ts)
    }

    // ── private ───────────────────────────────────────────────────────────────

    /// Returns `Some(ConflictError)` if a newer version exists in storage.
    fn check_conflict(
        &self,
        triple: &Triple,
        commit_ts: Timestamp,
    ) -> Result<Option<ConflictError>, StorageError> {
        let s = triple.subject();
        let pred_str = triple.predicate().0.as_str();
        let p = match self.store.predicate_id(pred_str) {
            Some(id) => id,
            None => return Ok(None), // predicate not yet in store → no conflict
        };
        let o = object_of(triple);

        // Scan the SPO key range for this exact (S,P,O) and check if any
        // entry has tt in (read_ts, commit_ts].
        let prefix = keys::encode_spo(&s, p, &o, Timestamp(0));
        // We use the full 44-byte key up to (but not including) the tt part
        // as our prefix — that's the first 36 bytes.
        let spo_prefix = &prefix[..36];

        let cf = self.store.cf_handle(cf::SPO)?;
        let db = self.store.db_ref();
        let iter = db.iterator_cf(&cf, IteratorMode::From(spo_prefix, Direction::Forward));

        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(spo_prefix) {
                break;
            }
            let decoded = keys::decode_spo(&key)?;
            if decoded.tt > self.read_ts && decoded.tt <= commit_ts {
                return Ok(Some(ConflictError {
                    subject: s,
                    predicate: pred_str.to_owned(),
                }));
            }
        }
        Ok(None)
    }
}

// ── Snapshot ──────────────────────────────────────────────────────────────────

/// A read-only point-in-time view of the graph.
///
/// All scans return only triples committed at or before `ts`, with each
/// (subject, predicate, object) deduplicated to its latest version.
///
/// `vt_as_of`: when `Some(t)`, scans additionally filter to triples whose
/// valid-time window covers `t` (`vt_start ≤ t < vt_end`).  `None` means no
/// valid-time filter — all triples visible at `ts` are returned regardless of
/// their valid-time range.
pub struct Snapshot {
    store: TripleStore,
    pub ts: Timestamp,
    /// Optional valid-time point-in-time filter (unix µs).
    pub vt_as_of: Option<i64>,
}

impl Snapshot {
    pub(crate) fn new(store: TripleStore, ts: Timestamp) -> Self {
        Self {
            store,
            ts,
            vt_as_of: None,
        }
    }

    /// Set the valid-time filter for this snapshot.
    ///
    /// All subsequent scans will restrict results to triples whose valid-time
    /// window `[vt_start, vt_end)` contains `vt` (i.e. `vt_start ≤ vt < vt_end`).
    pub fn with_vt_as_of(mut self, vt: i64) -> Self {
        self.vt_as_of = Some(vt);
        self
    }

    pub fn scan_by_subject(&self, subject: &NodeId) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_subject_at(subject, self.ts, self.vt_as_of)
    }

    pub fn scan_by_subject_predicate(
        &self,
        subject: &NodeId,
        predicate: &str,
    ) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_subject_predicate_at(subject, predicate, self.ts, self.vt_as_of)
    }

    pub fn scan_by_predicate(&self, predicate: &str) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_predicate_at(predicate, self.ts, self.vt_as_of)
    }

    pub fn scan_by_predicate_object(
        &self,
        predicate: &str,
        object: &NodeId,
    ) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_predicate_object_at(predicate, object, self.ts, self.vt_as_of)
    }

    pub fn scan_by_object(&self, object: &NodeId) -> Result<Vec<Triple>, StorageError> {
        self.store.scan_by_object_at(object, self.ts, self.vt_as_of)
    }

    pub fn scan_by_subject_object(
        &self,
        subject: &NodeId,
        object: &NodeId,
    ) -> Result<Vec<Triple>, StorageError> {
        self.store
            .scan_by_subject_object_at(subject, object, self.ts, self.vt_as_of)
    }

    pub fn scan_all(&self) -> Result<Vec<Triple>, StorageError> {
        self.store.scan_all_at(self.ts, self.vt_as_of)
    }

    /// Return `NodeId`s confirmed to have a live text value for `predicate` containing `query`.
    pub fn text_search(
        &self,
        predicate: &str,
        query: &str,
    ) -> Result<Vec<polargraph_core::id::NodeId>, StorageError> {
        self.store
            .text_search(predicate, query, self.ts, self.vt_as_of)
    }

    /// Return all annotations on `edge` as of this snapshot's timestamp.
    pub fn scan_edge_annotations(
        &self,
        edge: polargraph_core::id::EdgeId,
    ) -> Result<Vec<crate::store::EdgeAnnotation>, StorageError> {
        self.store.scan_edge_annotations(edge, self.ts)
    }

    /// Return edge property annotations for `predicate` as `Triple::EdgeProperty` variants.
    pub fn scan_annotations_by_predicate(
        &self,
        predicate: &str,
    ) -> Result<Vec<polargraph_core::triple::Triple>, StorageError> {
        self.store.scan_annotations_by_predicate(predicate, self.ts)
    }

    /// Return all annotations on `edge` as Triple variants.
    pub fn scan_edge_annotations_as_triples(
        &self,
        edge: polargraph_core::id::EdgeId,
    ) -> Result<Vec<polargraph_core::triple::Triple>, StorageError> {
        self.store.scan_edge_annotations_as_triples(edge, self.ts)
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Return the object NodeId for a hexastore triple, using the property sentinel
/// for property triples. Edge annotations are not routed through this helper.
fn object_of(triple: &Triple) -> NodeId {
    match triple {
        Triple::Relation { object, .. } => *object,
        Triple::Property { .. } | Triple::EdgeProperty { .. } | Triple::EdgeRelation { .. } => {
            // Property triples and edge annotations use the sentinel.
            // (EdgeProperty/EdgeRelation are handled separately before this
            // function would be reached, but we need an arm to satisfy Rust.)
            NodeId(uuid::Uuid::from_bytes(keys::PROPERTY_SENTINEL))
        }
    }
}

/// Produce a BiTemporalRange with the commit timestamp stamped as `tt`,
/// preserving the valid-time range from the original triple.
fn stamp_temporal(original: &BiTemporalRange, commit_ts: Timestamp) -> BiTemporalRange {
    BiTemporalRange {
        vt_start: original.vt_start,
        vt_end: original.vt_end,
        tt: commit_ts,
    }
}

/// Encode the RocksDB value bytes for a hexastore triple given a stamped temporal range.
///
/// Edge annotations (`EdgeProperty`/`EdgeRelation`) are handled separately in
/// `commit()` and should never be passed here; returns an empty vec for them.
fn encode_value(triple: &Triple, temporal: &BiTemporalRange) -> Result<Vec<u8>, StorageError> {
    match triple {
        Triple::Relation { edge_id, .. } => Ok(codec::encode_relation(edge_id, temporal)),
        Triple::Property { value, .. } => codec::encode_property(value, temporal),
        // Edge annotations routed to EPA/EPO — not encoded via this function.
        Triple::EdgeProperty { .. } | Triple::EdgeRelation { .. } => Ok(vec![]),
    }
}
