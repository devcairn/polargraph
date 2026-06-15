//! TripleStore — the main storage handle.
//!
//! # Public API (Phase 2)
//!
//! ```text
//! let store = TripleStore::open(path)?;
//!
//! // Single-triple convenience (auto-commits a one-triple transaction).
//! store.insert(&triple)?;
//!
//! // Multi-triple transaction with conflict detection.
//! let mut tx = store.begin();
//! tx.insert(triple_a);
//! tx.insert(triple_b);
//! let commit_ts = tx.commit()?;          // Err(StorageError::WriteConflict) if racing
//!
//! // Point-in-time snapshot read.
//! let snap = store.snapshot(commit_ts);
//! let triples = snap.scan_by_subject(&node_id)?;
//! ```
//!
//! # Internal responsibilities
//!
//! - Open RocksDB with 7 column families (6 triple indexes + META).
//! - Predicate interning: string ↔ u32 ID, persisted to META CF.
//! - `batch_triple`: write one logical triple into all 6 indexes via a
//!   caller-supplied WriteBatch (used by `insert` and `Transaction::commit`).
//! - Snapshot scan helpers (`scan_by_*_at`): prefix-scan + filter to
//!   `tt <= snapshot_ts` + deduplicate (S,P,O) to latest version.

use crate::{
    cf,
    codec::{self, DecodedValue},
    error::StorageError,
    hnsw::{self, HnswIndex, MmapState},
    keys::{self, PredId},
    mvcc::{Snapshot, TimestampOracle, Transaction, META_ORACLE_CTR},
};
use polargraph_core::{
    id::{EdgeId, NodeId},
    schema::StorageMode,
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode,
    MultiThreaded, Options, WriteBatch,
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use tracing::info;

// ── RDF-star annotation types ─────────────────────────────────────────────────

/// The value carried by an edge annotation — either a scalar or a node reference.
#[derive(Debug, Clone)]
pub enum EdgeAnnotationValue {
    Scalar(Value),
    Node(NodeId),
}

/// A single annotation on an edge triple (RDF-star).
#[derive(Debug, Clone)]
pub struct EdgeAnnotation {
    pub predicate: Predicate,
    pub value: EdgeAnnotationValue,
}

/// Encode the EPO column family value: `[vt_start BE(8)][vt_end BE(8)]`.
pub(crate) fn encode_epo_value(temporal: &BiTemporalRange) -> [u8; 16] {
    let mut v = [0u8; 16];
    v[0..8].copy_from_slice(&temporal.vt_start.to_be_bytes());
    v[8..16].copy_from_slice(&temporal.vt_end.to_be_bytes());
    v
}

// ── Store mode ────────────────────────────────────────────────────────────────

/// Whether this `TripleStore` is the primary read-write instance or a replica
/// that receives changes via WAL streaming replication from the primary.
///
/// In replica mode the underlying RocksDB is opened read-write so that the
/// replication client can apply incoming write batches, but all public write
/// APIs on `TripleStore` return `StorageError::ReadOnly` so application code
/// cannot write through the replica directly.
#[derive(Clone, Debug)]
pub enum StoreMode {
    Primary,
    /// gRPC address of the primary (e.g. `"http://192.168.1.10:50051"`).
    Replica { primary_address: String },
}

type DB = DBWithThreadMode<MultiThreaded>;

// META key namespace for predicate table.
const META_PRED_PREFIX: &[u8] = b"p/";
const META_PRED_CTR: &[u8] = b"__pred_ctr";

fn meta_forward_key(pred: &str) -> Vec<u8> {
    let mut k = META_PRED_PREFIX.to_vec();
    k.extend_from_slice(pred.as_bytes());
    k
}
fn meta_reverse_key(id: PredId) -> Vec<u8> {
    let mut k = b"pid/".to_vec();
    k.extend_from_slice(&id.to_be_bytes());
    k
}

// ── public handle ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct TripleStore {
    inner: Arc<Inner>,
}

struct Inner {
    db: DB,
    oracle: TimestampOracle,
    fwd: RwLock<HashMap<String, PredId>>, // predicate → id
    rev: RwLock<HashMap<PredId, String>>, // id → predicate
    next_pred_id: RwLock<PredId>,
    /// Named HNSW spaces: space_name → index.
    hnsw_spaces: RwLock<HashMap<String, HnswIndex>>,
    /// Path passed to `open()`, used to locate mmap `.vecs` files.
    data_dir: PathBuf,
    /// Whether this is a primary or replica instance.
    mode: StoreMode,
}

// META key for persisting the last replicated WAL sequence number.
const META_LAST_REPL_SEQ: &[u8] = b"__replication__/last_seq";

impl TripleStore {
    // ── lifecycle ─────────────────────────────────────────────────────────────

    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        // Keep WAL files for up to 1 hour so replicas can catch up after a gap.
        db_opts.set_wal_ttl_seconds(3600);
        db_opts.set_wal_size_limit_mb(512);

        let cf_descriptors: Vec<ColumnFamilyDescriptor> = cf::ALL
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();

        let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors)?;

        // Ensure the vectors/ subdirectory exists for any mmap spaces.
        let vectors_dir = path.join("vectors");
        std::fs::create_dir_all(&vectors_dir)?;

        let (fwd, rev, next_pred_id) = Self::load_predicates(&db)?;
        let oracle_ts = Self::load_oracle_ts(&db)?;
        let oracle = TimestampOracle::new(oracle_ts);
        let hnsw_spaces = Self::load_hnsw_spaces(&db, &vectors_dir)?;

        let total_nodes: usize = hnsw_spaces.values().map(|i| i.len()).sum();
        info!(
            path = %path.display(),
            predicates = fwd.len(),
            oracle_ts,
            hnsw_spaces = hnsw_spaces.len(),
            hnsw_nodes = total_nodes,
            "TripleStore opened"
        );

        Ok(Self {
            inner: Arc::new(Inner {
                db,
                oracle,
                fwd: RwLock::new(fwd),
                rev: RwLock::new(rev),
                next_pred_id: RwLock::new(next_pred_id),
                hnsw_spaces: RwLock::new(hnsw_spaces),
                data_dir: path.to_path_buf(),
                mode: StoreMode::Primary,
            }),
        })
    }

    /// Open a replica store at `path`.
    ///
    /// Identical to `open()` except the mode is set to `Replica`. The
    /// underlying RocksDB is opened read-write so the replication client can
    /// apply incoming write batches, but all public write APIs return
    /// `StorageError::ReadOnly`.
    ///
    /// `primary_address` is the gRPC endpoint of the primary, e.g.
    /// `"http://192.168.1.10:50051"`. It is stored for use by the caller to
    /// create a `WalReplicationClient`.
    pub fn open_as_replica(path: &Path, primary_address: String) -> Result<Self, StorageError> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_wal_ttl_seconds(3600);
        db_opts.set_wal_size_limit_mb(512);

        let cf_descriptors: Vec<ColumnFamilyDescriptor> = cf::ALL
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();

        let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors)?;

        let vectors_dir = path.join("vectors");
        std::fs::create_dir_all(&vectors_dir)?;

        let (fwd, rev, next_pred_id) = Self::load_predicates(&db)?;
        let oracle_ts = Self::load_oracle_ts(&db)?;
        let oracle = TimestampOracle::new(oracle_ts);
        let hnsw_spaces = Self::load_hnsw_spaces(&db, &vectors_dir)?;

        let total_nodes: usize = hnsw_spaces.values().map(|i| i.len()).sum();
        info!(
            path = %path.display(),
            primary_address = %primary_address,
            predicates = fwd.len(),
            oracle_ts,
            hnsw_spaces = hnsw_spaces.len(),
            hnsw_nodes = total_nodes,
            "TripleStore opened as replica"
        );

        Ok(Self {
            inner: Arc::new(Inner {
                db,
                oracle,
                fwd: RwLock::new(fwd),
                rev: RwLock::new(rev),
                next_pred_id: RwLock::new(next_pred_id),
                hnsw_spaces: RwLock::new(hnsw_spaces),
                data_dir: path.to_path_buf(),
                mode: StoreMode::Replica { primary_address },
            }),
        })
    }

    /// Return `true` if this store is a replica (write ops blocked at the API level).
    pub fn is_replica(&self) -> bool {
        matches!(self.inner.mode, StoreMode::Replica { .. })
    }

    /// Return the primary's gRPC address if this is a replica; `None` otherwise.
    pub fn primary_address(&self) -> Option<&str> {
        match &self.inner.mode {
            StoreMode::Replica { primary_address } => Some(primary_address.as_str()),
            StoreMode::Primary => None,
        }
    }

    /// Apply a raw WAL batch received from the primary.
    ///
    /// Writes the deserialized `WriteBatch` directly to RocksDB (bypassing the
    /// replica write guard), persists `seq` as the new `last_applied_seq`, and
    /// refreshes the in-memory predicate map and oracle so that subsequent reads
    /// reflect the newly written data.
    ///
    /// Only valid on a replica store; returns `StorageError::ReadOnly` otherwise.
    pub fn apply_replicated_batch(&self, seq: u64, batch_data: &[u8]) -> Result<(), StorageError> {
        if !self.is_replica() {
            return Err(StorageError::ReadOnly(
                "apply_replicated_batch is only valid on a replica".into(),
            ));
        }

        // Apply the raw write batch from the primary.
        let batch = WriteBatch::from_data(batch_data);
        self.inner.db.write(batch)?;

        // Persist the sequence number so we can resume after a restart.
        let meta_cf = self.cf_handle(cf::META)?;
        let mut seq_batch = WriteBatch::default();
        seq_batch.put_cf(&meta_cf, META_LAST_REPL_SEQ, seq.to_be_bytes());
        self.inner.db.write(seq_batch)?;

        // Refresh in-memory structures that the write batch may have updated.
        let (fwd, rev, next_pred_id) = Self::load_predicates(&self.inner.db)?;
        let oracle_ts = Self::load_oracle_ts(&self.inner.db)?;
        *self.inner.fwd.write().unwrap() = fwd;
        *self.inner.rev.write().unwrap() = rev;
        *self.inner.next_pred_id.write().unwrap() = next_pred_id;
        self.inner.oracle.advance_to(polargraph_core::temporal::Timestamp(oracle_ts));

        Ok(())
    }

    /// Return the last WAL sequence number applied by this replica.
    ///
    /// Reads `__replication__/last_seq` from the META CF. Returns 0 if no
    /// batches have been applied yet.
    pub fn last_applied_seq(&self) -> u64 {
        let meta_cf = match self.cf_handle(cf::META) {
            Ok(cf) => cf,
            Err(_) => return 0,
        };
        match self.inner.db.get_cf(&meta_cf, META_LAST_REPL_SEQ) {
            Ok(Some(v)) if v.len() == 8 => u64::from_be_bytes(v[..8].try_into().unwrap()),
            _ => 0,
        }
    }

    /// Return the current RocksDB WAL sequence number.
    ///
    /// Used by the primary to report replication lag in `ReplicaStatus`.
    pub fn latest_sequence_number(&self) -> u64 {
        self.inner.db.latest_sequence_number()
    }

    fn read_only_err() -> StorageError {
        StorageError::ReadOnly(
            "write operations are not supported on a read replica".to_string(),
        )
    }

    /// Provides read access to the underlying RocksDB instance.
    ///
    /// Used by `BackupManager` to pass the live DB to `BackupEngine::create_new_backup_flush`.
    pub(crate) fn with_db<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&DB) -> R,
    {
        f(&self.inner.db)
    }

    #[allow(clippy::type_complexity)]
    fn load_predicates(
        db: &DB,
    ) -> Result<(HashMap<String, PredId>, HashMap<PredId, String>, PredId), StorageError> {
        let mut fwd = HashMap::new();
        let mut rev = HashMap::new();

        let meta_cf = db
            .cf_handle(cf::META)
            .ok_or_else(|| StorageError::MissingCf(cf::META.into()))?;

        let iter = db.iterator_cf(&meta_cf, IteratorMode::From(b"p/", Direction::Forward));
        for item in iter {
            let (k, v) = item?;
            if !k.starts_with(b"p/") {
                break;
            }
            let name = std::str::from_utf8(&k[2..])
                .map_err(|e| StorageError::KeyDecode(e.to_string()))?
                .to_owned();
            if v.len() != 4 {
                return Err(StorageError::KeyDecode(format!("bad predicate value for {name}")));
            }
            let id = u32::from_be_bytes(v[..4].try_into().unwrap());
            fwd.insert(name.clone(), id);
            rev.insert(id, name);
        }

        let next_pred_id = match db.get_cf(&meta_cf, META_PRED_CTR)? {
            Some(v) if v.len() == 4 => u32::from_be_bytes(v[..4].try_into().unwrap()),
            _ => 1,
        };

        Ok((fwd, rev, next_pred_id))
    }

    /// Load all named HNSW spaces from the `hnsw` CF.
    ///
    /// If a `.vecs` file exists at `vectors_dir/<space>.vecs`, the space is
    /// loaded in mmap mode; otherwise it is loaded into memory as before.
    ///
    /// Key format:
    ///   `<space>/__ep`      → entry-point record
    ///   `<space>/n/<16 b>`  → serialised node (memory or mmap format)
    fn load_hnsw_spaces(
        db: &DB,
        vectors_dir: &Path,
    ) -> Result<HashMap<String, HnswIndex>, StorageError> {
        let hnsw_cf = db
            .cf_handle(cf::HNSW)
            .ok_or_else(|| StorageError::MissingCf(cf::HNSW.into()))?;

        // Phase 1: scan for EP keys → discover space names.
        let mut ep_data: Vec<(String, NodeId, usize)> = Vec::new();
        let iter = db.iterator_cf(&hnsw_cf, IteratorMode::Start);
        for item in iter {
            let (key, value) = item?;
            if let Some(space) = hnsw::parse_ep_key(&key) {
                let (ep_id, ep_layer) = hnsw::deserialize_entry_point(&value)?;
                ep_data.push((space, ep_id, ep_layer));
            }
        }

        // Phase 2: for each space, decide memory vs mmap, then load nodes.
        let mut spaces: HashMap<String, HnswIndex> = HashMap::new();
        for (space, ep_id, ep_layer) in ep_data {
            let vecs_path = vectors_dir.join(format!("{space}.vecs"));
            let is_mmap   = vecs_path.exists();

            let mut idx = if is_mmap {
                let mmap_state = MmapState::open(vecs_path)?;
                HnswIndex::new_mmap_with_state(mmap_state)
            } else {
                HnswIndex::new()
            };
            idx.load_entry_point(ep_id, ep_layer);

            let node_prefix = hnsw::node_prefix_for_space(&space);
            let id_start    = node_prefix.len();

            let iter = db.iterator_cf(
                &hnsw_cf,
                IteratorMode::From(&node_prefix, Direction::Forward),
            );
            for item in iter {
                let (key, value) = item?;
                if !key.starts_with(&node_prefix) { break; }
                if key.len() < id_start + 16      { continue; }
                let id = NodeId(uuid::Uuid::from_bytes(
                    key[id_start..id_start + 16].try_into().unwrap(),
                ));
                let (node_vector, max_layer, neighbors) = hnsw::deserialize_node(&value)?;
                idx.load_node(id, node_vector, max_layer, neighbors);
            }

            spaces.insert(space, idx);
        }

        Ok(spaces)
    }

    fn load_oracle_ts(db: &DB) -> Result<i64, StorageError> {
        let meta_cf = db
            .cf_handle(cf::META)
            .ok_or_else(|| StorageError::MissingCf(cf::META.into()))?;
        match db.get_cf(&meta_cf, META_ORACLE_CTR)? {
            Some(v) if v.len() == 8 => Ok(i64::from_be_bytes(v[..8].try_into().unwrap())),
            _ => Ok(0),
        }
    }

    // ── transaction API ───────────────────────────────────────────────────────

    /// Begin a new read-write transaction. Reads see all data committed so far.
    pub fn begin(&self) -> Transaction {
        let read_ts = self.inner.oracle.read_ts();
        Transaction::new(self.clone(), read_ts)
    }

    /// Return a read-only snapshot at `ts`.
    pub fn snapshot(&self, ts: Timestamp) -> Snapshot {
        Snapshot::new(self.clone(), ts)
    }

    /// Convenience: insert a single triple in its own auto-committed transaction.
    pub fn insert(&self, triple: &Triple) -> Result<(), StorageError> {
        let mut tx = self.begin();
        tx.insert(triple.clone());
        tx.commit()?;
        Ok(())
    }

    // ── vector index ──────────────────────────────────────────────────────────

    /// Insert or update a node's embedding vector in the named HNSW space.
    ///
    /// `mode` controls vector storage for the space. It is applied only when
    /// the space is first created; subsequent calls with a different `mode`
    /// on an existing space are ignored (the mode cannot be changed after the
    /// first insert).
    ///
    /// All modified neighbor lists are flushed to the `hnsw` CF in a single
    /// `WriteBatch`.
    pub fn insert_vector(
        &self,
        space: &str,
        node_id: NodeId,
        vector: Vec<f32>,
        mode: StorageMode,
    ) -> Result<(), StorageError> {
        if self.is_replica() {
            return Err(Self::read_only_err());
        }
        let mut spaces = self.inner.hnsw_spaces.write().unwrap();
        let idx = spaces.entry(space.to_string()).or_insert_with(|| {
            match mode {
                StorageMode::Mmap => {
                    let path = self.inner.data_dir.join("vectors").join(format!("{space}.vecs"));
                    HnswIndex::new_mmap(path)
                }
                StorageMode::Memory => HnswIndex::new(),
            }
        });
        let modified = idx.insert(node_id, vector);

        let hnsw_cf = self.cf_handle(cf::HNSW)?;
        let mut batch = WriteBatch::default();

        for id in &modified {
            let serialized = idx.serialize_node_for(*id);
            if !serialized.is_empty() {
                batch.put_cf(&hnsw_cf, hnsw::node_key_for_space(space, *id), serialized);
            }
        }

        if let Some(ep_id) = idx.entry_point {
            if let Some(ep_node) = idx.nodes.get(&ep_id) {
                batch.put_cf(
                    &hnsw_cf,
                    hnsw::ep_key_for_space(space),
                    hnsw::serialize_entry_point(ep_id, ep_node.max_layer),
                );
            }
        }

        self.inner.db.write(batch)?;
        Ok(())
    }

    /// Return the number of named HNSW vector spaces in this store.
    pub fn hnsw_space_count(&self) -> usize {
        self.inner.hnsw_spaces.read().unwrap().len()
    }

    /// Search for the `k` nearest neighbors of `query` in a named HNSW space.
    ///
    /// Returns `(NodeId, cosine_similarity)` pairs ordered by decreasing similarity.
    /// Returns an empty vec if the space does not exist.
    pub fn search_vector(&self, space: &str, query: Vec<f32>, k: usize) -> Vec<(NodeId, f32)> {
        let spaces = self.inner.hnsw_spaces.read().unwrap();
        match spaces.get(space) {
            Some(idx) => idx.search(&query, k, 0),
            None => vec![],
        }
    }

    /// Like `search_vector` but with an explicit exploration factor `ef`.
    ///
    /// Use `ef > k` to trade latency for a larger candidate pool before post-filtering.
    pub fn search_vector_ef(
        &self,
        space: &str,
        query: &[f32],
        k: usize,
        ef: usize,
    ) -> Vec<(NodeId, f32)> {
        let spaces = self.inner.hnsw_spaces.read().unwrap();
        match spaces.get(space) {
            Some(idx) => idx.search(query, k, ef),
            None => vec![],
        }
    }

    /// Score every node in `allowed` against `query` using vectors in the named
    /// space; return top-k by cosine similarity.
    ///
    /// Nodes absent from the index are silently skipped.  O(|allowed|).
    pub fn search_vector_in_set(
        &self,
        space: &str,
        query: &[f32],
        k: usize,
        allowed: &[NodeId],
    ) -> Vec<(NodeId, f32)> {
        let spaces = self.inner.hnsw_spaces.read().unwrap();
        match spaces.get(space) {
            Some(idx) => idx.search_in_set(query, k, allowed),
            None => vec![],
        }
    }

    /// Insert multiple vectors into a named space in a single write.
    ///
    /// `mode` is applied on space creation (first insert); ignored for existing
    /// spaces. Returns `(count_inserted, per_item_errors)`.
    pub fn batch_insert_vectors(
        &self,
        space: &str,
        items: &[(NodeId, Vec<f32>)],
        mode: StorageMode,
    ) -> (usize, Vec<(usize, StorageError)>) {
        if self.is_replica() {
            return (0, vec![(0, Self::read_only_err())]);
        }
        let hnsw_cf = match self.cf_handle(cf::HNSW) {
            Ok(cf) => cf,
            Err(e) => return (0, vec![(0, e)]),
        };

        let mut spaces = self.inner.hnsw_spaces.write().unwrap();
        let idx = spaces.entry(space.to_string()).or_insert_with(|| {
            match mode {
                StorageMode::Mmap => {
                    let path = self.inner.data_dir.join("vectors").join(format!("{space}.vecs"));
                    HnswIndex::new_mmap(path)
                }
                StorageMode::Memory => HnswIndex::new(),
            }
        });

        let mut batch    = WriteBatch::default();
        let mut inserted = 0usize;
        let mut errors: Vec<(usize, StorageError)> = Vec::new();

        for (node_id, vector) in items.iter() {
            let modified = idx.insert(*node_id, vector.clone());
            for id in &modified {
                let serialized = idx.serialize_node_for(*id);
                if !serialized.is_empty() {
                    batch.put_cf(&hnsw_cf, hnsw::node_key_for_space(space, *id), serialized);
                }
            }
            if let Some(ep_id) = idx.entry_point {
                if let Some(ep_node) = idx.nodes.get(&ep_id) {
                    batch.put_cf(
                        &hnsw_cf,
                        hnsw::ep_key_for_space(space),
                        hnsw::serialize_entry_point(ep_id, ep_node.max_layer),
                    );
                }
            }
            inserted += 1;
        }

        if let Err(e) = self.inner.db.write(batch) {
            errors.push((0, StorageError::Rocks(e)));
            return (0, errors);
        }

        (inserted, errors)
    }

    // ── convenience scan methods (snapshot at current read_ts) ────────────────
    //
    // These are equivalent to `store.snapshot(store.begin().read_ts).scan_*()`.
    // Useful for tests and simple read paths that don't need a named snapshot.

    pub fn scan_by_subject(&self, subject: &NodeId) -> Result<Vec<Triple>, StorageError> {
        self.scan_by_subject_at(subject, self.inner.oracle.read_ts(), None)
    }

    pub fn scan_by_subject_predicate(
        &self,
        subject: &NodeId,
        predicate: &str,
    ) -> Result<Vec<Triple>, StorageError> {
        self.scan_by_subject_predicate_at(subject, predicate, self.inner.oracle.read_ts(), None)
    }

    pub fn scan_by_predicate(&self, predicate: &str) -> Result<Vec<Triple>, StorageError> {
        self.scan_by_predicate_at(predicate, self.inner.oracle.read_ts(), None)
    }

    pub fn scan_by_predicate_object(
        &self,
        predicate: &str,
        object: &NodeId,
    ) -> Result<Vec<Triple>, StorageError> {
        self.scan_by_predicate_object_at(predicate, object, self.inner.oracle.read_ts(), None)
    }

    pub fn scan_by_object(&self, object: &NodeId) -> Result<Vec<Triple>, StorageError> {
        self.scan_by_object_at(object, self.inner.oracle.read_ts(), None)
    }

    pub fn scan_by_subject_object(
        &self,
        subject: &NodeId,
        object: &NodeId,
    ) -> Result<Vec<Triple>, StorageError> {
        self.scan_by_subject_object_at(subject, object, self.inner.oracle.read_ts(), None)
    }

    pub fn scan_all(&self) -> Result<Vec<Triple>, StorageError> {
        self.scan_all_at(self.inner.oracle.read_ts(), None)
    }

    /// Fast approximate triple count using RocksDB's built-in key estimate on
    /// the SPO column family. Not exact — use for monitoring/health only.
    pub fn estimate_triple_count(&self) -> u64 {
        self.cf_handle(cf::SPO)
            .ok()
            .and_then(|cf| {
                self.inner
                    .db
                    .property_int_value_cf(&cf, "rocksdb.estimate-num-keys")
                    .ok()
                    .flatten()
            })
            .unwrap_or(0)
    }

    // ── diagnostics ──────────────────────────────────────────────────────────

    /// Current MVCC oracle timestamp (µs since Unix epoch).
    pub fn oracle_ts(&self) -> i64 {
        self.inner.oracle.read_ts().0
    }

    /// Number of interned predicates.
    pub fn predicate_count(&self) -> u32 {
        self.inner.fwd.read().unwrap().len() as u32
    }

    /// RocksDB estimated key count for a named column family.
    pub fn cf_approx_key_count(&self, cf_name: &str) -> u64 {
        self.cf_handle(cf_name)
            .ok()
            .and_then(|cf| {
                self.inner
                    .db
                    .property_int_value_cf(&cf, "rocksdb.estimate-num-keys")
                    .ok()
                    .flatten()
            })
            .unwrap_or(0)
    }

    /// RocksDB total SST file size (bytes) for a named column family.
    pub fn cf_approx_size_bytes(&self, cf_name: &str) -> u64 {
        self.cf_handle(cf_name)
            .ok()
            .and_then(|cf| {
                self.inner
                    .db
                    .property_int_value_cf(&cf, "rocksdb.total-sst-files-size")
                    .ok()
                    .flatten()
            })
            .unwrap_or(0)
    }

    /// Sum of SST file sizes (bytes) across all column families.
    pub fn db_total_sst_size_bytes(&self) -> u64 {
        cf::ALL.iter().map(|name| self.cf_approx_size_bytes(name)).sum()
    }

    /// Sum of active memtable sizes (bytes) across all column families.
    pub fn db_memtable_size_bytes(&self) -> u64 {
        cf::ALL
            .iter()
            .filter_map(|name| self.cf_handle(name).ok())
            .filter_map(|cf| {
                self.inner
                    .db
                    .property_int_value_cf(&cf, "rocksdb.cur-size-all-mem-tables")
                    .ok()
                    .flatten()
            })
            .sum()
    }

    /// Count of live SST files across all column families (sum of num-live-versions).
    pub fn db_live_sst_files(&self) -> u64 {
        cf::ALL
            .iter()
            .filter_map(|name| self.cf_handle(name).ok())
            .filter_map(|cf| {
                self.inner
                    .db
                    .property_int_value_cf(&cf, "rocksdb.num-live-versions")
                    .ok()
                    .flatten()
            })
            .sum()
    }

    /// Snapshot of HNSW space names, node counts, and storage modes.
    pub fn hnsw_spaces_info(&self) -> Vec<(String, usize, bool)> {
        self.inner
            .hnsw_spaces
            .read()
            .unwrap()
            .iter()
            .map(|(name, idx)| (name.clone(), idx.len(), idx.is_mmap()))
            .collect()
    }

    // ── predicate interning ───────────────────────────────────────────────────

    pub fn intern_predicate(&self, pred: &str) -> Result<PredId, StorageError> {
        if let Some(&id) = self.inner.fwd.read().unwrap().get(pred) {
            return Ok(id);
        }
        if self.is_replica() {
            return Err(Self::read_only_err());
        }
        let mut fwd = self.inner.fwd.write().unwrap();
        if let Some(&id) = fwd.get(pred) {
            return Ok(id);
        }
        let mut next = self.inner.next_pred_id.write().unwrap();
        let id = *next;
        *next += 1;

        let meta_cf = self.cf_handle(cf::META)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, meta_forward_key(pred), id.to_be_bytes());
        batch.put_cf(&meta_cf, meta_reverse_key(id), pred.as_bytes());
        batch.put_cf(&meta_cf, META_PRED_CTR, next.to_be_bytes());
        self.inner.db.write(batch)?;

        fwd.insert(pred.to_owned(), id);
        self.inner.rev.write().unwrap().insert(id, pred.to_owned());
        Ok(id)
    }

    pub fn predicate_string(&self, id: PredId) -> Option<String> {
        self.inner.rev.read().unwrap().get(&id).cloned()
    }

    /// Look up a predicate ID without assigning one (used by conflict check).
    pub(crate) fn predicate_id(&self, pred: &str) -> Option<PredId> {
        self.inner.fwd.read().unwrap().get(pred).copied()
    }

    // ── snapshot scans ────────────────────────────────────────────────────────

    /// All triples for `subject` visible at `snapshot_ts`.
    pub(crate) fn scan_by_subject_at(
        &self,
        subject: &NodeId,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let prefix = keys::spo_prefix_s(subject);
        self.snapshot_scan_spo(&prefix, snapshot_ts, vt_as_of)
    }

    pub(crate) fn scan_by_subject_predicate_at(
        &self,
        subject: &NodeId,
        predicate: &str,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let p = match self.predicate_id(predicate) {
            Some(id) => id,
            None => return Ok(vec![]),
        };
        let prefix = keys::spo_prefix_sp(subject, p);
        self.snapshot_scan_spo(&prefix, snapshot_ts, vt_as_of)
    }

    pub(crate) fn scan_by_predicate_at(
        &self,
        predicate: &str,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let p = match self.predicate_id(predicate) {
            Some(id) => id,
            None => return Ok(vec![]),
        };
        let prefix = keys::pso_prefix_p(p);
        self.snapshot_scan_cf(cf::PSO, &prefix, snapshot_ts, vt_as_of, |key, value| {
            let dk = keys::decode_pso(key)?;
            Ok((dk.subject, dk.pred_id, dk.object, dk.tt, value.to_vec()))
        })
    }

    pub(crate) fn scan_by_predicate_object_at(
        &self,
        predicate: &str,
        object: &NodeId,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let p = match self.predicate_id(predicate) {
            Some(id) => id,
            None => return Ok(vec![]),
        };
        let prefix = keys::pos_prefix_po(p, object);
        self.snapshot_scan_cf(cf::POS, &prefix, snapshot_ts, vt_as_of, |key, value| {
            let dk = keys::decode_pos(key)?;
            Ok((dk.subject, dk.pred_id, dk.object, dk.tt, value.to_vec()))
        })
    }

    pub(crate) fn scan_by_object_at(
        &self,
        object: &NodeId,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let prefix = keys::osp_prefix_o(object);
        self.snapshot_scan_cf(cf::OSP, &prefix, snapshot_ts, vt_as_of, |key, value| {
            let dk = keys::decode_osp(key)?;
            Ok((dk.subject, dk.pred_id, dk.object, dk.tt, value.to_vec()))
        })
    }

    /// All triples for (subject, object) visible at `snapshot_ts` — uses the SOP CF.
    pub(crate) fn scan_by_subject_object_at(
        &self,
        subject: &NodeId,
        object: &NodeId,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let prefix = keys::sop_prefix_so(subject, object);
        self.snapshot_scan_cf(cf::SOP, &prefix, snapshot_ts, vt_as_of, |key, value| {
            let dk = keys::decode_sop(key)?;
            Ok((dk.subject, dk.pred_id, dk.object, dk.tt, value.to_vec()))
        })
    }

    /// All triples in the store visible at `snapshot_ts` — full SPO scan.
    pub(crate) fn scan_all_at(
        &self,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        // Empty prefix matches every key in the CF.
        self.snapshot_scan_spo(&[], snapshot_ts, vt_as_of)
    }

    // ── snapshot scan implementation ──────────────────────────────────────────

    /// Snapshot scan over the SPO column family.
    fn snapshot_scan_spo(
        &self,
        prefix: &[u8],
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        self.snapshot_scan_cf(cf::SPO, prefix, snapshot_ts, vt_as_of, |key, value| {
            let dk = keys::decode_spo(key)?;
            Ok((dk.subject, dk.pred_id, dk.object, dk.tt, value.to_vec()))
        })
    }

    /// Generic snapshot scan:
    ///
    /// 1. Prefix-scan the given CF.
    /// 2. Discard entries with `tt > snapshot_ts` (not yet committed at our snapshot).
    /// 3. If `vt_as_of` is set, discard entries whose valid-time window does not
    ///    cover the requested point (`vt_start <= vt_as_of < vt_end`).
    /// 4. Group by (subject, pred_id, object): keep only the entry with the
    ///    highest `tt` (latest committed version satisfying both filters).
    /// 5. Reconstruct and return `Triple` values.
    fn snapshot_scan_cf(
        &self,
        cf_name: &str,
        prefix: &[u8],
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
        decode_key: impl Fn(&[u8], &[u8]) -> Result<(NodeId, PredId, NodeId, Timestamp, Vec<u8>), StorageError>,
    ) -> Result<Vec<Triple>, StorageError> {
        let cf = self.cf_handle(cf_name)?;
        let iter = self
            .inner
            .db
            .iterator_cf(&cf, IteratorMode::From(prefix, Direction::Forward));

        // Map: (subject, pred_id, object) → (tt, value_bytes)
        // We keep only the latest version visible at snapshot_ts that also
        // satisfies the optional valid-time filter.
        let mut latest: HashMap<(NodeId, PredId, NodeId), (Timestamp, Vec<u8>)> = HashMap::new();

        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(prefix) {
                break;
            }
            let (subject, pred_id, object, tt, value_bytes) =
                decode_key(&key, &value)?;

            // Skip uncommitted-at-snapshot entries.
            if tt > snapshot_ts {
                continue;
            }

            // Valid-time filter: keep only entries whose vt window covers vt_as_of.
            // Checked before updating the latest map so that we pick the highest-tt
            // version that is actually valid at the requested point in time.
            if let Some(vt) = vt_as_of {
                let pass = match codec::decode_value(&value_bytes) {
                    Ok(DecodedValue::Relation { temporal, .. }) => {
                        temporal.vt_start.0 <= vt && vt < temporal.vt_end.0
                    }
                    Ok(DecodedValue::Property { temporal, .. }) => {
                        temporal.vt_start.0 <= vt && vt < temporal.vt_end.0
                    }
                    Err(_) => false,
                };
                if !pass {
                    continue;
                }
            }

            let slot = latest.entry((subject, pred_id, object)).or_insert((Timestamp(i64::MIN), vec![]));
            if tt > slot.0 {
                *slot = (tt, value_bytes);
            }
        }

        // Reconstruct triples from the winning versions.
        let mut triples = Vec::with_capacity(latest.len());
        for ((subject, pred_id, object), (tt, value_bytes)) in latest {
            let triple = self.reconstruct(subject, pred_id, object, tt, &value_bytes)?;
            triples.push(triple);
        }
        Ok(triples)
    }

    // ── internal write helpers (used by Transaction) ──────────────────────────

    /// Write one logical triple into all 6 index CFs via a caller-supplied batch.
    pub(crate) fn batch_triple(
        &self,
        batch: &mut WriteBatch,
        s: NodeId,
        p: PredId,
        o: NodeId,
        tt: Timestamp,
        value: &[u8],
    ) -> Result<(), StorageError> {
        batch.put_cf(&self.cf_handle(cf::SPO)?, keys::encode_spo(&s, p, &o, tt), value);
        batch.put_cf(&self.cf_handle(cf::SOP)?, keys::encode_sop(&s, &o, p, tt), value);
        batch.put_cf(&self.cf_handle(cf::PSO)?, keys::encode_pso(p, &s, &o, tt), value);
        batch.put_cf(&self.cf_handle(cf::POS)?, keys::encode_pos(p, &o, &s, tt), value);
        batch.put_cf(&self.cf_handle(cf::OSP)?, keys::encode_osp(&o, &s, p, tt), value);
        batch.put_cf(&self.cf_handle(cf::OPS)?, keys::encode_ops(&o, p, &s, tt), value);
        Ok(())
    }

    // ── trigram index ─────────────────────────────────────────────────────────

    /// Write trigram index entries for a text property into a caller-supplied batch.
    ///
    /// Appends one key per trigram extracted from `text` to the `tri` CF.
    /// Called from `Transaction::commit()` for every `Triple::Property { value: Text(..) }`.
    pub(crate) fn batch_text_trigrams(
        &self,
        batch: &mut WriteBatch,
        subject: &NodeId,
        pred_id: PredId,
        text: &str,
    ) -> Result<(), StorageError> {
        let tri_cf = self.cf_handle(cf::TRI)?;
        for trigram in keys::extract_trigrams(text) {
            batch.put_cf(&tri_cf, keys::encode_tri(trigram, pred_id, subject), b"");
        }
        Ok(())
    }

    // ── RDF-star EPA / EPO write helpers ──────────────────────────────────────

    /// Write one EPA (edge property annotation) entry into a caller-supplied batch.
    /// Also writes the corresponding PEA (predicate-first) secondary index entry.
    pub(crate) fn batch_epa(
        &self,
        batch: &mut WriteBatch,
        edge: EdgeId,
        pred_id: keys::PredId,
        tt: Timestamp,
        value_bytes: &[u8],
    ) -> Result<(), StorageError> {
        batch.put_cf(&self.cf_handle(cf::EPA)?, keys::encode_epa_key(&edge, pred_id, tt), value_bytes);
        batch.put_cf(&self.cf_handle(cf::PEA)?, keys::encode_pea_key(pred_id, &edge, tt), value_bytes);
        Ok(())
    }

    /// Write one PEA (predicate-first annotation index) entry into a caller-supplied batch.
    ///
    /// Normally called indirectly through `batch_epa`. Use this directly only
    /// when the EPA entry has already been written separately.
    #[allow(dead_code)]
    pub(crate) fn batch_pea(
        &self,
        batch: &mut WriteBatch,
        pred_id: keys::PredId,
        edge: EdgeId,
        tt: Timestamp,
        value_bytes: &[u8],
    ) -> Result<(), StorageError> {
        batch.put_cf(&self.cf_handle(cf::PEA)?, keys::encode_pea_key(pred_id, &edge, tt), value_bytes);
        Ok(())
    }

    /// Write one EPO (edge relation annotation) entry into a caller-supplied batch.
    ///
    /// Value layout: `[vt_start BE(8)][vt_end BE(8)]` = 16 bytes.
    pub(crate) fn batch_epo(
        &self,
        batch: &mut WriteBatch,
        edge: EdgeId,
        pred_id: keys::PredId,
        object: NodeId,
        tt: Timestamp,
        value_bytes: &[u8],
    ) -> Result<(), StorageError> {
        batch.put_cf(&self.cf_handle(cf::EPO)?, keys::encode_epo_key(&edge, pred_id, &object, tt), value_bytes);
        Ok(())
    }

    // ── RDF-star scan methods ─────────────────────────────────────────────────

    /// Scan all annotations (property + relation) on a given edge visible at `snapshot_ts`.
    ///
    /// Returns a deduplicated list: for each (pred_id) group in EPA and each
    /// (pred_id, obj_id) group in EPO, only the entry with the highest
    /// `tt <= snapshot_ts` is returned.
    pub fn scan_edge_annotations(
        &self,
        edge: EdgeId,
        snapshot_ts: Timestamp,
    ) -> Result<Vec<EdgeAnnotation>, StorageError> {
        let mut result = Vec::new();

        // ── EPA (property annotations) ────────────────────────────────────────
        {
            let cf = self.cf_handle(cf::EPA)?;
            let prefix = keys::epa_prefix_edge(&edge);
            let iter = self.inner.db.iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));

            // Map: pred_id → (tt, value_bytes)
            let mut latest: HashMap<keys::PredId, (Timestamp, Vec<u8>)> = HashMap::new();
            for item in iter {
                let (key, value) = item?;
                if !key.starts_with(&prefix) { break; }
                let dk = keys::decode_epa_key(&key)?;
                if dk.tt > snapshot_ts { continue; }
                let slot = latest.entry(dk.pred_id).or_insert((Timestamp(i64::MIN), vec![]));
                if dk.tt > slot.0 {
                    *slot = (dk.tt, value.to_vec());
                }
            }

            for (pred_id, (_, value_bytes)) in latest {
                let pred_str = self
                    .predicate_string(pred_id)
                    .ok_or_else(|| StorageError::KeyDecode(format!("unknown pred_id {pred_id}")))?;
                let decoded = codec::decode_value(&value_bytes)?;
                if let codec::DecodedValue::Property { value, .. } = decoded {
                    result.push(EdgeAnnotation {
                        predicate: Predicate::new(pred_str),
                        value: EdgeAnnotationValue::Scalar(value),
                    });
                }
            }
        }

        // ── EPO (relation annotations) ────────────────────────────────────────
        {
            let cf = self.cf_handle(cf::EPO)?;
            let prefix = keys::epo_prefix_edge(&edge);
            let iter = self.inner.db.iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));

            // Map: (pred_id, obj_id) → tt
            let mut latest: HashMap<(keys::PredId, NodeId), Timestamp> = HashMap::new();
            for item in iter {
                let (key, _value) = item?;
                if !key.starts_with(&prefix) { break; }
                let dk = keys::decode_epo_key(&key)?;
                if dk.tt > snapshot_ts { continue; }
                let slot = latest.entry((dk.pred_id, dk.object)).or_insert(Timestamp(i64::MIN));
                if dk.tt > *slot {
                    *slot = dk.tt;
                }
            }

            for ((pred_id, obj_id), _tt) in latest {
                let pred_str = self
                    .predicate_string(pred_id)
                    .ok_or_else(|| StorageError::KeyDecode(format!("unknown pred_id {pred_id}")))?;
                result.push(EdgeAnnotation {
                    predicate: Predicate::new(pred_str),
                    value: EdgeAnnotationValue::Node(obj_id),
                });
            }
        }

        Ok(result)
    }

    /// Point-lookup: retrieve the annotation for `(edge, predicate)` at `snapshot_ts`.
    ///
    /// Checks EPA first (property), then EPO (relation). Returns `None` if no
    /// annotation exists for the given predicate on this edge.
    pub fn get_edge_annotation(
        &self,
        edge: EdgeId,
        predicate: &str,
        snapshot_ts: Timestamp,
    ) -> Result<Option<EdgeAnnotation>, StorageError> {
        let pred_id = match self.predicate_id(predicate) {
            Some(id) => id,
            None => return Ok(None),
        };

        // Check EPA.
        {
            let cf = self.cf_handle(cf::EPA)?;
            let prefix = keys::epa_prefix_edge_pred(&edge, pred_id);
            let iter = self.inner.db.iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));
            let mut best: Option<(Timestamp, Vec<u8>)> = None;
            for item in iter {
                let (key, value) = item?;
                if !key.starts_with(&prefix) { break; }
                let dk = keys::decode_epa_key(&key)?;
                if dk.tt > snapshot_ts { continue; }
                match &best {
                    Some((bt, _)) if dk.tt <= *bt => {}
                    _ => best = Some((dk.tt, value.to_vec())),
                }
            }
            if let Some((_, value_bytes)) = best {
                let decoded = codec::decode_value(&value_bytes)?;
                if let codec::DecodedValue::Property { value, .. } = decoded {
                    return Ok(Some(EdgeAnnotation {
                        predicate: Predicate::new(predicate.to_owned()),
                        value: EdgeAnnotationValue::Scalar(value),
                    }));
                }
            }
        }

        // Check EPO.
        {
            let cf = self.cf_handle(cf::EPO)?;
            let prefix = keys::epa_prefix_edge_pred(&edge, pred_id); // same first 20 bytes
            let iter = self.inner.db.iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));
            let mut best: Option<(Timestamp, NodeId)> = None;
            for item in iter {
                let (key, _) = item?;
                if !key.starts_with(&prefix) { break; }
                if key.len() != 44 { continue; }
                let dk = keys::decode_epo_key(&key)?;
                if dk.tt > snapshot_ts { continue; }
                match &best {
                    Some((bt, _)) if dk.tt <= *bt => {}
                    _ => best = Some((dk.tt, dk.object)),
                }
            }
            if let Some((_, obj_id)) = best {
                return Ok(Some(EdgeAnnotation {
                    predicate: Predicate::new(predicate.to_owned()),
                    value: EdgeAnnotationValue::Node(obj_id),
                }));
            }
        }

        Ok(None)
    }

    /// Scan all edge property annotations with a given predicate, as of `snapshot_ts`.
    ///
    /// Uses the PEA (predicate-first) index. Returns one `Triple::EdgeProperty` per
    /// (edge_id, predicate) pair, deduplicated to the latest version ≤ `snapshot_ts`.
    pub fn scan_annotations_by_predicate(
        &self,
        predicate: &str,
        snapshot_ts: Timestamp,
    ) -> Result<Vec<Triple>, StorageError> {
        let pred_id = match self.predicate_id(predicate) {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        let cf = self.cf_handle(cf::PEA)?;
        let prefix = keys::pea_prefix_pred(pred_id);
        let iter = self.inner.db.iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));

        // Deduplicate by edge_id: keep highest tt ≤ snapshot_ts.
        let mut latest: HashMap<EdgeId, (Timestamp, Vec<u8>)> = HashMap::new();
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(&prefix) { break; }
            let dk = keys::decode_pea_key(&key)?;
            if dk.tt > snapshot_ts { continue; }
            let slot = latest.entry(dk.edge_id).or_insert((Timestamp(i64::MIN), vec![]));
            if dk.tt > slot.0 {
                *slot = (dk.tt, value.to_vec());
            }
        }

        let mut triples = Vec::with_capacity(latest.len());
        let pred = Predicate::new(predicate.to_owned());
        for (edge_id, (tt, value_bytes)) in latest {
            let decoded = codec::decode_value(&value_bytes)?;
            if let codec::DecodedValue::Property { value, temporal } = decoded {
                triples.push(Triple::EdgeProperty {
                    edge: edge_id,
                    predicate: pred.clone(),
                    value,
                    temporal: BiTemporalRange { tt, ..temporal },
                });
            }
        }
        Ok(triples)
    }

    /// Return all annotations on `edge` as `Triple::EdgeProperty` / `Triple::EdgeRelation` variants.
    pub fn scan_edge_annotations_as_triples(
        &self,
        edge: EdgeId,
        snapshot_ts: Timestamp,
    ) -> Result<Vec<Triple>, StorageError> {
        let annotations = self.scan_edge_annotations(edge, snapshot_ts)?;
        let triples = annotations
            .into_iter()
            .map(|ann| match ann.value {
                EdgeAnnotationValue::Scalar(value) => Triple::EdgeProperty {
                    edge,
                    predicate: ann.predicate,
                    value,
                    temporal: BiTemporalRange::assert_now(Timestamp::now()),
                },
                EdgeAnnotationValue::Node(object) => Triple::EdgeRelation {
                    edge,
                    predicate: ann.predicate,
                    object,
                    temporal: BiTemporalRange::assert_now(Timestamp::now()),
                },
            })
            .collect();
        Ok(triples)
    }

    /// Return all historical versions of a node property ordered newest-first by
    /// transaction time.
    ///
    /// Unlike the snapshot-based scans, this does **not** deduplicate: every
    /// committed write to `(subject, predicate)` is returned so callers can
    /// inspect the full audit trail.
    ///
    /// `limit` is the maximum number of versions to return; 0 means 50.
    pub fn scan_property_history(
        &self,
        subject: NodeId,
        predicate: &str,
        limit: u32,
    ) -> Result<Vec<(Value, i64)>, StorageError> {
        let limit = if limit == 0 { 50 } else { limit as usize };

        let pred_id = match self.predicate_id(predicate) {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        // Build the 36-byte SPO prefix: [subject(16)][pred_id(4)][sentinel(16)]
        let mut prefix = [0u8; 36];
        prefix[0..16].copy_from_slice(subject.as_bytes());
        prefix[16..20].copy_from_slice(&pred_id.to_be_bytes());
        prefix[20..36].copy_from_slice(&keys::PROPERTY_SENTINEL);

        let cf = self.cf_handle(cf::SPO)?;
        let iter = self
            .inner
            .db
            .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));

        let mut versions: Vec<(Value, i64)> = Vec::new();
        for item in iter {
            let (key, value_bytes) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            if key.len() < 44 {
                continue;
            }
            let tt = i64::from_be_bytes(key[36..44].try_into().unwrap());
            match codec::decode_value(&value_bytes) {
                Ok(codec::DecodedValue::Property { value, .. }) => {
                    versions.push((value, tt));
                }
                Ok(codec::DecodedValue::Relation { .. }) => {}
                Err(_) => {}
            }
        }

        // SPO keys sort oldest-first (lowest tt first), so reverse for newest-first.
        versions.reverse();
        versions.truncate(limit);
        Ok(versions)
    }

    /// Prefix-scan the `tri` CF for all subjects that contain `trigram` under `pred_id`.
    pub fn scan_trigram_candidates(&self, pred_id: keys::PredId, trigram: [u8; 3]) -> Vec<NodeId> {
        let prefix = keys::tri_prefix_tp(trigram, pred_id);
        let cf = match self.cf_handle(cf::TRI) {
            Ok(cf) => cf,
            Err(_) => return vec![],
        };
        let iter = self.inner.db.iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));
        let mut candidates = Vec::new();
        for item in iter {
            let Ok((key, _)) = item else { break };
            if !key.starts_with(&prefix) { break; }
            if let Ok(node_id) = keys::decode_tri_subject(&key) {
                candidates.push(node_id);
            }
        }
        candidates
    }

    /// Search for nodes whose `predicate` text value contains all trigrams of `query`.
    ///
    /// Returns `NodeId`s confirmed alive via MVCC snapshot scan. Stale trigram entries
    /// (from updated/deleted values) are eliminated by the confirmation step.
    pub fn text_search(
        &self,
        predicate: &str,
        query: &str,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<NodeId>, StorageError> {
        let pred_id = match self.predicate_id(predicate) {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        let trigrams = keys::extract_trigrams(query);
        if trigrams.is_empty() {
            return Ok(vec![]);
        }

        // Collect candidate sets per trigram then intersect smallest-first.
        let mut candidate_sets: Vec<Vec<NodeId>> = trigrams
            .iter()
            .map(|tg| self.scan_trigram_candidates(pred_id, *tg))
            .collect();
        candidate_sets.sort_by_key(|s| s.len());

        let mut candidates: std::collections::HashSet<NodeId> =
            candidate_sets[0].iter().copied().collect();
        for other_set in &candidate_sets[1..] {
            let other: std::collections::HashSet<NodeId> = other_set.iter().copied().collect();
            candidates.retain(|id| other.contains(id));
        }

        // Snapshot-confirm: the live text value must exist and contain the query.
        // This eliminates stale TRI entries from superseded text values.
        use polargraph_core::{triple::Triple, value::Value};
        let query_lower = query.to_lowercase();
        let mut result = Vec::new();
        for node_id in candidates {
            let triples = self.scan_by_subject_predicate_at(&node_id, predicate, snapshot_ts, vt_as_of)?;
            let confirmed = triples.iter().any(|t| {
                if let Triple::Property { value: Value::Text(text), .. } = t {
                    text.to_lowercase().contains(&query_lower)
                } else {
                    false
                }
            });
            if confirmed {
                result.push(node_id);
            }
        }
        Ok(result)
    }

    pub(crate) fn db_write(&self, batch: WriteBatch) -> Result<(), StorageError> {
        if self.is_replica() {
            return Err(Self::read_only_err());
        }
        Ok(self.inner.db.write(batch)?)
    }

    pub(crate) fn db_ref(&self) -> &DB {
        &self.inner.db
    }

    pub(crate) fn oracle(&self) -> &TimestampOracle {
        &self.inner.oracle
    }

    pub(crate) fn cf_handle(&self, name: &str) -> Result<Arc<BoundColumnFamily<'_>>, StorageError> {
        self.inner
            .db
            .cf_handle(name)
            .ok_or_else(|| StorageError::MissingCf(name.to_owned()))
    }

    /// Insert a triple at an explicit transaction timestamp, bypassing the oracle.
    ///
    /// Advances the oracle to at least `tt` so that subsequent reads see this
    /// triple. Useful in tests and offline tools that need control over `tt`.
    pub fn insert_at_ts(&self, triple: &Triple, tt: Timestamp) -> Result<(), StorageError> {
        if self.is_replica() {
            return Err(Self::read_only_err());
        }
        let mut batch = WriteBatch::default();
        match triple {
            Triple::Relation { subject, predicate, object, edge_id, temporal } => {
                let temporal_stamped = BiTemporalRange { tt, ..*temporal };
                let value_bytes = codec::encode_relation(edge_id, &temporal_stamped);
                let p = self.intern_predicate(&predicate.0)?;
                self.batch_triple(&mut batch, *subject, p, *object, tt, &value_bytes)?;
            }
            Triple::Property { subject, predicate, value, temporal } => {
                let temporal_stamped = BiTemporalRange { tt, ..*temporal };
                let value_bytes = codec::encode_property(value, &temporal_stamped)?;
                let sentinel = NodeId(uuid::Uuid::from_bytes(keys::PROPERTY_SENTINEL));
                let p = self.intern_predicate(&predicate.0)?;
                self.batch_triple(&mut batch, *subject, p, sentinel, tt, &value_bytes)?;
                // Trigram index for text properties.
                if let Value::Text(text) = value {
                    self.batch_text_trigrams(&mut batch, subject, p, text)?;
                }
            }
            Triple::EdgeProperty { edge, predicate, value, temporal } => {
                let temporal_stamped = BiTemporalRange { tt, ..*temporal };
                let value_bytes = codec::encode_property(value, &temporal_stamped)?;
                let p = self.intern_predicate(&predicate.0)?;
                self.batch_epa(&mut batch, *edge, p, tt, &value_bytes)?;
            }
            Triple::EdgeRelation { edge, predicate, object, temporal } => {
                let temporal_stamped = BiTemporalRange { tt, ..*temporal };
                let epo_val = encode_epo_value(&temporal_stamped);
                let p = self.intern_predicate(&predicate.0)?;
                self.batch_epo(&mut batch, *edge, p, *object, tt, &epo_val)?;
            }
        }
        self.db_write(batch)?;
        // Advance oracle so subsequent scans can see this triple.
        self.inner.oracle.advance_to(tt);
        Ok(())
    }

    /// Trigger a full RocksDB compaction on the named column family.
    ///
    /// Used by `CompactionManager` after issuing deletes to reclaim disk space.
    pub fn compact_cf(&self, cf_name: &str) -> Result<(), StorageError> {
        if self.is_replica() {
            return Err(Self::read_only_err());
        }
        let cf = self.cf_handle(cf_name)?;
        self.inner.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }

    /// Iterate all (key, value) pairs in a hexastore CF without snapshot filtering.
    ///
    /// Used by `CompactionManager` to scan for expired triples. The callback
    /// receives raw bytes and returns `true` to mark the key for deletion.
    pub fn scan_cf_raw<F>(&self, cf_name: &str, mut should_delete: F) -> Result<usize, StorageError>
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        if self.is_replica() {
            return Err(Self::read_only_err());
        }
        let cf = self.cf_handle(cf_name)?;
        let iter = self.inner.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();
        for item in iter {
            let (k, v) = item?;
            if should_delete(&k, &v) {
                keys_to_delete.push(k.to_vec());
            }
        }
        let count = keys_to_delete.len();
        if !keys_to_delete.is_empty() {
            let mut batch = WriteBatch::default();
            for key in keys_to_delete {
                batch.delete_cf(&cf, &key);
            }
            self.db_write(batch)?;
        }
        Ok(count)
    }

    // ── reconstruction ────────────────────────────────────────────────────────

    fn reconstruct(
        &self,
        subject: NodeId,
        pred_id: PredId,
        object: NodeId,
        tt: Timestamp,
        value_bytes: &[u8],
    ) -> Result<Triple, StorageError> {
        let pred_str = self
            .predicate_string(pred_id)
            .ok_or_else(|| StorageError::KeyDecode(format!("unknown pred_id {pred_id}")))?;
        let predicate = Predicate::new(pred_str);

        let decoded = codec::decode_value(value_bytes)?;
        let triple = match decoded {
            DecodedValue::Relation { edge_id, mut temporal } => {
                temporal.tt = tt;
                Triple::Relation { subject, predicate, object, edge_id, temporal }
            }
            DecodedValue::Property { value, mut temporal } => {
                temporal.tt = tt;
                Triple::Property { subject, predicate, value, temporal }
            }
        };
        Ok(triple)
    }
}
