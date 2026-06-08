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
    id::NodeId,
    schema::StorageMode,
    temporal::Timestamp,
    triple::{Predicate, Triple},
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
}

impl TripleStore {
    // ── lifecycle ─────────────────────────────────────────────────────────────

    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

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
            }),
        })
    }

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
                if !key.starts_with(&*node_prefix) { break; }
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

        for (_i, (node_id, vector)) in items.iter().enumerate() {
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
        self.scan_by_subject_at(subject, self.inner.oracle.read_ts())
    }

    pub fn scan_by_subject_predicate(
        &self,
        subject: &NodeId,
        predicate: &str,
    ) -> Result<Vec<Triple>, StorageError> {
        self.scan_by_subject_predicate_at(subject, predicate, self.inner.oracle.read_ts())
    }

    pub fn scan_by_predicate(&self, predicate: &str) -> Result<Vec<Triple>, StorageError> {
        self.scan_by_predicate_at(predicate, self.inner.oracle.read_ts())
    }

    pub fn scan_by_predicate_object(
        &self,
        predicate: &str,
        object: &NodeId,
    ) -> Result<Vec<Triple>, StorageError> {
        self.scan_by_predicate_object_at(predicate, object, self.inner.oracle.read_ts())
    }

    pub fn scan_by_object(&self, object: &NodeId) -> Result<Vec<Triple>, StorageError> {
        self.scan_by_object_at(object, self.inner.oracle.read_ts())
    }

    pub fn scan_by_subject_object(
        &self,
        subject: &NodeId,
        object: &NodeId,
    ) -> Result<Vec<Triple>, StorageError> {
        self.scan_by_subject_object_at(subject, object, self.inner.oracle.read_ts())
    }

    pub fn scan_all(&self) -> Result<Vec<Triple>, StorageError> {
        self.scan_all_at(self.inner.oracle.read_ts())
    }

    // ── predicate interning ───────────────────────────────────────────────────

    pub fn intern_predicate(&self, pred: &str) -> Result<PredId, StorageError> {
        if let Some(&id) = self.inner.fwd.read().unwrap().get(pred) {
            return Ok(id);
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
    ) -> Result<Vec<Triple>, StorageError> {
        let prefix = keys::spo_prefix_s(subject);
        self.snapshot_scan_spo(&prefix, snapshot_ts)
    }

    pub(crate) fn scan_by_subject_predicate_at(
        &self,
        subject: &NodeId,
        predicate: &str,
        snapshot_ts: Timestamp,
    ) -> Result<Vec<Triple>, StorageError> {
        let p = match self.predicate_id(predicate) {
            Some(id) => id,
            None => return Ok(vec![]),
        };
        let prefix = keys::spo_prefix_sp(subject, p);
        self.snapshot_scan_spo(&prefix, snapshot_ts)
    }

    pub(crate) fn scan_by_predicate_at(
        &self,
        predicate: &str,
        snapshot_ts: Timestamp,
    ) -> Result<Vec<Triple>, StorageError> {
        let p = match self.predicate_id(predicate) {
            Some(id) => id,
            None => return Ok(vec![]),
        };
        let prefix = keys::pso_prefix_p(p);
        self.snapshot_scan_cf(cf::PSO, &prefix, snapshot_ts, |key, value| {
            let dk = keys::decode_pso(key)?;
            Ok((dk.subject, dk.pred_id, dk.object, dk.tt, value.to_vec()))
        })
    }

    pub(crate) fn scan_by_predicate_object_at(
        &self,
        predicate: &str,
        object: &NodeId,
        snapshot_ts: Timestamp,
    ) -> Result<Vec<Triple>, StorageError> {
        let p = match self.predicate_id(predicate) {
            Some(id) => id,
            None => return Ok(vec![]),
        };
        let prefix = keys::pos_prefix_po(p, object);
        self.snapshot_scan_cf(cf::POS, &prefix, snapshot_ts, |key, value| {
            let dk = keys::decode_pos(key)?;
            Ok((dk.subject, dk.pred_id, dk.object, dk.tt, value.to_vec()))
        })
    }

    pub(crate) fn scan_by_object_at(
        &self,
        object: &NodeId,
        snapshot_ts: Timestamp,
    ) -> Result<Vec<Triple>, StorageError> {
        let prefix = keys::osp_prefix_o(object);
        self.snapshot_scan_cf(cf::OSP, &prefix, snapshot_ts, |key, value| {
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
    ) -> Result<Vec<Triple>, StorageError> {
        let prefix = keys::sop_prefix_so(subject, object);
        self.snapshot_scan_cf(cf::SOP, &prefix, snapshot_ts, |key, value| {
            let dk = keys::decode_sop(key)?;
            Ok((dk.subject, dk.pred_id, dk.object, dk.tt, value.to_vec()))
        })
    }

    /// All triples in the store visible at `snapshot_ts` — full SPO scan.
    pub(crate) fn scan_all_at(
        &self,
        snapshot_ts: Timestamp,
    ) -> Result<Vec<Triple>, StorageError> {
        // Empty prefix matches every key in the CF.
        self.snapshot_scan_spo(&[], snapshot_ts)
    }

    // ── snapshot scan implementation ──────────────────────────────────────────

    /// Snapshot scan over the SPO column family.
    fn snapshot_scan_spo(
        &self,
        prefix: &[u8],
        snapshot_ts: Timestamp,
    ) -> Result<Vec<Triple>, StorageError> {
        self.snapshot_scan_cf(cf::SPO, prefix, snapshot_ts, |key, value| {
            let dk = keys::decode_spo(key)?;
            Ok((dk.subject, dk.pred_id, dk.object, dk.tt, value.to_vec()))
        })
    }

    /// Generic snapshot scan:
    ///
    /// 1. Prefix-scan the given CF.
    /// 2. Discard entries with `tt > snapshot_ts` (not yet committed at our snapshot).
    /// 3. Group by (subject, pred_id, object): keep only the entry with the
    ///    highest `tt` (latest committed version).
    /// 4. Reconstruct and return `Triple` values.
    fn snapshot_scan_cf(
        &self,
        cf_name: &str,
        prefix: &[u8],
        snapshot_ts: Timestamp,
        decode_key: impl Fn(&[u8], &[u8]) -> Result<(NodeId, PredId, NodeId, Timestamp, Vec<u8>), StorageError>,
    ) -> Result<Vec<Triple>, StorageError> {
        let cf = self.cf_handle(cf_name)?;
        let iter = self
            .inner
            .db
            .iterator_cf(&cf, IteratorMode::From(prefix, Direction::Forward));

        // Map: (subject, pred_id, object) → (tt, value_bytes)
        // We keep only the latest version visible at snapshot_ts.
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

    pub(crate) fn db_write(&self, batch: WriteBatch) -> Result<(), StorageError> {
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
