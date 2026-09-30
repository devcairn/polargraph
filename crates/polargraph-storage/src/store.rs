//! TripleStore — the main storage handle.
//!
//! # Public API
//!
//! ```text
//! let store = TripleStore::open(path)?;
//!
//! // Single-triple convenience (auto-commits a one-triple transaction).
//! store.insert(&triple)?;
//!
//! // Multi-triple transaction with conflict detection.
//! let mut tx = store.begin();
//! tx.insert(triple_a);                          // default graph
//! tx.insert_in(triple_b, graph, WriteMode::Add); // named graph
//! let commit_ts = tx.commit()?;          // Err(StorageError::WriteConflict) if racing
//!
//! // Point-in-time snapshot read.
//! let snap = store.snapshot(commit_ts);
//! let triples = snap.scan_by_subject(&node_id)?;
//! ```
//!
//! # Internal responsibilities
//!
//! - Storage format v3 (`docs/design/v3-key-layout.md`): every quad is written
//!   to eight 48-byte-key orders; property keys carry the value's content hash;
//!   large values live once in the `blob` CF.
//! - Predicate and graph interning: string ↔ u32 ID, persisted to META CF.
//! - `stage_writes`: encode buffered writes (incl. `Replace` closing versions)
//!   into a caller-supplied WriteBatch (used by `Transaction::commit` and
//!   `insert_at_ts`).
//! - Snapshot scan helpers (`scan_by_*_at`): prefix-scan + filter to
//!   `tt <= snapshot_ts` + pick the governing version per quad + valid-time
//!   filter (default: valid now).

use crate::{
    cf,
    codec::{self, DecodedValue},
    error::StorageError,
    hnsw::{self, HnswIndex, MmapState},
    keys::{self, Order, PredId, QuadKey},
    mvcc::{Snapshot, TimestampOracle, Transaction, WriteMode, META_ORACLE_CTR},
};
use polargraph_core::{
    id::{EdgeId, GraphId, NodeId},
    schema::StorageMode,
    temporal::{BiTemporalRange, Timestamp},
    term,
    triple::{Predicate, Triple},
    value::Value,
};
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode,
    MultiThreaded, Options, WriteBatch,
};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
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
    Replica {
        primary_address: String,
    },
}

pub(crate) type DB = DBWithThreadMode<MultiThreaded>;

// META key namespace for predicate table.
const META_PRED_PREFIX: &[u8] = b"p/";
const META_PRED_CTR: &[u8] = b"__pred_ctr";

// META key namespace for the graph table. Graph 0 is the default graph and
// has no IRI; named graphs are numbered from 1.
const META_GRAPH_PREFIX: &[u8] = b"g/";
const META_GRAPH_REV_PREFIX: &[u8] = b"gid/";
const META_GRAPH_CTR: &[u8] = b"__graph_ctr";

/// META key holding the on-disk storage format (u32 BE). Distinct from the
/// logical schema-migration counter in `migrations.rs`.
pub(crate) const META_STORAGE_FORMAT: &[u8] = b"__storage__/format";

/// The storage format this build reads and writes.
pub const STORAGE_FORMAT: u32 = 3;

/// Default for [`TripleStore::set_inline_value_max_bytes`]: property payloads
/// larger than this are stored once in the `blob` CF.
pub const DEFAULT_INLINE_VALUE_MAX_BYTES: usize = 256;

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
fn meta_graph_key(iri: &str) -> Vec<u8> {
    let mut k = META_GRAPH_PREFIX.to_vec();
    k.extend_from_slice(iri.as_bytes());
    k
}
fn meta_graph_rev_key(id: GraphId) -> Vec<u8> {
    let mut k = META_GRAPH_REV_PREFIX.to_vec();
    k.extend_from_slice(&id.to_be_bytes());
    k
}

/// How `open_db` treats a store still in storage format v2.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum V2Policy {
    /// Refuse to open (`StorageError::NeedsMigration`).
    Refuse,
    /// Open as-is so `migrate_v3` can read the v2 column families.
    AllowForMigration,
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
    graph_fwd: RwLock<HashMap<String, GraphId>>, // graph IRI → id
    graph_rev: RwLock<HashMap<GraphId, String>>, // id → graph IRI
    /// Graph IRI node (`term::iri_to_node_id(iri)`) → graph id, so a graph
    /// variable bound to a node can be resolved back to its graph.
    graph_by_node: RwLock<HashMap<NodeId, GraphId>>,
    next_graph_id: RwLock<u32>,
    /// Property payloads above this many bytes go to the `blob` CF.
    inline_value_max_bytes: AtomicUsize,
    /// Named HNSW spaces: space_name → index.
    hnsw_spaces: RwLock<HashMap<String, HnswIndex>>,
    /// Path passed to `open()`, used to locate mmap `.vecs` files.
    data_dir: PathBuf,
    /// Whether this is a primary or replica instance.
    mode: StoreMode,
}

// META key for persisting the last replicated WAL sequence number.
const META_LAST_REPL_SEQ: &[u8] = b"__replication__/last_seq";

/// Text property values longer than this (in UTF-8 bytes) are not written to
/// the trigram index. Labels, names and titles stay searchable; long bodies
/// (descriptions, document chunks) would multiply the `tri` CF by their length
/// and are better served by vector search.
pub const TRIGRAM_MAX_TEXT_BYTES: usize = 512;

/// A raw (key, value) entry read straight from a column family.
pub type RawEntry = (Box<[u8]>, Box<[u8]>);

/// Per-quad winner while deduplicating versions in `snapshot_scan`:
/// `(vt_start, tt, vt_end, value_bytes)`.
type VersionSlot = (i64, Timestamp, i64, Vec<u8>);

/// `(subject, predicate, object slot, graph)` of a quad.
type QuadIds = (NodeId, PredId, NodeId, GraphId);

/// Property values staged in one write batch, per `(s, p, g)`.
type StagedValues = HashMap<(NodeId, PredId, GraphId), Vec<(NodeId, Vec<u8>)>>;

/// Which graphs a scan reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphScope {
    /// Every graph, the default graph included.
    Union,
    /// One graph.
    One(GraphId),
    /// A set of graphs (kept sorted; see [`GraphScope::set`]).
    Set(Vec<GraphId>),
    /// Every named graph — everything except the default graph. This is
    /// what a graph variable (`GRAPH ?g`) ranges over.
    Named,
}

impl GraphScope {
    /// A `Set` scope, sorted and de-duplicated.
    pub fn set(mut graphs: Vec<GraphId>) -> Self {
        graphs.sort();
        graphs.dedup();
        GraphScope::Set(graphs)
    }

    /// Whether quads in graph `g` are in scope.
    #[inline]
    pub fn admits(&self, g: GraphId) -> bool {
        match self {
            GraphScope::Union => true,
            GraphScope::One(want) => *want == g,
            GraphScope::Set(gs) => gs.binary_search(&g).is_ok(),
            GraphScope::Named => g != GraphId::DEFAULT,
        }
    }
}

/// A buffered write: the triple, the graph it goes to, and how properties
/// treat existing values of the same `(subject, predicate, graph)`.
#[derive(Clone, Debug)]
pub(crate) struct PendingWrite {
    pub triple: Triple,
    pub graph: GraphId,
    pub mode: WriteMode,
}

impl TripleStore {
    // ── lifecycle ─────────────────────────────────────────────────────────────

    /// Open (or create) a store. A store still in storage format v2 is
    /// refused with [`StorageError::NeedsMigration`] — run `polargraphd migrate`.
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        Self::open_db(path, StoreMode::Primary, V2Policy::Refuse)
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
        Self::open_db(
            path,
            StoreMode::Replica { primary_address },
            V2Policy::Refuse,
        )
    }

    pub(crate) fn open_db(
        path: &Path,
        mode: StoreMode,
        v2: V2Policy,
    ) -> Result<Self, StorageError> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        // Keep WAL files for up to 1 hour so replicas can catch up after a gap.
        db_opts.set_wal_ttl_seconds(3600);
        db_opts.set_wal_size_limit_mb(512);

        // RocksDB requires every existing CF to be opened, so include any
        // v2 CFs still on disk alongside the current set.
        let existing = DB::list_cf(&db_opts, path).unwrap_or_default();
        let mut names: Vec<String> = cf::ALL.iter().map(|s| s.to_string()).collect();
        for name in existing {
            if !names.contains(&name) && name != "default" {
                names.push(name);
            }
        }
        let cf_descriptors: Vec<ColumnFamilyDescriptor> = names
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
            .collect();

        let db = DB::open_cf_descriptors(&db_opts, path, cf_descriptors)?;
        Self::check_format(&db, v2)?;

        // Ensure the vectors/ subdirectory exists for any mmap spaces.
        let vectors_dir = path.join("vectors");
        std::fs::create_dir_all(&vectors_dir)?;

        let (fwd, rev, next_pred_id) = Self::load_predicates(&db)?;
        let (graph_fwd, graph_rev, next_graph_id) = Self::load_graphs(&db)?;
        let oracle_ts = Self::load_oracle_ts(&db)?;
        let oracle = TimestampOracle::new(oracle_ts);
        let hnsw_spaces = Self::load_hnsw_spaces(&db, &vectors_dir)?;

        let total_nodes: usize = hnsw_spaces.values().map(|i| i.len()).sum();
        info!(
            path = %path.display(),
            replica = matches!(mode, StoreMode::Replica { .. }),
            predicates = fwd.len(),
            graphs = graph_fwd.len(),
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
                graph_fwd: RwLock::new(graph_fwd),
                graph_by_node: RwLock::new(Self::graph_nodes(&graph_rev)),
                graph_rev: RwLock::new(graph_rev),
                next_graph_id: RwLock::new(next_graph_id),
                inline_value_max_bytes: AtomicUsize::new(DEFAULT_INLINE_VALUE_MAX_BYTES),
                hnsw_spaces: RwLock::new(hnsw_spaces),
                data_dir: path.to_path_buf(),
                mode,
            }),
        })
    }

    /// The storage format recorded in META, if any.
    pub(crate) fn stored_format(db: &DB) -> Result<Option<u32>, StorageError> {
        let meta = db
            .cf_handle(cf::META)
            .ok_or_else(|| StorageError::MissingCf(cf::META.into()))?;
        Ok(match db.get_cf(&meta, META_STORAGE_FORMAT)? {
            Some(v) if v.len() == 4 => Some(u32::from_be_bytes(v[..4].try_into().unwrap())),
            _ => None,
        })
    }

    /// True if any v2 data column family exists and holds at least one key.
    pub(crate) fn has_v2_data(db: &DB) -> bool {
        cf::v2::ALL.iter().any(|name| {
            db.cf_handle(name)
                .and_then(|h| db.iterator_cf(&h, IteratorMode::Start).next())
                .is_some()
        })
    }

    /// Drop any v2 column families still present (all data is in v3 CFs).
    pub(crate) fn drop_v2_cfs(db: &DB) -> Result<(), StorageError> {
        for name in cf::v2::ALL {
            if db.cf_handle(name).is_some() {
                db.drop_cf(name)?;
            }
        }
        Ok(())
    }

    /// Enforce the storage format on open:
    /// - format 3 → fine (finish dropping v2 CFs if a migration was interrupted
    ///   after its commit point);
    /// - no format and no v2 data → a new store: record format 3;
    /// - no format but v2 data → refuse unless opening for migration.
    fn check_format(db: &DB, v2: V2Policy) -> Result<(), StorageError> {
        match Self::stored_format(db)? {
            Some(STORAGE_FORMAT) => Self::drop_v2_cfs(db),
            Some(other) => Err(StorageError::UnsupportedFormat(other)),
            None if Self::has_v2_data(db) => match v2 {
                V2Policy::Refuse => Err(StorageError::NeedsMigration),
                V2Policy::AllowForMigration => Ok(()),
            },
            None => {
                Self::drop_v2_cfs(db)?;
                let meta = db
                    .cf_handle(cf::META)
                    .ok_or_else(|| StorageError::MissingCf(cf::META.into()))?;
                db.put_cf(&meta, META_STORAGE_FORMAT, STORAGE_FORMAT.to_be_bytes())?;
                Ok(())
            }
        }
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
        let (graph_fwd, graph_rev, next_graph_id) = Self::load_graphs(&self.inner.db)?;
        *self.inner.graph_fwd.write().unwrap() = graph_fwd;
        *self.inner.graph_by_node.write().unwrap() = Self::graph_nodes(&graph_rev);
        *self.inner.graph_rev.write().unwrap() = graph_rev;
        *self.inner.next_graph_id.write().unwrap() = next_graph_id;
        self.inner
            .oracle
            .advance_to(polargraph_core::temporal::Timestamp(oracle_ts));

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
        StorageError::ReadOnly("write operations are not supported on a read replica".to_string())
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
                return Err(StorageError::KeyDecode(format!(
                    "bad predicate value for {name}"
                )));
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

    #[allow(clippy::type_complexity)]
    fn load_graphs(
        db: &DB,
    ) -> Result<(HashMap<String, GraphId>, HashMap<GraphId, String>, u32), StorageError> {
        let meta_cf = db
            .cf_handle(cf::META)
            .ok_or_else(|| StorageError::MissingCf(cf::META.into()))?;
        let mut fwd = HashMap::new();
        let mut rev = HashMap::new();
        let iter = db.iterator_cf(
            &meta_cf,
            IteratorMode::From(META_GRAPH_PREFIX, Direction::Forward),
        );
        for item in iter {
            let (k, v) = item?;
            if !k.starts_with(META_GRAPH_PREFIX) {
                break;
            }
            let iri = std::str::from_utf8(&k[META_GRAPH_PREFIX.len()..])
                .map_err(|e| StorageError::KeyDecode(e.to_string()))?
                .to_owned();
            if v.len() != 4 {
                return Err(StorageError::KeyDecode(format!("bad graph id for {iri}")));
            }
            let id = GraphId(u32::from_be_bytes(v[..4].try_into().unwrap()));
            fwd.insert(iri.clone(), id);
            rev.insert(id, iri);
        }
        let next = match db.get_cf(&meta_cf, META_GRAPH_CTR)? {
            Some(v) if v.len() == 4 => u32::from_be_bytes(v[..4].try_into().unwrap()),
            _ => 1,
        };
        Ok((fwd, rev, next))
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
            let is_mmap = vecs_path.exists();

            let mut idx = if is_mmap {
                let mmap_state = MmapState::open(vecs_path)?;
                HnswIndex::new_mmap_with_state(mmap_state)
            } else {
                HnswIndex::new()
            };
            idx.load_entry_point(ep_id, ep_layer);

            let node_prefix = hnsw::node_prefix_for_space(&space);
            let id_start = node_prefix.len();

            let iter = db.iterator_cf(
                &hnsw_cf,
                IteratorMode::From(&node_prefix, Direction::Forward),
            );
            for item in iter {
                let (key, value) = item?;
                if !key.starts_with(&node_prefix) {
                    break;
                }
                if key.len() < id_start + 16 {
                    continue;
                }
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
        let idx = spaces
            .entry(space.to_string())
            .or_insert_with(|| match mode {
                StorageMode::Mmap => {
                    let path = self
                        .inner
                        .data_dir
                        .join("vectors")
                        .join(format!("{space}.vecs"));
                    HnswIndex::new_mmap(path)
                }
                StorageMode::Memory => HnswIndex::new(),
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
        let idx = spaces
            .entry(space.to_string())
            .or_insert_with(|| match mode {
                StorageMode::Mmap => {
                    let path = self
                        .inner
                        .data_dir
                        .join("vectors")
                        .join(format!("{space}.vecs"));
                    HnswIndex::new_mmap(path)
                }
                StorageMode::Memory => HnswIndex::new(),
            });

        let mut batch = WriteBatch::default();
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
    /// the `spog` column family. Not exact — use for monitoring/health only.
    pub fn estimate_triple_count(&self) -> u64 {
        self.cf_handle(cf::SPOG)
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
        cf::ALL
            .iter()
            .map(|name| self.cf_approx_size_bytes(name))
            .sum()
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

    /// Public version of [`predicate_id`] — returns `None` if the predicate has never been interned.
    pub fn lookup_predicate(&self, pred: &str) -> Option<PredId> {
        self.inner.fwd.read().unwrap().get(pred).copied()
    }

    // ── graph interning ───────────────────────────────────────────────────────

    /// The id for graph `iri`, assigning one on first use. The default graph
    /// has no IRI — use [`GraphId::DEFAULT`].
    pub fn intern_graph(&self, iri: &str) -> Result<GraphId, StorageError> {
        if iri.is_empty() {
            return Err(StorageError::Validation(
                "graph IRI must not be empty".into(),
            ));
        }
        if let Some(&id) = self.inner.graph_fwd.read().unwrap().get(iri) {
            return Ok(id);
        }
        if self.is_replica() {
            return Err(Self::read_only_err());
        }
        let mut fwd = self.inner.graph_fwd.write().unwrap();
        if let Some(&id) = fwd.get(iri) {
            return Ok(id);
        }
        let mut next = self.inner.next_graph_id.write().unwrap();
        let id = GraphId(*next);
        *next += 1;

        let meta_cf = self.cf_handle(cf::META)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, meta_graph_key(iri), id.to_be_bytes());
        batch.put_cf(&meta_cf, meta_graph_rev_key(id), iri.as_bytes());
        batch.put_cf(&meta_cf, META_GRAPH_CTR, next.to_be_bytes());
        // The graph IRI names a node too (graph variables bind to it), so
        // record it in the IRI dictionary for export.
        self.batch_iri(&mut batch, iri, &mut HashMap::new())?;
        self.inner.db.write(batch)?;
        self.inner
            .graph_by_node
            .write()
            .unwrap()
            .insert(term::iri_to_node_id(iri), id);

        fwd.insert(iri.to_owned(), id);
        self.inner
            .graph_rev
            .write()
            .unwrap()
            .insert(id, iri.to_owned());
        Ok(id)
    }

    fn graph_nodes(graph_rev: &HashMap<GraphId, String>) -> HashMap<NodeId, GraphId> {
        graph_rev
            .iter()
            .map(|(id, iri)| (term::iri_to_node_id(iri), *id))
            .collect()
    }

    /// The node a named graph's IRI names — what a graph variable binds to.
    /// `None` for the default graph (it has no IRI) and unknown ids.
    pub fn graph_node(&self, id: GraphId) -> Option<NodeId> {
        self.graph_iri(id).map(|iri| term::iri_to_node_id(&iri))
    }

    /// The named graph whose IRI names `node`, if any.
    pub fn graph_for_node(&self, node: &NodeId) -> Option<GraphId> {
        self.inner.graph_by_node.read().unwrap().get(node).copied()
    }

    /// The id of graph `iri` if it has been interned.
    pub fn graph_id(&self, iri: &str) -> Option<GraphId> {
        self.inner.graph_fwd.read().unwrap().get(iri).copied()
    }

    /// The IRI of a named graph (`None` for the default graph or unknown ids).
    pub fn graph_iri(&self, id: GraphId) -> Option<String> {
        self.inner.graph_rev.read().unwrap().get(&id).cloned()
    }

    /// Every interned named graph, ordered by id.
    pub fn list_graphs(&self) -> Vec<(GraphId, String)> {
        let mut out: Vec<(GraphId, String)> = self
            .inner
            .graph_rev
            .read()
            .unwrap()
            .iter()
            .map(|(id, iri)| (*id, iri.clone()))
            .collect();
        out.sort();
        out
    }

    // ── out-of-line values ────────────────────────────────────────────────────

    /// Property payloads larger than `n` bytes are written once to the `blob`
    /// CF (default [`DEFAULT_INLINE_VALUE_MAX_BYTES`]). Affects new writes only.
    pub fn set_inline_value_max_bytes(&self, n: usize) {
        self.inner
            .inline_value_max_bytes
            .store(n, Ordering::Relaxed);
    }

    pub fn inline_value_max_bytes(&self) -> usize {
        self.inner.inline_value_max_bytes.load(Ordering::Relaxed)
    }

    /// Encode a property value for an index entry, moving it to the `blob` CF
    /// (in `batch`) when its payload is over the inline threshold.
    pub(crate) fn encode_property_entry(
        &self,
        batch: &mut WriteBatch,
        value: &Value,
        temporal: &BiTemporalRange,
    ) -> Result<Vec<u8>, StorageError> {
        let inline = codec::encode_property(value, temporal)?;
        if inline.len() - 17 <= self.inline_value_max_bytes() {
            return Ok(inline);
        }
        batch.put_cf(
            &self.cf_handle(cf::BLOB)?,
            value.content_hash(),
            codec::encode_blob(value)?,
        );
        Ok(codec::encode_property_ref(temporal))
    }

    /// Resolve an out-of-line value by its content hash.
    fn load_blob(&self, hash: &NodeId) -> Result<Value, StorageError> {
        let cf = self.cf_handle(cf::BLOB)?;
        let bytes = self.inner.db.get_cf(&cf, hash.as_bytes())?.ok_or_else(|| {
            StorageError::KeyDecode(format!("missing blob for value hash {hash}"))
        })?;
        codec::decode_blob(&bytes)
    }

    /// All MVCC-visible EdgeIds of the relation `(subject, pred_id, object)`
    /// in any graph, newest first.
    ///
    /// Used by the SPARQL-star executor to resolve a quoted triple `<< S P O >>` to its
    /// stored edge UUID(s) so that edge annotations can be retrieved via
    /// [`scan_edge_annotations`].
    pub fn scan_spo_for_edge_ids(
        &self,
        subject: NodeId,
        pred_id: PredId,
        object: NodeId,
        snapshot_ts: Timestamp,
    ) -> Result<Vec<EdgeId>, StorageError> {
        let prefix = Order::Spog.prefix(Some(&subject), Some(pred_id), Some(&object), None);
        let cf = self.cf_handle(Order::Spog.cf())?;
        let iter = self
            .inner
            .db
            .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));

        // Latest version per graph, then newest first overall.
        let mut best: HashMap<GraphId, (Timestamp, EdgeId)> = HashMap::new();
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let tt = keys::key_tt(&key);
            if tt > snapshot_ts {
                continue;
            }
            if let Ok(DecodedValue::Relation { edge_id, .. }) = codec::decode_value(&value) {
                let g = Order::Spog.graph_of(&key);
                match best.get(&g) {
                    Some((prev, _)) if *prev >= tt => {}
                    _ => {
                        best.insert(g, (tt, edge_id));
                    }
                }
            }
        }
        let mut found: Vec<(Timestamp, EdgeId)> = best.into_values().collect();
        found.sort_by_key(|b| std::cmp::Reverse(b.0));
        let mut seen = HashSet::new();
        Ok(found
            .into_iter()
            .map(|(_, e)| e)
            .filter(|e| seen.insert(*e))
            .collect())
    }

    // ── snapshot scans ────────────────────────────────────────────────────────
    //
    // `vt_as_of: None` means "valid now": closed (deleted / replaced) facts are
    // hidden. Pass `Some(t)` for valid-time travel. Scans without a graph
    // argument cover every graph and de-duplicate on (s, p, o).

    /// All triples for `subject` visible at `snapshot_ts`.
    pub(crate) fn scan_by_subject_at(
        &self,
        subject: &NodeId,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let prefix = Order::Spog.prefix(Some(subject), None, None, None);
        self.scan_union(Order::Spog, &prefix, snapshot_ts, vt_as_of)
    }

    pub(crate) fn scan_by_subject_predicate_at(
        &self,
        subject: &NodeId,
        predicate: &str,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let Some(p) = self.predicate_id(predicate) else {
            return Ok(vec![]);
        };
        let prefix = Order::Spog.prefix(Some(subject), Some(p), None, None);
        self.scan_union(Order::Spog, &prefix, snapshot_ts, vt_as_of)
    }

    pub(crate) fn scan_by_predicate_at(
        &self,
        predicate: &str,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let Some(p) = self.predicate_id(predicate) else {
            return Ok(vec![]);
        };
        let prefix = Order::Psog.prefix(None, Some(p), None, None);
        self.scan_union(Order::Psog, &prefix, snapshot_ts, vt_as_of)
    }

    /// Triples with `(predicate, object)`. `object` may be a node or — for
    /// properties — a value's content hash ([`keys::value_object`]).
    pub(crate) fn scan_by_predicate_object_at(
        &self,
        predicate: &str,
        object: &NodeId,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let Some(p) = self.predicate_id(predicate) else {
            return Ok(vec![]);
        };
        let prefix = Order::Posg.prefix(None, Some(p), Some(object), None);
        self.scan_union(Order::Posg, &prefix, snapshot_ts, vt_as_of)
    }

    /// Property triples whose value equals `value` — an index lookup on the
    /// `posg` value hash, verified against the decoded value.
    pub(crate) fn scan_by_predicate_value_at(
        &self,
        predicate: &str,
        value: &Value,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let object = keys::value_object(value);
        let mut triples =
            self.scan_by_predicate_object_at(predicate, &object, snapshot_ts, vt_as_of)?;
        triples.retain(|t| matches!(t, Triple::Property { value: v, .. } if v == value));
        Ok(triples)
    }

    pub(crate) fn scan_by_object_at(
        &self,
        object: &NodeId,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let prefix = Order::Ospg.prefix(None, None, Some(object), None);
        self.scan_union(Order::Ospg, &prefix, snapshot_ts, vt_as_of)
    }

    /// All triples for (subject, object) visible at `snapshot_ts` — uses `sopg`.
    pub(crate) fn scan_by_subject_object_at(
        &self,
        subject: &NodeId,
        object: &NodeId,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let prefix = Order::Sopg.prefix(Some(subject), None, Some(object), None);
        self.scan_union(Order::Sopg, &prefix, snapshot_ts, vt_as_of)
    }

    /// All triples in the store visible at `snapshot_ts` — full `spog` scan.
    pub(crate) fn scan_all_at(
        &self,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        self.scan_union(Order::Spog, &[], snapshot_ts, vt_as_of)
    }

    /// Every triple in graph `g` — a `gspo` prefix scan.
    pub(crate) fn scan_graph_at(
        &self,
        g: GraphId,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let prefix = Order::Gspo.prefix(None, None, None, Some(g));
        Ok(self
            .snapshot_scan(
                Order::Gspo.cf(),
                Order::Gspo,
                &prefix,
                snapshot_ts,
                vt_as_of,
                &GraphScope::One(g),
            )?
            .into_iter()
            .map(|(_, t)| t)
            .collect())
    }

    /// Triples of `subject` in graph `g` — a `gspo` prefix scan.
    pub(crate) fn scan_by_subject_in_graph_at(
        &self,
        g: GraphId,
        subject: &NodeId,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let prefix = Order::Gspo.prefix(Some(subject), None, None, Some(g));
        Ok(self
            .snapshot_scan(
                Order::Gspo.cf(),
                Order::Gspo,
                &prefix,
                snapshot_ts,
                vt_as_of,
                &GraphScope::One(g),
            )?
            .into_iter()
            .map(|(_, t)| t)
            .collect())
    }

    /// Triples matching the bound slots within `scope`, each with its graph.
    ///
    /// `object` may be a node or a property's value hash. Picks the order
    /// whose prefix covers the most bound slots: `gspo` / `gpos` for a single
    /// graph, otherwise the usual subject/predicate/object orders with the
    /// scope applied to each key's graph slot. Results are not de-duplicated
    /// across graphs.
    pub(crate) fn scan_scoped_at(
        &self,
        subject: Option<&NodeId>,
        predicate: Option<&str>,
        object: Option<&NodeId>,
        scope: &GraphScope,
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<(GraphId, Triple)>, StorageError> {
        let p = match predicate {
            Some(name) => match self.predicate_id(name) {
                Some(id) => Some(id),
                None => return Ok(vec![]),
            },
            None => None,
        };
        let (s, o) = (subject, object);
        let (order, prefix) = match (scope, s, p, o) {
            (GraphScope::One(g), Some(_), _, _) => {
                // gspo: [g][s][p][o] — o only usable when p is bound.
                (Order::Gspo, Order::Gspo.prefix(s, p, o, Some(*g)))
            }
            (GraphScope::One(g), None, Some(_), _) => {
                (Order::Gpos, Order::Gpos.prefix(None, p, o, Some(*g)))
            }
            (GraphScope::One(_), None, None, Some(_)) => {
                (Order::Ospg, Order::Ospg.prefix(None, None, o, None))
            }
            (GraphScope::One(g), None, None, None) => {
                (Order::Gspo, Order::Gspo.prefix(None, None, None, Some(*g)))
            }
            (_, Some(_), Some(_), _) => (Order::Spog, Order::Spog.prefix(s, p, o, None)),
            (_, Some(_), None, Some(_)) => (Order::Sopg, Order::Sopg.prefix(s, None, o, None)),
            (_, Some(_), None, None) => (Order::Spog, Order::Spog.prefix(s, None, None, None)),
            (_, None, Some(_), _) => (Order::Posg, Order::Posg.prefix(None, p, o, None)),
            (_, None, None, Some(_)) => (Order::Ospg, Order::Ospg.prefix(None, None, o, None)),
            (_, None, None, None) => (Order::Spog, Order::Spog.prefix(None, None, None, None)),
        };
        let mut quads =
            self.snapshot_scan_keyed(order.cf(), order, &prefix, snapshot_ts, vt_as_of, scope)?;
        // A prefix may cover fewer slots than are bound; filter the rest.
        quads.retain(|((qs, qp, qo, _), _)| {
            s.map_or(true, |s| s == qs)
                && p.map_or(true, |p| p == *qp)
                && o.map_or(true, |o| o == qo)
        });
        Ok(quads.into_iter().map(|((_, _, _, g), t)| (g, t)).collect())
    }

    // ── snapshot scan implementation ──────────────────────────────────────────

    /// Scan across all graphs and de-duplicate on (s, p, o).
    fn scan_union(
        &self,
        order: Order,
        prefix: &[u8],
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
    ) -> Result<Vec<Triple>, StorageError> {
        let quads = self.snapshot_scan_keyed(
            order.cf(),
            order,
            prefix,
            snapshot_ts,
            vt_as_of,
            &GraphScope::Union,
        )?;
        // With no named graphs every quad is in the default graph, so there
        // is nothing to de-duplicate.
        if self.inner.graph_fwd.read().unwrap().is_empty() {
            return Ok(quads.into_iter().map(|(_, t)| t).collect());
        }
        // Keep one triple per (s, p, o): the one from the lowest graph id.
        let mut by_spo: HashMap<(NodeId, PredId, NodeId), (GraphId, Triple)> =
            HashMap::with_capacity(quads.len());
        for ((s, p, o, g), t) in quads {
            match by_spo.get(&(s, p, o)) {
                Some((prev, _)) if *prev <= g => {}
                _ => {
                    by_spo.insert((s, p, o), (g, t));
                }
            }
        }
        Ok(by_spo.into_values().map(|(_, t)| t).collect())
    }

    /// Generic snapshot scan over one quad order:
    ///
    /// 1. Prefix-scan `cf_name` (keys in `order`'s layout), skipping graphs
    ///    `graphs` doesn't admit.
    /// 2. Discard entries with `tt > snapshot_ts` (not yet committed at our snapshot).
    /// 3. Per quad, the governing version as of valid time `vt` (`vt_as_of`,
    ///    or now when `None`) is the one with the latest `vt_start <= vt`,
    ///    ties broken by highest `tt`. This distinguishes:
    ///      - a **new temporal version** (a later `vt_start`), which only takes
    ///        over once its own window has begun — earlier `vt` points still
    ///        resolve to the older version; and
    ///      - a **DELETE / correction / replaced value**, which reuses the
    ///        original `vt_start` with an earlier `vt_end`, so it wins the
    ///        tie-break and hides the fact rather than letting the older,
    ///        still-open-ended version win.
    /// 4. The winner is returned only if its window covers `vt` (`vt < vt_end`).
    /// 5. Reconstruct `Triple`s (resolving out-of-line values).
    pub(crate) fn snapshot_scan(
        &self,
        cf_name: &str,
        order: Order,
        prefix: &[u8],
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
        graphs: &GraphScope,
    ) -> Result<Vec<(GraphId, Triple)>, StorageError> {
        Ok(self
            .snapshot_scan_keyed(cf_name, order, prefix, snapshot_ts, vt_as_of, graphs)?
            .into_iter()
            .map(|((_, _, _, g), t)| (g, t))
            .collect())
    }

    /// [`Self::snapshot_scan`], keeping each result's `(s, p, o, g)` ids.
    fn snapshot_scan_keyed(
        &self,
        cf_name: &str,
        order: Order,
        prefix: &[u8],
        snapshot_ts: Timestamp,
        vt_as_of: Option<i64>,
        graphs: &GraphScope,
    ) -> Result<Vec<(QuadIds, Triple)>, StorageError> {
        let vt = vt_as_of.unwrap_or_else(|| Timestamp::now().0);
        let cf = self.cf_handle(cf_name)?;
        let iter = self
            .inner
            .db
            .iterator_cf(&cf, IteratorMode::From(prefix, Direction::Forward));

        let mut latest: HashMap<QuadIds, VersionSlot> = HashMap::new();

        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(prefix) {
                break;
            }
            // Cheap checks before decoding: tt, then graph.
            if keys::key_tt(&key) > snapshot_ts || !graphs.admits(order.graph_of(&key)) {
                continue;
            }
            let Some((vt_start, vt_end)) = codec::valid_time(&value) else {
                continue;
            };
            if vt_start.0 > vt {
                continue;
            }
            let q = order.decode(&key)?;
            let tt = q.tt;
            let slot = latest.entry((q.s, q.p, q.o, q.g)).or_insert((
                i64::MIN,
                Timestamp(i64::MIN),
                i64::MIN,
                vec![],
            ));
            if vt_start.0 > slot.0 || (vt_start.0 == slot.0 && tt > slot.1) {
                *slot = (vt_start.0, tt, vt_end.0, value.to_vec());
            }
        }

        let mut out = Vec::with_capacity(latest.len());
        for ((s, p, o, g), (_vt_start, tt, vt_end, value_bytes)) in latest {
            if vt >= vt_end {
                continue;
            }
            out.push(((s, p, o, g), self.reconstruct(s, p, o, tt, &value_bytes)?));
        }
        Ok(out)
    }

    // ── internal write helpers ────────────────────────────────────────────────

    /// Write one quad version into all eight orders.
    ///
    /// `handles` are the eight order CFs in [`Order::ALL`] order (see
    /// [`Self::order_handles`]), fetched once per batch.
    pub(crate) fn batch_quad(
        batch: &mut WriteBatch,
        handles: &[Arc<BoundColumnFamily<'_>>],
        q: &QuadKey,
        value: &[u8],
    ) {
        for (order, cf) in Order::ALL.iter().zip(handles) {
            batch.put_cf(cf, order.encode(q), value);
        }
    }

    /// Handles of the eight quad-order CFs, in [`Order::ALL`] order.
    pub(crate) fn order_handles(&self) -> Result<Vec<Arc<BoundColumnFamily<'_>>>, StorageError> {
        Order::ALL.iter().map(|o| self.cf_handle(o.cf())).collect()
    }

    /// The latest committed version of every property value of `(s, p, g)`
    /// that is still open at `at` — the values a `Replace` write closes.
    ///
    /// `gspo` is a raw iterator over the `gspo` CF, reused across calls.
    fn open_values(
        gspo: &mut rocksdb::DBRawIteratorWithThreadMode<'_, DB>,
        s: &NodeId,
        p: PredId,
        g: GraphId,
        at: Timestamp,
    ) -> Result<Vec<(NodeId, Vec<u8>)>, StorageError> {
        let prefix = Order::Gspo.prefix(Some(s), Some(p), None, Some(g));
        gspo.seek(prefix);
        // Keys sort oldest-first within a value, so the last one seen wins.
        let mut latest: Vec<(NodeId, Vec<u8>)> = Vec::new();
        while let (Some(key), Some(value)) = (gspo.key(), gspo.value()) {
            if !key.starts_with(&prefix) {
                break;
            }
            let o = Order::Gspo.decode(key)?.o;
            match latest.last_mut() {
                Some((prev, bytes)) if *prev == o => *bytes = value.to_vec(),
                _ => latest.push((o, value.to_vec())),
            }
            gspo.next();
        }
        gspo.status()?;
        Ok(latest
            .into_iter()
            .filter(|(_, bytes)| {
                bytes.first() != Some(&codec::DISC_RELATION)
                    && codec::valid_time(bytes).is_some_and(|(_, end)| end > at)
            })
            .collect())
    }

    /// Encode `writes` at transaction time `tt` into `batch`: every index
    /// entry, out-of-line values, trigrams, annotations, and — for `Replace`
    /// property writes — closing versions of the other open values of the
    /// same `(subject, predicate, graph)`. Conflict checks are the caller's.
    pub(crate) fn stage_writes(
        &self,
        batch: &mut WriteBatch,
        writes: &[PendingWrite],
        tt: Timestamp,
    ) -> Result<(), StorageError> {
        // Property values staged in this batch, per (s, p, g): a later Replace
        // in the same batch must close them too.
        let mut staged: StagedValues = HashMap::new();
        let handles = self.order_handles()?;
        let gspo_cf = self.cf_handle(Order::Gspo.cf())?;
        let mut gspo = self.inner.db.raw_iterator_cf(&gspo_cf);

        for w in writes {
            let temporal = BiTemporalRange {
                tt,
                ..*w.triple.temporal()
            };
            let p = self.intern_predicate(w.triple.predicate().0.as_str())?;
            let g = w.graph;
            match &w.triple {
                Triple::Relation {
                    subject,
                    object,
                    edge_id,
                    ..
                } => {
                    let q = QuadKey {
                        s: *subject,
                        p,
                        o: *object,
                        g,
                        tt,
                    };
                    Self::batch_quad(
                        batch,
                        &handles,
                        &q,
                        &codec::encode_relation(edge_id, &temporal),
                    );
                }
                Triple::Property { subject, value, .. } => {
                    let o = keys::value_object(value);
                    let bytes = self.encode_property_entry(batch, value, &temporal)?;
                    let slot = staged.entry((*subject, p, g)).or_default();
                    if w.mode.resolve(&w.triple) == WriteMode::Replace {
                        let closing_at = temporal.vt_start;
                        let mut to_close = Self::open_values(&mut gspo, subject, p, g, closing_at)?;
                        to_close.append(slot);
                        for (other, other_bytes) in to_close {
                            if other == o {
                                continue;
                            }
                            let q = QuadKey {
                                s: *subject,
                                p,
                                o: other,
                                g,
                                tt,
                            };
                            let closed = codec::with_vt_end(&other_bytes, closing_at)?;
                            Self::batch_quad(batch, &handles, &q, &closed);
                        }
                    }
                    slot.push((o, bytes.clone()));
                    let q = QuadKey {
                        s: *subject,
                        p,
                        o,
                        g,
                        tt,
                    };
                    Self::batch_quad(batch, &handles, &q, &bytes);
                    if let Some(text) = value.as_text() {
                        self.batch_text_trigrams(batch, subject, p, g, text)?;
                    }
                }
                Triple::EdgeProperty { edge, value, .. } => {
                    let value_bytes = codec::encode_property(value, &temporal)?;
                    self.batch_epa(batch, *edge, p, g, tt, &value_bytes)?;
                }
                Triple::EdgeRelation { edge, object, .. } => {
                    let epo_val = encode_epo_value(&temporal);
                    self.batch_epo(batch, *edge, p, *object, g, tt, &epo_val)?;
                }
            }
        }
        Ok(())
    }

    // ── IRI dictionary ────────────────────────────────────────────────────────

    /// Add an IRI dictionary entry for `iri` to `batch`.
    ///
    /// No-op for `urn:uuid:` IRIs and for IRIs already stored. `pending`
    /// carries entries added earlier in the same batch. Returns
    /// [`StorageError::IriCollision`] if a different IRI already names the node.
    pub(crate) fn batch_iri(
        &self,
        batch: &mut WriteBatch,
        iri: &str,
        pending: &mut HashMap<NodeId, String>,
    ) -> Result<(), StorageError> {
        if !term::needs_dictionary(iri) {
            return Ok(());
        }
        let node = term::iri_to_node_id(iri);
        let existing = match pending.get(&node) {
            Some(p) => Some(p.clone()),
            None => self.iri_of(&node)?,
        };
        match existing {
            Some(e) if e == iri => Ok(()),
            Some(e) => Err(StorageError::IriCollision {
                node,
                existing: e,
                new: iri.to_string(),
            }),
            None => {
                batch.put_cf(&self.cf_handle(cf::IRI)?, node.as_bytes(), iri.as_bytes());
                pending.insert(node, iri.to_string());
                Ok(())
            }
        }
    }

    /// The IRI stored for `node`, if any. `urn:uuid:` nodes have no entry —
    /// use [`term::fallback_iri`].
    pub fn iri_of(&self, node: &NodeId) -> Result<Option<String>, StorageError> {
        let cf = self.cf_handle(cf::IRI)?;
        match self.inner.db.get_cf(&cf, node.as_bytes())? {
            Some(bytes) => Ok(Some(String::from_utf8(bytes).map_err(|e| {
                StorageError::KeyDecode(format!("IRI for {node} is not UTF-8: {e}"))
            })?)),
            None => Ok(None),
        }
    }

    /// Stored IRIs for `nodes`; nodes without an entry are omitted.
    pub fn iris_of(&self, nodes: &[NodeId]) -> Result<HashMap<NodeId, String>, StorageError> {
        let cf = self.cf_handle(cf::IRI)?;
        let results = self
            .inner
            .db
            .multi_get_cf(nodes.iter().map(|n| (&cf, n.as_bytes())));
        let mut out = HashMap::new();
        for (node, res) in nodes.iter().zip(results) {
            if let Some(bytes) = res? {
                let iri = String::from_utf8(bytes).map_err(|e| {
                    StorageError::KeyDecode(format!("IRI for {node} is not UTF-8: {e}"))
                })?;
                out.insert(*node, iri);
            }
        }
        Ok(out)
    }

    /// Record IRIs in the dictionary in their own write (no triples).
    /// Used by offline tools; transactional callers use `Transaction::bind_iri`.
    pub fn bind_iris<'a>(
        &self,
        iris: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), StorageError> {
        if self.is_replica() {
            return Err(Self::read_only_err());
        }
        let mut batch = WriteBatch::default();
        let mut pending = HashMap::new();
        for iri in iris {
            self.batch_iri(&mut batch, iri, &mut pending)?;
        }
        if !batch.is_empty() {
            self.db_write(batch)?;
        }
        Ok(())
    }

    // ── trigram index ─────────────────────────────────────────────────────────

    /// Write trigram index entries for a text property into a caller-supplied batch.
    ///
    /// Appends one key per trigram extracted from `text` to the `tri` CF.
    /// Called from every write path (`Transaction::commit()`, `insert_at_ts`,
    /// SST import) for each `Triple::Property { value: Text(..) }`. Values
    /// longer than [`TRIGRAM_MAX_TEXT_BYTES`] are not indexed.
    pub(crate) fn batch_text_trigrams(
        &self,
        batch: &mut WriteBatch,
        subject: &NodeId,
        pred_id: PredId,
        g: GraphId,
        text: &str,
    ) -> Result<(), StorageError> {
        if text.len() > TRIGRAM_MAX_TEXT_BYTES {
            return Ok(());
        }
        let tri_cf = self.cf_handle(cf::TRI)?;
        for trigram in keys::extract_trigrams(text) {
            batch.put_cf(&tri_cf, keys::encode_tri(trigram, pred_id, g, subject), b"");
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
        pred_id: PredId,
        g: GraphId,
        tt: Timestamp,
        value_bytes: &[u8],
    ) -> Result<(), StorageError> {
        batch.put_cf(
            &self.cf_handle(cf::EPA)?,
            keys::encode_epa_key(&edge, pred_id, g, tt),
            value_bytes,
        );
        batch.put_cf(
            &self.cf_handle(cf::PEA)?,
            keys::encode_pea_key(pred_id, &edge, g, tt),
            value_bytes,
        );
        Ok(())
    }

    /// Write one EPO (edge relation annotation) entry into a caller-supplied batch.
    ///
    /// Value layout: `[vt_start BE(8)][vt_end BE(8)]` = 16 bytes.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn batch_epo(
        &self,
        batch: &mut WriteBatch,
        edge: EdgeId,
        pred_id: PredId,
        object: NodeId,
        g: GraphId,
        tt: Timestamp,
        value_bytes: &[u8],
    ) -> Result<(), StorageError> {
        batch.put_cf(
            &self.cf_handle(cf::EPO)?,
            keys::encode_epo_key(&edge, pred_id, &object, g, tt),
            value_bytes,
        );
        Ok(())
    }

    // ── RDF-star scan methods ─────────────────────────────────────────────────
    //
    // Annotations are read across all graphs; the newest version per
    // (predicate[, object]) wins.

    /// Scan all annotations (property + relation) on a given edge visible at `snapshot_ts`.
    ///
    /// Returns a deduplicated list: for each predicate in EPA and each
    /// (predicate, object) in EPO, only the entry with the highest
    /// `tt <= snapshot_ts` is returned.
    pub fn scan_edge_annotations(
        &self,
        edge: EdgeId,
        snapshot_ts: Timestamp,
    ) -> Result<Vec<EdgeAnnotation>, StorageError> {
        let mut result = Vec::new();
        let prefix = keys::annotation_prefix_edge(&edge);

        // ── EPA (property annotations) ────────────────────────────────────────
        {
            let cf = self.cf_handle(cf::EPA)?;
            let iter = self
                .inner
                .db
                .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));

            let mut latest: HashMap<PredId, (Timestamp, Vec<u8>)> = HashMap::new();
            for item in iter {
                let (key, value) = item?;
                if !key.starts_with(&prefix) {
                    break;
                }
                let dk = keys::decode_epa_key(&key)?;
                if dk.tt > snapshot_ts {
                    continue;
                }
                let slot = latest
                    .entry(dk.pred_id)
                    .or_insert((Timestamp(i64::MIN), vec![]));
                if dk.tt > slot.0 {
                    *slot = (dk.tt, value.to_vec());
                }
            }

            for (pred_id, (_, value_bytes)) in latest {
                let pred_str = self
                    .predicate_string(pred_id)
                    .ok_or_else(|| StorageError::KeyDecode(format!("unknown pred_id {pred_id}")))?;
                if let DecodedValue::Property { value, .. } = codec::decode_value(&value_bytes)? {
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
            let iter = self
                .inner
                .db
                .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));

            let mut latest: HashMap<(PredId, NodeId), Timestamp> = HashMap::new();
            for item in iter {
                let (key, _value) = item?;
                if !key.starts_with(&prefix) {
                    break;
                }
                let dk = keys::decode_epo_key(&key)?;
                if dk.tt > snapshot_ts {
                    continue;
                }
                let slot = latest
                    .entry((dk.pred_id, dk.object))
                    .or_insert(Timestamp(i64::MIN));
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
        let Some(pred_id) = self.predicate_id(predicate) else {
            return Ok(None);
        };
        let prefix = keys::annotation_prefix_edge_pred(&edge, pred_id);

        // Check EPA.
        {
            let cf = self.cf_handle(cf::EPA)?;
            let iter = self
                .inner
                .db
                .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));
            let mut best: Option<(Timestamp, Vec<u8>)> = None;
            for item in iter {
                let (key, value) = item?;
                if !key.starts_with(&prefix) {
                    break;
                }
                let dk = keys::decode_epa_key(&key)?;
                if dk.tt > snapshot_ts {
                    continue;
                }
                match &best {
                    Some((bt, _)) if dk.tt <= *bt => {}
                    _ => best = Some((dk.tt, value.to_vec())),
                }
            }
            if let Some((_, value_bytes)) = best {
                if let DecodedValue::Property { value, .. } = codec::decode_value(&value_bytes)? {
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
            let iter = self
                .inner
                .db
                .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));
            let mut best: Option<(Timestamp, NodeId)> = None;
            for item in iter {
                let (key, _) = item?;
                if !key.starts_with(&prefix) {
                    break;
                }
                let dk = keys::decode_epo_key(&key)?;
                if dk.tt > snapshot_ts {
                    continue;
                }
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
        let Some(pred_id) = self.predicate_id(predicate) else {
            return Ok(vec![]);
        };

        let cf = self.cf_handle(cf::PEA)?;
        let prefix = keys::pea_prefix_pred(pred_id);
        let iter = self
            .inner
            .db
            .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));

        // Deduplicate by edge_id: keep highest tt ≤ snapshot_ts.
        let mut latest: HashMap<EdgeId, (Timestamp, Vec<u8>)> = HashMap::new();
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let dk = keys::decode_pea_key(&key)?;
            if dk.tt > snapshot_ts {
                continue;
            }
            let slot = latest
                .entry(dk.edge_id)
                .or_insert((Timestamp(i64::MIN), vec![]));
            if dk.tt > slot.0 {
                *slot = (dk.tt, value.to_vec());
            }
        }

        let mut triples = Vec::with_capacity(latest.len());
        let pred = Predicate::new(predicate.to_owned());
        for (edge_id, (tt, value_bytes)) in latest {
            if let DecodedValue::Property { value, temporal } = codec::decode_value(&value_bytes)? {
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

    /// Return the historical values of a node property, newest-first by
    /// transaction time, across all graphs.
    ///
    /// Unlike the snapshot-based scans this does **not** deduplicate: every
    /// committed write to `(subject, predicate)` is returned so callers can
    /// inspect the full audit trail — except the closing versions a `Replace`
    /// writes for the values it supersedes (same `tt` as the new open value),
    /// which record the replacement rather than a write of their own.
    ///
    /// `limit` is the maximum number of versions to return; 0 means 50.
    pub fn scan_property_history(
        &self,
        subject: NodeId,
        predicate: &str,
        limit: u32,
    ) -> Result<Vec<(Value, i64)>, StorageError> {
        let limit = if limit == 0 { 50 } else { limit as usize };
        let Some(pred_id) = self.predicate_id(predicate) else {
            return Ok(vec![]);
        };

        let prefix = Order::Spog.prefix(Some(&subject), Some(pred_id), None, None);
        let cf = self.cf_handle(Order::Spog.cf())?;
        let iter = self
            .inner
            .db
            .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));

        // (tt, open?, object slot, value bytes) for every property version.
        let mut versions: Vec<(i64, bool, NodeId, Vec<u8>)> = Vec::new();
        for item in iter {
            let (key, value_bytes) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            if value_bytes.first() == Some(&codec::DISC_RELATION) {
                continue;
            }
            let Some((_, vt_end)) = codec::valid_time(&value_bytes) else {
                continue;
            };
            let q = Order::Spog.decode(&key)?;
            versions.push((
                q.tt.0,
                vt_end == Timestamp::END_OF_TIME,
                q.o,
                value_bytes.to_vec(),
            ));
        }

        // A closed version committed alongside an open version of another
        // value is a Replace's closing entry, not a write of its own.
        let open_at: HashSet<i64> = versions.iter().filter(|v| v.1).map(|v| v.0).collect();
        versions.retain(|(tt, open, _, _)| *open || !open_at.contains(tt));
        versions.sort_by_key(|b| std::cmp::Reverse(b.0));

        let mut out = Vec::new();
        for (tt, _, o, bytes) in versions.into_iter().take(limit) {
            let value = match codec::decode_value(&bytes)? {
                DecodedValue::Property { value, .. } => value,
                DecodedValue::PropertyRef { .. } => self.load_blob(&o)?,
                DecodedValue::Relation { .. } => continue,
            };
            out.push((value, tt));
        }
        Ok(out)
    }

    /// Prefix-scan the trigram CF for all subjects that contain `trigram`
    /// under `pred_id`, in any graph.
    pub fn scan_trigram_candidates(&self, pred_id: PredId, trigram: [u8; 3]) -> Vec<NodeId> {
        let prefix = keys::tri_prefix_tp(trigram, pred_id);
        let cf = match self.cf_handle(cf::TRI) {
            Ok(cf) => cf,
            Err(_) => return vec![],
        };
        let iter = self
            .inner
            .db
            .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));
        let mut candidates = Vec::new();
        for item in iter {
            let Ok((key, _)) = item else { break };
            if !key.starts_with(&prefix) {
                break;
            }
            if let Ok((_, node_id)) = keys::decode_tri(&key) {
                candidates.push(node_id);
            }
        }
        candidates
    }

    /// Search for nodes whose `predicate` text value contains all trigrams of `query`.
    ///
    /// Only values of at most [`TRIGRAM_MAX_TEXT_BYTES`] are indexed, so longer
    /// values are never returned.
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
        use polargraph_core::triple::Triple;
        let query_lower = query.to_lowercase();
        let mut result = Vec::new();
        for node_id in candidates {
            let triples =
                self.scan_by_subject_predicate_at(&node_id, predicate, snapshot_ts, vt_as_of)?;
            let confirmed = triples.iter().any(|t| match t {
                Triple::Property { value, .. } => value
                    .as_text()
                    .is_some_and(|text| text.to_lowercase().contains(&query_lower)),
                _ => false,
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

    /// Insert a triple (default graph, [`WriteMode::Auto`]) at an explicit
    /// transaction timestamp, bypassing the oracle and conflict checks.
    ///
    /// Advances the oracle to at least `tt` so that subsequent reads see this
    /// triple. Useful in tests and offline tools that need control over `tt`.
    pub fn insert_at_ts(&self, triple: &Triple, tt: Timestamp) -> Result<(), StorageError> {
        if self.is_replica() {
            return Err(Self::read_only_err());
        }
        let mut batch = WriteBatch::default();
        let write = PendingWrite {
            triple: triple.clone(),
            graph: GraphId::DEFAULT,
            mode: WriteMode::Auto,
        };
        self.stage_writes(&mut batch, std::slice::from_ref(&write), tt)?;
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
        self.inner
            .db
            .compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }

    /// Walk a CF in key order without snapshot filtering, grouping consecutive
    /// entries whose first `group_len` key bytes are equal, and delete the
    /// entries `select` picks from each group.
    ///
    /// For quad CFs with `group_len = keys::QUAD_TUPLE_LEN`, each
    /// group is every stored version of one quad, oldest `tt` first.
    /// `select` returns indices into the group slice. Used by
    /// `CompactionManager`. Returns `(entries_scanned, entries_deleted)`.
    pub fn prune_cf_groups<F>(
        &self,
        cf_name: &str,
        group_len: usize,
        mut select: F,
    ) -> Result<(usize, usize), StorageError>
    where
        F: FnMut(&[RawEntry]) -> Vec<usize>,
    {
        // Deletes are flushed in chunks to bound memory on large CFs. The
        // iterator reads from an implicit snapshot, so flushing mid-scan is safe.
        const FLUSH_EVERY: usize = 10_000;

        if self.is_replica() {
            return Err(Self::read_only_err());
        }
        let cf = self.cf_handle(cf_name)?;
        let iter = self.inner.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

        let mut scanned = 0usize;
        let mut deleted = 0usize;
        let mut group: Vec<RawEntry> = Vec::new();
        let mut batch = WriteBatch::default();

        let mut flush_group = |group: &mut Vec<RawEntry>, batch: &mut WriteBatch| -> usize {
            if group.is_empty() {
                return 0;
            }
            let picked = select(group);
            for &i in &picked {
                batch.delete_cf(&cf, &group[i].0);
            }
            group.clear();
            picked.len()
        };

        for item in iter {
            let (k, v) = item?;
            scanned += 1;
            let same_group = group.first().is_some_and(|(g, _)| {
                g.len() >= group_len && k.len() >= group_len && g[..group_len] == k[..group_len]
            });
            if !same_group {
                deleted += flush_group(&mut group, &mut batch);
                if batch.len() >= FLUSH_EVERY {
                    self.db_write(std::mem::take(&mut batch))?;
                }
            }
            group.push((k, v));
        }
        deleted += flush_group(&mut group, &mut batch);
        if !batch.is_empty() {
            self.db_write(batch)?;
        }
        Ok((scanned, deleted))
    }

    /// Delete `blob` entries no `spog` entry references (mark-and-sweep).
    /// Returns the number deleted. Called by retention after it deletes
    /// index entries; blobs are content-addressed, so a still-referenced
    /// blob is never touched.
    pub fn sweep_unreferenced_blobs(&self) -> Result<usize, StorageError> {
        if self.is_replica() {
            return Err(Self::read_only_err());
        }
        // Mark: every value hash behind a PropertyRef.
        let mut live: HashSet<[u8; 16]> = HashSet::new();
        let spog = self.cf_handle(Order::Spog.cf())?;
        for item in self.inner.db.iterator_cf(&spog, IteratorMode::Start) {
            let (key, value) = item?;
            if value.first() == Some(&codec::DISC_PROPERTY_REF) {
                live.insert(*Order::Spog.decode(&key)?.o.as_bytes());
            }
        }
        // Sweep.
        let blob = self.cf_handle(cf::BLOB)?;
        let mut batch = WriteBatch::default();
        let mut deleted = 0;
        for item in self.inner.db.iterator_cf(&blob, IteratorMode::Start) {
            let (key, _) = item?;
            let hash: [u8; 16] = match key.as_ref().try_into() {
                Ok(h) => h,
                Err(_) => continue,
            };
            if !live.contains(&hash) {
                batch.delete_cf(&blob, key);
                deleted += 1;
            }
        }
        if deleted > 0 {
            self.db_write(batch)?;
        }
        Ok(deleted)
    }

    // ── Derived triple store (DRV CF) ─────────────────────────────────────────

    /// Insert a batch of derived (inferred) Relation triples into the DRV CF.
    ///
    /// Uses the `spog` key layout (default graph). Each fact is written
    /// with a timestamp of `Timestamp::now()` so that subsequent `scan_derived()`
    /// calls see it. Deduplication (same S,P,O already in DRV) is left to the
    /// caller — the materializer builds its own in-memory dedup set.
    pub fn insert_derived_batch(
        &self,
        facts: &[(NodeId, PredId, NodeId)],
    ) -> Result<(), StorageError> {
        if self.is_replica() {
            return Err(Self::read_only_err());
        }
        if facts.is_empty() {
            return Ok(());
        }
        let tt = Timestamp::now();
        let drv_cf = self.cf_handle(cf::DRV)?;
        let edge_id = polargraph_core::id::EdgeId(uuid::Uuid::from_bytes([0u8; 16]));
        let temporal = BiTemporalRange::assert_now(tt);
        let value_bytes = codec::encode_relation(&edge_id, &temporal);
        let mut batch = WriteBatch::default();
        for &(s, p, o) in facts {
            let q = QuadKey {
                s,
                p,
                o,
                g: GraphId::DEFAULT,
                tt,
            };
            batch.put_cf(&drv_cf, Order::Spog.encode(&q), &value_bytes);
        }
        self.inner.db.write(batch)?;
        self.inner.oracle.advance_to(tt);
        Ok(())
    }

    /// Delete all entries from the DRV column family.
    ///
    /// Called at the start of a fresh materialization run to ensure the derived
    /// store is rebuilt from scratch without stale facts.
    pub fn clear_derived(&self) -> Result<(), StorageError> {
        if self.is_replica() {
            return Err(Self::read_only_err());
        }
        let drv_cf = self.cf_handle(cf::DRV)?;
        let iter = self
            .inner
            .db
            .iterator_cf(&drv_cf, rocksdb::IteratorMode::Start);
        let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();
        for item in iter {
            let (k, _) = item?;
            keys_to_delete.push(k.to_vec());
        }
        if !keys_to_delete.is_empty() {
            let mut batch = WriteBatch::default();
            for key in keys_to_delete {
                batch.delete_cf(&drv_cf, &key);
            }
            self.inner.db.write(batch)?;
        }
        Ok(())
    }

    /// Scan all derived (inferred) Relation triples visible at the current oracle timestamp.
    pub fn scan_derived(&self) -> Result<Vec<Triple>, StorageError> {
        self.scan_derived_at(self.inner.oracle.read_ts())
    }

    /// Scan all derived (inferred) Relation triples visible at `snapshot_ts`.
    pub fn scan_derived_at(&self, snapshot_ts: Timestamp) -> Result<Vec<Triple>, StorageError> {
        Ok(self
            .snapshot_scan(
                cf::DRV,
                Order::Spog,
                &[],
                snapshot_ts,
                None,
                &GraphScope::Union,
            )?
            .into_iter()
            .map(|(_, t)| t)
            .collect())
    }

    /// Approximate count of derived triples in the DRV CF (for Prometheus gauge).
    pub fn estimate_derived_count(&self) -> u64 {
        self.cf_approx_key_count(cf::DRV)
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
            DecodedValue::Relation {
                edge_id,
                mut temporal,
            } => {
                temporal.tt = tt;
                Triple::Relation {
                    subject,
                    predicate,
                    object,
                    edge_id,
                    temporal,
                }
            }
            DecodedValue::Property {
                value,
                mut temporal,
            } => {
                temporal.tt = tt;
                Triple::Property {
                    subject,
                    predicate,
                    value,
                    temporal,
                }
            }
            // Out-of-line value: the object slot is its content hash.
            DecodedValue::PropertyRef { mut temporal } => {
                temporal.tt = tt;
                Triple::Property {
                    subject,
                    predicate,
                    value: self.load_blob(&object)?,
                    temporal,
                }
            }
        };
        Ok(triple)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflicting_iri_for_a_node_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        let iri = "http://example.org/Alice";
        let node = term::iri_to_node_id(iri);

        // Simulate an xxHash3-128 collision: another IRI already names `node`.
        let cf = store.cf_handle(cf::IRI).unwrap();
        store
            .inner
            .db
            .put_cf(&cf, node.as_bytes(), b"http://example.org/Other")
            .unwrap();

        match store.bind_iris([iri]) {
            Err(StorageError::IriCollision { existing, new, .. }) => {
                assert_eq!(existing, "http://example.org/Other");
                assert_eq!(new, iri);
            }
            other => panic!("expected IriCollision, got {other:?}"),
        }
        // The same batch can't bind two different IRIs to one node either.
        let mut batch = WriteBatch::default();
        let mut pending = HashMap::new();
        pending.insert(node, "http://example.org/Pending".to_string());
        assert!(store.batch_iri(&mut batch, iri, &mut pending).is_err());
    }
}
