//! Pure-Rust HNSW (Hierarchical Navigable Small World) vector index.
//!
//! # Algorithm
//!
//! Multi-layer graph where each node exists at layers 0..=level (level chosen
//! randomly with exponential distribution). Greedy search descends from the
//! top layer to layer 0, using ef_construction candidates at construction time.
//!
//! Distance metric: cosine distance = 1 - cosine_similarity (lower = more similar).
//!
//! # Storage modes
//!
//! Each `HnswIndex` operates in one of two vector storage modes:
//!
//! - **Memory** (default): raw `Vec<f32>` stored inside each `HnswNode`. All
//!   vectors are loaded into heap RAM at `TripleStore::open` and held for the
//!   lifetime of the index.
//!
//! - **Mmap**: vectors are appended to a flat binary file
//!   (`<data_dir>/vectors/<space>.vecs`) and accessed through `memmap2::MmapMut`.
//!   The OS pages individual vectors in/out on demand. The in-memory `HnswNode`
//!   stores an empty vector; distance computations read directly from the mapped
//!   region without copying.
//!
//! # int8 quantization (step 9c)
//!
//! A space created with [`SpaceOptions::int8`] keeps each vector as `i8`
//! codes (`scale = max|x| / 127`, one scale per vector) plus the codes'
//! norm in RAM, and its full `f32` vectors in the mmap `.vecs` file. Graph
//! traversal (insert and search) uses cosine on the codes — the per-vector
//! scale cancels out of cosine — and search re-ranks its `max(ef, k)`
//! candidates with the exact `f32` vectors, so returned scores are exact.
//! Codes persist under `<space>/q/<id>`; the marker `<space>/__q` records
//! that the space is quantized. ~4× less vector RAM than memory mode.
//!
//! # Persistence
//!
//! Graph topology (max_layer, neighbor lists) is mirrored to a RocksDB column
//! family (`hnsw` CF). In memory mode the node serialization includes the
//! full float vector. In mmap mode the serialization stores a `0xFFFF_FFFF`
//! sentinel in the dim field followed by the node's dense index into the mmap
//! file; the actual float data lives only in the `.vecs` file.
//!
//! Key layout in the HNSW CF:
//! - `b"__ep"` → `[node_id: 16][max_layer: 4 LE]`
//! - `b"n/" + node_id_bytes` (18 bytes) → serialised `HnswNode`

use memmap2::MmapMut;
use polargraph_core::{id::NodeId, schema::StorageMode};
use std::{
    cmp::{self, Ordering},
    collections::{BinaryHeap, HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use crate::error::StorageError;

// ── Tunables ──────────────────────────────────────────────────────────────────

/// Max bidirectional connections per node per layer (M in the paper).
pub const DEFAULT_M: usize = 16;
/// Max connections at layer 0 (M_max0 = 2*M).
pub const DEFAULT_M_MAX0: usize = 32;
/// Candidate set size during construction (ef_construction).
pub const DEFAULT_EF_CONSTRUCTION: usize = 200;

// ── Heap wrappers (f32 is not Ord, so we need newtypes) ───────────────────────

/// Entry for the "best found so far" max-heap (furthest at top for trimming).
#[derive(Clone)]
struct Far(f32, NodeId);

impl PartialEq for Far {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits() && self.1 == other.1
    }
}
impl Eq for Far {}
impl PartialOrd for Far {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Far {
    fn cmp(&self, other: &Self) -> Ordering {
        // Larger distance = higher priority (max-heap).
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.1.cmp(&other.1))
    }
}

/// Entry for the "unexplored candidates" min-heap (nearest at top).
#[derive(Clone)]
struct Near(f32, NodeId);

impl PartialEq for Near {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits() && self.1 == other.1
    }
}
impl Eq for Near {}
impl PartialOrd for Near {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Near {
    fn cmp(&self, other: &Self) -> Ordering {
        // Smaller distance = higher priority (reverse for min-heap via BinaryHeap).
        other
            .0
            .partial_cmp(&self.0)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.1.cmp(&self.1))
    }
}

// ── Mmap vector storage ───────────────────────────────────────────────────────

/// Memory-mapped flat vector storage for one HNSW space.
///
/// File format (little-endian):
/// ```text
/// [dims:  u32  LE]           ← bytes  0..4
/// [count: u64  LE]           ← bytes  4..12
/// [f32 × dims × node_0]     ← bytes 12 ..
/// [f32 × dims × node_1]
/// ...
/// ```
/// Vector for dense index `i` starts at byte `12 + i × dims × 4`.
/// All offsets are 4-byte aligned (header is 12 bytes = 3 × 4).
pub struct MmapState {
    pub dims: usize,
    pub count: usize,
    /// NodeId → dense index (0-based, assigned in insertion order).
    pub id_to_index: HashMap<NodeId, usize>,
    file: File,
    /// Absolute path to the `.vecs` file.
    pub path: PathBuf,
    /// `None` only when `count == 0` (file has 12-byte header, no vector data yet).
    pub mmap: Option<MmapMut>,
    /// Vectors the file has room for (the file grows geometrically, so an
    /// append rarely resizes and remaps it).
    capacity: usize,
    /// Byte range written since the last [`MmapState::flush`].
    dirty: Option<(usize, usize)>,
}

impl MmapState {
    const HEADER: usize = 12; // 4 (dims u32) + 8 (count u64)

    /// Create a brand-new mmap file at `path` for vectors of `dims` dimensions.
    pub fn create(path: PathBuf, dims: usize) -> Result<Self, StorageError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.write_all(&(dims as u32).to_le_bytes())?;
        file.write_all(&0u64.to_le_bytes())?;
        file.flush()?;
        Ok(Self {
            dims,
            count: 0,
            id_to_index: HashMap::new(),
            file,
            path,
            mmap: None,
            capacity: 0,
            dirty: None,
        })
    }

    /// Open an existing mmap file. Reads dims and count from the header.
    /// `id_to_index` is populated later by `register_id` calls (one per
    /// loaded node, driven by the RocksDB HNSW CF scan in `store.rs`).
    pub fn open(path: PathBuf) -> Result<Self, StorageError> {
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let file_len = file.metadata()?.len() as usize;
        if file_len < Self::HEADER {
            return Err(StorageError::KeyDecode(format!(
                "mmap file '{}' too short ({} bytes)",
                path.display(),
                file_len
            )));
        }
        file.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; 12];
        file.read_exact(&mut header)?;
        let dims = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let count = u64::from_le_bytes(header[4..12].try_into().unwrap()) as usize;

        let mmap = if file_len > Self::HEADER {
            Some(unsafe { MmapMut::map_mut(&file)? })
        } else {
            None
        };

        // Files may be longer than `count` vectors (spare capacity); the
        // header count is authoritative.
        let capacity = (file_len - Self::HEADER) / (dims * 4).max(1);
        Ok(Self {
            dims,
            count,
            id_to_index: HashMap::new(),
            file,
            path,
            mmap,
            capacity,
            dirty: None,
        })
    }

    /// Register a known `(NodeId, dense_index)` pair during index load.
    pub fn register_id(&mut self, id: NodeId, index: usize) {
        self.id_to_index.insert(id, index);
    }

    /// Append a new vector to the file. Returns the assigned dense index.
    ///
    /// The file grows geometrically; it is resized and remapped only when
    /// full. Nothing is flushed here: call [`MmapState::flush`] once per
    /// batch, before committing records that reference the new vectors.
    /// (Resizing, remapping and flushing the whole file per vector made bulk
    /// loads quadratic.)
    pub fn append(&mut self, id: NodeId, vector: &[f32]) -> Result<usize, StorageError> {
        debug_assert_eq!(vector.len(), self.dims, "vector length != dims");
        let idx = self.count;
        self.ensure_capacity(idx + 1)?;
        let mmap = self.mmap.as_mut().expect("mapped after ensure_capacity");

        // Write vector bytes at the assigned offset.
        let byte_start = Self::HEADER + idx * self.dims * 4;
        let vector_bytes: &[u8] =
            unsafe { std::slice::from_raw_parts(vector.as_ptr() as *const u8, vector.len() * 4) };
        let byte_end = byte_start + vector_bytes.len();
        mmap[byte_start..byte_end].copy_from_slice(vector_bytes);

        // Update count in header.
        self.count += 1;
        mmap[4..12].copy_from_slice(&(self.count as u64).to_le_bytes());

        self.dirty = Some(match self.dirty {
            Some((a, b)) => (a.min(byte_start), b.max(byte_end)),
            None => (byte_start, byte_end),
        });
        self.id_to_index.insert(id, idx);
        Ok(idx)
    }

    /// Make room for `n` vectors: double the capacity (at least 1 024
    /// vectors), resize the file and remap.
    fn ensure_capacity(&mut self, n: usize) -> Result<(), StorageError> {
        if n <= self.capacity && self.mmap.is_some() {
            return Ok(());
        }
        let capacity = n.max(self.capacity * 2).max(1024);
        // Must drop the mmap before resizing the file (OS requirement); the
        // mapping is shared, so written pages stay in the file.
        drop(self.mmap.take());
        self.file
            .set_len((Self::HEADER + capacity * self.dims * 4) as u64)?;
        self.mmap = Some(unsafe { MmapMut::map_mut(&self.file)? });
        self.capacity = capacity;
        Ok(())
    }

    /// Write the vectors appended since the last flush, and the header, to
    /// disk.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        if let (Some((a, b)), Some(mmap)) = (self.dirty.take(), self.mmap.as_ref()) {
            mmap.flush_range(a, b - a)?;
            mmap.flush_range(0, Self::HEADER)?;
        }
        Ok(())
    }

    /// Return a borrowed `&[f32]` slice for `id`'s vector.
    ///
    /// # Safety
    ///
    /// Bytes at the returned range were written by `append` as IEEE 754
    /// little-endian `f32` values. The memory region is file-backed and
    /// valid for the lifetime of `&self`. All offsets are multiples of 4,
    /// satisfying `f32` alignment requirements.
    pub fn get_slice(&self, id: NodeId) -> Option<&[f32]> {
        let &idx = self.id_to_index.get(&id)?;
        if idx >= self.count {
            return None;
        }
        let mmap = self.mmap.as_ref()?;
        let byte_start = Self::HEADER + idx * self.dims * 4;
        let byte_end = byte_start + self.dims * 4;
        if byte_end > mmap.len() {
            return None;
        }
        Some(unsafe {
            std::slice::from_raw_parts(mmap[byte_start..byte_end].as_ptr() as *const f32, self.dims)
        })
    }

    /// Copy the vector for `id` into a new `Vec<f32>`.
    ///
    /// Used in the HNSW neighbor-pruning step, where we cannot hold a `&self`
    /// borrow concurrently with a `&mut self.nodes` borrow on the parent index.
    pub fn get_owned(&self, id: NodeId) -> Option<Vec<f32>> {
        self.get_slice(id).map(<[f32]>::to_vec)
    }
}

// ── NodeVector — discriminated union returned by `deserialize_node` ───────────

/// What `deserialize_node` found in the dim field of a RocksDB node record.
pub enum NodeVector {
    /// In-memory mode, legacy records: vector data is embedded in the record.
    Data(Vec<f32>),
    /// Mmap mode: the record stores a dense index into the `.vecs` file.
    MmapIndex(usize),
    /// In-memory mode: the vector is stored once under its own key
    /// ([`vector_key_for_space`]), so neighbour-list updates don't rewrite
    /// it. `deserialize_node` returns it empty; the loader fills it in.
    Separate(Vec<f32>),
}

// ── Core data structures ──────────────────────────────────────────────────────

pub struct HnswNode {
    /// **Memory mode**: the embedding vector.
    /// **Mmap mode**: always `Vec::new()` — vector lives in the mmap file.
    pub(crate) vector: Vec<f32>,
    /// Maximum layer this node participates in.
    pub(crate) max_layer: usize,
    /// `neighbors[l]` = neighbor IDs at layer l (0..=max_layer).
    pub(crate) neighbors: Vec<Vec<NodeId>>,
    /// Loaded from a legacy record with the vector inline; keeps that
    /// format when rewritten (its vector has no key of its own).
    pub(crate) inline: bool,
}

/// How a space stores vectors: [`StorageMode`] plus optional int8 codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SpaceOptions {
    pub mode: StorageMode,
    /// int8 quantization (implies mmap for the full vectors).
    pub int8: bool,
}

impl From<StorageMode> for SpaceOptions {
    fn from(mode: StorageMode) -> Self {
        Self { mode, int8: false }
    }
}

/// A vector's int8 codes and their Euclidean norm.
#[derive(Debug, Clone, PartialEq)]
pub struct Codes {
    pub codes: Vec<i8>,
    pub norm: f32,
}

impl Codes {
    /// Quantize `v` with its own scale (`max|x| / 127`).
    pub fn quantize(v: &[f32]) -> Self {
        let max = v.iter().fold(0f32, |m, x| m.max(x.abs()));
        let codes: Vec<i8> = if max == 0.0 {
            vec![0; v.len()]
        } else {
            v.iter()
                .map(|x| (x / max * 127.0).round().clamp(-127.0, 127.0) as i8)
                .collect()
        };
        let norm = (codes
            .iter()
            .map(|c| (*c as i32 * *c as i32) as i64)
            .sum::<i64>() as f32)
            .sqrt();
        Self { codes, norm }
    }

    /// Cosine distance between two code vectors.
    pub fn distance(&self, other: &Codes) -> f32 {
        if self.codes.len() != other.codes.len() || self.norm == 0.0 || other.norm == 0.0 {
            return 1.0;
        }
        let dot: i32 = self
            .codes
            .iter()
            .zip(&other.codes)
            .map(|(a, b)| *a as i32 * *b as i32)
            .sum();
        1.0 - dot as f32 / (self.norm * other.norm)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.codes.iter().map(|c| *c as u8).collect()
    }

    pub fn from_bytes(b: &[u8]) -> Self {
        let codes: Vec<i8> = b.iter().map(|x| *x as i8).collect();
        let norm = (codes
            .iter()
            .map(|c| (*c as i32 * *c as i32) as i64)
            .sum::<i64>() as f32)
            .sqrt();
        Self { codes, norm }
    }
}

/// A query as the index compares it: exact, or as int8 codes.
enum Probe<'a> {
    /// The query and its norm.
    Exact(&'a [f32], f32),
    Int8(Codes),
}

pub struct HnswIndex {
    pub(crate) nodes: HashMap<NodeId, HnswNode>,
    /// int8 codes by node (`Some` ⇒ quantized space).
    pub(crate) int8: Option<HashMap<NodeId, Codes>>,
    /// Exact vector norms by node (cosine then needs one dot product).
    norms: HashMap<NodeId, f32>,
    pub(crate) entry_point: Option<NodeId>,
    pub(crate) global_max_layer: usize,
    m: usize,
    m_max0: usize,
    ef_construction: usize,
    /// Simple xorshift64 state for random level generation.
    rng: u64,
    /// Path to the `.vecs` mmap file. `Some` ⇒ mmap mode; `None` ⇒ memory mode.
    mmap_path: Option<PathBuf>,
    /// Populated after first insert or on `open` for an existing mmap space.
    pub(crate) mmap_state: Option<MmapState>,
}

impl Default for HnswIndex {
    fn default() -> Self {
        Self::with_params(DEFAULT_M, DEFAULT_M_MAX0, DEFAULT_EF_CONSTRUCTION)
    }
}

impl HnswIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_params(m: usize, m_max0: usize, ef_construction: usize) -> Self {
        Self {
            nodes: HashMap::new(),
            entry_point: None,
            global_max_layer: 0,
            m,
            m_max0,
            ef_construction,
            rng: 0x9e3779b97f4a7c15,
            mmap_path: None,
            mmap_state: None,
            int8: None,
            norms: HashMap::new(),
        }
    }

    /// Whether the space keeps int8 codes.
    pub fn is_int8(&self) -> bool {
        self.int8.is_some()
    }

    /// The codes of `id`, for persisting.
    pub fn codes_of(&self, id: NodeId) -> Option<&Codes> {
        self.int8.as_ref()?.get(&id)
    }

    /// A node's full vector (memory or mmap).
    pub fn vector_of(&self, id: NodeId) -> Vec<f32> {
        self.get_vector_owned(id)
    }

    /// Load persisted codes.
    pub fn load_codes(&mut self, id: NodeId, codes: Codes) {
        self.int8.get_or_insert_with(HashMap::new).insert(id, codes);
    }

    /// Make this index quantized: full vectors move to the mmap file at
    /// `path` (if they are in memory), and every node gets codes. Returns
    /// the nodes whose records changed (to persist).
    pub fn enable_int8(&mut self, path: PathBuf) -> Result<Vec<NodeId>, StorageError> {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        let mut changed = Vec::new();
        if self.mmap_state.is_none() {
            if let Some(first) = ids.iter().find_map(|id| self.nodes.get(id)) {
                let mut ms = MmapState::create(path.clone(), first.vector.len())?;
                for id in &ids {
                    let v = std::mem::take(&mut self.nodes.get_mut(id).unwrap().vector);
                    ms.append(*id, &v)?;
                }
                self.mmap_state = Some(ms);
                changed.extend(ids.iter().copied());
            }
            self.mmap_path = Some(path);
        }
        let mut codes = HashMap::with_capacity(ids.len());
        for id in &ids {
            codes.insert(*id, Codes::quantize(&self.get_vector_owned(*id)));
        }
        self.int8 = Some(codes);
        Ok(changed)
    }

    /// Create a new mmap-backed HNSW index. The `.vecs` file at `path` is
    /// created on the first `insert` call (not here, so we avoid creating
    /// an empty file for spaces that are never written to).
    pub fn new_mmap(path: PathBuf) -> Self {
        let mut idx = Self::with_params(DEFAULT_M, DEFAULT_M_MAX0, DEFAULT_EF_CONSTRUCTION);
        idx.mmap_path = Some(path);
        idx
    }

    /// Create a mmap-backed index with an already-open `MmapState` (used when
    /// loading from `store.rs::load_hnsw_spaces`).
    pub fn new_mmap_with_state(state: MmapState) -> Self {
        let path = state.path.clone();
        let mut idx = Self::with_params(DEFAULT_M, DEFAULT_M_MAX0, DEFAULT_EF_CONSTRUCTION);
        idx.mmap_path = Some(path);
        idx.mmap_state = Some(state);
        idx
    }

    pub fn is_mmap(&self) -> bool {
        self.mmap_path.is_some()
    }

    /// Flush vectors appended to the mmap file since the last flush (no-op
    /// in memory mode). Call before committing the records that use them.
    pub fn flush_vectors(&mut self) -> Result<(), StorageError> {
        match &mut self.mmap_state {
            Some(ms) => ms.flush(),
            None => Ok(()),
        }
    }

    // ── persistence helpers (called by store.rs) ──────────────────────────────

    pub fn load_entry_point(&mut self, node_id: NodeId, max_layer: usize) {
        self.entry_point = Some(node_id);
        self.global_max_layer = max_layer;
    }

    /// Load a node from RocksDB. `node_data` is `Data(vec)` for memory mode
    /// and `MmapIndex(idx)` for mmap mode.
    pub fn load_node(
        &mut self,
        id: NodeId,
        node_data: NodeVector,
        max_layer: usize,
        neighbors: Vec<Vec<NodeId>>,
    ) {
        let (vector, inline) = match node_data {
            NodeVector::Data(v) => (v, true),
            NodeVector::Separate(v) => (v, false),
            NodeVector::MmapIndex(idx) => {
                // Register the dense index so distance queries can find it.
                if let Some(ms) = &mut self.mmap_state {
                    ms.register_id(id, idx);
                }
                (Vec::new(), false) // vector lives in mmap, not in the node struct
            }
        };
        let n = match &self.mmap_state {
            Some(ms) => ms.get_slice(id).map(norm),
            None => Some(norm(&vector)),
        };
        if let Some(n) = n {
            self.norms.insert(id, n);
        }
        self.nodes.insert(
            id,
            HnswNode {
                vector,
                max_layer,
                neighbors,
                inline,
            },
        );
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    // ── public API ─────────────────────────────────────────────────────────────

    /// Insert a node and return the IDs of all nodes whose neighbor lists changed
    /// (so the caller can persist them).
    pub fn insert(&mut self, id: NodeId, vector: Vec<f32>) -> Vec<NodeId> {
        // ── Mmap mode: ensure the backing file is open, then append the vector ──
        if let Some(path) = self.mmap_path.clone() {
            if self.mmap_state.is_none() {
                let dims = vector.len();
                match MmapState::create(path, dims) {
                    Ok(ms) => {
                        self.mmap_state = Some(ms);
                    }
                    Err(e) => {
                        tracing::error!("failed to create mmap file: {e}; falling back to memory");
                        self.mmap_path = None; // degrade to memory mode
                    }
                }
            }
            if let Some(ms) = &mut self.mmap_state {
                if let Err(e) = ms.append(id, &vector) {
                    tracing::error!(
                        "mmap append failed: {e}; falling back to memory for this node"
                    );
                    // Fall through to store vector in node struct as backup
                }
            }
        }

        if let Some(codes) = &mut self.int8 {
            codes.insert(id, Codes::quantize(&vector));
        }
        self.norms.insert(id, norm(&vector));
        let probe = self.probe(&vector);
        let level = self.random_level();
        let mut modified: Vec<NodeId> = Vec::new();

        // Store the node. In mmap mode, the vector field is empty.
        let stored_vector = if self.is_mmap() {
            Vec::new()
        } else {
            vector.clone()
        };

        if self.entry_point.is_none() {
            let neighbors = (0..=level).map(|_| Vec::new()).collect();
            self.nodes.insert(
                id,
                HnswNode {
                    vector: stored_vector,
                    max_layer: level,
                    neighbors,
                    inline: false,
                },
            );
            self.entry_point = Some(id);
            self.global_max_layer = level;
            modified.push(id);
            return modified;
        }

        let ep = self.entry_point.unwrap();

        // ── Phase 1: greedy descent from top layer to level+1 ─────────────────
        let mut ep_dist = self.dist(ep, &probe);
        let mut cur_ep = ep;

        for lc in (level + 1..=self.global_max_layer).rev() {
            let w = self.search_layer(&probe, cur_ep, 1, lc);
            if let Some(Far(d, nearest)) = w.into_sorted_vec().into_iter().next() {
                if d < ep_dist {
                    ep_dist = d;
                    cur_ep = nearest;
                }
            }
        }

        // ── Phase 2: build connections from level down to 0 ───────────────────
        self.nodes.insert(
            id,
            HnswNode {
                vector: stored_vector,
                max_layer: level,
                neighbors: (0..=level).map(|_| Vec::new()).collect(),
                inline: false,
            },
        );

        for lc in (0..=cmp::min(level, self.global_max_layer)).rev() {
            let w = self.search_layer(&probe, cur_ep, self.ef_construction, lc);

            let w_sorted = w.into_sorted_vec();
            if let Some(Far(d, nearest)) = w_sorted.first() {
                if *d < ep_dist {
                    ep_dist = *d;
                    cur_ep = *nearest;
                }
            }

            let m_at_layer = if lc == 0 { self.m_max0 } else { self.m };
            let candidates: Vec<(f32, NodeId)> =
                w_sorted.iter().map(|Far(d, nid)| (*d, *nid)).collect();
            let neighbors_for_new = self.select_neighbors_heuristic(&candidates, m_at_layer);

            self.nodes.get_mut(&id).unwrap().neighbors[lc] = neighbors_for_new.clone();
            modified.push(id);

            for &neighbor_id in &neighbors_for_new {
                let m_max = if lc == 0 { self.m_max0 } else { self.m };

                // Extract what we need while holding the mutable borrow, then drop.
                // For mmap mode n.vector is empty; we'll fetch it separately below.
                let prune_candidates: Option<Vec<NodeId>> =
                    if let Some(n) = self.nodes.get_mut(&neighbor_id) {
                        if lc <= n.max_layer {
                            n.neighbors[lc].push(id);
                            if n.neighbors[lc].len() > m_max {
                                Some(n.neighbors[lc].clone())
                            } else {
                                None
                            }
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    };

                if let Some(candidates) = prune_candidates {
                    // All &mut borrows are released here; we can call &self methods.
                    let mut scored: Vec<(f32, NodeId)> = candidates
                        .iter()
                        .map(|&c| (self.dist_nodes(neighbor_id, c), c))
                        .collect();
                    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(cmp::Ordering::Equal));
                    let pruned = self.select_neighbors_heuristic(&scored, m_max);
                    if let Some(n) = self.nodes.get_mut(&neighbor_id) {
                        n.neighbors[lc] = pruned;
                    }
                }

                if !modified.contains(&neighbor_id) {
                    modified.push(neighbor_id);
                }
            }
        }

        // ── Update entry point if new node has higher level ───────────────────
        if level > self.global_max_layer {
            self.entry_point = Some(id);
            self.global_max_layer = level;
        }

        modified
    }

    /// Score every node in `allowed` against `query`; return top-k by cosine similarity.
    ///
    /// Nodes absent from the index are silently skipped.  O(|allowed|).
    pub fn search_in_set(&self, query: &[f32], k: usize, allowed: &[NodeId]) -> Vec<(NodeId, f32)> {
        let qn = norm(query);
        // int8 spaces: rank by codes (in RAM), then re-rank the best 4k with
        // the full vectors (mmap) for exact scores.
        let shortlist;
        let allowed = if self.is_int8() && allowed.len() > 4 * k {
            let probe = Codes::quantize(query);
            let mut coarse: Vec<(f32, NodeId)> = allowed
                .iter()
                .filter_map(|&id| Some((self.codes_of(id)?.distance(&probe), id)))
                .collect();
            let keep = (4 * k).min(coarse.len());
            if keep < coarse.len() {
                coarse.select_nth_unstable_by(keep, |a, b| {
                    a.0.partial_cmp(&b.0).unwrap_or(cmp::Ordering::Equal)
                });
                coarse.truncate(keep);
            }
            shortlist = coarse.into_iter().map(|(_, id)| id).collect::<Vec<_>>();
            &shortlist[..]
        } else {
            allowed
        };
        let mut scored: Vec<(NodeId, f32)> = allowed
            .iter()
            .filter_map(|&id| {
                if !self.nodes.contains_key(&id) {
                    return None;
                }
                let d = self.cosine_with(id, query, qn);
                if d.is_infinite() {
                    None
                } else {
                    Some((id, 1.0 - d))
                }
            })
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(cmp::Ordering::Equal));
        scored.truncate(k);
        scored
    }

    /// Search for the `k` nearest neighbors of `query`.
    ///
    /// `ef` is the exploration factor; larger ef = better recall at cost of speed.
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<(NodeId, f32)> {
        let ep = match self.entry_point {
            Some(ep) => ep,
            None => return vec![],
        };

        let ef = if ef == 0 {
            cmp::max(self.ef_construction / 2, k)
        } else {
            ef
        };

        let probe = self.probe(query);
        let mut cur_ep = ep;
        let mut cur_dist = self.dist(ep, &probe);

        // Descend to layer 1.
        for lc in (1..=self.global_max_layer).rev() {
            let w = self.search_layer(&probe, cur_ep, 1, lc);
            if let Some(Far(d, nearest)) = w.into_sorted_vec().into_iter().next() {
                if d < cur_dist {
                    cur_dist = d;
                    cur_ep = nearest;
                }
            }
        }

        // Search layer 0 with full ef.
        let w = self.search_layer(&probe, cur_ep, cmp::max(ef, k), 0);

        if self.is_int8() {
            // Re-rank the candidates with the exact vectors.
            let qn = norm(query);
            let mut exact: Vec<(NodeId, f32)> = w
                .into_iter()
                .map(|Far(_, id)| (id, 1.0 - self.cosine_with(id, query, qn)))
                .collect();
            exact.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(cmp::Ordering::Equal));
            exact.truncate(k);
            return exact;
        }
        w.into_sorted_vec()
            .into_iter()
            .take(k)
            .map(|Far(d, id)| (id, 1.0 - d))
            .collect()
    }

    /// Serialize a single node's RocksDB record.
    ///
    /// In mmap mode, `dense_index` must be `Some(idx)` — the dim field is
    /// replaced by the sentinel `0xFFFF_FFFF` followed by the dense index.
    pub fn serialize_node_for(&self, id: NodeId) -> Vec<u8> {
        if let Some(node) = self.nodes.get(&id) {
            let mmap_index = self
                .mmap_state
                .as_ref()
                .and_then(|ms| ms.id_to_index.get(&id).copied());
            serialize_node(node, mmap_index)
        } else {
            Vec::new()
        }
    }

    // ── private helpers ───────────────────────────────────────────────────────

    /// How this index compares `query`: as int8 codes in a quantized space.
    fn probe<'a>(&self, query: &'a [f32]) -> Probe<'a> {
        if self.is_int8() {
            Probe::Int8(Codes::quantize(query))
        } else {
            Probe::Exact(query, norm(query))
        }
    }

    /// Distance from node `id` to `probe` (codes when quantized).
    fn dist(&self, id: NodeId, probe: &Probe) -> f32 {
        match probe {
            Probe::Exact(q, qn) => self.cosine_with(id, q, *qn),
            Probe::Int8(q) => match self.codes_of(id) {
                Some(c) => c.distance(q),
                None => f32::INFINITY,
            },
        }
    }

    /// Distance between two stored nodes, as the index compares them (codes
    /// when quantized), without copying either vector.
    fn dist_nodes(&self, a: NodeId, b: NodeId) -> f32 {
        if self.is_int8() {
            return match (self.codes_of(a), self.codes_of(b)) {
                (Some(x), Some(y)) => x.distance(y),
                _ => f32::INFINITY,
            };
        }
        match (self.slice_of(a), self.norm_of(a)) {
            (Some(v), Some(n)) => self.cosine_with(b, v, n),
            _ => f32::INFINITY,
        }
    }

    /// A node's vector norm: cached at insert / load (computed for nodes
    /// that predate the cache).
    fn norm_of(&self, id: NodeId) -> Option<f32> {
        match self.norms.get(&id) {
            Some(n) => Some(*n),
            None => self.slice_of(id).map(norm),
        }
    }

    /// Cosine distance from node `id` to `q` (norm `qn`), using the node's
    /// cached norm: one dot product per comparison.
    fn cosine_with(&self, id: NodeId, q: &[f32], qn: f32) -> f32 {
        let (Some(v), Some(vn)) = (self.slice_of(id), self.norm_of(id)) else {
            return f32::INFINITY;
        };
        if v.is_empty() || v.len() != q.len() || vn == 0.0 || qn == 0.0 {
            return 1.0;
        }
        (1.0 - dot(v, q) / (vn * qn)).clamp(0.0, 2.0)
    }

    /// A stored-or-mapped node's vector, borrowed.
    fn slice_of(&self, id: NodeId) -> Option<&[f32]> {
        match &self.mmap_state {
            Some(ms) => ms.get_slice(id),
            None => self.nodes.get(&id).map(|n| n.vector.as_slice()),
        }
    }

    /// Copy the vector for `id` into a new `Vec<f32>`.
    ///
    /// Used in the prune step of `insert` where we cannot simultaneously hold
    /// a shared borrow on `self.mmap_state` and a mutable borrow on
    /// `self.nodes`.
    fn get_vector_owned(&self, id: NodeId) -> Vec<f32> {
        if let Some(ms) = &self.mmap_state {
            ms.get_owned(id).unwrap_or_default()
        } else {
            self.nodes
                .get(&id)
                .map(|n| n.vector.clone())
                .unwrap_or_default()
        }
    }

    /// HNSW greedy beam search at a single layer.
    fn search_layer(
        &self,
        query: &Probe,
        entry: NodeId,
        ef: usize,
        layer: usize,
    ) -> BinaryHeap<Far> {
        let mut visited: HashSet<NodeId> = HashSet::new();
        let mut candidates: BinaryHeap<Near> = BinaryHeap::new();
        let mut found: BinaryHeap<Far> = BinaryHeap::new();

        let d_entry = self.dist(entry, query);
        visited.insert(entry);
        candidates.push(Near(d_entry, entry));
        found.push(Far(d_entry, entry));

        while let Some(Near(d_c, c)) = candidates.pop() {
            let d_worst = found.peek().map(|Far(d, _)| *d).unwrap_or(f32::INFINITY);
            if d_c > d_worst {
                break;
            }

            let neighbors = match self.nodes.get(&c) {
                Some(n) if layer <= n.max_layer => &n.neighbors[layer],
                _ => continue,
            };

            for &e in neighbors {
                if visited.contains(&e) {
                    continue;
                }
                visited.insert(e);
                let d_e = self.dist(e, query);
                let d_worst = found.peek().map(|Far(d, _)| *d).unwrap_or(f32::INFINITY);
                if d_e < d_worst || found.len() < ef {
                    candidates.push(Near(d_e, e));
                    found.push(Far(d_e, e));
                    if found.len() > ef {
                        found.pop();
                    }
                }
            }
        }

        found
    }

    /// Select up to `m` neighbours from `candidates` (`(distance to the base
    /// node, id)`, nearest first) with the HNSW heuristic (Malkov & Yashunin,
    /// algorithm 4, as in hnswlib): a candidate is kept only if it is closer
    /// to the base than to every neighbour already kept. Plain nearest-`m`
    /// selection links each node only inside its own cluster, so on
    /// clustered data (real embeddings) the graph splits into islands and
    /// searches can't leave the entry point's cluster.
    fn select_neighbors_heuristic(&self, candidates: &[(f32, NodeId)], m: usize) -> Vec<NodeId> {
        if candidates.len() <= m {
            return candidates.iter().map(|(_, id)| *id).collect();
        }
        let mut kept: Vec<NodeId> = Vec::with_capacity(m);
        for &(d, c) in candidates {
            if kept.len() >= m {
                break;
            }
            if kept.iter().all(|&k| self.dist_nodes(k, c) > d) {
                kept.push(c);
            }
        }
        kept
    }

    /// Generate a random layer using the HNSW exponential distribution.
    fn random_level(&mut self) -> usize {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        let f = (self.rng as f64) / (u64::MAX as f64);
        let ml = 1.0 / (self.m as f64).ln();
        ((-f.ln()) * ml).floor() as usize
    }
}

// ── Distance function ─────────────────────────────────────────────────────────

/// Cosine distance ∈ [0, 2]. Zero = identical direction, 2 = opposite.
fn norm(v: &[f32]) -> f32 {
    dot(v, v).sqrt()
}

/// Dot product with eight independent accumulators, so the compiler can
/// vectorize it (a single running sum fixes the order of float additions
/// and stays scalar). Neighbour selection runs thousands per insert.
fn dot(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = [0.0f32; 8];
    let (ca, cb) = (a.chunks_exact(8), b.chunks_exact(8));
    let tail: f32 = ca
        .remainder()
        .iter()
        .zip(cb.remainder())
        .map(|(x, y)| x * y)
        .sum();
    for (x, y) in ca.zip(cb) {
        for i in 0..8 {
            acc[i] += x[i] * y[i];
        }
    }
    acc.iter().sum::<f32>() + tail
}

pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() || b.is_empty() || a.len() != b.len() {
        return 1.0;
    }
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if mag_a == 0.0 || mag_b == 0.0 {
        return 1.0;
    }
    (1.0 - (dot / (mag_a * mag_b))).clamp(0.0, 2.0)
}

// ── Serialisation ─────────────────────────────────────────────────────────────

/// Suffix appended to a space name to form the RocksDB entry-point key.
const EP_SUFFIX: &[u8] = b"/__ep";
/// Sub-prefix inside a space for per-node records.
const NODE_INFIX: &[u8] = b"/n/";
/// Sentinel stored in the `dim` field of a mmap-mode node record.
const MMAP_SENTINEL: u32 = u32::MAX;

/// Sentinel stored in the `dim` field of a memory-mode node record whose
/// vector lives under its own key ([`vector_key_for_space`]).
const SEPARATE_SENTINEL: u32 = u32::MAX - 1;

/// RocksDB key for a memory-mode node's vector: `<space>/v/<16_id_bytes>`
/// (raw `f32` LE). Written once at insert.
pub fn vector_key_for_space(space: &str, id: NodeId) -> Vec<u8> {
    let mut k = space.as_bytes().to_vec();
    k.extend_from_slice(b"/v/");
    k.extend_from_slice(id.as_bytes());
    k
}

/// A vector as stored under [`vector_key_for_space`].
pub fn vector_to_bytes(v: &[f32]) -> Vec<u8> {
    v.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// Inverse of [`vector_to_bytes`].
pub fn vector_from_bytes(b: &[u8]) -> Vec<f32> {
    b.chunks_exact(4)
        .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
        .collect()
}

/// RocksDB key for a space's entry-point record: `<space>/__ep`.
pub fn ep_key_for_space(space: &str) -> Vec<u8> {
    let mut k = space.as_bytes().to_vec();
    k.extend_from_slice(EP_SUFFIX);
    k
}

/// RocksDB key for a specific node in a space: `<space>/n/<16_id_bytes>`.
pub fn node_key_for_space(space: &str, id: NodeId) -> Vec<u8> {
    let mut k = space.as_bytes().to_vec();
    k.extend_from_slice(NODE_INFIX);
    k.extend_from_slice(id.as_bytes());
    k
}

/// Prefix for all node keys in a space: `<space>/n/`.
pub fn node_prefix_for_space(space: &str) -> Vec<u8> {
    let mut k = space.as_bytes().to_vec();
    k.extend_from_slice(NODE_INFIX);
    k
}

/// RocksDB key for a node's int8 codes: `<space>/q/<16_id_bytes>`.
pub fn codes_key_for_space(space: &str, id: NodeId) -> Vec<u8> {
    let mut k = codes_prefix_for_space(space);
    k.extend_from_slice(id.as_bytes());
    k
}

/// Prefix of a space's int8 codes: `<space>/q/`.
pub fn codes_prefix_for_space(space: &str) -> Vec<u8> {
    let mut k = space.as_bytes().to_vec();
    k.extend_from_slice(b"/q/");
    k
}

/// Marker key: the space is int8-quantized.
pub fn int8_marker_key(space: &str) -> Vec<u8> {
    let mut k = space.as_bytes().to_vec();
    k.extend_from_slice(b"/__q");
    k
}

/// Extract the space name from a key if it has the EP suffix.
pub fn parse_ep_key(key: &[u8]) -> Option<String> {
    let key = key.strip_suffix(EP_SUFFIX)?;
    std::str::from_utf8(key).ok().map(|s| s.to_string())
}

/// Serialize the entry-point record.
pub fn serialize_entry_point(id: NodeId, max_layer: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(20);
    v.extend_from_slice(id.as_bytes());
    v.extend_from_slice(&(max_layer as u32).to_le_bytes());
    v
}

/// Deserialize the entry-point record.
pub fn deserialize_entry_point(bytes: &[u8]) -> Result<(NodeId, usize), StorageError> {
    if bytes.len() < 20 {
        return Err(StorageError::KeyDecode("ep record too short".into()));
    }
    let id = NodeId(uuid::Uuid::from_bytes(bytes[..16].try_into().unwrap()));
    let max_layer = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    Ok((id, max_layer))
}

/// Serialize a node's full state for RocksDB.
///
/// **Memory mode** (`dense_index = None`):
/// ```text
/// [max_layer: u32 LE][dim: u32 LE][f32 × dim LE]
/// [for l in 0..=max_layer: [n: u32 LE][NodeId × n]]
/// ```
///
/// **Mmap mode** (`dense_index = Some(idx)`):
/// ```text
/// [max_layer: u32 LE][0xFFFF_FFFF: u32 LE (sentinel)][idx: u32 LE]
/// [for l in 0..=max_layer: [n: u32 LE][NodeId × n]]
/// ```
pub fn serialize_node(node: &HnswNode, dense_index: Option<usize>) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&(node.max_layer as u32).to_le_bytes());

    if let Some(idx) = dense_index {
        // Mmap mode: sentinel + dense index, no vector bytes.
        v.extend_from_slice(&MMAP_SENTINEL.to_le_bytes());
        v.extend_from_slice(&(idx as u32).to_le_bytes());
    } else if !node.inline {
        // Memory mode: the vector has its own key.
        v.extend_from_slice(&SEPARATE_SENTINEL.to_le_bytes());
    } else {
        // Memory mode: dim + vector bytes.
        let dim = node.vector.len();
        v.extend_from_slice(&(dim as u32).to_le_bytes());
        for &f in &node.vector {
            v.extend_from_slice(&f.to_le_bytes());
        }
    }

    for l in 0..=node.max_layer {
        let nbrs = &node.neighbors[l];
        v.extend_from_slice(&(nbrs.len() as u32).to_le_bytes());
        for id in nbrs {
            v.extend_from_slice(id.as_bytes());
        }
    }
    v
}

/// Deserialize a node from RocksDB bytes.
///
/// Returns `(NodeVector, max_layer, neighbors)` where `NodeVector` is either
/// `Data(vec)` (memory mode) or `MmapIndex(idx)` (mmap mode).
pub fn deserialize_node(
    bytes: &[u8],
) -> Result<(NodeVector, usize, Vec<Vec<NodeId>>), StorageError> {
    if bytes.len() < 8 {
        return Err(StorageError::KeyDecode("hnsw node record too short".into()));
    }
    let max_layer = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let dim_or_sentinel = u32::from_le_bytes(bytes[4..8].try_into().unwrap());

    let (node_vector, neighbor_cursor) = if dim_or_sentinel == MMAP_SENTINEL {
        // Mmap mode: bytes[8..12] = dense index.
        if bytes.len() < 12 {
            return Err(StorageError::KeyDecode(
                "hnsw mmap node record too short".into(),
            ));
        }
        let idx = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        (NodeVector::MmapIndex(idx), 12)
    } else if dim_or_sentinel == SEPARATE_SENTINEL {
        (NodeVector::Separate(Vec::new()), 8)
    } else {
        // Memory mode: read float vector.
        let dim = dim_or_sentinel as usize;
        let float_end = 8 + dim * 4;
        if bytes.len() < float_end {
            return Err(StorageError::KeyDecode("hnsw node vector truncated".into()));
        }
        let mut vector = Vec::with_capacity(dim);
        for i in 0..dim {
            let off = 8 + i * 4;
            vector.push(f32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()));
        }
        (NodeVector::Data(vector), float_end)
    };

    let mut cursor = neighbor_cursor;
    let mut neighbors = Vec::with_capacity(max_layer + 1);
    for _ in 0..=max_layer {
        if cursor + 4 > bytes.len() {
            return Err(StorageError::KeyDecode(
                "hnsw neighbor count truncated".into(),
            ));
        }
        let n = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let mut nbrs = Vec::with_capacity(n);
        for _ in 0..n {
            if cursor + 16 > bytes.len() {
                return Err(StorageError::KeyDecode("hnsw neighbor id truncated".into()));
            }
            let id = NodeId(uuid::Uuid::from_bytes(
                bytes[cursor..cursor + 16].try_into().unwrap(),
            ));
            cursor += 16;
            nbrs.push(id);
        }
        neighbors.push(nbrs);
    }

    Ok((node_vector, max_layer, neighbors))
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use polargraph_core::id::NodeId;
    use uuid::Uuid;

    fn make_id(seed: u8) -> NodeId {
        NodeId(Uuid::from_bytes([seed; 16]))
    }

    fn nth_id(i: usize) -> NodeId {
        NodeId(Uuid::from_u128(i as u128 + 1))
    }

    fn unit_vec(dim: usize, hot: usize) -> Vec<f32> {
        let mut v = vec![0.0_f32; dim];
        v[hot % dim] = 1.0;
        v
    }

    // ── cosine_distance ───────────────────────────────────────────────────────

    #[test]
    fn cosine_identical_is_zero() {
        let a = vec![1.0_f32, 2.0, 3.0];
        assert!((cosine_distance(&a, &a) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_orthogonal_is_one() {
        let a = vec![1.0_f32, 0.0];
        let b = vec![0.0_f32, 1.0];
        assert!((cosine_distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_opposite_is_two() {
        let a = vec![1.0_f32, 0.0];
        let b = vec![-1.0_f32, 0.0];
        assert!((cosine_distance(&a, &b) - 2.0).abs() < 1e-6);
    }

    // ── serialisation round-trips ─────────────────────────────────────────────

    #[test]
    fn entry_point_round_trips() {
        let id = make_id(0xAB);
        let bytes = serialize_entry_point(id, 3);
        let (back_id, back_layer) = deserialize_entry_point(&bytes).unwrap();
        assert_eq!(back_id, id);
        assert_eq!(back_layer, 3);
    }

    #[test]
    fn legacy_inline_node_round_trips() {
        let id_a = make_id(1);
        let id_b = make_id(2);
        let node = HnswNode {
            vector: vec![1.0, 2.0, 3.0],
            max_layer: 1,
            neighbors: vec![vec![id_a], vec![id_b]],
            inline: true,
        };
        let bytes = serialize_node(&node, None); // legacy memory-mode record
        let (nv, max_layer, nbrs) = deserialize_node(&bytes).unwrap();
        match nv {
            NodeVector::Data(v) => assert_eq!(v, node.vector),
            _ => panic!("expected Data"),
        }
        assert_eq!(max_layer, 1);
        assert_eq!(nbrs[0], vec![id_a]);
        assert_eq!(nbrs[1], vec![id_b]);
    }

    #[test]
    fn separate_vector_node_round_trips() {
        let id_a = make_id(1);
        let node = HnswNode {
            vector: vec![1.0, 2.0, 3.0],
            max_layer: 0,
            neighbors: vec![vec![id_a]],
            inline: false,
        };
        let bytes = serialize_node(&node, None);
        // The record carries no vector: 4 + 4 + (4 + 16) bytes.
        assert_eq!(bytes.len(), 28);
        let (nv, _, nbrs) = deserialize_node(&bytes).unwrap();
        assert!(matches!(nv, NodeVector::Separate(v) if v.is_empty()));
        assert_eq!(nbrs[0], vec![id_a]);
        assert_eq!(
            vector_from_bytes(&vector_to_bytes(&node.vector)),
            node.vector
        );
    }

    #[test]
    fn mmap_node_round_trips() {
        let id_a = make_id(1);
        let node = HnswNode {
            vector: Vec::new(), // empty in mmap mode
            max_layer: 0,
            neighbors: vec![vec![id_a]],
            inline: false,
        };
        let bytes = serialize_node(&node, Some(42)); // mmap mode, dense index 42
        let (nv, max_layer, nbrs) = deserialize_node(&bytes).unwrap();
        match nv {
            NodeVector::MmapIndex(idx) => assert_eq!(idx, 42),
            _ => panic!("expected MmapIndex"),
        }
        assert_eq!(max_layer, 0);
        assert_eq!(nbrs[0], vec![id_a]);
    }

    #[test]
    fn node_zero_dim_round_trips() {
        let node = HnswNode {
            vector: vec![],
            max_layer: 0,
            neighbors: vec![vec![]],
            inline: true,
        };
        let bytes = serialize_node(&node, None);
        let (nv, max_layer, nbrs) = deserialize_node(&bytes).unwrap();
        match nv {
            NodeVector::Data(v) => assert!(v.is_empty()),
            _ => panic!("expected Data"),
        }
        assert_eq!(max_layer, 0);
        assert!(nbrs[0].is_empty());
    }

    // ── insert / search (memory mode) ────────────────────────────────────────

    /// Clustered unit vectors (`clusters` centroids, noise of norm ≈ 0.9)
    /// and queries near populated centroids — the shape of real embeddings.
    fn clustered(n: usize, dims: usize, clusters: usize) -> (Vec<Vec<f32>>, Vec<Vec<f32>>) {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut normal = move || {
            let mut unit = || {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 11) as f64 / (1u64 << 53) as f64
            };
            let (u1, u2) = (unit().max(f64::MIN_POSITIVE), unit());
            (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
        };
        let mut around = |c: Option<&[f32]>, spread: f64| {
            let sigma = spread / (dims as f64).sqrt();
            let mut v: Vec<f32> = (0..dims)
                .map(|i| (c.map_or(0.0, |c| c[i] as f64) + sigma * normal()) as f32)
                .collect();
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            v.iter_mut().for_each(|x| *x /= norm);
            v
        };
        let centroids: Vec<Vec<f32>> = (0..clusters).map(|_| around(None, 1.0)).collect();
        let data = (0..n)
            .map(|i| around(Some(&centroids[i % clusters]), 0.9))
            .collect();
        let queries = (0..40)
            .map(|q| around(Some(&centroids[(q * 7) % clusters]), 0.9))
            .collect();
        (data, queries)
    }

    /// recall@10 of `idx` against brute force.
    fn recall_at_10(
        idx: &HnswIndex,
        ids: &[NodeId],
        data: &[Vec<f32>],
        queries: &[Vec<f32>],
    ) -> f64 {
        let mut hit = 0;
        for q in queries {
            let mut exact: Vec<(f32, NodeId)> = data
                .iter()
                .zip(ids)
                .map(|(v, id)| (cosine_distance(v, q), *id))
                .collect();
            exact.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            let got: HashSet<NodeId> = idx
                .search(q, 10, 100)
                .into_iter()
                .map(|(id, _)| id)
                .collect();
            hit += exact
                .iter()
                .take(10)
                .filter(|(_, id)| got.contains(id))
                .count();
        }
        hit as f64 / (10 * queries.len()) as f64
    }

    #[test]
    fn clustered_data_stays_connected() {
        // Plain nearest-M linking split clustered data into islands
        // (recall@10 0.67 here, 0.73 for int8); the heuristic keeps
        // clusters linked (1.0).
        let (data, queries) = clustered(6_000, 64, 120);
        let ids: Vec<NodeId> = (0..data.len()).map(nth_id).collect();
        let mut idx = HnswIndex::new();
        for (id, v) in ids.iter().zip(&data) {
            idx.insert(*id, v.clone());
        }
        let r = recall_at_10(&idx, &ids, &data, &queries);
        assert!(r >= 0.9, "recall@10 on clustered data: {r}");
    }

    #[test]
    fn int8_in_set_ranking_matches_exact() {
        // Codes pick a 4k shortlist; exact re-ranking orders it.
        let dir = tempfile::tempdir().unwrap();
        let (data, queries) = clustered(3_000, 64, 30);
        let ids: Vec<NodeId> = (0..data.len()).map(nth_id).collect();
        let mut idx = HnswIndex::new_mmap(dir.path().join("s.vecs"));
        idx.enable_int8(dir.path().join("s.vecs")).unwrap();
        for (id, v) in ids.iter().zip(&data) {
            idx.insert(*id, v.clone());
        }
        let set: Vec<NodeId> = ids.iter().step_by(2).copied().collect();
        let mut hit = 0;
        for q in &queries {
            let mut exact: Vec<(f32, NodeId)> = data
                .iter()
                .zip(&ids)
                .step_by(2)
                .map(|(v, id)| (cosine_distance(v, q), *id))
                .collect();
            exact.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            let got: HashSet<NodeId> = idx
                .search_in_set(q, 10, &set)
                .into_iter()
                .map(|(id, _)| id)
                .collect();
            hit += exact
                .iter()
                .take(10)
                .filter(|(_, id)| got.contains(id))
                .count();
        }
        let r = hit as f64 / (10 * queries.len()) as f64;
        assert!(r >= 0.95, "int8 in-set recall@10: {r}");
    }

    #[test]
    fn clustered_data_stays_connected_int8() {
        let dir = tempfile::tempdir().unwrap();
        let (data, queries) = clustered(4_000, 64, 80);
        let ids: Vec<NodeId> = (0..data.len()).map(nth_id).collect();
        let mut idx = HnswIndex::new_mmap(dir.path().join("s.vecs"));
        idx.enable_int8(dir.path().join("s.vecs")).unwrap();
        for (id, v) in ids.iter().zip(&data) {
            idx.insert(*id, v.clone());
        }
        let r = recall_at_10(&idx, &ids, &data, &queries);
        assert!(r >= 0.9, "int8 recall@10 on clustered data: {r}");
    }

    #[test]
    fn insert_single_and_search() {
        let mut idx = HnswIndex::new();
        let id = make_id(1);
        idx.insert(id, vec![1.0, 0.0]);
        let results = idx.search(&[1.0, 0.0], 1, 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, id);
    }

    #[test]
    fn search_empty_returns_empty() {
        let idx = HnswIndex::new();
        assert!(idx.search(&[1.0], 5, 10).is_empty());
    }

    #[test]
    fn nearest_neighbor_is_correct() {
        let mut idx = HnswIndex::new();
        let near_id = make_id(1);
        let far_id = make_id(2);
        idx.insert(near_id, vec![1.0_f32, 0.0, 0.0]);
        idx.insert(far_id, vec![0.0_f32, 0.0, 1.0]);
        let results = idx.search(&[1.0, 0.0, 0.0], 1, 10);
        assert_eq!(results[0].0, near_id);
    }

    #[test]
    fn search_in_set_returns_correct_order() {
        let mut idx = HnswIndex::new();
        let a = make_id(1);
        idx.insert(a, vec![1.0_f32, 0.0]);
        let b = make_id(2);
        idx.insert(b, vec![0.9_f32, 0.1]);
        let c = make_id(3);
        idx.insert(c, vec![0.0_f32, 1.0]);
        let results = idx.search_in_set(&[1.0, 0.0], 2, &[a, b, c]);
        assert_eq!(results[0].0, a);
        assert_eq!(results[1].0, b);
    }

    // ── MmapState unit tests ──────────────────────────────────────────────────

    #[test]
    fn mmap_state_create_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.vecs");
        let mut ms = MmapState::create(path, 4).unwrap();

        let id0 = make_id(0);
        let id1 = make_id(1);
        ms.append(id0, &[1.0, 0.0, 0.0, 0.0]).unwrap();
        ms.append(id1, &[0.0, 1.0, 0.0, 0.0]).unwrap();

        let s0 = ms.get_slice(id0).unwrap();
        assert_eq!(s0, [1.0_f32, 0.0, 0.0, 0.0]);
        let s1 = ms.get_slice(id1).unwrap();
        assert_eq!(s1, [0.0_f32, 1.0, 0.0, 0.0]);
    }

    #[test]
    fn mmap_state_grows_geometrically_and_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("size.vecs");
        let mut ms = MmapState::create(path.clone(), 3).unwrap();
        ms.append(make_id(1), &[1.0, 2.0, 3.0]).unwrap();
        ms.append(make_id(2), &[4.0, 5.0, 6.0]).unwrap();
        // Room for 1 024 vectors up front; the header counts two.
        let len = |p: &PathBuf| std::fs::metadata(p).unwrap().len() as usize;
        assert_eq!(len(&path), MmapState::HEADER + 1024 * 3 * 4);
        for i in 2..1100u32 {
            let v = i as f32;
            ms.append(NodeId(Uuid::from_u128(i as u128 + 100)), &[v, v, v])
                .unwrap();
        }
        assert_eq!(len(&path), MmapState::HEADER + 2048 * 3 * 4, "doubled once");
        ms.flush().unwrap();
        drop(ms);

        let mut ms = MmapState::open(path.clone()).unwrap();
        assert_eq!(ms.count, 1100);
        ms.register_id(make_id(2), 1);
        assert_eq!(ms.get_slice(make_id(2)).unwrap(), &[4.0, 5.0, 6.0]);
        // Appends after reopening continue at the counted end.
        assert_eq!(ms.append(make_id(9), &[9.0, 9.0, 9.0]).unwrap(), 1100);
    }

    #[test]
    fn mmap_state_opens_exact_size_files() {
        // Files written before spare capacity: header + exactly `count` vectors.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.vecs");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&1.5f32.to_le_bytes());
        bytes.extend_from_slice(&2.5f32.to_le_bytes());
        std::fs::write(&path, bytes).unwrap();
        let mut ms = MmapState::open(path).unwrap();
        ms.register_id(make_id(1), 0);
        assert_eq!(ms.get_slice(make_id(1)).unwrap(), &[1.5, 2.5]);
        assert_eq!(ms.append(make_id(2), &[3.0, 4.0]).unwrap(), 1);
        assert_eq!(ms.get_slice(make_id(1)).unwrap(), &[1.5, 2.5]);
    }

    #[test]
    fn mmap_state_open_reads_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("open.vecs");
        {
            let mut ms = MmapState::create(path.clone(), 2).unwrap();
            ms.append(make_id(1), &[1.0, 0.0]).unwrap();
            ms.append(make_id(2), &[0.0, 1.0]).unwrap();
        }
        let ms = MmapState::open(path).unwrap();
        assert_eq!(ms.dims, 2);
        assert_eq!(ms.count, 2);
    }

    // ── HnswIndex mmap mode tests ─────────────────────────────────────────────

    #[test]
    fn mmap_index_insert_and_search() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.vecs");
        let mut idx = HnswIndex::new_mmap(path);

        let id_near = make_id(1);
        let id_far = make_id(2);
        idx.insert(id_near, vec![1.0_f32, 0.0, 0.0]);
        idx.insert(id_far, vec![0.0_f32, 0.0, 1.0]);

        let results = idx.search(&[1.0, 0.0, 0.0], 1, 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, id_near);
    }

    #[test]
    fn mmap_index_recall_matches_memory() {
        // Insert identical vectors into both modes and check that recall is the same.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recall.vecs");

        let mut mem_idx = HnswIndex::new();
        let mut mmap_idx = HnswIndex::new_mmap(path);

        let query = vec![1.0_f32, 0.0, 0.0, 0.0];

        for i in 0..20u8 {
            let id = make_id(i);
            let v = unit_vec(4, i as usize);
            mem_idx.insert(id, v.clone());
            mmap_idx.insert(id, v);
        }

        let mem_ids: Vec<NodeId> = mem_idx
            .search(&query, 5, 20)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let mmap_ids: Vec<NodeId> = mmap_idx
            .search(&query, 5, 20)
            .into_iter()
            .map(|(id, _)| id)
            .collect();

        assert_eq!(
            mem_ids, mmap_ids,
            "mmap search must return the same results as in-memory"
        );
    }
}
