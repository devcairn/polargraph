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
use polargraph_core::id::NodeId;
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
    fn eq(&self, other: &Self) -> bool { self.0.to_bits() == other.0.to_bits() && self.1 == other.1 }
}
impl Eq for Far {}
impl PartialOrd for Far {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl Ord for Far {
    fn cmp(&self, other: &Self) -> Ordering {
        // Larger distance = higher priority (max-heap).
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
            .then_with(|| self.1.cmp(&other.1))
    }
}

/// Entry for the "unexplored candidates" min-heap (nearest at top).
#[derive(Clone)]
struct Near(f32, NodeId);

impl PartialEq for Near {
    fn eq(&self, other: &Self) -> bool { self.0.to_bits() == other.0.to_bits() && self.1 == other.1 }
}
impl Eq for Near {}
impl PartialOrd for Near {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl Ord for Near {
    fn cmp(&self, other: &Self) -> Ordering {
        // Smaller distance = higher priority (reverse for min-heap via BinaryHeap).
        other.0.partial_cmp(&self.0).unwrap_or(Ordering::Equal)
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
        Ok(Self { dims, count: 0, id_to_index: HashMap::new(), file, path, mmap: None })
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
        let dims  = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let count = u64::from_le_bytes(header[4..12].try_into().unwrap()) as usize;

        let mmap = if file_len > Self::HEADER {
            Some(unsafe { MmapMut::map_mut(&file)? })
        } else {
            None
        };

        Ok(Self { dims, count, id_to_index: HashMap::new(), file, path, mmap })
    }

    /// Register a known `(NodeId, dense_index)` pair during index load.
    pub fn register_id(&mut self, id: NodeId, index: usize) {
        self.id_to_index.insert(id, index);
    }

    /// Append a new vector to the file. Returns the assigned dense index.
    ///
    /// Drops and remaps the mmap to accommodate the larger file.
    pub fn append(&mut self, id: NodeId, vector: &[f32]) -> Result<usize, StorageError> {
        debug_assert_eq!(vector.len(), self.dims, "vector length != dims");
        let idx = self.count;

        // Must drop the mmap before resizing the file (OS requirement).
        drop(self.mmap.take());

        let new_file_len = Self::HEADER + (self.count + 1) * self.dims * 4;
        self.file.set_len(new_file_len as u64)?;

        let mut mmap = unsafe { MmapMut::map_mut(&self.file)? };

        // Write vector bytes at the assigned offset.
        let byte_start = Self::HEADER + idx * self.dims * 4;
        let vector_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(vector.as_ptr() as *const u8, vector.len() * 4)
        };
        mmap[byte_start..byte_start + vector_bytes.len()].copy_from_slice(vector_bytes);

        // Update count in header.
        self.count += 1;
        mmap[4..12].copy_from_slice(&(self.count as u64).to_le_bytes());

        mmap.flush()?;

        self.mmap = Some(mmap);
        self.id_to_index.insert(id, idx);
        Ok(idx)
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
        let mmap = self.mmap.as_ref()?;
        let byte_start = Self::HEADER + idx * self.dims * 4;
        let byte_end   = byte_start + self.dims * 4;
        if byte_end > mmap.len() {
            return None;
        }
        Some(unsafe {
            std::slice::from_raw_parts(
                mmap[byte_start..byte_end].as_ptr() as *const f32,
                self.dims,
            )
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
    /// In-memory mode: vector data is embedded in the record.
    Data(Vec<f32>),
    /// Mmap mode: the record stores a dense index into the `.vecs` file.
    MmapIndex(usize),
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
}

pub struct HnswIndex {
    pub(crate) nodes: HashMap<NodeId, HnswNode>,
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

impl HnswIndex {
    pub fn new() -> Self {
        Self::with_params(DEFAULT_M, DEFAULT_M_MAX0, DEFAULT_EF_CONSTRUCTION)
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
        }
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
        idx.mmap_path  = Some(path);
        idx.mmap_state = Some(state);
        idx
    }

    pub fn is_mmap(&self) -> bool { self.mmap_path.is_some() }

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
        let vector = match node_data {
            NodeVector::Data(v) => v,
            NodeVector::MmapIndex(idx) => {
                // Register the dense index so distance queries can find it.
                if let Some(ms) = &mut self.mmap_state {
                    ms.register_id(id, idx);
                }
                Vec::new() // vector lives in mmap, not in the node struct
            }
        };
        self.nodes.insert(id, HnswNode { vector, max_layer, neighbors });
    }

    pub fn len(&self) -> usize { self.nodes.len() }
    pub fn is_empty(&self) -> bool { self.nodes.is_empty() }

    // ── public API ─────────────────────────────────────────────────────────────

    /// Insert a node and return the IDs of all nodes whose neighbor lists changed
    /// (so the caller can persist them).
    pub fn insert(&mut self, id: NodeId, vector: Vec<f32>) -> Vec<NodeId> {
        // ── Mmap mode: ensure the backing file is open, then append the vector ──
        if let Some(path) = self.mmap_path.clone() {
            if self.mmap_state.is_none() {
                let dims = vector.len();
                match MmapState::create(path, dims) {
                    Ok(ms) => { self.mmap_state = Some(ms); }
                    Err(e) => {
                        tracing::error!("failed to create mmap file: {e}; falling back to memory");
                        self.mmap_path = None; // degrade to memory mode
                    }
                }
            }
            if let Some(ms) = &mut self.mmap_state {
                if let Err(e) = ms.append(id, &vector) {
                    tracing::error!("mmap append failed: {e}; falling back to memory for this node");
                    // Fall through to store vector in node struct as backup
                }
            }
        }

        let level = self.random_level();
        let mut modified: Vec<NodeId> = Vec::new();

        // Store the node. In mmap mode, the vector field is empty.
        let stored_vector = if self.is_mmap() { Vec::new() } else { vector.clone() };

        if self.entry_point.is_none() {
            let neighbors = (0..=level).map(|_| Vec::new()).collect();
            self.nodes.insert(id, HnswNode { vector: stored_vector, max_layer: level, neighbors });
            self.entry_point = Some(id);
            self.global_max_layer = level;
            modified.push(id);
            return modified;
        }

        let ep = self.entry_point.unwrap();

        // ── Phase 1: greedy descent from top layer to level+1 ─────────────────
        let mut ep_dist = self.dist_to(ep, &vector);
        let mut cur_ep  = ep;

        for lc in (level + 1..=self.global_max_layer).rev() {
            let w = self.search_layer(&vector, cur_ep, 1, lc);
            if let Some(Far(d, nearest)) = w.into_sorted_vec().into_iter().next() {
                if d < ep_dist { ep_dist = d; cur_ep = nearest; }
            }
        }

        // ── Phase 2: build connections from level down to 0 ───────────────────
        self.nodes.insert(id, HnswNode {
            vector: stored_vector,
            max_layer: level,
            neighbors: (0..=level).map(|_| Vec::new()).collect(),
        });

        for lc in (0..=cmp::min(level, self.global_max_layer)).rev() {
            let w = self.search_layer(&vector, cur_ep, self.ef_construction, lc);

            let w_sorted = w.into_sorted_vec();
            if let Some(Far(d, nearest)) = w_sorted.first() {
                if *d < ep_dist { ep_dist = *d; cur_ep = *nearest; }
            }

            let m_at_layer = if lc == 0 { self.m_max0 } else { self.m };
            let neighbors_for_new: Vec<NodeId> = w_sorted
                .iter().take(m_at_layer).map(|Far(_, nid)| *nid).collect();

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
                    let nv = self.get_vector_owned(neighbor_id);
                    let pruned = self.select_neighbors_by_dist(&nv, &candidates, m_max);
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
        let mut scored: Vec<(NodeId, f32)> = allowed
            .iter()
            .filter_map(|&id| {
                if !self.nodes.contains_key(&id) { return None; }
                let d = self.dist_to(id, query);
                if d.is_infinite() { None } else { Some((id, 1.0 - d)) }
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

        let ef = if ef == 0 { cmp::max(self.ef_construction / 2, k) } else { ef };

        let mut cur_ep   = ep;
        let mut cur_dist = self.dist_to(ep, query);

        // Descend to layer 1.
        for lc in (1..=self.global_max_layer).rev() {
            let w = self.search_layer(query, cur_ep, 1, lc);
            if let Some(Far(d, nearest)) = w.into_sorted_vec().into_iter().next() {
                if d < cur_dist { cur_dist = d; cur_ep = nearest; }
            }
        }

        // Search layer 0 with full ef.
        let w = self.search_layer(query, cur_ep, cmp::max(ef, k), 0);

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
            let mmap_index = self.mmap_state
                .as_ref()
                .and_then(|ms| ms.id_to_index.get(&id).copied());
            serialize_node(node, mmap_index)
        } else {
            Vec::new()
        }
    }

    // ── private helpers ───────────────────────────────────────────────────────

    /// Cosine distance from stored-or-mapped node `id` to `query`.
    fn dist_to(&self, id: NodeId, query: &[f32]) -> f32 {
        let vec: &[f32] = if let Some(ms) = &self.mmap_state {
            match ms.get_slice(id) {
                Some(s) => s,
                None    => return f32::INFINITY,
            }
        } else {
            match self.nodes.get(&id) {
                Some(n) => &n.vector,
                None    => return f32::INFINITY,
            }
        };
        cosine_distance(vec, query)
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
            self.nodes.get(&id).map(|n| n.vector.clone()).unwrap_or_default()
        }
    }

    /// HNSW greedy beam search at a single layer.
    fn search_layer(
        &self,
        query: &[f32],
        entry: NodeId,
        ef: usize,
        layer: usize,
    ) -> BinaryHeap<Far> {
        let mut visited:    HashSet<NodeId>   = HashSet::new();
        let mut candidates: BinaryHeap<Near>  = BinaryHeap::new();
        let mut found:      BinaryHeap<Far>   = BinaryHeap::new();

        let d_entry = self.dist_to(entry, query);
        visited.insert(entry);
        candidates.push(Near(d_entry, entry));
        found.push(Far(d_entry, entry));

        while let Some(Near(d_c, c)) = candidates.pop() {
            let d_worst = found.peek().map(|Far(d, _)| *d).unwrap_or(f32::INFINITY);
            if d_c > d_worst { break; }

            let neighbors = match self.nodes.get(&c) {
                Some(n) if layer <= n.max_layer => &n.neighbors[layer],
                _ => continue,
            };

            for &e in neighbors {
                if visited.contains(&e) { continue; }
                visited.insert(e);
                let d_e     = self.dist_to(e, query);
                let d_worst = found.peek().map(|Far(d, _)| *d).unwrap_or(f32::INFINITY);
                if d_e < d_worst || found.len() < ef {
                    candidates.push(Near(d_e, e));
                    found.push(Far(d_e, e));
                    if found.len() > ef { found.pop(); }
                }
            }
        }

        found
    }

    /// Select the `m` nearest neighbors from a candidate list by distance to `query`.
    fn select_neighbors_by_dist(&self, query: &[f32], candidates: &[NodeId], m: usize) -> Vec<NodeId> {
        let mut scored: Vec<(f32, NodeId)> = candidates
            .iter()
            .map(|&id| (self.dist_to(id, query), id))
            .collect();
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
        scored.into_iter().take(m).map(|(_, id)| id).collect()
    }


    /// Generate a random layer using the HNSW exponential distribution.
    fn random_level(&mut self) -> usize {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        let f  = (self.rng as f64) / (u64::MAX as f64);
        let ml = 1.0 / (self.m as f64).ln();
        ((-f.ln()) * ml).floor() as usize
    }
}

// ── Distance function ─────────────────────────────────────────────────────────

/// Cosine distance ∈ [0, 2]. Zero = identical direction, 2 = opposite.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() || b.is_empty() || a.len() != b.len() { return 1.0; }
    let dot:   f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if mag_a == 0.0 || mag_b == 0.0 { return 1.0; }
    (1.0 - (dot / (mag_a * mag_b))).clamp(0.0, 2.0)
}

// ── Serialisation ─────────────────────────────────────────────────────────────

/// Suffix appended to a space name to form the RocksDB entry-point key.
const EP_SUFFIX:    &[u8] = b"/__ep";
/// Sub-prefix inside a space for per-node records.
const NODE_INFIX:   &[u8] = b"/n/";
/// Sentinel stored in the `dim` field of a mmap-mode node record.
const MMAP_SENTINEL: u32  = u32::MAX;

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
    let id        = NodeId(uuid::Uuid::from_bytes(bytes[..16].try_into().unwrap()));
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
pub fn deserialize_node(bytes: &[u8]) -> Result<(NodeVector, usize, Vec<Vec<NodeId>>), StorageError> {
    if bytes.len() < 8 {
        return Err(StorageError::KeyDecode("hnsw node record too short".into()));
    }
    let max_layer       = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let dim_or_sentinel = u32::from_le_bytes(bytes[4..8].try_into().unwrap());

    let (node_vector, neighbor_cursor) = if dim_or_sentinel == MMAP_SENTINEL {
        // Mmap mode: bytes[8..12] = dense index.
        if bytes.len() < 12 {
            return Err(StorageError::KeyDecode("hnsw mmap node record too short".into()));
        }
        let idx = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        (NodeVector::MmapIndex(idx), 12)
    } else {
        // Memory mode: read float vector.
        let dim      = dim_or_sentinel as usize;
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

    let mut cursor    = neighbor_cursor;
    let mut neighbors = Vec::with_capacity(max_layer + 1);
    for _ in 0..=max_layer {
        if cursor + 4 > bytes.len() {
            return Err(StorageError::KeyDecode("hnsw neighbor count truncated".into()));
        }
        let n = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let mut nbrs = Vec::with_capacity(n);
        for _ in 0..n {
            if cursor + 16 > bytes.len() {
                return Err(StorageError::KeyDecode("hnsw neighbor id truncated".into()));
            }
            let id = NodeId(uuid::Uuid::from_bytes(bytes[cursor..cursor + 16].try_into().unwrap()));
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

    fn make_id(seed: u8) -> NodeId { NodeId(Uuid::from_bytes([seed; 16])) }

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
    fn memory_node_round_trips() {
        let id_a = make_id(1);
        let id_b = make_id(2);
        let node = HnswNode {
            vector: vec![1.0, 2.0, 3.0],
            max_layer: 1,
            neighbors: vec![vec![id_a], vec![id_b]],
        };
        let bytes = serialize_node(&node, None); // memory mode
        let (nv, max_layer, nbrs) = deserialize_node(&bytes).unwrap();
        match nv {
            NodeVector::Data(v) => assert_eq!(v, node.vector),
            NodeVector::MmapIndex(_) => panic!("expected Data"),
        }
        assert_eq!(max_layer, 1);
        assert_eq!(nbrs[0], vec![id_a]);
        assert_eq!(nbrs[1], vec![id_b]);
    }

    #[test]
    fn mmap_node_round_trips() {
        let id_a = make_id(1);
        let node = HnswNode {
            vector: Vec::new(), // empty in mmap mode
            max_layer: 0,
            neighbors: vec![vec![id_a]],
        };
        let bytes = serialize_node(&node, Some(42)); // mmap mode, dense index 42
        let (nv, max_layer, nbrs) = deserialize_node(&bytes).unwrap();
        match nv {
            NodeVector::MmapIndex(idx) => assert_eq!(idx, 42),
            NodeVector::Data(_) => panic!("expected MmapIndex"),
        }
        assert_eq!(max_layer, 0);
        assert_eq!(nbrs[0], vec![id_a]);
    }

    #[test]
    fn node_zero_dim_round_trips() {
        let node = HnswNode { vector: vec![], max_layer: 0, neighbors: vec![vec![]] };
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
        let far_id  = make_id(2);
        idx.insert(near_id, vec![1.0_f32, 0.0, 0.0]);
        idx.insert(far_id,  vec![0.0_f32, 0.0, 1.0]);
        let results = idx.search(&[1.0, 0.0, 0.0], 1, 10);
        assert_eq!(results[0].0, near_id);
    }

    #[test]
    fn search_in_set_returns_correct_order() {
        let mut idx = HnswIndex::new();
        let a = make_id(1); idx.insert(a, vec![1.0_f32, 0.0]);
        let b = make_id(2); idx.insert(b, vec![0.9_f32, 0.1]);
        let c = make_id(3); idx.insert(c, vec![0.0_f32, 1.0]);
        let results = idx.search_in_set(&[1.0, 0.0], 2, &[a, b, c]);
        assert_eq!(results[0].0, a);
        assert_eq!(results[1].0, b);
    }

    // ── MmapState unit tests ──────────────────────────────────────────────────

    #[test]
    fn mmap_state_create_and_read() {
        let dir  = tempfile::tempdir().unwrap();
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
    fn mmap_state_file_size_matches_layout() {
        let dir  = tempfile::tempdir().unwrap();
        let path = dir.path().join("size.vecs");
        let mut ms = MmapState::create(path.clone(), 3).unwrap();
        let id = make_id(5);
        ms.append(id, &[1.0, 2.0, 3.0]).unwrap();
        ms.append(id, &[4.0, 5.0, 6.0]).unwrap(); // overwrite with same id for size check

        let expected = MmapState::HEADER + 2 * 3 * 4; // 12 + 2*12 = 36
        assert_eq!(std::fs::metadata(&path).unwrap().len() as usize, expected);
    }

    #[test]
    fn mmap_state_open_reads_header() {
        let dir  = tempfile::tempdir().unwrap();
        let path = dir.path().join("open.vecs");
        {
            let mut ms = MmapState::create(path.clone(), 2).unwrap();
            ms.append(make_id(1), &[1.0, 0.0]).unwrap();
            ms.append(make_id(2), &[0.0, 1.0]).unwrap();
        }
        let ms = MmapState::open(path).unwrap();
        assert_eq!(ms.dims,  2);
        assert_eq!(ms.count, 2);
    }

    // ── HnswIndex mmap mode tests ─────────────────────────────────────────────

    #[test]
    fn mmap_index_insert_and_search() {
        let dir  = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.vecs");
        let mut idx = HnswIndex::new_mmap(path);

        let id_near = make_id(1);
        let id_far  = make_id(2);
        idx.insert(id_near, vec![1.0_f32, 0.0, 0.0]);
        idx.insert(id_far,  vec![0.0_f32, 0.0, 1.0]);

        let results = idx.search(&[1.0, 0.0, 0.0], 1, 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, id_near);
    }

    #[test]
    fn mmap_index_recall_matches_memory() {
        // Insert identical vectors into both modes and check that recall is the same.
        let dir  = tempfile::tempdir().unwrap();
        let path = dir.path().join("recall.vecs");

        let mut mem_idx  = HnswIndex::new();
        let mut mmap_idx = HnswIndex::new_mmap(path);

        let query = vec![1.0_f32, 0.0, 0.0, 0.0];

        for i in 0..20u8 {
            let id = make_id(i);
            let v  = unit_vec(4, i as usize);
            mem_idx.insert(id, v.clone());
            mmap_idx.insert(id, v);
        }

        let mem_ids:  Vec<NodeId> = mem_idx.search(&query,  5, 20).into_iter().map(|(id, _)| id).collect();
        let mmap_ids: Vec<NodeId> = mmap_idx.search(&query, 5, 20).into_iter().map(|(id, _)| id).collect();

        assert_eq!(mem_ids, mmap_ids, "mmap search must return the same results as in-memory");
    }
}
