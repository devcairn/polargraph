# PolarGraph DB Engine — Architecture

This document describes the design of PolarGraph: why the major choices were
made, how the pieces fit together, and what the current capabilities are.

---

## Overview

PolarGraph is a purpose-built graph database. Its core abstraction is the
**triple** — an atomic (subject, predicate, object) statement — stored with
full bitemporal versioning and indexed six ways for O(log n) lookups on any
combination of bound variables. On top of this sit an optimistic MVCC
concurrency layer, a Datalog query evaluator, a pure-Rust HNSW vector index,
and a gRPC server.

The motivating constraints:

- **Temporal correctness**: facts have a real-world validity period distinct
  from when they were recorded. Both axes must be queryable independently.
- **Graph flexibility**: relationships and properties share one index; no
  schema migration is needed to add a new relationship type.
- **Vector search as a first-class citizen**: node embeddings live alongside
  graph structure in the same store, not in a sidecar system.
- **Embeddability**: the storage layer is a library, not just a server, so the
  application can own the process boundary.

---

## Crate graph

```
polargraph-core       (no external I/O, no async)
      ↑
polargraph-storage    (RocksDB, MVCC, HNSW index)
      ↑
polargraph-query      (Datalog evaluator, view projection)
      ↑
polargraph-server     (gRPC binary: polargraphd)
```

`polargraph-core` is intentionally dependency-free at the I/O level. Every
type that crosses a crate boundary lives here, keeping the type system the
single source of truth and compile times for upper crates fast.

---

## Data model

### Triple

The atomic unit of storage is a `Triple`. There are two variants:

**Relation triple** — a directed edge between two nodes:

```
(subject: NodeId) --[predicate: String]--> (object: NodeId)
```

**Property triple** — a scalar attribute on a node:

```
(subject: NodeId) --[predicate: String]--> (value: Value)
```

Both variants carry a `BiTemporalRange`. There is no separate table for
nodes: a node exists by virtue of appearing as the subject (or object) of at
least one triple.

### Predicate

Predicates are arbitrary strings (`"works_at"`, `"name"`, `"since"`, …).
They are stored verbatim externally but **interned** to compact `u32` IDs
inside index keys. The intern table lives in the META column family and is
loaded into memory at store-open time. Adding a new predicate requires no
schema change — the first insert auto-assigns an ID.

### Value

```
Null
Bool(bool)
Int(i64)
Float(f64)
Text(String)
Blob(Vec<u8>)
Vector(Vec<f32>)   — dense embedding vector; stored in binary (not JSON)
```

All variants except `Vector` are JSON-encoded in the RocksDB value bytes.
`Vector` uses a dedicated binary codec (discriminant `0x03`) to avoid JSON
overhead on large float arrays — see [Value encoding](#value-encoding).

### Identifiers

`NodeId` and `EdgeId` wrap UUID v7. UUID v7 is time-ordered, so IDs sort
chronologically in the index without a central sequence generator.
The 16-byte representation is used directly in index keys.

---

## Bitemporal model

Every `BiTemporalRange` carries three fields:

| Field | Meaning |
|-------|---------|
| `vt_start` | When the fact became true in the world (valid time, µs since epoch) |
| `vt_end` | When it ceased to be true; `i64::MAX` if still current |
| `tt` | Transaction time: the µs timestamp at which this version was written |

This supports three classes of historical query:

- **Current state** — filter to `vt_end = END_OF_TIME` and `tt ≤ now`
- **Valid-time query** — "what was true at world-time T?" → `vt_start ≤ T < vt_end`
- **Bitemporally anchored** — "what did we *believe* at audit time A about
  world-time T?" → filter both axes simultaneously

`tt` is stored in the last 8 bytes of every index key, not in the value
payload. MVCC snapshot filtering therefore requires only a key comparison —
no value decode needed to discard invisible entries.

---

## Storage layer

### Column families

PolarGraph opens eight RocksDB column families:

| CF | Purpose |
|----|---------|
| `spo` | Subject → Predicate → Object index |
| `sop` | Subject → Object → Predicate index |
| `pso` | Predicate → Subject → Object index |
| `pos` | Predicate → Object → Subject index |
| `osp` | Object → Subject → Predicate index |
| `ops` | Object → Predicate → Subject index |
| `meta` | Predicate intern table, timestamp oracle counter |
| `hnsw` | HNSW vector index nodes and entry-point record |

The six triple-index CFs implement the **hexastore** pattern. Every insert
writes atomically to all six via a single `WriteBatch`. This makes every
read O(log n) with no secondary lookups, at the cost of 6× write amplification.

### Index selection

The query planner maps each (S, P, O) bind pattern to the cheapest CF:

| Bound slots | CF used | Prefix width |
|-------------|---------|-------------|
| S, P, O | SPO | 36-byte exact key |
| S, P | SPO | 20 bytes |
| S, O | SOP | 32 bytes |
| S | SPO | 16 bytes |
| P, O | POS | 20 bytes |
| P | PSO | 4 bytes |
| O | OSP | 16 bytes |
| (none) | SPO | full scan |

### Key layout

All triple-index keys are fixed-width at 44 bytes, so RocksDB's default
lexicographic comparator gives correct range scans without a custom comparator:

```
SPO  [subject(16)][pred_id(4)][object(16)][tt(8)]
SOP  [subject(16)][object(16)][pred_id(4)][tt(8)]
PSO  [pred_id(4)][subject(16)][object(16)][tt(8)]
POS  [pred_id(4)][object(16)][subject(16)][tt(8)]
OSP  [object(16)][subject(16)][pred_id(4)][tt(8)]
OPS  [object(16)][pred_id(4)][subject(16)][tt(8)]
```

`tt` is always the last 8 bytes. All versions of an (S,P,O) tuple therefore
sort together and in chronological order, enabling MVCC snapshot filtering
via forward scan with early exit.

### Property triples in the index

Property triples (scalar values) use a **sentinel object** (`0xFF × 16`) in
the object slot of every index key. This puts them in the same key space as
relation triples, allowing a single scan implementation for both variants.

### Value encoding

The RocksDB value bytes carry a 1-byte discriminant followed by temporal and
payload data:

```
Relation  [0x01][edge_id: 16][vt_start: 8 BE][vt_end: 8 BE]        = 33 bytes
Property  [0x02][vt_start: 8 BE][vt_end: 8 BE][json_payload: N]     = 17+N bytes
Vector    [0x03][vt_start: 8 BE][vt_end: 8 BE][len: 4 LE][f32×len LE]
```

`tt` is recovered from the key and is not duplicated in the value bytes.
`Vector` values bypass JSON entirely: `len` is a little-endian `u32` count
of `f32` elements followed by the raw IEEE 754 bytes, also little-endian.

---

## MVCC layer

PolarGraph uses **optimistic concurrency control** (OCC):

```
begin()      → snapshot read_ts from TimestampOracle (AtomicI64 load)
reads        → filter all index entries to tt ≤ read_ts
write buffer → collect triples in memory (no locks held)
commit()     → acquire commit_lock (Mutex)
               advance oracle → commit_ts = read_ts + 1
               conflict check: any (S,P,O) with read_ts < tt ≤ commit_ts?
               if clean: flush WriteBatch with commit_ts stamped on every key
               persist oracle counter to META CF
               release commit_lock
```

**Conflict detection** scans the SPO key range for each buffered (S,P,O)
tuple and checks whether any key has `tt` in `(read_ts, commit_ts]`. A hit
means another transaction committed to that triple after the current
transaction began; the commit returns `StorageError::WriteConflict`.

**TimestampOracle** is an `AtomicI64` plus a `Mutex<()>`. Read transactions
sample the atomic without touching it (no contention). Commit transactions
acquire the mutex to serialize the increment-and-write sequence.

The oracle counter persists to the META CF so restarts do not reuse
timestamps and old snapshot reads remain valid.

**Snapshots** (`polargraph_storage::Snapshot`) are the read-only counterpart:
a `TripleStore` handle plus a fixed `ts`; all scan methods filter to `tt ≤ ts`.

---

## HNSW vector index

### Algorithm

The HNSW (Hierarchical Navigable Small World) index is implemented in pure
Rust in `polargraph_storage::hnsw`, with no external HNSW dependencies.

Each node in the index exists at layers `0..=level`, where `level` is drawn
from an exponential distribution at insert time using an xorshift64 PRNG.
The graph is multi-layer: higher layers have sparser connections and act as
long-range highways; layer 0 contains all nodes with the densest connections.

**Default parameters:**

| Parameter | Default | Meaning |
|-----------|---------|---------|
| M | 16 | Max bidirectional connections per node per layer |
| M_max0 | 32 | Max connections at layer 0 (typically 2×M) |
| ef_construction | 200 | Beam width during index construction |

**Distance metric**: cosine distance = `1 − cosine_similarity`. Lower is
more similar. Search results are returned as cosine *similarity* scores
(higher = more similar) in the range [−1, 1].

**Insert**: greedy descent from the entry-point layer to the new node's layer,
then beam-search construction at each layer down to 0, wiring bidirectional
edges and pruning neighbor lists that exceed M or M_max0.

**Search**: greedy descent to layer 1, then beam search at layer 0 with
`ef = max(ef_construction / 2, k)`. Returns the k nearest nodes sorted by
descending similarity.

### Named spaces

`TripleStore` holds a `HashMap<String, HnswIndex>` keyed by *space name*
rather than a single global index. This allows different node types (or any
logical collection) to have separate ANN indexes with independently sized
embeddings. The caller supplies a space name on every `insert_vector` and
`search_vector` call; an empty string is treated as `"default"`.

When a `NodeTypeDef` is registered with a `VectorSpaceDef`, `insert_vector`
validates that the incoming vector's dimensionality matches the declared
`dimensions` field and rejects mismatches with `InvalidArgument`.

### Persistence

The store holds each named `HnswIndex` behind a `RwLock<HashMap<String, HnswIndex>>`
and mirrors it to the `hnsw` RocksDB CF on every insert.

Key layout in the `hnsw` CF:

```
<space>/__ep            → [node_id: 16][max_layer: 4 LE]   (entry point for this space)
<space>/n/<node_id(16)> → serialised HnswNode
```

On `TripleStore::open`, the store does a two-phase scan: it first finds all
`/__ep` suffixed keys to discover the set of space names, then prefix-scans
each space's `<space>/n/` range to reconstruct the in-memory indexes.

`HnswNode` serialisation:

```
[max_layer: 4 LE][dim: 4 LE][f32 × dim LE]
[for l = 0..=max_layer: [n_neighbors: 4 LE][node_id × n LE]]
```

### Vector storage modes

Each HNSW space is created in one of two vector storage modes, controlled by
`StorageMode` on `VectorSpaceDef`:

| Mode | How vectors are stored | When to use |
|------|----------------------|-------------|
| **Memory** (default) | `Vec<f32>` inside each `HnswNode`, all in heap RAM | Small-to-medium spaces, fastest search |
| **Mmap** | Flat binary `.vecs` file under `<data_dir>/vectors/`, accessed via `memmap2::MmapMut` | Large spaces that exceed available RAM; OS pages vectors in on demand |

In **Mmap** mode the graph topology (neighbor lists, entry point) is still
stored in RocksDB as usual. Only the raw float data lives in the `.vecs`
file. The in-memory `HnswNode` stores a dense index into the mmap region
instead of the vector bytes; distance computations read directly from the
mapped region without copying. On reopen, the mmap file is re-mapped and
the `id → dense_index` mapping is reconstructed from the RocksDB scan.

**Trade-offs:** Mmap mode avoids the upfront heap allocation for large
indexes but adds a system call overhead on cold pages. For spaces that fit
comfortably in RAM, Memory mode is faster. Once a space is created in a
given mode it remains in that mode for the lifetime of the data directory.

### API

```rust
store.insert_vector(space: &str, node_id: NodeId, vector: Vec<f32>, mode: StorageMode) -> Result<()>
store.search_vector(space: &str, query: Vec<f32>, k: usize) -> Vec<(NodeId, f32)>
store.search_vector_ef(space: &str, query: &[f32], k: usize, ef: usize) -> Vec<(NodeId, f32)>
store.search_vector_in_set(space: &str, query: &[f32], k: usize, allowed: &[NodeId]) -> Vec<(NodeId, f32)>
store.batch_insert_vectors(space: &str, items: &[(NodeId, Vec<f32>)], mode: StorageMode) -> (usize, Vec<(usize, StorageError)>)
```

`search_vector` acquires a read lock and performs no I/O. `insert_vector`
acquires a write lock, runs the HNSW insert, then flushes all modified node
records to RocksDB in a single `WriteBatch`.

`search_vector_ef` is identical to `search_vector` but lets the caller set
the exploration factor explicitly — use `ef = k * 10` when building a large
candidate pool for post-filtering (e.g. `SearchVectorFiltered`).

`search_vector_in_set` linearly scores every node in `allowed` against the
query vector using stored embeddings, returning the top-k. This is O(|allowed|)
and is used when the candidate set comes from a preceding graph traversal.

`batch_insert_vectors` acquires the write lock once for the entire batch,
avoiding per-item lock overhead. Returns count inserted and any per-item errors.

---

## Query layer

### Pattern evaluation (`polargraph_query::eval`)

`evaluate(pattern: &Pattern, snapshot: &Snapshot)` drives a single-CF prefix
scan selected by `planner::choose_index`, filters to `tt ≤ snapshot.ts`,
deduplicates each (S,P,O) tuple to its latest committed version, and returns
the matching `Triple` values.

### Conjunctive queries (`polargraph_query::datalog`)

`execute_query(query: &Query, snapshot: &Snapshot)` evaluates a list of
`VarPattern`s left-to-right, binding variables greedily:

1. The first pattern is evaluated against storage to produce an initial set of
   `Bindings` (maps from variable name to `NodeId`).
2. Each subsequent pattern is evaluated once per existing binding, substituting
   bound values into the pattern's slots. Only bindings that satisfy all
   patterns survive.

This is a nested-loop join. Performance scales with selectivity of earlier
patterns, so patterns with more bound slots should be placed first.

### Recursive rules (`polargraph_query::datalog`)

`execute_recursive(seed, rules, snapshot)` runs a semi-naïve fixpoint over
a set of `Rule` objects. A `Rule` has a head predicate and a body of
`VarPattern`s. On each iteration, new derived facts are computed from the
previous iteration's additions and added to the `DerivedFacts` map. Iteration
terminates when no new facts are derived (fixpoint reached).

### Transitive reachability

Two convenience functions build on the recursive evaluator:

```rust
// Unbounded: follows `predicate` edges until the graph is exhausted.
reachable_from(start: NodeId, predicate: &str, snapshot: &Snapshot)
    -> Result<HashSet<NodeId>>

// Bounded: BFS limited to at most `max_hops` edge traversals.
reachable_from_hops(start: NodeId, predicate: &str, snapshot: &Snapshot, max_hops: usize)
    -> Result<HashSet<NodeId>>
```

`reachable_from` constructs a single recursive rule
`reachable(X, Z) :- reachable(X, Y), predicate(Y, Z)` and runs it to fixpoint.
`reachable_from_hops` is a plain BFS over `scan_by_subject_predicate` calls,
stopping after `max_hops` layers.

Neither function includes the start node in the returned set.

### View projection (`polargraph_query::projection`)

`apply_view(view: &View, triples)` filters a triple set through a `View`:

- **`node_filter`**: include only nodes whose type is in the allowed set,
  plus any explicitly pinned `NodeId`s.
- **`visible_predicates`**: whitelist of predicate strings; empty = show all.
- **`edge_presentations`**: per-predicate label overrides and direction-flip hints.

The returned `ProjectedTriple` values carry a `display_label` (the overridden
or canonical predicate name) and an `is_reversed` flag (rendering hint only —
the underlying triple is unchanged).

---

## gRPC API

The server binary (`polargraphd`) exposes `polargraph.v1.PolarGraphService`
over gRPC. The generated code lives in `polargraph_server::proto`.

### RPCs

| RPC | Request | Response | Notes |
|-----|---------|----------|-------|
| `Insert` | `InsertRequest` | `InsertResponse` | Atomically commits ≥1 triples; returns `ABORTED` on write-write conflict |
| `Query` | `QueryRequest` | `QueryResponse` | Conjunctive pattern query; returns all satisfying variable bindings |
| `InsertVector` | `InsertVectorRequest` | `InsertVectorResponse` | Upserts a node's embedding vector into the HNSW index |
| `SearchVector` | `SearchVectorRequest` | `SearchVectorResponse` | Returns k nearest neighbors with cosine similarity scores |
| `Reachable` | `ReachableRequest` | `ReachableResponse` | Transitive closure from a start node along a named predicate |
| `RegisterNodeType` | `RegisterNodeTypeRequest` | `RegisterNodeTypeResponse` | Register or overwrite a node type schema |
| `GetNodeType` | `GetNodeTypeRequest` | `GetNodeTypeResponse` | Look up a schema by type name; returns empty if unknown |
| `ListNodeTypes` | `ListNodeTypesRequest` | `ListNodeTypesResponse` | Return all registered schemas |
| `ValidateNode` | `ValidateNodeRequest` | `ValidateNodeResponse` | Validate a property map against a schema; returns errors if invalid |
| `RegisterEdgeType` | `RegisterEdgeTypeRequest` | `RegisterEdgeTypeResponse` | Register or overwrite an edge type schema |
| `GetEdgeType` | `GetEdgeTypeRequest` | `GetEdgeTypeResponse` | Look up an edge schema by predicate name; returns empty if unknown |
| `ListEdgeTypes` | `ListEdgeTypesRequest` | `ListEdgeTypesResponse` | Return all registered edge type schemas |
| `ValidateEdge` | `ValidateEdgeRequest` | `ValidateEdgeResponse` | Validate endpoint types and property map; returns errors if invalid |
| `ListPredicatesBetween` | `ListPredicatesBetweenRequest` | `ListPredicatesBetweenResponse` | Return predicate names whose domain/range match the given node types |
| `SearchVectorFiltered` | `SearchVectorFilteredRequest` | `SearchVectorFilteredResponse` | HNSW search with a node-type or reachability post-filter |
| `SearchVectorInSet` | `SearchVectorInSetRequest` | `SearchVectorInSetResponse` | Score an explicit node-ID set against a query vector; return top-k |
| `BatchInsertVectors` | `BatchInsertVectorsRequest` | `BatchInsertVectorsResponse` | Insert multiple vectors into a named space in a single write |

### Insert

`InsertRequest.triples` is a list of `Triple` (union of `RelationTriple` and
`PropertyTriple`). All triples are committed in a single transaction; on
conflict the entire batch is rejected. The response carries `commit_ts` — the
transaction time assigned to the write.

### Query

`QueryRequest.patterns` is a list of `VarPattern`s evaluated as a conjunctive
query. The response is a list of `Binding` maps from variable name to `NodeId`.

Two optional fields enable point-in-time time-travel (see [Time-travel queries](#time-travel-queries)):

| Field | Type | Meaning |
|-------|------|---------|
| `snapshot_ts` | `int64` | Read at this transaction timestamp (µs). 0 = latest. |
| `as_of_tx_time` | `int64` | Override `snapshot_ts`: only triples committed at or before this wall-clock time are visible. 0 = use `snapshot_ts`. |
| `as_of_valid_time` | `int64` | Additional valid-time filter: only triples whose `[vt_start, vt_end)` window contains this value are returned. 0 = no filter. |

### InsertVector / SearchVector

`InsertVectorRequest` carries a `node_id`, a `repeated float vector`, and an
optional `space` string (empty → `"default"`). If a `NodeTypeDef` with a
`VectorSpaceDef` matching the space name is registered, the server validates
`vector.len() == VectorSpaceDef.dimensions` and rejects mismatches.

`SearchVectorRequest` carries a `repeated float query`, a `uint32 k` (0
defaults to 10), and an optional `space` string. Results are
`VectorSearchResult` pairs of `(node_id, similarity)` ordered by descending
similarity.

### SearchVectorFiltered

`SearchVectorFilteredRequest` carries a space, query, k, and a `oneof filter`:

- **`NodeTypeFilter { type_name }`** — restricts candidates to nodes whose
  `__type` property equals `type_name`. The server scans the `__type`
  predicate in the current snapshot to build the allowed set, then runs HNSW
  with `ef = k * 10` and post-filters.
- **`ReachabilityFilter { from_node, predicate, max_hops }`** — restricts
  candidates to nodes reachable from `from_node` via `predicate` within
  `max_hops` hops (0 = unlimited). Reachability is computed first via BFS /
  Datalog fixpoint, then HNSW is searched with a large candidate pool and
  filtered to the reachable set.

### SearchVectorInSet

`SearchVectorInSetRequest` carries a space, query, k, and an explicit
`repeated NodeId node_ids`. The server scores every listed node against the
query using the stored embedding (skipping nodes absent from the index) and
returns the top-k. O(|node_ids|) — appropriate for small sets derived from
graph traversals.

### BatchInsertVectors

`BatchInsertVectorsRequest` carries a space and a list of `(node_id, vector)`
`VectorItem`s. All items are inserted under a single write-lock acquisition.
Dimension validation is applied per item if a space def is registered; items
that fail validation are returned as `BatchInsertError` records and do not
count toward `count_inserted`.

### Reachable

`ReachableRequest` carries a `start` `NodeId`, a `predicate` string, and an
optional `max_hops` count. `max_hops = 0` means unlimited depth (uses the
recursive Datalog fixpoint); any positive value runs BFS limited to that many
hops. The response is an unordered list of reachable `NodeId`s; the start
node is not included.

### VectorSeedQuery

`VectorSeedQueryRequest` combines ANN vector search with a conjunctive Datalog
graph query in a single server-side call. It solves the common hybrid query
pattern: *"find nodes semantically similar to X that are also connected to Y
via predicate Z"* — without requiring two separate RPCs and client-side joining.

**Protocol**

The handler runs four steps in sequence:

1. **ANN search** — retrieve up to `k` nearest neighbors of `query_vector` in
   the named HNSW space, applying the optional `filter` (NodeType or
   Reachability) as a post-filter on ANN candidates. This uses
   `search_vector_ef` with `ef = k × 10` for candidate over-retrieval.

2. **Seed bindings** — each ANN hit becomes an initial `Bindings` row with
   `seed_variable` bound to that node's ID. A score map
   `(NodeId → similarity)` is built from the same hits.

3. **Graph pattern join** — the initial seed bindings are passed into
   `execute_query_seeded` (the seeded variant of the conjunctive Datalog
   evaluator). Each pattern is applied in order, narrowing or expanding
   solutions exactly as in a normal `Query` call. If `patterns` is empty,
   `execute_query_seeded` returns the seeds unchanged — the RPC then
   behaves identically to `SearchVector` but with `ScoredBinding` output.

4. **Score attachment** — each surviving `Bindings` row is converted to a
   `ScoredBinding` by looking up the `seed_variable` value in the score map.
   Rows where the seed variable was re-bound by a later pattern use the
   original ANN score of the *initial* seed value.

**`execute_query_seeded`**

This function in `polargraph_query::datalog` is a generalization of
`execute_query`. Both accept a `&Query` and a `&Snapshot`; `execute_query_seeded`
also accepts an `initial: Vec<Bindings>` starting set.
`execute_query` delegates to it with `vec![HashMap::new()]` as the initial
set — the identity element for join. Passing multiple pre-bound rows allows
the ANN results to act as entry points into the graph query without
materializing a separate triple for each seed.

**Wire format**

```proto
message ScoredBinding {
    map<string, NodeId> vars  = 1;
    float               score = 2;
}
message VectorSeedQueryRequest {
    string              space         = 1;
    repeated float      query_vector  = 2;
    uint32              k             = 3;
    string              seed_variable = 4;
    repeated VarPattern patterns      = 5;
    int64               snapshot_ts   = 6;
    oneof filter {
        NodeTypeFilter     node_type_filter    = 7;
        ReachabilityFilter reachability_filter = 8;
    }
}
message VectorSeedQueryResponse {
    repeated ScoredBinding bindings = 1;
}
```

---

## Server configuration

`polargraphd` accepts configuration via CLI flags or environment variables.
Flags take priority.

| Flag | Env variable | Default | Description |
|------|-------------|---------|-------------|
| `--data-dir PATH` | `POLARGRAPH_DATA_DIR` | `/data` | RocksDB data directory (created if absent) |
| `--listen ADDR` | `POLARGRAPH_LISTEN_ADDR` | `0.0.0.0:50051` | gRPC listen address |
| `--log FILTER` | `RUST_LOG` | `info` | Log filter (same syntax as `RUST_LOG`) |

The server handles graceful shutdown on SIGTERM and Ctrl-C.

---

## View system

A `View` is a named lens over the graph defined by:

- **`node_filter`** — include only nodes whose `node_type` is in a given set,
  plus any explicitly pinned `NodeId`s.
- **`visible_predicates`** — a whitelist of predicate strings (empty = show all).
- **`edge_presentations`** — per-predicate label overrides and direction-flip hints.

Views are themselves stored as triples in the graph, gaining bitemporal
versioning and access control for free. The in-memory `View` struct is the
deserialized form the projection engine works with.

---

## Node type registry

### Design

PolarGraph supports **optional, advisory schemas** for node types. A schema
declares which property predicates a type is expected to have, their value
kinds, and which fields are required. The store is open-world: it accepts any
triple regardless of whether the subject's type has a registered schema.
Validation is an explicit call, not a write-time gate.

### Schema types (`polargraph-core::schema`)

```rust
enum FieldKind { Bool, Int, Float, Text, Blob, Vector }
struct FieldDef      { name: String, kind: FieldKind, required: bool }
enum StorageMode { Memory, Mmap }
struct VectorSpaceDef {
    space_name:      String,         // HNSW space key (e.g. the type name)
    dimensions:      u32,            // expected vector dimensionality
    embedding_model: Option<String>, // informational: which model produces these vectors
    storage_mode:    StorageMode,    // Memory (default) or Mmap
}
struct NodeTypeDef {
    type_name:    String,
    fields:       Vec<FieldDef>,
    vector_space: Option<VectorSpaceDef>,  // if set, dimension-validates insert_vector calls
}
struct EdgeTypeDef {
    predicate: String,           // relation predicate this schema governs
    domain: Option<String>,      // allowed subject node type (None = unconstrained)
    range:  Option<String>,      // allowed object node type  (None = unconstrained)
    fields: Vec<FieldDef>,       // edge property constraints
}
```

### Storage (`polargraph-storage::registry`)

Both node and edge schemas are stored as property triples under a single
well-known subject so they participate in bitemporal versioning and persist
across restarts with zero extra infrastructure.

```
subject:   SCHEMA_REGISTRY_NODE (fixed, deterministic UUID)

# Node type
predicate: "__schema__/<TypeName>"
value:     Value::Text(serde_json::to_string(&{fields: Vec<FieldDef>, vector_space: Option<VectorSpaceDef>}))

# Edge type
predicate: "__edge_schema__/<PredicateName>"
value:     Value::Text(serde_json::to_string(&{domain, range, fields}))
```

`NodeTypeRegistry::new(store)` and `EdgeTypeRegistry::new(store)` each scan
by subject at open time and load their schemas into an in-memory
`RwLock<HashMap<String, _>>`. `register_type`/`register_edge_type` commit
to the store and update the cache atomically.

### Node type validation

`validate_properties(type_name, &HashMap<String, Value>)` checks:

1. All fields declared `required: true` must be present.
2. Present fields must carry the declared `FieldKind`.
3. Unknown fields are silently accepted — open-world.

### Edge type validation

`validate_edge(predicate, subject_type, object_type, &HashMap<String, Value>)` checks:

1. If `domain` is set: `subject_type` must match it (or be `None` → error).
2. If `range` is set: `object_type` must match it (or be `None` → error).
3. Required edge properties present and of the correct kind.
4. If no schema is registered for the predicate, the edge is considered valid.

### `list_predicates_between`

Returns all registered predicate names whose `domain` and `range` both match
the supplied node type names. A `None` domain/range slot is unconstrained and
matches any type argument.

---

## Backup and restore

PolarGraph uses RocksDB's built-in `BackupEngine` for point-in-time backups.

### Configuration

Pass `--backup-dir <PATH>` (or set `POLARGRAPH_BACKUP_DIR`) when starting
the server. The directory is created if it does not exist. Without this flag
the `CreateBackup`, `ListBackups`, and `PurgeOldBackups` RPCs return
`FAILED_PRECONDITION`.

### How incremental backups work

RocksDB's `BackupEngine` hard-links unchanged SST files between successive
backups. Only SST files that were compacted or newly written since the previous
backup are actually copied. This means:

- The first backup copies everything.
- Subsequent backups copy only the delta, typically a small fraction of the
  total data size.
- The sum of all backup sizes displayed by `ListBackups` is larger than the
  actual disk usage because sizes are reported per-backup without accounting
  for shared files.

### Creating and managing backups

```bash
# Create a backup (incremental)
grpcurl -plaintext -d '{}' localhost:50051 polargraph.v1.PolarGraphService/CreateBackup

# List all backups
grpcurl -plaintext -d '{}' localhost:50051 polargraph.v1.PolarGraphService/ListBackups

# Keep only the 5 most recent backups
grpcurl -plaintext -d '{"keep_n": 5}' localhost:50051 polargraph.v1.PolarGraphService/PurgeOldBackups
```

### Restore runbook (offline operation)

Restore **cannot** be performed while the server is running. The server must
be stopped first.

```bash
# 1. Stop the server (send SIGTERM or Ctrl-C).

# 2. Find the backup ID to restore from:
grpcurl -plaintext -d '{}' localhost:50051 polargraph.v1.PolarGraphService/ListBackups
# (or inspect the backup directory directly)

# 3. Run a one-shot restore process (Rust snippet / future polargraphd subcommand):
#    BackupManager::open(backup_dir, &store)?
#        .restore_from_backup(backup_id, restore_dir)?;
#    (This is not yet exposed as a CLI command; use a small Rust program or
#     the polargraph-storage library directly.)

# 4. Restart the server pointing at the restored directory:
polargraphd --data-dir /path/to/restore_dir --backup-dir /path/to/backup_dir
```

The `restore_from_backup` function in `polargraph-storage::backup::BackupManager`
copies all SST files for the chosen backup ID into `restore_dir` and writes the
RocksDB `CURRENT` file so the directory is immediately openable. The WAL
directory is the same as the data directory (RocksDB default).

---

## Bulk import

For large initial data loads, `polargraph-import` ingests N-Triples files
directly into RocksDB via SST file ingestion — completely bypassing gRPC,
the WAL write path, and per-insert MVCC overhead. Expected throughput is
10–100× faster than streaming inserts over gRPC.

### Use case

Use `polargraph-import` when loading millions of triples into a fresh or
offline database (knowledge graph seed data, migration from another store,
test fixture load). It is **not** suitable for incremental updates to a live
database — use the `Insert` RPC for that.

### How to run

```bash
# Stop polargraphd first — SST ingestion requires exclusive DB access.

polargraph-import \
  --data-dir /var/lib/polargraph \
  --input    ./dump.nt \
  --batch-size 100000

# Example output:
# Imported 100000 triples (batch 1) in 312ms
# Imported 100000 triples (batch 2) in 298ms
# Total: 200000 triples in 612ms (326797 triples/sec)

# Restart the server — all imported triples are immediately visible.
polargraphd --data-dir /var/lib/polargraph
```

### N-Triples support

`polargraph-import` handles the common N-Triples subset:

| Input form | Storage result |
|---|---|
| `<uri> <uri> <uri> .` | `Triple::Relation` — subject/object URIs hashed to stable `NodeId`s |
| `<uri> <uri> "literal" .` | `Triple::Property` — `Value::Text` |
| `<uri> <uri> "literal"@lang .` | `Triple::Property` — language tag stripped |
| Lines starting with `#` | Skipped (comments) |
| Blank lines | Skipped |
| `_:blank_node` objects | Skipped (not supported) |

URIs are hashed to `NodeId` using xxHash3-128 — the same URI always
produces the same `NodeId` across runs.

### Why offline-only

RocksDB SST file ingestion acquires exclusive locks on the column families
being written. Running `polargraph-import` against a database that is also
being served by `polargraphd` will cause RocksDB errors or data corruption.
The gRPC `Insert` RPC is the correct path for concurrent writes to a live server.

### Implementation

`polargraph-storage::SstImporter` (in `sst_import.rs`):

1. Buffers triples in memory.
2. On `finish(&store)`: interns all predicates, acquires a commit timestamp
   via `begin_commit()`, encodes keys for all 6 hexastore CFs, sorts per CF
   (RocksDB SST requires sorted order), writes one `.sst` file per CF via
   `SstFileWriter`, calls `db.ingest_external_file_cf()` for each CF, then
   persists the updated oracle counter to the META CF.
3. Returns `ImportStats { triples_imported, duration_ms }`.

`polargraph-import/src/main.rs` drives `SstImporter` in batches (default
100 000 triples/batch) and prints per-batch and summary progress lines.

---

## Compaction and retention

PolarGraph stores every write as an immutable versioned entry. Without
pruning, storage grows unboundedly. `CompactionManager` (in
`polargraph-storage::compaction`) scans the six hexastore column families
and deletes entries that have expired under a `RetentionPolicy`, then triggers
a full RocksDB compaction on any CF that received deletions.

### What timestamps are checked

Every hexastore key ends with 8 bytes of `tt` (transaction time, microseconds
since Unix epoch, wall-clock). The codec value bytes begin with the
discriminant and carry `vt_start` and `vt_end` (also microseconds since epoch).

`RetentionPolicy` has two independent knobs:

| Field | Effect |
|-------|--------|
| `tx_age_secs` | Delete any triple whose `tt` is more than `tx_age_secs` seconds before now. This is the primary retention knob — it bounds how much transaction history is kept. |
| `vt_lookback_secs` (optional) | Also delete triples whose `vt_end` (the end of their valid-time window) is more than `vt_lookback_secs` seconds in the past. Useful for purging facts whose real-world validity has ended. Disabled when `None`. |

Either condition is sufficient for deletion — a triple matching either is
removed from all six CFs atomically via a single `WriteBatch`.

### Transaction time and the oracle

The MVCC oracle uses `max(committed + 1, wall_clock_µs)` as the commit
timestamp. This means `tt` values are real wall-clock times, so comparing
them against `now - tx_age_secs * 1_000_000` is meaningful. When an existing
store is opened, the oracle loads the last committed `tt` from the META CF
and then immediately aligns with wall-clock time on the next commit.

### How to trigger retention

**At startup** — set `--retention-tx-age-secs N` (and optionally
`--retention-vt-lookback-secs M`). Retention runs once before the server
starts accepting connections:

```bash
polargraphd \
  --data-dir /var/lib/polargraph \
  --retention-tx-age-secs 2592000 \   # 30 days
  --retention-vt-lookback-secs 86400    # 1 day
```

**Via gRPC** — the `RunRetention` RPC runs retention without a restart:

```
RunRetentionRequest {
    tx_age_secs: 2592000,
    vt_lookback_secs: 86400  // 0 = disabled
}
```

Returns `RetentionStats { triples_scanned, triples_deleted, duration_ms }`.

### Performance implications

Retention does a **full scan** of all six hexastore CFs. Each row is checked
against the policy; matching rows are batched into a `WriteBatch` and deleted.
After deletion, `db.compact_range_cf` is called on each modified CF so that
RocksDB reclaims the on-disk space promptly (rather than waiting for the next
scheduled compaction). On a large store this can take tens of seconds.

META and HNSW column families are never touched.

---

## Time-travel queries

PolarGraph's bitemporal model stores two independent time axes on every triple:

- **Transaction time (`tt`)** — when the triple was committed to the database.
  Stored in the RocksDB key (last 8 bytes). Values are wall-clock microseconds
  (`max(committed+1, now_µs)`), so they are comparable to real timestamps.
- **Valid time (`vt_start` / `vt_end`)** — when the fact was true in the
  real world, as asserted by the writer. Stored in the value bytes.
  `vt_end = i64::MAX` (or proto `0`, which `convert.rs` maps to `END_OF_TIME`)
  means the fact is open-ended.

Both axes are independently filterable on the `Query` RPC.

### Transaction-time filter (`as_of_tx_time`)

```proto
QueryRequest {
    patterns: [...],
    as_of_tx_time: 1_718_000_000_000_000   // unix µs
}
```

Only triples whose `tt ≤ as_of_tx_time` are visible. This is equivalent to
"what did the database look like at this wall-clock instant?" It is implemented
by passing `as_of_tx_time` as the snapshot timestamp instead of `snapshot_ts`.

Typical use cases:
- **Audit**: replay what the system knew at a specific past moment.
- **Debugging**: isolate the state before a bad write.

### Valid-time filter (`as_of_valid_time`)

```proto
QueryRequest {
    patterns: [...],
    as_of_valid_time: 1_700_000_000_000_000   // unix µs
}
```

Only triples whose valid-time window `[vt_start, vt_end)` contains
`as_of_valid_time` are returned. Triples outside the window are skipped even
if they are the latest version in the MVCC sense.

Typical use cases:
- **Historical state**: "What was Alice's role on January 1st?" — query with
  `as_of_valid_time` set to the start of that day.
- **Versioned facts**: store multiple non-overlapping versions of the same
  (S, P) pair with different vt windows and query each independently.

### Filter ordering and correctness

The valid-time filter runs **inside** `snapshot_scan_cf`, before the MVCC
deduplication step that picks the highest-`tt` entry for each `(S, P, O)`.

This ordering is necessary for correctness. Consider two versions of the same
fact:

```
version 1: tt=T1, vt=[100, 200)
version 2: tt=T2, vt=[200, MAX)     (T2 > T1)
```

A query with `as_of_valid_time=150` should return version 1. If filtering
happened _after_ deduplication, version 2 (the higher-`tt` entry) would
be selected first, then rejected, leaving no result — incorrect. By filtering
before deduplication each eligible version participates in the latest-version
selection independently.

### Combining both filters

Both filters can be set simultaneously. A triple must satisfy **both** to
appear in the results:

```proto
QueryRequest {
    patterns: [...],
    as_of_tx_time:    1_718_000_000_000_000,   // how the DB looked at this instant
    as_of_valid_time: 1_700_000_000_000_000,   // what was "currently valid" then
}
```

This is a full bitemporal point query — a precise snapshot along both axes.

### Implementation

- `Snapshot.vt_as_of: Option<i64>` — set by `Snapshot::with_vt_as_of(vt)`.
- All `scan_*` methods on `Snapshot` forward `vt_as_of` to the underlying
  `scan_*_at` methods on `TripleStore`.
- `snapshot_scan_cf` in `store.rs` applies the valid-time filter inline before
  updating the `latest` deduplication map.
- The `datalog`, `eval`, and query planner layers need no changes — they call
  `Snapshot` scan methods and inherit the filter transparently.

---

## Read replicas

PolarGraph supports horizontal read scale-out via RocksDB secondary instances. A secondary instance opens the primary's data directory in read-only mode and periodically ingests new SST files without copying them.

### Opening a secondary

`TripleStore::open_secondary(data_dir, primary_path)` calls
`DB::open_cf_descriptors_as_secondary` with all 8 column families (SPO, SOP,
PSO, POS, OSP, OPS, META, HNSW). `data_dir` is a small local directory for
RocksDB secondary metadata; `primary_path` is the primary's data directory
(must be readable from the replica host).

### Catch-up

`TripleStore::try_catch_up_with_primary()` calls RocksDB's
`try_catch_up_with_primary`, which re-reads the primary's MANIFEST and
hard-links any new SST files into the secondary's view. PolarGraph then
refreshes its in-memory predicate maps and advances the MVCC oracle to the
latest transaction timestamp so that reads at `snapshot_ts=0` see the
newest data.

The `polargraphd` server spawns a background tokio task that calls this on the
interval set by `--replica-catchup-interval-ms` (default 1 s).

### Consistency model

The secondary provides **eventual consistency**: reads may lag the primary by
up to one catch-up interval. There is no read-your-own-writes guarantee.
Transaction timestamps on the replica are always ≤ the primary's committed
timestamp at the last catch-up.

### Write blocking

All storage-layer write operations (`db_write`, `intern_predicate`,
`insert_vector`, `batch_insert_vectors`, `insert_at_ts`, `compact_cf`,
`scan_cf_raw`) return `StorageError::ReadOnly` when called on a secondary. The
gRPC service layer adds a second guard (`check_not_replica`) that returns
`FAILED_PRECONDITION` before the request reaches the storage layer, giving
clients a clear error message.

### ReplicaStatus RPC

The `ReplicaStatus` RPC returns:
- `is_replica` — whether this instance is a secondary.
- `primary_path` — the primary's data directory.
- `last_catchup_at` — Unix microseconds of the most recent successful catch-up.
- `catchup_count` — total successful catch-ups since server start.

---

## Planned extensions

| Feature | Notes |
|---------|-------|
| Replication | Log-structured writes, follower replay; oracle becomes distributed (Hybrid Logical Clocks) |
