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

PolarGraph opens twelve RocksDB column families:

| CF | Purpose |
|----|---------|
| `spo` | Subject → Predicate → Object index |
| `sop` | Subject → Object → Predicate index |
| `pso` | Predicate → Subject → Object index |
| `pos` | Predicate → Object → Subject index |
| `osp` | Object → Subject → Predicate index |
| `ops` | Object → Predicate → Subject index |
| `meta` | Predicate intern table, timestamp oracle counter, migration version |
| `hnsw` | HNSW vector index nodes and entry-point records (per named space) |
| `tri` | Trigram full-text index (key: `[trigram:3][pred_id:4][subject_id:16]`) |
| `drv` | OWL 2 RL derived (materialized) facts — same SPO key layout, separate CF |
| `epa` | Edge property annotations (key: `[edge_id:16][pred_id:4][tt:8]`) |
| `epo` | Edge relation annotations (key: `[edge_id:16][pred_id:4][obj_id:16][tt:8]`) |

The six `spo`/`sop`/`pso`/`pos`/`osp`/`ops` CFs implement the **hexastore** pattern. Every insert
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

## HNSW ef tuning

The **exploration factor** (`ef`) controls the size of the candidate list
maintained during ANN graph traversal at query time. A larger `ef` explores
more of the graph before picking the final top-k, trading latency for recall.

### Quality / speed tradeoff

| ef | Character |
|----|-----------|
| 20 | Fastest; a few percent recall loss vs. brute force |
| 50 | Safe default; good recall on most workloads |
| 100+ | High-recall mode; noticeably slower on large indexes |

**Rule of thumb:** `ef ≥ k`. Below `k` the search may not even fill the
result set. The built-in fallback in `search_vector` uses
`ef = max(ef_construction / 2, k)` when no caller `ef` is supplied.

**Benchmark data point:** at 100K nodes / 128 dims / k=10, ef=50 runs ~583 µs
p50; ef=20 roughly halves that at the cost of a few percent recall.

### Three-level resolution hierarchy

When a search request arrives, `ef` is resolved in priority order:

1. **Cypher inline** — `VECTOR_NEAR(a, "space", 10, ef=100)` overrides
   everything for that specific predicate in the query.
2. **Per-request proto field** — the `ef` field on `SearchVectorRequest`,
   `VectorSeedQueryRequest`, and `CypherQueryRequest`. A value of `0` means
   "use the server default".
3. **Server default** — `--default-vector-ef` CLI flag /
   `POLARGRAPH_DEFAULT_VECTOR_EF` env var / `[query] default_vector_ef` in
   the TOML config file. Built-in default: **50**.

```toml
[query]
timeout_ms        = 30000
slow_query_ms     = 1000
default_vector_ef = 50    # override here to tune globally
```

This hierarchy lets you set a conservative global default while allowing
latency-sensitive callers to drop ef and recall-critical callers to raise it,
without server restarts.

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

## Cypher query surface

PolarGraph supports a subset of Cypher as a higher-level query language that
compiles down to the Datalog IR at the server. This lets clients write
readable graph queries without constructing `VarPattern` lists by hand.

### Supported syntax

| Construct | Example |
|-----------|---------|
| Node label match | `MATCH (a:Person)` |
| Relationship traversal | `MATCH (a)-[:knows]->(b)` |
| Property filter | `WHERE a.name = "Alice"` |
| Comparison filter | `WHERE a.age > 18` |
| Text predicates | `WHERE a.name CONTAINS "Ali"` / `STARTS WITH` / `=~` (regex) |
| Transitive closure | `MATCH (a)-[:knows*]->(b)` |
| Bounded path | `MATCH (a)-[:knows*1..3]->(b)` |
| Vector predicate | `WHERE VECTOR_NEAR(a, "space", k)` or `WHERE VECTOR_NEAR(a, "space", k, ef=100)` |
| Result projection | `RETURN a, b` |
| Aggregations | `RETURN a, COUNT(*) AS cnt ORDER BY cnt DESC` |
| WITH clause | `WITH a ORDER BY a.name MATCH (a)-[:knows]->(b) RETURN b` |
| Write operations | `CREATE`, `MERGE`, `SET`, `DELETE` (via `CypherWrite` RPC) |
| Named parameters | `WHERE a.name = $name` |
| Row limit / skip | `LIMIT 20`, `SKIP 5` |

Unsupported Cypher features (multiple `MATCH` clauses in a single statement,
`OPTIONAL MATCH`) are rejected with `INVALID_ARGUMENT`.

### How it compiles to Datalog IR

The Cypher parser (`polargraph_query::cypher`) translates each clause to
equivalent Datalog structures before execution:

- **Node patterns** like `(a:Person)` become a `VarPattern` binding the type
  predicate: `[?a, :__type, "Person"]`.
- **Relationship patterns** like `(a)-[:knows]->(b)` become
  `[?a, :knows, ?b]`.
- **`[:pred*]` transitive closure** generates a recursive `Rule` with
  head predicate `pred_reach` and the two-hop body
  `[?x, :pred, ?y], [?y, pred_reach, ?z]`, then queries the derived facts.
- **`WHERE` value filters** on node properties are pushed down as additional
  `VarPattern`s (`[?a, :name, "Alice"]` for equality tests).
- **`VECTOR_NEAR`** routes through `VectorSeedQuery`: the predicate becomes the
  seed variable, the rest of the `MATCH` body forms the graph patterns, and
  `LIMIT` caps the returned rows.

### `CypherQuery` RPC and `POST /cypher`

**gRPC:**
```proto
rpc CypherQuery(CypherQueryRequest) returns (CypherQueryResponse);

message CypherQueryRequest {
    string         cypher       = 1;
    repeated float vector       = 2;  // required when VECTOR_NEAR is present
    uint32         ef           = 3;  // 0 = use server default
    int64          snapshot_ts  = 4;
}
message CypherQueryResponse {
    repeated ScoredBinding results = 1;
}
```

**REST:**
```
POST /cypher
Content-Type: application/json
```

Request body:
```json
{
  "cypher":  "MATCH (a:Person)-[:knows]->(b) WHERE a.name = \"Alice\" RETURN b",
  "vector":  [],        // required only when VECTOR_NEAR is used
  "ef":      0,         // 0 = server default
  "snapshot_ts": 0
}
```

### Example queries

```cypher
-- Simple label match
MATCH (a:Person) RETURN a LIMIT 10
```

```cypher
-- Relationship traversal with property filter
MATCH (a:Person)-[:knows]->(b:Person)
WHERE a.name = "Alice"
RETURN b
```

```cypher
-- Transitive closure (arbitrary depth)
MATCH (a:Person)-[:knows*]->(b:Person)
RETURN b
```

```cypher
-- Vector + graph (unified ANN seed query)
-- vector is passed in the request body alongside the cypher string
MATCH (a:Document)-[:cites]->(b:Document)
WHERE VECTOR_NEAR(a, "doc_embeddings", 10)
RETURN b LIMIT 20
```

```cypher
-- Explicit ef override for high-recall search
MATCH (a:Document)-[:cites]->(b:Document)
WHERE VECTOR_NEAR(a, "doc_embeddings", 10, ef=100)
RETURN b LIMIT 20
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
| `--query-timeout-ms MS` | `POLARGRAPH_QUERY_TIMEOUT_MS` | `30000` | Max query execution time in ms; 0 = unlimited |

The server handles graceful shutdown on SIGTERM and Ctrl-C.

### Query timeouts

The `--query-timeout-ms` flag (default 30 000 ms / 30 s) limits how long a
single `Query`, `VectorSeedQuery`, or `Reachable` RPC may run. When the
deadline is exceeded the RPC returns `DEADLINE_EXCEEDED` with a message of the
form `"query exceeded timeout of Xms"`.

Set the value to `0` to disable the timeout entirely. This is not recommended
in production because badly written recursive rules on dense graphs can
produce fixed-point evaluation that runs indefinitely.

The deadline propagates through the full call stack — conjunctive joins,
hybrid derived-fact evaluation, and the fixed-point loop in `execute_recursive`
all check it at iteration boundaries.

---

### Slow query logging

When a query RPC (`Query`, `VectorSeedQuery`, `Reachable`) takes longer than
the configured threshold, PolarGraph emits a `WARN`-level log entry with
structured fields and increments a Prometheus counter.

| Flag | Env variable | Default | Description |
|------|-------------|---------|-------------|
| `--slow-query-ms MS` | `POLARGRAPH_SLOW_QUERY_MS` | `1000` | Threshold in ms; 0 = disabled |

Log fields emitted on a slow query:

```
method       = "Query" | "VectorSeedQuery" | "Reachable"
duration_ms  = <actual elapsed time>
threshold_ms = <configured slow_query_ms>
extra        = "patterns=N" | "predicate=P max_hops=H" | "space=S k=K patterns=N"
message      = "slow query detected"
```

The Prometheus counter `polargraph_slow_queries_total{method}` increments on
each slow query, making it easy to alert on sustained slow-query rates.

---

### Query planner (EXPLAIN)

The `ExplainQuery` RPC accepts a `QueryRequest` and returns an `ExplainResponse`
without touching storage — it performs pure static analysis of the execution plan.

```protobuf
rpc ExplainQuery(QueryRequest) returns (ExplainResponse);

message ExplainResponse {
    string plan_text = 1;        // human-readable multi-line plan
    repeated PlanNode nodes = 2; // structured plan nodes
}

message PlanNode {
    string node_type   = 1; // "PatternScan"
    string description = 2; // e.g. "[?s, :knows, ?o]"
    string index_used  = 3; // e.g. "POS  (predicate bound)"
    repeated PlanNode children = 4;
}
```

**Index selection** follows the hexastore table: after each pattern is
evaluated, the variables it binds become "bound" for subsequent patterns. The
planner simulates this symbolically — a `Var` slot is treated as bound if the
variable was introduced by an earlier pattern.

Example `plan_text` output:

```
Query Plan
──────────
Step 1: PatternScan  [?s, :knows, ?o]
  Index: PSO  (predicate bound)
  Binds: ?s, ?o

Step 2: PatternScan  [?s, :name, ?n]
  Index: SPO  (subject bound)
  Binds: ?n

Recursive rules: none
Estimated steps: 2
```

The `polargraph_query::explain::explain_query` function in `polargraph-query`
drives this analysis; it requires no storage access and runs in microseconds.

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

## Authentication

PolarGraph uses transport-level API key authentication implemented as a tower
middleware layer (`polargraph_server::auth::ApiKeyLayer`). This is not
per-user RBAC — all callers with a valid key have identical access.

### Configuration

| Flag | Env variable | Description |
|------|--------------|-------------|
| `--api-key KEY` | `POLARGRAPH_API_KEY` | Required key; repeatable |
| `--no-auth` | — | Suppress the no-key startup warning |

`--api-key` can be specified multiple times. The env var accepts
comma-separated keys: `POLARGRAPH_API_KEY=key1,key2`.

When the server starts without any key configured, a warning is logged and
all requests are accepted. Pass `--no-auth` to suppress the warning.

### Request format

Clients must include one of these headers on every call:

```
Authorization: Bearer <key>
Authorization: ApiKey <key>
```

Requests without a matching key receive `UNAUTHENTICATED` (gRPC status 16).

### Key rotation without downtime

Configure two keys (old + new), deploy, then remove the old key and redeploy:

```bash
# Step 1 — add new key alongside existing one
POLARGRAPH_API_KEY=old-key,new-key polargraphd ...

# Step 2 — migrate clients to new-key

# Step 3 — remove old key
POLARGRAPH_API_KEY=new-key polargraphd ...
```

No rolling restart is needed; both keys are valid simultaneously during
the migration window.

### Exempt RPCs

`ReplicaStatus` bypasses authentication so load-balancer health probes work
without possessing a key. All other RPCs require auth when enabled.

### Implementation

`ApiKeyLayer` is a `tower::Layer` applied at the server transport level via
`Server::builder().layer(auth_layer)`. It intercepts `http::Request<B>` before
tonic routing, checks the `Authorization` header using `subtle::ConstantTimeEq`
for timing-attack resistance, and returns a `grpc-status: 16` response for
invalid credentials. Path-based exemptions are matched against `req.uri().path()`.

---

## Observability

PolarGraph ships structured logging, per-RPC tracing, and a Prometheus metrics
endpoint out of the box.

### Logging

| Flag | Env var | Default | Values |
|------|---------|---------|--------|
| `--log-level` | `RUST_LOG` | `info` | Any `tracing` filter (`info`, `debug`, `warn,polargraph_storage=trace`, …) |
| `--log-format` | `LOG_FORMAT` | `pretty` | `pretty` (human-readable) or `json` (newline-delimited JSON) |

Use `json` in production / containers so log aggregators (Loki, Datadog, etc.)
can parse fields directly. The `pretty` format uses colour and is intended for
local development.

Key structured fields emitted at startup:

```
level=INFO  listen=0.0.0.0:50051  data_dir=/data  replica_mode=false
            metrics_enabled=true  log_format=json
            message="polargraphd starting"
```

### Per-RPC tracing

`TelemetryLayer` (a `tower::Layer` in `polargraph-server::telemetry`) wraps every
gRPC handler. Each request opens an `info_span!` that carries `method` and `peer`
fields, so all log events emitted by the handler inherit those fields automatically.

On completion, the layer logs:

```
INFO  rpc{method="Insert" peer="127.0.0.1:51234"}: completed status=ok duration_ms=2
WARN  rpc{method="Insert" peer="127.0.0.1:51234"}: completed status=failed_precondition duration_ms=1
ERROR rpc{method="Query"  peer="127.0.0.1:51234"}: transport error duration_ms=0
```

For error responses, the `status` field is the gRPC status code name
(`invalid_argument`, `failed_precondition`, `aborted`, etc.). For successful
streaming responses where the status arrives in HTTP/2 trailers, `status=ok`
is assumed.

### Prometheus metrics

The server exposes a Prometheus scrape endpoint on a separate HTTP port (default
9090). Use `--no-metrics` to disable it.

| Flag | Env var | Default |
|------|---------|---------|
| `--metrics-port` | `POLARGRAPH_METRICS_PORT` | `9090` |
| `--no-metrics` | — | false |

Scrape the endpoint:

```bash
curl http://localhost:9090/metrics
```

#### Available metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `polargraph_rpc_requests_total` | counter | `method`, `status` | Total RPC calls by method and gRPC status |
| `polargraph_rpc_duration_seconds` | histogram | `method` | RPC latency distribution |
| `polargraph_triples_total` | gauge | — | Running count of inserted triples (incremented on each `Insert` batch; not absolute at startup) |
| `polargraph_vector_spaces_total` | gauge | — | Number of named HNSW vector spaces |
| `polargraph_wal_applied_seq` | gauge | — | Last WAL sequence number applied (replica only) |
| `polargraph_wal_lag_entries` | gauge | — | Entries behind the primary (replica only; sampled on `ReplicaStatus` poll) |
| `polargraph_backup_last_size_bytes` | gauge | — | Size of the most recently created backup |
| `polargraph_compaction_deleted_total` | counter | — | Total triples deleted by retention runs |

#### Sample Prometheus scrape config

```yaml
scrape_configs:
  - job_name: polargraph
    static_configs:
      - targets: ["polargraphd-host:9090"]
    scrape_interval: 15s
```

#### Sample Grafana alert (high WAL lag)

```yaml
- alert: PolarGraphReplicaLag
  expr: polargraph_wal_lag_entries > 10000
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "PolarGraph replica is lagging behind primary"
```

---

## Management UI

PolarGraph ships a browser-based management interface served from the same
process as the gRPC server. It is a single-page app embedded directly in the
binary (`include_str!("ui.html")`) — no build step, no separate process, no
external assets.

### Server configuration

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--ui-port PORT` | `POLARGRAPH_UI_PORT` | `8080` | HTTP port for the web UI |
| `--no-ui` | — | false | Disable the UI entirely |

The UI server binds independently of the gRPC port (default 50051) and the
Prometheus metrics port (default 9090).

### REST endpoints

All endpoints live under `/api/`. The root `GET /` always serves the HTML
regardless of auth state so the UI can load and prompt for a key.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Single-page app HTML |
| `GET` | `/api/status` | Server info: version, uptime, data_dir, is_replica, auth_enabled |
| `GET` | `/api/node-types` | All registered node types with field definitions |
| `GET` | `/api/edge-types` | All registered edge types |
| `GET` | `/api/metrics` | Key metrics snapshot (vector spaces, WAL seq, etc.) |
| `POST` | `/api/query` | Datalog query — body: `{"patterns":[{"s":"?x","p":"__type","o":"Person"}]}` |
| `POST` | `/api/insert` | Insert a triple — UUID object → Relation, text → Property |
| `GET` | `/api/search` | Triple scan — params: `q=`, `type=`, `limit=` |

Query patterns use `?varname` for variables, empty string for any, and UUID
strings or plain text for bound values. `as_of_valid_time` and
`as_of_tx_time` fields support time-travel queries.

### Authentication

When API keys are configured (`--api-key` / `POLARGRAPH_API_KEY`), every
`/api/*` route requires `Authorization: Bearer <key>`. The UI stores the
key in `localStorage` and prompts on 401. `GET /` bypasses auth so the UI
always loads.

### Architecture

The UI server (`ui_api.rs`) holds an `Arc<UiState>` containing a clone of
the `PolarGraphServer` handle. Handlers call into the gRPC service trait
directly in-process — no network hop. The REST → gRPC adaptor is thin:
JSON request bodies are decoded and forwarded as tonic `Request<T>` values,
and tonic `Response<T>` values are serialised back to JSON.

---

## Graceful shutdown

`polargraphd` handles SIGTERM and SIGINT (Ctrl-C) without dropping in-flight
requests.

### How it works

All concurrent components — the gRPC server, the WAL replication client, and
the metrics/UI HTTP servers — share a single `tokio_util::sync::CancellationToken`.
When a shutdown signal is received:

1. The signal is logged (`received shutdown signal signal=SIGTERM`).
2. The `CancellationToken` is cancelled.
3. tonic's `serve_with_shutdown` stops accepting new connections and waits
   for all in-flight RPCs to complete before returning.
4. The WAL replication loop exits cleanly at its next `tokio::select!` point
   (between stream messages or backoff sleeps).
5. The metrics and UI HTTP servers drain their in-flight HTTP requests via
   axum's `with_graceful_shutdown`.
6. `main` logs each stopped component in sequence, then returns, dropping
   the `Arc<TripleStore>` which triggers RocksDB WAL flush and close.

### Shutdown sequence log

```
INFO  polargraphd: received shutdown signal signal=SIGTERM
INFO  polargraphd: draining in-flight requests max_wait_secs=10
INFO  polargraphd: gRPC server stopped
INFO  polargraphd: WAL replication stopped      # replica mode only
INFO  polargraphd: HTTP servers stopped
INFO  polargraphd: RocksDB closed
INFO  polargraphd: shutdown complete
```

### Timeout and force-exit

| Flag | Env variable | Default | Description |
|------|-------------|---------|-------------|
| `--shutdown-timeout-ms MS` | `POLARGRAPH_SHUTDOWN_TIMEOUT_MS` | `10000` | Max milliseconds to drain before force-exit |

A watchdog task starts as soon as the token is cancelled.  If any component
is still running when the timeout elapses the process calls
`std::process::exit(1)` and logs an error.  This prevents a stuck RPC from
blocking container orchestrators indefinitely.

### RocksDB WAL flushing

RocksDB's own write-ahead log is separate from PolarGraph's WAL replication
stream.  When the `Arc<TripleStore>` drops at the end of `main`, RocksDB
flushes its memtable and syncs the WAL to disk automatically — no explicit
flush call is needed.  The `INFO: RocksDB closed` log line appears after
this drop, so data committed before the shutdown signal is guaranteed to be
durable on disk.

---

## TLS

PolarGraph supports mutual-TLS-free server-side TLS on all three network surfaces (gRPC, management UI HTTP, Prometheus metrics HTTP).

### Enabling TLS

Supply a PEM certificate and private key via CLI flags or environment variables:

| Flag | Env variable | Description |
|------|-------------|-------------|
| `--tls-cert PATH` | `POLARGRAPH_TLS_CERT` | Path to PEM certificate (chain) file |
| `--tls-key PATH` | `POLARGRAPH_TLS_KEY` | Path to PEM private key file |

Both flags must be supplied together; omitting either keeps the server in plaintext mode.

```bash
# TLS-enabled primary
polargraphd \
  --tls-cert /etc/pg/server.crt \
  --tls-key  /etc/pg/server.key
```

### gRPC TLS

TLS on the gRPC server is provided by tonic's built-in `ServerTlsConfig` (rustls 0.22 backend).  Clients must connect using `https://` URLs.

### HTTP TLS (UI + metrics)

The management UI and Prometheus metrics HTTP servers use the same cert/key pair via a `tokio-rustls` TLS acceptor wrapped in hyper's `accept::from_stream` bridge.  Both servers honour graceful shutdown regardless of TLS mode.

### Replica TLS

When a replica connects to a TLS-enabled primary, supply the CA certificate used to sign the primary's cert:

| Flag | Env variable | Description |
|------|-------------|-------------|
| `--replica-tls-ca PATH` | `POLARGRAPH_REPLICA_TLS_CA` | CA certificate for verifying the primary |

```bash
polargraphd --replica-of https://primary:50051 \
            --replica-tls-ca /etc/pg/ca.crt
```

When not set the WAL client connects without TLS (existing behaviour).

---

## Health checks

### gRPC health service (`grpc.health.v1.Health`)

`polargraphd` registers the standard gRPC health service so load balancers and Kubernetes probes can use it.

```
grpc.health.v1.Health/Check
  request:  { service: "polargraph.v1.PolarGraphService" }
  response: { status: SERVING }  # primary always; replica when WAL stream active
```

Implementation: `tonic_health::server::health_reporter()` creates a `HealthReporter` + `HealthServer`.  The `HealthServer` is added to the tonic `Server::builder()` chain alongside the `PolarGraphServiceServer`.

For replicas, the WAL replication loop calls:
- `reporter.set_service_status("polargraph.v1.PolarGraphService", NotServing)` — while reconnecting.
- `reporter.set_service_status("polargraph.v1.PolarGraphService", Serving)` — once the stream is established.

### HTTP `/health` endpoint

The management UI server (`--ui-port`, default 8080) exposes a lightweight HTTP health endpoint:

```
GET /health
```

**200 OK** — server is healthy:
```json
{ "status": "ok", "mode": "primary", "triples": 12345 }
```

**503 Service Unavailable** — replica is not connected to its primary:
```json
{ "status": "degraded" }
```

`mode` is `"primary"` or `"replica"`.  `triples` is a fast approximate count from RocksDB's `estimate-num-keys` property on the SPO column family (not exact; use for monitoring only).

The `/health` endpoint requires no authentication and is always available regardless of `--api-key` configuration.

---

## Configuration file

In addition to CLI flags and environment variables, `polargraphd` can read
settings from a TOML file.  The priority order is:

```
CLI flag  >  environment variable  >  config file  >  built-in default
```

### Auto-detection

When `--config` is not supplied, the server searches in order:

1. `./polargraph.toml` (current working directory)
2. `~/.config/polargraph/config.toml`

If neither file exists the server starts normally using environment variables
and built-in defaults.

### Explicit path

```bash
polargraphd --config /etc/polargraph/polargraph.toml
# or
POLARGRAPH_CONFIG=/etc/polargraph/polargraph.toml polargraphd
```

An explicit path that does not exist or cannot be parsed is a fatal error.

### File format

```toml
[server]
data_dir            = "/var/lib/polargraph"
grpc_port           = 50051    # port only; interface is always 0.0.0.0
ui_port             = 8080
metrics_port        = 9090
shutdown_timeout_ms = 10000

[storage]
backup_dir               = "/var/lib/polargraph/backups"
retention_tx_age_secs    = 2592000   # 30 days
retention_vt_lookback_secs = 604800  # 7 days

[replication]
replica_of = "http://primary.example.com:50051"
tls_ca     = "/etc/polargraph/ca.crt"

[tls]
cert = "/etc/polargraph/server.crt"
key  = "/etc/polargraph/server.key"

[auth]
api_keys = ["key-one", "key-two"]
no_auth  = false

[observability]
log_level  = "info"
log_format = "pretty"   # or "json"
no_metrics = false
no_ui      = false

[query]
timeout_ms    = 30000
slow_query_ms = 1000
```

Every key is optional.  Missing sections and missing keys within a section
fall through to the next priority level (environment variable → built-in
default).

A fully-commented example file covering every option is provided at
`polargraph.example.toml` in the repository root.

### Override semantics for boolean flags

The boolean flags `no_metrics`, `no_auth`, and `no_ui` follow **OR** semantics:
setting the flag in the config file to `true` disables the feature; passing
`--no-metrics` on the CLI also disables it.  There is no way to re-enable a
feature that the config file has disabled without editing the config file,
because the CLI flags can only assert `true`.

---

## Rate limiting

`polargraphd` supports per-client-IP token-bucket rate limiting applied as a
tower middleware layer (`RateLimitLayer`) immediately before authentication in
the gRPC server pipeline.

### Token-bucket algorithm

Each unique client IP gets an independent bucket with capacity equal to
`max_rps` tokens.  On every request:

1. Elapsed seconds since the last request are computed and multiplied by
   `max_rps` to calculate newly earned tokens.
2. The bucket is refilled up to the configured cap.
3. If `tokens >= 1.0` the request proceeds and one token is deducted.
4. Otherwise `RESOURCE_EXHAUSTED` ("rate limit exceeded") is returned
   immediately — no request body is parsed.

### Client IP resolution

The layer resolves the client IP in priority order:

1. `x-forwarded-for` header — leftmost address (original client behind a proxy).
2. `TcpConnectInfo` extension from tonic — the TCP peer address of a direct
   connection.
3. `0.0.0.0` fallback sentinel — all such clients share one bucket.

For deployments behind a trusted reverse proxy, set `x-forwarded-for` at the
proxy level so the layer sees the real client IP rather than the proxy's IP.

### Exemptions

`/polargraph.v1.PolarGraphService/ReplicaStatus` bypasses rate limiting so
load-balancer health probes do not consume quota.

### Stale-entry cleanup

Every 1 000 requests the layer scans the bucket map and removes entries that
have not received a request in the last 60 seconds, keeping memory bounded for
transient clients.

### Configuration

| Flag | Env variable | Default | Description |
|---|---|---|---|
| `--rate-limit-rps N` | `POLARGRAPH_RATE_LIMIT_RPS` | `0` | Max requests/sec per client IP; 0 = disabled |

TOML config file equivalent:

```toml
[rate_limit]
max_rps = 100
```

When disabled (`max_rps = 0`) the layer is a zero-cost pass-through.

---

## REST gateway

`polargraph-rest` is a standalone binary (`polargraph-rest`) that accepts
HTTP/JSON requests and proxies them to a running `polargraphd` gRPC server.
It is useful for clients that cannot use a gRPC stub — browsers, scripting
environments, or languages without mature gRPC support.

### Architecture

```
HTTP client
    │  HTTP/JSON
    ▼
polargraph-rest  ──────  gRPC/proto  ──────▶  polargraphd
  (axum 0.6)               (tonic 0.11)
```

The gateway holds a single pooled `tonic::transport::Channel` to the upstream
and clones it cheaply per request. An `AuthInterceptor` tower layer
automatically attaches the configured API key as an `Authorization: Bearer`
header on every gRPC call.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/query` | Conjunctive graph query (patterns → bindings) |
| `POST` | `/insert` | Insert a single relation triple |
| `GET`  | `/triples` | Scan triples by subject/predicate/object filter |
| `POST` | `/vector/search` | k-NN vector search in a named HNSW space |
| `GET`  | `/health` | Proxy upstream `ReplicaStatus` as a health JSON |
| `POST` | `/explain` | Static query plan (no DB access) |

### Pattern string format (`/query` and `/explain`)

Patterns are 3-token strings: `<subject> <predicate> <object>`.

- `?varname` — variable; bound on first match, constrains subsequent matches
- `_` — wildcard; matches anything but is not captured
- UUID string — bound `NodeId` term
- Predicate accepts an optional leading `:` (stripped automatically)

Example:

```json
{
  "patterns": ["?s :knows ?o", "?o :name ?n"],
  "as_of_valid_time": null
}
```

### Datalog rules in `/query`

The `/query` endpoint accepts an optional `rules` array for recursive /
derived-predicate queries. Each rule has the form:

```json
{
  "rules": [
    {
      "head_predicate": "reachable",
      "head_subject_var": "x",
      "head_object_var": "z",
      "body": ["?x :edge ?y", "?y :edge ?z"]
    }
  ],
  "patterns": ["?src :reachable ?dst"]
}
```

When rules are present the server runs them to a fixed point (deriving IDB
facts), then evaluates `patterns` against the combined base + derived fact
set. Body patterns follow the same 3-token string format as query patterns.

### Edge properties in `/insert`

`POST /insert` accepts an optional `properties` array to store scalar
properties on the edge at insert time. The response includes `edge_id` — the
UUID under which those properties are stored — so clients can query them later.

```json
{
  "subject": "<uuid>",
  "predicate": "works_at",
  "object": "<uuid>",
  "properties": [
    {"name": "since", "value": {"int_val": 2019}},
    {"name": "role",  "value": {"text_val": "engineer"}}
  ]
}
```

Value encoding mirrors the proto `Value` oneof: `bool_val`, `int_val`,
`float_val`, `text_val`, `blob_val` (array of integers 0–255), `vec_val`
(`{"values": [...]}`), or `null_val`.

### CLI flags

| Flag | Env variable | Default | Description |
|------|---|---|---|
| `--upstream URL` | `POLARGRAPH_UPSTREAM` | `http://localhost:50051` | gRPC server address |
| `--listen ADDR` | `POLARGRAPH_REST_LISTEN` | `0.0.0.0:8000` | HTTP listen address |
| `--api-key KEY` | `POLARGRAPH_REST_API_KEY` | *(none)* | Forwarded as `Authorization: Bearer` to upstream |
| `--tls-ca PATH` | `POLARGRAPH_REST_TLS_CA` | *(none)* | PEM CA cert for upstream TLS verification |

### Error mapping

gRPC status codes map to HTTP status codes:

| gRPC | HTTP |
|------|------|
| `NOT_FOUND` | 404 |
| `UNAUTHENTICATED` | 401 |
| `PERMISSION_DENIED` | 403 |
| `RESOURCE_EXHAUSTED` | 429 |
| `DEADLINE_EXCEEDED` | 408 |
| `INVALID_ARGUMENT` | 400 |
| anything else | 500 |

### Known limitations

- `/triples` does not return the predicate value when `predicate` is omitted
  from the query params — the VarPattern proto does not support variable
  predicates, so the response echoes back an empty string in that slot.
- Node property triples (scalar values whose subject is a `NodeId`) must be
  inserted via the gRPC API directly; `/insert` only creates relation triples
  (with optional edge properties).

---

## Schema migrations

PolarGraph ships a versioned migration system that applies schema changes to
a live store at startup (and on demand via gRPC). Migrations run in ascending
version order; each is a Rust function that receives the live `TripleStore`
handle and can read or write META CF entries, insert triples, or invoke
registry APIs.

### Storage layout

All migration state lives in the META column family:

| Key | Value |
|-----|-------|
| `__migrations__/version` | Current schema version as a little-endian `u32` |
| `__migrations__/applied/<version>` | JSON-encoded `AppliedMigration` record |

On a fresh database the version key is absent (reads as `0`).

### Built-in migrations

| Version | Description |
|---------|-------------|
| 1 | `initial schema` — writes `__schema__/version = "1"` to META CF to establish the baseline |
| 2 | `normalize node type schema records` — re-registers all existing node type schemas through `NodeTypeRegistry` to ensure new serde-defaulted fields (e.g. `storage_mode` on `VectorSpaceDef`) are serialized explicitly in older records |

### Auto-migration on startup

`main.rs` calls `MigrationRunner::new(store).run_pending()` before the gRPC
server accepts connections. If any migration fails, the server exits with an
error. On success, applied version numbers are logged at `info` level.

Auto-migration is skipped in replica mode (replicas are read-only and receive
changes via WAL streaming).

### gRPC API

```
rpc MigrateSchema(MigrateRequest) returns (MigrateResponse)
rpc MigrationStatus(MigrationStatusRequest) returns (MigrationStatusResponse)
```

**`MigrateSchema`**

Applies all pending migrations. Setting `dry_run = true` reports what would
run without making any changes. Returns `FAILED_PRECONDITION` on a replica.

**`MigrationStatus`**

Returns the current schema version, the latest available version, and the
full history of applied migrations with wall-clock timestamps.

### Adding a migration

1. Write an `up` and `down` function with signature `fn(&TripleStore) -> Result<(), StorageError>`.
2. Append a new `Migration` struct to the `MIGRATIONS` static list in `polargraph-storage/src/migrations.rs`. The version must be strictly greater than the previous entry.
3. Add a test (at minimum: verify `run_pending()` applies the new version on a fresh database).

```rust
fn migration_v3_up(store: &TripleStore) -> Result<(), StorageError> {
    // ... make changes via store.insert(), store.db_write(), etc.
    Ok(())
}

fn migration_v3_down(_store: &TripleStore) -> Result<(), StorageError> {
    Ok(())
}

pub static MIGRATIONS: &[Migration] = &[
    // ... existing entries ...
    Migration {
        version: 3,
        description: "add my new feature",
        up: migration_v3_up,
        down: migration_v3_down,
    },
];
```

---

## Cypher query layer

PolarGraph exposes a Cypher surface over the Datalog evaluator. The compiler in `polargraph-query::cypher` translates Cypher AST nodes into `Query` / `Rule` / `VarPattern` structs that the existing evaluator pipeline already understands. No separate execution engine exists for Cypher — it is purely a frontend.

### Compiler pipeline

```
Cypher string
  → lexer / parser (hand-written recursive descent)
  → AST (MatchClause, WhereClause, ReturnClause, WriteClause)
  → CypherCompiler::compile()
  → Query { patterns, rules } + optional AggregationPlan
  → execute_query() / execute_recursive() / apply_aggregations()
  → CypherResponse
```

### MATCH and WHERE

Node patterns `(a:Person)` become two `VarPattern`s: one binding `a` to any subject and one constraining `a :__type "Person"`. Relationship patterns `(a)-[:knows]->(b)` add a third pattern for the relation triple.

`WHERE` equality predicates (`a.name = "Alice"`) compile to bound patterns `(a, "name", "Alice")`. Comparison predicates use post-filter evaluation. Text predicates (`CONTAINS`, `STARTS WITH`, `=~`) are routed to the trigram index (see Full-text trigram search below) and therefore do not generate Datalog patterns at all — the trigram scan returns a candidate node set that is then intersected with the rest of the join.

### Aggregations

`polargraph-query::aggregation` implements `apply_aggregations()` which runs after the core evaluator. The `AggregationPlan` struct describes:

- **Grouping keys** — the non-aggregated `RETURN` variables
- **Aggregates** — `COUNT(*)`, `COUNT(var)`, `COLLECT(var)`
- **Order spec** — list of `(key, Direction)` pairs
- **Skip / limit** — applied after sorting

The `WITH` clause compiles to a sub-plan: run the left-hand query, apply any aggregations, then feed the resulting bindings as a seed into the right-hand query via `execute_query_seeded`.

### Cypher writes

`polargraph-query::cypher::parse_write()` parses the write portion of a Cypher statement into a `Vec<WriteOp>`:

| WriteOp | Action |
|---------|--------|
| `CreateNode { var, labels, props }` | Allocates a new `NodeId`; inserts `__type` + property triples |
| `CreateRelation { from_var, predicate, to_var, props }` | Inserts a relation triple |
| `Merge { pattern }` | Runs a MATCH; if no results, executes CREATE |
| `SetProperty { var, key, value }` | Writes a new property triple (MVCC supersedes the old one) |
| `Delete { var }` | Marks facts as logically deleted by writing an end-of-life triple |

`execute_write_ops()` in the service handler runs these ops inside an MVCC `Transaction` and returns the new node IDs and total triple count.

### VECTOR_NEAR

`VECTOR_NEAR(a, "space", k)` is a special Cypher predicate. The compiler recognises it in the WHERE clause and emits a `VectorSeedCall` annotation instead of a Datalog pattern. The gRPC handler runs the ANN search first to obtain seed bindings, then calls `execute_query_seeded` with those bindings, exactly as `VectorSeedQuery` does. An inline `ef=N` argument overrides the exploration factor for that specific call.

---

## Full-text trigram search

### TRI column family

Text properties that are candidates for `CONTAINS` / `STARTS WITH` / `=~` filtering are indexed in a seventh column family (`TRI`). On every property triple write where `value` is `Value::Text`, the storage layer calls `extract_trigrams()` and writes one entry per trigram:

```
Key:   [trigram: 3 bytes][pred_id: 4 bytes][subject_id: 16 bytes]
Value: (empty)
```

The key layout sorts first by trigram, then by predicate, then by subject. A prefix scan on `[trigram][pred_id]` returns all subjects that contain the trigram under that predicate in O(log n + |results|) time.

### Trigram extraction

`extract_trigrams(text) -> HashSet<[u8; 3]>` pads the input with two null bytes, then slides a 3-byte window across the UTF-8 bytes. For `STARTS WITH`, only the leading trigram(s) of the pattern are extracted. For `=~`, the regex is statically analysed for literal substrings long enough to produce trigrams; if none can be found, the query falls back to a full SPO scan.

### Query path

1. `compile_cypher()` identifies text predicates in the WHERE clause.
2. It calls `text_search(store, predicate, pattern, mode)` → `HashSet<NodeId>`.
3. The resulting node set becomes an allowed-set filter applied to the join variables before the Datalog patterns execute, equivalent to the `SearchVectorInSet` approach used for vector post-filtering.

### Insert path

`TripleStore::insert()` detects `Value::Text` payloads and calls `insert_trigrams()` inside the same `WriteBatch` as the hexastore keys. There is no separate indexing step — trigrams are always consistent with the triple data.

---

## Schema-aware query optimization

`evaluate_with_registry(pattern, snapshot, registry: &EdgeTypeRegistry)` is an augmented variant of `evaluate()` in `polargraph-query::eval`. Before issuing the storage scan, it consults the registry for the pattern's predicate:

1. If the predicate has a registered `EdgeTypeDef` with a `domain` type, the evaluator prefixes the scan with a type filter: only subjects that have `__type = domain` are considered.
2. If the predicate has a `range` type, the same filter is applied to the object variable.

This prunes join branches early when the schema indicates only a subset of node types can participate in a predicate, avoiding unnecessary hexastore scans. The optimization is applied automatically by the gRPC handler when an `EdgeTypeRegistry` is present; no query syntax changes are required.

`SchemaHints` is a lightweight wrapper that caches per-predicate domain/range lookups in a `HashMap` to avoid repeated registry reads within a single multi-pattern query.

---

## Wire transactions

### Overview

Wire transactions allow a client to group multiple writes (and reads) into a single atomic MVCC unit across several RPC calls. The server holds the in-progress `Transaction` in memory until the client commits or rolls back.

### Storage in PolarGraphServer

```rust
open_txns: Arc<DashMap<String, Arc<Mutex<Transaction>>>>
```

`DashMap` provides lock-free sharded concurrent access. The outer `Arc` enables the map to be shared across tasks; the inner `Mutex<Transaction>` serializes access to each transaction.

### Lifecycle

1. **BeginTransaction** — allocates a UUID v4 token, calls `TripleStore::begin()`, stores the transaction in `open_txns`, returns the token as `tx_id`.
2. **InsertRequest / CypherWriteRequest with `tx_id`** — locks the transaction, buffers writes, releases the lock.
3. **QueryRequest with `tx_id`** — locks the transaction, performs a read at the transaction's `read_ts` (consistent snapshot).
4. **CommitTransaction** — removes the transaction from `open_txns`, calls `txn.commit()`, returns the commit timestamp.
5. **RollbackTransaction** — removes and drops the transaction (silent rollback).

### TTL cleanup

A background task runs every 60 seconds and evicts transactions whose last-access timestamp is more than 5 minutes old. Evicted transactions are silently rolled back. Clients that hold a transaction for longer than 5 minutes must handle `NOT_FOUND` on commit and retry.

### Conflict behavior

Commit returns `ABORTED` (gRPC) if the MVCC conflict checker detects a concurrent write to any (subject, predicate) pair touched by the transaction since `read_ts`.

---

## Server-streaming queries

### QueryStream and CypherQueryStream

Both RPCs follow the same pattern: the server opens the result cursor, then sends `StreamChunk` messages until exhausted.

```
STREAM_CHUNK_SIZE = 500  // bindings per message
```

Choosing 500 balances message-framing overhead against head-of-line blocking on slow consumers. The value is a compile-time constant in `polargraph-server::service`.

### REST NDJSON endpoints

`POST /query/stream` and `POST /cypher/stream` in `polargraph-rest` accept the same JSON bodies as their non-streaming counterparts but respond with `Content-Type: application/x-ndjson`. Each line is one JSON object representing a single variable binding map. The connection closes after the last result.

Clients that want to process results incrementally can consume the stream line-by-line without waiting for the full response body. This avoids buffering arbitrarily large result sets in the REST gateway process.

---

## Diagnostics RPCs

### ShowIndexes

Returns metadata for every column family (including `TRI` and `HNSW`) without performing any data scans:

| Field | Source |
|-------|--------|
| `cf_name` | Column family name string |
| `estimated_key_count` | RocksDB `estimate-num-keys` property |
| `estimated_size_bytes` | RocksDB `live-sst-files-size` property |
| `hnsw_space` | Present for the `hnsw` CF; includes space name, node count, dimensions, storage mode |

### ShowStats

Returns a snapshot of server internals:

| Field | Description |
|-------|-------------|
| `rocksdb_*` | Selected RocksDB statistics properties |
| `oracle_ts` | Current committed timestamp from the MVCC oracle |
| `open_transaction_count` | Number of in-flight wire transactions |
| `triple_count` | Approximate triple count from `estimate-num-keys` on the SPO CF |

Neither RPC touches the data plane, so they are safe to call on heavily loaded primaries.

---

## Scheduled retention

The `retention_scheduler.rs` module in `polargraph-server` provides an async loop that fires `CompactionManager::run_retention()` on a configurable interval.

### Configuration

```toml
[storage.retention_schedule]
enabled       = true
interval_secs = 3600   # default: hourly
```

Or at runtime via `--retention-schedule` / `POLARGRAPH_RETENTION_SCHEDULE` and `--retention-interval-secs` / `POLARGRAPH_RETENTION_INTERVAL_SECS`.

### Prometheus counters added

| Metric | Description |
|--------|-------------|
| `polargraph_retention_runs_total` | Total number of scheduled retention runs |
| `polargraph_retention_deleted_total` | Triples deleted across all scheduled runs |
| `polargraph_retention_last_run_ts` | Unix timestamp (seconds) of the most recent run |

The scheduler respects the `CancellationToken` and exits cleanly on graceful shutdown.

---

## Client SDKs

Three official client libraries wrap the gRPC API. All support API key auth, TLS, the full RPC surface, and wire transactions.

### Python (`clients/python/`)

Package name: `polargraph-client` (PyPI).

- `PolarGraphClient` — synchronous client backed by `grpc` channel
- `AsyncPolarGraphClient` — async client backed by `grpc.aio`
- Helper methods: `insert_node`, `insert_edge`, `query`, `cypher`, `cypher_write`, `insert_vector`, `search_vector`, `stream_query`, `begin_tx` / `commit_tx` / `rollback_tx`
- Proto stubs regenerated via `clients/python/scripts/regen_proto.sh`

### Go (`clients/go/`)

Module: `github.com/devcairn/polargraph-go`.

- `polargraph.New(addr, ...Option) (*Client, error)` — constructor
- Functional options: `WithAPIKey(key)`, `WithTLSCA(path)`
- Methods mirror the gRPC surface; return `(result, error)` idiom
- 5 unit tests; integration tests behind `POLARGRAPH_TEST_ADDR` env var

### JavaScript / TypeScript (`clients/js/`)

Package name: `@polargraph/client` (npm).

- `PolarGraphClient` class and `createClient(addr, key)` factory shorthand
- Full TypeScript types for all request/response shapes
- `streamQuery` and `cypherStream` return `AsyncIterable`
- `beginTx` / `commitTx` / `rollbackTx` for wire transactions
- Built with `tsup`; ships both ESM and CJS bundles

---

## Helm chart / deployment

The chart at `deploy/helm/polargraph/` provides a production-ready deployment for Kubernetes.

### Resources

| Kind | Name | Purpose |
|------|------|---------|
| `Namespace` | `polargraph` | Isolation boundary |
| `ConfigMap` | `polargraph-config` | Rendered `polargraph.toml` from Helm values |
| `Secret` | `polargraph-auth` | API key(s) |
| `PersistentVolumeClaim` | `polargraph-data` | RocksDB data; configurable `storageClass` and `size` |
| `Deployment` | `polargraph` | Single `polargraphd` pod |
| `Service` (ClusterIP) | `polargraph-grpc` | Internal gRPC access on port 50051 |
| `Service` (configurable) | `polargraph-ui` | Management UI on port 8080 |
| `Service` (ClusterIP) | `polargraph-metrics` | Prometheus scrape on port 9090 |
| `HorizontalPodAutoscaler` | `polargraph` | CPU-based autoscaling; set `minReplicas`/`maxReplicas` in values |

### Key values

```yaml
image:
  repository: polargraph/polargraphd
  tag: latest

auth:
  apiKey: "change-me"

storage:
  size: 20Gi
  storageClass: ""   # default storage class

replicaCount: 1      # increase for read replicas (requires --replica-of)

resources:
  requests:
    cpu: "500m"
    memory: "512Mi"
  limits:
    cpu: "2"
    memory: "2Gi"
```

For read-replica deployments, set `polargraph.replicaOf` in values to point at the primary's gRPC service address.

---

## Access Control

PolarGraph implements graph-native access control: permissions are stored as
ordinary triples, enabling the same query and traversal primitives that work
on domain data to also express policy.

### Data model

Two built-in node types (`User`, `Group`) and three built-in predicates form
the access-control schema:

| Triple pattern | Semantics |
|---|---|
| `(User) -[MEMBER_OF]-> (Group)` | User belongs to a group. |
| `(Group) -[HAS_ACCESS]-> (Node)` | Group has explicit access to a specific node. |
| `(Group) -[HAS_ACCESS_TYPE]-> type_name` | Group has access to *all* nodes of a registered type. |

`HAS_ACCESS_TYPE` is stored as a `Triple::Property` (text value = the type
name) rather than a relation, so no sentinel node is needed for type grants.

### AccessCache

At startup the server scans all `MEMBER_OF`, `HAS_ACCESS`, and
`HAS_ACCESS_TYPE` triples and builds an in-memory
`HashMap<String, HashSet<NodeId>>` keyed by `user_id.to_string()`.
This cache is invalidated and rebuilt after any `Insert` that touches the AC
predicates or `__type` (which changes the nodes covered by type-level grants).

The cache contains the *expanded* node set: for each user, all nodes
reachable via their group memberships (both direct and type-expanded grants)
are stored together so access checks are a single `HashSet::contains` call.

### Identity propagation

Every query RPC (`Query`, `CypherQuery`, `VectorSeedQuery`,
`SearchVectorFiltered`) accepts an optional `user_id: string` field.
When non-empty the server looks up the access set for that user and post-filters
result bindings to those whose bound `NodeId` values all appear in the set.
Bindings with no `NodeId` values (e.g., scalar-only results) are retained.

The user identity can also be supplied out-of-band via the
`x-polargraph-user-id` gRPC metadata header (or `X-User-Id` HTTP header in
the REST gateway). The proto field takes precedence.

### Management RPCs

| RPC | Description |
|---|---|
| `AddUserToGroup(user_id, group_id)` | Write `(User) -[MEMBER_OF]-> (Group)` and refresh the cache. |
| `GrantAccess(group_id, target)` | Write `HAS_ACCESS` (node) or `HAS_ACCESS_TYPE` (type name) triple. |
| `RevokeAccess(group_id, target)` | Close the `vt_end` of the matching access triple. |
| `GetUserAccess(user_id)` | Return the expanded node ID set and type grants for a user. |

REST equivalents: `POST /access/add-user`, `POST /access/grant`,
`POST /access/revoke`, `GET /access/user/:user_id`.

### Replica behaviour

`AddUserToGroup`, `GrantAccess`, and `RevokeAccess` return
`FAILED_PRECONDITION` on read replicas (write RPCs are always blocked). The
cache on a replica is built from replicated triples at startup and refreshed
on each WAL-applied batch that touches AC predicates.

---

## Named Cypher parameters

Cypher queries may include `$param` placeholders in WHERE clauses and
property equality filters:

```cypher
MATCH (a:Person) WHERE a.name = $name AND a.age > $min_age RETURN a
```

Parameters are supplied as a `map<string, string>` in the gRPC request (both
`CypherQueryRequest` and `VectorSeedQueryRequest`).  Each map value is a
JSON-encoded `Value` (e.g. `"\"Alice\""` for a string, `"42"` for an integer,
`"true"` for a boolean).

### How it works

1. **Lexer**: `$identifier` tokens are emitted as `Token::Param(name)`.
2. **Compiler**: `parse_literal()` returns `CypherValue::Param(name)`.
   The `emit_node_filters()` path propagates this through to
   `FilterValue::Param(name)` in the compiled `ValueFilter`.
3. **Cache hit**: the pre-substitution `CompiledQuery` (with `Param` placeholders)
   is stored in the plan cache keyed by the raw Cypher string.
4. **Substitution**: after retrieval from cache, `substitute_params()` clones
   the plan and replaces every `FilterValue::Param(name)` with
   `FilterValue::Literal(value)`.  Returns `CypherError` if a referenced
   parameter is absent from the map.
5. **Execution**: the substituted plan is executed normally.

REST endpoint: the `params` key of the JSON body sent to `POST /cypher` or
`POST /cypher/stream` accepts the same `{"name": "\"Alice\""}` shape.

Python SDK: `client.cypher(q, params={"name": "Alice"})` — the SDK
JSON-serialises values automatically.

Go SDK: `WithParams(map[string]string{"name": `\`"Alice"`\`})` option.

TypeScript SDK: `params?: Record<string, string>` in `CypherOptions`.

---

## Query plan cache

Compiled Cypher plans are cached in-process to avoid re-parsing and
re-compiling the same query string on every request.

### Cache behaviour

- **Key**: the raw Cypher string (before parameter substitution).
- **Value**: `Arc<CompiledQuery>` — the pre-substitution plan.
- **Implementation**: `Arc<DashMap<String, Arc<CompiledQuery>>>` on
  `PolarGraphServer` (lock-free sharded hash map from the `dashmap` crate).
- **Size limit**: configurable via `--query-cache-size N`
  (`POLARGRAPH_QUERY_CACHE_SIZE`, `[query] cache_size` TOML key); default
  1 000 entries.  When the cache is full, new entries are evicted (LRU
  eviction is not implemented — the cache silently skips inserting when at
  capacity).
- **Eviction on schema change**: not yet implemented; restarting the server
  clears the cache.

### Prometheus metrics

| Metric | Type | Description |
|--------|------|-------------|
| `polargraph_query_cache_hits_total` | counter | Plans served from cache |
| `polargraph_query_cache_misses_total` | counter | Plans compiled fresh |

`ShowStats` also returns `query_cache_hits`, `query_cache_misses`, and
`query_cache_size` as fields on `ShowStatsResponse`.

### Configuration

| Flag | Env variable | Default | Description |
|---|---|---|---|
| `--query-cache-size N` | `POLARGRAPH_QUERY_CACHE_SIZE` | `1000` | Maximum number of cached query plans |

---

## SPARQL 1.1 endpoint

PolarGraph exposes a SPARQL 1.1 HTTP protocol endpoint in the REST gateway
(`polargraph-rest`), implemented by the `polargraph-sparql` library crate.

### Architecture

```
SPARQL query string
  → spargebra::Query::parse()           # third-party SPARQL parser
  → polargraph_sparql::translate_query()  # algebra walker
  → SparqlTranslation { branches, filters, aggregates, … }
  → REST gateway: serialize to QueryRequest proto
  → polargraphd gRPC Query / ExplainQuery
  → bind results back to SparqlBindings
  → serialize_json() / serialize_csv()   # SPARQL 1.1 results formats
```

The translation layer (`polargraph-sparql`) has no dependency on
`polargraph-server` or `polargraph-storage`. It converts parsed SPARQL algebra
nodes into `VarPattern`, `Rule`, and `Branch` structs that the gRPC API already
understands.

### HTTP endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/sparql?query=…` | SPARQL query via URL parameter |
| `POST` | `/sparql` | SPARQL query; body can be `application/sparql-query`, `application/x-www-form-urlencoded` (with `query=` key), or raw query string |
| `POST` | `/sparql/update` | SPARQL 1.1 Update |

Content negotiation via `Accept` header: `application/sparql-results+json`
(default) or `text/csv`.

### Supported query features

| Feature | Notes |
|---------|-------|
| SELECT, ASK | Full support |
| CONSTRUCT | WHERE clause evaluated; template triples assembled from bindings; output in N-Triples or Turtle |
| DESCRIBE | Fetches all triples for the described subjects; output in N-Triples or Turtle |
| BGP (Basic Graph Patterns) | Translated to `VarPattern` lists |
| UNION | Each branch translated independently; results merged |
| OPTIONAL / LEFT JOIN | Implemented in `polargraph_sparql::execute::left_join()` |
| FILTER | BOUND, equality, comparison (`>`, `<`, `>=`, `<=`), NOT, AND, OR, `isIRI()` |
| Property paths: simple | Named node paths translated to a single `VarPattern` |
| Property paths: sequence | `a/b` — translated to two patterns with an intermediate variable |
| Property paths: `+`, `*`, `?` | `+` and `*` compile to recursive Datalog rules; `?` treated as single-hop |
| Property paths: reverse | `^pred` — subject/object swapped |
| Property paths: alternative | First alternative only (full UNION requires separate branches; see limitations) |
| GROUP BY + aggregates | COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, SAMPLE via `execute_sparql_aggregations()` |
| HAVING | Applied as a post-filter after aggregation |
| ORDER BY | Passthrough to aggregation ordering |
| LIMIT / OFFSET | Passthrough |
| DISTINCT | Deduplicated after projection |
| GRAPH (named graphs) | Graph IRI recorded; mapped to PolarGraph `View` for pattern scoping |
| SPARQL-star (full support) | Subject-position `<< :s :p :o >> :annot ?val` → `EdgeAnnotationStep`; object-position `?s :p << :a :b :c >>` → `GetEdgeIdsByTriple` lookup; variable predicate `<< :s ?p :o >> :annot ?val`; Turtle-star / N-Triples-star serialization in CONSTRUCT/DESCRIBE; SPARQL Update with embedded triples (`INSERT DATA { << s p o >> :annot val }`) |

### Supported Update operations

| Operation | Notes |
|-----------|-------|
| INSERT DATA | Each triple translated to an `InsertRequest` gRPC call |
| DELETE DATA | Each triple translated to a `DeleteTriples` gRPC call grouped by subject |
| INSERT/DELETE WHERE | WHERE clause evaluated via `Query` RPC; templates applied per binding row |

### Other known limitations

- Property path alternatives (`|`) use only the first branch — the full UNION is not yet materialized as separate query branches.
- `VALUES` inline data, `MINUS`, and `SERVICE` federation are not supported.
- Variable graph names in the `GRAPH` clause are rejected.
- `GRAPH` clause runtime scoping (restricting scans to the named View) is recorded in the translation but not enforced during pattern evaluation.

---

## OWL 2 RL materialization

PolarGraph implements OWL 2 RL Phase 1 forward-chaining materialization via
the `polargraph-storage::owl_rl` module. Materialized (derived) facts are
stored in the `DRV` column family, separate from the base hexastore, so they
can be cleared and re-derived independently.

### DRV column family

The `DRV` CF uses the same 44-byte SPO key layout as the hexastore CFs. On a
hexastore read the caller chooses whether to include derived facts by also
scanning DRV, or reads only the base CFs for the authoritative fact set.

### Implemented rules

`polargraph_storage::owl_rl::materialize()` applies 12 forward-chaining rules:

| Rule | Semantics |
|------|-----------|
| `rdfs2` | `P domain C`, `s P o` → `s type C` |
| `rdfs3` | `P range C`, `s P o` → `o type C` |
| `rdfs5` | `P subPropertyOf Q`, `Q subPropertyOf R` → `P subPropertyOf R` |
| `rdfs7 / prp-spo1` | `P subPropertyOf Q`, `s P o` → `s Q o` |
| `rdfs9` | `C subClassOf D`, `s type C` → `s type D` |
| `rdfs11` | `C subClassOf D`, `D subClassOf E` → `C subClassOf E` |
| `prp-symp` | `P type SymmetricProperty`, `s P o` → `o P s` |
| `prp-trp` | `P type TransitiveProperty`, `s P o`, `o P x` → `s P x` |
| `prp-inv1` | `P inverseOf Q`, `s P o` → `o Q s` |
| `prp-inv2` | `P inverseOf Q`, `s Q o` → `o P s` |
| `eq-sym` | `s sameAs o` → `o sameAs s` |
| `eq-trans` | `s sameAs o`, `o sameAs x` → `s sameAs x` |

Predicates are represented as `NodeId` values via `uri_to_node_id(uri)` (a
stable xxHash3-128 of the predicate URI string). `predicate_node(pred)` is
the public helper that converts a predicate string to its node representation.

### API

**Storage layer:**
- `store.insert_derived_batch(triples)` — write derived triples to `DRV`
- `store.clear_derived()` — truncate the DRV CF
- `store.scan_derived(snapshot_ts)` — iterate all derived triples
- `store.estimate_derived_count()` — approximate DRV entry count

**gRPC RPC:**
```
rpc RunMaterialization(RunMaterializationRequest) returns (RunMaterializationResponse)
```
Runs in `spawn_blocking` (CPU-intensive), guarded against replicas.
Returns `RunMaterializationResponse { derived_count }`.

**Prometheus gauge:** `polargraph_materialization_derived_total`

**Startup flag:** `--auto-materialize` / `POLARGRAPH_AUTO_MATERIALIZE` / `[storage] auto_materialize`
runs materialization at startup before accepting connections.

**REST endpoint:** `POST /materialize` — calls `RunMaterialization` and returns
`{"derived_count": N}`.

---

## RDF-star edge annotations

PolarGraph supports RDF-star-style annotations on edges (relation triples) via
two dedicated column families: `EPA` (edge property annotations) and `EPO`
(edge relation annotations).

### Data model

Three `Triple` variants handle edge-level data:

| Variant | Meaning |
|---------|---------|
| `Triple::Relation { subject, predicate, object, edge_id, temporal }` | A directed edge; `edge_id` is stable across MVCC versions |
| `Triple::EdgeProperty { edge, predicate, value, temporal }` | A scalar property on an edge (subject = `EdgeId`, maps to `NodeId(edge_id.0)`) |
| `Triple::EdgeRelation { edge, predicate, object, temporal }` | A relation from an edge to another node |

`EdgeProperty` triples are stored in the `EPA` CF with key `[edge_id:16][pred_id:4][tt:8]`.
`EdgeRelation` triples are stored in the `EPO` CF with key `[edge_id:16][pred_id:4][obj_id:16][tt:8]`.

The base hexastore CFs are unchanged — existing `Triple::Relation` and
`Triple::Property` storage is unaffected.

### API

**Storage:**
```rust
store.scan_edge_annotations(edge: EdgeId, snapshot_ts: Timestamp) -> Vec<EdgeAnnotation>
store.get_edge_annotation(edge: EdgeId, predicate: &str, snapshot_ts: Timestamp) -> Option<EdgeAnnotation>
```

**Insert:** `InsertRequest.edge_annotations` carries a `repeated EdgeAnnotation`; each is
converted by `edge_annotation_from_proto` and written in the same batch as the main triples.
`InsertResponse.edge_ids` returns the assigned `EdgeId` UUIDs.

**gRPC RPC:**
```
rpc GetEdgeAnnotations(GetEdgeAnnotationsRequest) returns (GetEdgeAnnotationsResponse)
```

**REST endpoints:**
- `POST /edge-annotations` — insert edge annotations
- `GET /edge-annotations/:edge_id` — retrieve all annotations for an edge

**SPARQL-star integration:** The SPARQL endpoint supports full SPARQL-star:
- **Subject position** `<< :Alice :knows :Bob >> :since ?date` — translated by `translate_sparql_star_subject()` into an `EdgeAnnotationStep`, resolved via `GetEdgeAnnotations`.
- **Object position** `?s :p << :a :b :c >>` — resolved via `GetEdgeIdsByTriple` to obtain the `EdgeId`, then used as an object term in the main pattern.
- **Variable predicate** `<< :s ?p :o >> :annot ?val` — enumerates annotations across predicates.
- **Turtle-star / N-Triples-star** — CONSTRUCT and DESCRIBE results serialize quoted triples in star format when the result contains edge annotations.
- **SPARQL Update** `INSERT DATA { << s p o >> :annot val }` — parses the embedded triple, resolves or creates the edge ID, and writes the annotation.

---

## Property version history

Every triple write creates a new MVCC entry (keyed by transaction time `tt`).
`scan_property_history` makes the full history accessible without MVCC
deduplication.

### Storage

```rust
store.scan_property_history(
    subject: &NodeId,
    predicate: &str,
    limit: usize,
) -> Vec<(Value, i64)>
```

Scans the SPO CF with a 36-byte prefix `[subject:16][pred_id:4][sentinel:16]`
(bypassing MVCC's latest-version selection) and returns all historical values
with their `tt` timestamps, ordered newest-first. `limit` caps the result.

### gRPC RPC

```
rpc GetPropertyHistory(GetPropertyHistoryRequest) returns (GetPropertyHistoryResponse)
```

Request: `subject_id`, `predicate`, `limit`. Response:
`repeated PropertyVersion { value_json, transaction_time }`.

### REST endpoint

```
GET /property-history?subject=<uuid>&predicate=<string>&limit=<n>
```

---

## BSBM benchmark suite

The `polargraph-bench::bsbm` module implements the Berlin SPARQL Benchmark
adapted to PolarGraph's native query API.

### Dataset generator

`polargraph-bench bsbm --scale-factor N` generates a deterministic e-commerce
dataset of `N×100` products, `N×10` product types (3-level hierarchy), `N×20`
features, `N×5` vendors, `N×50` offers, and `N×20` reviews. All entity IDs
are deterministic xxHash3-128 values so datasets are reproducible across runs.

### 12 query templates

Each query template exercises a different index access pattern:

| Query | Key operation |
|-------|---------------|
| Q1 | Type + feature filter + numeric property range |
| Q2 | Full subject scan (detail lookup) |
| Q3 | Two-feature join + numeric range |
| Q4 | UNION of two feature branches |
| Q5 | Two-hop star join (similar products via shared features) |
| Q6 | Trigram full-text search on product label |
| Q7 | Five-way join (offers + vendors + reviews + reviewers) |
| Q8 | Two-hop join (product → reviews → reviewer info) |
| Q9 | Single subject scan (review detail) |
| Q10 | Two-hop join (vendor → offer → product) |
| Q11 | COUNT aggregate |
| Q12 | Two-hop join (reviewer → review → product) |

### Running

```bash
# In-process (no polargraphd required)
cargo run -p polargraph-bench --release -- bsbm --scale-factor 1

# Criterion micro-benchmarks for Q1 and Q7
cargo bench -p polargraph-storage -- bsbm
```

See `BENCHMARKS.md` Part 3 for measured results (scale factor 1, Apple M-series).

### Criterion benchmarks

`polargraph-storage/benches/storage.rs` adds a `bsbm` group containing:
- `bsbm/q1_product_search` — PSO + SPO feature filter + property scan
- `bsbm/q7_five_way_join` — POS × 2 + SPO × 4 join at scale 1

---

## RDF interoperability

PolarGraph supports multi-format RDF import and export through the REST gateway
and the offline bulk importer.

### Supported formats

| Format | MIME type | Import | Export |
|--------|-----------|--------|--------|
| N-Triples | `application/n-triples` | ✓ | ✓ |
| Turtle | `text/turtle` | ✓ | ✓ |
| JSON-LD | `application/ld+json` | ✓ | ✓ |
| RDF/XML | `application/rdf+xml` | — | — |
| OWL/XML | `application/owl+xml` | — | — |

RDF/XML and OWL/XML are not supported; `POST /import/rdf` returns HTTP 415 for
those content types.

### Parsing pipeline (`polargraph-sparql::rdf_import`)

All RDF parsing lives in `crates/polargraph-sparql/src/rdf_import.rs` and uses
the [rio_api 0.8 / rio_turtle 0.8](https://github.com/oxigraph/rio) Oxigraph
parser family.

```
RDF bytes
  │
  ├─ parse_ntriples()   ──┐
  ├─ parse_turtle()     ──┼──► Vec<ImportedTriple>
  └─ parse_jsonld()     ──┘          │
                                     ▼
                           ImportedObject:
                             Iri(String)        → RelationTriple
                             BlankNode(String)  → RelationTriple (bnode NodeId)
                             Literal{Value,dt}  → PropertyTriple
```

IRIs are mapped to deterministic `NodeId`s via xxHash3-128:

```
uri_to_node_id("http://example.org/Alice")  →  stable NodeId
bnode_to_node_id("b0")  →  NodeId from "_:bnode_b0"
edge_id_for(s, p, o)    →  stable EdgeId for a Relation triple
```

RDF-star quoted triples (as subject or object) are stored as the N-Triples-star
string representation.

### Import endpoints

#### `POST /import/rdf`

Accepts N-Triples, Turtle, or JSON-LD based on `Content-Type`.
Triples are bulk-inserted via the gRPC `Insert` RPC in batches of 1 000.

```
POST /import/rdf
Content-Type: text/turtle

@prefix ex: <http://example.org/> .
ex:Alice ex:knows ex:Bob .
```

Response:
```json
{ "imported": 1, "total_parsed": 1, "duration_ms": 12 }
```

#### `POST /import/subgraph`

Identical to `POST /import/rdf`; provided as a semantic alias for
PolarGraph-to-PolarGraph subgraph transfers.

### Export endpoints

#### `GET /export/jsonld?subject=<uri>&predicates=p1,p2`
#### `POST /export/jsonld`  body: `{ "subjects": [...], "predicates": [...], "view_id": "..." }`

Returns a JSON-LD document grouping triples by subject:

```json
{
  "@context": { "xsd": "http://www.w3.org/2001/XMLSchema#", ... },
  "@graph": [
    {
      "@id": "http://example.org/Alice",
      "http://schema.org/knows": { "@id": "http://example.org/Bob" },
      "http://schema.org/age":   { "@value": "30", "@type": "xsd:integer" }
    }
  ]
}
```

When `predicates` is empty, a wildcard relation scan is performed.
**Known limitation**: the Query RPC's `VarPattern.predicate` is a string filter,
not a variable, so predicate names cannot be retrieved generically.
Exports with no predicate list use `<urn:polargraph:unknownPredicate>` as the
predicate IRI for relation triples whose predicate is not specified.

#### `GET /export/subgraph?subjects=uuid1,uuid2&predicates=p1,p2`

Exports a subgraph rooted at the given subjects, including edge annotations.
Accept-header negotiation:

| Accept | Response type |
|--------|---------------|
| `application/ld+json` | JSON-LD |
| `text/turtle` | Turtle |
| `application/n-triples` (default) | N-Triples |

### Schema RDF import/export

#### `GET /schema/rdf`

Exports all registered `NodeTypeDef` and `EdgeTypeDef` records as OWL/RDFS Turtle.
Uses the following IRI namespaces:

| Concept | IRI prefix |
|---------|-----------|
| Node type | `urn:polargraph:type:<TypeName>` |
| Property | `urn:polargraph:prop:<TypeName>/<fieldName>` |
| Edge relation | `urn:polargraph:rel:<predicateName>` |

```turtle
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

<urn:polargraph:type:Person> a owl:Class .
<urn:polargraph:prop:Person/name>
    a owl:DatatypeProperty ;
    rdfs:domain <urn:polargraph:type:Person> ;
    rdfs:range  xsd:string .
<urn:polargraph:rel:works_at>
    a owl:ObjectProperty ;
    rdfs:domain <urn:polargraph:type:Person> ;
    rdfs:range  <urn:polargraph:type:Company> .
```

#### `POST /schema/rdf`

Parses OWL/RDFS Turtle (or N-Triples) and calls `RegisterNodeType` /
`RegisterEdgeType` for each discovered class and property/relation.

### Offline bulk importer (multi-format)

`polargraph-import` accepts a `--format` flag to switch RDF parsers:

```bash
polargraph-import \
  --data-dir /data \
  --input data.ttl \
  --format turtle   # ntriples (default) | turtle | jsonld
```

The binary must run while `polargraphd` is stopped (SST ingestion requires
exclusive DB access).
