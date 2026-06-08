# PolarGraph DB Engine — API Reference

Public types and methods, organized by crate. This supplements `rustdoc`
(run `cargo doc --open` for the rendered HTML version).

---

## `polargraph-core`

Dependency-free primitive types. Every other crate depends on this one.

---

### `NodeId`

Opaque node identifier. Wraps a UUID v7 (time-ordered).

```rust
pub struct NodeId(pub Uuid);
```

| Method | Description |
|--------|-------------|
| `NodeId::new() -> NodeId` | Allocate a new unique ID (UUID v7) |
| `NodeId::as_bytes() -> &[u8; 16]` | 16-byte big-endian representation for index keys |
| `impl Display` | Renders as a UUID string |

---

### `EdgeId`

Opaque edge identifier. Same shape as `NodeId`.

```rust
pub struct EdgeId(pub Uuid);
```

| Method | Description |
|--------|-------------|
| `EdgeId::new() -> EdgeId` | Allocate a new unique ID (UUID v7) |
| `EdgeId::as_bytes() -> &[u8; 16]` | 16-byte big-endian representation |

---

### `Timestamp`

Microseconds since Unix epoch, stored as `i64`.

```rust
pub struct Timestamp(pub i64);
```

| Method / Constant | Description |
|-------------------|-------------|
| `Timestamp::now() -> Timestamp` | Current wall-clock time |
| `Timestamp::END_OF_TIME` | `i64::MAX` — sentinel for "fact still current" |
| `Timestamp::to_be_bytes() -> [u8; 8]` | Sortable big-endian bytes for index keys |
| `Timestamp::from_be_bytes([u8; 8]) -> Timestamp` | Decode from index key bytes |

---

### `BiTemporalRange`

Bitemporal envelope attached to every triple.

```rust
pub struct BiTemporalRange {
    pub vt_start: Timestamp,   // when the fact became true in the world
    pub vt_end:   Timestamp,   // when it stopped (END_OF_TIME if still current)
    pub tt:       Timestamp,   // transaction time — when we recorded it
}
```

| Method | Description |
|--------|-------------|
| `BiTemporalRange::assert_now(valid_from: Timestamp) -> BiTemporalRange` | Construct an open-ended, currently-valid range recorded right now |

---

### `Predicate`

An interned predicate / relationship label. Stored as a `String`; the
storage layer interns it to a `u32` ID inside index keys.

```rust
pub struct Predicate(pub String);
```

| Method | Description |
|--------|-------------|
| `Predicate::new(s: impl Into<String>) -> Predicate` | Construct from any string |
| `impl Display` | Renders the inner string |

---

### `Triple`

The atomic storage unit. Two variants share the same index structure.

```rust
pub enum Triple {
    Relation {
        subject:   NodeId,
        predicate: Predicate,
        object:    NodeId,
        edge_id:   EdgeId,
        temporal:  BiTemporalRange,
    },
    Property {
        subject:   NodeId,
        predicate: Predicate,
        value:     Value,
        temporal:  BiTemporalRange,
    },
}
```

| Method | Description |
|--------|-------------|
| `triple.subject() -> NodeId` | Subject node of the triple |
| `triple.predicate() -> &Predicate` | Predicate label |
| `triple.temporal() -> &BiTemporalRange` | Bitemporal envelope |

---

### `Node`

In-memory projection of a node assembled from query results. Not a primary
storage type.

```rust
pub struct Node {
    pub id:         NodeId,
    pub node_type:  String,
    pub properties: HashMap<String, Value>,
}
```

| Method | Description |
|--------|-------------|
| `Node::new(node_type: impl Into<String>) -> Node` | Allocate with a fresh `NodeId` |

---

### `Edge`

In-memory projection of a directed relationship.

```rust
pub struct Edge {
    pub id:        EdgeId,
    pub from:      NodeId,
    pub to:        NodeId,
    pub predicate: Predicate,
    pub properties: HashMap<String, Value>,
    pub temporal:  BiTemporalRange,
}
```

| Method | Description |
|--------|-------------|
| `Edge::new(from, to, predicate) -> Edge` | Allocate with a fresh `EdgeId`, open-ended temporal range |

---

### `Value`

Typed scalar property value.

```rust
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}
```

`From<bool>`, `From<i64>`, `From<f64>`, `From<String>`, `From<&str>` are
all implemented. A `Vector(Vec<f32>)` variant is planned for embedding
support.

Serialization uses tagged JSON: `{ "type": "Int", "v": 42 }`.

---

### `View`

A named lens over the graph. Controls which nodes/predicates are visible and
how edge labels are rendered.

```rust
pub struct View {
    pub id:                ViewId,
    pub display_name:      String,
    pub node_filter:       Option<NodeFilter>,
    pub visible_predicates: HashSet<String>,
    pub edge_presentations: HashMap<String, EdgePresentation>,
}
```

| Method | Description |
|--------|-------------|
| `View::new(id, display_name) -> View` | Create an unfiltered view (shows everything) |
| `view.edge_label(predicate) -> &str` | Display label for a predicate; falls back to the canonical name |
| `view.is_reversed(predicate) -> bool` | Whether to flip the arrow direction in rendering |
| `view.shows_predicate(predicate) -> bool` | Whether the predicate is visible in this view |

---

### `NodeFilter`

Selects which nodes belong to a view.

```rust
pub struct NodeFilter {
    pub include_types:  HashSet<String>,   // empty = all types
    pub explicit_nodes: HashSet<NodeId>,   // always include these
}
```

| Method | Description |
|--------|-------------|
| `NodeFilter::by_types(types) -> NodeFilter` | Include only nodes whose `node_type` is in the given set |

---

### `EdgePresentation`

Per-predicate rendering override within a view.

```rust
pub struct EdgePresentation {
    pub label:             String,   // display label
    pub reverse_direction: bool,     // flip arrow in UI
}
```

---

## `polargraph-storage`

RocksDB-backed triple store with predicate interning and MVCC.

---

### `TripleStore`

The main storage handle. Cheap to clone (`Arc`-backed).

```rust
pub struct TripleStore { /* private */ }
```

#### Opening

```rust
TripleStore::open(path: &Path) -> Result<TripleStore, StorageError>
```

Opens (or creates) a RocksDB database at `path` with all 7 column families
(6 triple indexes + META). Loads the predicate intern table from META on
startup.

#### Predicate interning

```rust
store.intern_predicate(pred: &str) -> Result<PredId, StorageError>
store.predicate_string(id: PredId) -> Option<String>
```

`intern_predicate` assigns and persists a new `u32` ID on first call for a
given string. Subsequent calls are a read-locked hash-map lookup (fast path).

#### Insert

```rust
store.insert(triple: &Triple) -> Result<(), StorageError>
```

Writes the triple to all 6 index CFs atomically. For property triples, the
property sentinel (`0xFF × 16`) is used in the object slot.

> **Note**: `insert` writes directly with the triple's existing `tt`. For
> MVCC-stamped writes, use `Transaction::insert` instead.

#### Snapshot scans (unfiltered)

These scan all versions; use the MVCC snapshot variants for point-in-time reads.

```rust
store.scan_by_subject(subject: &NodeId) -> Result<Vec<Triple>, StorageError>
store.scan_by_subject_predicate(subject: &NodeId, predicate: &str) -> Result<Vec<Triple>, StorageError>
store.scan_by_predicate(predicate: &str) -> Result<Vec<Triple>, StorageError>
store.scan_by_predicate_object(predicate: &str, object: &NodeId) -> Result<Vec<Triple>, StorageError>
store.scan_by_object(object: &NodeId) -> Result<Vec<Triple>, StorageError>
```

Each method uses the optimal CF for the given bind pattern (see architecture
doc for the full mapping).

#### MVCC entry points

```rust
store.begin() -> Transaction
store.snapshot() -> Snapshot
store.snapshot_at(ts: Timestamp) -> Snapshot
```

---

### `Transaction`

An in-progress read-write transaction. Obtained via `TripleStore::begin()`.

```rust
pub struct Transaction {
    pub read_ts: Timestamp,
    // write buffer is private
}
```

#### Writes

```rust
txn.insert(triple: Triple)
```

Buffers the triple. The `tt` field is ignored at this point; the actual
commit timestamp is assigned at `commit()`.

#### Snapshot reads (via `read_ts`)

```rust
txn.scan_by_subject(subject: &NodeId) -> Result<Vec<Triple>, StorageError>
txn.scan_by_subject_predicate(subject, predicate) -> Result<Vec<Triple>, StorageError>
txn.scan_by_predicate(predicate: &str) -> Result<Vec<Triple>, StorageError>
txn.scan_by_predicate_object(predicate, object) -> Result<Vec<Triple>, StorageError>
txn.scan_by_object(object: &NodeId) -> Result<Vec<Triple>, StorageError>
```

All return only triples with `tt <= read_ts`.

#### Commit / rollback

```rust
txn.commit() -> Result<Timestamp, StorageError>
```

Returns the commit timestamp on success. Returns
`StorageError::WriteConflict(ConflictError)` if a write-write conflict was
detected. Dropping a `Transaction` without calling `commit()` is a silent
rollback (nothing is written).

---

### `Snapshot`

A read-only point-in-time view. Obtained via `TripleStore::snapshot()` or
`TripleStore::snapshot_at(ts)`.

```rust
pub struct Snapshot {
    pub ts: Timestamp,
}
```

Exposes the same five scan methods as `Transaction`, all filtered to
`tt <= ts`.

---

### `TimestampOracle`

Monotonically increasing transaction timestamp source. Shared by all
transactions on a store. Cheap to clone.

```rust
pub struct TimestampOracle { /* Arc-backed */ }
```

| Method | Description |
|--------|-------------|
| `oracle.read_ts() -> Timestamp` | Snapshot current committed timestamp (no lock) |

Advancing the oracle (via `begin_commit`) is internal to `Transaction::commit`.

---

### `ConflictError`

Returned inside `StorageError::WriteConflict` when a transaction's commit
detects a concurrent write to the same (subject, predicate) pair.

```rust
pub struct ConflictError {
    pub subject:   NodeId,
    pub predicate: String,
}
```

---

### `StorageError`

```rust
pub enum StorageError {
    RocksDb(rocksdb::Error),
    Json(serde_json::Error),
    KeyDecode(String),
    MissingCf(String),
    WriteConflict(ConflictError),
}
```

---

### Key encoding (`polargraph_storage::keys`)

Internal module, but useful to understand when debugging index contents.

| Function | Output size | Description |
|----------|-------------|-------------|
| `encode_spo(s, p, o, tt)` | 44 bytes | SPO key |
| `encode_sop(s, o, p, tt)` | 44 bytes | SOP key |
| `encode_pso(p, s, o, tt)` | 44 bytes | PSO key |
| `encode_pos(p, o, s, tt)` | 44 bytes | POS key |
| `encode_osp(o, s, p, tt)` | 44 bytes | OSP key |
| `encode_ops(o, p, s, tt)` | 44 bytes | OPS key |
| `decode_spo(key)` | `DecodedSpo` | Decode SPO key |
| `decode_pso(key)` | `DecodedPso` | Decode PSO key |
| `decode_pos(key)` | `DecodedPos` | Decode POS key |
| `decode_osp(key)` | `DecodedOsp` | Decode OSP key |
| `spo_prefix_s(s)` | 16 bytes | Prefix for all triples with subject `s` |
| `spo_prefix_sp(s, p)` | 20 bytes | Prefix for `(s, p, ?)` scan |
| `pso_prefix_p(p)` | 4 bytes | Prefix for all triples with predicate `p` |
| `pos_prefix_po(p, o)` | 20 bytes | Prefix for `(?, p, o)` scan |
| `osp_prefix_o(o)` | 16 bytes | Prefix for all triples with object `o` |

`PROPERTY_SENTINEL: [u8; 16]` — `[0xFF; 16]`, the sentinel placed in the
object slot for property triples.

---

### Value codec (`polargraph_storage::codec`)

| Function | Description |
|----------|-------------|
| `encode_relation(edge_id, temporal) -> Vec<u8>` | 33-byte relation value |
| `encode_property(value, temporal) -> Result<Vec<u8>, StorageError>` | 17+N property value |
| `decode_value(bytes) -> Result<DecodedValue, StorageError>` | Decode either variant |

`DecodedValue` is an enum with `Relation { edge_id, temporal }` and
`Property { value, temporal }` arms. The decoded `temporal.tt` is always
`Timestamp(0)` — the caller fills it in from the index key.

---

## `polargraph-query` (stubs)

Modules `planner` and `projection` exist but contain no public API yet.
This crate will expose:

- A query type (conjunctive Datalog-style pattern)
- A `plan(query) -> ExecutionPlan` function
- A `project(triples, view) -> ProjectedGraph` function

---

## `polargraph-server`

Binary crate (`polargraphd`). No public library API. Entry point:

```
RUST_LOG=info cargo run -p polargraph-server
```

Currently starts the tracing subscriber and exits. Config loading, store
opening, and gRPC binding are not yet implemented.
