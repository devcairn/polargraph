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
    // RDF-star edge annotations (stored in EPA CF)
    EdgeProperty {
        edge:      EdgeId,
        predicate: Predicate,
        value:     Value,
        temporal:  BiTemporalRange,
    },
    // RDF-star edge relations (stored in EPO CF)
    EdgeRelation {
        edge:      EdgeId,
        predicate: Predicate,
        object:    NodeId,
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
    Vector(Vec<f32>),   // dense embedding; binary codec (not JSON)
}
```

`From<bool>`, `From<i64>`, `From<f64>`, `From<String>`, `From<&str>` are
all implemented.

Non-vector variants serialize to tagged JSON: `{ "type": "Int", "v": 42 }`.
`Vector` uses a dedicated binary codec (discriminant `0x03` + little-endian
`u32` length + raw IEEE 754 floats) to avoid JSON overhead on large arrays.

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

## `polargraph-query`

Pattern-based query evaluation, Cypher frontend, aggregations, and view
projection.

---

### `compile_cypher` (`polargraph_query::cypher`)

```rust
pub fn compile_cypher(cypher: &str) -> Result<CypherQuery, CypherError>
```

Parses a Cypher string and returns a `CypherQuery` containing:

- `patterns: Vec<VarPattern>` — compiled MATCH body
- `rules: Vec<Rule>` — recursive rules (from transitive closure syntax)
- `aggregation: Option<AggregationPlan>` — ORDER BY / COUNT / COLLECT
- `write_ops: Option<Vec<WriteOp>>` — present for write statements

Returns `CypherError::Parse` on invalid syntax and `CypherError::Unsupported`
for Cypher features not yet implemented.

---

### `execute_write_ops` (`polargraph_query::cypher`)

```rust
pub fn execute_write_ops(
    ops: &[WriteOp],
    txn: &mut Transaction,
    store: &TripleStore,
) -> Result<WriteResult, QueryError>
```

Executes a compiled list of write operations inside a caller-supplied
transaction. Returns `WriteResult { created_node_ids, triples_written }`.

---

### `apply_aggregations` (`polargraph_query::aggregation`)

```rust
pub fn apply_aggregations(
    plan: &AggregationPlan,
    bindings: Vec<Bindings>,
) -> Vec<Bindings>
```

Groups, aggregates, sorts, and applies skip/limit to a flat binding list.
Called by the service handler after `execute_query` or `execute_recursive`.

---

### `evaluate_with_registry` (`polargraph_query::eval`)

```rust
pub fn evaluate_with_registry(
    pattern: &VarPattern,
    snapshot: &Snapshot,
    registry: &EdgeTypeRegistry,
    bound: &Bindings,
) -> Result<Vec<Bindings>, QueryError>
```

Schema-aware variant of `evaluate`. Consults `registry` for the pattern
predicate's domain/range types and applies a type pre-filter before the
hexastore scan.

---

## `polargraph-server` — gRPC RPCs

Service: `polargraph.v1.PolarGraphService`

Proto source: `crates/polargraph-server/proto/polargraph.proto`

---

### `CypherQuery`

```
rpc CypherQuery(CypherQueryRequest) returns (CypherQueryResponse)
```

Parses and executes a Cypher read query. The Cypher string is compiled
to Datalog IR and evaluated by the standard query pipeline.

**Request fields:**

| Field | Type | Description |
|-------|------|-------------|
| `cypher` | `string` | Cypher query string |
| `vector` | `repeated float` | Required when `VECTOR_NEAR` is used |
| `ef` | `uint32` | HNSW exploration factor override (0 = server default) |
| `limit` | `uint32` | Result limit (overrides `LIMIT N` in the query string) |
| `tx_id` | `string` | Optional wire transaction ID for consistent reads |

**Response fields:** `repeated CypherRow rows` where each row contains a
`map<string, Value> columns` matching the `RETURN` clause variables.

---

### `CypherQueryStream`

```
rpc CypherQueryStream(CypherQueryRequest) returns (stream CypherStreamChunk)
```

Server-streaming variant of `CypherQuery`. Delivers rows in chunks of
`STREAM_CHUNK_SIZE = 500`. Accepts the same request fields as `CypherQuery`.

---

### `CypherWrite`

```
rpc CypherWrite(CypherWriteRequest) returns (CypherWriteResponse)
```

Executes a Cypher write statement (CREATE, MERGE, SET, DELETE).

**Request fields:**

| Field | Type | Description |
|-------|------|-------------|
| `cypher` | `string` | Write Cypher statement |
| `tx_id` | `string` | Optional wire transaction ID; writes buffered until commit |

**Response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `created_node_ids` | `repeated bytes` | UUIDs of newly created nodes |
| `triples_written` | `uint32` | Total triples committed (0 if using a wire transaction) |
| `commit_ts` | `int64` | Commit timestamp (0 if using a wire transaction) |

---

### `QueryStream`

```
rpc QueryStream(QueryRequest) returns (stream QueryStreamChunk)
```

Server-streaming variant of `Query`. Accepts the same `QueryRequest` message
(patterns, rules, time-travel fields, `tx_id`). Each `QueryStreamChunk`
carries up to 500 `Bindings`.

---

### `BeginTransaction`

```
rpc BeginTransaction(BeginTransactionRequest) returns (BeginTransactionResponse)
```

Opens a new wire transaction. Returns `tx_id` — a UUID v4 string that must
be supplied on subsequent `Insert`, `CypherWrite`, and `Query` calls to
associate them with this transaction.

---

### `CommitTransaction`

```
rpc CommitTransaction(CommitTransactionRequest) returns (CommitTransactionResponse)
```

Commits the transaction identified by `tx_id`. Returns `commit_ts`.
Returns `ABORTED` on write-write conflict; `NOT_FOUND` if the transaction
has expired or does not exist.

---

### `RollbackTransaction`

```
rpc RollbackTransaction(RollbackTransactionRequest) returns (RollbackTransactionResponse)
```

Discards the transaction identified by `tx_id`. No-op if the transaction has
already expired. Returns `NOT_FOUND` only if the `tx_id` was never valid.

---

### `ShowIndexes`

```
rpc ShowIndexes(ShowIndexesRequest) returns (ShowIndexesResponse)
```

Returns per-column-family statistics without scanning data.

**Response `IndexInfo` fields:**

| Field | Description |
|-------|-------------|
| `cf_name` | Column family name |
| `estimated_key_count` | From RocksDB `estimate-num-keys` property |
| `estimated_size_bytes` | From RocksDB `live-sst-files-size` property |
| `hnsw_info` | Present only for the `hnsw` CF; includes space name, node count, dimensions, storage mode |

---

### `ShowStats`

```
rpc ShowStats(ShowStatsRequest) returns (ShowStatsResponse)
```

Returns server internals snapshot.

**Response fields:**

| Field | Description |
|-------|-------------|
| `rocksdb_stats` | Map of selected RocksDB property name → value strings |
| `oracle_ts` | Current MVCC oracle timestamp (µs since Unix epoch) |
| `open_transaction_count` | Number of active wire transactions |
| `triple_count` | Approximate triple count from `estimate-num-keys` on SPO CF |

---

## REST gateway — updated endpoints

The following endpoints are available in addition to those documented in
`docs/architecture.md`.

| Method | Path | gRPC equivalent |
|--------|------|-----------------|
| `POST` | `/cypher` | `CypherQuery` |
| `POST` | `/cypher/write` | `CypherWrite` |
| `POST` | `/query/stream` | `QueryStream` (NDJSON) |
| `POST` | `/cypher/stream` | `CypherQueryStream` (NDJSON) |
| `GET` | `/indexes` | `ShowIndexes` |
| `GET` | `/stats` | `ShowStats` |
| `POST` | `/tx/begin` | `BeginTransaction` |
| `POST` | `/tx/commit` | `CommitTransaction` |
| `POST` | `/tx/rollback` | `RollbackTransaction` |

### `POST /cypher`

Request body mirrors `CypherQueryRequest`. Returns `{"rows": [{...}, ...]}`.

### `POST /cypher/write`

Request body: `{"cypher": "...", "tx_id": "..."}`. Returns
`{"created_node_ids": [...], "triples_written": N, "commit_ts": N}`.

### `POST /query/stream` and `POST /cypher/stream`

Identical request bodies to `/query` and `/cypher` respectively.
Response: `Content-Type: application/x-ndjson`; one JSON object per line,
each representing one row/binding. Connection closes after the last row.

### `GET /indexes` and `GET /stats`

No request body. Return JSON objects matching the gRPC response shapes.

### `POST /tx/begin`

No request body. Returns `{"tx_id": "<uuid>"}`.

### `POST /tx/commit`

Request body: `{"tx_id": "<uuid>"}`. Returns
`{"commit_ts": N, "triples_written": N}`.

### `POST /tx/rollback`

Request body: `{"tx_id": "<uuid>"}`. Returns `{}`.

---

## Additional gRPC RPCs

### `GetEdgeAnnotations`

```
rpc GetEdgeAnnotations(GetEdgeAnnotationsRequest) returns (GetEdgeAnnotationsResponse)
```

Returns all RDF-star annotations (properties and relations) stored on an edge
identified by its `edge_id` UUID. Annotations are MVCC-filtered to the latest
version at the current snapshot timestamp.

**Request fields:** `edge_id` (bytes, 16-byte UUID), optional `snapshot_ts`.

**Response fields:** `repeated EdgeAnnotation { predicate, value, object_id, transaction_time }`.

---

### `RunMaterialization`

```
rpc RunMaterialization(RunMaterializationRequest) returns (RunMaterializationResponse)
```

Runs OWL 2 RL forward-chaining materialization over the current triple store,
writing derived facts to the `DRV` column family. Returns
`RunMaterializationResponse { derived_count }`. Returns `FAILED_PRECONDITION`
on a read replica.

---

### `DeleteTriples`

```
rpc DeleteTriples(DeleteTriplesRequest) returns (DeleteTriplesResponse)
```

Soft-deletes triples by closing their valid-time window (`vt_end = now`). Used
by the SPARQL Update DELETE DATA handler and available directly.

**Request fields:**

| Field | Type | Description |
|-------|------|-------------|
| `subject_ids` | `repeated bytes` | UUIDs of the subjects whose triples to close |
| `predicate` | `string` | Predicate to filter on (empty = all predicates) |
| `vt_end` | `int64` | Valid-time end to write (0 = current timestamp) |

**Response fields:** `deleted_count` — number of entries closed.

---

### `GetPropertyHistory`

```
rpc GetPropertyHistory(GetPropertyHistoryRequest) returns (GetPropertyHistoryResponse)
```

Returns the full MVCC history of a scalar property without deduplication,
ordered newest-first. Useful for audit trails.

**Request fields:** `subject_id`, `predicate`, `limit` (max versions to return).

**Response fields:** `repeated PropertyVersion { value_json, transaction_time }`.

---

### `AddApiKey` / `RevokeApiKey` / `ListApiKeys`

```
rpc AddApiKey(AddApiKeyRequest) returns (AddApiKeyResponse)
rpc RevokeApiKey(RevokeApiKeyRequest) returns (RevokeApiKeyResponse)
rpc ListApiKeys(ListApiKeysRequest) returns (ListApiKeysResponse)
```

Runtime API key management without server restart. Keys added/revoked via these
RPCs take effect immediately on the running server. Require an existing valid key.

---

### `ValidateOntology`

```
rpc ValidateOntology(ValidateOntologyRequest) returns (ValidateOntologyResponse)
```

Validates that a set of triples is internally consistent with the registered
node and edge type schemas. Returns a list of validation errors (empty = valid).

---

## REST gateway — SPARQL endpoints

### `GET /sparql?query=<encoded>`

Executes a SPARQL 1.1 SELECT, ASK, CONSTRUCT, or DESCRIBE query supplied as a
URL-encoded `query` parameter. Content negotiation via `Accept` header:
`application/sparql-results+json` (default) or `text/csv`.

### `POST /sparql`

Body can be:
- `Content-Type: application/sparql-query` — raw SPARQL string
- `Content-Type: application/x-www-form-urlencoded` — `query=<encoded>` form body
- Anything else — treated as a raw SPARQL string

Response format negotiated via `Accept` header, same as `GET /sparql`.

### `POST /sparql/update`

Executes a SPARQL 1.1 Update request. Body is a raw SPARQL Update string.
Supports `INSERT DATA`, `DELETE DATA`, and `INSERT/DELETE WHERE`. Returns
`{"inserted": N, "deleted": N}`.

---

## REST gateway — additional endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/materialize` | Run OWL 2 RL materialization; returns `{"derived_count": N}` |
| `GET` | `/property-history` | Property version history; params: `subject`, `predicate`, `limit` |
| `POST` | `/edge-annotations` | Insert RDF-star edge annotations |
| `GET` | `/edge-annotations/:edge_id` | Retrieve all annotations for an edge |
| `POST` | `/access/grant` | Grant a group access to a node or type |
| `POST` | `/access/revoke` | Revoke group access |
| `POST` | `/access/add-user` | Add a user to a group |
| `GET` | `/access/user/:user_id` | Return expanded access set for a user |
