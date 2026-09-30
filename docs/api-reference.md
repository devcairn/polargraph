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
| `NodeId::from_iri(iri) -> NodeId` | xxHash3-128 of the IRI (prefer `term::iri_to_node_id`) |
| `NodeId::as_bytes() -> &[u8; 16]` | 16-byte big-endian representation for index keys |
| `impl Display` | Renders as a UUID string |

---

### `GraphId` and `Quad`

```rust
pub struct GraphId(pub u32);        // GraphId::DEFAULT = GraphId(0)
pub struct Quad { pub triple: Triple, pub graph: GraphId }
```

Graph IRIs are interned to `GraphId`s by the store (`intern_graph`). Graph 0
is the default graph and has no IRI.

---

### Term identity (`polargraph_core::term`, `polargraph_core::skolem`)

| Function | Description |
|----------|-------------|
| `term::iri_to_node_id(iri)` | `urn:uuid:<u>` → `NodeId(u)`; any other IRI → xxHash3-128 |
| `term::fallback_iri(&NodeId)` | `urn:uuid:<id>` — the IRI of a node with no dictionary entry |
| `term::needs_dictionary(iri)` | True for hashed (non-`urn:uuid:`) IRIs |
| `term::edge_id_for(s, p, o)` | Deterministic `EdgeId` for RDF relations |
| `term::literal_to_value(lexical, datatype, lang)` | The one RDF literal → `Value` mapping |
| `skolem::ImportScope::new(base, import_id)` / `::fresh(base)` | Blank-node scope; `skolem_iri(label)`, `bnode_node_id(label)` |
| `skolem::parse_skolem_iri(iri)`, `skolem::deskolemized_label(iri)` | For de-skolemizing export |

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

The atomic storage unit. `Relation` and `Property` share the quad index (a
property's object slot is its value's content hash); the two RDF-star
variants live in the annotation CFs.

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
    // RDF-star edge annotations (stored in epag / peag)
    EdgeProperty {
        edge:      EdgeId,
        predicate: Predicate,
        value:     Value,
        temporal:  BiTemporalRange,
    },
    // RDF-star edge relations (stored in epog)
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
    LangText { text: String, lang: String },       // "Acme"@en
    Typed { lexical: String, datatype: String },   // "2026-09-29"^^xsd:date
}
```

`From<bool>`, `From<i64>`, `From<f64>`, `From<String>`, `From<&str>` are
all implemented. `value.as_text()` returns the string of `Text` and
`LangText` (used by trigram indexing, text search and Cypher string
predicates, so tagged labels stay searchable).

RDF literals map to values through `polargraph_core::term::literal_to_value`
on every path: a language tag → `LangText`; no datatype or `xsd:string` →
`Text`; XSD integer types → `Int`; `double`/`float`/`decimal` → `Float`;
`boolean` → `Bool`; any other datatype, or a lexical form that doesn't parse
as its datatype → `Typed`. Language tag and datatype are part of equality, so
the SPARQL pattern `?s :label "Acme"@en` doesn't match `"Acme"` or
`"Acme"@fr`. Cypher, which has no language tags, compares a `LangText` by its
text.

On the wire these are `Value.lang_text` (`LangText { text, lang }`) and
`Value.typed` (`TypedLiteral { lexical, datatype }`). The REST gateway renders
them as JSON-LD value objects (`{"@value", "@language"}` /
`{"@value", "@type"}`) and accepts the same objects on input. The Python,
Go and TypeScript SDKs don't decode the new kinds yet.

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

The main storage handle. Cheap to clone (`Arc`-backed). Storage format v3 —
see `docs/architecture.md` (Storage layer) and `docs/design/v3-key-layout.md`.

```rust
pub struct TripleStore { /* private */ }
```

#### Opening

```rust
TripleStore::open(path: &Path) -> Result<TripleStore, StorageError>
TripleStore::open_as_replica(path: &Path, primary_address: String) -> Result<TripleStore, StorageError>
polargraph_storage::migrate_v3::open_for_migration(path) -> Result<TripleStore, StorageError>
```

`open` opens (or creates) the database with all 17 column families and loads
the predicate and graph intern tables. A new store is stamped storage format
3. A store that still holds v2 data returns `StorageError::NeedsMigration`
(run `polargraphd migrate`); one written by a newer build returns
`StorageError::UnsupportedFormat`. `open_as_replica` opens the same way but
every public write API returns `StorageError::ReadOnly`.

#### Writes

```rust
store.insert(triple: &Triple) -> Result<(), StorageError>          // one-triple transaction
store.insert_at_ts(triple: &Triple, tt: Timestamp) -> Result<(), StorageError>
store.begin() -> Transaction
```

`insert` and `insert_at_ts` write to the default graph with `WriteMode::Auto`.
`insert_at_ts` stamps an explicit transaction time, skips conflict checks
and advances the oracle — for tests and offline tools.

#### Reads (current state)

```rust
store.snapshot(ts: Timestamp) -> Snapshot
store.oracle_ts() -> i64                                            // latest commit ts
store.scan_by_subject(subject: &NodeId)
store.scan_by_subject_predicate(subject: &NodeId, predicate: &str)
store.scan_by_predicate(predicate: &str)
store.scan_by_predicate_object(predicate: &str, object: &NodeId)
store.scan_by_object(object: &NodeId)
store.scan_by_subject_object(subject: &NodeId, object: &NodeId)
store.scan_all()
    // all -> Result<Vec<Triple>, StorageError>
store.scan_property_history(subject: NodeId, predicate: &str, limit: u32)
    -> Result<Vec<(Value, i64)>, StorageError>                     // newest first
store.text_search(predicate: &str, query: &str, snapshot_ts: Timestamp, vt_as_of: Option<i64>)
    -> Result<Vec<NodeId>, StorageError>
```

The `scan_*` convenience methods read at the latest commit and **valid now**
(closed / replaced facts are hidden), across all graphs, returning each
`(s, p, o)` once. Each uses the optimal quad order for its bound slots.

#### Predicates and graphs

```rust
store.intern_predicate(pred: &str) -> Result<PredId, StorageError>
store.predicate_string(id: PredId) -> Option<String>
store.lookup_predicate(pred: &str) -> Option<PredId>
store.intern_graph(iri: &str) -> Result<GraphId, StorageError>    // assigns on first use
store.graph_id(iri: &str) -> Option<GraphId>
store.graph_iri(id: GraphId) -> Option<String>                    // None for the default graph
store.list_graphs() -> Vec<(GraphId, String)>
store.graph_node(id: GraphId) -> Option<NodeId>                  // the graph IRI's node
store.graph_for_node(node: &NodeId) -> Option<GraphId>
```

#### Named-graph management (`polargraph_storage::graphs`)

```rust
store.system_graph() -> Result<GraphId, StorageError>             // urn:pg:graph:meta
store.create_graph(iri: &str, metadata: &[(String, Value)]) -> Result<GraphId, StorageError>
store.set_graph_metadata(g: GraphId, metadata: &[(String, Value)]) -> Result<(), StorageError>
store.graph_metadata(g: GraphId) -> Result<Vec<(String, Value)>, StorageError>
store.graph_stats(g: GraphId) -> Result<GraphStats, StorageError> // { live_quads, last_write_tt }
store.drop_graph(g: GraphId) -> Result<usize, StorageError>       // bitemporal: closes live quads
store.copy_graph(src: GraphId, dst: GraphId, clear_target: bool) -> Result<usize, StorageError>
store.move_graph(src: GraphId, dst: GraphId) -> Result<usize, StorageError>
```

Metadata lives on the graph's IRI node in the system graph
(`SYSTEM_GRAPH_IRI`); one value per predicate. The default graph has no
metadata (`Validation` error). Drop and copy commit in chunks of
`GRAPH_OP_CHUNK` (50 000); a chunked copy flags the target with
`COPY_IN_PROGRESS_PRED = true` until it completes.

#### IRI dictionary

```rust
store.iri_of(node: &NodeId) -> Result<Option<String>, StorageError>
store.iris_of(nodes: &[NodeId]) -> Result<HashMap<NodeId, String>, StorageError>
store.bind_iris(iris: impl IntoIterator<Item = &str>) -> Result<(), StorageError>
```

Only hashed IRIs are stored (`urn:uuid:` IRIs carry their id). A different
IRI for an already-named node is `StorageError::IriCollision`.

#### Out-of-line values and maintenance

```rust
store.set_inline_value_max_bytes(n: usize)   // default DEFAULT_INLINE_VALUE_MAX_BYTES = 256
store.inline_value_max_bytes() -> usize
store.sweep_unreferenced_blobs() -> Result<usize, StorageError>
store.compact_cf(cf_name: &str) -> Result<(), StorageError>
store.estimate_triple_count() -> u64
```

Vector (`insert_vector`, `search_vector*`, `batch_insert_vectors`), RDF-star
annotation (`scan_edge_annotations`, `get_edge_annotation`,
`scan_annotations_by_predicate`), derived-fact (`insert_derived_batch`,
`scan_derived*`, `clear_derived`) and replication (`apply_replicated_batch`,
`last_applied_seq`) methods are documented in the architecture doc sections
for those features.

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
txn.insert(triple: Triple)                                    // default graph, WriteMode::Auto
txn.insert_in(triple: Triple, graph: GraphId, mode: WriteMode)
txn.bind_iri(iri: impl Into<String>)
txn.pending_triples() -> &[Triple]
```

Writes are buffered; `tt` is assigned at `commit()`. `bind_iri` records
`iri` in the IRI dictionary on commit, naming `term::iri_to_node_id(iri)`.

```rust
pub enum WriteMode {
    Auto,     // default: Replace for an open-ended property, Add otherwise
    Replace,  // close every other open value of (s, p, graph), then write
    Add,      // write alongside existing values
}
```

`WriteMode` only affects property triples.

#### Reads (as of `read_ts`, valid now)

```rust
txn.scan_by_subject(subject: &NodeId)
txn.scan_by_subject_predicate(subject: &NodeId, predicate: &str)
txn.scan_by_predicate(predicate: &str)
txn.scan_by_predicate_object(predicate: &str, object: &NodeId)
txn.scan_by_object(object: &NodeId)
txn.scan_by_subject_object(subject: &NodeId, object: &NodeId)
txn.scan_all()
```

#### Commit / rollback

```rust
txn.commit() -> Result<Timestamp, StorageError>
```

Returns the commit timestamp. Returns `StorageError::WriteConflict` if a
version committed after `read_ts` exists for the same quad — or, for a
`Replace` write, for any value of the same `(subject, predicate, graph)`.
Dropping a `Transaction` without calling `commit()` is a silent rollback.

---

### `Snapshot`

A read-only point-in-time view: `store.snapshot(ts)`.

```rust
pub struct Snapshot {
    pub ts: Timestamp,
    pub vt_as_of: Option<i64>,   // valid-time point; defaults to now
}
```

| Method | Description |
|--------|-------------|
| `with_vt_as_of(vt)` | Pin the valid-time point (time travel) |
| `scan_by_subject`, `scan_by_subject_predicate`, `scan_by_predicate`, `scan_by_predicate_object`, `scan_by_object`, `scan_by_subject_object`, `scan_all` | As on `TripleStore`, at `ts` / `vt_as_of`, all graphs |
| `scan_by_predicate_value(predicate, &Value)` | Property triples with exactly that value — a value-index lookup |
| `scan_graph(g: GraphId)` | Every triple in graph `g` |
| `scan_by_subject_in_graph(g, subject)` | Triples of `subject` in `g` |
| `scan_scoped(subject, predicate, object, &GraphScope)` | Match any bound slots within a graph scope; returns `(GraphId, Triple)` |
| `text_search(predicate, query)` | Trigram search, confirmed against live values |
| `scan_edge_annotations(edge)`, `scan_annotations_by_predicate(p)`, `scan_edge_annotations_as_triples(edge)` | RDF-star annotations |

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
    Rocks(rocksdb::Error),
    Serde(serde_json::Error),
    Io(std::io::Error),
    MissingCf(String),
    KeyDecode(String),
    WriteConflict(ConflictError),
    ReadOnly(String),
    Validation(String),
    NeedsMigration,              // store is still storage format v2
    UnsupportedFormat(u32),      // written by a newer build
    IriCollision { node: NodeId, existing: String, new: String },
}
```

gRPC mapping: `WriteConflict` → `ABORTED`; `ReadOnly`, `Validation`,
`NeedsMigration`, `UnsupportedFormat` → `FAILED_PRECONDITION`;
`IriCollision` (two different IRIs hashing to one `NodeId`) →
`ALREADY_EXISTS`; the rest → `INTERNAL`.

---

### Key encoding (`polargraph_storage::keys`)

Internal module, but useful to understand when debugging index contents.

| Item | Description |
|------|-------------|
| `QUAD_KEY_LEN` = 48, `QUAD_TUPLE_LEN` = 40 | Key width; the quad prefix shared by every version |
| `Order` | `Spog`, `Sopg`, `Psog`, `Posg`, `Ospg`, `Opsg`, `Gspo`, `Gpos`; `Order::ALL` |
| `order.cf()` | The column family of that order |
| `order.encode(&QuadKey) -> [u8; 48]` / `order.decode(&[u8]) -> QuadKey` | `QuadKey { s, p, o, g, tt }` |
| `order.prefix(s, p, o, g) -> KeyPrefix` | Bound leading slots, stopping at the first unbound one (stack-allocated) |
| `order.graph_of(key) -> GraphId` | Read the graph slot without decoding |
| `key_tt(key)` | Transaction time from the last 8 bytes |
| `value_object(&Value) -> NodeId` | A property's object slot (its content hash) |
| `encode_tri` / `decode_tri`, `encode_epa_key`, `encode_epo_key`, `encode_pea_key` | Ancillary CF keys (all carry the graph) |
| `keys::v2` | Read-only v2 layouts, used by the migration |

---

### Value codec (`polargraph_storage::codec`)

| Function | Description |
|----------|-------------|
| `encode_relation(edge_id, temporal) -> Vec<u8>` | 33-byte relation value |
| `encode_property(value, temporal) -> Result<Vec<u8>, StorageError>` | 17+N property value (vectors binary) |
| `encode_property_ref(temporal) -> Vec<u8>` | 17-byte reference to an out-of-line value |
| `encode_blob(value)` / `decode_blob(bytes)` | `blob` CF payload |
| `decode_value(bytes) -> Result<DecodedValue, StorageError>` | `Relation`, `Property` or `PropertyRef` |
| `valid_time(bytes) -> Option<(Timestamp, Timestamp)>` | `(vt_start, vt_end)` without a full decode |
| `with_vt_end(bytes, vt_end)` | Copy with `vt_end` replaced (closing versions) |

The decoded `temporal.tt` is always `Timestamp(0)` — the caller fills it in
from the index key. A `PropertyRef`'s value is `blob[key object slot]`.

`GraphScope` (`polargraph_storage::GraphScope`):

```rust
pub enum GraphScope {
    Union,             // every graph, each (s, p, o) once
    One(GraphId),
    Set(Vec<GraphId>), // GraphScope::set(vec) sorts and dedupes
    Named,             // every named graph (not the default graph)
}
```

---

## `polargraph-query`

Pattern-based query evaluation, Cypher frontend, aggregations, and view
projection.

### `GraphTerm` (`polargraph_query::datalog`)

```rust
pub enum GraphTerm {
    Union,              // default
    Default,            // default graph only
    Bound(GraphId),
    Var(String),        // named graphs; binds the graph IRI node
    Set(Vec<GraphId>),
}

VarPattern { subject, predicate: Some("pred".into()), object, ..VarPattern::new() }
    .graph(GraphTerm::Var("g".into()))
```

Once a graph variable is bound, later patterns using it are restricted to
that graph. Pending-transaction writes and rule-derived facts only match
`Union` patterns; `max_hops` patterns ignore the graph term.

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
quad-index scan.

---

## `polargraph-server` — gRPC RPCs

Service: `polargraph.v1.PolarGraphService`

Proto source: `crates/polargraph-server/proto/polargraph.proto`

---

### `Insert` — graph, write mode and IRI bindings

`InsertRequest.graph` (`string`) names the graph (IRI) every triple and
annotation in the request goes to; it is interned on first use. Empty means
the default graph. REST `POST /insert` accepts the same as `"graph"`.

`PropertyTriple.mode` (`PropertyWriteMode`) says how a property treats other
values of the same `(subject, predicate, graph)`:

| Value | Behaviour |
|---|---|
| `PROPERTY_WRITE_MODE_AUTO` (0, default) | `REPLACE` for an open-ended value, `ADD` for a closing write — pre-v3 behaviour |
| `PROPERTY_WRITE_MODE_REPLACE` | Close every other open value, then write this one |
| `PROPERTY_WRITE_MODE_ADD` | Write alongside existing values |

REST `/import/rdf`, SPARQL `INSERT DATA` and `INSERT` templates write with
`ADD`, so multi-valued RDF properties survive.

`InsertRequest.iris` (`repeated string`) lists IRIs of nodes written in the
request. They are recorded in the IRI dictionary in the same commit (or
buffered into the open transaction when `tx_id` is set). Each IRI names
`iri_to_node_id(iri)`, so a client can't attach a name to the wrong node;
`urn:uuid:` IRIs are ignored. A request may carry only IRIs. Empty strings are
`INVALID_ARGUMENT`; an IRI that collides with a different stored IRI is
`ALREADY_EXISTS`.

### `ResolveIris`

```
rpc ResolveIris(ResolveIrisRequest) returns (ResolveIrisResponse)
```

`nodes` (≤ 10 000) → `iris`, one per node in request order: the stored IRI,
or `urn:uuid:<id>` when the node has none.

### `Query` — graph terms and dataset

`VarPattern.graph` (`GraphTerm`, optional) scopes a pattern:

```proto
message GraphTerm {
    oneof kind {
        bool     default_graph = 1;  // the default graph
        string   iri           = 2;  // one named graph
        string   var           = 3;  // graph variable (named graphs)
        GraphSet set           = 4;  // any of these graphs
    }
}
```

Unset = union of all graphs. `QueryRequest.graphs` (`repeated string`) is the
dataset for patterns without a graph term. Unknown IRIs match nothing. A
graph variable's binding is the graph IRI's NodeId (resolve it with
`ResolveIris`). `QueryStream` and `ExplainQuery` accept the same fields.

### Named-graph RPCs

| RPC | Request → Response | Notes |
|---|---|---|
| `CreateGraph` | `{iri, metadata: [GraphMetadata{predicate, value}]}` → `{graph: GraphInfo}` | Idempotent; metadata predicates replaced. Primary only |
| `ListGraphs` | `{filter: [GraphMetadata], include_system}` → `{graphs: [GraphInfo]}` | Filter = all metadata pairs must match |
| `GraphStats` | `{iri}` → `{iri, live_quads, last_write_tt}` | Empty IRI = default graph |
| `CopyGraph` | `{source, target, clear_target}` → `{quads}` | `COPY` (`clear_target`) or `ADD`; target interned. Primary only |
| `MoveGraph` | `{source, target}` → `CopyGraphResponse` | Copy, then drop source. Primary only |
| `DropGraph` | `{iri}` → `{quads_closed}` | Bitemporal close; history stays queryable. Primary only |
| `ExportGraph` | `{iri, all_graphs}` → stream `ExportGraphChunk{quads: [ExportedQuad]}` | Live relation/property quads; `ExportedQuad{subject, predicate, node \| value, graph}` |

`GraphInfo` = `{iri, id, metadata}`. A named graph that
doesn't exist is `NOT_FOUND`.

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
| `POST` | `/graphs` | `CreateGraph` |
| `GET` | `/graphs?include_system=` | `ListGraphs` |
| `GET` | `/graphs/stats?iri=` | `GraphStats` |
| `POST` | `/graphs/copy` | `CopyGraph` |
| `POST` | `/graphs/move` | `MoveGraph` |
| `DELETE` | `/graphs?iri=` | `DropGraph` |
| `GET` | `/graphs/export?iri=\|all=true` | `ExportGraph` |

### Named graphs over REST

Graph IRIs go in the body or the `iri` query parameter, never the path.

- `POST /graphs` — `{"iri": "urn:g:p1", "metadata": {"cb:status": "Proposed"}}`
  (values use the usual property JSON encoding).
- `POST /graphs/copy` — `{"source": "...", "target": "...", "clear_target": true}`
  (`clear_target` defaults to `true` = SPARQL `COPY`; `false` = `ADD`).
- `POST /graphs/move` — `{"source": "...", "target": "..."}`.
- `GET /graphs/export` — `iri` (empty = default graph) or `all=true`,
  `deskolemize`. N-Quads by default, TriG for `Accept: application/trig`;
  a single graph also as N-Triples / Turtle / JSON-LD. `all=true` with a
  triple format is 406.

`POST /query` patterns take an optional fourth token for the graph:
`?s :knows ?o @default`, `?s :knows ?o @<urn:g:p1>`, `?s :knows ?o @?g`.
The body's `"graphs": ["urn:g:p1", ...]` sets the dataset for patterns
without a suffix.

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

### `GetEdgeIdsByTriple`

```
rpc GetEdgeIdsByTriple(GetEdgeIdsByTripleRequest) returns (GetEdgeIdsByTripleResponse)
```

Resolves one or more base relation triples `(subject, predicate, object)` to their
stable `EdgeId` UUIDs. Used by the SPARQL-star translator to map object-position
quoted triples (`?s :p << :a :b :c >>`) to edge annotation lookups, and available
directly for callers that need the edge ID for a known triple.

**Request fields:** `repeated TripleRef { subject_id (bytes), predicate (string), object_id (bytes) }`.

**Response fields:** `repeated EdgeIdResult { triple_ref, edge_id (bytes) }` — one entry per
resolved triple; triples not found in the store are omitted rather than returned as errors.

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
| `object_id` | `bytes` | Optional 16-byte object: only relations to this node are closed (properties untouched) |
| `value` | `Value` | Optional: only properties with exactly this value are closed (relations untouched). Mutually exclusive with `object_id` |

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
`{"ok": bool, "inserted": N, "deleted": N, "failed": N}`. Each deleted quad
closes exactly that triple. IRIs map to nodes the same way as `/import/rdf`
(`urn:uuid:` IRIs keep their UUID, others are hashed).

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

---

## REST gateway — RDF interoperability endpoints

### `POST /import/rdf`

Import RDF triples into PolarGraph. Accepts N-Triples, Turtle, or JSON-LD
determined by the `Content-Type` header.

| Content-Type | Format |
|---|---|
| `application/n-triples` | N-Triples |
| `text/turtle` | Turtle |
| `application/ld+json` | JSON-LD (`@graph` array) |
| `application/n-quads` | N-Quads — each quad into its graph |
| `application/trig` | TriG — each quad into its graph |
| `application/rdf+xml` | Not supported — returns 415 |
| `application/owl+xml` | Not supported — returns 415 |

Triples are inserted in batches of 1 000. Relations become `RelationTriple`s;
literals become `PropertyTriple`s. IRIs map to `NodeId`s via deterministic
xxHash3-128 (`uri_to_node_id`).

Blank nodes are skolemized per import
(`{skolem-base}/.well-known/genid/{import_id}/{label}`), so the same label in
two imports is two different nodes.

| Query parameter | Description |
|---|---|
| `import_id` | Optional. 1–128 chars of `[A-Za-z0-9._~-]`. Re-importing with the same id maps blank nodes to the same NodeIds. Defaults to a fresh UUIDv7. |
| `graph` | Optional. Graph IRI for triple formats (N-Triples, Turtle, JSON-LD); default graph otherwise. Quad formats carry their own graphs; blank-node graph names are skolemized. |

**Response:**
```json
{ "imported": 42, "total_parsed": 42, "duration_ms": 18,
  "import_id": "01928c7e-5b1a-7f3e-9d0c-2a4b6c8d0e1f" }
```

### `POST /import/subgraph`

Semantic alias for `POST /import/rdf`. Accepts the same formats and behaves
identically; provided to make PolarGraph-to-PolarGraph subgraph transfer
workflows self-documenting.

### `GET /export/jsonld`

Query parameters:

| Parameter | Required | Description |
|---|---|---|
| `subject` | No | Subject URI to export |
| `predicates` | No | Comma-separated predicate IRIs to include |
| `deskolemize` | No | `true` renders skolem IRIs as blank nodes (`_:{import_id}_{label}`) |

### `POST /export/jsonld`

Body:
```json
{
  "subjects":   ["http://example.org/Alice"],
  "predicates": ["http://schema.org/knows"],
  "view_id":    "optional-view-id",
  "deskolemize": false
}
```

Both GET and POST return a JSON-LD document:

```json
{
  "@context": { "xsd": "http://www.w3.org/2001/XMLSchema#", "rdf": "...", "rdfs": "...", "owl": "..." },
  "@graph": [
    {
      "@id": "http://example.org/Alice",
      "http://schema.org/knows": { "@id": "http://example.org/Bob" },
      "http://schema.org/age":   { "@value": "30", "@type": "xsd:integer" }
    }
  ]
}
```

### `GET /export/subgraph`

Query parameters:

| Parameter | Required | Description |
|---|---|---|
| `subjects` | Yes | Comma-separated subject UUIDs or URIs |
| `predicates` | No | Comma-separated predicate IRIs |
| `deskolemize` | No | `true` renders skolem IRIs as blank nodes (`_:{import_id}_{label}`) |

Nodes are rendered under the IRI stored in the IRI dictionary (`ResolveIris`),
falling back to `urn:uuid:` — the same applies to SPARQL SELECT results
(`uri` values, including inside `GROUP_CONCAT`), CONSTRUCT/DESCRIBE and the
JSON-LD export.

Response format negotiated via `Accept` header:

| Accept | Format |
|---|---|
| `application/ld+json` | JSON-LD |
| `text/turtle` | Turtle |
| `application/n-quads` | N-Quads — each triple with its graph |
| `application/trig` | TriG — one block per named graph |
| `application/n-triples` (default) | N-Triples |

Quad formats query the default graph and each named graph separately, so a
triple in two graphs appears twice; triple formats return the union.
Includes edge annotations as property triples on the edge NodeId.

### `GET /schema/rdf`

Exports all registered node types and edge types as OWL/RDFS Turtle.
Node types become `owl:Class` declarations; property fields become
`owl:DatatypeProperty` with `rdfs:domain`/`rdfs:range`; edge types become
`owl:ObjectProperty` with domain and range.

IRI namespaces:
- `urn:polargraph:type:<TypeName>` — node types
- `urn:polargraph:prop:<TypeName>/<field>` — property fields
- `urn:polargraph:rel:<predicate>` — edge predicates

### `POST /schema/rdf`

Body: OWL/RDFS Turtle (or N-Triples). Parses `owl:Class`,
`owl:DatatypeProperty`, and `owl:ObjectProperty` declarations (with
`rdfs:domain`/`rdfs:range`) and calls `RegisterNodeType` / `RegisterEdgeType`
for each discovered type.

**Response:**
```json
{ "node_types_registered": 2, "edge_types_registered": 1 }
```
