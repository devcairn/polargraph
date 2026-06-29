# PolarGraph DB Engine — Engineering Roadmap

This document describes planned feature areas for PolarGraph, grounded in the
current implementation.  Each section covers the gap, the proposed design,
ordered implementation tasks, complexity estimates, and open design questions.

---

## Summary table

| Feature | Complexity | Priority | Notes |
|---------|-----------|----------|-------|
| [1. Full-text / trigram search](#1-full-text--trigram-search) | **M** | High | New `tri` CF; no schema change needed |
| [2. Geo / spatial index](#2-geo--spatial-index) | **L** | Medium | New `Value::Geo`, new `geo` CF, geohash covering |
| [3a. Python SDK](#3a-python-sdk) | **M** | High | grpcio-tools codegen; PyPI packaging |
| [3b. JavaScript / TypeScript SDK](#3b-javascript--typescript-sdk) | **M** | High | ts-proto codegen; Node + grpc-web support |
| [3c. Go SDK](#3c-go-sdk) | **S** | Medium | protoc-gen-go; idiomatic context support |
| [4. Transactions over the wire](#4-transactions-over-the-wire) | **L** | High | BeginTx/CommitTx/RollbackTx RPCs; server-side state |
| [5a. Visual graph explorer](#5a-visual-graph-explorer) | **M** | Medium | Force-directed graph in ui.html |
| [5b. Query history](#5b-query-history) | **S** | Low | In-memory ring buffer per session |
| [5c. Schema diagram](#5c-schema-diagram) | **S** | Low | Mermaid.js ERD from registry |
| [5d. Diagnostics RPCs](#5d-diagnostics-rpcs) | **M** | Medium | ShowIndexes, ShowStats; REST + UI |

Complexity: **S** = days, **M** = 1–2 weeks, **L** = 2–4 weeks, **XL** = month+.

---

## 1. Full-text / trigram search

### Gap

The only text-matching path today is exact equality: `WHERE a.name = "Alice"`
compiles to a `VarPattern` with a bound object slot and a single SPO/POS scan.
Substring search (`CONTAINS`), prefix search (`STARTS WITH`), and regular
expression matching (`=~`) are rejected with `INVALID_ARGUMENT`.  Large graph
workloads routinely need "find all nodes whose `description` contains the word
`fraud`" without doing a full table scan.

### Proposed design

#### Trigram index (`tri` CF)

At insert time, for every `Triple::Property` whose value is `Value::Text`,
extract all 3-grams from the text and write one key per 3-gram into a new
`tri` column family.

Key layout (23 bytes, fixed-width):
```
[trigram: 3 bytes (UTF-8, padded if necessary)]
[pred_id: 4 bytes LE]
[subject_id: 16 bytes]
```

Value: empty (`&[]`).

This layout supports prefix scans of the form `[trigram]` to retrieve all
(predicate, subject) pairs that contain a given 3-gram.  The predicate slot
allows the planner to narrow the scan when the Cypher `WHERE` clause names a
specific property (`a.description CONTAINS "fraud"` vs a predicate-wildcard
search).

**Insert path** (`polargraph-storage::store::TripleStore`):
Extend `insert()` to call `extract_trigrams(text) -> Vec<[u8; 3]>` (pads
the last 1- or 2-character gram with `\0`) and append the resulting keys to
the existing `WriteBatch` atomically with all 6 hexastore CFs.

**Query path** (`polargraph-query`):
1. Extract query 3-grams from the search string.
2. For each trigram, prefix-scan `tri` CF to retrieve candidate
   `(pred_id, subject_id)` pairs.
3. Intersect candidate sets across all trigrams (the smallest set is the
   result; single-gram strings fall back to a full `tri` scan filtered by
   predicate).
4. Resolve candidate subjects through the MVCC snapshot (confirm the
   property triple still exists at the current snapshot timestamp via
   `scan_by_subject_predicate`).
5. Post-filter with the exact predicate check:
   `CONTAINS` → `str.contains()`, `STARTS WITH` → `str.starts_with()`,
   `=~` → compiled `regex::Regex`.

#### Cypher surface

Extend `WhereClause` (in `polargraph-query::cypher`) with three new variants:

```rust
WhereClause::Contains   { var, prop, value: String }
WhereClause::StartsWith { var, prop, value: String }
WhereClause::Regex      { var, prop, pattern: String }
```

`STARTS WITH` can skip the trigram index entirely and use a RocksDB range scan
if the predicate is bound (prefix-scan the PSO CF from `[pred_id][sentinel]`
then compare the decoded text value).

`=~` always falls through to the trigram pre-filter (to reduce the scan
surface) followed by regex post-filter.

#### Aggregation module note

The new `aggregation.rs` (currently untracked) introduces `AggFunc`,
`AggregationSpec`, and `OrderSpec`.  The trigram search results feed into
this pipeline unchanged — a `CONTAINS` clause produces a `Bindings` set, and
aggregation/ordering apply downstream.

### Implementation tasks

1. **`polargraph-core`**: add `extract_trigrams(s: &str) -> Vec<[u8; 3]>`
   utility function.  Handle Unicode by operating on UTF-8 bytes with 3-byte
   windows (alternative: char-level 3-grams — see open questions).
   _Complexity: S_

2. **`polargraph-storage::cf`**: add constant `pub const TRI: &str = "tri"`;
   add to `ALL` slice.  _Complexity: S_

3. **`polargraph-storage::store`**: register `tri` CF in `open_cf_descriptors`,
   extend `WriteBatch` construction in `insert()` to write trigram keys,
   add `search_trigrams(pred_id: u32, trigrams: &[[u8; 3]]) -> Vec<NodeId>`.
   _Complexity: M_

4. **`polargraph-storage::sst_import`**: extend `SstImporter` to also write
   trigram keys when ingesting `Text` property triples.  _Complexity: S_

5. **`polargraph-query::cypher`**: add `Contains`, `StartsWith`, `Regex`
   variants to `WhereClause`; extend the parser to recognise
   `WHERE a.prop CONTAINS "x"`, `STARTS WITH "x"`, `=~ "regex"`.
   _Complexity: M_

6. **`polargraph-query`**: add `apply_text_filters` function that resolves
   trigram candidates from storage, intersects sets, confirms via snapshot,
   and applies the exact post-filter.  Wire into the Cypher execution path
   after `execute_query` / `execute_recursive`.  _Complexity: M_

7. **`polargraph-server::proto`**: add `CONTAINS`, `STARTS_WITH`, `REGEX`
   filter variants on `VarPattern` or as standalone `TextFilter` message.
   _Complexity: M_

8. **`polargraph-server::convert`**: round-trip new proto variants to
   `WhereClause`.  _Complexity: S_

9. **`polargraph-rest`**: parse `contains`, `starts_with`, `regex` fields
   in the pattern string format or as a dedicated JSON field on `/query`.
   _Complexity: S_

10. **Tests**: unit tests for `extract_trigrams`; storage integration tests
    for `tri` CF round-trip; query integration tests for all three `WHERE`
    forms including Unicode strings and empty-result cases.  _Complexity: M_

11. **`CLAUDE.md` / `docs/architecture.md`**: document the `tri` CF layout,
    key encoding, and the text-filter query pipeline.  _Complexity: S_

### Open design questions

- **UTF-8 vs char-level 3-grams.** Byte-level 3-grams are simpler and cheaper
  to extract but produce false sharing across multi-byte character boundaries
  (e.g. two unrelated Cyrillic words may share a 3-gram).  Char-level grams
  are more precise but require variable-length encoding — need a separator
  byte or length prefix to keep the key fixed-width.

- **MVCC-awareness of the `tri` CF.** The current proposal writes trigrams
  on insert without a `tt` stamp and relies on snapshot confirmation in step 4
  to handle MVCC.  This is correct but means old trigram entries for
  superseded property values permanently consume space until compaction.
  An alternative is to stamp `tri` keys with `tt` and filter during the
  trigram scan — same overhead as the hexastore, but doubles the compaction
  scope.  Recommended starting point: start without `tt` stamp, add it in a
  schema migration if the false-positive rate proves costly.

- **Trigram index for non-indexed text.** Should trigrams be written for
  every `Text` property, or only for predicates that have been explicitly
  declared "full-text indexed" via `FieldDef`?  Opt-in reduces write
  amplification for short lookup fields (e.g. `__type`) that will never be
  searched with `CONTAINS`.

- **Update path.** Re-inserting a property triple with a new `Text` value
  creates a new MVCC version but does not delete the old trigram entries for
  the previous value.  These become dead entries that are eliminated by the
  snapshot confirmation in step 4.  If a predicate value changes frequently,
  the `tri` CF accumulates stale entries.  Mitigations: (a) retention
  compaction already covers the hexastore — extend it to also scan `tri`
  and delete entries whose subject/predicate combination has no live snapshot
  triple; or (b) accept the staleness and document it.

- **`STARTS WITH` range scan vs trigram.** For ASCII-heavy workloads, a
  PSO range scan on `[pred_id][sentinel_object_prefix]` may outperform
  trigram intersection for prefix queries.  The planner should prefer the
  range-scan path when the predicate is bound and the prefix is at least 3
  characters (to avoid a full PSO scan).

---

## 2. Geo / spatial index

### Gap

There is no way to store or query geographic coordinates.  Nodes with
latitude/longitude attributes must store them as two separate `Float`
properties and any proximity query requires a full scan with client-side
Haversine computation.

### Proposed design

#### `Value::Geo`

Add a new variant to `polargraph_core::value::Value`:

```rust
Value::Geo { lat: f64, lon: f64 }
```

This follows the existing `Value::Vector` pattern: a dedicated discriminant
byte (`0x04`) and binary encoding in the codec to avoid JSON overhead:

```
Geo  [0x04][lat: 8 BE][lon: 8 BE]   = 17 bytes
```

Add `FieldKind::Geo` to `polargraph-core::schema` and update `FieldKind::matches`.

#### Geohash index (`geo` CF)

For every `Triple::Property` with `Value::Geo`, write one key per geohash
precision level (levels 4–8, covering roughly 40km → 150m cells) into a new
`geo` column family.

Key layout (fixed-width per precision, 1 + 4 + 16 = 21 bytes + 1 byte length prefix):
```
[precision: 1 byte (4..=8)]
[geohash_chars: precision bytes, ASCII, right-padded to 8 bytes]
[pred_id: 4 bytes LE]
[subject_id: 16 bytes]
```

Storing multiple precisions at insert time lets the query planner choose the
coarsest cell that fully contains the bounding box for a given radius, then
refine with Haversine post-filter.  A single-precision write is wrong because
a circle centred on a cell boundary spans two or more cells.

**Query path:**
1. `DISTANCE(a.location, lat, lon) < metres` → compute the set of geohash
   cells at the appropriate precision that fully cover the circle's bounding
   box (typically 4–9 cells at the chosen precision).
2. For each covering cell, prefix-scan the `geo` CF at `[precision][cell_prefix][pred_id]`.
3. Union candidate subject IDs.
4. Fetch each candidate's `Value::Geo` via `scan_by_subject_predicate`.
5. Post-filter with Haversine to remove false positives from cell boundary
   effects.

For the covering cell computation, use the `geohash` crate (pure Rust,
no C dependencies, ~5 KB compiled) or implement the 2-4 required geohash
primitives inline (encode, decode, neighbors).

#### Cypher surface

Add a new `WhereClause` variant:

```rust
WhereClause::GeoDistance {
    var: String,
    prop: String,
    lat: f64,
    lon: f64,
    metres: f64,
}
```

Parsed from: `WHERE DISTANCE(a.location, 37.77, -122.41) < 1000`.

#### HNSW 2D trade-off discussion

PolarGraph's existing HNSW index uses **cosine distance**, which is well-suited
for high-dimensional embedding spaces but meaningless for geographic coordinates
where direction (angle) is irrelevant and absolute magnitude should be
normalised out.  Using HNSW for geo would require:

1. Adding an L2 distance metric to `HnswIndex` (non-trivial change to a
   core data structure shared by all spaces).
2. Converting (lat, lon) to Cartesian coordinates on a unit sphere to make
   L2 approximate great-circle distance.
3. Accepting that HNSW recall degrades for near-duplicate distances in 2D
   (the graph structure is inefficient at low dimensions — HNSW was designed
   for ≥32 dims).

Conclusion: the geohash approach is more accurate, does not require changes
to the HNSW algorithm, and supports exact radius queries.  HNSW 2D is an
interesting experimental extension but is out of scope for this feature.

### Implementation tasks

1. **`polargraph-core::value`**: add `Value::Geo { lat: f64, lon: f64 }`.
   Update `Serialize`/`Deserialize` (tagged JSON `{"geo": {"lat": 37.77, "lon": -122.41}}`).
   _Complexity: S_

2. **`polargraph-core::schema`**: add `FieldKind::Geo`; update `matches()`.
   _Complexity: S_

3. **`polargraph-storage::codec`**: add discriminant `0x04`; implement
   `encode_geo` / `decode_geo` using 8-byte big-endian doubles.  Add
   round-trip test.  _Complexity: S_

4. **`polargraph-storage::cf`**: add `pub const GEO: &str = "geo"`;
   add to `ALL`.  _Complexity: S_

5. **`polargraph-storage::store`**: register `geo` CF; in `insert()`, when
   `Value::Geo` is detected, encode geohash at precisions 4–8 and add all 5
   keys to the `WriteBatch`.  Add `search_geo(pred_id, covering_cells) -> Vec<NodeId>`.
   _Complexity: M_

6. **`polargraph-core` or `polargraph-storage`**: add `geohash` dependency;
   implement `covering_cells(lat, lon, radius_m, precision) -> Vec<String>`.
   _Complexity: M_

7. **`polargraph-query::cypher`**: add `GeoDistance` variant to `WhereClause`;
   parse `WHERE DISTANCE(var.prop, lat, lon) < metres`.  _Complexity: S_

8. **`polargraph-query`**: add `apply_geo_filter` that resolves covering
   cells, scans `geo` CF, fetches candidate values, and applies Haversine.
   Wire into Cypher execution.  _Complexity: M_

9. **`polargraph-server::proto`**: add `GeoValue` message
   (`double lat`, `double lon`); add `geo_val` field to the `Value` oneof;
   add `GeoDistanceFilter` message to `QueryRequest` or `VarPattern`.
   _Complexity: M_

10. **`polargraph-server::convert`**: round-trip `Value::Geo` ↔ proto.
    _Complexity: S_

11. **`polargraph-storage::sst_import`**: extend bulk import to write `geo`
    CF entries for `Value::Geo` triples.  _Complexity: S_

12. **`polargraph-storage::compaction`**: extend retention scan to clean up
    `geo` CF entries for deleted property triples (same dead-entry problem
    as `tri` CF — see § 1 open questions).  _Complexity: S_

13. **Tests**: unit tests for codec round-trip; Haversine accuracy at known
    coordinates; storage integration test for radius query returning expected
    subset; Cypher integration test end-to-end.  _Complexity: M_

### Open design questions

- **Bounding box vs circular radius.** Geohash cells are rectangular, so
  a radius query is always an approximation refined by Haversine.  Should the
  API also expose `BBOX(a.loc, min_lat, min_lon, max_lat, max_lon)` as a
  complementary exact-match form that avoids post-filtering?

- **Precision selection heuristic.** The optimal geohash precision depends on
  the query radius.  The mapping used in practice: precision 4 ≈ 40 km,
  5 ≈ 5 km, 6 ≈ 1.2 km, 7 ≈ 150 m, 8 ≈ 38 m.  The planner should choose
  the smallest precision where the cell is smaller than the query radius, to
  limit false positives.  Queries with `< 100 m` radii may need precision 8
  which produces very dense geohash keys.

- **Single precision vs multi-precision.** Storing 5 keys per geo property
  is a 5× write amplification over a single-precision design.  An alternative
  is to store only precision 6 (covers most "find nearby" use cases) and fall
  back to a wider scan for large radii.  The tradeoff: fewer write ops vs
  less query flexibility.

- **Geo in the hexastore.** `Value::Geo` stored in the hexastore object slot
  uses the property sentinel (`0xFF × 16`) like all scalar properties, so it
  is queryable via existing patterns.  The `geo` CF is supplementary — it
  only accelerates radius queries.  Exact-equality Geo matches (`WHERE a.loc = ...`)
  would be unusual in practice but do work via the existing equality path.

---

## 3. Client SDKs

### Gap

There are no official client libraries.  Callers must use `grpcurl`, hand-roll
proto stubs, or use the REST gateway for the six HTTP endpoints.  For any
language-native development experience, clients need generated type-safe
bindings and convenience wrappers.

### 3a. Python SDK

**Directory:** `clients/python/polargraph/`

#### Design

```
clients/python/
  pyproject.toml
  polargraph/
    __init__.py          # re-exports PolarGraphClient, AsyncPolarGraphClient
    client.py            # synchronous wrapper
    async_client.py      # asyncio wrapper
    proto/               # generated stubs (committed or built at install time)
      polargraph_pb2.py
      polargraph_pb2_grpc.py
    types.py             # typed dataclasses mirroring proto messages
  tests/
    test_client.py
  README.md
```

`PolarGraphClient` wraps every RPC with a Pythonic interface:

```python
client = PolarGraphClient("localhost:50051", api_key="...")

# Insert a node property
client.insert_property(node_id, "name", "Alice")

# Insert a relation
client.insert_relation(alice_id, "knows", bob_id)

# Run a conjunctive query
bindings = client.query([("?a", ":knows", "?b"), ("?a", ":name", "?n")])

# Cypher
rows = client.cypher('MATCH (a:Person)-[:knows]->(b) RETURN b LIMIT 10')

# Vector insert + search
client.insert_vector(node_id, [0.1, 0.2, ...], space="embeddings")
neighbors = client.search_vector([0.1, 0.2, ...], k=5, space="embeddings")

# Streaming: yields Bindings dicts
for row in client.stream_query([("?a", ":knows", "?b")]):
    print(row)
```

`AsyncPolarGraphClient` mirrors the same surface with `async def` methods
using `grpc.aio`.

**Packaging:** `pyproject.toml` with `[build-system]` using `hatchling`,
`grpcio` + `grpcio-tools` as build-time dependencies, `grpcio` + `protobuf`
as runtime dependencies.  The generated stubs are committed to the repo so
`pip install` does not require `protoc` on the user's machine.  A
`scripts/regen_proto.sh` script regenerates stubs from `polargraph.proto`
for maintainers.

**PyPI packaging:** publish as `polargraph-client` using
`pypa/gh-action-pypi-publish` on GitHub Actions on version tag.

#### Implementation tasks

1. Scaffold `clients/python/` directory with `pyproject.toml`, `README.md`,
   stub regeneration script.  _Complexity: S_

2. Run `grpc_tools.protoc` on `polargraph.proto`; commit generated stubs.
   _Complexity: S_

3. Implement synchronous `PolarGraphClient` with all RPCs, TLS constructor
   (`PolarGraphClient.with_tls(ca_cert_path=...)`), and retry helper.
   _Complexity: M_

4. Implement `AsyncPolarGraphClient` using `grpc.aio`.  _Complexity: S_

5. Write `types.py` typed dataclasses for `BindingsRow`, `VectorResult`,
   `ScoredBinding`, `NodeTypeDef`, `EdgeTypeDef`.  _Complexity: S_

6. Tests: integration tests against a live `polargraphd` (skip if
   `POLARGRAPH_TEST_ADDR` unset).  _Complexity: M_

7. `pyproject.toml` extras: `[async]` pulls in `grpcio >= 1.54`.  _Complexity: S_

8. CI: add `clients/python` to GitHub Actions matrix.  _Complexity: S_

**Total complexity: M** (1–2 weeks including tests and packaging)

### 3b. JavaScript / TypeScript SDK

**Directory:** `clients/js/`

#### Design

Target: Node.js (primary) + browser via `grpc-web` (secondary, same generated
types, different transport).

```
clients/js/
  package.json
  tsconfig.json
  src/
    index.ts             # re-exports PolarGraphClient
    client.ts            # Node.js gRPC client
    web_client.ts        # grpc-web browser client
    types.ts             # TypeScript interfaces
    proto/               # generated by ts-proto (committed)
  tests/
  README.md
```

Use `ts-proto` for code generation rather than `@grpc/proto-loader`: it
produces idiomatic TypeScript interfaces and avoids runtime reflection.

```typescript
const client = new PolarGraphClient({
  address: "localhost:50051",
  apiKey: "...",
  tls: false,
});

const { bindings } = await client.query({
  patterns: [{ s: "?a", p: ":knows", o: "?b" }],
});

const results = await client.searchVector({
  space: "embeddings",
  queryVector: [0.1, 0.2, ...],
  k: 5,
});
```

Browser entry point uses `@improbable-eng/grpc-web` transport; Node.js entry
point uses `@grpc/grpc-js`.  The `PolarGraphClient` constructor accepts a
`transport` option; the default is auto-detected from `typeof window`.

**npm packaging:** published as `@polargraph/client` (scoped package).
Dual CJS + ESM output via `tsup`.  Types bundled.

#### Implementation tasks

1. Scaffold `clients/js/` with `package.json`, `tsconfig.json`, `.npmignore`.
   _Complexity: S_

2. Run `ts-proto` codegen on `polargraph.proto`; commit generated files.
   _Complexity: S_

3. Implement `PolarGraphClient` (Node.js) with all RPCs plus TLS helper.
   _Complexity: M_

4. Implement `PolarGraphWebClient` using `grpc-web` transport.  _Complexity: M_

5. Write TypeScript union types and helper constructors for `Value`, `Term`,
   `PatternString`.  _Complexity: S_

6. Tests using `jest` + `ts-jest`; integration tests against live server.
   _Complexity: M_

7. Build pipeline: `tsup` bundles CJS + ESM; `package.json` exports map.
   _Complexity: S_

8. CI: add `clients/js` to GitHub Actions matrix.  _Complexity: S_

**Total complexity: M** (1–2 weeks)

### 3c. Go SDK

**Directory:** `clients/go/`

#### Design

```
clients/go/
  go.mod               # module: github.com/devcairn/polargraph/clients/go
  polargraph/
    client.go          # PolarGraphClient struct
    types.go           # idiomatic Go types wrapping proto
    proto/             # generated (committed)
  examples/
    quickstart/main.go
  README.md
```

Idiomatic Go: `context.Context` on every method, `Option` functional pattern
for construction, no global state.

```go
client, err := polargraph.NewClient("localhost:50051",
    polargraph.WithAPIKey("..."),
    polargraph.WithTLS(caCertPath),
)
defer client.Close()

resp, err := client.Query(ctx, []polargraph.Pattern{
    {S: "?a", P: ":knows", O: "?b"},
})
```

Use `google.golang.org/grpc` + `google.golang.org/protobuf` (not the older
`github.com/golang/protobuf`).  Connection pooling is handled by the gRPC
channel internally; document the `grpc.WithDefaultCallOptions` pattern for
callers that need to tune timeouts.

#### Implementation tasks

1. Scaffold `clients/go/go.mod`; run `protoc` with `protoc-gen-go` +
   `protoc-gen-go-grpc`; commit generated code.  _Complexity: S_

2. Implement `PolarGraphClient` with full RPC surface and option functions.
   _Complexity: M_

3. Write idiomatic Go wrapper types (`BindingsRow`, `VectorResult`, etc.)
   that avoid exposing raw proto structs in the public API.  _Complexity: S_

4. Quickstart example under `examples/quickstart/`.  _Complexity: S_

5. Tests using `testing` package; integration tests with build tag
   `//go:build integration`.  _Complexity: S_

6. CI: `go test ./...` in GitHub Actions.  _Complexity: S_

**Total complexity: S–M** (less than a week)

### Common across all SDKs

- `README.md` in each SDK directory: installation, quickstart (3–5 code
  examples), TLS configuration, connection-pooling notes.
- Proto regeneration script committed at `scripts/regen_clients.sh`.
- All three SDKs should be kept in sync with `polargraph.proto` — add a CI
  check that fails if the committed generated files are stale relative to
  the proto file.
- Versioning: SDK versions track the server's proto compatibility, not the
  Rust crate version.  Start at `v0.1.0` and follow semver independently.

---

## 4. Transactions over the wire

### Gap

The MVCC layer (`polargraph-storage::mvcc`) provides full snapshot-isolated
optimistic concurrency internally.  However, every gRPC `Insert` call is a
single auto-committed transaction.  Clients that need to insert multiple
logically related triples — or interleave reads and writes — must either
pack everything into one `Insert` batch (atomic but unbounded in size) or
accept that partial failures leave the graph in an intermediate state.  There
is no `BeginTransaction` / `CommitTransaction` surface.

### Proposed design

#### New RPCs

```protobuf
rpc BeginTransaction(BeginTransactionRequest)
    returns (BeginTransactionResponse);

rpc CommitTransaction(CommitTransactionRequest)
    returns (CommitTransactionResponse);

rpc RollbackTransaction(RollbackTransactionRequest)
    returns (RollbackTransactionResponse);
```

```protobuf
message BeginTransactionRequest {}

message BeginTransactionResponse {
    string tx_id = 1;   // UUID v4 string, opaque to client
}

message CommitTransactionRequest {
    string tx_id = 1;
}

message CommitTransactionResponse {
    int64 commit_ts = 1;   // transaction time assigned on commit
}

message RollbackTransactionRequest {
    string tx_id = 1;
}

message RollbackTransactionResponse {}
```

#### Joining existing RPCs to an open transaction

Extend `InsertRequest`, `QueryRequest`, `CypherQueryRequest`, and
`CypherWriteRequest` with an optional `string tx_id` field.  When set and
non-empty, the handler looks up the open `Transaction` object instead of
auto-beginning a new one.

- **Insert** with `tx_id`: appends triples to the in-memory write buffer of
  the named transaction; does not commit.  Returns `NOT_FOUND` if `tx_id` is
  unknown or expired.
- **Query** with `tx_id`: reads from the transaction's `read_ts` snapshot,
  including any triples buffered in the transaction's write buffer (write-your-
  own-reads within a transaction).  This requires a small extension to the
  snapshot evaluation: after the storage scan, overlay the in-memory write
  buffer of the transaction before returning results.
- **CypherQuery / CypherWrite** with `tx_id`: same semantics as Insert/Query.

#### Server-side state

```rust
// polargraph-server::service
struct OpenTransaction {
    tx: polargraph_storage::Transaction,
    created_at: std::time::Instant,
    last_used: std::time::Instant,
}

type TxMap = DashMap<String, Arc<tokio::sync::Mutex<OpenTransaction>>>;
```

`PolarGraphServer` gains a `tx_map: Arc<TxMap>` field.

A TTL background task (tokio interval, default 5 minutes idle) scans for
entries where `now - last_used > idle_timeout` and rolls them back.  On
graceful shutdown the CancellationToken stops this task; the shutdown drain
waits for any in-flight Commit/Rollback RPCs to complete, then discards all
remaining open transactions (effectively rolling them back).

`DashMap` is already used in the rate-limiter (`polargraph-server::rate_limit`)
so the dependency is already in `Cargo.toml`.

#### Conflict detection

Conflict detection is unchanged: it happens at commit time in the existing
`Transaction::commit()` path.  The gRPC `CommitTransaction` handler acquires
the per-transaction mutex, calls `tx.commit()`, and maps
`StorageError::WriteConflict` → `ABORTED` (the same status as the current
`Insert` conflict path).

#### Replica guard

`BeginTransaction`, `CommitTransaction`, and `RollbackTransaction` all call the
existing `check_not_replica()` guard before any logic.  Attempting to open a
transaction on a replica returns `FAILED_PRECONDITION`.

#### Interaction with rate limiting and auth

Rate limiting and auth middleware layers sit at the transport level and are
applied to each individual RPC call — no special handling needed.

#### Scope note

Distributed transactions across multiple PolarGraph primaries are explicitly
out of scope.  `BeginTransaction` is a single-primary construct; multi-primary
coordination would require Hybrid Logical Clocks and a two-phase commit
protocol, which is a separate project.

### Implementation tasks

1. **`polargraph-server::proto`**: add `BeginTransaction`, `CommitTransaction`,
   `RollbackTransaction` RPC signatures and messages; add `tx_id` field to
   `InsertRequest`, `QueryRequest`, `CypherQueryRequest`, `CypherWriteRequest`.
   _Complexity: S_

2. **`polargraph-server::service`**: add `tx_map: Arc<TxMap>` to
   `PolarGraphServer`; implement `begin_transaction` handler (generates UUID v4
   tx_id, inserts `OpenTransaction` into the map); implement
   `commit_transaction` (locks entry, calls `tx.commit()`, maps errors,
   removes from map); implement `rollback_transaction` (locks, drops, removes).
   _Complexity: M_

3. **`polargraph-server::service`**: extend `insert` handler to branch on
   `tx_id`: if set, append to buffered transaction instead of auto-committing.
   _Complexity: S_

4. **`polargraph-query`**: add `evaluate_with_overlay` function that
   supplements a `Snapshot` scan with in-memory triples from a write buffer.
   Used for read-your-own-writes within a transaction.  _Complexity: M_

5. **`polargraph-server::service`**: extend `query`, `cypher_query`,
   `cypher_write` handlers to branch on `tx_id`.  _Complexity: M_

6. **TTL background task**: tokio interval task in `main.rs`; configurable
   via `--tx-idle-timeout-ms` CLI flag (default 300 000 ms).  _Complexity: S_

7. **Graceful shutdown integration**: stop TTL task on CancellationToken;
   log discarded transactions.  _Complexity: S_

8. **`polargraph-server::config`**: add `tx_idle_timeout_ms` to `QueryConfig`
   struct; wire into config resolution chain.  _Complexity: S_

9. **`polargraph-rest`**: add `POST /tx/begin`, `POST /tx/{id}/commit`,
   `POST /tx/{id}/rollback` endpoints; pass `tx_id` field through existing
   `/query` and `/insert` endpoints.  _Complexity: M_

10. **`polargraph-server::telemetry`**: log `tx_id` as a structured field
    on Insert/Query/Commit/Rollback operations that carry one.  _Complexity: S_

11. **Prometheus metrics**: add `polargraph_open_transactions` gauge (current
    count of open transactions); increment on Begin, decrement on Commit/
    Rollback/TTL-expiry.  Expose in `ShowStats` RPC (see § 5d).  _Complexity: S_

12. **Tests**: unit tests for TTL expiry; integration tests for multi-insert
    transaction commit; conflict detection (two concurrent transactions writing
    same triple); rollback leaves no trace; replica rejection; 5-minute
    idle expiry (with accelerated clock).  _Complexity: M_

13. **`CLAUDE.md` / `docs/architecture.md`**: document the transaction state
    machine, TTL semantics, and the read-your-own-writes overlay.  _Complexity: S_

### Open design questions

- **Write-your-own-reads granularity.** The overlay approach (step 4) must
  handle both `Triple::Relation` and `Triple::Property` inserts and must
  honour the MVCC deduplication logic (if the same (S, P, O) is inserted twice
  within the same transaction, only the latest buffered version should be
  visible).  The existing `Transaction` struct buffers an unordered list of
  `Triple`; building a per-(S, P, O) overlay map is straightforward but needs
  careful testing.

- **Query snapshot within a transaction.** A `Query` with a `tx_id` should
  read from `read_ts` (the snapshot time at which `BeginTransaction` was
  called), not the current oracle.  This is consistent with the existing
  snapshot isolation model.  The server must thread the transaction's `read_ts`
  into the `Snapshot` construction rather than using `store.snapshot()`.

- **Transaction size limits.** A client that calls `Insert` with `tx_id` N
  thousand times before committing accumulates N write buffers in server RAM.
  Consider a per-transaction write buffer size limit (e.g. 100 MB) that returns
  `RESOURCE_EXHAUSTED` on overflow.

- **Interaction with WAL replication.** Transactions commit as a single
  `WriteBatch`; the WAL streamer replays `WriteBatch` entries.  No changes
  are needed for replication — the transaction is invisible to replicas until
  committed.

- **Distributed transactions** are out of scope.  Document explicitly in
  the proto comments that `tx_id` is only valid on the primary that issued it.

---

## 5. Operability improvements

### 5a. Visual graph explorer

#### Gap

The management UI (`polargraph-server::ui_api`, `ui.html`) has five tabs:
Query, Schema, Insert, Search, Status.  Query results are displayed as raw
JSON binding maps.  There is no visual representation of the graph structure,
making it hard to understand connectivity and spot patterns interactively.

#### Proposed design

Add a **Graph** tab to the SPA in `ui.html`.  When the user runs a query in
the Query tab (or directly in the Graph tab's own query box), the result
bindings are rendered as a force-directed graph using `vis-network` loaded
from a CDN.

**Rendering logic:**

1. Parse the `/api/query` JSON response.  Every binding value that is a
   UUID string is a potential node.
2. For each binding row with two UUID variables and an edge between them
   (e.g. `{a: "uuid1", b: "uuid2"}`), add `uuid1` and `uuid2` as nodes
   and draw a directed edge from `a` to `b`.
3. Single-variable bindings render as isolated nodes.
4. Node labels default to the last 8 chars of the UUID; clicking a node
   fetches its properties via `/api/query?s=<uuid>&limit=20` and renders
   them in a sidebar panel.
5. **Neighbourhood expansion**: clicking a node appends a new query pattern
   `(?a, *, <uuid>)` and re-renders the graph with the union of existing
   and new bindings.  A configurable depth limit (default 2 hops, max 5)
   prevents runaway expansion on dense nodes.

**CDN dependency**: `vis-network@9.x` from `unpkg.com` or `cdnjs` —
same CDN pattern already used in `ui.html` for other assets.  No build step.

**Layout persistence**: save node positions in `sessionStorage` so they are
stable between query re-runs in the same browser tab.

#### Implementation tasks

1. Add `<div id="graph-tab">` and the graph tab button to `ui.html`.
   Load `vis-network` from CDN.  _Complexity: S_

2. Implement `renderGraph(bindings)` JavaScript function: parse bindings,
   create `vis.DataSet` for nodes and edges, initialise `vis.Network`.
   _Complexity: M_

3. Implement node click handler: fetch properties, render sidebar, add
   expansion button.  _Complexity: M_

4. Implement depth-limited neighbourhood expansion: BFS via repeated
   `/api/query` calls, merge results into the existing graph dataset.
   _Complexity: M_

5. Add depth-limit control and a "clear graph" button to the UI.
   _Complexity: S_

6. Tests: existing UI integration tests (`tests/ui_integration_tests.rs`)
   test HTTP endpoints, not the SPA rendering — no new Rust tests needed.
   Add a comment in `ui.html` noting the CDN dependency and the fallback
   behaviour if the CDN is unreachable (degrade to the Query tab table view).
   _Complexity: S_

**Complexity: M**

### 5b. Query history

#### Gap

Every time the management UI is reloaded, the query box is empty.  There is
no way to recall a previous query without typing it again.

#### Proposed design

Store the last N query strings (default N = 20) in an in-memory ring buffer
per browser session.  Persist across page reloads using `localStorage` (never
sent to the server — the server side holds nothing).

**UX:** A dropdown arrow next to the query submit button opens a list of
recent queries.  Clicking an entry populates the query box.  The history is
shared across the Query and Graph tabs.

All state is client-side JavaScript; no server changes are required.

#### Implementation tasks

1. Add a `QueryHistory` JavaScript class backed by `localStorage`
   (`polargraph_query_history` key, JSON array of strings, capped at 20).
   _Complexity: S_

2. On query submit, prepend the query string to the history; on load,
   populate the dropdown.  _Complexity: S_

3. Add a "clear history" button.  _Complexity: S_

**Complexity: S**

### 5c. Schema diagram

#### Gap

The Schema tab lists node and edge types as JSON.  Understanding the overall
entity-relationship structure requires reading and mentally composing the raw
type definitions.

#### Proposed design

Auto-generate a Mermaid `erDiagram` from the `ListNodeTypes` and
`ListEdgeTypes` responses, rendered via `mermaid.js` loaded from CDN.

**Diagram generation logic:**

```javascript
function buildMermaid(nodeTypes, edgeTypes) {
    let out = "erDiagram\n";
    for (const nt of nodeTypes) {
        out += `  ${nt.type_name} {\n`;
        for (const f of nt.fields) {
            out += `    ${f.kind} ${f.name}\n`;
        }
        out += `  }\n`;
    }
    for (const et of edgeTypes) {
        const domain = et.domain || "ANY";
        const range  = et.range  || "ANY";
        out += `  ${domain} }|--o{ ${range} : "${et.predicate}"\n`;
    }
    return out;
}
```

Render the diagram inline in the Schema tab, below the existing type list.
Add a "Copy Mermaid source" button for documentation workflows.

#### Implementation tasks

1. Load `mermaid.js` from CDN in `ui.html` (already present on mermaid.js
   CDN; add `<script>` tag with `defer`).  _Complexity: S_

2. Implement `buildMermaid(nodeTypes, edgeTypes) -> String` in JavaScript.
   _Complexity: S_

3. Add an `<div id="schema-diagram">` to the Schema tab; call
   `mermaid.render("schema-mermaid", source)` when the Schema tab is activated
   or when the type list refreshes.  _Complexity: S_

4. Add "Copy Mermaid source" clipboard button.  _Complexity: S_

**Complexity: S**

### 5d. Diagnostics RPCs (`ShowIndexes`, `ShowStats`)

#### Gap

Operators today must log in to the host running `polargraphd` and use
`rocksdb_ldb` to inspect CF-level statistics, or write a custom Rust program
to open the DB in secondary mode.  There is no RPC or REST surface that
reports DB internals.

#### Proposed design

Two new RPCs that expose live storage and server statistics without requiring
DB access.

#### `ShowIndexes`

```protobuf
rpc ShowIndexes(ShowIndexesRequest) returns (ShowIndexesResponse);

message ShowIndexesRequest {}

message ShowIndexesResponse {
    repeated CfStats column_families = 1;
    repeated HnswSpaceStats vector_spaces = 2;
}

message CfStats {
    string name                  = 1;   // "spo", "tri", "geo", etc.
    int64  approx_key_count      = 2;   // RocksDB estimate-num-keys property
    int64  approx_size_bytes     = 3;   // GetApproximateSizes on full key range
}

message HnswSpaceStats {
    string name        = 1;   // space name
    uint32 dimensions  = 2;   // from VectorSpaceDef if registered, else 0
    uint64 node_count  = 3;   // number of nodes in the HNSW graph
    string storage_mode = 4;  // "memory" or "mmap"
}
```

Implementation: the handler iterates `cf::ALL` (plus `TRI`, `GEO` once
those CFs land), calls `db.property_value(cf, "rocksdb.estimate-num-keys")`
and `db.get_approximate_sizes(cf, &[(b"", b"\xff\xff")])`.  For HNSW spaces,
reads the `HashMap<String, HnswIndex>` under the existing `RwLock`.

#### `ShowStats`

```protobuf
rpc ShowStats(ShowStatsRequest) returns (ShowStatsResponse);

message ShowStatsRequest {}

message ShowStatsResponse {
    int64  oracle_ts             = 1;   // current MVCC oracle timestamp (µs)
    uint32 predicate_count       = 2;   // number of interned predicates
    uint64 live_sst_files        = 3;   // RocksDB num-live-versions
    int64  total_live_bytes      = 4;   // RocksDB total SST file size (bytes)
    int64  memtable_bytes        = 5;   // RocksDB cur-size-all-mem-tables
    uint32 open_transactions     = 6;   // current count of open wire transactions
    int64  uptime_secs           = 7;   // seconds since polargraphd start
}
```

#### REST endpoints

Expose as:
```
GET /indexes   → ShowIndexesResponse as JSON
GET /stats     → ShowStatsResponse as JSON
```

Added to both `polargraph-rest` and the management UI's `ui_api.rs` in-process API.

#### Management UI Status tab

Extend the existing Status tab to display the new fields from `ShowStats` (live
SST count, memtable size, open transactions) and the HNSW space table from
`ShowIndexes`.  The CF key count table provides a quick data-volume overview
without needing DB admin credentials.

#### Implementation tasks

1. **`polargraph-storage::store`**: expose `oracle_ts() -> i64`,
   `predicate_count() -> u32` accessors.  _Complexity: S_

2. **`polargraph-server::proto`**: add `ShowIndexes`, `ShowStats` RPC
   signatures and all message types.  _Complexity: S_

3. **`polargraph-server::service`**: implement `show_indexes` handler —
   iterate CFs, collect RocksDB property values, iterate HNSW map.
   Implement `show_stats` handler — oracle, predicate table, RocksDB
   properties, open tx count, uptime.  _Complexity: M_

4. **`polargraph-rest`**: add `GET /indexes` and `GET /stats` handlers
   proxying to the new RPCs.  _Complexity: S_

5. **`polargraph-server::ui_api`**: add `/api/indexes` and `/api/stats`
   routes that call into the in-process service.  _Complexity: S_

6. **`ui.html`**: extend Status tab to render CF table and HNSW spaces;
   call `/api/stats` on tab activate.  _Complexity: S_

7. **Tests**: integration tests for both RPCs returning valid (non-zero)
   values after inserting triples; REST proxy test for `/indexes`, `/stats`.
   _Complexity: S_

**Complexity: M**

---

## Appendix: Dependency and sequencing notes

### Cross-feature dependencies

- **Transactions over the wire (§4)** should land before the SDKs (§3) expose
  a transaction API surface — otherwise the SDK has to version bump when
  transactions are added.  The SDKs can ship first with a note that `tx_id`
  is reserved for a future release.

- **Full-text search (§1) and Geo (§2)** are both new column families.  If
  both are implemented concurrently, a single schema migration (`v3`) can
  register both CFs and avoid two separate startup migrations.

- **ShowIndexes (§5d)** will naturally report `tri` and `geo` CFs once they
  exist.  No extra work is needed — adding the CF names to `cf::ALL` is
  sufficient for the CF iteration loop.

- **Query history (§5b) and Schema diagram (§5c)** are purely client-side
  JavaScript and can land at any time as incremental UI commits.

### Schema migration notes

Both `tri` (§1) and `geo` (§2) add new RocksDB column families.  RocksDB
requires that all CFs a DB was opened with are present on subsequent opens.
Since PolarGraph opens CFs by name from `cf::ALL`, adding a new CF name to
`ALL` must be paired with a migration that detects existing databases and
handles the absence of the CF gracefully (RocksDB's `open_cf_descriptors`
with `create_missing_column_families = true` is the safe path).  Add this as
migration v3 (or v4 if geo ships separately).
