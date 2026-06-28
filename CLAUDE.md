# PolarGraph DB Engine — Codebase Guide

This file is the primary orientation document for contributors and AI coding
assistants working in this repository. Keep it up to date when making
structural changes.

---

## What this is

PolarGraph is a purpose-built, Rust-based graph database engine. The core
data model is a **triple store** (subject → predicate → object) with:

- **Bitemporal versioning** on every fact (valid time + transaction time)
- A **hexastore index** (6 RocksDB column families + ancillary CFs) for O(log n) lookups on
  any (S, P, O) bind pattern
- **Optimistic MVCC** for snapshot-isolated reads and conflict-detected writes
- A **View** system for projecting subsets of the graph with label overrides
- A **Datalog query layer** (conjunctive + recursive rules) and **gRPC server** with HTTP/JSON REST gateway

---

## Workspace layout

```
polargraph/
├── Cargo.toml                  # workspace root
├── CLAUDE.md                   # this file
├── docs/
│   ├── architecture.md         # design narrative
│   └── api-reference.md        # public API surface
├── clients/
│   ├── python/                 # Python SDK (sync + async, grpc)
│   ├── go/                     # Go SDK (functional options, full RPC surface)
│   └── js/                     # TypeScript/JavaScript SDK (@polargraph/client)
├── deploy/
│   └── helm/polargraph/        # Helm chart (8 resources: Namespace, ConfigMap, Secret, PVC, Deployment, Services×3, HPA)
└── crates/
    ├── polargraph-core/        # primitive types, no I/O
    ├── polargraph-storage/     # RocksDB triple store + MVCC
    ├── polargraph-query/       # query planner + Datalog + Cypher + aggregations
    ├── polargraph-sparql/      # SPARQL 1.1 translation layer (library, no server dep)
    ├── polargraph-server/      # gRPC binary (polargraphd)
    ├── polargraph-bench/       # end-to-end benchmark scenarios (binary)
    ├── polargraph-import/      # bulk N-Triples importer via SST ingestion (binary)
    └── polargraph-rest/        # HTTP/JSON REST gateway — proxies to polargraphd over gRPC (binary)
```

Dependency order (no cycles): `core` ← `storage` ← `query` ← `server` ← `bench`
                                                    ↑              ↑
                                           `polargraph-import`   `polargraph-sparql` ← `polargraph-rest`
                                           (core + storage only)   (query + core, no server dep)

---

## Build & test

```bash
# Build everything
cargo build

# Run all tests
cargo test

# Run storage integration tests only
cargo test -p polargraph-storage

# Build the server binary
cargo build -p polargraph-server

# Run Criterion micro-benchmarks (storage layer)
cargo bench -p polargraph-storage

# Build the end-to-end bench binary (requires a running polargraphd for most scenarios)
cargo build -p polargraph-bench

# Build the bulk import binary (run while polargraphd is stopped)
cargo build -p polargraph-import

# Build the REST gateway binary
cargo build -p polargraph-rest

# Check without linking
cargo check

# Lint
cargo clippy -- -D warnings

# Format
cargo fmt
```

Minimum supported Rust version: **1.78** (set in `workspace.package`).

---

## Crate responsibilities

### `polargraph-core`

Dependency-free. No I/O, no async. Contains every shared type.

| Module | Contents |
|--------|----------|
| `id` | `NodeId`, `EdgeId` — UUID v7 wrappers |
| `temporal` | `Timestamp` (i64 µs), `BiTemporalRange` |
| `triple` | `Triple` (enum), `Node`, `Edge`, `Predicate` |
| `value` | `Value` enum: Null, Bool, Int, Float, Text, Blob, Vector |
| `view` | `View`, `ViewId`, `NodeFilter`, `EdgePresentation` |
| `schema` | `FieldKind`, `FieldDef`, `NodeTypeDef`, `EdgeTypeDef`, `VectorSpaceDef` |

**Do not add I/O or async imports here.**

### `polargraph-storage`

RocksDB-backed persistence. Owns the hexastore layout and MVCC layer.

| Module | Contents |
|--------|----------|
| `store` | `TripleStore` — main handle, insert, scan, predicate interning, named HNSW spaces; `StoreMode` (`Primary`/`Secondary`), `open_secondary`, `try_catch_up_with_primary` |
| `mvcc` | `TimestampOracle`, `Transaction`, `Snapshot`, `ConflictError` |
| `keys` | Fixed-width key encoding/decoding for all hexastore CFs |
| `codec` | Value serialization (discriminant + temporal + payload) |
| `cf` | Column family name constants (SPO, SOP, PSO, POS, OSP, OPS, META, HNSW, TRI, DRV, EPA, EPO) |
| `error` | `StorageError` |
| `hnsw` | `HnswIndex` — pure-Rust HNSW, named-space key helpers, serialize/deserialize, mmap storage |
| `registry` | `NodeTypeRegistry`, `EdgeTypeRegistry`, `ValidationError` |
| `sst_import` | `SstImporter`, `ImportStats` — bulk triple import via RocksDB SST file ingestion |
| `compaction` | `CompactionManager`, `RetentionStats` — bitemporal retention scan + RocksDB compaction |
| `backup` | `BackupManager` — incremental RocksDB `BackupEngine` wrapper |
| `migrations` | `MigrationRunner`, `Migration`, `AppliedMigration` — versioned schema migrations |
| `wal_stream` | `WalStreamer`, `WalEntry` — WAL streaming for replication |
| `owl_rl` | `materialize()` — OWL 2 RL forward-chaining engine, 12 rules, DRV CF |

`TripleStore` is `Clone` (Arc-backed). Prefer passing it by clone rather
than wrapping it again in Arc.

### `polargraph-query`

Pattern-based query evaluation and view projection.

| Module | Contents |
|--------|----------|
| `planner` | `Pattern`, `IndexChoice`, `choose_index` — picks cheapest CF for a bind pattern |
| `eval` | `evaluate(pattern, snapshot)` — drives the storage scan the planner chose; `evaluate_with_registry()` prunes patterns using `EdgeTypeRegistry` domain/range hints |
| `projection` | `ProjectedTriple`, `apply_view` — filters and label-remaps triples for a View |
| `datalog` | `Query`, `VarPattern`, `Term`, `Bindings`, `execute_query` — conjunctive query evaluator; `Rule`, `DerivedFacts`, `execute_recursive`, `reachable_from` — recursive / transitive-closure queries |
| `cypher` | `CypherQuery`, `CypherCompiler`, `compile_cypher()` — Cypher→Datalog compiler; MATCH/WHERE/RETURN/WITH pipeline; property equality, comparison, text predicates (CONTAINS, STARTS WITH, =~); `VECTOR_NEAR` function; CREATE/MERGE/SET/DELETE write ops |
| `aggregation` | `AggregationPlan`, `apply_aggregations()` — COUNT(*), COUNT(var), COLLECT(), SUM, AVG, MIN, MAX; ORDER BY (multi-key, ASC/DESC); SKIP N; WITH clause pipeline |
| `explain` | `explain_query()` — static query plan analysis, index selection, plan text output |

### `polargraph-server`

Binary crate (`polargraphd`) and companion library. Exposes the storage and
query layers over gRPC.

| Module | Contents |
|--------|----------|
| `proto` | Generated types from `polargraph.proto` (tonic/prost) |
| `service` | `PolarGraphServer` — implements all RPCs; carries `NodeTypeRegistry`, `EdgeTypeRegistry`, `AccessCache`, query plan cache |
| `convert` | Conversions between proto wire types and Rust domain types |
| `auth` | `ApiKeyLayer` / `ApiKeyService` — tower middleware for gRPC auth; `check_bearer_auth` shared with UI |
| `config` | `Config` structs + `load_config` — TOML config file parsing and auto-detection |
| `telemetry` | `TelemetryLayer` — per-RPC structured logging and Prometheus counters |
| `ui_api` | `UiState`, `build_ui_router` — axum REST handlers + embedded SPA for the management UI |
| `wal_client` | `run_replication` — WAL streaming client (replica mode) |
| `rate_limit` | `RateLimitLayer` / `RateLimitService` — per-IP token-bucket rate limiting tower middleware |
| `retention_scheduler` | `run_retention_scheduler()` — background task that fires `CompactionManager::run_retention()` on a configurable interval |

Configuration priority: **CLI flag > environment variable > config file > built-in default**

Config file locations (searched in order when `--config` is not provided):
1. `./polargraph.toml`
2. `~/.config/polargraph/config.toml`

See `polargraph.example.toml` in the repo root for a fully-commented example.

| Flag | Env variable | Default | Description |
|---|---|---|---|
| `--config PATH` | `POLARGRAPH_CONFIG` | *(auto-detect)* | TOML config file path |
| `--data-dir PATH` | `POLARGRAPH_DATA_DIR` | `/data` | RocksDB data directory |
| `--listen ADDR` | `POLARGRAPH_LISTEN_ADDR` | `0.0.0.0:50051` | gRPC listen address |
| `--log-level FILTER` | `RUST_LOG` | `info` | Log level / filter directive |
| `--log-format FORMAT` | `LOG_FORMAT` | `pretty` | Log format: `pretty` or `json` |
| `--backup-dir PATH` | `POLARGRAPH_BACKUP_DIR` | *(none)* | Backup directory (optional) |
| `--metrics-port PORT` | `POLARGRAPH_METRICS_PORT` | `9090` | Prometheus /metrics HTTP port |
| `--no-metrics` | — | false | Disable metrics endpoint |
| `--api-key KEY` | `POLARGRAPH_API_KEY` | *(none)* | API key (repeatable; comma-separated in env) |
| `--no-auth` | — | false | Suppress no-key warning at startup |
| `--ui-port PORT` | `POLARGRAPH_UI_PORT` | `8080` | Management UI HTTP port |
| `--no-ui` | — | false | Disable the management UI |
| `--query-timeout-ms MS` | `POLARGRAPH_QUERY_TIMEOUT_MS` | `30000` | Max query execution time (ms); 0 = unlimited |
| `--shutdown-timeout-ms MS` | `POLARGRAPH_SHUTDOWN_TIMEOUT_MS` | `10000` | Max ms to drain in-flight requests before force-exit |
| `--slow-query-ms MS` | `POLARGRAPH_SLOW_QUERY_MS` | `1000` | Log warn + increment counter when query exceeds this (ms); 0 = disabled |
| `--tls-cert PATH` | `POLARGRAPH_TLS_CERT` | *(none)* | PEM certificate — enables TLS on gRPC + HTTP when combined with `--tls-key` |
| `--tls-key PATH` | `POLARGRAPH_TLS_KEY` | *(none)* | PEM private key — must be supplied with `--tls-cert` |
| `--replica-of URL` | `POLARGRAPH_REPLICA_OF` | *(none)* | gRPC address of primary; enables WAL streaming replica mode |
| `--replica-tls-ca PATH` | `POLARGRAPH_REPLICA_TLS_CA` | *(none)* | CA cert for verifying the primary's TLS certificate (replica mode only) |
| `--rate-limit-rps N` | `POLARGRAPH_RATE_LIMIT_RPS` | `0` | Max requests/sec per client IP (token bucket); 0 = disabled |
| `--retention-tx-age-secs N` | `POLARGRAPH_RETENTION_TX_AGE_SECS` | *(none)* | Delete triples older than N seconds (transaction time); runs once at startup |
| `--retention-vt-lookback-secs N` | `POLARGRAPH_RETENTION_VT_LOOKBACK_SECS` | *(none)* | Also delete triples whose valid-time end is more than N seconds in the past |
| `--retention-schedule` | `POLARGRAPH_RETENTION_SCHEDULE` | `false` | Enable background periodic retention task |
| `--default-vector-ef N` | `POLARGRAPH_DEFAULT_VECTOR_EF` | `50` | Default HNSW exploration factor for vector searches |
| `--query-cache-size N` | `POLARGRAPH_QUERY_CACHE_SIZE` | `1000` | Max Cypher query plans to cache |
| `--auto-materialize` | `POLARGRAPH_AUTO_MATERIALIZE` | `false` | Run OWL 2 RL materialization at startup |

### `polargraph-import`

Binary crate (`polargraph-import`). Bulk-imports N-Triples files directly
into RocksDB via SST file ingestion — no server required.

| Flag | Default | Description |
|---|---|---|
| `--data-dir PATH` | *(required)* | RocksDB data directory |
| `--input FILE` | *(required)* | N-Triples input file |
| `--batch-size N` | `100000` | Triples per SST import batch |
| `--temp-dir PATH` | `<data-dir>/sst_tmp` | Temporary SST file directory |

**Must be run while `polargraphd` is stopped** — SST ingestion requires
exclusive DB access.

### `polargraph-rest`

Binary crate (`polargraph-rest`). HTTP/JSON REST gateway that proxies requests
to a running `polargraphd` over gRPC. No RocksDB dependency — compiles only
the generated proto client code via `tonic`.

| Flag | Env variable | Default | Description |
|---|---|---|---|
| `--upstream URL` | `POLARGRAPH_UPSTREAM` | `http://localhost:50051` | gRPC server address |
| `--listen ADDR` | `POLARGRAPH_REST_LISTEN` | `0.0.0.0:8000` | HTTP listen address |
| `--api-key KEY` | `POLARGRAPH_REST_API_KEY` | *(none)* | Forwarded as `Authorization: Bearer` to upstream |
| `--tls-ca PATH` | `POLARGRAPH_REST_TLS_CA` | *(none)* | PEM CA cert for upstream TLS verification |

Endpoints include: `POST /query`, `POST /query/stream`, `POST /insert`, `GET /triples`,
`POST /vector/search`, `GET /health`, `POST /explain`, `POST /cypher`, `POST /cypher/write`,
`POST /cypher/stream`, `GET /sparql`, `POST /sparql`, `POST /sparql/update`,
`POST /tx/begin`, `POST /tx/commit`, `POST /tx/rollback`, `GET /indexes`, `GET /stats`,
`POST /materialize`, `GET /property-history`, `POST /edge-annotations`, `GET /edge-annotations/:id`,
`POST /access/grant`, `POST /access/revoke`, `POST /access/add-user`, `GET /access/user/:id`.
See `docs/architecture.md` for full documentation including the pattern string format.

### `polargraph-sparql`

Library crate. Translates SPARQL 1.1 queries into PolarGraph native query types
(`VarPattern`, `Rule`) for execution via the gRPC API. No storage dependency —
used exclusively by `polargraph-rest`.

| Module | Contents |
|--------|----------|
| `translate` | `translate_query()`, `translate_construct()`, `translate_pattern_pub()` — SPARQL algebra walker; `SparqlTranslation`, `Branch`, `SparqlFilter`, `EdgeAnnotationStep`, CONSTRUCT/DESCRIBE types |
| `execute` | `left_join()`, `execute_sparql_aggregations()` — in-process SPARQL semantics for OPTIONAL and GROUP BY |
| `response` | `serialize_json()`, `serialize_csv()`, `node_bindings_to_sparql()` — SPARQL results serializers |
| `serialize` | `serialize_ntriples()`, `serialize_turtle()`, `node_id_to_iri()` — RDF output for CONSTRUCT/DESCRIBE; Turtle-star / N-Triples-star serialization for SPARQL-star results |
| `protocol` | `negotiate_format()`, `extract_query_from_form()` — HTTP content negotiation and form-encoded body parsing |
| `rdf_import` | `parse_ntriples()`, `parse_turtle()`, `parse_jsonld()` — multi-format RDF parsing (rio_api 0.8); `uri_to_node_id`, `bnode_to_node_id`, `edge_id_for` — deterministic IRI → NodeId/EdgeId mapping; `serialize_jsonld`, `serialize_schema_rdf`, `parse_schema_rdf` |

---

## Key design decisions

### Triple model

Everything is a `Triple`. There are two variants:

- **Relation**: subject → predicate → object (both `NodeId`)
- **Property**: subject → predicate → value (scalar `Value`)

Property triples use a 16-byte sentinel (`0xFF × 16`) in the object slot of
every index key, so both variants share the same index structure.

### Column families

PolarGraph uses 12 RocksDB column families. Every triple is written atomically
to all 6 hexastore CFs via a single `WriteBatch`; the others are written in the
same batch or separately as appropriate:

| CF | Purpose |
|----|---------|
| SPO, SOP, PSO, POS, OSP, OPS | Hexastore triple index (6 CFs, see below) |
| META | Predicate intern table, timestamp oracle, migration version |
| HNSW | HNSW vector index nodes and entry points (per named space) |
| TRI | Trigram full-text index (key: `[trigram:3][pred_id:4][subject:16]`) |
| DRV | OWL 2 RL derived facts (same SPO key layout as hexastore, separate CF) |
| EPA | Edge property annotations (key: `[edge_id:16][pred_id:4][tt:8]`) |
| EPO | Edge relation annotations (key: `[edge_id:16][pred_id:4][obj_id:16][tt:8]`) |

### Hexastore (6-CF sub-index)

Each CF supports a different bind pattern:

| CF | Prefix scan gives you |
|----|-----------------------|
| SPO | all predicates/objects for a given subject |
| SOP | all predicates between a given (subject, object) pair |
| PSO | all subjects/objects for a given predicate |
| POS | all subjects that have a given (predicate, object) |
| OSP | all subjects/predicates that point to a given object |
| OPS | all subjects for a given (object, predicate) pair |

Key layout: `[slot_a][slot_b][slot_c][tt(8)]` — always 44 bytes. The `tt`
(transaction time) is the last 8 bytes so keys sort oldest-first within a
given (S,P,O) tuple, enabling efficient MVCC range queries.

### Bitemporal model

Every `BiTemporalRange` has three fields:

- `vt_start` / `vt_end` — when the fact was true in the world (valid time)
- `tt` — when it was written to the DB (transaction time)

`vt_end = Timestamp::END_OF_TIME` means the fact is still current.
`tt` is stored in the index key; `vt_start`/`vt_end` live in the value bytes.

### Predicate interning

Predicates are stored as variable-length strings externally but interned to
`u32` IDs in the META column family for compact index keys. The in-memory
`fwd`/`rev` maps are loaded at `TripleStore::open` and kept in sync under a
`RwLock` pair. Assigning a new predicate ID requires a `WriteBatch` flush.

### MVCC

Optimistic concurrency:

1. `begin()` snapshots `read_ts` from the `TimestampOracle`.
2. Reads filter to `tt <= read_ts`.
3. Writes buffer in memory.
4. `commit()` acquires the commit mutex, increments the oracle, checks for
   write-write conflicts (any (S,P,O) with `read_ts < tt <= commit_ts`),
   then flushes with the new `commit_ts`.

The oracle counter persists to the META CF so restarts don't reuse timestamps.

### IDs

`NodeId` and `EdgeId` are UUID v7 (time-ordered). This gives chronological
sort order and is cluster-safe without a central sequence generator.

---

## Conventions

- **Error handling**: `thiserror`-derived enums (`StorageError`). Prefer
  `?` propagation. Avoid `unwrap()` outside tests and index math where the
  invariant is enforced by construction.
- **Logging**: `tracing` crate. Use `info!` for lifecycle events, `debug!`
  for per-operation detail. No `println!` in library crates.
- **Tests**: Unit tests live in the same file (`#[cfg(test)] mod tests`).
  Integration tests live under `crates/<crate>/tests/`.
- **Serde**: All public types derive `Serialize`/`Deserialize` unless there's
  a good reason not to.
- **Naming**: Rust standard (snake_case functions/modules, CamelCase types).
  Column family names are uppercase 3-letter strings (`SPO`, `PSO`, etc.).

---

## Current state (phase 2 in progress)

- [x] Core type system (`polargraph-core`)
- [x] Hexastore storage layer with predicate interning
- [x] Bitemporal key encoding
- [x] MVCC (optimistic, timestamp-oracle-based)
- [x] Snapshot reads
- [x] Pattern-based query planner (`polargraph-query::planner`)
- [x] Pattern evaluator (`polargraph-query::eval`)
- [x] SOP prefix scan for (S,?,O) bind pattern — `decode_sop`, `scan_by_subject_object` (`polargraph-storage`, `polargraph-query`)
- [x] Full scan for (?,?,?) unconstrained pattern — iterates SPO CF with no prefix (`polargraph-storage::scan_all`, `polargraph-query::eval`)
- [x] View projection engine (`polargraph-query::projection`)
- [x] Conjunctive query evaluator / multi-pattern joins (`polargraph-query::datalog`)
- [x] Recursive Datalog rules — `Rule`, `execute_recursive`, `reachable_from` (`polargraph-query::datalog`)
- [x] gRPC API (`polargraph-server`) — `Insert`, `Query`, `InsertVector`, `SearchVector`, `Reachable`
- [x] CLI flags (`--data-dir`, `--listen`, `--log`) with env-var fallback (`polargraph-server`)
- [x] Docker packaging — multi-stage `Dockerfile` + `docker-compose.yml` with named volume
- [x] HNSW vector index — `Value::Vector(Vec<f32>)`, pure-Rust HNSW in `polargraph-storage::hnsw`, RocksDB-persisted `hnsw` CF, `insert_vector` / `search_vector` on `TripleStore`, `InsertVector` / `SearchVector` gRPC RPCs
- [x] Dynamic node type registry — `FieldKind`, `FieldDef`, `NodeTypeDef` in `polargraph-core::schema`; `NodeTypeRegistry` in `polargraph-storage::registry` (schemas stored as triples, loaded on open, RwLock cache); `RegisterNodeType`, `GetNodeType`, `ListNodeTypes`, `ValidateNode` gRPC RPCs
- [x] Dynamic edge type registry — `EdgeTypeDef` in `polargraph-core::schema`; `EdgeTypeRegistry` in `polargraph-storage::registry` (stored under `__edge_schema__/<predicate>`, domain/range/field constraints, `list_predicates_between`); `RegisterEdgeType`, `GetEdgeType`, `ListEdgeTypes`, `ValidateEdge`, `ListPredicatesBetween` gRPC RPCs
- [x] Full vector store feature set — named HNSW spaces (`HashMap<String, HnswIndex>`, RocksDB keys `<space>/n/<id>` + `<space>/__ep`); `VectorSpaceDef` on `NodeTypeDef`; dimension validation; `search_vector_ef`, `search_vector_in_set`, `batch_insert_vectors` on `TripleStore`; `SearchVectorFiltered` (node-type or reachability post-filter), `SearchVectorInSet`, `BatchInsertVectors` gRPC RPCs; `space` field on `InsertVector` / `SearchVector`
- [x] Benchmark suite — Criterion micro-benchmarks in `polargraph-storage/benches/storage.rs` (triple writes, pattern queries, HNSW insert/search/recall, filtered-search comparison); `polargraph-bench` binary crate with 5 CLI scenarios (write, read, mixed, recovery, filtered-search); `BENCHMARKS.md` run guide
- [x] Filtered vector search cache — `Arc<RwLock<HashMap<String, HashSet<NodeId>>>>` type cache in `PolarGraphServer`; populated at startup from existing `__type` triples, updated incrementally on every `Insert` commit; `SearchVectorFiltered(NodeTypeFilter)` reads cache instead of calling `scan_by_predicate`; also fixed O(|allowed|) `Vec::contains` post-filter to O(1) `HashSet::contains`; 13× speedup (6.8 ms → 515 µs at 5K nodes)
- [x] VectorSeedQuery RPC — `ScoredBinding`, `VectorSeedQueryRequest/Response` proto messages; `execute_query_seeded` in `polargraph-query::datalog` (generalises `execute_query` with caller-supplied initial bindings); `vector_seed_query` handler in `polargraph-server::service` (ANN → seed bindings → Datalog join → scored results, with optional NodeType/Reachability pre-filter); 4 integration tests; `docs/architecture.md` VectorSeedQuery section
- [x] Memmap HNSW vector storage — `StorageMode` enum (`Memory` / `Mmap`) in `polargraph-core::schema`; `MmapState` in `polargraph-storage::hnsw` (flat `.vecs` file, `memmap2::MmapMut`, OS page-in on demand); `VectorStorage` enum in `HnswIndex`; `insert_vector` / `batch_insert_vectors` take `StorageMode`; `load_hnsw_spaces` reopens mmap files at startup; `storage_mode` string field on `VectorSpaceDefProto` and round-trips through `convert.rs`; service looks up space def to pass mode; 4 unit tests (insert/search, recall parity, persistence, batch); 2 gRPC integration tests
- [x] Backup and restore — `BackupManager` in `polargraph-storage::backup` wraps RocksDB `BackupEngine`; incremental SST hard-linking; `create_backup`, `list_backups`, `restore_from_backup`, `purge_old_backups` on `BackupManager`; `--backup-dir PATH` CLI flag + `POLARGRAPH_BACKUP_DIR` env var; `CreateBackup`, `ListBackups`, `PurgeOldBackups` gRPC RPCs; `FailedPrecondition` when backup dir not configured; restore is offline (documented in `docs/architecture.md` with runbook); 4 unit tests, 7 gRPC integration tests
- [x] Bulk import via SST ingestion — `SstImporter` in `polargraph-storage::sst_import`; buffers triples, sorts per CF, writes via `SstFileWriter`, ingests with `ingest_external_file_cf`, advances MVCC oracle; `polargraph-import` binary crate (`crates/polargraph-import`) with N-Triples parser, clap CLI (`--data-dir`, `--input`, `--batch-size`, `--temp-dir`), per-batch progress output; URI → stable `NodeId` via xxHash3-128; offline-only (requires exclusive DB access — server must be stopped); 6 storage integration tests, 7 import unit tests; documented in `docs/architecture.md`
- [x] Compaction and bitemporal retention — `RetentionPolicy` in `polargraph-core::schema` (`tx_age_secs`, `vt_lookback_secs`); `CompactionManager` + `RetentionStats` in `polargraph-storage::compaction`; scans all 6 hexastore CFs, deletes expired entries via `WriteBatch`, triggers `compact_range_cf` on modified CFs; oracle changed to wall-clock µs (`max(committed+1, now_µs)`) so `tt` values are real timestamps; `TripleStore::insert_at_ts` (explicit tt, advances oracle) and `scan_cf_raw` / `compact_cf` helpers; `--retention-tx-age-secs` + `--retention-vt-lookback-secs` CLI flags run retention at startup; `RunRetention` gRPC RPC; 6 storage integration tests, 3 gRPC integration tests; documented in `docs/architecture.md`
- [x] Bitemporal time-travel queries — `as_of_valid_time` and `as_of_tx_time` fields on `QueryRequest` proto; `Snapshot.vt_as_of: Option<i64>` + `Snapshot::with_vt_as_of()` in `polargraph-storage::mvcc`; vt filter applied inside `snapshot_scan_cf` **before** MVCC deduplication for correctness; `as_of_tx_time` overrides `snapshot_ts` in the gRPC handler; 6 gRPC integration tests; "Time-travel queries" section in `docs/architecture.md`
- [x] WAL streaming replication — `WalStreamer` + `WalEntry` in `polargraph-storage::wal_stream`; `TripleStore::open_as_replica`, `apply_replicated_batch`, `last_applied_seq`, `latest_sequence_number`; `StreamWal` server-streaming gRPC RPC on primary; `run_replication` in `polargraph-server::wal_client` with exponential backoff (1s→30s); `ReplicaState` tracks `last_applied_seq` + lag; `--replica-of URL` takes gRPC address (no shared filesystem required); `FAILED_PRECONDITION` on all write RPCs and `StreamWal` on replicas; WAL retention 1 h / 512 MB on primary; `last_applied_seq` persisted to META CF for restart resumption; 5 gRPC integration tests; `docs/scaling.md` updated
- [x] API key authentication — `ApiKeyLayer` + `ApiKeyService` in `polargraph-server::auth`; tower `Layer` applied at transport level via `Server::builder().layer()`; `Bearer <key>` and `ApiKey <key>` header formats; `subtle::ConstantTimeEq` constant-time comparison; multiple keys for zero-downtime rotation; `ReplicaStatus` exempt for health probes; `--api-key KEY` (repeatable) + `POLARGRAPH_API_KEY` (comma-separated); `--no-auth` suppresses startup warning; 6 gRPC integration tests + 7 unit tests; "Authentication" section in `docs/architecture.md`
- [x] Observability — `TelemetryLayer` in `polargraph-server::telemetry`; tower middleware logs every RPC with method, peer, gRPC status, and duration; `tracing-subscriber` with `json` feature enables `--log-format json` (newline-delimited JSON) vs `pretty` (human-readable); `--log-level`/`RUST_LOG` replaces `--log`; `metrics` + `metrics-exporter-prometheus` crates; Prometheus `/metrics` HTTP endpoint via `axum 0.6` on `--metrics-port` (default 9090) / `POLARGRAPH_METRICS_PORT`; `--no-metrics` flag to disable; counters/gauges: `polargraph_rpc_requests_total{method,status}`, `polargraph_rpc_duration_seconds{method}`, `polargraph_triples_total`, `polargraph_vector_spaces_total`, `polargraph_wal_applied_seq`, `polargraph_wal_lag_entries`, `polargraph_backup_last_size_bytes`, `polargraph_compaction_deleted_total`; `docker-compose.yml` exposes port 9090 and sets `LOG_FORMAT=json`; "Observability" section in `docs/architecture.md`
- [x] Management UI — `UiState` + `build_ui_router` in `polargraph-server::ui_api`; axum REST endpoints (`/api/status`, `/api/node-types`, `/api/edge-types`, `/api/metrics`, `/api/query`, `/api/insert`, `/api/search`) call into `PolarGraphService` trait in-process (no network hop); dark-theme SPA embedded via `include_str!("ui.html")` with 5 tabs (Query, Schema, Insert, Search, Status), auth overlay, localStorage key storage; `--ui-port PORT` (default 8080) / `POLARGRAPH_UI_PORT`; `--no-ui` to disable; `require_auth` reuses `check_bearer_auth` from `auth.rs`; 3 integration tests (GET /, status fields, 401 enforcement); "Management UI" section in `docs/architecture.md`
- [x] Graceful shutdown — `tokio_util::sync::CancellationToken` shared across all background tasks; SIGTERM + SIGINT handled via `tokio::signal`; tonic `serve_with_shutdown` drains in-flight RPCs; axum metrics and UI HTTP servers use `with_graceful_shutdown`; WAL replication loop exits at next `tokio::select!` point; watchdog task force-exits after `--shutdown-timeout-ms` (default 10 000 ms) / `POLARGRAPH_SHUTDOWN_TIMEOUT_MS`; sequenced log messages at each stop; 3 integration tests (`shutdown_token_stops_server`, `shutdown_rejects_new_connections_after_drain`, `wal_client_stops_on_cancellation`); "Graceful shutdown" section in `docs/architecture.md`
- [x] Query timeouts — `QueryError` enum (`Timeout` + `Storage`) in `polargraph-query::datalog`; `deadline: Option<Instant>` propagated through `execute_query`, `execute_query_seeded`, `execute_recursive`, `execute_query_hybrid`, `reachable_from`, `reachable_from_hops`; checked at each iteration boundary; `query_timeout_ms: u64` field on `PolarGraphServer` (default 30 000); `with_query_timeout_ms()` builder method; `--query-timeout-ms MS` CLI flag + `POLARGRAPH_QUERY_TIMEOUT_MS` env var; 0 = disabled; maps `QueryError::Timeout` → `Status::deadline_exceeded`; applied to `Query`, `VectorSeedQuery`, `Reachable`, `SearchVectorFiltered(ReachabilityFilter)` RPCs; 3 unit tests in `polargraph-query::datalog::tests` + 3 gRPC integration tests; "Query timeouts" section in `docs/architecture.md`
- [x] Slow query logging — `slow_query_ms: u64` on `PolarGraphServer` (default 1 000); `with_slow_query_ms()` builder; `check_slow_query()` helper emits `warn!` with structured fields (method, duration_ms, threshold_ms, extra); increments `polargraph_slow_queries_total{method}` Prometheus counter; applied to `Query`, `VectorSeedQuery`, `Reachable` RPCs; `--slow-query-ms MS` CLI flag + `POLARGRAPH_SLOW_QUERY_MS` env var; 0 = disabled; documented in `docs/architecture.md`
- [x] ExplainQuery RPC — `ExplainResponse` + `PlanNode` proto messages; `ExplainQuery(QueryRequest) returns (ExplainResponse)` RPC (pure static analysis, no DB access); `polargraph_query::explain` module with `explain_query(query, rules) -> ExplainPlan`; symbolic binding-state tracking across patterns; index selection via same `choose_index` as evaluator; recursive rule detection; multi-line `plan_text` output; 4 unit tests in `polargraph-query::explain::tests`; "Query planner (EXPLAIN)" section in `docs/architecture.md`
- [x] TLS — `--tls-cert` + `--tls-key` CLI flags (`POLARGRAPH_TLS_CERT` / `POLARGRAPH_TLS_KEY`) enable TLS on gRPC (via tonic `ServerTlsConfig`), management UI, and Prometheus metrics HTTP servers (via `tokio-rustls` + hyper `accept::from_stream`); `--replica-tls-ca PATH` (`POLARGRAPH_REPLICA_TLS_CA`) enables TLS on the WAL replication channel (tonic `ClientTlsConfig`); omitting both cert+key keeps plaintext mode unchanged; "TLS" section in `docs/architecture.md`
- [x] gRPC health checks — `tonic-health 0.11` `HealthServer` added to the gRPC server; service status set to `Serving` at startup; WAL client toggles between `NotServing` (reconnecting) and `Serving` (stream active); `ReplicaState.connected: AtomicBool` tracks WAL connectivity; HTTP `GET /health` on the management UI server returns `{"status":"ok","mode":"primary"|"replica","triples":<n>}` (200) or `{"status":"degraded"}` (503) when replica is disconnected; 2 integration tests; "Health checks" section in `docs/architecture.md`
- [x] TOML config file — `polargraph-server::config` module (`Config`, `ServerConfig`, `StorageConfig`, `ReplicationConfig`, `TlsConfig`, `AuthConfig`, `ObservabilityConfig`, `QueryConfig`); `load_config(Option<&Path>)` searches `./polargraph.toml` then `~/.config/polargraph/config.toml` if no explicit path given; `--config PATH` CLI flag + `POLARGRAPH_CONFIG` env var; priority chain CLI > env > config > default applied in `main.rs` via `resolve`/`resolve_path`/`resolve_path_opt` helpers; `polargraph.example.toml` fully-commented example at repo root; 5 unit tests; "Configuration file" section in `docs/architecture.md`
- [x] Per-client rate limiting — `RateLimitLayer` + `RateLimitService` tower middleware in `polargraph-server::rate_limit`; `DashMap<IpAddr, TokenBucket>` for per-IP state (lock-free sharded map); token-bucket refill based on elapsed time; client IP resolved from `x-forwarded-for` header then `TcpConnectInfo` tonic extension; lazy stale-entry cleanup every 1 000 requests (entries idle >60 s removed); `RateLimitLayer::disabled()` / `is_enabled()`; `ReplicaStatus` exempt; `RESOURCE_EXHAUSTED` on quota exhaustion; `RateLimitConfig` section in `config.rs`; `--rate-limit-rps N` CLI flag + `POLARGRAPH_RATE_LIMIT_RPS` env var; layer applied before auth in gRPC `Server::builder()` chain; 4 unit tests + 2 gRPC integration tests; "Rate limiting" section in `docs/architecture.md`
- [x] REST gateway — `polargraph-rest` binary crate (`crates/polargraph-rest`); axum 0.6 HTTP server proxies to polargraphd via tonic 0.11 gRPC client; `AuthInterceptor` tower layer forwards API key; `POST /query`, `POST /insert`, `GET /triples`, `POST /vector/search`, `GET /health`, `POST /explain` endpoints; pattern string parser (`?var :pred ?obj` format, UUID bound terms, `_` wildcard); gRPC status → HTTP status mapping (`NOT_FOUND`→404, `UNAUTHENTICATED`→401, `PERMISSION_DENIED`→403, `RESOURCE_EXHAUSTED`→429, `DEADLINE_EXCEEDED`→408, `INVALID_ARGUMENT`→400); TLS CA cert support via `--tls-ca`; lazy channel connect; 7 unit tests; "REST gateway" section in `docs/architecture.md`
- [x] REST gateway Datalog rules — `/query` and `/explain` now accept a `rules` array; each rule has `head_predicate`, `head_subject_var`, `head_object_var`, `body` (pattern strings); forwarded to gRPC `QueryRequest.rules`; server runs `execute_recursive` to fixpoint then `execute_query_hybrid` against combined base + derived facts; `DatalogRule` proto message added to `polargraph.proto`; `rule_from_proto` in `convert.rs`; `execute_query_hybrid` made `pub` in `polargraph-query::datalog`
- [x] Edge property storage — `RelationTriple` proto gains `repeated EdgeProperty properties`; `InsertResponse` gains `repeated bytes edge_ids`; `triples_from_proto` in `convert.rs` expands a relation with properties into the relation triple + one `Triple::Property` per property (subject = `NodeId(edge_id.0)`); insert handler collects and returns edge UUIDs; REST `/insert` accepts `properties` array and surfaces `edge_id` in response; `EdgeProperty` proto message added; `schema.rs` doc updated
- [x] Schema migrations — `MigrationRunner` + `Migration` + `AppliedMigration` + `MigrationStats` in `polargraph-storage::migrations`; version stored in META CF under `__migrations__/version` (little-endian u32); applied records under `__migrations__/applied/<version>` as JSON; `MIGRATIONS` static list with 2 built-in versions (v1: baseline marker, v2: normalize node type schema records); auto-migration at server startup before gRPC accepts connections, skipped in replica mode; `MigrateSchema(MigrateRequest)` RPC (dry_run support, replica guard), `MigrationStatus` RPC; `rollback(to_version)` for reversals; 3 storage unit tests + 4 gRPC integration tests; "Schema migrations" section in `docs/architecture.md`
- [x] Cypher query layer — `CypherQuery` gRPC RPC; `polargraph-query::cypher` module; full Cypher→Datalog compiler (`compile_cypher()`); MATCH/WHERE/RETURN pipeline; property equality, comparison, and text predicates (CONTAINS, STARTS WITH, =~); `VECTOR_NEAR(var, "space", k)` function seeds queries from ANN results; `CypherQueryRequest/Response` proto messages; REST `POST /cypher`; "Cypher query layer" section in `docs/architecture.md`
- [x] Cypher aggregations — `polargraph-query::aggregation` module; `AggregationPlan` + `apply_aggregations()`; COUNT(*), COUNT(var), COLLECT(); SUM(var.prop), AVG(var.prop), MIN(var.prop), MAX(var.prop) (numeric property aggregations via snapshot lookup); ORDER BY with multi-key, ASC/DESC; SKIP N; WITH clause pipeline for multi-step Cypher; integrated into Cypher compiler output
- [x] Variable-length path syntax — `[r*1..n]` and `[r*n]` in Cypher MATCH patterns compile to bounded BFS via `reachable_from_hops` (max_hops field on `VarPattern`); `[r*]` (no bound) still uses unlimited Datalog TC rules; unbound subject automatically enumerates all starting nodes via predicate scan
- [x] Cypher write operations — `CypherWrite` gRPC RPC; `WriteOp` enum, `CompiledWrite`, `parse_write()`, `execute_write_ops()` in `polargraph-query::cypher`; supports CREATE node/relation, MERGE, SET property, DELETE; `CypherWriteRequest/Response` proto messages; REST `POST /cypher/write`; returns created node IDs and triple counts
- [x] Full-text trigram search — `TRI` column family (key: `[trigram:3][pred_id:4][subject_id:16]`); `extract_trigrams()`, `insert_trigrams()`, `text_search()` on `TripleStore`; integrated with Cypher WHERE CONTAINS / STARTS WITH / =~ so text predicates hit the `TRI` CF instead of doing full scans
- [x] Schema-aware query optimization — `evaluate_with_registry()` in `polargraph-query::eval`; accepts `&EdgeTypeRegistry`; uses domain/range type hints to prune impossible join branches before evaluation; `SchemaHints` struct wraps registry lookups; no change to the triple wire format
- [x] Wire transactions — `BeginTransaction` / `CommitTransaction` / `RollbackTransaction` gRPC RPCs; optional `tx_id: string` field on `InsertRequest`, `QueryRequest`, `CypherWriteRequest`; `open_txns: Arc<DashMap<String, Arc<Mutex<Transaction>>>>` in `PolarGraphServer`; UUID v4 token assigned at `BeginTransaction`; idle-TTL cleanup task evicts transactions open for >5 min; REST `POST /tx/begin`, `POST /tx/commit`, `POST /tx/rollback`
- [x] Server-streaming queries — `QueryStream` and `CypherQueryStream` server-streaming gRPC RPCs; results streamed in chunks of `STREAM_CHUNK_SIZE = 500` bindings; REST `POST /query/stream` and `POST /cypher/stream` return NDJSON (newline-delimited JSON); useful for large result sets without holding a full in-memory buffer
- [x] Diagnostics RPCs — `ShowIndexes` RPC returns per-CF key count + estimated size + HNSW space metadata; `ShowStats` RPC returns selected RocksDB properties, current oracle timestamp, and open transaction count; REST `GET /indexes` and `GET /stats`
- [x] Scheduled retention — `retention_scheduler.rs` in `polargraph-server`; `run_retention_scheduler()` tokio task fires on configurable `interval_secs`; enabled via `[storage.retention_schedule]` TOML section or `--retention-schedule` / `POLARGRAPH_RETENTION_SCHEDULE`; adds `polargraph_retention_runs_total`, `polargraph_retention_deleted_total`, `polargraph_retention_last_run_ts` Prometheus counters
- [x] Query history + schema diagram in UI — 50-entry ring buffer in management SPA stores recent queries in `localStorage`; Query tab shows history dropdown to re-run past queries; Schema tab renders a Mermaid.js ER diagram from live node/edge type data
- [x] Python SDK — `clients/python/` package (`polargraph-client`); `PolarGraphClient` (sync) and `AsyncPolarGraphClient` (grpc.aio); full RPC surface including Cypher, wire transactions, streaming, vector operations; `pyproject.toml`; `clients/python/README.md`
- [x] Go SDK — `clients/go/` module (`github.com/polarops/polargraph-go`); `Client` struct with functional options `WithAPIKey`, `WithTLSCA`; full method surface; 5 unit tests; `clients/go/README.md`
- [x] JavaScript/TypeScript SDK — `clients/js/` package (`@polargraph/client`); `@grpc/grpc-js` transport; full TypeScript types; `tsup` build; streaming, wire transactions, Cypher; `clients/js/README.md`
- [x] Helm chart — `deploy/helm/polargraph/` with 8 Kubernetes resources: Namespace, ConfigMap, Secret, PVC, Deployment, Services×3 (gRPC, UI, metrics), HPA; values file for image tag, replica count, resource limits, storage size
- [x] Simplified CI/CD — single `.github/workflows/ci.yml`; jobs: `test` (cargo test), `lint` (clippy + fmt), `release` (binary artifact upload on `main`/tags), `docker-build` (build-only smoke test, no push)
- [x] ef tuning — `default_vector_ef: u32` field on `PolarGraphServer`; `--default-vector-ef N` CLI flag + `POLARGRAPH_DEFAULT_VECTOR_EF` env var; `[query] default_vector_ef` TOML key; three-level resolution hierarchy: Cypher inline `ef=N` > per-request `ef` field > server default (built-in: 50); `with_default_vector_ef()` builder method
- [x] VectorSpaceDef in core schema — `VectorSpaceDef` with `space_name`, `dimensions`, `embedding_model`, `storage_mode` string fields in `polargraph-core::schema`; associated with `NodeTypeDef`; `storage_mode` round-trips through proto `VectorSpaceDefProto` and `convert.rs`
- [x] RDF-star edge annotations — additive `Triple::EdgeProperty { edge, predicate, value, temporal }` and `Triple::EdgeRelation { edge, predicate, object, temporal }` variants in `polargraph-core::triple`; two new RocksDB column families: `EPA` (key `[edge_id:16][pred_id:4][tt:8]` = 28 bytes, value = codec Property bytes) and `EPO` (key `[edge_id:16][pred_id:4][obj_id:16][tt:8]` = 44 bytes, value = `[vt_start:8][vt_end:8]`); `EdgeAnnotation`, `EdgeAnnotationValue` in `polargraph-storage::store`; `scan_edge_annotations(edge, snapshot_ts)` and `get_edge_annotation(edge, predicate, snapshot_ts)` on `TripleStore`; `EdgeAnnotation` + `GetEdgeAnnotationsRequest/Response` proto messages; `repeated EdgeAnnotation edge_annotations` on `InsertRequest`; `GetEdgeAnnotations` gRPC RPC; `edge_annotation_from_proto` / `edge_annotation_to_proto` in `convert.rs`; REST `POST /edge-annotations` and `GET /edge-annotations/:edge_id`; 6 storage integration tests in `crates/polargraph-storage/tests/annotation.rs`; 3 gRPC integration tests; existing `Triple` variants and key formats unchanged
- [x] Graph-native access control — `BUILTIN_USER_TYPE`, `BUILTIN_GROUP_TYPE`, `BUILTIN_MEMBER_OF_PRED`, `BUILTIN_HAS_ACCESS_PRED`, `BUILTIN_HAS_ACCESS_TYPE_PRED` constants + `builtin_node_types()` / `builtin_edge_types()` in `polargraph-core::schema`; `AccessCache: Arc<RwLock<HashMap<String, HashSet<NodeId>>>>` on `PolarGraphServer` built from MEMBER_OF + HAS_ACCESS + HAS_ACCESS_TYPE triples at startup and refreshed on AC-touching inserts; `user_id: string` field on `QueryRequest`, `CypherQueryRequest`, `VectorSeedQueryRequest`, `SearchVectorFilteredRequest`; identity also accepted via `x-polargraph-user-id` gRPC metadata / `X-User-Id` HTTP header; `filter_bindings()` post-filter restricts results to allowed NodeIds; `GrantAccess`, `RevokeAccess`, `AddUserToGroup`, `GetUserAccess` gRPC RPCs (blocked on replicas); `attach_user_id()` helper + `/access/grant`, `/access/revoke`, `/access/add-user`, `/access/user/:user_id` REST endpoints; 4 storage integration tests in `crates/polargraph-storage/tests/access_control.rs`; 5 gRPC integration tests; 2 REST unit tests; "Access Control" section in `docs/architecture.md`
- [x] Named Cypher parameters — `$param` syntax in WHERE/property clauses; `FilterValue` enum (`Literal(Value)` | `Param(String)`) in `polargraph-query::cypher`; `Token::Param(String)` in lexer; `CypherValue::Param(String)` variant; `substitute_params(&HashMap<String,Value>) -> Result<CompiledQuery, CypherError>` on `CompiledQuery`; `params: map<string,string>` added to `QueryRequest`, `CypherQueryRequest`, `VectorSeedQueryRequest` proto messages; `deserialize_params()` helper in `service.rs`; `#[derive(Debug, Clone, PartialEq)]` on `FilterValue`; `#[derive(Debug)]` on `CompiledQuery`, `ValueFilter`, `VectorNearClause`, `TextFilter`, `TextFilterKind`, `EdgeAnnotationFilter`; Python SDK `params` kwarg (auto-JSON-serialised); Go SDK `WithParams(map[string]string)` option; TypeScript SDK `params?: Record<string, string>` in `CypherOptions`; REST `POST /cypher` and `POST /cypher/stream` accept `params` JSON key; 4 unit tests in `polargraph-query::cypher::tests`; "Named Cypher parameters" section in `docs/architecture.md`
- [x] Query plan cache — `Arc<DashMap<String, Arc<CompiledQuery>>>` on `PolarGraphServer`; caches pre-substitution compiled plans keyed by raw Cypher string; `query_cache_size: usize` (default 1 000); `with_query_cache_size()` builder; `query_cache_hits` / `query_cache_misses` `Arc<AtomicU64>` counters; increments `polargraph_query_cache_hits_total` / `polargraph_query_cache_misses_total` Prometheus counters; `ShowStatsResponse` gains `query_cache_hits`, `query_cache_misses`, `query_cache_size` fields; cache applied in `cypher_query()` and `cypher_query_stream()`; `--query-cache-size N` CLI flag + `POLARGRAPH_QUERY_CACHE_SIZE` env var + `[query] cache_size` TOML key; "Query plan cache" section in `docs/architecture.md`
- [x] Property version history — `scan_property_history(subject, predicate, limit) -> Vec<(Value, i64)>` on `TripleStore`; scans SPO CF with 36-byte prefix `[subject:16][pred_id:4][sentinel:16]` bypassing MVCC deduplication to return all historical `tt` values newest-first; `GetPropertyHistory(GetPropertyHistoryRequest) returns (GetPropertyHistoryResponse)` gRPC RPC; `PropertyVersion { value_json, transaction_time }` proto message; REST `GET /property-history?subject=<uuid>&predicate=<string>&limit=<n>`; 5 storage integration tests in `crates/polargraph-storage/tests/property_history.rs`; 3 gRPC integration tests
- [x] OWL 2 RL Phase 1 — `DRV` column family for derived facts (same SPO key layout, separate from base hexastore); `polargraph-storage::owl_rl` module with `materialize(store, clear_first)` forward-chaining engine; 12 rules: rdfs2, rdfs3, rdfs5, rdfs7/prp-spo1, rdfs9, rdfs11, prp-symp, prp-trp, prp-inv1, prp-inv2, eq-sym, eq-trans; `uri_to_node_id(uri)` xxHash3-128 predicate bridge (predicate string → stable `NodeId` for schema triples); `predicate_node(pred)` public helper; `insert_derived_batch`, `clear_derived`, `scan_derived`, `scan_derived_at`, `estimate_derived_count` on `TripleStore`; `RunMaterializationRequest/Response` proto messages; `RunMaterialization` gRPC RPC (runs in `spawn_blocking`, replica-guarded); `polargraph_materialization_derived_total` Prometheus gauge; `--auto-materialize` CLI flag + `POLARGRAPH_AUTO_MATERIALIZE` env var + `[storage] auto_materialize` TOML key; startup materialization on primary when flag is set; `POST /materialize` REST endpoint; 9 storage integration tests in `crates/polargraph-storage/tests/owl_rl.rs`; 3 gRPC integration tests
- [x] SPARQL 1.1 endpoint — `polargraph-sparql` library crate; `polargraph-rest` exposes `GET /sparql`, `POST /sparql`, `POST /sparql/update`; SELECT, ASK, CONSTRUCT, DESCRIBE; UNION, OPTIONAL, FILTER (comparison, BOUND, isIRI, boolean ops), property paths (sequence, alternative-first-branch, `+`/`*`/`?`, reverse), GROUP BY + aggregates (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, SAMPLE), HAVING, LIMIT/OFFSET, DISTINCT; SPARQL-star subject-position quoted triples mapped to edge annotation lookups; named graphs (GRAPH clause, runtime enforcement pending); SPARQL Update: INSERT DATA, DELETE DATA, INSERT/DELETE WHERE; content negotiation (JSON / CSV)
- [x] SPARQL-star full support — object-position embedded triples (`?s :p << :a :b :c >>` → `GetEdgeIdsByTriple` lookup); variable annotation predicates (`<< :s ?p :o >> :annot ?val`); Turtle-star / N-Triples-star serialization in CONSTRUCT/DESCRIBE results; SPARQL Update with embedded triples (`INSERT DATA { << s p o >> :annot val }`); `GetEdgeIdsByTriple` gRPC RPC (`GetEdgeIdsByTripleRequest { repeated TripleRef }` → `repeated EdgeIdResult`)
- [x] BSBM benchmark suite — `polargraph-bench::bsbm` module; deterministic e-commerce dataset generator (scale-factor-N: N×100 products, N×10 types in 3-level hierarchy, N×20 features, N×5 vendors, N×50 offers, N×20 reviews; generated via xxHash3-128 deterministic NodeIds); 12 BSBM query templates implemented via native `execute_query` + `Snapshot` scans: Q1 (type+feature+numeric), Q2 (detail), Q3 (2-feature+range), Q4 (UNION), Q5 (similar products), Q6 (trigram full-text), Q7 (5-way join), Q8 (reviews), Q9 (review detail), Q10 (vendor offers), Q11 (COUNT), Q12 (reviewer products); `polargraph-bench bsbm` subcommand with `--scale-factor`, `--warmup-runs`, `--measure-runs`, `--data-dir` flags; CLI restructured to `clap::Subcommand` pattern; Criterion `bsbm/q1_product_search` and `bsbm/q7_five_way_join` micro-benchmarks in `polargraph-storage/benches/storage.rs` using storage-layer primitives only; `BENCHMARKS.md` Part 3 with scale-1 measured results (35,682 avg QPS across 12 queries on Apple M-series, Q6 full-text slowest at 0.158 ms avg, Q9/Q11 fastest at ~0.001 ms)
- [x] RDF interoperability — `polargraph-sparql::rdf_import` module with `parse_ntriples`, `parse_turtle`, `parse_jsonld` (rio_api 0.8 / rio_turtle 0.8); `uri_to_node_id`, `bnode_to_node_id`, `edge_id_for` for deterministic ID mapping; `serialize_jsonld` in `polargraph-sparql::serialize` (JSON-LD `@graph` format, XSD datatype mapping); `serialize_schema_rdf` / `parse_schema_rdf` for OWL/RDFS Turtle schema import/export (`urn:polargraph:type:`, `urn:polargraph:prop:`, `urn:polargraph:rel:` IRIs); `polargraph-rest` endpoints: `POST /import/rdf` (N-Triples/Turtle/JSON-LD, 1000-triple batches, 415 for RDF+XML), `POST /import/subgraph`, `GET /export/jsonld`, `POST /export/jsonld`, `GET /export/subgraph` (Accept-negotiated N-Triples/Turtle/JSON-LD), `GET /schema/rdf`, `POST /schema/rdf`; `polargraph-import --format ntriples|turtle|jsonld` flag; 6 integration tests in `crates/polargraph-sparql/tests/interop.rs` (roundtrip_ntriples, roundtrip_turtle, jsonld_export_structure, jsonld_import, subgraph_export_import, schema_rdf_roundtrip)

---

## Adding a new predicate

Predicates are interned automatically on first `insert()` — no schema
migration needed. Just use the string you want in `Triple::Relation` or
`Triple::Property`. The intern table persists across restarts.

## Adding a new `Value` variant

1. Add the variant to `polargraph_core::value::Value`.
2. Update the `serde` representation if needed (it uses tagged JSON).
3. `codec::encode_property` / `decode_value` will pick up the new variant
   automatically via `serde_json`.
4. Add a round-trip test in `polargraph_storage::codec::tests`.
