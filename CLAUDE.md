# PolarGraph DB Engine — Codebase Guide

This file is the primary orientation document for contributors and AI coding
assistants working in this repository. Keep it up to date when making
structural changes.

---

## What this is

PolarGraph is a purpose-built, Rust-based graph database engine. The core
data model is a **triple store** (subject → predicate → object) with:

- **Bitemporal versioning** on every fact (valid time + transaction time)
- A **hexastore index** (6 RocksDB column families) for O(log n) lookups on
  any (S, P, O) bind pattern
- **Optimistic MVCC** for snapshot-isolated reads and conflict-detected writes
- A **View** system for projecting subsets of the graph with label overrides
- A planned Datalog query layer and gRPC server (stubs exist, not yet wired)

---

## Workspace layout

```
polargraph/
├── Cargo.toml                  # workspace root
├── CLAUDE.md                   # this file
├── docs/
│   ├── architecture.md         # design narrative
│   └── api-reference.md        # public API surface
└── crates/
    ├── polargraph-core/        # primitive types, no I/O
    ├── polargraph-storage/     # RocksDB triple store + MVCC
    ├── polargraph-query/       # query planner + projection (stubs)
    ├── polargraph-server/      # gRPC binary (skeleton)
    ├── polargraph-bench/       # end-to-end benchmark scenarios (binary)
    └── polargraph-import/      # bulk N-Triples importer via SST ingestion (binary)
```

Dependency order (no cycles): `core` ← `storage` ← `query` ← `server` ← `bench`
                                                    ↑
                                           `polargraph-import` also depends only on `core` + `storage`

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
| `store` | `TripleStore` — main handle, insert, scan, predicate interning, named HNSW spaces |
| `mvcc` | `TimestampOracle`, `Transaction`, `Snapshot`, `ConflictError` |
| `keys` | Fixed-width key encoding/decoding for all 6 CFs |
| `codec` | Value serialization (discriminant + temporal + payload) |
| `cf` | Column family name constants |
| `error` | `StorageError` |
| `hnsw` | `HnswIndex` — pure-Rust HNSW, named-space key helpers, serialize/deserialize |
| `registry` | `NodeTypeRegistry`, `EdgeTypeRegistry`, `ValidationError` |
| `sst_import` | `SstImporter`, `ImportStats` — bulk triple import via RocksDB SST file ingestion |
| `compaction` | `CompactionManager`, `RetentionStats` — bitemporal retention scan + RocksDB compaction |
| `store` | `StoreMode` (`Primary`/`Secondary`); `TripleStore::open_secondary` + `try_catch_up_with_primary` — read-only secondary instance |

`TripleStore` is `Clone` (Arc-backed). Prefer passing it by clone rather
than wrapping it again in Arc.

### `polargraph-query`

Pattern-based query evaluation and view projection.

| Module | Contents |
|--------|----------|
| `planner` | `Pattern`, `IndexChoice`, `choose_index` — picks cheapest CF for a bind pattern |
| `eval` | `evaluate(pattern, snapshot)` — drives the storage scan the planner chose |
| `projection` | `ProjectedTriple`, `apply_view` — filters and label-remaps triples for a View |
| `datalog` | `Query`, `VarPattern`, `Term`, `Bindings`, `execute_query` — conjunctive query evaluator; `Rule`, `DerivedFacts`, `execute_recursive`, `reachable_from` — recursive / transitive-closure queries |

### `polargraph-server`

Binary crate (`polargraphd`) and companion library. Exposes the storage and
query layers over gRPC.

| Module | Contents |
|--------|----------|
| `proto` | Generated types from `polargraph.proto` (tonic/prost) |
| `service` | `PolarGraphServer` — implements all RPCs; carries `NodeTypeRegistry` and `EdgeTypeRegistry` |
| `convert` | Conversions between proto wire types and Rust domain types |

Configuration via CLI flags or environment variables (flags take priority):

| Flag | Env variable | Default | Description |
|---|---|---|---|
| `--data-dir PATH` | `POLARGRAPH_DATA_DIR` | `/data` | RocksDB data directory |
| `--listen ADDR` | `POLARGRAPH_LISTEN_ADDR` | `0.0.0.0:50051` | gRPC listen address |
| `--log FILTER` | `RUST_LOG` | `info` | Log filter (same syntax as `RUST_LOG`) |
| `--backup-dir PATH` | `POLARGRAPH_BACKUP_DIR` | *(none)* | Backup directory (optional) |

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

---

## Key design decisions

### Triple model

Everything is a `Triple`. There are two variants:

- **Relation**: subject → predicate → object (both `NodeId`)
- **Property**: subject → predicate → value (scalar `Value`)

Property triples use a 16-byte sentinel (`0xFF × 16`) in the object slot of
every index key, so both variants share the same index structure.

### Hexastore (6-CF index)

Every triple is written atomically to all 6 column families via a single
`WriteBatch`. Each CF supports a different bind pattern:

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
