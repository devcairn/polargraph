# PolarGraph DB Engine — Roadmap

## Vision

PolarGraph is a self-hosted, embeddable graph database built for applications that need to store and query heterogeneous, dynamically-typed entities alongside semantic vector embeddings — in a single store, without giving up temporal correctness or graph traversal power. Where most systems force a choice between a graph database, a vector store, and a document store, PolarGraph treats all three as first-class citizens of one unified triple-based data model.

---

## Current State (as of June 2026)

The core engine is working end-to-end. Here's what's fully operational:

**Storage layer** (`polargraph-storage`): The RocksDB-backed hexastore is complete across all six column families with bitemporal key encoding and predicate interning. Optimistic MVCC with snapshot-isolated reads and conflict detection is implemented. A pure-Rust HNSW vector index (`polargraph-storage::hnsw`) is persisted in a dedicated `hnsw` CF. Node and edge type schemas are stored as triples in `polargraph-storage::registry` and loaded into memory at startup.

**Query layer** (`polargraph-query`): Pattern-based query planner, conjunctive Datalog evaluator, recursive rules, transitive closure (`reachable_from`, `reachable_from_hops`), and view projection are all implemented and tested.

**Schema registries**: `NodeTypeRegistry` and `EdgeTypeRegistry` support runtime registration of type schemas with domain/range constraints, required/optional field declarations, and property-map validation. `EdgeTypeRegistry::list_predicates_between` queries which predicates are valid between two node types.

**Server** (`polargraph-server`): The gRPC server (`polargraphd`) implements 23 RPCs: `Insert`, `Query` (with bitemporal `as_of_valid_time` / `as_of_tx_time` time-travel), `InsertVector`, `SearchVector`, `Reachable`, `RegisterNodeType`, `GetNodeType`, `ListNodeTypes`, `ValidateNode`, `RegisterEdgeType`, `GetEdgeType`, `ListEdgeTypes`, `ValidateEdge`, `ListPredicatesBetween`, `SearchVectorFiltered`, `SearchVectorInSet`, `BatchInsertVectors`, `VectorSeedQuery`, `CreateBackup`, `ListBackups`, `PurgeOldBackups`, `RunRetention`, `StreamWal`. A web management UI (dark-theme SPA, embedded in binary) is served at port 8080. CLI flags and env-var fallbacks are wired up. Docker packaging is in place.

**Backup** (`polargraph-storage::backup`): `BackupManager` wraps RocksDB's `BackupEngine` for incremental point-in-time backups. Enabled with `--backup-dir PATH`. Unchanged SST files are hard-linked between backups (space-efficient). Restore is an offline operation documented in `docs/architecture.md`.

**REST gateway** (`polargraph-rest`): A standalone `polargraph-rest` binary exposes HTTP/JSON endpoints (`POST /query`, `POST /insert`, `GET /triples`, `POST /vector/search`, `GET /health`, `POST /explain`) that proxy to a running `polargraphd` gRPC server. Useful for clients that cannot use a gRPC stub. Patterns use a `?var :pred ?obj` string format. Auth key forwarding, TLS CA support, and gRPC→HTTP status mapping are all wired up.

---

## Near-Term (next few weeks)

These are the next concrete priorities given the current engine state.

### ~~Filtered vector search optimisation~~ ✓ Done

**Implemented June 2026.** `PolarGraphServer` now carries a
`Arc<RwLock<HashMap<String, HashSet<NodeId>>>>` type cache. On startup it is
populated by scanning existing `__type` triples (O(N) once); on every `Insert`
commit it is updated incrementally in O(k) where k is the number of `__type`
triples in the batch. `SearchVectorFiltered` with `NodeTypeFilter` reads the
cache under a shared read lock — zero triple scans per query.

Result at 5 000 nodes, 128 dims, k = 10: **6 815 µs → 515 µs (13× faster)**,
recall@10 unchanged at 0.934. The O(1) HashSet lookup in the post-filter also
fixed a secondary bug where the original code used `Vec::contains` (O(|allowed|)
per candidate) instead of a HashSet (O(1)).

### ~~Query planner integration for HNSW~~ ✓ Done

**Implemented June 2026.** The `VectorSeedQuery` RPC combines ANN vector search
with a conjunctive Datalog graph query in a single server-side call. The k ANN
hits become initial bindings for a named `seed_variable`; graph patterns then
join against those seeds using `execute_query_seeded` (a new entry point added
to `polargraph_query::datalog` that accepts caller-supplied initial bindings
instead of starting from a single empty row).

Optional `NodeTypeFilter` and `ReachabilityFilter` can be applied as ANN
pre-filters before the Datalog join, using the same type cache as
`SearchVectorFiltered`. When `patterns` is empty the RPC is equivalent to
`SearchVector` but with scored `ScoredBinding` output. 4 integration tests
cover the pattern join, type-filtered seeds, empty-patterns pass-through, and
the no-edges zero-result case.

### ~~Backup and restore~~ ✓ Done

**Implemented June 2026.** `polargraph-storage::backup::BackupManager` wraps
RocksDB's `BackupEngine`. Three new gRPC RPCs — `CreateBackup`, `ListBackups`,
`PurgeOldBackups` — are available when the server is started with
`--backup-dir <PATH>`. Incremental backups hard-link unchanged SST files so
only the delta is copied on each run. Restore is an offline operation: stop the
server, call `restore_from_backup`, restart against the restored directory. Full
runbook in `docs/architecture.md`.

### ~~Bitemporal time-travel queries~~ ✓ Done

**Implemented June 2026.** `QueryRequest` now carries two optional filter fields:

- `as_of_tx_time` (int64, unix µs) — only triples committed at or before this
  wall-clock time are visible. Maps directly to the MVCC snapshot timestamp,
  since the oracle uses wall-clock µs.
- `as_of_valid_time` (int64, unix µs) — only triples whose valid-time window
  `[vt_start, vt_end)` contains this value are returned. The filter runs inside
  `snapshot_scan_cf` **before** MVCC deduplication, which is the correct order
  for accurate historical reconstruction.

Both filters are independent and can be combined for a full bitemporal point
query. 6 gRPC integration tests cover: zero-value pass-through, window boundary
behaviour, two-version correctness, tx-time before/after commit, and the
combined case. Documented in `docs/architecture.md` under "Time-travel queries".

### ~~API key authentication~~ ✓ Done

**Implemented June 2026.** `polargraph_server::auth::ApiKeyLayer` is a tower
`Layer` applied at the transport level. Callers must include
`Authorization: Bearer <key>` or `Authorization: ApiKey <key>` on every
request when auth is enabled. Multiple keys are accepted simultaneously to
allow zero-downtime rotation (`--api-key key1 --api-key key2` or
`POLARGRAPH_API_KEY=key1,key2`). Key comparison uses `subtle::ConstantTimeEq`
to prevent timing attacks. `ReplicaStatus` is exempt so load-balancer health
probes work without a key. 6 gRPC integration tests + 7 unit tests.

### ~~Observability~~ ✓ Done

**Implemented June 2026.** `polargraph_server::telemetry::TelemetryLayer` is a
tower `Layer` that wraps every gRPC handler. Each request opens an `info_span!`
carrying `method` and `peer` so child log events inherit those fields. On
completion it logs status + duration and increments
`polargraph_rpc_requests_total{method,status}` / `polargraph_rpc_duration_seconds{method}`.

Log format is selectable at runtime: `--log-format json` emits
newline-delimited JSON (for log aggregators); `--log-format pretty` (default)
emits human-readable coloured output. `--log-level` / `RUST_LOG` controls
verbosity.

A Prometheus `/metrics` HTTP endpoint runs on a separate port (default 9090,
`--metrics-port` / `POLARGRAPH_METRICS_PORT`) served by `axum`. Metrics
include RPC counters/histograms, triple/vector-space gauges, WAL replication
lag, backup size, and compaction delete count. Use `--no-metrics` to disable.
`docker-compose.yml` now exposes port 9090 and sets `LOG_FORMAT=json`.
Documented in `docs/architecture.md` "Observability" section.

### Schema-aware query optimisation

`EdgeTypeRegistry` knows which predicates are valid between which node types. The query planner can use this to prune bind patterns early — if a pattern has a bound predicate and a bound object type, only SPO/SOP scans where the subject type matches the domain need to be considered. This is a pure optimisation and does not change query semantics.

### Health-check endpoint

The Docker image lacks a health probe. Add a minimal `Health` RPC (or a lightweight HTTP handler) that returns `OK` once the store is open. This is needed for container orchestration (Kubernetes liveness/readiness probes, `docker-compose` `healthcheck`).

---

## Production Readiness Backlog

These items are actively being worked on and are required before PolarGraph can be considered production-ready for general deployment.

### TLS (gRPC server + replica connection) 🔄 In Progress

Encrypt traffic in transit for both client-to-server gRPC connections and the replica WAL streaming connection. The server should accept `--tls-cert` / `--tls-key` flags (PEM files) and the replica client should validate the server certificate. Mutual TLS (mTLS) is a stretch goal for service-to-service auth.

### Health check (grpc.health.v1.Health + HTTP /health) 🔄 In Progress

Implement the standard gRPC health checking protocol (`grpc.health.v1.Health/Check` and `Watch`) alongside a lightweight HTTP `GET /health` endpoint on the metrics or UI port. Required for Kubernetes liveness/readiness probes and `docker-compose healthcheck`. The check should report `SERVING` once the store is open and `NOT_SERVING` during startup/shutdown. Supersedes the near-term "Health-check endpoint" item.

### Rate limiting (per-client token bucket, tower middleware) 🔄 In Progress

Add a tower `Layer` that enforces a per-client (keyed by API key or peer IP) token-bucket rate limit on gRPC requests. Configurable via `--rate-limit-rps N` / `POLARGRAPH_RATE_LIMIT_RPS`. Requests exceeding the limit return `Status::resource_exhausted`. A Prometheus counter (`polargraph_rate_limited_total{client}`) tracks rejections. The middleware should sit between `ApiKeyLayer` and `TelemetryLayer` so rejected requests are still logged.

### Config file (TOML/YAML as alternative to CLI flags) 🔄 In Progress

Support loading server configuration from a TOML or YAML file (e.g. `polargraph.toml`) as an alternative to CLI flags and environment variables. Precedence: CLI flags > env vars > config file > built-in defaults. The config file path is set via `--config PATH` / `POLARGRAPH_CONFIG`. All existing flags should have config-file equivalents. Useful for Kubernetes ConfigMap mounts and reproducible deployments.

### CI/CD (GitHub Actions: cargo test + clippy on PR) 📋 Planned

Add a GitHub Actions workflow that runs `cargo test`, `cargo clippy -- -D warnings`, and `cargo fmt --check` on every pull request and push to `main`. Matrix should cover the MSRV (1.78) and stable. A separate job should build the Docker image to catch `Dockerfile` regressions. Failing checks block merge.

### Kubernetes manifests (Deployment, Service, PVC, ConfigMap) 📋 Planned

Add a `k8s/` directory with production-ready manifests: `Deployment` (with liveness/readiness probes pointing at the health endpoint), `Service` (ClusterIP for gRPC, optional LoadBalancer), `PersistentVolumeClaim` for the data directory, and `ConfigMap` / `Secret` for configuration and API keys. A `kustomization.yaml` should allow overlay-based environment customisation.

### REST gateway (HTTP/JSON wrapper over gRPC) 📋 Planned

Provide a thin HTTP/JSON translation layer for callers that cannot use gRPC. The gateway can be implemented as additional axum routes on the existing UI port (extending `ui_api`) or as a separate binary. Priority endpoints: `POST /triples` (Insert), `POST /query` (Query), `POST /vector/search` (SearchVector). Use `serde_json` for request/response mapping rather than introducing a code-generation dependency.

### Scheduled compaction (cron-style RunRetention trigger) 📋 Planned

Allow the server to run `RunRetention` automatically on a configurable schedule without requiring an external cron job or manual RPC call. Add `--compaction-schedule CRON` / `POLARGRAPH_COMPACTION_SCHEDULE` (standard 5-field cron expression, e.g. `0 2 * * *` for daily at 02:00). A `tokio` background task parses the schedule and calls the compaction manager at each firing, respecting the `CancellationToken` for graceful shutdown.

### Schema migrations (versioned up/down) 📋 Planned

As node and edge type schemas evolve, existing data may become invalid against the new schema. Add a migration framework: each schema change is a numbered migration with an `up` function (e.g. backfill a new required field, rename a predicate) and an optional `down` function. Migrations are stored as triples so the applied version is tracked bitemporally. The server refuses to start if the data-dir schema version is ahead of the binary's known migrations, preventing accidental rollbacks.

---

## Medium-Term

### Schema and ontology layer

Build on the dynamic node type registry to add optional predicate-level constraints: allowed value types, cardinality, inverse-predicate declarations, and subtype relationships. All constraints are stored as triples, so they participate in bitemporal versioning — schema changes are auditable and reversible. The query planner should be able to use type information for better index selection.

### ~~Bulk import via SST ingestion~~ ✅ Done

**Implemented June 2026.** `polargraph-storage::SstImporter` buffers triples
in memory, sorts keys per column family, writes SST files via
`SstFileWriter`, and ingests them using `ingest_external_file_cf`. The
`polargraph-import` binary reads N-Triples (`.nt`) files, hashes URIs to
stable `NodeId`s via xxHash3-128, and processes them in configurable batches
(default 100 000 triples). Predicates are automatically interned before each
batch; the MVCC oracle is advanced atomically so imported data is immediately
visible via normal queries. The tool is offline-only — the server must be
stopped before running. See `docs/architecture.md` for the full runbook.

### ~~Disk-backed HNSW (memmap paging)~~ ✅ Done

**Implemented.** `StorageMode::Mmap` is available on every `VectorSpaceDef`.
Raw float vectors are stored in `<data_dir>/vectors/<space>.vecs` via
`memmap2::MmapMut`; graph topology stays in the RocksDB HNSW CF. Set
`storage_mode: "mmap"` when registering a node type to opt in. Memory mode
remains the default for backward compatibility. See `docs/benchmarks.md` for
the RAM comparison and `docs/architecture.md` for implementation details.

### ~~Basic HTTP query API and management UI~~ ✅ Done

**Implemented June 2026.** `polargraph-server::ui_api` provides a REST JSON
API (`/api/status`, `/api/node-types`, `/api/edge-types`, `/api/metrics`,
`/api/query`, `/api/insert`, `/api/search`) served by `axum` on a dedicated
port (default 8080, `--ui-port` / `POLARGRAPH_UI_PORT`). Handlers call into
the `PolarGraphService` trait in-process — no network hop. A dark-theme SPA
is embedded in the binary via `include_str!("ui.html")` and served at `GET /`;
it features 5 tabs (Query, Schema, Insert, Search, Status), an auth overlay
for API key entry, and stores the key in `localStorage`. Use `--no-ui` to
disable. 3 integration tests cover the HTML root, status fields, and 401
enforcement.

---

## Long-Term

### Replication

**Read replicas via RocksDB secondary instances** (done — `docs/scaling.md`). `TripleStore::open_secondary` opens any running instance as a read-only secondary against a shared data directory. `try_catch_up_with_primary` pulls new SST files non-destructively. `--replica-of PATH` CLI flag, background catch-up task, and `ReplicaStatus` RPC are implemented. Consistency model is eventual; shared-filesystem access is required.

**Next step: log-shipping replication.** The RocksDB secondary approach requires shared filesystem access. A network-transparent replication path would stream the write log (MVCC commit batches + oracle advances) over gRPC so replicas can run on separate hosts without shared storage. The `TimestampOracle` would need to become a distributed Hybrid Logical Clock (HLC) to preserve global monotonicity across nodes. Write distribution (active-active) comes after single-primary log shipping is stable.

### ~~Compaction and retention policies~~ (done)

`CompactionManager::run_retention` in `polargraph-storage::compaction` scans
all six hexastore CFs and deletes entries that exceed a `RetentionPolicy`
(tx-time age or valid-time lookback). After deletion it triggers
`compact_range_cf` on modified CFs to reclaim disk space. The `RunRetention`
gRPC RPC allows triggering this at runtime; `--retention-tx-age-secs` runs it
at startup. The oracle was changed to wall-clock µs so `tt` values are real
timestamps and can be compared against retention windows meaningfully.

### Distributed sharding

Longer-term, the hexastore's fixed-width keys and UUID v7 node IDs (time-ordered, cluster-safe) were chosen with distribution in mind. A consistent hash ring over node IDs can shard the SPO family across nodes while keeping OSP and PSO centralized or replicated. This is a significant architectural step and should not be attempted before replication is stable.

### Plugin node type loaders

Allow external code to register node type schemas and custom serialization/deserialization logic at runtime via a plugin interface (likely a shared library ABI or WASM sandbox). This enables domain-specific node types — geospatial coordinates, time-series measurements, structured documents — to plug into the vector and graph query layers without changes to the core engine.

---

## Key Design Principles

These are not constraints imposed by past decisions but properties worth preserving as the engine grows.

**Bitemporal by default.** Every fact carries both valid time and transaction time. This should never be optional or an add-on mode — it is the cost of correctness for any system that models a changing world. Future features should assume bitemporality and not introduce paths that bypass it.

**RocksDB as the storage substrate.** The hexastore's fixed-width key layout, the MVCC snapshot mechanism, and the planned SST bulk import all rely on RocksDB's specific guarantees. Replacing the storage backend would require rethinking the entire index structure. The boundary is `polargraph-storage`; everything above it is storage-agnostic.

**Datalog as the query language.** Datalog's semantics — conjunctive pattern matching, recursive rules, set-based results — map cleanly onto a triple store. The conjunctive evaluator and recursive fixpoint are already implemented. New query features should extend Datalog rather than introduce a parallel query language.

**Vector and graph are peers, not a bolt-on.** The HNSW index should be a first-class scan source in the query planner, not a post-processing step. A query like "nodes semantically similar to X that are also reachable from Y via predicate Z" should be expressible as a single Datalog query, with the planner choosing the most selective entry point.

**Embeddability first.** `polargraph-storage` is a library, not a daemon dependency. The server binary is one packaging of the engine, not the only one. API design decisions should preserve the ability to embed the store directly in an application process.
