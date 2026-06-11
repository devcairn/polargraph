# PolarGraph DB Engine — Roadmap

## Vision

PolarGraph is a self-hosted, embeddable graph database built for applications that need to store and query heterogeneous, dynamically-typed entities alongside semantic vector embeddings — in a single store, without giving up temporal correctness or graph traversal power. Where most systems force a choice between a graph database, a vector store, and a document store, PolarGraph treats all three as first-class citizens of one unified triple-based data model.

---

## Current State (as of June 2026)

The core engine is complete and production-capable. All major subsystems are operational:

**Storage & indexing** — RocksDB hexastore (6 CFs), bitemporal MVCC with wall-clock timestamps, predicate interning, full-text trigram index (TRI CF), pure-Rust HNSW vector index (memory + mmap modes), bulk SST import, compaction and bitemporal retention with scheduled runs.

**Query** — Pattern-based query planner, conjunctive Datalog evaluator, recursive rules and transitive closure, view projection, schema-aware optimization (EdgeTypeRegistry domain/range pruning), Cypher layer (MATCH/WHERE/RETURN, text predicates, VECTOR_NEAR, aggregations, write operations), ExplainQuery, server-streaming query variants.

**Schema & ontology** — Dynamic node and edge type registries, field validation, domain/range constraints, cardinality, inverse predicates, parent types, ValidateOntology RPC, versioned schema migrations (MigrationRunner, dry_run, RPC).

**gRPC server** — 40+ RPCs covering all subsystems: insert, query, vector, schema, backup, replication, diagnostics, transactions, migrations, retention. Wire transactions (BeginTx/CommitTx/RollbackTx) with idle-TTL cleanup. Filtered vector search with type-cache (13× speedup). VectorSeedQuery (ANN → Datalog join). Diagnostics (ShowIndexes, ShowStats). Bitemporal time-travel queries (as_of_valid_time / as_of_tx_time).

**Infrastructure** — API key auth (tower middleware, constant-time compare, multi-key rotation), per-client rate limiting (token bucket, DashMap), TLS on gRPC and HTTP, gRPC health checks, graceful shutdown (CancellationToken + watchdog), query timeouts and slow query logging, TOML config file.

**Observability** — TelemetryLayer (structured per-RPC logging), Prometheus /metrics endpoint, JSON and pretty log formats.

**Management UI** — Dark-theme SPA embedded in binary (5 tabs: Query, Schema, Insert, Search, Status), query history ring buffer, Mermaid.js ER diagram, API key overlay with localStorage persistence.

**REST gateway** — `polargraph-rest` binary: HTTP/JSON proxy over gRPC, Datalog rules support, pattern string format, gRPC→HTTP status mapping, TLS CA cert forwarding.

**Replication** — WAL streaming replication over gRPC (no shared filesystem), exponential backoff reconnect, last_applied_seq persistence, write-guard on replicas.

**SDKs** — Python (sync + async), Go (functional options), TypeScript/JavaScript (grpc-js, full types).

**Deployment** — Multi-stage Docker image, Helm chart (8 Kubernetes resources: Namespace, ConfigMap, Secret, PVC, Deployment, Services×3, HPA), CI/CD (GitHub Actions: test, lint, release, docker-build). Docker E2E test suite (`tests/e2e/`) in progress.

---

## Near-Term

### Management UI improvements

The UI covers basic operations but is missing several operational panels that would make it self-sufficient for day-to-day administration.

1. **Backups tab** — list existing backups (name, size, timestamp), Create Backup button, Purge Old with keep-N input, offline restore instructions.

2. **Runtime API key management** — `AddApiKey` / `RevokeApiKey` gRPC RPCs backed by `Arc<RwLock<Vec<String>>>` in the server struct; UI tab to view active keys, add new ones, and revoke by prefix — no restart required.

3. **Retention tab** — display current `RetentionPolicy` (tx age, vt lookback), Run Now button, last-run timestamp and deleted-count from Prometheus.

4. **Replication tab** — WAL lag gauge, `last_applied_seq`, primary/replica mode badge, connected/reconnecting status.

5. **Migrations tab** — current schema version, applied migration history with timestamps, Run Migrations button with dry-run toggle.

6. **Live metrics panel** — sparklines for key counters (RPC rate, triple count, slow queries, WAL lag), auto-refreshing every 10 s.

7. **Type registration via UI** — form in the Schema tab to register node and edge types without writing raw JSON; field list builder for `FieldDef` entries.

### Docker E2E test suite

Complete the `tests/e2e/` suite. Covers the full client→server path in a container: insert, query, Cypher, vector search, wire transactions, backup, and replication handoff. Should run as a CI job after the docker-build step.

---

## Future

### Geo/spatial search

`GEO` column family with geohash-based prefix indexing; `NEAR(lat, lon, radius_km)` Cypher predicate; `polargraph-core::geo` types. Enables location-aware graph queries without a separate spatial index.

---

## Long-Term

### Distributed sharding

Consistent hash ring over UUID v7 node IDs to shard the SPO family across nodes while keeping OSP/PSO replicated. Requires stable single-primary WAL replication (already done) as a foundation. Active-active comes after single-primary sharding is stable.

### Plugin node type loaders

WASM sandbox or shared-library ABI for custom node type serialization, domain-specific embedding hooks, and external validator logic. Enables geospatial, time-series, and document node types without changes to the core engine.

---

## Key Design Principles

These are not constraints imposed by past decisions but properties worth preserving as the engine grows.

**Bitemporal by default.** Every fact carries both valid time and transaction time. This should never be optional or an add-on mode — it is the cost of correctness for any system that models a changing world. Future features should assume bitemporality and not introduce paths that bypass it.

**RocksDB as the storage substrate.** The hexastore's fixed-width key layout, the MVCC snapshot mechanism, and the SST bulk import all rely on RocksDB's specific guarantees. Replacing the storage backend would require rethinking the entire index structure. The boundary is `polargraph-storage`; everything above it is storage-agnostic.

**Datalog as the query language.** Datalog's semantics — conjunctive pattern matching, recursive rules, set-based results — map cleanly onto a triple store. New query features should extend Datalog rather than introduce a parallel query language. The Cypher layer compiles to Datalog rather than bypassing it.

**Vector and graph are peers, not a bolt-on.** The HNSW index should be a first-class scan source in the query planner, not a post-processing step. A query like "nodes semantically similar to X that are also reachable from Y via predicate Z" should be expressible as a single Datalog query, with the planner choosing the most selective entry point.

**Embeddability first.** `polargraph-storage` is a library, not a daemon dependency. The server binary is one packaging of the engine, not the only one. API design decisions should preserve the ability to embed the store directly in an application process.
