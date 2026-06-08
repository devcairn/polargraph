# PolarGraph DB Engine — Roadmap

## Vision

PolarGraph is a self-hosted, embeddable graph database built for applications that need to store and query heterogeneous, dynamically-typed entities alongside semantic vector embeddings — in a single store, without giving up temporal correctness or graph traversal power. Where most systems force a choice between a graph database, a vector store, and a document store, PolarGraph treats all three as first-class citizens of one unified triple-based data model.

---

## Current State (as of June 2026)

The core engine is working end-to-end. Here's what's fully operational:

**Storage layer** (`polargraph-storage`): The RocksDB-backed hexastore is complete across all six column families with bitemporal key encoding and predicate interning. Optimistic MVCC with snapshot-isolated reads and conflict detection is implemented. A pure-Rust HNSW vector index (`polargraph-storage::hnsw`) is persisted in a dedicated `hnsw` CF. Node and edge type schemas are stored as triples in `polargraph-storage::registry` and loaded into memory at startup.

**Query layer** (`polargraph-query`): Pattern-based query planner, conjunctive Datalog evaluator, recursive rules, transitive closure (`reachable_from`, `reachable_from_hops`), and view projection are all implemented and tested.

**Schema registries**: `NodeTypeRegistry` and `EdgeTypeRegistry` support runtime registration of type schemas with domain/range constraints, required/optional field declarations, and property-map validation. `EdgeTypeRegistry::list_predicates_between` queries which predicates are valid between two node types.

**Server** (`polargraph-server`): The gRPC server (`polargraphd`) implements 18 RPCs: `Insert`, `Query`, `InsertVector`, `SearchVector`, `Reachable`, `RegisterNodeType`, `GetNodeType`, `ListNodeTypes`, `ValidateNode`, `RegisterEdgeType`, `GetEdgeType`, `ListEdgeTypes`, `ValidateEdge`, `ListPredicatesBetween`, `SearchVectorFiltered`, `SearchVectorInSet`, `BatchInsertVectors`, `VectorSeedQuery`. CLI flags and env-var fallbacks are wired up. Docker packaging is in place.

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

### Schema-aware query optimisation

`EdgeTypeRegistry` knows which predicates are valid between which node types. The query planner can use this to prune bind patterns early — if a pattern has a bound predicate and a bound object type, only SPO/SOP scans where the subject type matches the domain need to be considered. This is a pure optimisation and does not change query semantics.

### Health-check endpoint

The Docker image lacks a health probe. Add a minimal `Health` RPC (or a lightweight HTTP handler) that returns `OK` once the store is open. This is needed for container orchestration (Kubernetes liveness/readiness probes, `docker-compose` `healthcheck`).

---

## Medium-Term

### Schema and ontology layer

Build on the dynamic node type registry to add optional predicate-level constraints: allowed value types, cardinality, inverse-predicate declarations, and subtype relationships. All constraints are stored as triples, so they participate in bitemporal versioning — schema changes are auditable and reversible. The query planner should be able to use type information for better index selection.

### Bulk import via SST ingestion

`TripleStore::insert` goes through the write path one triple at a time, which is fine for transactional workloads but slow for large initial loads. RocksDB's SST file ingestion (`IngestExternalFile`) allows bulk-loading pre-sorted key files at close to storage bandwidth. A `polargraph-import` tool (or a `bulk_insert` API) should build all six CF files in parallel, sort them, and ingest them atomically. This is important for the practical use case of bootstrapping a large knowledge graph from an existing dataset.

### ~~Disk-backed HNSW (memmap paging)~~ ✅ Done

**Implemented.** `StorageMode::Mmap` is available on every `VectorSpaceDef`.
Raw float vectors are stored in `<data_dir>/vectors/<space>.vecs` via
`memmap2::MmapMut`; graph topology stays in the RocksDB HNSW CF. Set
`storage_mode: "mmap"` when registering a node type to opt in. Memory mode
remains the default for backward compatibility. See `docs/benchmarks.md` for
the RAM comparison and `docs/architecture.md` for implementation details.

### Basic HTTP query API

A lightweight HTTP/JSON API alongside the existing gRPC interface. The primary target is developer tooling and scripting — curl-able queries, a simple `POST /query` endpoint accepting a Datalog pattern body and returning JSON triples. This doesn't need to replace gRPC for production clients but dramatically lowers the barrier to exploration and integration testing.

---

## Long-Term

### Replication

Log-structured writes are already partially implied by the append-only bitemporal model. The path forward is to extract a structured write log from the MVCC commit path and stream it to follower replicas. The `TimestampOracle` becomes a distributed Hybrid Logical Clock (HLC) so followers can participate in reads without a round-trip to the primary. Read-scale-out is the first target; write distribution comes later.

### Compaction and retention policies

The bitemporal model means the store grows monotonically — superseded fact versions are never deleted by default. A configurable retention policy should allow purging versions outside a rolling window (e.g. "keep the last 90 days of transaction history, but retain all valid-time versions"). The compaction job runs as a background RocksDB compaction filter, so it doesn't block reads or writes.

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
