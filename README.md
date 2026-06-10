# PolarGraph DB Engine

A purpose-built, Rust graph database engine — triple store with bitemporal versioning, a hexastore index, MVCC concurrency, pure-Rust HNSW vector search, and a Datalog query evaluator, exposed over gRPC and a REST gateway.

---

## Key features

- **Triple store** — every fact is an atomic (subject, predicate, object) statement; relations and properties share one index
- **Bitemporal versioning** — valid time (when a fact was true in the world) and transaction time (when it was recorded) are independently queryable on every triple
- **Hexastore index** — six RocksDB column families give O(log n) lookups on any (S, P, O) bind pattern with no secondary scans
- **Optimistic MVCC** — snapshot-isolated reads, conflict-detected writes; timestamps are wall-clock µs so they compare to real time-travel queries
- **HNSW vector index** — pure-Rust HNSW per named space; Memory or Mmap storage modes; dimension-validated against node type schemas
- **Datalog queries** — conjunctive pattern joins, recursive rules, transitive reachability, and hybrid vector-seed queries
- **gRPC API** — full-featured `polargraph.v1.PolarGraphService` via tonic
- **REST gateway** — standalone `polargraph-rest` binary proxies HTTP/JSON to gRPC for clients that can't use a gRPC stub
- **Production features** — TLS, API key auth, rate limiting, Prometheus metrics, management UI, graceful shutdown, WAL streaming replication, incremental backups, bulk SST import, schema migrations

---

## Quick start

### Docker

```bash
git clone <this repo>
cd polargraph
docker compose up
```

The server starts on `localhost:50051` (gRPC), `localhost:8080` (management UI), and `localhost:9090` (Prometheus metrics).

### Build from source

Requires Rust 1.78+.

```bash
# Build all crates
cargo build --release

# Run the server (stores data in ./data by default)
./target/release/polargraphd --data-dir ./data --listen 0.0.0.0:50051

# Build and run the REST gateway (requires polargraphd running)
cargo build --release -p polargraph-rest
./target/release/polargraph-rest --upstream http://localhost:50051
```

Run the tests:

```bash
cargo test
```

---

## Configuration

Configuration priority: **CLI flag > environment variable > config file > built-in default**

Copy `polargraph.example.toml` to `polargraph.toml` in your working directory (or `~/.config/polargraph/config.toml`) for file-based configuration. Every option is commented in that file.

### Key flags

| Flag | Env variable | Default | Description |
|------|-------------|---------|-------------|
| `--data-dir PATH` | `POLARGRAPH_DATA_DIR` | `/data` | RocksDB data directory |
| `--listen ADDR` | `POLARGRAPH_LISTEN_ADDR` | `0.0.0.0:50051` | gRPC listen address |
| `--api-key KEY` | `POLARGRAPH_API_KEY` | *(none)* | API key; repeatable; comma-separated in env |
| `--tls-cert PATH` | `POLARGRAPH_TLS_CERT` | *(none)* | PEM certificate — enables TLS when combined with `--tls-key` |
| `--tls-key PATH` | `POLARGRAPH_TLS_KEY` | *(none)* | PEM private key |
| `--backup-dir PATH` | `POLARGRAPH_BACKUP_DIR` | *(none)* | Incremental RocksDB backup directory |
| `--metrics-port PORT` | `POLARGRAPH_METRICS_PORT` | `9090` | Prometheus `/metrics` port |
| `--ui-port PORT` | `POLARGRAPH_UI_PORT` | `8080` | Management UI port |
| `--rate-limit-rps N` | `POLARGRAPH_RATE_LIMIT_RPS` | `0` | Token-bucket rate limit per client IP; 0 = disabled |
| `--query-timeout-ms MS` | `POLARGRAPH_QUERY_TIMEOUT_MS` | `30000` | Max query execution time; 0 = unlimited |
| `--replica-of URL` | `POLARGRAPH_REPLICA_OF` | *(none)* | gRPC primary address — enables replica mode |

See `polargraph.example.toml` for the full list including replication, retention, and observability options.

---

## REST gateway

Start `polargraph-rest` alongside `polargraphd`:

```bash
polargraph-rest \
  --upstream http://localhost:50051 \
  --listen 0.0.0.0:8000 \
  --api-key my-key
```

### Example requests

**Insert a relation triple:**
```bash
curl -s -X POST http://localhost:8000/insert \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{"subject":"<uuid-a>","predicate":"knows","object":"<uuid-b>"}'
```

**Conjunctive graph query:**
```bash
curl -s -X POST http://localhost:8000/query \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{"patterns":["?s :knows ?o","?o :name ?n"]}'
```

**k-NN vector search:**
```bash
curl -s -X POST http://localhost:8000/vector/search \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{"space":"articles","query":[0.1,0.2,0.3],"k":5}'
```

**Health check:**
```bash
curl http://localhost:8000/health
# {"status":"ok","mode":"primary","triples":12345}
```

**Query plan (no DB access):**
```bash
curl -s -X POST http://localhost:8000/explain \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{"patterns":["?s :knows ?o","?o :name ?n"]}'
```

Pattern strings: `?varname` for variables, `_` for wildcards, UUID or plain text for bound terms, optional `:` prefix on predicates.

**Cypher query:**
```bash
curl -s -X POST http://localhost:8000/cypher \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{"cypher":"MATCH (a:Person)-[:knows]->(b:Person) WHERE a.name = \"Alice\" RETURN b"}'
```

**Vector + graph query (VECTOR_NEAR):**
```bash
curl -s -X POST http://localhost:8000/cypher \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "cypher": "MATCH (a:Document)-[:cites]->(b) WHERE VECTOR_NEAR(a, \"docs\", 10) RETURN b LIMIT 20",
    "vector": [0.1, 0.2, 0.3]
  }'
```

The `vector` field is required when `VECTOR_NEAR` appears in the query. Pass `"ef": N` to override the exploration factor for this request (0 = use server default).

---

## Workspace layout

```
polargraph/
├── Cargo.toml                  # workspace root
├── polargraph.example.toml     # fully-commented config reference
├── docs/
│   ├── architecture.md         # design narrative and internals
│   └── api-reference.md        # public API surface
└── crates/
    ├── polargraph-core/        # shared types — no I/O, no async
    ├── polargraph-storage/     # RocksDB triple store, MVCC, HNSW, migrations
    ├── polargraph-query/       # Datalog evaluator, view projection, EXPLAIN
    ├── polargraph-server/      # polargraphd — gRPC binary + management UI
    ├── polargraph-rest/        # REST gateway — HTTP/JSON → gRPC proxy
    ├── polargraph-import/      # bulk N-Triples import via SST ingestion (offline)
    └── polargraph-bench/       # end-to-end benchmark scenarios
```

Dependency order (no cycles): `core` ← `storage` ← `query` ← `server`

`polargraph-import` depends only on `core` + `storage`.
`polargraph-rest` depends only on the generated proto client (no storage deps).

---

## Further reading

- [`docs/architecture.md`](docs/architecture.md) — hexastore layout, MVCC, HNSW algorithm, Datalog evaluator, bitemporal model, replication, TLS, rate limiting, schema migrations
- [`CLAUDE.md`](CLAUDE.md) — codebase guide for contributors and AI assistants; conventions, crate responsibilities, current implementation status
