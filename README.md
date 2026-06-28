# PolarGraph DB Engine

PolarGraph is a purpose-built, Rust graph database engine. The core abstraction is the **triple** — an atomic (subject, predicate, object) statement — stored with full bitemporal versioning, indexed six ways via a hexastore for O(log n) lookups on any bind pattern, and protected by optimistic MVCC. On top of this sit a Datalog query evaluator, a Cypher surface layer (including the `VECTOR_NEAR` predicate for unified ANN + graph queries), a pure-Rust HNSW vector index with named spaces and configurable exploration factor, and a gRPC server with a REST gateway, management UI, TLS, API key authentication, rate limiting, WAL streaming replication, incremental backups, schema migrations, and Prometheus observability — all in a single statically-linked binary.

---

## Feature highlights

- **Hexastore index** — six RocksDB column families give O(log n) lookups on any (S, P, O) bind pattern without secondary scans
- **Bitemporal versioning** — valid time (world truth) and transaction time (record time) independently queryable on every triple; time-travel queries on both axes
- **Optimistic MVCC** — snapshot-isolated reads; conflict-detected writes; timestamps are real wall-clock µs for meaningful time-travel
- **Datalog queries** — conjunctive pattern joins, recursive rules, transitive reachability, hybrid vector-seed queries, named parameters
- **Cypher surface** — readable graph queries compiled to Datalog IR; `MATCH`, `WHERE`, `RETURN`, `LIMIT`, `ORDER BY`, `SKIP`, `WITH`, aggregations (`COUNT`, `SUM`, `AVG`, etc.), `CREATE`/`MERGE`/`SET`/`DELETE`, transitive closure `[:pred*]`, bounded paths `[:pred*1..n]`, and `VECTOR_NEAR`; plan cache
- **SPARQL 1.1 endpoint** — `polargraph-sparql` library; `GET /sparql`, `POST /sparql`, `POST /sparql/update`; SELECT, ASK, CONSTRUCT, DESCRIBE; UNION, OPTIONAL, FILTER, property paths, GROUP BY, HAVING; full SPARQL-star (subject and object position, variable predicates, Turtle-star/N-Triples-star serialization, Update with embedded triples); INSERT/DELETE WHERE
- **HNSW vector index** — pure-Rust; named spaces with independent dimensionality; Memory and Mmap storage modes; batch insert; configurable exploration factor (`ef`) for recall/latency tuning
- **OWL 2 RL materialization** — forward-chaining engine; 12 rules (rdfs2, rdfs3, rdfs5, rdfs7/prp-spo1, rdfs9, rdfs11, prp-symp, prp-trp, prp-inv1/2, eq-sym/trans); derived facts in dedicated `DRV` CF; `RunMaterialization` RPC + `POST /materialize`
- **RDF interoperability** — multi-format import (`POST /import/rdf`): N-Triples, Turtle, JSON-LD with `Content-Type` detection; JSON-LD export (`GET`/`POST /export/jsonld`); Accept-negotiated subgraph export (`GET /export/subgraph`): N-Triples, Turtle, or JSON-LD; PolarGraph-to-PolarGraph transfer via `POST /import/subgraph`; OWL/RDFS schema round-trip (`GET /schema/rdf`, `POST /schema/rdf`); `polargraph-import --format ntriples|turtle|jsonld`
- **RDF-star edge annotations** — `EdgeProperty` and `EdgeRelation` triple variants; `EPA`/`EPO` column families; `GetEdgeAnnotations` and `GetEdgeIdsByTriple` RPCs; full SPARQL-star integration (subject and object position)
- **gRPC API** — full-featured `polargraph.v1.PolarGraphService` via tonic; server-streaming variants for large result sets
- **REST gateway** — standalone `polargraph-rest` binary; HTTP/JSON → gRPC proxy; no client stub required
- **Schema registry** — optional advisory node and edge type schemas with field validation; stored as triples with bitemporal versioning
- **Graph-native access control** — permissions as triples (`User`, `Group`, `HAS_ACCESS`, `HAS_ACCESS_TYPE`); access cache; identity via request field or HTTP header
- **Full-text trigram search** — `TRI` column family; `CONTAINS`, `STARTS WITH`, `=~` in Cypher WHERE automatically routed to trigram index
- **Property version history** — `GetPropertyHistory` RPC and `GET /property-history` expose full MVCC history of any scalar property
- **Bulk import** — `polargraph-import` binary ingests N-Triples via RocksDB SST file ingestion (10–100× faster than streaming inserts)
- **WAL streaming replication** — `--replica-of` enables read replicas with automatic reconnect and exponential backoff
- **TLS** — server-side TLS on gRPC, management UI, and Prometheus; replica CA cert for mutual chain verification
- **API key authentication** — tower middleware; constant-time key comparison; multi-key rotation without downtime; runtime key management RPCs
- **Rate limiting** — per-client-IP token bucket; configurable RPS; stale-entry cleanup
- **Prometheus metrics** — per-RPC counters and histograms; vector space, WAL, backup, compaction, retention, materialization, and query cache gauges
- **Management UI** — embedded SPA with Query, Schema, Insert, Search, and Status tabs; query history; Mermaid schema diagram; auth overlay
- **Backup & restore** — incremental RocksDB `BackupEngine` with hard-linked SST files
- **Schema migrations** — versioned, auto-applied at startup; `MigrateSchema` / `MigrationStatus` RPCs
- **Scheduled retention** — background periodic retention task; configurable interval; separate Prometheus counters
- **Wire transactions** — multi-RPC atomic transactions via `BeginTransaction` / `CommitTransaction`; idle-TTL cleanup
- **Query timeouts** — deadline propagated through all evaluator layers; `DEADLINE_EXCEEDED` on breach
- **Slow query logging** — structured `WARN` + Prometheus counter when any query exceeds the threshold
- **Graceful shutdown** — `CancellationToken`-coordinated drain on SIGTERM/SIGINT; force-exit watchdog
- **Docker** — multi-stage `Dockerfile` + `docker-compose.yml` with named volume

---

## Quick start

### Docker Compose

```bash
git clone <this repo>
cd polargraph
docker compose up
```

The server starts on:
- `localhost:50051` — gRPC
- `localhost:8080` — management UI
- `localhost:9090` — Prometheus metrics

Start the REST gateway alongside it:

```bash
cargo build --release -p polargraph-rest
./target/release/polargraph-rest \
  --upstream http://localhost:50051 \
  --listen 0.0.0.0:8000
```

Verify:

```bash
curl http://localhost:8000/health
# {"status":"ok","mode":"primary","triples":0}
```

### Build from source

Requires Rust 1.78+.

```bash
cargo build --release

# Start the server
./target/release/polargraphd \
  --data-dir ./data \
  --listen   0.0.0.0:50051

# In another terminal, start the REST gateway
./target/release/polargraph-rest \
  --upstream http://localhost:50051 \
  --listen   0.0.0.0:8000
```

---

## Configuration

Configuration priority: **CLI flag > environment variable > config file > built-in default**

Copy `polargraph.example.toml` to `./polargraph.toml` (or `~/.config/polargraph/config.toml`) and uncomment the options you want to change. The server auto-detects these locations; supply `--config PATH` (or `POLARGRAPH_CONFIG`) for an explicit path.

An explicit path that does not exist or cannot be parsed is a fatal startup error.

### [server]

Controls ports, data directory, and process lifecycle.

```toml
[server]
data_dir            = "/var/lib/polargraph"   # --data-dir / POLARGRAPH_DATA_DIR (default: /data)
grpc_port           = 50051                   # port portion of --listen / POLARGRAPH_LISTEN_ADDR
ui_port             = 8080                    # --ui-port / POLARGRAPH_UI_PORT
metrics_port        = 9090                    # --metrics-port / POLARGRAPH_METRICS_PORT
shutdown_timeout_ms = 10000                   # --shutdown-timeout-ms / POLARGRAPH_SHUTDOWN_TIMEOUT_MS
```

| Flag | Env variable | Default | Description |
|------|-------------|---------|-------------|
| `--data-dir PATH` | `POLARGRAPH_DATA_DIR` | `/data` | RocksDB data directory (created if absent) |
| `--listen ADDR` | `POLARGRAPH_LISTEN_ADDR` | `0.0.0.0:50051` | Full gRPC bind address (interface + port) |
| `--ui-port PORT` | `POLARGRAPH_UI_PORT` | `8080` | Management UI HTTP port |
| `--metrics-port PORT` | `POLARGRAPH_METRICS_PORT` | `9090` | Prometheus `/metrics` HTTP port |
| `--shutdown-timeout-ms MS` | `POLARGRAPH_SHUTDOWN_TIMEOUT_MS` | `10000` | Force-exit watchdog timeout after signal |

### [storage]

Backup directory and bitemporal retention policy.

```toml
[storage]
backup_dir                 = "/var/lib/polargraph/backups"  # --backup-dir / POLARGRAPH_BACKUP_DIR
retention_tx_age_secs      = 2592000   # 30 days  --retention-tx-age-secs / POLARGRAPH_RETENTION_TX_AGE_SECS
retention_vt_lookback_secs = 604800    #  7 days  --retention-vt-lookback-secs / POLARGRAPH_RETENTION_VT_LOOKBACK_SECS

[storage.retention_schedule]
enabled      = false   # --retention-schedule / POLARGRAPH_RETENTION_SCHEDULE
interval_secs = 3600   # --retention-interval-secs / POLARGRAPH_RETENTION_INTERVAL_SECS
```

| Flag | Env variable | Default | Description |
|------|-------------|---------|-------------|
| `--backup-dir PATH` | `POLARGRAPH_BACKUP_DIR` | *(none)* | Enables incremental backups; directory created if absent |
| `--retention-tx-age-secs N` | `POLARGRAPH_RETENTION_TX_AGE_SECS` | *(none)* | Delete triples older than N seconds (transaction time); runs once at startup |
| `--retention-vt-lookback-secs N` | `POLARGRAPH_RETENTION_VT_LOOKBACK_SECS` | *(none)* | Also delete triples whose valid-time end is more than N seconds in the past |
| `--retention-schedule` | `POLARGRAPH_RETENTION_SCHEDULE` | `false` | Enable background periodic retention task |
| `--retention-interval-secs N` | `POLARGRAPH_RETENTION_INTERVAL_SECS` | `3600` | How often the background retention task fires |

### [replication]

Enable replica mode by pointing at a primary.

```toml
[replication]
replica_of = "http://primary.example.com:50051"  # --replica-of / POLARGRAPH_REPLICA_OF
tls_ca     = "/etc/polargraph/ca.crt"            # --replica-tls-ca / POLARGRAPH_REPLICA_TLS_CA
```

| Flag | Env variable | Default | Description |
|------|-------------|---------|-------------|
| `--replica-of URL` | `POLARGRAPH_REPLICA_OF` | *(none)* | gRPC address of the primary; enables replica mode |
| `--replica-tls-ca PATH` | `POLARGRAPH_REPLICA_TLS_CA` | *(none)* | CA cert for verifying the primary's TLS certificate |

### [tls]

Enable TLS on gRPC, management UI, and Prometheus.

```toml
[tls]
cert = "/etc/polargraph/server.crt"   # --tls-cert / POLARGRAPH_TLS_CERT
key  = "/etc/polargraph/server.key"   # --tls-key  / POLARGRAPH_TLS_KEY
```

Both `cert` and `key` must be supplied together; omitting either keeps the server in plaintext mode.

### [auth]

API key authentication for all gRPC and HTTP management requests.

```toml
[auth]
api_keys = ["key-one", "key-two"]   # --api-key (repeatable) / POLARGRAPH_API_KEY (comma-sep)
no_auth  = false                    # --no-auth
```

| Flag | Env variable | Default | Description |
|------|-------------|---------|-------------|
| `--api-key KEY` | `POLARGRAPH_API_KEY` | *(none)* | Accepted bearer key; flag is repeatable; env is comma-separated |
| `--no-auth` | — | false | Suppress the "no API key configured" startup warning |

### [observability]

Logging and metrics.

```toml
[observability]
log_level  = "info"    # --log-level / RUST_LOG
log_format = "pretty"  # --log-format / LOG_FORMAT  ("pretty" or "json")
no_metrics = false     # --no-metrics
no_ui      = false     # --no-ui
```

| Flag | Env variable | Default | Description |
|------|-------------|---------|-------------|
| `--log-level FILTER` | `RUST_LOG` | `info` | `tracing` filter directive (e.g. `debug`, `polargraph_storage=trace`) |
| `--log-format FORMAT` | `LOG_FORMAT` | `pretty` | `pretty` (human-readable) or `json` (newline-delimited JSON) |
| `--no-metrics` | — | false | Disable the Prometheus `/metrics` endpoint |
| `--no-ui` | — | false | Disable the management UI |

### [query]

Query timeout, slow-query logging, and default HNSW ef.

```toml
[query]
timeout_ms        = 30000   # --query-timeout-ms / POLARGRAPH_QUERY_TIMEOUT_MS
slow_query_ms     = 1000    # --slow-query-ms / POLARGRAPH_SLOW_QUERY_MS
default_vector_ef = 50      # --default-vector-ef / POLARGRAPH_DEFAULT_VECTOR_EF
```

| Flag | Env variable | Default | Description |
|------|-------------|---------|-------------|
| `--query-timeout-ms MS` | `POLARGRAPH_QUERY_TIMEOUT_MS` | `30000` | Max query execution time; 0 = unlimited |
| `--slow-query-ms MS` | `POLARGRAPH_SLOW_QUERY_MS` | `1000` | Emit WARN + increment counter when exceeded; 0 = disabled |
| `--default-vector-ef N` | `POLARGRAPH_DEFAULT_VECTOR_EF` | `50` | Default HNSW exploration factor for all vector searches |

### [rate_limit]

Per-client-IP token bucket.

```toml
[rate_limit]
max_rps = 100   # --rate-limit-rps / POLARGRAPH_RATE_LIMIT_RPS
```

| Flag | Env variable | Default | Description |
|------|-------------|---------|-------------|
| `--rate-limit-rps N` | `POLARGRAPH_RATE_LIMIT_RPS` | `0` | Max requests/sec per client IP; 0 = disabled |

---

## Inserting data

All examples assume `polargraph-rest` is running on `localhost:8000` with key `my-key`.

### Insert a node property

```bash
curl -s -X POST http://localhost:8000/insert \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "subject":   "019012ab-cdef-7000-8000-000000000001",
    "predicate": "name",
    "value":     {"text_val": "Alice"}
  }'
```

### Insert a relation triple

```bash
curl -s -X POST http://localhost:8000/insert \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "subject":   "019012ab-cdef-7000-8000-000000000001",
    "predicate": "knows",
    "object":    "019012ab-cdef-7000-8000-000000000002"
  }'
```

### Insert a relation with edge properties

The `properties` array stores scalar attributes on the edge itself. The response includes `edge_id` — the UUID under which those properties are stored.

```bash
curl -s -X POST http://localhost:8000/insert \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "subject":   "019012ab-cdef-7000-8000-000000000001",
    "predicate": "works_at",
    "object":    "019012ab-cdef-7000-8000-000000000003",
    "properties": [
      {"name": "since", "value": {"int_val": 2019}},
      {"name": "role",  "value": {"text_val": "engineer"}}
    ]
  }'
# Response: {"commit_ts": 1718000000000001, "edge_id": "019012ab-..."}
```

Value encoding: `null_val` (bool), `bool_val`, `int_val` (sint64), `float_val` (double), `text_val`, `blob_val` (array of 0–255 ints), or `vec_val` (`{"values": [...]}`).

### Insert a vector embedding

```bash
curl -s -X POST http://localhost:8000/vector/insert \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "node_id": "019012ab-cdef-7000-8000-000000000001",
    "space":   "people",
    "vector":  [0.12, 0.45, 0.78, 0.33]
  }'
```

---

## Querying

### Pattern queries via REST

POST patterns as 3-token strings: `?varname` for variables, `_` for wildcards, UUID or plain text for bound terms, optional `:` prefix on predicates.

```bash
# Who does Alice know, and what are their names?
curl -s -X POST http://localhost:8000/query \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "patterns": [
      "019012ab-cdef-7000-8000-000000000001 :knows ?o",
      "?o :name ?n"
    ],
    "limit": 20
  }'
# Response: {"bindings": [{"o": "<uuid>", "n": "<uuid>"}, ...]}
```

### Datalog rules (recursive reachability)

Supply a `rules` array to derive new facts before querying. The server runs the rules to a fixed point, then evaluates `patterns` against the combined base + derived fact set.

```bash
# Find all nodes transitively reachable via "follows"
curl -s -X POST http://localhost:8000/query \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "rules": [
      {
        "head_predicate":   "follows_reach",
        "head_subject_var": "x",
        "head_object_var":  "z",
        "body": [
          "?x :follows ?y",
          "?y :follows_reach ?z"
        ]
      }
    ],
    "patterns": ["019012ab-cdef-7000-8000-000000000001 :follows_reach ?dst"]
  }'
```

### Query plan (no DB access)

`POST /explain` returns the static execution plan — which index each pattern uses — without touching storage.

```bash
curl -s -X POST http://localhost:8000/explain \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{"patterns": ["?s :knows ?o", "?o :name ?n"]}'
```

Example output:
```
Query Plan
──────────
Step 1: PatternScan  [?s, :knows, ?o]
  Index: PSO  (predicate bound)
  Binds: ?s, ?o

Step 2: PatternScan  [?o, :name, ?n]
  Index: SPO  (subject bound)
  Binds: ?n

Recursive rules: none
Estimated steps: 2
```

### Cypher queries

`POST /cypher` accepts a Cypher string and compiles it to the Datalog IR.

**Simple label match:**

```bash
curl -s -X POST http://localhost:8000/cypher \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{"cypher": "MATCH (a:Person) RETURN a LIMIT 10"}'
```

**Property filter:**

```bash
curl -s -X POST http://localhost:8000/cypher \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "cypher": "MATCH (a:Person)-[:knows]->(b:Person) WHERE a.name = \"Alice\" RETURN b"
  }'
```

**Transitive closure (arbitrary depth):**

```bash
curl -s -X POST http://localhost:8000/cypher \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "cypher": "MATCH (a:Person)-[:knows*]->(b:Person) RETURN b"
  }'
```

**Unified ANN + graph query with `VECTOR_NEAR`:**

`VECTOR_NEAR(var, "space", k)` seeds the query from the `k` nearest neighbors of the supplied `vector`, then joins the rest of the `MATCH` body against those seeds.

```bash
curl -s -X POST http://localhost:8000/cypher \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "cypher": "MATCH (a:Document)-[:cites]->(b:Document) WHERE VECTOR_NEAR(a, \"doc_embeddings\", 10) RETURN b LIMIT 20",
    "vector": [0.1, 0.2, 0.3, 0.4]
  }'
```

Override the HNSW exploration factor inline for higher recall on a specific query:

```bash
curl -s -X POST http://localhost:8000/cypher \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "cypher": "MATCH (a:Document)-[:cites]->(b) WHERE VECTOR_NEAR(a, \"doc_embeddings\", 10, ef=100) RETURN b LIMIT 20",
    "vector": [0.1, 0.2, 0.3, 0.4]
  }'
```

**Aggregations, ORDER BY, and WITH:**

```bash
curl -s -X POST http://localhost:8000/cypher \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "cypher": "MATCH (a:Person)-[:knows]->(b:Person) RETURN a.name, COUNT(*) AS cnt ORDER BY cnt DESC LIMIT 5"
  }'
```

Supported aggregation functions: `COUNT(*)`, `COUNT(var)`, `COLLECT(var)`. `ORDER BY` accepts multiple keys with `ASC`/`DESC`. `SKIP N` offsets into a result set. `WITH` pipelines intermediate projections.

**Full-text search in WHERE:**

Cypher `WHERE` clauses that use `CONTAINS`, `STARTS WITH`, or `=~` (regex) against string properties are automatically routed to the trigram index (`TRI` CF) for sub-millisecond lookups:

```bash
curl -s -X POST http://localhost:8000/cypher \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "cypher": "MATCH (a:Person) WHERE a.name CONTAINS \"Ali\" RETURN a"
  }'
```

`STARTS WITH` and `=~` are handled identically via trigram extraction. Falls back to a full scan if the predicate has no trigram index entries yet.

Supported Cypher: `MATCH`, `WHERE` (equality, comparison, text predicates), `RETURN`, `LIMIT`, `ORDER BY`, `SKIP`, `WITH`, `COUNT`, `COLLECT`, directed relationships `(a)-[:pred]->(b)`, transitive closure `[:pred*]`. Unsupported features return `INVALID_ARGUMENT`.

### Cypher write operations

`POST /cypher/write` (or the `CypherWrite` gRPC RPC) executes CREATE, MERGE, SET, and DELETE statements.

```bash
# Create a node
curl -s -X POST http://localhost:8000/cypher/write \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{"cypher": "CREATE (c:Company {name: \"Acme\", founded: 2010})"}'
# Response: {"created_node_ids": ["019012ab-..."], "triples_written": 3}

# MERGE (create if not exists)
curl -s -X POST http://localhost:8000/cypher/write \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "cypher": "MATCH (a:Person {name: \"Alice\"}) MERGE (a)-[:works_at]->(c:Company {name: \"Acme\"})"
  }'

# SET property
curl -s -X POST http://localhost:8000/cypher/write \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{"cypher": "MATCH (a:Person {name: \"Alice\"}) SET a.age = 31"}'

# DELETE
curl -s -X POST http://localhost:8000/cypher/write \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{"cypher": "MATCH (a:Person {name: \"Temp\"}) DELETE a"}'
```

Cypher writes can be included in a wire transaction by supplying `tx_id` (see below).

### Wire transactions

Batch multiple insert and write operations into a single atomic transaction. The server holds the transaction open until `CommitTransaction` or `RollbackTransaction` is called.

```bash
# 1. Open a transaction — returns a tx_id token
TX=$(curl -s -X POST http://localhost:8000/tx/begin \
  -H 'Authorization: Bearer my-key' | jq -r .tx_id)

# 2. Write inside the transaction
curl -s -X POST http://localhost:8000/cypher/write \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d "{\"cypher\": \"CREATE (n:Event {name: \\\"Deploy\\\"})\", \"tx_id\": \"$TX\"}"

curl -s -X POST http://localhost:8000/insert \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d "{\"subject\": \"$NODE1\", \"predicate\": \"triggered\", \"object\": \"$NODE2\", \"tx_id\": \"$TX\"}"

# 3. Commit
curl -s -X POST http://localhost:8000/tx/commit \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d "{\"tx_id\": \"$TX\"}"
# Response: {"commit_ts": 1718000000000001, "triples_written": 4}

# Or rollback
curl -s -X POST http://localhost:8000/tx/rollback \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d "{\"tx_id\": \"$TX\"}"
```

Transactions are automatically evicted after 5 minutes of inactivity. `Query` calls that supply `tx_id` read at the transaction's `read_ts`, providing a consistent snapshot across multiple reads.

### Streaming queries

For large result sets, stream bindings one chunk at a time instead of waiting for the full response:

```bash
# NDJSON stream — one JSON object per line
curl -s -X POST http://localhost:8000/query/stream \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{"patterns": ["?s :name ?n"]}' \
  | while IFS= read -r line; do echo "$line"; done

# Cypher streaming
curl -s -X POST http://localhost:8000/cypher/stream \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{"cypher": "MATCH (a:Person) RETURN a.name"}'
```

The server delivers results in chunks of 500 bindings. The connection stays open until the result set is exhausted or the client disconnects. At the gRPC level, use the `QueryStream` and `CypherQueryStream` server-streaming RPCs directly.

### Diagnostics

Inspect index statistics and server internals without restarting:

```bash
# Per-column-family key counts, estimated sizes, and HNSW space metadata
curl -s http://localhost:8000/indexes \
  -H 'Authorization: Bearer my-key'

# RocksDB internal properties, oracle timestamp, open transaction count
curl -s http://localhost:8000/stats \
  -H 'Authorization: Bearer my-key'
```

Or via gRPC:

```bash
grpcurl -plaintext -d '{}' localhost:50051 polargraph.v1.PolarGraphService/ShowIndexes
grpcurl -plaintext -d '{}' localhost:50051 polargraph.v1.PolarGraphService/ShowStats
```

### Time-travel queries

Both filters can be combined. All times are Unix microseconds.

```bash
# What was in the DB at a specific wall-clock instant?
curl -s -X POST http://localhost:8000/query \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "patterns": ["?s :name ?n"],
    "as_of_tx_time": 1718000000000000
  }'

# What facts were valid at a specific real-world time?
curl -s -X POST http://localhost:8000/query \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "patterns": ["?s :role ?r"],
    "as_of_valid_time": 1700000000000000
  }'

# Full bitemporal point query: both axes simultaneously
curl -s -X POST http://localhost:8000/query \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "patterns": ["?s :role ?r"],
    "as_of_tx_time":    1718000000000000,
    "as_of_valid_time": 1700000000000000
  }'
```

---

## Vector search

### Named spaces

Each HNSW space is independent — different node types or collections can have separate indexes with different dimensionalities. Supply a `space` name on every insert/search call; an empty string defaults to `"default"`.

### Basic k-NN search

```bash
curl -s -X POST http://localhost:8000/vector/search \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer my-key' \
  -d '{
    "space":  "doc_embeddings",
    "query":  [0.1, 0.2, 0.3, 0.4],
    "k":      10
  }'
# Response: {"results": [{"node_id": "...", "similarity": 0.97}, ...]}
```

Results are ordered by descending cosine similarity (range: −1 to 1; higher = more similar).

### Filtered search

**By node type** — restricts candidates to nodes whose `__type` property matches:

```bash
grpcurl -plaintext \
  -d '{"space":"people","query":[0.1,0.2,0.3],"k":5,"node_type_filter":{"type_name":"Person"}}' \
  localhost:50051 polargraph.v1.PolarGraphService/SearchVectorFiltered
```

**By reachability** — restricts candidates to nodes reachable from a start node:

```bash
grpcurl -plaintext \
  -d '{
    "space": "docs",
    "query": [0.1, 0.2, 0.3],
    "k": 5,
    "reachability_filter": {
      "from_node": {"bytes": "..."},
      "predicate": "cites",
      "max_hops": 3
    }
  }' \
  localhost:50051 polargraph.v1.PolarGraphService/SearchVectorFiltered
```

### ef tuning

`ef` (exploration factor) controls the candidate list size during ANN graph traversal: larger `ef` explores more of the graph before picking the final top-k, trading latency for recall.

| ef | Character |
|----|-----------|
| 20 | Fastest; minor recall loss vs. brute force |
| 50 | Safe default; good recall on most workloads |
| 100+ | High-recall mode; noticeably slower on large indexes |

**Rule of thumb:** `ef ≥ k`. Below `k` the search may not fill the result set.

**Benchmark:** at 100K nodes / 128 dims / k=10, ef=50 runs ~583 µs p50; ef=20 roughly halves that at a few percent recall cost.

**Three-level resolution hierarchy** (highest priority wins):

1. **Cypher inline** — `VECTOR_NEAR(a, "space", 10, ef=100)` overrides everything for that predicate
2. **Per-request field** — `ef` field on `SearchVectorRequest`, `VectorSeedQueryRequest`, `CypherQueryRequest` (0 = use server default)
3. **Server default** — `[query] default_vector_ef` in TOML / `--default-vector-ef` / `POLARGRAPH_DEFAULT_VECTOR_EF` (built-in: 50)

This lets you set a conservative global default while allowing latency-sensitive callers to drop `ef` and recall-critical callers to raise it — without server restarts.

### Storage modes

Each HNSW space is created in one of two modes, set at registration time:

| Mode | How vectors are stored | When to use |
|------|----------------------|-------------|
| `memory` (default) | `Vec<f32>` on the heap | Small-to-medium spaces; fastest search |
| `mmap` | Flat `.vecs` file under `<data_dir>/vectors/`, OS-paged | Large spaces that exceed available RAM |

Once a space is created in a given mode it retains that mode for the lifetime of the data directory.

---

## Schema registry

Schemas are optional and advisory — the store is open-world. Validation is an explicit call, not a write-time gate.

### Register a node type

```bash
grpcurl -plaintext \
  -d '{
    "definition": {
      "type_name": "Person",
      "fields": [
        {"field_name": "name",  "kind": "text",  "required": true},
        {"field_name": "age",   "kind": "int",   "required": false},
        {"field_name": "email", "kind": "text",  "required": false}
      ],
      "vector_space": {
        "space_name":      "people",
        "dimensions":      128,
        "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
        "storage_mode":    "memory"
      }
    }
  }' \
  localhost:50051 polargraph.v1.PolarGraphService/RegisterNodeType
```

### Register an edge type

```bash
grpcurl -plaintext \
  -d '{
    "definition": {
      "predicate": "works_at",
      "domain":    "Person",
      "range":     "Organisation",
      "fields": [
        {"field_name": "since", "kind": "int",  "required": true},
        {"field_name": "role",  "kind": "text", "required": false}
      ]
    }
  }' \
  localhost:50051 polargraph.v1.PolarGraphService/RegisterEdgeType
```

### Validate a node

```bash
grpcurl -plaintext \
  -d '{
    "type_name": "Person",
    "properties": {
      "name":  {"text_val": "Alice"},
      "age":   {"int_val":  30}
    }
  }' \
  localhost:50051 polargraph.v1.PolarGraphService/ValidateNode
# {"valid": true, "errors": []}
```

Validation checks: required fields are present; present fields have the declared kind; unknown fields are accepted (open-world). When a `VectorSpaceDef` is registered, `InsertVector` enforces `vector.len() == dimensions` at write time.

### List predicates between types

```bash
grpcurl -plaintext \
  -d '{"domain_type": "Person", "range_type": "Organisation"}' \
  localhost:50051 polargraph.v1.PolarGraphService/ListPredicatesBetween
# {"predicates": ["works_at", "founded"]}
```

---

## Bulk import

`polargraph-import` ingests RDF files (N-Triples, Turtle, or JSON-LD) directly into RocksDB via SST file ingestion — bypassing gRPC, the WAL write path, and per-insert MVCC overhead. Expected throughput: 10–100× faster than streaming inserts over gRPC.

**The server must be stopped first** — SST ingestion requires exclusive DB access.

```bash
# Stop polargraphd
kill $POLARGRAPHD_PID

# Import
polargraph-import \
  --data-dir  /var/lib/polargraph \
  --input     ./seed-data.nt \
  --batch-size 100000

# Example output:
# Imported 100000 triples (batch 1) in 312ms
# Imported 100000 triples (batch 2) in 298ms
# Total: 200000 triples in 612ms (326797 triples/sec)

# Restart
polargraphd --data-dir /var/lib/polargraph
```

### CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir PATH` | *(required)* | RocksDB data directory |
| `--input FILE` | *(required)* | Input file |
| `--format FORMAT` | `ntriples` | Input format: `ntriples` (default), `turtle`, `jsonld` |
| `--batch-size N` | `100000` | Triples per SST import batch |
| `--temp-dir PATH` | `<data-dir>/sst_tmp` | Temporary SST file directory |

### Supported input formats

Pass `--format turtle` or `--format jsonld` to switch parsers; default is `ntriples`.

### N-Triples specifics

| Input form | Storage result |
|---|---|
| `<uri> <uri> <uri> .` | `Triple::Relation` — URIs hashed to stable `NodeId`s via xxHash3-128 |
| `<uri> <uri> "literal" .` | `Triple::Property` — `Value::Text` |
| `<uri> <uri> "literal"@lang .` | `Triple::Property` — language tag stripped |
| Lines starting with `#` | Skipped (comments) |
| Blank lines | Skipped |
| `_:blank_node` objects | Skipped (not supported) |

The same URI always produces the same `NodeId` across runs.

---

## Backup & restore

Requires `--backup-dir PATH` (or `POLARGRAPH_BACKUP_DIR`) at server startup. Without it, backup RPCs return `FAILED_PRECONDITION`.

RocksDB's `BackupEngine` hard-links unchanged SST files between successive backups — only new or compacted files are copied.

### Create and manage backups

```bash
# Create an incremental backup
grpcurl -plaintext -d '{}' \
  localhost:50051 polargraph.v1.PolarGraphService/CreateBackup
# {"backup_id": 3, "size_bytes": 4096000, "created_at": 1718000000}

# List all backups
grpcurl -plaintext -d '{}' \
  localhost:50051 polargraph.v1.PolarGraphService/ListBackups

# Keep only the 5 most recent backups
grpcurl -plaintext -d '{"keep_n": 5}' \
  localhost:50051 polargraph.v1.PolarGraphService/PurgeOldBackups
```

Or via the REST gateway (not yet exposed as HTTP endpoints — use grpcurl or the gRPC client directly).

### Restore runbook (offline)

Restore cannot be performed while the server is running.

```bash
# 1. Stop the server
kill $POLARGRAPHD_PID

# 2. Find the backup ID to restore from
grpcurl -plaintext -d '{}' localhost:50051 polargraph.v1.PolarGraphService/ListBackups

# 3. Run a restore using BackupManager::restore_from_backup(backup_id, restore_dir)
#    (Rust library call or future polargraphd subcommand; see docs/architecture.md)

# 4. Restart the server pointing at the restored directory
polargraphd --data-dir /path/to/restore_dir \
            --backup-dir /path/to/backup_dir
```

---

## Replication

WAL streaming replication runs over the gRPC connection between primary and replica. No shared filesystem is required.

### Primary configuration

No special flag needed on the primary. Ensure `--backup-dir` is set if you want WAL retention logging, and that the replica can reach the primary's gRPC address.

### Starting a replica

```bash
polargraphd \
  --data-dir  /var/lib/polargraph-replica \
  --replica-of http://primary.example.com:50051
```

With TLS:

```bash
polargraphd \
  --data-dir       /var/lib/polargraph-replica \
  --replica-of     https://primary.example.com:50051 \
  --replica-tls-ca /etc/polargraph/ca.crt
```

All write RPCs (`Insert`, `InsertVector`, `BatchInsertVectors`, etc.) return `FAILED_PRECONDITION` on a replica. The WAL client reconnects automatically with exponential backoff (1 s → 30 s) if the primary is unavailable.

### Checking replication status

```bash
grpcurl -plaintext -d '{}' \
  localhost:50051 polargraph.v1.PolarGraphService/ReplicaStatus
# {
#   "is_replica": true,
#   "primary_address": "http://primary:50051",
#   "last_catchup_at": 1718000000000000,
#   "last_applied_seq": 12345,
#   "replication_lag_entries": 0
# }
```

The HTTP `/health` endpoint returns `503` with `{"status":"degraded"}` when the replica's WAL stream is disconnected — useful for load-balancer health probes.

---

## Schema migrations

Migrations are versioned Rust functions applied automatically at startup before the gRPC server accepts connections. Replicas skip auto-migration (read-only).

### Check migration status

```bash
grpcurl -plaintext -d '{}' \
  localhost:50051 polargraph.v1.PolarGraphService/MigrationStatus
# {"current_version": 2, "latest_version": 2, "applied": [...]}
```

### Run migrations on demand

```bash
# Dry run — see what would be applied without changing anything
grpcurl -plaintext -d '{"dry_run": true}' \
  localhost:50051 polargraph.v1.PolarGraphService/MigrateSchema

# Apply pending migrations
grpcurl -plaintext -d '{"dry_run": false}' \
  localhost:50051 polargraph.v1.PolarGraphService/MigrateSchema
```

Returns `FAILED_PRECONDITION` on a read replica.

### Built-in migrations

| Version | Description |
|---------|-------------|
| 1 | Baseline schema marker |
| 2 | Normalize node type records (backfills `storage_mode` on `VectorSpaceDef`) |

---

## Observability

### Log formats

```bash
# Human-readable (development)
polargraphd --log-format pretty --log-level debug

# Newline-delimited JSON (production / log aggregators)
polargraphd --log-format json --log-level info
```

`TelemetryLayer` wraps every gRPC handler and logs on completion:

```
INFO  rpc{method="Insert" peer="127.0.0.1:51234"}: completed status=ok duration_ms=2
WARN  rpc{method="Query"  peer="127.0.0.1:51234"}: completed status=deadline_exceeded duration_ms=30001
```

### Slow query logging

When a `Query`, `VectorSeedQuery`, or `Reachable` RPC exceeds `--slow-query-ms` (default 1000 ms):

```
WARN  slow query detected method=Query duration_ms=1452 threshold_ms=1000 extra="patterns=3"
```

The Prometheus counter `polargraph_slow_queries_total{method}` increments on each slow query.

### Prometheus metrics

Scraped from `http://localhost:9090/metrics`.

```yaml
# Sample scrape config
scrape_configs:
  - job_name: polargraph
    static_configs:
      - targets: ["polargraphd-host:9090"]
    scrape_interval: 15s
```

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `polargraph_rpc_requests_total` | counter | `method`, `status` | Total RPC calls by method and gRPC status |
| `polargraph_rpc_duration_seconds` | histogram | `method` | RPC latency distribution |
| `polargraph_triples_total` | gauge | — | Running count of inserted triples |
| `polargraph_vector_spaces_total` | gauge | — | Number of named HNSW spaces |
| `polargraph_wal_applied_seq` | gauge | — | Last WAL sequence number applied (replica only) |
| `polargraph_wal_lag_entries` | gauge | — | Entries behind primary (replica only) |
| `polargraph_backup_last_size_bytes` | gauge | — | Size of most recently created backup |
| `polargraph_compaction_deleted_total` | counter | — | Total triples deleted by retention runs |
| `polargraph_slow_queries_total` | counter | `method` | Queries exceeding the slow-query threshold |

Example Grafana alert for replica lag:

```yaml
- alert: PolarGraphReplicaLag
  expr: polargraph_wal_lag_entries > 10000
  for: 2m
  labels:
    severity: warning
  annotations:
    summary: "PolarGraph replica is lagging behind primary"
```

---

## Rate limiting

Per-client-IP token bucket applied before authentication in the gRPC middleware pipeline.

```toml
[rate_limit]
max_rps = 100
```

- Each unique client IP gets an independent bucket with capacity = `max_rps` tokens
- Client IP resolved from `x-forwarded-for` (proxy) or TCP peer (direct connection)
- `RESOURCE_EXHAUSTED` ("rate limit exceeded") returned immediately when quota exhausted
- `ReplicaStatus` is exempt so load-balancer health probes do not consume quota
- Stale entries (idle > 60 s) purged every 1 000 requests to bound memory usage
- `max_rps = 0` disables rate limiting entirely (zero-cost pass-through)

---

## TLS

Enable TLS on all three network surfaces (gRPC, management UI HTTP, Prometheus HTTP) by supplying a cert/key pair:

```bash
polargraphd \
  --tls-cert /etc/polargraph/server.crt \
  --tls-key  /etc/polargraph/server.key
```

Or in TOML:

```toml
[tls]
cert = "/etc/polargraph/server.crt"
key  = "/etc/polargraph/server.key"
```

Both fields must be supplied together; omitting either keeps the server in plaintext mode.

gRPC clients must use `https://` URIs. The REST gateway connects to a TLS primary via `--tls-ca`:

```bash
polargraph-rest \
  --upstream http://localhost:50051 \
  --tls-ca   /etc/polargraph/ca.crt
```

For replicas connecting to a TLS primary:

```bash
polargraphd \
  --replica-of     https://primary:50051 \
  --replica-tls-ca /etc/polargraph/ca.crt
```

---

## Management UI

The management UI is a single-page app embedded in the `polargraphd` binary (no external assets, no build step). Access it at `http://localhost:8080`.

### Tabs

| Tab | Description |
|-----|-------------|
| **Query** | Run Datalog or Cypher queries; displays variable bindings in a table |
| **Schema** | Browse registered node and edge type definitions |
| **Insert** | Insert triples interactively |
| **Search** | Run vector searches or triple scans |
| **Status** | Server version, uptime, mode (primary / replica), auth status |

When API keys are configured, the UI prompts for a key on first load and stores it in `localStorage`. `GET /` always loads the SPA regardless of auth state.

### Disabling the UI

```bash
polargraphd --no-ui
# or
[observability]
no_ui = true
```

---

## gRPC API

Service: `polargraph.v1.PolarGraphService` — full proto at `crates/polargraph-server/proto/polargraph.proto`.

| RPC | Description |
|-----|-------------|
| `Insert` | Atomically commit ≥1 triples; returns `ABORTED` on write-write conflict |
| `Query` | Conjunctive pattern query; returns all satisfying variable bindings |
| `ExplainQuery` | Static execution plan for a query; no DB access |
| `CypherQuery` | Parse and execute a Cypher query string |
| `Reachable` | Transitive closure from a start node along a named predicate |
| `InsertVector` | Upsert a node's embedding vector into a named HNSW space |
| `SearchVector` | k-NN search with optional `ef` override |
| `SearchVectorFiltered` | k-NN with node-type or reachability post-filter |
| `SearchVectorInSet` | Score an explicit node-ID set against a query vector; return top-k |
| `BatchInsertVectors` | Insert multiple vectors in a single write-lock acquisition |
| `VectorSeedQuery` | ANN search → seed bindings → Datalog graph join, in one call |
| `RegisterNodeType` | Register or overwrite a node type schema |
| `GetNodeType` | Look up a schema by type name |
| `ListNodeTypes` | Return all registered node type schemas |
| `ValidateNode` | Validate a property map against a schema |
| `RegisterEdgeType` | Register or overwrite an edge type schema |
| `GetEdgeType` | Look up an edge schema by predicate name |
| `ListEdgeTypes` | Return all registered edge type schemas |
| `ValidateEdge` | Validate endpoint types and edge property map |
| `ListPredicatesBetween` | Return predicate names whose domain/range match given node types |
| `CreateBackup` | Create an incremental backup of the live store |
| `ListBackups` | List available backups with ID, timestamp, size, file count |
| `PurgeOldBackups` | Delete all but the `keep_n` most recent backups |
| `RunRetention` | Scan and delete expired triples; triggers RocksDB compaction |
| `ReplicaStatus` | Replication status: is_replica, lag, last_applied_seq (auth-exempt) |
| `StreamWal` | Server-streaming WAL entries from the primary (replica use only) |
| `MigrateSchema` | Apply pending schema migrations; `dry_run` for preview |
| `MigrationStatus` | Current and latest version; full applied-migration history |
| `CypherQuery` | Parse and execute a Cypher read query (MATCH/WHERE/RETURN) |
| `CypherWrite` | Execute a Cypher write statement (CREATE/MERGE/SET/DELETE) |
| `QueryStream` | Server-streaming `Query` — delivers bindings in 500-row chunks |
| `CypherQueryStream` | Server-streaming `CypherQuery` |
| `BeginTransaction` | Open a wire transaction; returns `tx_id` token |
| `CommitTransaction` | Commit a wire transaction atomically |
| `RollbackTransaction` | Discard a wire transaction |
| `ShowIndexes` | CF key counts, estimated sizes, HNSW space metadata |
| `ShowStats` | RocksDB properties, oracle timestamp, open transaction count |
| `GetEdgeAnnotations` | RDF-star annotations on an edge (MVCC-filtered) |
| `GetEdgeIdsByTriple` | Resolve `(subject, predicate, object)` tuples to `EdgeId` UUIDs |
| `RunMaterialization` | OWL 2 RL forward-chaining materialization; writes derived facts to `DRV` CF |

### Authentication

All RPCs except `ReplicaStatus` require `Authorization: Bearer <key>` (or `Authorization: ApiKey <key>`) when the server is configured with `--api-key`. Requests without a valid key receive gRPC status `UNAUTHENTICATED` (16).

### gRPC health service

`polargraphd` registers the standard `grpc.health.v1.Health` service for load-balancer and Kubernetes liveness probes:

```bash
grpcurl -plaintext \
  -d '{"service": "polargraph.v1.PolarGraphService"}' \
  localhost:50051 grpc.health.v1.Health/Check
# {"status": "SERVING"}
```

---

## Client SDKs

Official client libraries are under `clients/`. Each has its own README with full method listings.

### Python

```bash
pip install polargraph-client
```

```python
from polargraph import PolarGraphClient

with PolarGraphClient("localhost", 50051, api_key="secret") as client:
    client.insert_node(alice_id, "Person", name="Alice", age=30)
    rows = client.cypher("MATCH (a:Person)-[:knows]->(b) RETURN a, b LIMIT 10")
    print(rows)

    # Wire transaction
    with client.transaction() as tx:
        client.insert_edge(alice_id, "knows", bob_id, tx_id=tx)
```

An async variant (`AsyncPolarGraphClient`) is available for use with `asyncio`. See [`clients/python/README.md`](clients/python/README.md).

### Go

```bash
go get github.com/polarops/polargraph-go
```

```go
client, _ := polargraph.New("localhost:50051",
    polargraph.WithAPIKey("secret"),
)
defer client.Close()

rows, _ := client.Cypher(ctx, "MATCH (a:Person) RETURN a LIMIT 10")
result, _ := client.CypherWrite(ctx, `CREATE (c:Company {name: "Acme"})`)
```

See [`clients/go/README.md`](clients/go/README.md) for the full method surface and transaction example.

### JavaScript / TypeScript

```bash
npm install @polargraph/client
```

```typescript
import { PolarGraphClient } from "@polargraph/client";

const client = new PolarGraphClient("localhost", 50051, { apiKey: "secret" });

// Streaming query — no memory buffering
for await (const row of client.streamQuery([{ s: "?a", p: "knows", o: "?b" }])) {
  console.log(row.a, row.b);
}

// Wire transaction
const txId = await client.beginTx();
await client.insertNode(id, "Person", { name: "Eve" });
await client.commitTx(txId);

client.close();
```

See [`clients/js/README.md`](clients/js/README.md) for TypeScript types and full API reference.

---

## Kubernetes / Helm

A production-ready Helm chart lives at `deploy/helm/polargraph/`.

```bash
helm install polargraph ./deploy/helm/polargraph \
  --set image.tag=latest \
  --set auth.apiKey=my-secret-key \
  --set storage.size=50Gi
```

The chart deploys:

- **Namespace** — isolated `polargraph` namespace
- **ConfigMap** — `polargraph.toml` rendered from Helm values
- **Secret** — API key
- **PersistentVolumeClaim** — configurable storage class and size
- **Deployment** — single `polargraphd` pod (increase replicas for read-only replicas)
- **Services** — ClusterIP for gRPC (50051), NodePort/LoadBalancer for the management UI (8080), and Prometheus (9090)
- **HorizontalPodAutoscaler** — scales on CPU; configure `minReplicas`/`maxReplicas` in `values.yaml`

---

## Development

```bash
# Build everything
cargo build

# Build release binaries
cargo build --release

# Run all tests
cargo test

# Run storage integration tests only
cargo test -p polargraph-storage

# Run Criterion micro-benchmarks (storage layer)
cargo bench -p polargraph-storage

# Build the end-to-end benchmark binary
cargo build -p polargraph-bench

# Build the bulk import binary
cargo build -p polargraph-import

# Build the REST gateway
cargo build -p polargraph-rest

# Check without linking
cargo check

# Lint (warnings become errors)
cargo clippy -- -D warnings

# Format
cargo fmt
```

Minimum supported Rust version: **1.78**.

### Workspace layout

```
polargraph/
├── Cargo.toml                  # workspace root
├── polargraph.example.toml     # fully-commented config reference
├── docs/
│   ├── architecture.md         # design narrative and internals
│   └── api-reference.md        # public API surface
├── clients/
│   ├── python/                 # Python SDK
│   ├── go/                     # Go SDK
│   └── js/                     # TypeScript/JavaScript SDK
├── deploy/
│   └── helm/polargraph/        # Helm chart
└── crates/
    ├── polargraph-core/        # shared types — no I/O, no async
    ├── polargraph-storage/     # RocksDB triple store, MVCC, HNSW, migrations
    ├── polargraph-query/       # Datalog evaluator, Cypher parser, aggregations, view projection, EXPLAIN
    ├── polargraph-server/      # polargraphd — gRPC binary + management UI
    ├── polargraph-rest/        # REST gateway — HTTP/JSON → gRPC proxy
    ├── polargraph-import/      # bulk N-Triples import via SST ingestion (offline)
    └── polargraph-bench/       # end-to-end benchmark scenarios
```

Dependency order (no cycles):
```
core ← storage ← query ← server
polargraph-import: core + storage only
polargraph-rest:   generated proto client only (no storage deps)
```

### Conventions

- **Error handling**: `thiserror`-derived enums (`StorageError`). Prefer `?` propagation. Avoid `unwrap()` outside tests.
- **Logging**: `tracing` crate. `info!` for lifecycle events, `debug!` for per-operation detail. No `println!` in library crates.
- **Tests**: unit tests in the same file (`#[cfg(test)] mod tests`); integration tests under `crates/<crate>/tests/`.
- **Serde**: all public types derive `Serialize`/`Deserialize` unless there is a specific reason not to.

---

## Further reading

- [`docs/architecture.md`](docs/architecture.md) — hexastore layout, MVCC, HNSW algorithm, Datalog evaluator, bitemporal model, Cypher compilation, replication, TLS, rate limiting, schema migrations
- [`polargraph.example.toml`](polargraph.example.toml) — fully-commented configuration reference covering every option
- [`CLAUDE.md`](CLAUDE.md) — codebase guide for contributors and AI assistants; conventions, crate responsibilities, implementation status
