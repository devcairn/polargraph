# PolarGraph Benchmarks

Two complementary benchmark suites are provided:

- **Criterion micro-benchmarks** (`polargraph-storage/benches/storage.rs`) — isolated,
  reproducible µs-level measurements of the storage layer internals.
- **`polargraph-bench` binary** — end-to-end scenarios over a live gRPC server,
  measuring real-world throughput and latency distributions.

---

## Part 1 — Criterion micro-benchmarks

### What's measured

| Group | Benchmarks |
|-------|-----------|
| `triple_writes` | Commit latency for batches of 1 / 10 / 100 / 1 000 triples |
| `pattern_query` | `scan_by_subject`, `scan_by_predicate`, `scan_by_predicate_object`, `scan_by_object` on a 500-node store |
| `hnsw_insert` | Single-vector insert for 32 / 128 / 512 dimensions |
| `hnsw_search` | ANN search for (n=500,d=128) / (n=2000,d=128) / (n=500,d=512) |
| `hnsw_recall` | Recall@10 vs brute-force on 1 000 vectors, d=128 |
| `filtered_search` | `search_vector_ef` vs `search_vector_in_set` on a 10 % subset of 500 nodes |

### Running

```bash
# All groups — takes several minutes; HTML report in target/criterion/
cargo bench -p polargraph-storage

# Single group
cargo bench -p polargraph-storage -- triple_writes

# Compile only (fast, no execution)
cargo build --benches -p polargraph-storage
```

### Output

Criterion prints mean/median ± confidence interval to stdout and writes an
HTML report under `target/criterion/<group>/<benchmark>/report/index.html`.

---

## Part 2 — `polargraph-bench` binary

### Prerequisites

A running `polargraphd` instance:

```bash
# Development server (creates /tmp/pg-bench automatically)
cargo run -p polargraph-server -- --data-dir /tmp/pg-bench

# Or with Docker
docker compose up
```

The binary connects to `http://localhost:50051` by default. Use `--addr` to
override.

### Scenarios

#### `write` — insert throughput

Inserts `--nodes` nodes (2 property triples each) followed by `--edges-per-node`
relation triples per node, sent in batches of 100 triples per RPC. Reports
triples/sec and per-batch latency percentiles.

```bash
cargo run -p polargraph-bench -- write
cargo run -p polargraph-bench -- write --nodes 50000 --edges-per-node 8
```

#### `read` — point-query latency

Pre-populates `--nodes` nodes, then issues one `Query` RPC per node (bound
subject, any predicate). Reports p50/p95/p99 latency.

```bash
cargo run -p polargraph-bench -- read --nodes 5000
```

#### `mixed` — concurrent reads + writes

Pre-populates `--nodes` nodes, then runs `--concurrency` workers in parallel.
Each worker performs 500 operations: 80% bound-subject queries, 20% single-node
inserts. Reports per-operation latency separated by read vs write.

```bash
cargo run -p polargraph-bench -- mixed --nodes 10000 --concurrency 8
```

#### `recovery` — store re-open time

Writes `--nodes` triples directly to RocksDB (no server needed), drops the
store, then times 5 consecutive re-opens. Reports mean/min/max open latency.

```bash
cargo run -p polargraph-bench -- recovery --nodes 100000
```

#### `filtered-search` — ANN latency + recall

Registers a `BenchNode` type with a named HNSW space (`BenchVec`), inserts
`--nodes` nodes with `--vector-dims`-dimensional embeddings via
`BatchInsertVectors`, then runs 50 `SearchVectorFiltered` queries with a
`NodeTypeFilter`. Reports p50/p95/p99 latency and mean Recall@10 vs brute-force.

```bash
cargo run -p polargraph-bench -- filtered-search --nodes 5000 --vector-dims 128
```

### Common flags

| Flag | Default | Description |
|------|---------|-------------|
| `--addr` | `http://localhost:50051` | gRPC server address |
| `--nodes` | `10000` | Nodes to insert / query |
| `--edges-per-node` | `4` | Relations per node (write, mixed) |
| `--vector-dims` | `128` | Vector dimensions (filtered-search) |
| `--concurrency` | `4` | Parallel workers (mixed) |

### Compile-only check

```bash
cargo build -p polargraph-bench
```
