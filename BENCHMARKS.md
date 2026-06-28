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

#### `bsbm` — BSBM 12-query e-commerce suite (no server needed)

Generates a synthetic e-commerce dataset (products, vendors, offers, reviews)
derived from the Berlin SPARQL Benchmark and runs all 12 standard query
templates in-process against a local `TripleStore`. No `polargraphd` required.

```bash
cargo run -p polargraph-bench --release -- bsbm --scale-factor 1
cargo run -p polargraph-bench --release -- bsbm --scale-factor 1 --data-dir /tmp/bsbm-data
cargo run -p polargraph-bench --release -- bsbm --scale-factor 5 --warmup-runs 5 --measure-runs 50
```

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | *(temp dir)* | RocksDB data directory |
| `--scale-factor` | `1` | Dataset scale (products = N×100, offers = N×50, …) |
| `--warmup-runs` | `10` | Discarded warm-up runs per query |
| `--measure-runs` | `100` | Measured runs per query |

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

---

## Part 3 — BSBM Results

### Dataset (scale factor 1)

| Entity | Count |
|--------|-------|
| Products | 100 |
| ProductTypes | 10 (3-level hierarchy: 2 roots, 3 mid, 5 leaves) |
| Features | 20 |
| Vendors | 5 |
| Offers | 50 |
| Reviews | 20 |

Dataset generated in ~7 ms.

### Query descriptions

| Query | Description | Key operation |
|-------|-------------|---------------|
| Q1 | Products of a type with a feature and numeric constraint | PSO scan + SPO filter + property post-filter |
| Q2 | All properties of a given product (detail lookup) | Single subject scan |
| Q3 | Products with two features and numeric range filter | Two-feature join + property scan |
| Q4 | Products with feature F1 OR feature F2 (UNION) | Two branch union, deduplication |
| Q5 | Products similar to a given product (shared features) | Two-hop star join |
| Q6 | Full-text search on product label | Trigram index scan |
| Q7 | Cheapest offer + review for a product (5-way join) | Multi-join: offers + vendors + reviews + reviewers |
| Q8 | All reviews for a product with reviewer info | Two-hop join |
| Q9 | All properties of a single review | Single subject scan |
| Q10 | Products offered by a specific vendor | Two-hop join: vendor → offer → product |
| Q11 | Review count for a product (COUNT) | Scan + count |
| Q12 | Products reviewed by a given reviewer | Two-hop join: reviewer → review → product |

### Results (scale factor 1, warmup=10, measure=100 runs, release build, Apple M-series)

| Query | avg | p50 | p95 | p99 | QPS |
|-------|-----|-----|-----|-----|-----|
| Q1 (product search) | 0.052 ms | 0.051 ms | 0.054 ms | 0.060 ms | 19,336 |
| Q2 (detail lookup) | 0.004 ms | 0.004 ms | 0.005 ms | 0.005 ms | 275,689 |
| Q3 (2-feature + range) | 0.051 ms | 0.051 ms | 0.056 ms | 0.074 ms | 19,611 |
| Q4 (UNION) | 0.015 ms | 0.015 ms | 0.017 ms | 0.019 ms | 64,706 |
| Q5 (similar products) | 0.022 ms | 0.022 ms | 0.028 ms | 0.030 ms | 44,589 |
| Q6 (full-text) | 0.158 ms | 0.158 ms | 0.178 ms | 0.209 ms | 6,347 |
| Q7 (5-way join) | 0.006 ms | 0.005 ms | 0.006 ms | 0.009 ms | 175,439 |
| Q8 (reviews) | 0.003 ms | 0.003 ms | 0.004 ms | 0.004 ms | 378,007 |
| Q9 (review detail) | 0.001 ms | 0.001 ms | 0.002 ms | 0.002 ms | 705,128 |
| Q10 (vendor offers) | 0.021 ms | 0.021 ms | 0.022 ms | 0.023 ms | 48,203 |
| Q11 (COUNT) | 0.001 ms | 0.001 ms | 0.001 ms | 0.001 ms | 990,991 |
| Q12 (reviewer) | 0.003 ms | 0.003 ms | 0.003 ms | 0.003 ms | 331,325 |
| **TOTAL** | — | — | — | — | **35,682 avg QPS** |

### Criterion micro-benchmarks (BSBM Q1 + Q7)

Added to `polargraph-storage/benches/storage.rs` in the `bsbm` group:

| Benchmark | Description |
|-----------|-------------|
| `bsbm/q1_product_search` | PSO + SPO feature filter + property scan at scale 1 |
| `bsbm/q7_five_way_join` | POS × 2 + SPO × 4 (offer/vendor/review/reviewer) at scale 1 |

```bash
# Run only the BSBM Criterion group
cargo bench -p polargraph-storage -- bsbm
```
