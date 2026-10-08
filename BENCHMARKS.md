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

## Part 4 — cb-bench (ContxtBroker engine targets)

`polargraph-bench cb --scale SF` generates a synthetic company from the
hand-built seed (`crates/polargraph-bench/seed/`), bulk-loads it and
measures the plan's performance targets in-process, reading as a team
member with the graph ACL. Design: `docs/design/cb-bench.md`. CI runs
`--scale 0.05` on main and PRs as a job-summary report (no thresholds).

```bash
cargo run --release -p polargraph-bench -- cb --scale 0.05            # ~5 min
cargo run --release -p polargraph-bench -- cb --scale 1 --vectors f32  # ~55 min (team scale)
# --json report.json  --markdown report.md  --samples N  --budget-secs S
```

The plan's targets are for the **company** scenario (`--scale 20`, NVMe);
the runs below are smaller. Recorded 2026-10-05 on an Apple M4 Pro (14
cores, 48 GiB RAM, internal NVMe), after the HNSW connectivity fix.

### Summary

Team scale "first run" is cb-bench's first measurement (2026-10-05);
"current" is main plus `db/hybrid-search` after the fixes it led to
(HNSW connectivity #17, DRed #19, mmap append #20, hybrid search + `ef`).

| Plan target | Scale 0.05 (410K quads, 100K vectors) | Team scale, first run | Team scale, current |
|---|---|---|---|
| Load one graph (1K quads), p50 ≤ 2 ms | ✅ 0.87 ms | ✅ 0.76 ms | ✅ 0.97 ms |
| describe(entity), p95 ≤ 10 ms | ✅ 2.3 ms | ❌ 15 ms | ❌ 16 ms |
| Hybrid search k=20, p95 ≤ 25 ms | ❌ 256 ms | ❌ 2,681 ms | ❌ 162 ms (f32) / 140 ms (int8); ✅ 17 / 13 ms for mention sets < 10K chunks |
| Context assembly 4K tokens, p95 ≤ 300 ms | ✅ 260 ms | ❌ 2,253 ms | ✅ 173 ms |
| Promote 500-quad proposal incl. SHACL, p95 ≤ 200 ms | ✅ 58 ms | ❌ 450 ms | ❌ 452 ms |
| Incremental materialization lag ≤ 2 s | ✅ 1.3 s | ❌ 12.3 s | ✅ 0.97 s |
| Sustained ingestion ≥ 50 records/s | ✅ 101/s | ❌ 27/s | ❌ 27/s (f32), 42/s (int8) |
| int8 vector space at 2M vectors | — | unmeasured (bulk load unfinished) | builds in 24 min; recall@20 0.94 at ef 400 |
| Recall@20 at the default `ef` | 1.0 | 0.52 (ef 50) | 0.96 f32 / 0.94 int8 (ef 400) |

Open items behind the remaining misses are on the "Later / nice-to-have"
list in [`docs/STATUS.md`](docs/STATUS.md).

### Findings (scale 1, first run)

Status of each (fixed, open, with numbers): `docs/STATUS.md` — 1 fixed
(#19), 3 fixed (#20), 5 fixed and 2 mostly fixed (hybrid search; the
popular-entity cache is on the "Later" list), 4 and 6 open.

1. **Inference lag grows with the store, not the change.** Each DRed batch
   (`owl_rl::infer_changes`) scans every live inferred quad
   (`live_inferred`, 4.1M at scale 1) to know what is already stored.
2. **Hybrid search over a mention set doesn't scale.** Hot entities have
   mention sets of up to 86K chunks; building the set (mention → chunks
   query) costs up to 323 ms p95 and `SearchVectorInSet` the rest, including
   a per-node graph-visibility scan. The ANN + join alternative is fast
   (7 ms) but keeps a median of one result for a specific entity.
3. **Promotion slows as the approved graph grows** (58 → 450 ms p95); not yet
   profiled (SHACL overlay validation vs `ApplyChanges`).
4. **Ingestion** drops to 27 records/s: HNSW inserts into a 2M-vector graph
   dominate (~36 ms per record of 20 chunks, single writer).
5. **Recall at 2M vectors** needs a higher `ef`: recall@20 0.64 at ef 100,
   0.96 at ef 400 (server default ef is 50).
6. **mmap / int8 bulk load is quadratic**: every append to a `.vecs` file
   resizes, remaps and flushes the whole mapping.
7. **Capacity matches the plan**: 11.0 GB disk vs ~9 GB estimated; HNSW RAM
   (memory mode) 4.0 GB vs ~4 GB. `hnsw` is the largest column family
   (3.9 GB); each quad-order CF is ~0.6 GB.

### Full reports

#### cb-bench — scale 0.05 (410042 base quads, 209022 inferred, 100000 chunks)

Host: macos · Apple M4 Pro · 14 cores · 48 GiB RAM. In-process (no network); reads as a team member with the graph ACL.

| Plan target | Status | Measured |
|---|:-:|---|
| Load one graph (1K quads) | ✅ met | p50 0.87 ms |
| describe(entity) across visible graphs | ✅ met | p95 2.32 ms |
| Hybrid search, k=20 | ❌ missed | (f32) p95 256 ms; (int8) p95 257 ms |
| Context assembly, 4K tokens | ✅ met | p95 260 ms |
| Promote a 500-quad proposal incl. SHACL | ✅ met | p95 58 ms |
| Incremental materialization lag | ✅ met | p95 1321 ms |
| Sustained ingestion | ✅ met | 101/s |

| Measurement | Target | n | p50 ms | p95 ms | p99 ms | max ms | Rate | Meets | Notes |
|---|---|---:|---:|---:|---:|---:|---:|:-:|---|
| Load one graph (ExportGraph) | p50 ≤ 2 ms | 200 | 0.87 | 1.49 | 1.51 | 1.57 |  | ✅ | 1034 quads per graph on average |
| describe(entity) | p95 ≤ 10 ms | 200 | 0.61 | 2.32 | 2.44 | 3.23 |  | ✅ | facts returned: median 548, max 1959 |
| Hybrid search k=20, mention set (f32) | p95 ≤ 25 ms | 200 | 85 | 256 | 264 | 265 |  | ❌ | set size: median 9800, max 28760 |
| Hybrid search, ANN k=200 + join (f32) |  | 200 | 1.97 | 2.48 | 2.72 | 3.00 |  |  | results kept: median 23, max 124 |
| Hybrid search k=20, mention set (int8) | p95 ≤ 25 ms | 200 | 86 | 257 | 261 | 266 |  | ❌ | set size: median 9800, max 28760 |
| Hybrid search, ANN k=200 + join (int8) |  | 200 | 1.91 | 2.42 | 2.69 | 3.21 |  |  | results kept: median 23, max 123 |
| Context assembly proxy (describe + hybrid + chunk text) | p95 ≤ 300 ms | 200 | 85 | 260 | 273 | 292 |  | ✅ | 14067 bytes of chunk text (~3517 tokens) |
| Promote 500-quad proposal incl. SHACL | p95 ≤ 200 ms | 200 | 50 | 58 | 60 | 63 |  | ✅ | 0 SHACL results in total (expected 0) |
| Time travel: decision status as of load vs now |  | 200 | 0.01 | 0.01 | 0.02 | 114 |  |  | 200/200 differed (expected all) |
| Sustained ingestion (ApplyChanges + 20 vectors per record) | ≥ 50 records/s | 500 | 9.80 | 12 | 13 | 14 | 101/s | ✅ | 500 records, single writer, vectors into cb_chunks_f32 |
| Incremental materialization lag | ≤ 2 s (p95) | 381 | 861 | 1321 | 1347 | 1428 |  | ✅ | 381 commits at 20/s over 20 s; 0 not covered within 10 s |

| Load / capacity | Value |
|---|---|
| Base load (SST import) | 410042 quads in 2.9 s (139312 quads/s) |
| Supersession pass (live writes) | 347 status changes in 0.0 s |
| Graph grants | 28 |
| Full materialization | 209022 inferred quads in 2.8 s |
| Vector space `cb_chunks_f32` (f32) | 100000 vectors in 59.9 s; vector RAM 146.5 MiB; RSS growth 293.0 MiB |
| Vector space `cb_chunks_int8` (int8) | 100000 vectors in 64.1 s; vector RAM 37.0 MiB; RSS growth 173.1 MiB |
| Recall@20 `cb_chunks_f32 ef=100` | 1.000 |
| Recall@20 `cb_chunks_f32 ef=400` | 1.000 |
| Recall@20 `cb_chunks_int8 ef=100` | 1.000 |
| Recall@20 `cb_chunks_int8 ef=400` | 1.000 |
| Disk (after compaction) | 2596.7 MiB (4398 bytes per quad, incl. inferred quads, blobs and vectors) |
| Disk vs plan estimate (~9 GB × scale) | 2.7 GB vs ~0.5 GB |
| HNSW RAM, memory mode, vs plan (~4 GB × scale) | 0.31 GB vs ~0.20 GB |

#### cb-bench — scale 1 (8008381 base quads, 4130354 inferred, 2000000 chunks)

Host: macos · Apple M4 Pro · 14 cores · 48 GiB RAM. In-process (no network); reads as a team member with the graph ACL.

| Plan target | Status | Measured |
|---|:-:|---|
| Load one graph (1K quads) | ✅ met | p50 0.76 ms |
| describe(entity) across visible graphs | ❌ missed | p95 15 ms |
| Hybrid search, k=20 | ❌ missed | p95 2681 ms |
| Context assembly, 4K tokens | ❌ missed | p95 2253 ms |
| Promote a 500-quad proposal incl. SHACL | ❌ missed | p95 450 ms |
| Incremental materialization lag | ❌ missed | p95 12251 ms |
| Sustained ingestion | ❌ missed | 27/s |

| Measurement | Target | n | p50 ms | p95 ms | p99 ms | max ms | Rate | Meets | Notes |
|---|---|---:|---:|---:|---:|---:|---:|:-:|---|
| Load one graph (ExportGraph) | p50 ≤ 2 ms | 200 | 0.76 | 1.21 | 1.33 | 2.28 |  | ✅ | 917 quads per graph on average |
| describe(entity) | p95 ≤ 10 ms | 200 | 2.64 | 15 | 16 | 21 |  | ❌ | facts returned: median 619, max 4454 |
| Hybrid search k=20, mention set (f32) | p95 ≤ 25 ms | 30 | 366 | 2681 | 4750 | 4750 |  | ❌ | set size: median 13720, max 86020; building the set (mention query) p95 323 ms, the rest is SearchVectorInSet |
| Hybrid search, ANN k=200 + join (f32) |  | 200 | 5.68 | 6.89 | 7.41 | 7.45 |  |  | results kept: median 1, max 17 |
| Context assembly proxy (describe + hybrid + chunk text) | p95 ≤ 300 ms | 54 | 156 | 2253 | 2276 | 2276 |  | ❌ | 14067 bytes of chunk text (~3517 tokens) |
| Promote 500-quad proposal incl. SHACL | p95 ≤ 200 ms | 69 | 426 | 450 | 473 | 473 |  | ❌ | 0 SHACL results in total (expected 0) |
| Time travel: decision status as of load vs now |  | 200 | 0.03 | 0.04 | 0.04 | 94 |  |  | 200/200 differed (expected all) |
| Sustained ingestion (ApplyChanges + 20 vectors per record) | ≥ 50 records/s | 500 | 36 | 43 | 48 | 51 | 27/s | ❌ | 500 records, single writer, vectors into cb_chunks_f32 |
| Incremental materialization lag | ≤ 2 s (p95) | 381 | 9257 | 12251 | 12516 | 12589 |  | ❌ | 381 commits at 20/s over 20 s; 0 not covered within 10 s |

| Load / capacity | Value |
|---|---|
| Base load (SST import) | 8008381 quads in 72.6 s (110255 quads/s) |
| Supersession pass (live writes) | 1167 status changes in 0.0 s |
| Graph grants | 290 |
| Full materialization | 4130354 inferred quads in 116.3 s |
| Vector space `cb_chunks_f32` (f32) | 2000000 vectors in 2195.8 s; vector RAM 2929.7 MiB; RSS growth 3821.3 MiB |
| Recall@20 `cb_chunks_f32 ef=100` | 0.640 |
| Recall@20 `cb_chunks_f32 ef=400` | 0.963 |
| Disk (after compaction) | 10533.4 MiB (910 bytes per quad, incl. inferred quads, blobs and vectors) |
| Disk by column family | `hnsw` 3890.1 MiB, `ospg` 596.8 MiB, `opsg` 596.4 MiB, `psog` 595.8 MiB, `blob` 574.1 MiB, `sopg` 565.4 MiB, `gspo` 564.5 MiB, `gpos` 553.7 MiB; `.vecs` files 0.0 MiB |
| Disk vs plan estimate (~9 GB × scale) | 11.0 GB vs ~9.0 GB |
| HNSW RAM, memory mode, vs plan (~4 GB × scale) | 4.01 GB vs ~4.00 GB |

### int8 at team scale (after the mmap append fix)

Recorded 2026-10-06, same host. `--scale 1 --vectors int8`. Inference lag
here predates the DRed batch fix (branch `db/dred-batch-cost`, 0.97 s p95).

#### cb-bench — scale 1 (8008381 base quads, 4130354 inferred, 2000000 chunks)

Host: macos · Apple M4 Pro · 14 cores · 48 GiB RAM. In-process (no network); reads as a team member with the graph ACL.

| Plan target | Status | Measured |
|---|:-:|---|
| Load one graph (1K quads) | ✅ met | p50 0.86 ms |
| describe(entity) across visible graphs | ❌ missed | p95 16 ms |
| Hybrid search, k=20 | ❌ missed | p95 2959 ms |
| Context assembly, 4K tokens | ❌ missed | p95 2343 ms |
| Promote a 500-quad proposal incl. SHACL | ❌ missed | p95 449 ms |
| Incremental materialization lag | ❌ missed | p95 12416 ms |
| Sustained ingestion | ❌ missed | 42/s |

| Measurement | Target | n | p50 ms | p95 ms | p99 ms | max ms | Rate | Meets | Notes |
|---|---|---:|---:|---:|---:|---:|---:|:-:|---|
| Load one graph (ExportGraph) | p50 ≤ 2 ms | 200 | 0.86 | 1.45 | 1.55 | 3.23 |  | ✅ | 917 quads per graph on average |
| describe(entity) | p95 ≤ 10 ms | 200 | 2.85 | 16 | 18 | 22 |  | ❌ | facts returned: median 619, max 4454 |
| Hybrid search k=20, mention set (int8) | p95 ≤ 25 ms | 25 | 811 | 2959 | 4878 | 4878 |  | ❌ | set size: median 24860, max 86020; building the set (mention query) p95 378 ms, the rest is SearchVectorInSet |
| Hybrid search, ANN k=200 + join (int8) |  | 200 | 5.98 | 7.00 | 8.29 | 8.70 |  |  | results kept: median 1, max 18 |
| Context assembly proxy (describe + hybrid + chunk text) | p95 ≤ 300 ms | 53 | 162 | 2343 | 2624 | 2624 |  | ❌ | 14068 bytes of chunk text (~3517 tokens) |
| Promote 500-quad proposal incl. SHACL | p95 ≤ 200 ms | 68 | 431 | 449 | 453 | 453 |  | ❌ | 0 SHACL results in total (expected 0) |
| Time travel: decision status as of load vs now |  | 200 | 0.03 | 0.04 | 0.04 | 100 |  |  | 200/200 differed (expected all) |
| Sustained ingestion (ApplyChanges + 20 vectors per record) | ≥ 50 records/s | 500 | 23 | 29 | 31 | 46 | 42/s | ❌ | 500 records, single writer, vectors into cb_chunks_int8 |
| Incremental materialization lag | ≤ 2 s (p95) | 381 | 9418 | 12416 | 12731 | 12887 |  | ❌ | 381 commits at 20/s over 20 s; 0 not covered within 10 s |

| Load / capacity | Value |
|---|---|
| Base load (SST import) | 8008381 quads in 88.6 s (90393 quads/s) |
| Supersession pass (live writes) | 1167 status changes in 0.0 s |
| Graph grants | 290 |
| Full materialization | 4130354 inferred quads in 141.2 s |
| Vector space `cb_chunks_int8` (int8) | 2000000 vectors in 1442.4 s; vector RAM 740.1 MiB; RSS growth 2351.0 MiB |
| Recall@20 `cb_chunks_int8 ef=100` | 0.693 |
| Recall@20 `cb_chunks_int8 ef=400` | 0.920 |
| Disk (after compaction) | 11525.2 MiB (996 bytes per quad, incl. inferred quads, blobs and vectors) |
| Disk by column family | `hnsw` 1671.9 MiB, `ospg` 597.9 MiB, `opsg` 597.5 MiB, `psog` 597.2 MiB, `blob` 574.1 MiB, `sopg` 568.0 MiB, `gspo` 564.5 MiB, `gpos` 555.4 MiB; `.vecs` files 3072.0 MiB |
| Disk vs plan estimate (~9 GB × scale) | 12.1 GB vs ~9.0 GB |

### Hybrid search and default `ef` at team scale (after `db/hybrid-search`)

Recorded 2026-10-08, same host, `--scale 1 --vectors both`. Hybrid search
now passes candidates as patterns (`SearchVectorInSet.candidate_patterns`);
the old id path is kept as a comparison row. Before → after (p95):
hybrid search 3,683 → 162 ms (f32), 2,412 → 140 ms (int8); context
assembly 2,420 → 173 ms; recall@20 at the default `ef` 0.52 → 0.96 (f32),
0.50 → 0.94 (int8).

#### cb-bench — scale 1 (8008381 base quads, 4130354 inferred, 2000000 chunks)

Host: macos · Apple M4 Pro · 14 cores · 48 GiB RAM. In-process (no network); reads as a team member with the graph ACL.

| Plan target | Status | Measured |
|---|:-:|---|
| Load one graph (1K quads) | ✅ met | p50 0.97 ms |
| describe(entity) across visible graphs | ❌ missed | p95 16 ms |
| Hybrid search, k=20 | ❌ missed | (f32) p95 162 ms; (int8) p95 140 ms |
| Context assembly, 4K tokens | ✅ met | p95 173 ms |
| Promote a 500-quad proposal incl. SHACL | ❌ missed | p95 452 ms |
| Incremental materialization lag | ✅ met | p95 969 ms |
| Sustained ingestion | ❌ missed | 27/s |

| Measurement | Target | n | p50 ms | p95 ms | p99 ms | max ms | Rate | Meets | Notes |
|---|---|---:|---:|---:|---:|---:|---:|:-:|---|
| Load one graph (ExportGraph) | p50 ≤ 2 ms | 200 | 0.97 | 1.66 | 1.89 | 2.72 |  | ✅ | 917 quads per graph on average |
| describe(entity) | p95 ≤ 10 ms | 200 | 2.78 | 16 | 18 | 21 |  | ❌ | facts returned: median 619, max 4454 |
| Hybrid search k=20, mention set (f32) | p95 ≤ 25 ms | 200 | 18 | 162 | 386 | 5120 |  | ❌ | candidates as patterns (mention → chunks) evaluated in the server; p95 for sets ≥ 10K chunks 173 ms (n 96), < 10K 17 ms (n 104) |
| Mention set as ids, k=20 (f32) |  | 200 | 20 | 206 | 214 | 218 |  |  | set size: median 8840, max 86020; building the set (mention query) p95 161 ms, the rest is SearchVectorInSet |
| Hybrid search, ANN k=200 + join (f32) |  | 200 | 3.60 | 5.29 | 6.24 | 8.16 |  |  | results kept: median 2, max 28 |
| Hybrid search k=20, mention set (int8) | p95 ≤ 25 ms | 200 | 14 | 140 | 149 | 162 |  | ❌ | candidates as patterns (mention → chunks) evaluated in the server; p95 for sets ≥ 10K chunks 143 ms (n 96), < 10K 13 ms (n 104) |
| Mention set as ids, k=20 (int8) |  | 200 | 18 | 189 | 193 | 200 |  |  | set size: median 8840, max 86020; building the set (mention query) p95 161 ms, the rest is SearchVectorInSet |
| Hybrid search, ANN k=200 + join (int8) |  | 200 | 3.29 | 4.62 | 5.84 | 6.00 |  |  | results kept: median 2, max 28 |
| Context assembly proxy (describe + hybrid + chunk text) | p95 ≤ 300 ms | 200 | 21 | 173 | 177 | 191 |  | ✅ | 14069 bytes of chunk text (~3517 tokens) |
| Promote 500-quad proposal incl. SHACL | p95 ≤ 200 ms | 68 | 435 | 452 | 466 | 466 |  | ❌ | 0 SHACL results in total (expected 0) |
| Time travel: decision status as of load vs now |  | 200 | 0.04 | 0.04 | 0.04 | 108 |  |  | 200/200 differed (expected all) |
| Sustained ingestion (ApplyChanges + 20 vectors per record) | ≥ 50 records/s | 500 | 37 | 44 | 49 | 61 | 27/s | ❌ | 500 records, single writer, vectors into cb_chunks_f32 |
| Incremental materialization lag | ≤ 2 s (p95) | 380 | 529 | 969 | 1013 | 1023 |  | ✅ | 380 commits at 20/s over 20 s; 0 not covered within 10 s |

| Load / capacity | Value |
|---|---|
| Base load (SST import) | 8008381 quads in 74.3 s (107726 quads/s) |
| Supersession pass (live writes) | 1167 status changes in 0.0 s |
| Graph grants | 290 |
| Full materialization | 4130354 inferred quads in 119.2 s |
| Vector space `cb_chunks_f32` (f32) | 2000000 vectors in 2215.9 s; vector RAM 2929.7 MiB; RSS growth 2417.8 MiB |
| Vector space `cb_chunks_int8` (int8) | 2000000 vectors in 1368.0 s; vector RAM 740.1 MiB; RSS growth 1787.8 MiB |
| Recall@20 `cb_chunks_f32 ef=50 (search p50 0.65 ms, p95 0.95 ms)` | 0.520 |
| Recall@20 `cb_chunks_f32 ef=100 (search p50 1.02 ms, p95 1.63 ms)` | 0.677 |
| Recall@20 `cb_chunks_f32 ef=200 (search p50 1.55 ms, p95 2.63 ms)` | 0.881 |
| Recall@20 `cb_chunks_f32 ef=400, server default (search p50 2.30 ms, p95 3.66 ms)` | 0.956 |
| Recall@20 `cb_chunks_f32 ef=800 (search p50 3.16 ms, p95 4.60 ms)` | 0.983 |
| Recall@20 `cb_chunks_int8 ef=50 (search p50 0.47 ms, p95 0.82 ms)` | 0.501 |
| Recall@20 `cb_chunks_int8 ef=100 (search p50 0.73 ms, p95 1.23 ms)` | 0.685 |
| Recall@20 `cb_chunks_int8 ef=200 (search p50 1.21 ms, p95 1.91 ms)` | 0.844 |
| Recall@20 `cb_chunks_int8 ef=400, server default (search p50 1.97 ms, p95 3.30 ms)` | 0.939 |
| Recall@20 `cb_chunks_int8 ef=800 (search p50 2.95 ms, p95 4.70 ms)` | 0.984 |
| Disk (after compaction) | 17099.9 MiB (1477 bytes per quad, incl. inferred quads, blobs and vectors) |
| Disk by column family | `hnsw` 5553.8 MiB, `ospg` 595.4 MiB, `opsg` 594.9 MiB, `psog` 594.2 MiB, `blob` 574.1 MiB, `sopg` 567.9 MiB, `gspo` 563.5 MiB, `spog` 552.5 MiB; `.vecs` files 3072.0 MiB |
| Disk vs plan estimate (~9 GB × scale) | 17.9 GB vs ~9.0 GB |
| HNSW RAM, memory mode, vs plan (~4 GB × scale) | 2.54 GB vs ~4.00 GB |
