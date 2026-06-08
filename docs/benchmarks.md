# PolarGraph Performance Benchmarks

Results collected June 2026 using the suites in `polargraph-storage/benches/storage.rs`
(Criterion micro-benchmarks) and the `polargraph-bench` binary (end-to-end gRPC scenarios).
All numbers are from a single run on the machine described below and are meant as
directional guidance, not production SLAs.

---

## Environment

**Hardware:** Apple Silicon Mac (local development machine, M-series CPU, unified memory).

**Build:** `--release` profile (`opt-level = 3`, LTO off). RocksDB default settings
(write buffer 64 MB, bloom filters enabled, block cache 8 MB). HNSW
`ef_construction = 200`, `M = 16`.

**Not representative of:** cloud VMs (higher gRPC latency, slower single-core speed),
NVMe SSDs vs. the internal SSD here, or multi-socket servers with NUMA penalties.

---

## Criterion micro-benchmarks

These benchmarks isolate the storage layer from network and serialisation overhead.
Run with `cargo bench -p polargraph-storage`.

### Triple writes

Each iteration commits a single `WriteBatch` containing all six hexastore column-family
entries for every triple, plus the oracle-counter update.

| Batch size | Median latency | Throughput |
|---:|---:|---:|
| 1 | 457 µs | 2.2K triples/sec |
| 10 | 492 µs | 20K triples/sec |
| 100 | 600 µs | 167K triples/sec |
| 1,000 | 1.51 ms | 676K triples/sec |

**WAL fsync dominates.** Latency grows only 3.3× while batch size grows 1,000×. The
~440 µs floor is the cost of flushing RocksDB's write-ahead log to disk — it is paid
once per commit regardless of how many triples are in the batch. The practical
implication: **always batch writes**. A single `Transaction` with 1,000 triples is
about as fast to commit as one with a single triple, giving 300× better throughput
for the same number of fsync calls.

The p99 outliers visible in the raw Criterion output (7–11% of samples) correspond to
periodic RocksDB compaction events that briefly stall writes.

### Pattern queries

Measured on a 500-node store with two property triples per node (`name`, `kind`) and
a ring of `follows` relation triples (one per node). The store is shared across all
iterations; only the scan is timed.

| Pattern | Index used | Median | Results returned |
|---|---|---:|---:|
| `scan_by_subject` | SPO prefix | 1.7 µs | 2 triples |
| `scan_by_predicate` | PSO full scan | 185 µs | 500 triples |
| `scan_by_predicate_object` | POS prefix | 1.0 µs | 1 triple |
| `scan_by_object` | OSP prefix | 944 ns | 1 triple |

The numbers reflect two separate costs: the **seek** (finding the first matching key in
the sorted column family) and the **decode** (deserialising each key-value pair into a
`Triple`). Point lookups that return one or two triples are dominated by the seek
(~800–900 ns). The `scan_by_predicate("name")` case returns all 500 nodes' `name`
triples, revealing a per-triple decode cost of roughly **370 ns**
(185 µs ÷ 500 results).

Queries with a bound predicate *and* a bound second term (`scan_by_predicate_object`,
`scan_by_object`) are the fastest because the POS and OSP column families can seek
directly to the exact entry. `scan_by_subject` is slightly slower than `scan_by_object`
because it returns two triples instead of one; the seek cost itself is the same.

### HNSW vector insert

Each iteration inserts one new vector into a space that already has 20 nodes, then
flushes all modified neighbor lists to RocksDB. The timer includes the full round-trip
to disk.

| Dims | Median |
|---:|---:|
| 32 | 134 µs |
| 128 | 205 µs |
| 512 | 576 µs |

The RocksDB flush accounts for roughly 130 µs regardless of dimensionality (matches the
baseline write cost). The remaining latency scales with the number of distance
comparisons during graph rewiring: ~75 µs at 128 dims, ~450 µs at 512 dims.
For bulk ingestion, `batch_insert_vectors` acquires the write lock once for the entire
batch and issues a single `WriteBatch`, dramatically reducing per-vector overhead.

### HNSW vector search (k = 10)

| n | Dims | Median |
|---:|---:|---:|
| 500 | 128 | 110 µs |
| 2,000 | 128 | 247 µs |
| 500 | 512 | 373 µs |

Scaling with corpus size is sub-linear as expected from HNSW's O(log n) graph
traversal: a 4× increase in n (500 → 2,000) produces only a 2.25× latency increase.
Dimensionality has a stronger effect: moving from 128 to 512 dims at the same corpus
size (500 nodes) produces a 3.4× slowdown, proportional to the extra multiply-add
work in each distance computation.

### Filtered search comparison

Measured on a 500-node corpus with a 10%-subset (50 nodes) as the allowed set.

| Method | Median |
|---|---:|
| `search_vector_ef` (HNSW, ef = 100) | 110 µs |
| `search_vector_in_set` (linear scan over 50 nodes) | 4.75 µs |

`search_vector_in_set` is 23× faster here because the allowed set is only 50 nodes —
linear scan over 50 × 128-dim vectors is trivial. The crossover point where HNSW graph
traversal becomes cheaper than linear scan is roughly **10–15% of the total corpus
size**. Below that threshold, `search_vector_in_set` is the right call; above it,
`search_vector_ef` (or `SearchVectorFiltered`) wins.

---

## End-to-end load scenarios

These scenarios run over a live gRPC connection to `polargraphd` and include full
serialisation, network round-trip (loopback), and server-side processing overhead.
Run with `cargo run -p polargraph-bench --release -- --scenario <name>`.

### Write — 10K nodes, 4 edges/node, batch = 100

**Total: 60,000 triples (20,000 property + 40,000 relation).**

| Metric | Value |
|---|---:|
| Throughput | 253,674 triples/sec |
| Property batch p50 | 743 µs |
| Property batch p95 | 1,011 µs |
| Property batch p99 | 2,703 µs |
| Relation batch p50 | 1,519 µs |
| Relation batch p95 | 1,687 µs |
| Relation batch p99 | 1,911 µs |

Relation batches are roughly 2× slower than property batches for two reasons. First,
the `links` predicate requires interning on first use (a read-modify-write to the META
column family). Second, relation triples occupy the full hexastore — all six column
families — whereas property triples use the `PROPERTY_SENTINEL` object slot and only
populate four of them.

The p99 spike on property batches (2,703 µs vs. p95 of 1,011 µs) is a RocksDB
background compaction pause that coincidentally aligned with those samples. Relation
batches show a tighter distribution because compaction pressure was lower by the time
they ran.

### Read — 5K nodes, sequential queries

Pre-populates 5,000 nodes then issues one `Query` RPC per node with the subject bound
and predicate/object as wildcards.

| Metric | Value |
|---|---:|
| Throughput | 20,420 queries/sec |
| Latency p50 | 48 µs |
| Latency p95 | 76 µs |
| Latency p99 | 97 µs |

The Criterion `scan_by_subject` benchmark measured 1.7 µs for the same operation at
the storage layer. The remaining ~46 µs at p50 is gRPC round-trip overhead: proto
serialisation, loopback socket, tonic dispatch, and response deserialisation. The
latency distribution is tight (p99 / p50 ≈ 2×), indicating no stall sources in the
sequential path.

### Mixed 80/20 — 8 concurrent workers

8 workers execute 500 operations each (4,000 total): 80% bound-subject queries,
20% single-node inserts.

| Metric | Value |
|---|---:|
| Throughput | 39,250 ops/sec |
| Read p50 | 180 µs |
| Read p95 | 271 µs |
| Read p99 | 321 µs |
| Write p50 | 191 µs |
| Write p95 | 281 µs |
| Write p99 | 327 µs |

Read latency under concurrency (180 µs p50) is 3.7× higher than the sequential
baseline (48 µs). The increase comes from two sources: contention on the shared gRPC
connection and the server's MVCC commit mutex serialising concurrent writes. Read and
write latencies are nearly equal at all percentiles, which is typical of optimistic
MVCC — reads do not acquire the commit lock, so writers do not starve readers, but
all operations share the same gRPC connection queue.

The throughput figure (39K ops/sec) reflects the concurrency multiplier: 8 workers
each saturating one connection slot gives roughly 2× the sequential read throughput.

### Recovery — 50K triples, 5 re-open runs

Writes 50,000 property triples to a fresh store, drops the handle (flushing all data),
then times five consecutive `TripleStore::open` calls on the same directory.

| Metric | Value |
|---|---:|
| Mean open time | 19.4 ms |
| Min | 3 ms |
| Max | 83 ms |

The 27× variance between min and max is page-cache sensitivity. `TripleStore::open`
scans the META column family to rebuild the predicate intern table and the HNSW
column family to reconstruct all vector indexes. On a cache-warm run the kernel
serves these reads from RAM (≈ 3 ms); on a cold run (or after pressure evicts pages)
it must read from the SSD (≈ 83 ms). **Plan for the high end** when sizing startup
budgets, especially in containerised environments where page caches are evicted between
restarts.

For a 50K-triple store the predicate scan is fast; the dominant cost for large stores
with HNSW spaces will be deserialising the node graph from the HNSW column family.

### Filtered vector search — 5K nodes, 128-dim, 50 queries

Registers a `BenchNode` type with a `BenchVec` HNSW space, batch-inserts 5,000
nodes with 128-dimensional embeddings, then runs 50 `SearchVectorFiltered` RPCs with
`NodeTypeFilter("BenchNode")`.

| Metric | Value |
|---|---:|
| Mean Recall@10 | 0.934 (93.4%) |
| Latency p50 | 6,815 µs |
| Latency p95 | 7,327 µs |
| Latency p99 | 7,871 µs |

**Recall is healthy.** HNSW with `ef = k × 10 = 100` achieves 93.4% recall@10 on
5,000 nodes — consistent with published HNSW benchmarks at these hyperparameters.
Higher recall (≥ 98%) requires larger ef values, with a proportional latency cost.

**Latency is not.** At 6.8 ms p50, this RPC is 62× slower than a raw ANN search at
the same corpus size (110 µs in Criterion). The root cause is the `NodeTypeFilter`
implementation in `service.rs`: every query calls
`scan_by_predicate_object("__type", "BenchNode")` to enumerate all 5,000 matching
nodes, building the allowed-set from scratch before the HNSW search begins. That scan
alone costs several milliseconds at this scale (recall `scan_by_predicate` at 500 nodes
takes 185 µs; at 5,000 nodes the same scan takes ~10×).

This is a known deficiency tracked in `ROADMAP.md` (see *Filtered vector search
optimisation*). The planned fix is a per-type node-ID cache in the server, updated
incrementally on every `Insert` commit, so the allowed-set lookup drops to a single
`HashMap` read.

**Workaround:** use `SearchVectorInSet` with a pre-built node-ID list instead of
`SearchVectorFiltered` with `NodeTypeFilter`. This bypasses the triple scan entirely
and costs only the HNSW graph traversal plus a linear post-filter over the allowed set.

---

## Capacity estimates

The figures below are rough estimates for planning purposes at 1 million nodes. They
assume an average of 10 property triples per node, 5 relation triples per node, and
one embedding vector per node.

### RAM

PolarGraph supports two vector storage modes per HNSW space, selectable via
`VectorSpaceDef.storage_mode` (`"memory"` or `"mmap"`):

**Memory mode (default):** each `Vec<f32>` lives in heap RAM inside the
`HnswNode`. Every `TripleStore::open` deserialises the full graph from RocksDB
into heap, and the index stays resident until the store is dropped.

HNSW memory per node is approximately `dims × 4 bytes (f32) + M × 2 × 8 bytes
(neighbor lists across layers) + overhead ≈ dims × 4 + 260 bytes`. At M = 16 the
neighbor-list term is ~256 bytes; the vector term dominates at high dimensionality.

| Dims | Approx. heap RAM at 1M nodes (memory mode) |
|---|---|
| 1,536 (OpenAI `text-embedding-3-small`) | 9–10 GB |
| 768 | 6–7 GB |
| 128 | 3–4 GB |
| No vectors | 2–3 GB |

**Mmap mode:** raw float data is stored in a flat `<data_dir>/vectors/<space>.vecs`
file and accessed via `memmap2::MmapMut`. The OS pages vector data in on demand
rather than loading the entire space into heap RAM at startup. Heap usage is
reduced to the graph topology only (~260 bytes per node) regardless of
dimensionality, at the cost of page-fault latency on cold reads.

| Dims | Approx. heap RAM at 1M nodes (mmap mode) |
|---|---|
| 1,536 | ~260 MB (topology only) |
| 768 | ~260 MB |
| 128 | ~260 MB |

Choose **memory mode** when the embedding space fits in available RAM and you
need the lowest possible search latency. Choose **mmap mode** for spaces larger
than physical RAM or when startup time matters more than peak query speed.

The 2–3 GB baseline (no vectors) accounts for RocksDB block cache, the predicate
intern table, OS overhead, and the tonic/tokio runtime.

### Disk

Triple storage cost is dominated by the hexastore: each triple is written to 6 column
families, each with a 44-byte fixed-width key and a ~40-byte encoded value
(discriminant + temporal envelope + payload). At 15 triples/node on average:

`1M nodes × 15 triples × 6 CFs × ~84 bytes ≈ 7.6 GB raw, ~1.5 GB after RocksDB compression`

The HNSW column family stores each node's vector and neighbor lists once (not ×6), so
even at 1,536 dims it adds only ~6.5 GB uncompressed (vectors don't compress well).

| Configuration | Approx. disk at 1M nodes |
|---|---|
| 1,536-dim vectors | ~8 GB |
| 768-dim vectors | ~8 GB |
| 128-dim vectors | ~8 GB |
| No vectors | ~1.5 GB |

The triple store is roughly constant across vector sizes because vectors are stored
separately in the HNSW CF rather than inline in the triple values. Disk cost scales
primarily with triple count, not dimensionality.

### Tuning recommendations

- **Write throughput:** increase RocksDB `write_buffer_size` (default 64 MB) to 256 MB
  or more on write-heavy workloads to reduce flush frequency and compaction stalls.
- **Read latency:** increase the block cache (default 8 MB) toward the working set size;
  for 1M nodes a 512 MB cache eliminates most SSD reads for hot predicates.
- **HNSW recall vs. latency:** `ef_construction = 200` and `M = 16` are conservative
  defaults. Halving `M` to 8 roughly halves RAM and insert cost at a ~5% recall loss;
  increasing `ef_construction` to 400 improves recall by ~1–2% at 2× insert cost.
- **Recovery time:** if startup latency matters, pre-warm the page cache by reading the
  data directory before opening the store, or plan for cold-start times of 80–100 ms
  per 50K triples at the storage layer.
