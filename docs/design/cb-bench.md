# cb-bench — engine performance harness and scale-up generator — design note

Status: approved (Mark, 2026-10-03). Scope narrowed 2026-10-03: **the
engine repo keeps the performance harness and the synthetic generator**
(they measure the engine); the repo-as-a-graph extractor, architecture
model and diagrams are an application built on public APIs —
`docs/design/repo-graph-app.md`.

## 1. Goal

Put numbers on the plan's performance targets (plan § Capacity) at team and
company scale, as a repeatable `polargraph-bench cb` scenario, and track
them in `BENCHMARKS.md`.

## 2. Seed

A small **hand-built seed** checked into `crates/polargraph-bench/` —
a few entity types (person, team, service, customer, record, chunk,
decision) with their relations, one ontology (for inference: subclasses,
inverses, a transitive `partOf`) and a few SHACL shapes (for the promotion
path). It fixes the *shape*; the generator multiplies it. No dependency on
the repo-graph application.

## 3. Generator and measurements

`polargraph-bench cb --scale <SF>` generates on top of the seed, following
the plan's company model:

- **Spine entities** (people, teams, services, customers) — counts from the
  plan scenarios; org structure as in WS4 seed packages.
- **Records with chunks** (WS7 ladder: envelope ~20 quads, 20 chunks × 3
  quads, text in `blob`, one 384-dim vector per chunk), **mentions** to
  entities with **Zipf skew (s ≈ 1.1)** so a few entities have 50K mentions.
- **Graphs and ACL**: record graphs per team, approved graphs per domain,
  proposals, grants per group (WS2.8) — so every read runs with a real user
  bitmap.
- **Decision-shaped projects**: synthetic projects with decisions, changes
  and supersession chains (Zipf-sized), exercising inference and
  time-travel at scale.
- **Vectors**: synthetic clustered unit vectors (so recall is measurable),
  int8 and f32 spaces.
- Loaded through **SST import** (WS2.7) for the base, then the live paths
  for the measured operations.

`SF = 1` ≈ team (~10M quads, 2M chunk vectors); `SF = 20` ≈ company
(~195M quads, 40M vectors).

| Plan target | How cb-bench measures it |
|---|---|
| Load one graph (1K quads) p50 ≤ 2 ms | `ExportGraph` / GSPO scan of record graphs |
| `describe(entity)` p95 ≤ 10 ms | All facts of a Zipf-sampled entity across a user's visible graphs (hot entities dominate the tail) |
| Hybrid search k=20 p95 ≤ 25 ms | `VectorSeedQuery` / `SearchVectorInSet` over an entity's mention set, f32 vs int8 |
| Context assembly 4K tokens p95 ≤ 300 ms | Proxy: describe + hybrid search + chunk text fetch (the real assembly is WS8, app-side) |
| Promote 500-quad proposal incl. SHACL p95 ≤ 200 ms | `ValidateShapes` overlay + `ApplyChanges` with `read_ts` |
| Incremental materialization lag ≤ 2 s | `--inference` under sustained writes; `polargraph_inference_lag_seconds` |
| Sustained ingestion ≥ 50 records/s | One `ApplyChanges` + vector batch per record, model latency excluded |
| (capacity) disk / RAM | Bytes per quad, HNSW RAM f32 vs int8 — checks the plan's estimates |

CI runs a small scale (`SF = 0.05`) as a **regression report** (no hard
thresholds on shared runners); the company run is manual on NVMe, with
results in `BENCHMARKS.md` and the tracker.


## 4. Layout

```
crates/polargraph-bench/
  seed/     ontology.ttl  shapes.ttl  seed.trig
  src/cb/   generate.rs  load.rs  measure.rs  report.rs
```

## 5. Decisions (as approved, engine side)

| # | Decision |
|---|---|
| G | Hand-built seed + synthetic generator (spine entities, records / chunks / mentions with Zipf skew, ACL graphs, decision-shaped projects, clustered vectors), `--scale`; SST bulk load for the base. |
| H | CI: `SF = 0.05` regression report, no hard thresholds; company scale manual on NVMe, recorded in `BENCHMARKS.md`. |
| J | Synthetic vectors (clustered, so recall is measurable); f32 and int8 spaces. |
| K | One engine PR: seed, generator, loader, measurements, report, CI job. |

## 6. As built

`polargraph-bench cb` (`crates/polargraph-bench/src/cb/`, seed in
`crates/polargraph-bench/seed/`):

- **Scale** (`--scale SF`, linear): 300 people, 15 teams, 120 services, 400
  customers, 100K records × 20 chunks, 200 decision projects at SF 1;
  384-dim vectors in 256 topic clusters. Generation is deterministic
  (SplitMix64 per item), so vectors are regenerated for exact recall.
- **Load**: seed + generated base through `SstImporter` in 1M-quad batches
  (IRIs into the dictionary, chunk text in `blob`); supersession on the
  live path (status `Replace`, so decisions have history); team-group
  grants; inference schema graph = `<urn:cb:ontology>` (runs the full
  materialization); vector spaces `cb_chunks_f32` (memory) and
  `cb_chunks_int8` (`--vectors both|f32|int8|none`).
- **Measure**: in-process through `PolarGraphServer` (the RPC handlers,
  graph ACL included; no network), reads as a sampled person. Each latency
  scenario stops at `--samples` or `--budget-secs`. Rows: the seven plan
  targets, plus ANN + join hybrid, time travel, recall@20 at ef 100 / 400
  (before ingestion), load and capacity figures.
- **Mapping notes**: "Load one graph" exports decision-project graphs
  nearest 1K quads (`ExportGraph`, as the owning team); "Hybrid search" is
  the plan's mitigation shape — mention set via `Query`, then
  `SearchVectorInSet` k = 20; context assembly is a proxy (describe +
  hybrid + top-chunk text); promotion validates a 500-quad overlay then
  `ApplyChanges` (adds to the approved graph, retractions from the proposal
  graph) with `read_ts`; inference lag runs the `--inference` task while
  decisions are written at `--lag-rate`.
- **Report**: plan targets as met / missed / unmeasured, then all rows;
  JSON (`--json`) and Markdown (`--markdown`, appended — CI writes the job
  summary).
- **CI**: job `cb-bench report` (`.github/workflows/ci.yml`), scale 0.05
  on main and PRs into main, no thresholds.
- **Finding**: the first runs exposed HNSW recall collapsing on clustered
  data and heavy f32 vector writes — fixed separately (tracker row 11a).
