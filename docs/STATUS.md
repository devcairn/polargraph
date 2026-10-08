# Engine status — handoff

Snapshot: 2026-10-07. Detailed history: §0.2 tracker in
`docs/contxtbroker-platform-plan.md`. Per-feature notes: `CLAUDE.md`
("Current state"), `docs/design/*`, release notes `docs/upgrade-*.md`.

## Merged to main

| Step | What | PR |
|---|---|---|
| 0–3 | Foundation fixes (retention, skolemization, trigram cap), canonical terms, IRI dictionary, lossless literals, storage format v3 (offline `polargraphd migrate`) | — |
| 4 | Named graphs: `GraphScope`, graph RPCs, N-Quads/TriG | #2 |
| 5 | Graph-aware SPARQL (datasets, graph Update ops) and Cypher `USE GRAPH` | #3 |
| 6 | Graph-level ACL inside scans (always enforced) | #4 |
| 7 | `Subscribe` change feed (`chg` CF) | #5–#7 |
| 8 | `ApplyChanges` (atomic changesets) + SHACL `ValidateShapes` | — |
| 8.5 | Cypher over RDF (labels as `rdf:type`, runtime vocabulary; Cypher writes deprecated) | — |
| 8.6 | Value bindings (literal-valued query variables; SPARQL values, FILTER, ORDER BY) | — |
| 9 | Queryable inference (inferred graphs), DRed `--inference`, int8 vectors, counters (`sts` CF) | #15 |
| 10 | Atomic SPARQL Update (`?dry_run`, `?read_ts`), inference schema graphs, vector `graphs` filter, Cypher reads confined to the dataset, time-travel ACL fix | #16 |
| 11 | cb-bench: engine benchmark vs the plan targets, CI report job, results in `BENCHMARKS.md` Part 4 | #18 |
| 11b #1 | DRed batches look up only the facts they touch: team-scale inference lag p95 13.0 s → 0.97 s | #19 |
| 11b #3 | mmap / int8 vector files grow geometrically, flushed once per batch: 200K int8 build 166 → 72 s; 2M int8 now builds (24 min) | #20 |
| 11a | HNSW connectivity (heuristic neighbour selection; recall@20 on clustered data 0.30 → 1.00), memory-mode vectors stored once, cached norms + vectorized dot product | #17 |

Main CI is green on the #20 merge (`ac5d594`), including the cb-bench report job.

## In progress

- **Finding #2 (hybrid search) with #5 (default `ef`) folded in** — Mark's
  pick (2026-10-07). Design note first (`docs/design/hybrid-search.md`),
  decisions before code.

## cb-bench results (Apple M4 Pro, 48 GiB, NVMe)

The plan's targets are for company scale (`--scale 20`); measured so far:

| Plan target | Scale 0.05 | Scale 1 (team) |
|---|---|---|
| Load one graph p50 ≤ 2 ms | ✅ 0.87 | ✅ 0.76 |
| describe p95 ≤ 10 ms | ✅ 2.3 | ❌ 15 |
| Hybrid search p95 ≤ 25 ms | ❌ 256 | ❌ 2,681 |
| Context assembly p95 ≤ 300 ms | ✅ 260 | ❌ 2,253 |
| Promote 500 quads p95 ≤ 200 ms | ✅ 58 | ❌ 450 |
| Inference lag ≤ 2 s | ✅ 1.3 s | ❌ 12.3 s → ✅ 0.97 s after #19 |
| Ingestion ≥ 50 records/s | ✅ 101 | ❌ 27 |

## cb-bench findings

Numbers are from `BENCHMARKS.md` Part 4 (scale 1 = 8.0M base + 4.1M
inferred quads, 2M 384-dim chunk vectors).

1. **Inference lag grows with the store** — ✅ **fixed** (#19): team-scale
   p95 13.0 s → 0.97 s (re-measured on main with #19 + #20: 0.97 s).
   - Cause: each DRed batch (`owl_rl::infer_changes`) built a map of every
     live inferred quad (`live_inferred`, `crates/polargraph-storage/src/owl_rl.rs:731`)
     to check over-deletes and find quads to close.
   - Evidence: lag p95 1.3 s at scale 0.05 → 12.3 s at scale 1 (4.1M
     inferred quads), at 20 commits/s.
   - Approach: memoized point lookups of only the facts a batch touches;
     live count kept in META.
2. **Hybrid search over a mention set doesn't scale** — *next (with #5);
   design note in progress.*
   - Cause: hot entities' mention sets are unbounded (up to 86K chunks at
     team scale; Zipf 1.1); building the set (mention → chunk query) is up
     to 323 ms p95; `SearchVectorInSet`
     (`crates/polargraph-server/src/service.rs:2926`) then checks graph
     visibility node by node (`node_visible_in`, `service.rs:1010`, one
     subject scan per node) before exact scoring.
   - Evidence: p95 256 ms at scale 0.05, 2,681 ms at scale 1 (target
     25 ms); context assembly misses for the same reason (2,253 ms). The
     ANN + join alternative is 7 ms but keeps a median of one result.
   - Approaches to weigh: skip the per-node check when the set came from
     an ACL-scoped query; filtered ANN (graph search restricted to an
     allowed set or bitmap); bounded / pre-ranked mention sets (recency,
     importance) per the plan's "never materialize the full fan-out".
3. **int8 / mmap bulk load is quadratic** — ✅ **fixed** (#20): 200K int8
   vectors 166 → 72 s, ingestion 36 → ~105 records/s; 2M int8 vectors build
   in 24 min (recall@20 0.69 at ef 100, 0.92 at ef 400; ingestion 42/s).
   - Cause: `MmapState::append` (`crates/polargraph-storage/src/hnsw.rs:218`)
     resizes the `.vecs` file, remaps it and flushes the whole mapping on
     every vector.
   - Evidence: 100K vectors int8 64 s (≈ f32 60 s); 2M vectors: f32 37 min,
     int8 unfinished after 80 min (~1.5M appended).
   - Approach: grow the file geometrically, remap only on growth, flush
     once per batch (or on close); count in the header kept consistent.
4. **Promotion slows as the approved graph grows** — *needs profiling.*
   - Cause unknown: `ValidateShapes` overlay (`service.rs:2145`) vs
     `ApplyChanges` with `read_ts` (`service.rs:1938`).
   - Evidence: p95 58 ms at scale 0.05 → 450 ms at scale 1 for the same
     500-quad proposal (target 200 ms).
   - Approach: time the two halves in cb-bench; check whether SHACL target
     resolution (`sh:targetClass` with subclasses) scans the whole class.
5. **Default `ef` too low at scale** — *next, folded into #2's design note.*
   - Cause: server default `ef` 50 (`service.rs:273`, `--default-vector-ef`).
   - Evidence: recall@20 at 2M clustered vectors 0.64 at ef 100, 0.96 at
     ef 400 (1.0 at 100K for both).
   - Approach: raise the default (e.g. 200), or scale it with space size;
     measure latency vs recall in cb-bench.
6. **Ingestion at 2M vectors** — *needs measurement of options.*
   - Cause: HNSW inserts into a 2M-vector graph dominate (~36 ms per
     record of 20 chunks, single writer; `ef_construction` 200,
     `hnsw.rs:67`).
   - Evidence: 101 records/s at scale 0.05 → 27/s at scale 1 (target 50).
   - Approach: parallel batch insert (search phase under a read lock),
     a lower `ef_construction` for online inserts, or int8 for chunk
     spaces (cheaper distances; depends on #3).
7. **Company-scale run** (`--scale 20`, manual, NVMe) — after #1–#3 (and
   ideally #2) land; needs int8 for the vector budget.

**Open decision — rebuild old HNSW spaces.** Spaces built before the
connectivity fix (#17) keep their old links and only improve as vectors
are added. A rebuild operation (re-insert every vector into a fresh
index, swap) would restore full recall for existing data; not built —
decide whether it's needed (it matters only for stores with vectors
written before #17).

## Agreed boundaries (engine vs application)

- **Application-side**: proposals/review/promotion workflow (the engine
  gives `ApplyChanges` + `ValidateShapes`); type packages; which schemas
  and graphs a query reads (the engine gives `FROM` / `USE GRAPH` /
  `graphs` and the inference schema-graphs setting); the repo-as-a-graph
  extractor and architecture diagrams (`docs/design/repo-graph-app.md`);
  context assembly and ranking.
- **Cypher is read-only** going forward: `CypherWrite` / `POST /cypher/write`
  are deprecated (warning header, metric); writes go through `ApplyChanges`
  (`POST /changes`) or SPARQL Update.

## Open follow-ups (not scheduled)

- **F1 verified identity** — JWT from an IdP (issuer, audience, JWKS,
  expiry) instead of trusting a bare `user_id` / `x-polargraph-user-id`.
- **Remove Cypher writes** after the deprecation release.
- **PQ / vector tiering** — decide from cb-bench numbers (HNSW RAM at team
  scale is 4.0 GB in memory mode, as the plan estimated).
- **Inference full-recompute memory** — `materialize()` holds the whole
  closure in memory; a schema change or pruned change log triggers it.
  Risk at company scale; measure with cb-bench, then bound it.
- **SDK convenience methods** — newer RPCs (inference settings, counters,
  vector `graphs`, `ApplyChanges` helpers) are only partly wrapped.
- **Node-level ACL cache** — the legacy `AccessCache` (per-user
  `HashSet<NodeId>`) won't scale; graph-level ACL is the primary model.
- **HNSW rebuild** — see "Open decision" above.

## Working rules

- Design note and Mark's decisions before code for anything non-trivial;
  check with Mark before big design decisions.
- One fresh branch per step off main (no stacking), small commits, push
  often; Mark merges via PR. Send the PR link only when green.
- Pre-PR checks: `cargo fmt --all -- --check`; `cargo clippy --workspace
  -- -D warnings` on Rust 1.96 (MSRV stays 1.78); `cargo test --workspace`;
  Docker e2e `tests/e2e/run.sh`.
- Keep the §0.2 tracker, `CLAUDE.md` and this file current; release notes
  for behaviour changes.
