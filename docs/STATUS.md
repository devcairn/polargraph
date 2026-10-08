# Engine status — handoff

Snapshot: 2026-10-08. Detailed history: §0.2 tracker in
`docs/contxtbroker-platform-plan.md`. Per-feature notes: `CLAUDE.md`
("Current state"), `docs/design/*`, release notes `docs/upgrade-*.md`.

## Where we are

**The engine phase is complete, pending the merge of `db/hybrid-search`.**
Everything the platform plan asked of the engine is built and measured;
what's left on the engine side is the "Later / nice-to-have" list below,
none of it blocking.

**Next work is application-side** — the ContxtBroker service on top of the
engine's public APIs:

- **MCP / context assembly** (plan WS8): retrieval over hybrid search
  (`SearchVectorInSet` with `candidate_patterns`), `describe`, chunk text;
  ranking and candidate caps are application policy.
- **Promotion / review** (WS3): proposals, review, promotion policy and
  provenance, using `ApplyChanges` (atomic, `read_ts`) and `ValidateShapes`.
- **Ingestion** (WS7): records, chunks and embeddings through
  `ApplyChanges` + `BatchInsertVectors` (or SST import for backfills).
- **Repo-graph tool**: specified in `docs/design/repo-graph-app.md`
  (extractor, architecture model, diagrams) — not built in the engine.

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
| 11a | HNSW connectivity (heuristic neighbour selection; recall@20 on clustered data 0.30 → 1.00), memory-mode vectors stored once, cached norms + vectorized dot product | #17 |
| 11b #1 | DRed batches look up only the facts they touch: team-scale inference lag p95 13.0 s → 0.97 s | #19 |
| 11b #3 | mmap / int8 vector files grow geometrically, flushed once per batch: 200K int8 build 166 → 72 s; 2M int8 now builds (24 min) | #20 |
| 11c | **In review** (`db/hybrid-search`): hybrid search (vector visibility index, `candidate_patterns`, int8 in-set ranking) + default `ef` 400 | — |

## Performance against the plan's targets

cb-bench on an Apple M4 Pro (14 cores, 48 GiB, NVMe), in-process, reading as
a team member with the graph ACL. The plan's targets are for **company**
scale (`--scale 20`, not yet run); team scale is `--scale 1` (8.0M base +
4.1M inferred quads, 2M 384-dim chunk vectors). Current = main plus
`db/hybrid-search`.

| Plan target | Scale 0.05 | Team scale, current |
|---|---|---|
| Load one graph p50 ≤ 2 ms | ✅ 0.87 ms | ✅ 0.97 ms |
| `describe(entity)` p95 ≤ 10 ms | ✅ 2.3 ms | ❌ 16 ms |
| Hybrid search k=20 p95 ≤ 25 ms | ❌ 256 ms (before hybrid search; not re-run) | ❌ 162 ms (f32) / 140 ms (int8); ✅ 17 / 13 ms for mention sets < 10K chunks |
| Context assembly p95 ≤ 300 ms | ✅ 260 ms | ✅ 173 ms |
| Promote 500 quads incl. SHACL p95 ≤ 200 ms | ✅ 58 ms | ❌ 452 ms |
| Incremental materialization lag ≤ 2 s | ✅ 1.3 s | ✅ 0.97 s |
| Sustained ingestion ≥ 50 records/s | ✅ 101/s | ❌ 27/s (f32 space), 42/s (int8) |

Recall@20 at the default `ef` 400: 0.96 (f32) / 0.94 (int8) at 2M
vectors. Disk at team scale: 11 GB with one vector space (plan estimate
~9 GB); HNSW RAM 2.5–4 GB in memory mode (estimate ~4 GB).

## Agreed boundaries (engine vs application)

- **Application-side**: proposals / review / promotion workflow (the engine
  gives `ApplyChanges` + `ValidateShapes`); type packages; which schemas
  and graphs a query reads (the engine gives `FROM` / `USE GRAPH` /
  `graphs` and the inference schema-graphs setting); retrieval policy,
  candidate caps, context assembly and ranking; the repo-as-a-graph tool
  (`docs/design/repo-graph-app.md`).
- **Cypher is read-only** going forward: `CypherWrite` / `POST /cypher/write`
  are deprecated (warning header, metric); writes go through `ApplyChanges`
  (`POST /changes`) or SPARQL Update.

## Trust model

**The engine trusts the user id the calling application sends** (`user_id`
field, `x-polargraph-user-id` gRPC metadata, REST `X-User-Id`) and enforces
graph-level access control for it. **The application is responsible for
authentication**: it must forward only ids it has verified. The API key
(`--api-key`) authenticates the application itself; a call without a user
id is a trusted service call with full access. Decided by Mark, 2026-10-08
("for now we trust what the app sends"); verified identity is the first
item below.

## Later / nice-to-have

Not scheduled; none blocks the application work. Numbers are team scale
unless noted.

1. **F1 — verified identity (JWT).** Accept signed identity tokens from an
   IdP and derive the caller's user id / groups from verified claims, instead
   of trusting `user_id`. Approach: a tower layer validating issuer,
   audience, expiry and signature against a JWKS (key rotation, cached
   keys); a claim → principal mapping; `user_id` accepted only from
   service callers. Until then: trust model above.
2. **D — cached candidate sets for popular entities** (hybrid search
   follow-up; measured as needed). Hybrid search p95 is 17 / 13 ms
   (f32 / int8) for mention sets < 10K chunks but 173 / 143 ms for sets
   ≥ 10K (96 of 200 Zipf-sampled entities): evaluating the mention → chunk
   join (~150 ms for 86K chunks) dominates. Sets grow ~20× at company scale.
   Approach (`docs/design/hybrid-search.md` B3): cache candidate sets keyed
   by (patterns with bound values, dataset, readable graphs), invalidated
   via the change log by the patterns' predicates; expected ~10–20 ms p95
   for cached entities. Alternative / complement: an application cap on
   candidates (most recent N records).
3. **#4 — promotion latency grows with the approved graph.** p95 58 ms at
   scale 0.05 → 452 ms at team scale for the same 500-quad proposal (target
   200 ms). Cause not profiled: `ValidateShapes` overlay
   (`crates/polargraph-server/src/service.rs`, `validate_shapes`) vs
   `ApplyChanges` with `read_ts` (`apply_changes`). Approach: time the two
   halves in cb-bench; check whether SHACL `sh:targetClass` resolution
   (with subclasses) scans the whole class instead of the touched nodes.
4. **#6 — ingestion at 2M vectors.** 101 records/s at scale 0.05 → 27/s at
   team scale with an f32 space (42/s int8; target 50): HNSW inserts
   dominate (~36 ms per record of 20 chunks, single writer;
   `ef_construction` 200, `crates/polargraph-storage/src/hnsw.rs`).
   Approach: parallel batch insert (search phase under a read lock, link
   phase serialized), a lower `ef_construction` for online inserts, or
   int8 for chunk spaces.
5. **`describe(entity)` tail.** p95 16 ms vs 10 ms at team scale; popular
   entities return up to 4,454 facts (incoming mentions). Approach: page or
   cap incoming facts per predicate (with counts) in the describe shape the
   application uses, or count-only for high-fan-in predicates.
6. **Company-scale run** (`--scale 20`, manual, NVMe; ~195M quads, 40M
   vectors). Needs int8 for the vector budget and, ideally, D. Approach:
   `polargraph-bench cb --scale 20 --vectors int8` on a large NVMe host;
   record in `BENCHMARKS.md`.
7. **Rebuild old HNSW spaces.** Spaces built before the connectivity fix
   (#17) keep their old links (recall@20 0.17–0.30 on clustered data) and
   only improve as vectors are added. Approach: a `RebuildVectorSpace`
   operation that re-inserts every vector into a fresh index and swaps it
   in. Only matters for stores with vectors written before #17.
8. **Remove Cypher writes** after the deprecation release
   (`docs/upgrade-cypher-rdf.md`): delete `CypherWrite`, `POST /cypher/write`
   and the SDK write methods.
9. **SDK convenience methods.** Newer RPCs (`ApplyChanges` helpers,
   candidate patterns, inference settings, counters, vector `graphs`) are
   reachable through the generated stubs but only partly wrapped in the
   Python / Go / TS clients. Approach: wrap them alongside the first
   application that uses them.
10. **Inference full-recompute memory.** `owl_rl::materialize()` holds the
    whole closure in memory (team scale: 4.1M inferred facts, ~2 min). A
    schema change, a schema-graphs setting change or a pruned change log
    triggers it; memory at company scale (~80M inferred facts) is
    unmeasured. Approach: measure RSS in cb-bench; then compute per source
    graph or in chunks, streaming the diff.
11. **Node-level ACL cache.** The legacy `AccessCache` (per-user
    `HashSet<NodeId>`) won't scale to tens of millions of nodes; graph-level
    ACL is the primary model. Approach: deprecate node-level grants, or back
    them with per-graph bitmaps.
12. **PQ / vector tiering.** Team scale: 2M vectors use 2.9 GB (f32) or
    0.74 GB (int8) of vector RAM; company scale (40M) needs ~15 GB with
    int8 codes plus graph links. Approach: product quantization for
    another ~4×, and/or document-level vectors for old records (plan
    § Capacity), decided from a company-scale run.
13. **`DiffGraphs` / graph digest** (P4 / P5, dropped from the engine
    plan). Diffs can be computed from `ExportGraph`; a per-graph digest
    would help verify replicas and backups. Approach: revisit if replica
    verification is needed.
14. **The 5.1 s hybrid-search outlier.** One f32 sample (of 200) at team
    scale took 5.1 s (p99 386 ms). Hypothesis: it ran before the vector
    visibility index finished its background build, falling back to the
    per-node check. Approach: report index readiness and build time in
    cb-bench (and wait for it before measuring); add a metric for fallbacks.

## Working rules

- Design note and Mark's decisions before code for anything non-trivial;
  check with Mark before big design decisions.
- One fresh branch per step off main (no stacking), small commits, push
  often; Mark merges via PR. Send the PR link only when green.
- Pre-PR checks, **from a clean checkout of the pushed branch** (a fresh
  `git worktree` — untracked local files must not mask missing commits):
  `cargo fmt --all -- --check`; `cargo clippy --workspace -- -D warnings`
  on Rust 1.96 (MSRV stays 1.78); `cargo test --workspace --locked`;
  Docker e2e `tests/e2e/run.sh`.
- Keep the §0.2 tracker, `CLAUDE.md` and this file current; release notes
  for behaviour changes.
