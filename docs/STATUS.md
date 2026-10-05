# Engine status — handoff

Snapshot: 2026-10-05. Detailed history: §0.2 tracker in
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
| 11a | HNSW connectivity (heuristic neighbour selection; recall@20 on clustered data 0.30 → 1.00), memory-mode vectors stored once, cached norms + vectorized dot product | #17 |

Main CI is green on the 11a merge (`85d96b4`).

## In progress

- **`db/cb-bench`** — engine benchmark (tracker row 11), rebased on main,
  **in review**: seed, generator, SST loader, in-process measurements vs
  the plan targets, report (targets met / missed / unmeasured, hardware,
  disk by CF), CI job (`--scale 0.05`, job summary, no thresholds),
  results in `BENCHMARKS.md` Part 4.

## cb-bench results (Apple M4 Pro, 48 GiB, NVMe)

The plan's targets are for company scale (`--scale 20`); measured so far:

| Plan target | Scale 0.05 | Scale 1 (team) |
|---|---|---|
| Load one graph p50 ≤ 2 ms | ✅ 0.87 | ✅ 0.76 |
| describe p95 ≤ 10 ms | ✅ 2.3 | ❌ 15 |
| Hybrid search p95 ≤ 25 ms | ❌ 256 | ❌ 2,681 |
| Context assembly p95 ≤ 300 ms | ✅ 260 | ❌ 2,253 |
| Promote 500 quads p95 ≤ 200 ms | ✅ 58 | ❌ 450 |
| Inference lag ≤ 2 s | ✅ 1.3 s | ❌ 12.3 s |
| Ingestion ≥ 50 records/s | ✅ 101 | ❌ 27 |

Engine work these point to (proposed order; needs Mark's prioritisation):

1. **DRed batch cost** — `infer_changes` scans every live inferred quad per
   batch (`live_inferred`); look up only the facts the batch touches.
2. **Mention-set hybrid search** — unbounded sets (86K chunks for hot
   entities) and a per-node visibility scan in `SearchVectorInSet`; needs a
   design (bounded / ranked mention sets, filtered ANN, or bitmap
   visibility).
3. **mmap / int8 bulk append** — quadratic (resize + remap + full flush per
   vector); grow in chunks. Blocks measuring int8 at scale.
4. **Promotion latency growth** (58 → 450 ms) — profile SHACL overlay vs
   `ApplyChanges`.
5. **Default `ef`** — recall@20 at 2M vectors is 0.64 at ef 100, 0.96 at
   ef 400; the server default is 50.
6. **Ingestion at 2M vectors** (27/s) — HNSW insert cost; parallel batch
   insert or a lower `ef_construction` for online inserts.
7. Then the company-scale run (`--scale 20`, manual NVMe).

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
- **HNSW rebuild** — an operation to rebuild a space built before the
  connectivity fix (needs a decision).

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
