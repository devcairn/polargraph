# Engine status — handoff

Snapshot: 2026-10-03. Detailed history: §0.2 tracker in
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

Main CI is green on the step 10 merge (`c3fa88f`).

## In progress

1. **`db/hnsw-connectivity`** — HNSW fix found by cb-bench (Mark chose
   option A: its own PR first). Tracker row 11a.
   - Heuristic neighbour selection. Plain nearest-M split clustered data
     into islands: recall@20 0.17 (f32) / 0.19 (int8) at 20K clustered
     vectors, unchanged by `ef`.
   - Memory-mode vectors stored once (`<space>/v/<id>`); node records hold
     only neighbour lists and are written once per batch. Before: 20K f32
     vectors grew RSS by 1.4 GB.
   - Legacy records load as is; no migration. Existing spaces keep their
     old links (improve as vectors are added); a rebuild operation would
     be a new decision.
   - State: code and tests done, storage tests green; before/after numbers
     being collected; then full pre-PR checks, PR link, wait for merge.
2. **`db/cb-bench`** — engine benchmark (tracker row 11): seed
   (`crates/polargraph-bench/seed/`), generator, SST loader, in-process
   measurements vs the plan targets, JSON/Markdown report. Pushed as WIP
   (no PR). Next: rebase onto main after the HNSW merge, add the CI job
   (`--scale 0.05`, regression report, no hard thresholds), record numbers
   in `BENCHMARKS.md`, then PR. Company scale (`--scale 20`) is a manual
   NVMe run.

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
- **PQ / vector tiering** — decide from cb-bench numbers.
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
