# cb-bench — this repo as a living knowledge graph — design note

Status: **proposal** (decisions in §8). Asked for by Mark, 2026-10-02:
"make it based on this database and repo, so it can stand as a living
example … tie [new nodes and edges] to commits."

## 1. Goals

1. **A real, growing dataset** — the repo's own history (commits, PRs, design
   notes and their decisions, plan steps, crates, RPCs, column families,
   findings) as a knowledge graph with provenance, extended automatically by
   each new commit.
2. **A readable demo** — a newcomer runs a few SPARQL / Cypher / SHACL /
   inference / `Subscribe` / time-travel queries over a graph they already
   understand (this project) and sees how the engine works.
3. **The plan's benchmark** — the repo graph is the *seed and fixture*; a
   synthetic generator scales its shape up to the plan's team and company
   scenarios, so the performance targets (plan § Capacity) get numbers.
4. **Golden questions** for the WS8 evaluation harness ("why were Cypher
   writes deprecated, who approved it, which PR did it?").

Honest about scale: the real graph is small — ~206 commits, 17 PRs, 7
design notes (~45 lettered decisions), ~200 tracked files in 9 crates, ~20
plan steps → roughly **5–15K quads**. Fine for the demo and correctness; it measures nothing at
company scale. §6 covers the scale-up.

## 2. Model

### 2.1 Vocabulary

Reuse the plan's vocabularies where they fit (`prov:`, `dcterms:`, `cb:`
graph kinds / supersession, `rec:` records, `work:` plan items), plus a
small `pgr:` vocabulary (`https://polargraph.dev/ns/repo#`) for code
artifacts. Shipped as `bench/repo-graph/ontology.ttl` (classes, properties,
domains / ranges, inverses — exercised by inference) and `shapes.ttl` (SHACL
— exercised by validation).

| Class | Instances | Key IRIs (deterministic) |
|---|---|---|
| `pgr:Commit` (⊑ `prov:Activity`) | each commit | `…/repo/commit/<sha>` |
| `pgr:PullRequest` (⊑ `prov:Activity`) | each merged PR | `…/repo/pr/<n>` |
| `prov:Person`, `prov:SoftwareAgent` | authors, co-authors (Claude) | `…/repo/agent/<github-login or name-slug>` (no emails) |
| `pgr:Crate`, `pgr:Module`, `pgr:File` | code layout | `…/repo/crate/<name>`, `…/repo/file/<path>` |
| `pgr:DesignNote` (⊑ `rec:Document`) | `docs/design/*.md` | `…/repo/note/<slug>` |
| `pgr:Decision` | lettered decisions (step 6 A–F, …) | `…/repo/decision/<note-slug>/<id>` |
| `work:PlanStep`, `work:Workstream` | §0.2 rows, WS1–WS10 | `…/repo/step/<id>`, `…/repo/ws/<n>` |
| `pgr:ColumnFamily`, `pgr:Rpc`, `pgr:RestEndpoint` | `cf.rs`, the proto, REST routes | `…/repo/cf/<name>`, `…/repo/rpc/<Name>` |
| `pgr:OpenQuestion` | open questions in notes / plan | `…/repo/question/<note>/<id>` |
| `pgr:Finding` | bugs found and fixed ("finding while building") | `…/repo/finding/<slug>` |

| Property | From → to |
|---|---|
| `pgr:decidedIn` | Decision → DesignNote |
| `pgr:approvedBy` / `pgr:approvedOn` | Decision → Person / date |
| `pgr:implementedBy` (inverse `pgr:implements`) | Decision, PlanStep → Commit, PullRequest |
| `cb:supersedes` | Decision → Decision (e.g. cypher-rdf G2 → E) |
| `pgr:dependsOn` (transitive) | PlanStep → PlanStep; Crate → Crate |
| `pgr:partOf` (transitive) | File → Module → Crate; Commit → PullRequest; PlanStep → Workstream |
| `pgr:touches` | Commit → File |
| `pgr:introduces` / `pgr:removes` | Commit → Rpc, ColumnFamily, RestEndpoint |
| `pgr:fixes` / `pgr:foundIn` | Commit → Finding / Finding → PullRequest |
| `prov:wasAttributedTo`, `prov:generatedAtTime` | Commit, PR, graph → agent / time |

### 2.2 Graphs and provenance

- **One graph per commit** — `…/repo/graph/commit/<sha>` — holding exactly
  the facts extracted from that commit, with graph metadata
  `prov:wasDerivedFrom <commit>`, `prov:wasAttributedTo <author>`,
  `prov:generatedAtTime`, `cb:graphKind cb:RecordGraph`. Immutable history:
  "what did this commit say?".
- **One graph per PR** — PR-level facts (title, merge, decisions approved in
  it).
- **`…/repo/graph/current`** (`cb:ApprovedGraph`) — the current state:
  decision statuses, live RPCs / CFs, step status. When a PR merges, the
  extractor promotes its commits' facts into `current` with **one
  `ApplyChanges` (with `read_ts`) after `ValidateShapes` with the changes as
  an overlay** — the step-8 promotion flow, dogfooded.
- **Valid time = commit time**, so `as_of_valid_time` answers "which
  decisions stood on 2026-10-01?"; transaction time is load time.

## 3. Capturing decisions

Most design notes use a `| # | Question | Recommendation |` table with
letter ids (the step 9 note has its decisions only in prose); approval
lives in prose ("approved as recommended", "A changed"). Prose is not
reliably parseable, so:

- **Front matter** on each design note — authoritative for status:

  ```yaml
  ---
  id: step9-inference-vectors-stats
  step: "9"
  status: built            # proposal | approved | building | built | merged
  decisions:
    - id: A
      title: Delivery
      status: changed      # proposed | approved | changed | superseded | rejected
      decided_by: mark
      decided_on: 2026-10-02
      note: one PR instead of four
    - id: B
      status: approved
      decided_by: mark
      decided_on: 2026-10-02
      supersedes: []      # e.g. ["cypher-rdf/E"]
  ---
  ```

  The table stays for humans; the extractor reads titles from it and
  statuses from front matter, and fails CI if they disagree (unknown id,
  missing status).
- **Commit trailers**, going forward (git `interpret-trailers` format):
  `Plan-Step: 9a`, `Implements: step9-inference-vectors-stats/B`,
  `Fixes: finding/owl-rl-hash-byte-order`. Cheap to add to the existing
  commit template.
- **Backfill** for the 206 historic commits: a checked-in
  `bench/repo-graph/backfill.toml` (commit / PR → step, decisions,
  findings), drafted from branch names, PR titles and commit prefixes, then
  reviewed by hand once.

## 4. Extraction, incremental runs, idempotency

`polargraph-bench repo-graph` (Rust, in the existing bench crate; reads git
via the `git` CLI, design notes with a YAML + table parser, `proto` and
`cf.rs` for RPCs / CFs):

- `extract [--since <sha>]` → N-Quads, one graph per commit / PR, plus the
  `current` promotion changesets.
- `load --url <rest>` → creates graphs, imports, promotes; records the
  watermark `pgr:lastExtractedCommit` in `…/repo/graph/meta`.
- **Idempotent**: IRIs and edge ids are deterministic (SHA / path /
  note-slug / letter), so re-extracting a commit yields identical quads;
  `load` skips commits whose graph already exists with the same content
  digest (graph metadata `pgr:extractorVersion`, `pgr:digest`), and
  re-extracts all when the extractor version changes.
- **History rewrites**: commit graphs whose SHA is no longer reachable from
  `main` are dropped (`DropGraph`, bitemporal).
- **The dataset is not committed** — it is a pure function of git history +
  front matter + backfill, so committing it would only create a loop. CI
  regenerates it.
- **CI job `repo-graph`** (on push to `main`): extract → load into a fresh
  `polargraphd` → SHACL-validate → run the demo queries and the small bench
  → upload the N-Quads and a report as artifacts. An optional long-running
  demo instance gets incremental loads from the watermark.

## 5. Demo queries (`bench/repo-graph/queries/`, runnable as a script)

| Engine feature | Query |
|---|---|
| SPARQL + provenance | Decisions Mark approved, the PR that implemented each, and the commit graph each fact came from (`GRAPH ?g { … } ?g prov:wasDerivedFrom ?commit`) |
| Cypher | `MATCH (d:Decision)-[:implementedBy]->(c:Commit)-[:partOf]->(p:PullRequest) RETURN d.title, p.title` |
| SHACL | "every approved decision has an implementation" and "every RPC is introduced by a commit" — surfaces gaps (e.g. 8c `DiffGraphs` never built) |
| Inference | `partOf` transitive + `implements` inverse: "everything in `polargraph-storage` that step 9 touched" without writing the join |
| Value bindings / aggregates | Commits per crate per month, `ORDER BY` — the touched-file histogram |
| Time travel | Decision statuses `as_of_valid_time` 2026-10-01 vs now (G2 superseding E) |
| `Subscribe` | Tail `current` while the extractor promotes a new PR |
| Vector search | Design-note sections embedded (real model via `--embed-cmd`, else a deterministic hash embedding) → "notes most similar to this question" |

## 6. Scale-up to the plan's targets

The real graph is the **seed**. `polargraph-bench cb --scale <SF>` generates
on top of it, cloning its shape and the plan's company model:

- **Spine entities** (people, teams, services, customers) — counts from the
  plan scenarios; org structure as in WS4 seed packages.
- **Records with chunks** (WS7 ladder: envelope ~20 quads, 20 chunks × 3
  quads, text in `blob`, one 384-dim vector per chunk), **mentions** to
  entities with **Zipf skew (s ≈ 1.1)** so a few entities have 50K mentions.
- **Graphs and ACL**: record graphs per team, approved graphs per domain,
  proposals, grants per group (WS2.8) — so every read runs with a real user
  bitmap.
- **Repo-shaped projects**: N synthetic "projects" each a renamed clone of
  the repo graph (decisions, commits, PRs) with Zipf-sized histories — keeps
  the demo vocabulary exercised at scale.
- **Vectors**: synthetic clustered unit vectors (so recall is measurable),
  int8 and f32 spaces.
- Loaded through **SST import** (WS2.7) for the base, then the live paths
  for the measured operations.

`SF = 1` ≈ team (~10M quads, 2M chunk vectors); `SF = 20` ≈ company
(~195M quads, 40M vectors).

| Plan target | How cb-bench measures it |
|---|---|
| Load one graph (1K quads) p50 ≤ 2 ms | `ExportGraph` / GSPO scan of record graphs and repo commit graphs |
| `describe(entity)` p95 ≤ 10 ms | All facts of a Zipf-sampled entity across a user's visible graphs (hot entities dominate the tail) |
| Hybrid search k=20 p95 ≤ 25 ms | `VectorSeedQuery` / `SearchVectorInSet` over an entity's mention set, f32 vs int8 |
| Context assembly 4K tokens p95 ≤ 300 ms | Proxy: describe + hybrid search + chunk text fetch (the real assembly is WS8, app-side) |
| Promote 500-quad proposal incl. SHACL p95 ≤ 200 ms | `ValidateShapes` overlay + `ApplyChanges` with `read_ts` — same flow as the repo PR promotion |
| Incremental materialization lag ≤ 2 s | `--inference` under sustained writes; `polargraph_inference_lag_seconds` |
| Sustained ingestion ≥ 50 records/s | One `ApplyChanges` + vector batch per record, model latency excluded |
| (capacity) disk / RAM | Bytes per quad, HNSW RAM f32 vs int8 — checks the plan's estimates |

CI runs a small scale (`SF = 0.05`) as a **regression report** (no hard
thresholds on shared runners); the company run is manual on NVMe, with
results in `BENCHMARKS.md` and the tracker.

## 7. Layout

```
bench/repo-graph/
  ontology.ttl  shapes.ttl  backfill.toml
  queries/      *.rq *.cypher demo.sh
crates/polargraph-bench/src/
  repo_graph/   extract.rs  notes.rs  load.rs
  cb/           generate.rs  measure.rs
```

## 8. Decisions needed

| # | Question | Recommendation |
|---|----------|----------------|
| A | Where decision status lives | **YAML front matter** on design notes (authoritative), tables kept for humans; CI checks they agree. Backfill the 7 existing notes once. |
| B | Linking commits to steps / decisions | **Git trailers** going forward (`Plan-Step`, `Implements`, `Fixes`) + a reviewed **`backfill.toml`** for history. |
| C | Graph granularity | **Graph per commit and per PR** (immutable, with `prov:wasDerivedFrom` the SHA and the author) + a **`current`** approved graph promoted on merge via `ValidateShapes` + `ApplyChanges` (dogfoods step 8). |
| D | Vocabulary | Plan vocabularies (`prov`, `dcterms`, `cb`, `rec`, `work`) + a small `pgr:` namespace for code artifacts; ontology and shapes checked in. |
| E | Extractor | **Rust**, a `repo-graph` subcommand of `polargraph-bench`; git via the CLI. |
| F | Is the dataset committed? | **No** — regenerated deterministically from git; CI uploads it as an artifact; incremental loads into a demo instance by watermark. |
| G | Scale-up | Repo graph as seed + synthetic generator (spine entities, records / chunks / mentions with Zipf skew, ACL graphs, cloned repo-shaped projects, clustered vectors), `--scale`; SST bulk load. |
| H | CI vs manual | CI: extract + validate + demo queries + `SF = 0.05` regression report (no hard thresholds). Company scale: manual, recorded in `BENCHMARKS.md`. |
| I | People | Agents by GitHub login / name slug, **no email addresses** in the dataset; Claude co-authors as `prov:SoftwareAgent`. |
| J | Embeddings for the demo | Deterministic hash embedding by default; `--embed-cmd` to plug in a real model. |
| K | Delivery | Two PRs: (1) conventions (front matter + backfill + trailers), ontology / shapes, extractor, loader, demo queries, CI job; (2) scale-up generator and measurements. |
