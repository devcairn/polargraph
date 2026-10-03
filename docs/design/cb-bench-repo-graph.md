# cb-bench — this repo as a living knowledge graph — design note

Status: decisions A–K **approved as recommended** (Mark, 2026-10-03). §9
(architecture layer) added on Mark's request; its decisions L–S await
approval. Asked for by Mark, 2026-10-02: "make it based on this database
and repo, so it can stand as a living example … tie [new nodes and edges]
to commits."

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

## 9. Architecture layer — how the engine works, as a navigable graph

Mark, 2026-10-03: "store the current working state of how the database
works (query engine, n-quads, indexes, etc.) for a junior dev to come in
and understand and be able to dig into the codebase from a graph in
diagram form."

### 9.1 What it models

| Level | Nodes (`pgr:`) | Example |
|---|---|---|
| System | `System` | PolarGraph |
| Subsystem | `Subsystem` | Query languages, Datalog engine, Storage, MVCC / bitemporal, Change feed, Inference, Graph ACL, Vector index, gRPC server, REST gateway, SHACL |
| Crate | `Crate` (+ crate dependencies) | `polargraph-query` |
| Module | `Module` | `polargraph_query::datalog` |
| File / symbol | `File`, `Symbol` (fn / struct / enum / trait / impl method) | `datalog.rs`, `execute_query_full` |

Alongside the containment tree:

- **Data artifacts**: `ColumnFamily` (the 8 quad orders, `meta`, `hnsw`,
  `trig`, `epag` / `epog` / `peag`, `iri`, `blob`, `chg`, `sts`), `KeyLayout`
  with ordered `KeySegment`s (name, offset, width — e.g. `spog`:
  `s:16 p:4 o:16 g:4 tt:8`), and the IR types data passes through (SPARQL
  algebra, `VarPattern` / `Query` Datalog IR, `Pattern` + `IndexChoice`,
  `Solution`, proto messages).
- **Interfaces**: `Rpc` (from the proto), `RestEndpoint` (from the router),
  each with `pgr:handledBy` → the handler symbol and `pgr:exposedAs` (RPC →
  REST).
- **Flows**: `Flow` with ordered `FlowStep`s — *component / symbol*, *input
  artifact*, *output artifact*, *CFs read or written*. Canonical flows:
  life of a SPARQL query, life of a Cypher query, life of a Datalog `Query`,
  life of a write (`Insert` / `ApplyChanges` → `Transaction` →
  `stage_writes` → one `WriteBatch` over 8 orders + `chg` + `trig` + `blob`
  → commit → change feed → type index / inference task), MVCC read, vector
  search, inference (full and DRed).
- **Concepts**: `Concept` glossary nodes (quad, value hash, bitemporal
  range, skolem IRI, graph ACL bitmap, inferred graph, …) linked to where
  they're implemented.
- **Edges**: `partOf` (symbol → … → system), `uses` (subsystem / crate
  dependencies), `readsFrom` / `writesTo` (component → CF), `handledBy`,
  `exposedAs`, `implements` (symbol → concept / RPC), **`shapedBy`**
  (component, symbol, CF → `Decision`), `documentedIn` (→ design-note
  section), `sourceAt` → a `SourceLocation` (path, start / end line, commit,
  GitHub permalink `…/blob/<sha>/<path>#L<a>-L<b>`).

### 9.2 Extracted vs curated

| Automatic (every commit) | Source |
|---|---|
| Crates, versions, crate dependencies | `cargo metadata` |
| Module tree, files, public symbols with line spans, module docs (`//!`) as descriptions | `syn` over each crate (span locations) |
| RPCs, messages, request / response types | the `.proto` |
| REST endpoints → handler functions | the `axum` router (`.route(…)`) via `syn` |
| RPC → handler (`impl PolarGraphService` methods) | `syn` |
| Column families and their doc comments; key widths | `cf.rs`, `keys.rs` constants |
| Commits / PRs touching each file / symbol | git (line ranges vs diffs) |

| Curated (checked in, reviewed) | File |
|---|---|
| Subsystems and which modules belong to each | `docs/architecture/model.yaml` |
| Flows (ordered steps referencing symbols, artifacts, CFs) | `docs/architecture/flows/*.yaml` |
| Key layouts' segment meanings (offsets come from code) | `model.yaml` |
| Concepts | `model.yaml` |
| `shapedBy` links not already implied by trailers | `model.yaml` |

**Keeping curated parts honest**: the extractor resolves every reference
(module path, symbol path such as
`polargraph_query::datalog::execute_query_full`, file, RPC, CF, decision
id) against the automatic index and **fails CI on a dangling reference**.
It also reports (warning, then failure once clean) modules not assigned to
a subsystem and RPCs / CFs not appearing in any flow. Renaming a function
therefore forces the flow that names it to be updated in the same PR.

### 9.3 Diagrams

Each canonical view is **a graph query plus a template**, rendered to
**Mermaid** now (our docs use it) and **D2** later (WS5):

| View | Query → diagram |
|---|---|
| System overview | subsystems + `uses` → flowchart, subsystem boxes link to their pages |
| Subsystem page | modules / key symbols + CFs read / written → flowchart |
| Life of a SPARQL / Cypher / Datalog query | `Flow` steps in order → sequence diagram (participants = components; messages = artifacts) |
| Life of a write | the write `Flow` + CFs → flowchart |
| Key layouts | `KeyLayout` segments → Mermaid `packet` diagrams |
| Column families | CF ↔ components (`readsFrom` / `writesTo`) → flowchart |

- Every node carries a `click` link to its GitHub permalink at the commit,
  so a diagram is a map you drill from into code.
- Output: `docs/architecture/generated/*.md` — **committed**, so GitHub
  renders it; CI regenerates and **fails if the committed output is
  stale** (no bot commits). `polargraph-bench repo-graph render [view]`
  regenerates locally.

### 9.4 Versioning

- Each commit graph records what the commit **changed** in the
  architecture (symbols / RPCs / CFs introduced, removed, moved — and
  curated-model edits).
- The `current` graph holds the architecture **bitemporally**: a fact opens
  at the commit time it appeared and closes when it went away. So
  "how did the engine look at commit X" is `as_of_valid_time` = X's time,
  and "what changed between X and Y" is `DiffGraphs` on `current` at the two
  times (type-packages P4) — architecture diffs dogfood time travel.

### 9.5 Onboarding query

"Show me everything involved in how the engine handles Cypher queries":

```sparql
PREFIX pgr: <https://polargraph.dev/ns/repo#>
SELECT ?code ?flow ?rpc ?decision ?change WHERE {
  {   ?code pgr:partOf+ <https://polargraph.dev/repo/subsystem/cypher> }
  UNION { ?flow pgr:about <https://polargraph.dev/repo/subsystem/cypher> }
  UNION { ?rpc pgr:handledBy/pgr:partOf+ <https://polargraph.dev/repo/subsystem/cypher> }
  UNION { <https://polargraph.dev/repo/subsystem/cypher> pgr:shapedBy ?decision }
  UNION { <https://polargraph.dev/repo/subsystem/cypher> pgr:shapedBy ?d .
          ?change pgr:implements ?d }
}
```

(Each node then carries `rdfs:label` and `pgr:sourceAt` → permalink. The
`repo-graph explain <term>` command wraps this. Finding while designing:
SPARQL `BIND` is currently **ignored silently** by the translator, so the
natural `BIND("code" AS ?kind)` form returns nothing useful — PR 1 adds
`BIND` of constants / variables and makes unsupported expressions an
error rather than a no-op.)

→ components (`polargraph_query::cypher` parser, compiler,
`compile_with_vocabulary`), the "life of a Cypher query" flow, the
`CypherQuery` / `CypherQueryStream` / deprecated `CypherWrite` RPCs and
`/cypher*` endpoints, decisions (cypher-rdf A–F, G2; value-bindings
E), and the PRs / commits that implemented them — each with a link into
the code. The same question as text or vector search returns **instances**
(decisions, symbols, flows), not the `pgr:Decision` class or its shape:
the ontology and shapes are loaded as **schema graphs** (type-packages
note, graph roles), which data queries and search leave out.

### 9.6 Decisions (architecture layer)

| # | Question | Recommendation |
|---|----------|----------------|
| L | Granularity | System → subsystem → crate → module → file → **public** symbols (private items only when a flow names them). |
| M | Symbol extraction | `syn` with span locations (exact line ranges, no rust-analyzer dependency). |
| N | Curated model format | YAML under `docs/architecture/` (`model.yaml`, `flows/*.yaml`), compiled to RDF by the extractor; references by Rust path / file / RPC / CF / decision id. |
| O | Honesty checks | Dangling references **fail CI**; unassigned modules and RPCs / CFs missing from flows warn first, then fail. |
| P | Diagrams | Generated from graph queries; **Mermaid** now (sequence, flowchart, `packet` for key layouts), D2 via WS5 later; click-through to GitHub permalinks. |
| Q | Generated docs | Committed under `docs/architecture/generated/`; CI fails if stale (no bot commits). |
| R | Versioning | Architecture changes in commit graphs; `current` holds it bitemporally (valid time = commit time); diffs via `DiffGraphs` with `as_of`. |
| S | Delivery | Part of cb-bench PR 1 (with the repo graph); initial curated model: the subsystems and the six canonical flows above. |
