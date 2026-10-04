# Repo graph — this repo as a living knowledge graph (application spec)

Status: **application-side** (Mark, 2026-10-03: "the application will be
responsible for doing all of that"). Lives in ContxtBroker or a separate
tool, **built only on PolarGraph's public APIs** (gRPC / REST); nothing
here is built in the engine repo. Decisions A–F, I (from the original
cb-bench note) and the architecture-layer design (L–S) carry over as the
app's design; the engine-side benchmark is `docs/design/cb-bench.md`.

## 1. What it is

The engine repo's own history — commits, PRs, design notes and their
lettered decisions, plan steps, crates, RPCs, column families, findings —
plus **how the engine works** (subsystems, flows, key layouts, code
locations) as a knowledge graph with per-commit provenance, grown by CI,
rendered as diagrams a newcomer can drill from into the code.

## 2. Engine APIs it uses

| Need | Engine API |
|---|---|
| Per-commit / per-PR graphs with provenance metadata | `CreateGraph` (metadata), `ApplyChanges`, `/import/rdf` (N-Quads) |
| Promote merged facts into `current` | `ValidateShapes` (overlay) + `ApplyChanges` with `read_ts` |
| Ontology / shapes | ordinary graphs; the app scopes queries to its data graphs with `FROM` / `graphs` / `USE GRAPH`, and names its shapes graphs in `ValidateShapes` |
| Inference over its ontology | OWL RL inference, with the app's ontology graph as the inference schema graph |
| Queries and views | SPARQL (property paths, `GRAPH`, `ORDER BY`, aggregates), Cypher, `as_of_valid_time` |
| Live updates | `Subscribe` |
| Search | vector search with a `graphs` filter |

Engine gaps this app surfaced: SPARQL `BIND` is ignored silently (needed by
the onboarding query's natural form); there is no graph diff RPC (the app
diffs two as-of query results itself).

## 3. Model

### 3.1 Vocabulary

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

### 3.2 Graphs and provenance

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

## 4. Capturing decisions

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

## 5. Extraction, incremental runs, idempotency

The `repo-graph` tool (Rust; reads git
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
- **A CI job in the engine repo** (on push to `main`, calling the tool): extract → load into a fresh
  `polargraphd` → SHACL-validate → run the demo queries and the small bench
  → upload the N-Quads and a report as artifacts. An optional long-running
  demo instance gets incremental loads from the watermark.

## 6. Demo queries

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

## 7. Architecture layer — how the engine works, as a navigable graph

Mark, 2026-10-03: "store the current working state of how the database
works (query engine, n-quads, indexes, etc.) for a junior dev to come in
and understand and be able to dig into the codebase from a graph in
diagram form."

### 7.1 What it models

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

### 7.2 Extracted vs curated

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

### 7.3 Diagrams

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
  stale** (no bot commits). `repo-graph render [view]`
  regenerates locally.

### 7.4 Versioning

- Each commit graph records what the commit **changed** in the
  architecture (symbols / RPCs / CFs introduced, removed, moved — and
  curated-model edits).
- The `current` graph holds the architecture **bitemporally**: a fact opens
  at the commit time it appeared and closes when it went away. So
  "how did the engine look at commit X" is `as_of_valid_time` = X's time,
  and "what changed between X and Y" diffs two queries of `current` with
  `as_of_valid_time` at the two times — architecture diffs dogfood time travel.

### 7.5 Onboarding query

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
the app keeps its ontology and shapes in their own graphs and scopes data
queries and search (`FROM` / `graphs` / the vector `graphs` filter) to its
data graphs.

### 7.6 Decisions (architecture layer)

| # | Question | Recommendation |
|---|----------|----------------|
| L | Granularity | System → subsystem → crate → module → file → **public** symbols (private items only when a flow names them). |
| M | Symbol extraction | `syn` with span locations (exact line ranges, no rust-analyzer dependency). |
| N | Curated model format | YAML under `docs/architecture/` (`model.yaml`, `flows/*.yaml`), compiled to RDF by the extractor; references by Rust path / file / RPC / CF / decision id. |
| O | Honesty checks | Dangling references **fail CI**; unassigned modules and RPCs / CFs missing from flows warn first, then fail. |
| P | Diagrams | Generated from graph queries; **Mermaid** now (sequence, flowchart, `packet` for key layouts), D2 via WS5 later; click-through to GitHub permalinks. |
| Q | Generated docs | Committed under `docs/architecture/generated/`; CI fails if stale (no bot commits). |
| R | Versioning | Architecture changes in commit graphs; `current` holds it bitemporally (valid time = commit time); diffs from two `as_of_valid_time` queries. |
| S | Delivery | In the app, with the repo graph; initial curated model: the subsystems and the six canonical flows above. |
