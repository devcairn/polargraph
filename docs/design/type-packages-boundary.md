# WS4 type packages — where the engine stops and the application starts — design note

Status: **revised proposal** (decisions in §5). Revision 2026-10-03 after
Mark's review: the distinction that matters is **schema vs instances**, not
active vs inactive — P1 is now **graph roles** (§2a). Plan WS4 (§4.1–4.7) and registry
derivation (§3.5). Same exercise as step 8, where promotion moved to a
ContxtBroker service and the engine kept general primitives
(`ApplyChanges`, `ValidateShapes`).

## 1. Principle

The engine stays **general-purpose and embeddable**: it knows graphs,
quads, shapes, inference, time, access control — not "packages",
"registries", semver, licenses, D2, or a network. A type package is, to the
engine, **a named graph of RDF plus metadata quads**. Everything that gives
packages their meaning (manifest, registry, resolution, lock files,
policies, rendering) lives in a ContxtBroker tool / service that talks to
the engine through its normal APIs.

Test for each piece: *would a non-ContxtBroker user of PolarGraph want this
in the database?* If only ContxtBroker needs it → app. If any user with
versioned schemas / staged data would → engine primitive.

## 2a. Schema vs instances — graph roles

Mark, 2026-10-03: "types are meant to be instantiated, not searched across
in most queries (if I search for cypher decisions, I don't want to see the
decision type, I want to see all decisions …, code, and general structure
that relate to how the db engine deals with cypher queries)."

A type package's class and property definitions, shapes, and render /
ingest metadata are **schema** (TBox). They must **drive** the engine —
`rdf:type` matching, Cypher labels, inference, SHACL, registry derivation,
the vocabulary — but must **not appear** in ordinary data queries or
searches. So a graph has a **role**:

| Role | Default reads (union / default dataset, Cypher `MATCH`, exports of "all") | Text and vector search | Inference | SHACL / registry derivation | Named explicitly (`GRAPH <g>`, `graphs: [g]`, `FROM <g>`) |
|---|---|---|---|---|---|
| `data` (default) | ✓ | ✓ | premises (instance and schema) | data under validation | ✓ |
| `schema` | — | — | **schema premises** | **shapes / definitions** (the default `shapes_graphs` when none are given) | ✓ |
| `staged` | — | — | — | only when named | ✓ |

(Existing implicit roles stay as they are: the system graph, and inferred
graphs — which take the **role of their source graph**.)

- **Instances keep working.** `?d a pgr:Decision`, Cypher `(d:Decision)`
  and typed vector filters match `rdf:type` triples, and those live in
  **data graphs** (where the instance is asserted). Checked in the 9a
  implementation: an inferred type takes the label of its **instance**
  premise, so `tom rdf:type Animal` (from `tom a Cat` in a data graph +
  `Cat ⊑ Animal` in a schema graph) lands in the data graph's inferred
  companion — visible. Pure schema closures (`Employee ⊑ Agent` from
  `rdfs11`) land in the schema graph's companion — hidden like the schema.
- **Search returns instances.** "cypher decisions" over text / vector
  search finds `Decision` instances, code, flows — not the `pgr:Decision`
  class node or its shape. Vector-hit visibility must then apply to service
  calls too (today it is only checked for users): a node is a hit only if it
  has a quad in a readable graph of role `data`.
- **Opt in when you want schema.** `include_schema: true` on `Query` /
  `CypherQuery` / search RPCs, `?include_schema=true` for SPARQL, or name the
  graph — for ontology browsers, the type-package tool, the architecture
  views.
- **Staged vs active is a sub-case**: installing a new version creates its
  type graph as `staged`; activation flips it to `schema` and the previous
  version to `staged` in **one** atomic `SetGraphRoles` call — so exactly
  the active schema feeds reasoning, and the flip is one change-feed event
  (and one inference recompute).
- **Per-graph, not per-triple.** A `rdfs:subClassOf` written into a data
  graph stays visible (ad-hoc modelling keeps working). Hiding TBox triples
  by predicate was considered and rejected: surprising, and it would hide
  legitimate data.
- **Access control** is unchanged: roles decide *which graphs a query
  ranges over*, grants decide *who may read them*. Reasoning and validation
  read schema graphs as the engine (service), so users need no grants on
  type graphs to benefit from them.

## 2. Piece by piece

| # | Piece (plan §) | Side | Why |
|---|---|---|---|
| 1 | Package format, `pkg.toml` manifest, file layout (4.1) | **App** | Product convention (licenses, reverse-DNS names, `render.ttl`, `ingest.ttl`). The engine only ever sees the RDF it contains. |
| 2 | Presentation (`cbr:`) and ingestion (`cbi:`) vocabularies (4.2, 4.3) | **App** | Already "the engine only stores it" (4.3). Stored as ordinary quads. |
| 3 | Registry repository, CI checks, org verification (4.4) | **App** | GitHub workflow and policy. |
| 4 | Fetch from registry / private registries / mirror | **App (tool)** | Network I/O, credentials, caching, trust. An embedded engine must work offline; fetching is not a database job. |
| 5 | Hash verification (RDFC-1.0) | **Shared** | The tool verifies the downloaded package against the lock file. The **engine** offers a general **graph digest** (canonical N-Quads → SHA-256) so the tool can verify what is *installed* matches the lock, and anyone can fingerprint a graph (integrity, cache keys, "published = rendered"). Packages have no blank nodes (rule in 4.1), so RDFC-1.0 reduces to sorting canonical N-Quads; skolem IRIs count as IRIs. |
| 6 | Dependency resolution, semver, lock files (4.6) | **App (tool)** | Files in the instance's config directory; semver policy is product. |
| 7 | CLI `polargraph types …` (4.6) | **App (tool)** | A separate binary (`contxtbroker-types`) calling the engine over gRPC / REST — not inside `polargraphd`. |
| 8 | Install into a TypeGraph (4.6) | **Engine (existing)** | `CreateGraph` with metadata (`cb:graphKind cb:TypeGraph`, version, digest) + `ApplyChanges` (≤ 100K quads — packages are small) or `/import/rdf?graph=`. Graph ACL decides who may install (graph admin). **No new primitive.** |
| 9 | Active-version pointer (4.6) | **App data + one engine primitive** | The pointer (`<urn:pg:types/…> cb:activeVersion <…@1.2.3>`) is app data in the app's own graph (bitemporal, so "which schema was active on June 3" is a time-travel query). What the engine must honour is the *effect*: schema graphs drive reasoning without showing up in data queries, and only the active version reasons — today inference takes schema from **every** base graph and union reads include every graph. → primitive **P1 graph roles** (§2a): activation is a `staged` → `schema` role flip. |
| 10 | Check / dry-run of live data against staged shapes (4.6) | **Engine (existing) + P2** | `ValidateShapes { shapes_graphs: [staged], data_graphs, overlay }` already does it. To check *after* a migration, the migration's changes must be previewed → **P2** (dry run) feeds them in as the overlay. |
| 11 | Migrations (SPARQL Update scripts, a wire transaction per graph) (4.6) | **Shared** | Choosing and ordering scripts is the tool's. Running one **atomically** is the engine's — and today REST SPARQL Update applies each operation / triple separately (not atomic, no precondition). → primitive **P2**: SPARQL Update compiled to one changeset — dry run returns it, apply commits it in one transaction with a `read_ts` precondition. General: anyone running SPARQL Update wants atomicity. |
| 12 | Rematerialization (4.6) | **Engine (existing)** | Activating a version is a schema change; the step 9b inference task recomputes (with P1, inference reads schema from `schema` and `data` graphs, never `staged`). Nothing new. |
| 13 | Registry derivation from SHACL / OWL (§3.5) | **Engine** | The advisory registry (`NodeTypeRegistry` / `EdgeTypeRegistry`) is an engine feature the engine itself consumes (schema-aware pruning, `ValidateNode`, vector-space definitions), and SHACL parsing already lives in `polargraph-shacl`. → primitive **P3**: derive registry entries from shapes — by default from all `schema`-role graphs, re-derived when a role flips; the app decides *when* (or explicit graphs, dry run). `RegisterNodeType` stays for ad-hoc use and warns when a derived entry exists for the same type. |
| 14 | `types diff <from> <to>` (4.6) | **Shared** | Term- and shape-level presentation is the tool's; the quad-level diff of two graphs is general → primitive **P4** (`DiffGraphs`, the optional 8c, with an `as_of` per side). |
| 15 | Rollback (4.6) | **Shared** | Re-pointing is app data + P1. Undoing a migration's data changes = diff the graph between "before upgrade" and now (P4 with `as_of`) and apply the inverse via `ApplyChanges`. Backups (existing) cover disasters. |
| 16 | Runtime prefixes for package namespaces | **Engine (existing)** | `PutPrefix` from the tool on activation. |
| 17 | Notifications on activation / migration | **Engine (existing)** | Change feed (`Subscribe`) — graph events and quad events are already there. |
| 18 | "Published = rendered" (4.7) | **App** | D2 rendering of package examples in the registry's CI; out of the engine (and out of scope here). P5 digests can key the render cache. |

## 3. Minimal engine primitives

| | Primitive | Shape | Also useful for |
|---|---|---|---|
| **P1** | **Graph roles** (§2a) | Graph metadata `pg:graphRole` = `data` (default) / `schema` / `staged`. `schema`: left out of default reads and search, consulted by inference, SHACL (default shapes) and registry derivation. `staged`: left out of everything unless named. Inferred companions take their source's role. `include_schema` request flag. `SetGraphRoles { roles: [{graph, role}] }` flips several graphs in one commit (graph admin). | Any ontology / shapes deployment (keeps TBox out of search results); staging imports; blue / green datasets |
| **P2** | **Atomic SPARQL Update** | `POST /sparql/update?dry_run=true` returns the changeset (adds / retractions per graph, `ApplyChanges` shape); without it, the update commits as **one** transaction, optionally with `read_ts`. Server-side: compile Update → changeset → `ApplyChanges`. | Every SPARQL Update user (today's per-triple application can half-apply) |
| **P3** | **Registry from shapes** | `SyncRegistryFromShapes { shapes_graphs, dry_run }` → derived `NodeTypeDef` / `EdgeTypeDef` (marked derived, with source graph); plus the `RegisterNodeType` warning. | Any SHACL-first deployment |
| **P4** | **`DiffGraphs` with `as_of`** | `DiffGraphs { a: {graph, as_of?}, b: {graph, as_of?} }` → quads only in a, only in b (streaming). | Proposals (8c), audits, rollback |
| **P5** | **Graph digest** | `GraphDigest { graph, as_of? }` → SHA-256 of canonical N-Quads (RDFC-1.0 for blank-node-free graphs). | Integrity checks, cache keys, replica verification |

Everything else WS4 needs is **already there**: named graphs + metadata
(TypeGraphs), `ApplyChanges` / import (install), `ValidateShapes` with
overlay (check), OWL RL inference with DRed (rematerialize), runtime
vocabulary (prefixes), change feed (notify), time travel (which version was
active when), graph ACL (who may install / activate), backups.

## 4. What the app owns (for the ContxtBroker side)

`contxtbroker-types` CLI / service: manifest parsing, registry client and
mirror, signature / digest verification of downloads, semver resolution and
lock files, the active-version pointer graph, upgrade orchestration
(`install → check → migrate → activate → rematerialize → sync registry`),
rollback policy (`--force-time-travel-restore`), the registry repository and
its CI, rendering.

## 5. Decisions needed

| # | Question | Recommendation |
|---|----------|----------------|
| A | Overall split | As §2: the engine has **no package concept**; type packages are graphs + metadata, orchestrated by a ContxtBroker tool. |
| B | Where the tool lives | A **separate ContxtBroker repo / binary** (`contxtbroker-types`), not a `polargraph types` subcommand in `polargraphd`. |
| C | Schema vs instances | **Graph roles (P1)**: `data` (default), `schema` (hidden from default reads, text and vector search; always consulted by inference, SHACL and registry derivation), `staged` (hidden and not consulted). Per graph, not per triple. Inferred companions inherit their source's role. Opt in with `include_schema` or by naming the graph. |
| D | Active-version pointer | **App data** in the app's own graph; activation is an atomic **`SetGraphRoles`** flip (`staged` → `schema` for the new version, `schema` → `staged` for the old). No engine "package version" concept. |
| E | Search visibility | Vector and text search hits require a quad in a readable `data` graph — for service calls too (today only user calls are checked). |
| F | Default `shapes_graphs` | `ValidateShapes` with no `shapes_graphs` uses all `schema`-role graphs. |
| G | Migrations | **P2**: SPARQL Update atomic by default (one transaction per request) with `dry_run` and `read_ts`. Breaking only for callers relying on partial application — release note. |
| H | Registry derivation | **P3** in the engine (`polargraph-shacl` + server RPC), invoked by the tool; registry entries marked `derived`; `RegisterNodeType` warns on conflicts. |
| I | Diff and digest | Build **P4** (`DiffGraphs`, with `as_of`) and **P5** (`GraphDigest`, blank-node-free RDFC-1.0) as general primitives. |
| J | Delivery | One engine step (P1–P5, each its own commits) after cb-bench PR 1; the ContxtBroker tool is out of this repo. |
