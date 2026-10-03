# WS4 type packages — where the engine stops and the application starts — design note

Status: **proposal** (decisions in §5). Plan WS4 (§4.1–4.7) and registry
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
| 9 | Active-version pointer (4.6) | **App data + one engine primitive** | The pointer (`<urn:pg:types/…> cb:activeVersion <…@1.2.3>`) is app data in the app's own graph (bitemporal, so "which schema was active on June 3" is a time-travel query). What the engine must honour is the *effect*: a **staged** (inactive) type graph must not leak into default reads or inference — today inference takes schema from **every** base graph and union reads include every graph. → primitive **P1**. |
| 10 | Check / dry-run of live data against staged shapes (4.6) | **Engine (existing) + P2** | `ValidateShapes { shapes_graphs: [staged], data_graphs, overlay }` already does it. To check *after* a migration, the migration's changes must be previewed → **P2** (dry run) feeds them in as the overlay. |
| 11 | Migrations (SPARQL Update scripts, a wire transaction per graph) (4.6) | **Shared** | Choosing and ordering scripts is the tool's. Running one **atomically** is the engine's — and today REST SPARQL Update applies each operation / triple separately (not atomic, no precondition). → primitive **P2**: SPARQL Update compiled to one changeset — dry run returns it, apply commits it in one transaction with a `read_ts` precondition. General: anyone running SPARQL Update wants atomicity. |
| 12 | Rematerialization (4.6) | **Engine (existing)** | Activating a version is a schema change; the step 9b inference task recomputes (once P1 makes inactive graphs invisible to it). Nothing new. |
| 13 | Registry derivation from SHACL / OWL (§3.5) | **Engine** | The advisory registry (`NodeTypeRegistry` / `EdgeTypeRegistry`) is an engine feature the engine itself consumes (schema-aware pruning, `ValidateNode`, vector-space definitions), and SHACL parsing already lives in `polargraph-shacl`. → primitive **P3**: derive registry entries from given shapes graphs (dry run / apply); the app says *which* graphs (the active type graphs) and *when*. `RegisterNodeType` stays for ad-hoc use and warns when a derived entry exists for the same type. |
| 14 | `types diff <from> <to>` (4.6) | **Shared** | Term- and shape-level presentation is the tool's; the quad-level diff of two graphs is general → primitive **P4** (`DiffGraphs`, the optional 8c, with an `as_of` per side). |
| 15 | Rollback (4.6) | **Shared** | Re-pointing is app data + P1. Undoing a migration's data changes = diff the graph between "before upgrade" and now (P4 with `as_of`) and apply the inverse via `ApplyChanges`. Backups (existing) cover disasters. |
| 16 | Runtime prefixes for package namespaces | **Engine (existing)** | `PutPrefix` from the tool on activation. |
| 17 | Notifications on activation / migration | **Engine (existing)** | Change feed (`Subscribe`) — graph events and quad events are already there. |
| 18 | "Published = rendered" (4.7) | **App** | D2 rendering of package examples in the registry's CI; out of the engine (and out of scope here). P5 digests can key the render cache. |

## 3. Minimal engine primitives

| | Primitive | Shape | Also useful for |
|---|---|---|---|
| **P1** | **Graph activation flag** | Graph metadata `pg:active false` (default true): the graph is left out of **union / default-dataset reads** and **inference premises** unless named explicitly (`GRAPH <g>`, `graphs: [g]`, `shapes_graphs`). Set via `CreateGraph` metadata; flipping it is one commit (change feed event). | Staging any data (imports under review, archives), blue / green datasets |
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
| C | Active-version pointer | **App data** in the app's own graph; the engine primitive is the **activation flag (P1)** that the tool flips. (Alternative: an engine-level "active version" concept — rejected as package-specific.) |
| D | P1 semantics | Inactive graphs are excluded from union reads **and** inference premises (schema and instance), included when named explicitly. Default active. |
| E | Migrations | **P2**: SPARQL Update atomic by default (one transaction per request) with `dry_run` and `read_ts`. Breaking only for callers relying on partial application — release note. |
| F | Registry derivation | **P3** in the engine (`polargraph-shacl` + server RPC), invoked by the tool; registry entries marked `derived`; `RegisterNodeType` warns on conflicts. |
| G | Diff and digest | Build **P4** (`DiffGraphs`, with `as_of`) and **P5** (`GraphDigest`, blank-node-free RDFC-1.0) as general primitives. |
| H | Delivery | One engine step (P1–P5, each its own commits) after cb-bench PR 1; the ContxtBroker tool is out of this repo. |
