# Implementation Plan: ContxtBroker Knowledge Platform on PolarGraph

**Date**: 2026-09-29
**Status**: Draft — engine decisions recorded 2026-09-29 (see §0.1); engine work
in progress on branch `db/ws1-foundations`
**Scope**: Everything needed to turn PolarGraph from a triple store into a shared
knowledge platform where humans and LLMs build, trust, render, and retrieve
context together. Ten workstreams; engine work lives in this repo, platform
services (registry, MCP, ingestion workers) live beside it.

---

## 0. Summary

### What we are building

A knowledge graph that every department (engineering, sales, operations, CS,
finance) writes into and reads from, with LLMs as first-class participants.
The graph holds the **living state** of the organisation — entities, ownership,
dependencies, commitments, decisions, definitions — while **records** (chats,
plans, tickets, documents) are stored mostly as text and linked into it.

### Platform principles

These sit alongside the engine principles in `ROADMAP.md` (bitemporal by
default, Datalog as the query language, vector and graph as peers,
embeddability first). Nothing in this plan should violate those.

1. **Structure what LLMs cannot recover from text.** Identity, ownership,
   dependencies, commitments, decision status, definitions, lifecycle state.
   Leave rationale, discussion, and nuance as text linked to entities.
2. **Records are not living state.** A chat or plan is ingested shallowly
   (envelope + chunks + extracted claims). Only reviewed claims are promoted.
3. **LLM writes are always proposals.** Nothing an agent writes reaches the
   approved graph without passing SHACL validation and a promotion policy.
4. **Provenance is mandatory, not optional.** Every quad can answer: who said
   it, when, from what source, and has anyone checked it.
5. **Types are open and published.** Type definitions (meaning, constraints,
   presentation, ingestion policy) are openly licensed packages. If a type is
   rendered, it is published.
6. **Reference systems of record; don't copy them.** The graph holds IDs, key
   fields, and relationships. Revenue figures stay in the ERP.

### Workstream summary

| # | Workstream | Where | Depends on | Effort (1 eng + agents) |
|---|------------|-------|------------|-------------------------|
| WS1 | Foundation fixes (bnodes, term identity / IRI dictionary, value-hashed property keys, out-of-line text, trigram scope, retention) | engine | — | 4–6 wks |
| WS2 | Named graphs (quads) | engine | WS1 | 6–8 wks |
| WS3 | Trust layer (provenance, proposals, SHACL) | engine | WS2 | 6–8 wks |
| WS4 | Open type registry + instance pinning | registry repo + engine | WS2, WS3 (SHACL) | 5–7 wks |
| WS5 | D2 rendering | engine (`polargraph-render`) | WS2, WS4 | 3–4 wks |
| WS6 | Change feed (`Subscribe`) | engine | WS2 | 2–3 wks |
| WS7 | Ingestion (records, chunks, extraction, entity resolution) | workers | WS3, WS4, WS6 | 8–10 wks |
| WS8 | LLM interface (MCP + context assembly) | service | WS3, WS4 | 4–6 wks |
| WS9 | Collaboration UX (editor, entity pages, glossary) | UI | WS5, WS6, WS3 | 5–7 wks |
| WS10 | Maintenance and feedback (incremental reasoning, usage) | engine + service | WS3, WS8 | 4–5 wks |

**Critical path**: WS1 → WS2 → WS3 → WS8. WS4 and WS5 run in parallel after
WS2; WS6 can start as soon as WS2's key layout is fixed.

### 0.1 Engine decisions (2026-09-29)

Recorded after auditing this plan against the code at `849839a`.

| # | Decision | Replaces |
|---|----------|----------|
| D1 | **Value-hashed property keys** (§1.2): the object slot of a property key holds a hash of the value instead of the `0xFF×16` sentinel. `POS` becomes the value index, and a subject can hold several values for one predicate. | The separate `VAL` CF and its v3 backfill |
| D2 | **IRI dictionary and one term-identity layer in WS1** (§1.6), before WS2. | IRIs being one-way hashes; each import/query path mapping terms its own way |
| D3 | **One offline key rewrite** (migration **v3**) carries value-hashed property keys, the graph slot, `GSPO`/`GPOS`, and out-of-line value refs together (§2.9). | Separate migrations v3 (VAL), v4 (BLOB rewrite), v5 (48-byte keys) |
| D4 | **Retention prunes history, never live state** — fixed as a bug, not a profile default (§1.5). | "Off by default + startup warning" |
| D5 | **Engine sequencing** as in §0.2. | — |

### 0.2 Engine sequencing

| Step | Work | Migration | Status |
|------|------|-----------|--------|
| 0 | Retention fix (1.5); bnode skolemization (1.1); trigram size cap (1.4) | none | ✅ `5cb5af4`, `d2ae884`, `b3536c5` |
| 1 | Refactor only: key-width constants/accessors in `keys::`, `VarPattern` literals via `..Default::default()`. No behaviour change. (`Quad` type moved to step 3, where it's first used.) | none | ✅ |
| 2 | Term identity + IRI dictionary (1.6) | additive CF, no rewrite | |
| 3 | New key layout: value-hashed property keys (1.2), 48-byte keys with `g` (2.3), `GSPO`/`GPOS`, `BLOB` CF + value refs (1.3), conflict detection on `(s,p,o,g)`, graph interning | **v3** offline rewrite | |
| 4 | `GraphTerm` in Datalog/planner; graph RPCs; N-Quads/TriG | — | |
| 5 | SPARQL dataset semantics + graph Update ops; Cypher `USE GRAPH` | — | |
| 6 | Graph-bitmap ACL inside scans (2.8) | — | |
| 7 | `Subscribe` change feed (WS6) | — | |
| 8 | WS3 engine parts: provenance enforcement, proposal RPCs, `polargraph-shacl` | — | |
| 9 | DRed, `STATS` CF, int8 quantization | — | |

---

## 1. Current state (what research found)

Audit of the repo as of commit `849839a`.

| Area | Finding | Implication |
|------|---------|-------------|
| Key layout | Fixed 44-byte keys, six hexastore CFs, `tt` last (`docs/architecture.md` §Key layout) | Adding a graph dimension changes key width → migration by rewrite |
| Value storage | `batch_triple()` writes the **same value bytes** into all six CFs (`store.rs`) — including `Value::Vector`, so a 384-dim embedding is ~1.5 KB × 6 on top of the HNSW CF | Long text and vectors are stored 6×; needs out-of-line storage |
| Property triples | Use a sentinel object `0xFF×16` in every index | `POS` cannot look up a property by value; `status = "blocked"` is a scan + post-filter. **Also: one value per `(s,p)`** — two literals for the same subject and predicate share a key, so the second overwrites the first (`rdfs:label "Acme"@en, "Acmé"@fr` keeps one) |
| Term identity | IRIs are hashed to `NodeId` and **never stored**; export renders `urn:uuid:`. REST import hashes every IRI, SPARQL `INSERT DATA` silently skips IRIs that aren't `urn:uuid:`, REST import lets the server assign edge IDs while the bulk importer derives them; language tags are dropped | RDF round-trips lose IRIs; the same data gets different IDs depending on the path it came in by. Fixed by §1.6 |
| Trigram index | `batch_text_trigrams()` runs for **every** `Text` property. Cypher `CONTAINS`/`STARTS WITH`/`=~` **do not use it** (post-filter in `apply_text_filters`); only `Snapshot::text_search` reads it | Descriptions and chunk text explode the `tri` CF for almost no query benefit. Capped at 512 bytes (§1.4, done) |
| Blank nodes | REST/SPARQL import hashes `"_:bnode_" + label` with no import scope (`rdf_import.rs`); bulk importer's N-Triples path skips bnodes (Turtle/JSON-LD paths keep them, unscoped); JSON-LD `_:` ids treated as IRIs | Two files with `_:b0` collide; bulk loads silently drop structure. Fixed by §1.1 (done) |
| Named graphs | None. SPARQL `GRAPH <iri>` maps to a `View` (a projection, not a partition) | No per-source partitioning, promotion, or graph-level ACL |
| Views | `View { node_filter, visible_predicates, edge_presentations }` | Good fit for **render presets** (WS5), not for data partitioning |
| Schema registry | Node/edge types, field kinds, cardinality, inverse, parent types, `ValidateOntology` | Advisory only; no closed-world gate; no SHACL |
| OWL 2 RL | 12 rules, full forward-chaining run into `DRV` | No incremental maintenance; retraction handling unclear |
| Access control | `AccessCache: HashMap<user, HashSet<NodeId>>`, post-filter on bindings | Per-node sets won't scale to tens of millions of nodes |
| Change feed | Only `StreamWal` for replicas | No public subscription for renderers, embedders, notifications |
| Retention | Deletes **every** version older than `tx_age`, including the current value of a fact that hasn't changed; `vt_lookback` can delete a DELETE tombstone alone and resurrect the fact | Destroys **live data**, not just history. Fixed by §1.5 (done) |
| Column families | 13, not 12 (`PEA` predicate-first annotation index exists) | — |
| Vector index | Pure-Rust HNSW, memory/mmap, named spaces | No quantization; RAM heavy at 10M+ vectors |

---

## WS1. Foundation fixes

Small, independent changes that the rest of the plan relies on.

### 1.1 Blank-node skolemization

> **Done** (`d2ae884`). `polargraph-core::skolem::ImportScope`; REST
> `?import_id=` + `--skolem-base`; `polargraph-import --import-id --skolem-base`;
> JSON-LD `_:` ids are blank nodes. De-skolemizing export waits on §1.6.

**Problem.** `bnode_to_node_id(label)` is deterministic on the label alone, so
`_:b0` in two separate imports is the same node. The bulk importer drops
bnodes entirely.

**Design.** Mint an import-scoped skolem IRI per the RDF 1.1 skolemization
convention:

```
https://{instance-host}/.well-known/genid/{import_id}/{label}
```

- `import_id` is a UUIDv7 generated per import call (or supplied by the caller
  for idempotent re-imports).
- The skolem IRI is hashed through the existing `uri_to_node_id()`, so the
  resulting `NodeId` is stable for the same `(import_id, label)`.
- Export can optionally de-skolemize back to `_:` labels (`?deskolemize=true`).

**Changes.**
- `crates/polargraph-sparql/src/rdf_import.rs` — thread an `ImportScope`
  through `parse_ntriples/turtle/jsonld`; replace `bnode_to_node_id`.
- `crates/polargraph-import/src/main.rs` — stop skipping bnodes; add
  `--import-id` flag.
- `crates/polargraph-sparql/src/serialize.rs` — optional de-skolemization.

**Tests.** Two files both using `_:b0` produce distinct nodes; re-import with
the same `import_id` is idempotent; bulk import preserves bnode structure.

### 1.2 Value-hashed property keys

*(Decision D1 — replaces the separate `VAL` CF.)*

**Problem.** Property keys carry a sentinel object, so

- equality lookups on property values (`status = "blocked"`,
  `external_id = "SF-0042"`) can't use an index — exactly the lookups entity
  resolution and cross-department queries need; and
- a subject can hold only **one** value per predicate: every value of
  `(s, p)` has the same key, so a second value overwrites the first. RDF data
  with several labels, aliases or emails per subject is silently truncated.

**Design.** Put a hash of the value in the object slot:

```
SPO  [s16][p4][value_hash16][tt8]      (property)   — was [s][p][0xFF×16][tt]
POS  [p4][value_hash16][s16][tt8]      (property)   — the value index
```

- `value_hash` = xxHash3-128 of the canonical value encoding: a type tag plus
  the value bytes (and, after §1.6, the datatype IRI / language tag), so
  `"30"` and `30` differ and `"Acme"@en` ≠ `"Acme"@fr`.
- Relation vs property is decided by the value's discriminant byte (already
  read with every key), not by the key. A value hash equal to a real
  `NodeId` is a 2⁻¹²⁸ event; lookups verify the discriminant and the value on
  every hit.
- **Value lookup** `(?s, :p, "literal")` → `POS` prefix `[p][value_hash]`,
  verify value. No extra CF, no extra write, available for every property
  (no `indexed: true` flag needed).
- **Current value(s) of `s.p`** → `SPO` prefix `[s][p]` (was
  `[s][p][sentinel]`) — same cost; returns every live value.
- Out-of-line values (§1.3) hash the value, not the ref, so the key doesn't
  depend on where the bytes live.

**Write semantics.** With distinct keys per value, "replace" is no longer
implicit:

| Mode | Behaviour | Used by |
|------|-----------|---------|
| `Replace` (default) | Close `vt_end` on every live value of `(s,p)`, then insert the new value | Existing `Insert` RPC callers, Cypher `SET`, REST `/insert` — preserves today's behaviour |
| `Add` | Insert alongside existing values | RDF import, SPARQL `INSERT DATA`, multi-valued fields |

`PropertyWriteMode` is a new optional field on the proto property triple.
Conflict detection: `Replace` checks the `[s][p]` prefix; `Add` checks the
exact `[s][p][value_hash]`.

**Modelling guidance (documented, not enforced).** Low-cardinality states
should still be **IRIs, not literals** (`work:status work:Blocked`) so they
participate in reasoning and rendering.

**Changes.** `polargraph-storage::{keys, store, mvcc, sst_import, compaction}`
(the five sentinel call sites plus `scan_property_history`),
`polargraph-query::planner` (`choose_index` for bound literal objects),
Cypher `SET`, proto `PropertyWriteMode`. Lands in migration **v3** together
with the WS2 key layout (D3).

### 1.3 Out-of-line large values

**Problem.** Text payloads are copied into six CFs. A 2 KB description costs
12 KB before compression.

**Design.** Values over a threshold (default 256 bytes, configurable) are
stored once in a content-addressed `BLOB` CF; index entries carry a reference.

```
BLOB      key [content_hash(16)]   value [len:4][bytes]
Value     [0x04][vt_start:8][vt_end:8][content_hash:16]   = PropertyRef, 33 bytes
```

- Reads resolve refs lazily — only when a property value is projected.
- Identical text (boilerplate, repeated chunks) is stored once.
- Garbage collection: refcount in `META`, or a mark-and-sweep pass during
  retention/compaction runs.

**Changes.** `codec.rs` (new discriminant), `store.rs` (write + resolve),
`compaction.rs` (GC). Existing large values are moved out of line during
migration **v3** (D3), since every key is rewritten then anyway. Config: `[storage] inline_value_max_bytes = 256`.

### 1.4 Trigram scoping

> **Size cap done** (`b3536c5`): values over 512 bytes are not indexed, at the
> single write choke point. Safe for correctness because Cypher text
> predicates don't read `TRI` today. The per-field `text_index` override
> below needs the registry consulted at write time and is still open; wiring
> Cypher text predicates to `TRI` (with a scan fallback over the cap) is a
> separate follow-up.

**Design.** Add `text_index: Option<bool>` to `FieldDef`. Default: index text
properties ≤ 512 bytes, skip larger ones. Explicit `true/false` overrides.
Labels, names, and titles stay searchable; descriptions and chunk text go
through vectors instead.

**Changes.** `batch_text_trigrams()` call site in `Transaction::commit()`
consults the registry; `ShowIndexes` reports skipped-field counts.

### 1.5 Retention safety

> **Done** (`5cb5af4`), as a bug fix rather than a profile default (D4).

- `tx_age_secs` now bounds **history**: a version is deleted only when a newer
  version with the same `vt_start` was committed before the cutoff (a
  correction or DELETE tombstone). The current value is never deleted, and
  valid-time history (later `vt_start`) is kept.
- `vt_lookback_secs` removes a triple only once **every** version is closed
  before the cutoff, so a tombstone is never removed alone (which previously
  resurrected deleted facts).
- Still to do after WS2: per-graph retention exemptions (`cb:retain
  cb:Forever` on graph metadata) so record graphs can expire while approved
  state never does.

### 1.6 Term identity and IRI dictionary

*(Decision D2 — new in WS1, before WS2.)*

**Problem.** IRIs are hashed to `NodeId`s and never stored, so export can only
render `urn:uuid:…`; skolem IRIs can't be de-skolemized; graph IRIs, type
package terms and provenance agents (WS2–WS4, WS8) can't be shown to people or
LLMs. Each path also maps terms differently (REST import hashes every IRI,
SPARQL `INSERT DATA` only accepts `urn:uuid:`, edge IDs are derived on one path
and server-assigned on another), and literal datatypes and language tags are
dropped.

**Design.**

- One `polargraph-core::term` module owns IRI ↔ `NodeId` mapping, used by every
  import, query translation and export path:
  - `urn:uuid:<u>` ↔ `NodeId(u)` (no hashing, so native IDs round-trip);
  - any other IRI → `NodeId::from_iri(iri)` (xxHash3-128);
  - blank nodes → skolem IRI via `ImportScope` (§1.1), then as above;
  - relation edge IDs for RDF paths → `edge_id_for(s_iri, p, o_iri)`, one
    implementation.
- New `IRI` CF: `[node_id(16)] → iri (UTF-8)`, written in the same
  `WriteBatch` as the triple whenever a write carries an IRI. On write, an
  existing entry with a different IRI is a hash collision → hard error.
- Export (`serialize`, SPARQL results, JSON-LD) looks up the dictionary and
  falls back to `urn:uuid:`; skolem IRIs export as `_:` labels on request
  (`?deskolemize=true`).
- Literals keep datatype and language: a language-tagged text value variant
  in `polargraph-core::value`, and the datatype IRI for non-native XSD types,
  included in the §1.2 value hash.
- SPARQL `INSERT DATA`/`DELETE DATA` map terms through the same module instead
  of dropping non-`urn:uuid` IRIs, and report failures rather than skipping.

**Migration.** Additive (new CF); no rewrite. Existing hashed IRIs can't be
recovered (hashes are one-way), so pre-existing data exports as `urn:uuid:`
until re-imported. `urn:uuid:` IRIs imported via REST before this change were
hashed rather than parsed; those nodes keep their old IDs — document, don't
migrate.

**Tests.** Round-trip N-Triples/Turtle/JSON-LD preserves IRIs, language tags
and datatypes; the same document through REST import, bulk import and SPARQL
`INSERT DATA` yields identical `NodeId`s and `EdgeId`s; collision detection.

---

## WS2. Named graphs (quads)---

## WS2. Named graphs (quads)

### 2.1 Goals

- Partition data by source, review status, tenant, and context.
- Make promotion (proposed → approved) a graph copy, and rejection a graph drop.
- Enforce access control per graph, cheaply, inside the scan.
- Full SPARQL dataset semantics: default graph, `GRAPH`, `FROM`, `FROM NAMED`.

### 2.2 Graph identity

Graph IRIs are interned exactly like predicates:

```
META  graph_fwd:{iri}  → graph_id (u32, big-endian)
META  graph_rev:{id}   → iri
META  graph_ctr        → next id
```

- `graph_id = 0` is the **default graph**. All existing data lands there, so
  current APIs keep working unchanged.
- 4 bytes keeps keys compact; 4 billion graphs is ample even with one graph
  per record.

### 2.3 Key layout

Two options were considered:

| Option | Layout | Pros | Cons |
|--------|--------|------|------|
| A. Graph in value | keys unchanged, `graph_id` in value bytes | No key migration | No per-graph prefix scans; post-filter everything; can't hold the same triple in two graphs |
| **B. Graph in key** | `g` before `tt` in existing CFs + 2 graph-leading CFs | Per-graph scans, in-scan ACL, true quads | Key rewrite migration; write amplification 6× → 8× |

**Decision: Option B.**

```
SPOG [s16][p4][o16][g4][tt8]   = 48 bytes   (was SPO)
SOPG [s16][o16][p4][g4][tt8]
PSOG [p4][s16][o16][g4][tt8]
POSG [p4][o16][s16][g4][tt8]
OSPG [o16][s16][p4][g4][tt8]
OPSG [o16][p4][s16][g4][tt8]
GSPO [g4][s16][p4][o16][tt8]   new — "everything in graph g", "g's view of s"
GPOS [g4][p4][o16][s16][tt8]   new — "in graph g, who has p = o"
```

- `g` sits immediately before `tt`, so all versions of one quad still sort
  together and MVCC snapshot filtering is unchanged.
- Union (all-graph) queries use the existing six orders; results are
  de-duplicated on `(s,p,o)` unless the query binds `?g`.
- Property keys use the §1.2 value hash in the object slot in every order.
- `TRI`, `DRV`, `EPA`, `EPO`, `PEA` gain `g` (`TRI` gets it before the subject,
  so graph ACL applies to text-search candidates too).
- Write amplification rises from 6 to 8 CF writes per quad. Budget: ≤ 30%
  insert-throughput regression on `polargraph-bench`.

**Planner index selection (additions).**

| Bound slots | CF | Prefix |
|-------------|----|--------|
| G | GSPO | 4 bytes |
| G, S | GSPO | 20 bytes |
| G, S, P | GSPO | 24 bytes |
| G, P | GPOS | 8 bytes |
| G, P, O | GPOS | 24 bytes |
| S, P, O, G | SPOG | 40-byte exact |
| any without G | existing order, graph filter applied in-scan on key bytes 36–39 (same offset in all six orders) |

### 2.4 MVCC and transactions

- Conflict detection keys on `(s, p, o, g)`. The same triple written to two
  different graphs in concurrent transactions does not conflict.
- `DropGraph` is **bitemporal**: it writes tombstones (set `vt_end`) for every
  live quad in the graph rather than deleting keys. History remains queryable.
- `CopyGraph(src, dst)` streams `GSPO[src]` into a `WriteBatch` for `dst`
  inside one transaction. Large graphs (>100K quads) use chunked commits with
  a graph-level `cb:copyInProgress` flag.

### 2.5 Edge identity and RDF-star

**Open question resolved here:** edge IDs remain `edge_id_for(s, p, o)` —
graph-independent, because an RDF-star quoted triple denotes the triple, not
its occurrence. Annotation rows in `EPA`/`EPO` carry the `g` of the graph in
which the annotation was asserted. So "confidence 0.8 according to extraction
run X" and "confidence 1.0 after review" coexist in two graphs.

### 2.6 Query surfaces

**Datalog IR.**

```rust
pub struct VarPattern {
    pub s: Term,
    pub p: Term,
    pub o: Term,
    pub g: GraphTerm,        // new
}

pub enum GraphTerm {
    Default,                 // graph 0 only
    Union,                   // all graphs visible to the caller (default for existing APIs)
    Bound(GraphId),
    Var(String),             // binds ?g
    Set(Vec<GraphId>),       // FROM-style dataset
}
```

**SPARQL.** Replace the `GRAPH → View` mapping in `translate.rs`:
`GRAPH <iri>` → `Bound`, `GRAPH ?g` → `Var`, `FROM`/`FROM NAMED` → `Set`.
SPARQL Update gains `INSERT DATA { GRAPH <g> { ... } }`, `CLEAR`, `DROP`,
`COPY`, `MOVE`, `ADD`.

**Cypher.** Two additions, both compile to `GraphTerm`:

```cypher
USE GRAPH <https://cb.ai/graph/approved>
MATCH (s:Service)-[:dependsOn]->(d) RETURN s, d
```

and a request-level `graphs: [...]` field on `CypherQueryRequest`.
Graph-scoped writes: `CREATE ... IN GRAPH <iri>` (extension) or `graph` field
on `CypherWriteRequest`.

**gRPC / REST.**

| RPC | REST | Notes |
|-----|------|-------|
| `Insert` (+`graph` field) | `POST /insert` | defaults to graph 0 |
| `Query` (+`graphs`, `bind_graph_var`) | `POST /query` | |
| `CreateGraph` | `POST /graphs` | registers IRI + metadata quads in `urn:pg:graph:meta` |
| `ListGraphs` | `GET /graphs` | filter by metadata (status, source, type) |
| `GraphStats` | `GET /graphs/:id/stats` | quad count, size estimate, last write |
| `CopyGraph` / `MoveGraph` | `POST /graphs/:id/copy` | transactional |
| `DropGraph` | `DELETE /graphs/:id` | bitemporal tombstone |

### 2.7 Formats

`rio_turtle` already provides TriG and N-Quads parsers; reuse them.

- Import: `POST /import/rdf` with `application/trig`, `application/n-quads`;
  JSON-LD with named `@graph`. Triple formats still load into graph 0 (or a
  `?graph=` parameter).
- Bulk: `polargraph-import --format nquads|trig`. N-Quads is the preferred
  bulk format (line-splittable, parallel parse).
- Export: `GET /export/subgraph` negotiates `application/trig` and
  `application/n-quads`; `GET /graphs/:id/export` for a whole graph.
- Star variants (TriG-star, N-Quads-star) follow the existing Turtle-star path.

### 2.8 Graph-level access control

Replace the per-user `HashSet<NodeId>` post-filter as the primary mechanism:

- New builtin predicates `HAS_GRAPH_ACCESS` (user/group → graph) and
  `GRAPH_ACCESS_LEVEL` (`read` | `propose` | `write` | `admin`).
- `AccessCache` becomes `HashMap<UserId, RoaringBitmap<GraphId>>` — tiny even
  with millions of graphs.
- The scan layer receives the caller's bitmap and skips keys whose `g` isn't
  allowed **before** decoding values. For graph-leading CFs this is a prefix
  skip; for others it's a 4-byte check per key.
- Node-level grants remain for fine-grained exceptions and are applied as
  today's post-filter.

### 2.9 Migration

Migration **v3** (D3): a single key rewrite from the 44-byte layout to the
48-byte layout, which also replaces property sentinels with value hashes
(§1.2), moves large values out of line (§1.3), and populates `GSPO`/`GPOS`.

- Offline path (recommended): export all CFs → rewrite keys with `g = 0`
  (and value hashes / refs) → SST ingest into new CFs → swap. Reuses
  `SstImporter`. Verify per-CF entry counts and a checksum of decoded
  `(s,p,o,value,tt)` tuples before the swap.
- Two versions of one property written in the **same** commit collided under
  the old layout, so there's nothing to split; distinct historical values
  become distinct keys naturally.
- Online path (later): dual-write period + background backfill; not needed
  for the first release.
- Replicas: primary performs the migration; replicas re-bootstrap from a
  backup taken after migration.

### 2.10 Tests and benchmarks

- Storage: quad round-trip, same triple in two graphs, drop/copy/move with
  time travel, conflict semantics.
- Query: every `GraphTerm` variant in Datalog, SPARQL dataset tests from the
  W3C test suite (dataset subset), Cypher `USE GRAPH`.
- ACL: users see only permitted graphs across all query paths, including
  vector search and streaming.
- Bench: extend BSBM with a graph-partitioned variant (one graph per vendor).
  Targets: union-query regression ≤ 10%; single-graph full load (`GSPO` prefix
  scan of 1K quads) p50 ≤ 2 ms.

---

## WS3. Trust layer

### 3.1 Provenance profile (`cb-prov`)

A small, published profile of PROV-O. Graph-level metadata lives in the
system graph `urn:pg:graph:meta`; per-fact metadata uses RDF-star.

**Required on every non-default graph:**

| Property | Range | Example |
|----------|-------|---------|
| `prov:wasAttributedTo` | `prov:Person` or `cb:ModelAgent` | `:mark`, `:agent/claude-opus-5-5` |
| `prov:wasGeneratedBy` | `prov:Activity` | `:run/2026-09-29T14:02Z-extract` |
| `prov:generatedAtTime` | `xsd:dateTime` | |
| `cb:status` | `cb:Proposed` \| `cb:Approved` \| `cb:Rejected` \| `cb:Superseded` | |
| `cb:graphKind` | `cb:RecordGraph` \| `cb:ProposalGraph` \| `cb:ApprovedGraph` \| `cb:TypeGraph` \| `cb:DerivedGraph` | |

**Optional:** `prov:wasDerivedFrom` (record or chunk IRIs), `cb:reviewedBy`,
`cb:reviewedAt`, `cb:supersedes`, `cb:retain`.

**`cb:ModelAgent`** carries `cb:modelId`, `cb:promptHash`, `cb:toolVersion`, so
any extracted fact can be traced to the exact model and prompt that produced it.

**Per-fact annotations (RDF-star):** `cb:confidence` (0–1), `cb:sourceSpan`
(chunk IRI + char offsets), `cb:assertedAt`.

**Enforcement.** `CreateGraph` rejects non-default graphs missing required
metadata (configurable to `warn` during rollout).

### 3.2 Graph kinds and lifecycle

```
                    ┌──────────────┐
  ingestion/LLM ──► │ ProposalGraph│──reject──► status=Rejected (kept for audit)
  human edits   ──► │  (Proposed)  │
                    └──────┬───────┘
                           │ PromoteGraph (SHACL pass + policy)
                           ▼
                    ┌──────────────┐
                    │ ApprovedGraph│  one per domain/tenant, e.g.
                    │ (living state)│  graph/approved/eng, graph/approved/sales
                    └──────┬───────┘
                           │ contradicted/replaced by later promotion
                           ▼
                    quads get vt_end set; proposal marked cb:supersedes
```

- **Record graphs** hold record envelopes and chunk metadata; they are never
  promoted, only linked.
- **Approved graphs** are partitioned by domain (and tenant). Cross-domain
  queries union them; ACL decides visibility.
- **Derived graphs** hold OWL RL output per approved graph (see WS10).

### 3.3 Proposal workflow RPCs

| RPC | Behaviour |
|-----|-----------|
| `ProposeGraph(quads, provenance, target)` | Creates a `ProposalGraph` with status `Proposed` and a `cb:targetGraph` |
| `DiffGraph(proposal, target)` | Returns adds, removals (explicit retractions), conflicts (same `s,p` with a different `o` where cardinality is one), and no-ops |
| `ValidateGraph(proposal, target)` | Runs SHACL over the **merged** view (target ∪ proposal − retractions) for affected focus nodes only |
| `PromoteGraph(proposal, policy)` | In one transaction: validate → close superseded facts (`vt_end = now`) → copy adds → set status `Approved` → record `cb:reviewedBy` |
| `RejectGraph(proposal, reason)` | Status `Rejected`; quads remain for audit and feedback |

**Promotion policy** (per target graph, stored in `urn:pg:graph:meta`):

```turtle
:graph/approved/eng cb:promotionPolicy [
    cb:autoApproveFor :group/eng-owners ;      # human edits by owners
    cb:requireReviewFor cb:ModelAgent ;         # all LLM proposals
    cb:minConfidence 0.7                        # below this, never auto-approve
] .
```

(Policies are named nodes in practice — the inline form is shown for brevity
and is skolemized on import per WS1.1.)

### 3.4 SHACL engine (`polargraph-shacl`)

New crate, depending on `polargraph-core` and `polargraph-query` only.

**Supported subset (v1):**

| Feature | Notes |
|---------|-------|
| `sh:NodeShape`, `sh:PropertyShape` | |
| Targets | `sh:targetClass`, `sh:targetNode`, `sh:targetSubjectsOf`, `sh:targetObjectsOf` |
| Paths | predicate, inverse (`sh:inversePath`), sequence |
| Cardinality | `sh:minCount`, `sh:maxCount` |
| Value type | `sh:datatype`, `sh:class`, `sh:nodeKind` |
| Value range | `sh:minInclusive`, `sh:maxInclusive`, `sh:minExclusive`, `sh:maxExclusive` |
| String | `sh:pattern`, `sh:minLength`, `sh:maxLength` |
| Enumeration | `sh:in` |
| Structure | `sh:node`, `sh:closed`, `sh:ignoredProperties` |
| Severity | `sh:Violation`, `sh:Warning`, `sh:Info` |

Deferred: `sh:sparql` constraints, `sh:qualifiedValueShape`, full property
path algebra.

**Execution model.** Shapes compile to **Datalog violation rules** (consistent
with the "extend Datalog" principle). E.g. `sh:minCount 1` on `:owner` for
`:Service` becomes a rule producing `violation(?focus, shape_id)` for every
`?focus` of type `:Service` lacking an `:owner` in the evaluated dataset.

**Incremental validation.** For a proposal, focus nodes = subjects and objects
touched by the proposal plus nodes whose targets could change (type additions).
Validation cost scales with the proposal, not the graph.

**Report.** Standard `sh:ValidationReport` as quads in the proposal graph plus
a JSON rendering for UIs and the MCP layer.

### 3.5 Registry derivation

Make SHACL + OWL the **source of truth** and derive the existing advisory
registry from them:

- `sh:NodeShape` with `sh:targetClass C` → `NodeTypeDef { type_name: C, fields }`
- property shapes → `FieldDef { kind, required: minCount ≥ 1, cardinality }`
- `rdfs:domain`/`rdfs:range` + shapes → `EdgeTypeDef`
- `RegisterNodeType`/`RegisterEdgeType` remain for ad-hoc/dev use but emit a
  deprecation notice once a type package is installed for the same IRI.

### 3.6 Changes

- New crate `crates/polargraph-shacl` (parser from quads, compiler to Datalog, report builder).
- `polargraph-server`: proposal RPCs, promotion transaction, policy evaluation.
- `polargraph-core::schema`: `from_shapes()` conversion.
- Proto: `ProposeGraph`, `DiffGraph`, `ValidateGraph`, `PromoteGraph`, `RejectGraph`.
- Tests: W3C SHACL core test-suite subset; promotion atomicity under concurrent
  writes; supersession preserves history under `as_of_valid_time`.

---

## WS4. Open type registry and instance pinning

### 4.1 Package specification

```
packages/<org>/<name>/
  pkg.toml         manifest
  ontology.ttl     OWL classes and properties (meaning)
  shapes.ttl       SHACL shapes (constraints)
  render.ttl       presentation defaults (D2)
  ingest.ttl       ingestion policy (optional)
  migrations/      SPARQL Update scripts, one per major version step
  examples.trig    fixtures; also CI validation data
  README.md
```

```toml
[package]
name        = "io.github.contxtbroker/org"
version     = "1.2.0"
namespace   = "https://types.contxtbroker.ai/org#"
license     = "CC-BY-4.0"
description = "Customers, accounts, products, services, contracts, commitments"

[dependencies]
"io.github.contxtbroker/core" = "^1.0"
"io.github.contxtbroker/prov" = "^1.0"

[render]
default_direction = "right"
```

**Rules.**
- Term IRIs are **unversioned** (`org#Customer`); `owl:versionIRI` records the release.
- Package names are reverse-DNS; `io.github.<org>` namespaces are owned by the
  matching GitHub org (verified in CI via the PR author's org membership).
- Allowed licenses: CC0-1.0, CC-BY-4.0, Apache-2.0, MIT.
- **No blank nodes** in any package file (styles, policies, restrictions are
  named nodes) — keeps hashes and diffs stable.
- Packages may `rdfs:subClassOf` / `owl:equivalentClass` standard vocabularies
  (schema.org, FOAF, SKOS, PROV-O, Dublin Core) and are encouraged to.

### 4.2 Presentation vocabulary (`cbr:`)

```turtle
@prefix cbr: <https://types.contxtbroker.ai/render#> .

org:ServiceStyle a cbr:Style ;
    cbr:shape "rectangle" ;
    cbr:fill "#4f46e5" ;
    cbr:stroke "#1e40af" ;
    cbr:fontColor "#ffffff" ;
    cbr:icon <https://icons.terrastruct.com/essentials/112-server.svg> .

org:Service cbr:style org:ServiceStyle ;
    cbr:labelTemplate "{rdfs:label}" ;
    cbr:containerBy org:partOfSystem .       # render inside its System

org:dependsOn cbr:edgeStyle org:DependsOnStyle ;
    cbr:edgeLabelTemplate "" .

org:DependsOnStyle a cbr:Style ;
    cbr:stroke "#22c55e" ;
    cbr:arrowhead "triangle" .
```

Style resolution order: instance override → most specific class with a style
→ superclass chain → package default → generic fallback.

### 4.3 Ingestion policy vocabulary (`cbi:`)

```turtle
rec:Conversation cbi:defaultLevel cbi:Retrievable ;     # levels 0–4
    cbi:extract cbi:Decisions, cbi:Commitments, cbi:EntityMentions .

work:Plan cbi:defaultLevel cbi:Retrievable ;
    cbi:promoteWhen work:PlanIsActive ;
    cbi:promoteTo cbi:FullStructure .

work:PlanIsActive a cbi:Condition ;          # named node, not a blank node
    cbi:property work:status ;
    cbi:equals work:Active .
```

Consumed by WS7 workers; the engine only stores it.

### 4.4 Registry repository (`contxtbroker/types`)

- GitHub monorepo, layout as 4.1.
- Release tag per package: `<org>/<name>/v<semver>`.
- **Content hash**: SHA-256 over the **RDFC-1.0 canonical N-Quads** of all
  package RDF files (each file loaded into a graph named after its role).
  Formatting changes don't change the hash; semantic changes always do.
- `index/<org>/<name>.json` (generated, append-only) lists versions, hashes,
  dependency ranges, yanked flags. Git tags can move; the index + hash is the
  trust anchor.
- A mirror/proxy (static site or small service) serves immutable tarballs by
  hash so instances never depend on GitHub tag integrity. This is the upgrade
  path to a Go-modules-style decentralized registry.

**CI checks per PR:**

1. Parse all files; reject blank nodes.
2. Load into an embedded PolarGraph (library mode); run OWL 2 RL
   materialization; fail on `owl:Nothing` inferences.
3. Validate `examples.trig` against `shapes.ttl`.
4. Semver diff against the previous release (`cb-types semver-check`):
   - patch: labels, comments, render/ingest changes only
   - minor: added terms, relaxed shapes (lower `minCount`, wider `sh:in`)
   - major: removed/renamed terms, changed domain/range, tightened shapes
5. Major bumps must ship a migration script covering removed/renamed terms.
6. Render D2 snapshots of every class and of `examples.trig`; commit SVGs.
7. License, namespace ownership, dependency resolution.

**Catalog site** (static, generated from the index + snapshots): browse by
package, class, or property; each class page shows its rendered snapshot,
shapes as a readable table, and "used by" dependents.

### 4.5 Seed packages

| Package | Contents |
|---------|----------|
| `core` | `Entity`, `Person`, `Team`, `Organisation`, `ExternalId` (system + key), `owns`, `memberOf`, `sameAs` conventions |
| `prov` | The `cb-prov` profile from WS3.1 |
| `rec` | `Record`, `Conversation`, `Document`, `Ticket`, `Meeting`, `Chunk`, `mentions`, `hasChunk`, span properties |
| `org` | `Customer`, `Account`, `Contract`, `Product`, `Feature`, `Service`, `System`, `Vendor`, `dependsOn`, `exposes`, `uses`, `governedBy` |
| `work` | `Plan`, `Phase`, `Task`, `Decision`, `Risk`, `Commitment`, `Incident`, statuses as IRIs |
| `glossary` | SKOS profile: `skos:Concept` with `cb:definedBy` (owner), `cb:definitionScope` (department) |

`org` + `core` form the **entity spine**; everything else hangs off it.

### 4.6 Instance side

**Files (in the instance's config directory):**

```toml
# polargraph.types.toml
[registry]
url = "https://types.contxtbroker.ai"
# private registries use the same protocol:
# [[registry.private]]  prefix = "com.acme"  url = "https://types.acme.internal"

[types]
"io.github.contxtbroker/org"  = "^1.2"
"io.github.contxtbroker/work" = "~1.0"
```

```toml
# polargraph.types.lock (generated; commit it)
[[package]]
name    = "io.github.contxtbroker/org"
version = "1.2.3"
hash    = "sha256:9f2c…"
deps    = ["io.github.contxtbroker/core@1.0.4"]
```

**Storage.** Each installed version is a `TypeGraph`:

```
https://types.contxtbroker.ai/pkg/io.github.contxtbroker/org@1.2.3
```

The active version pointer is a quad in `urn:pg:graph:meta`:

```turtle
<urn:pg:types/io.github.contxtbroker/org> cb:activeVersion
    <https://types.contxtbroker.ai/pkg/io.github.contxtbroker/org@1.2.3> .
```

Because the pointer is bitemporal, "which schema was active on June 3" is a
time-travel query. Schema-level queries (shapes, render styles) read the union
of **active** type graphs only.

**CLI (`polargraph types …`, part of `polargraphd` or a separate binary):**

| Command | Action |
|---------|--------|
| `add <pkg>@<range>` | Edit manifest, resolve, update lock |
| `install` | Fetch by hash, verify, stage (not active) |
| `diff <pkg> <from> <to>` | Term-level and shape-level diff |
| `check` | Dry-run: validate live data against staged shapes; report violations by graph |
| `upgrade <pkg>` | install → check → migrate → activate → rematerialize → rebuild registry |
| `rollback <pkg>` | Re-point to the previous installed version (no data rewrite unless migrations ran) |
| `list` | Installed, active, and staged versions |

**Server RPCs:** `InstallTypePackage`, `CheckTypePackage`,
`ActivateTypeVersion`, `RollbackTypePackage`, `ListInstalledTypes`.

**Upgrade semantics.**
- Resolution fails on incompatible major versions of a shared dependency (no
  side-by-side versions — term IRIs are shared).
- Migrations run in a wire transaction per affected graph; the check step
  reports estimated quad rewrites first.
- Rollback after a migration requires a reverse migration or a restore to the
  pre-upgrade `as_of_tx_time`; the CLI refuses rollback past a migration
  without `--force-time-travel-restore`.

### 4.7 "Published = rendered"

The renderer (WS5) resolves styles only from active type graphs. A node whose
most specific type has no published package renders with a generic shape and
an **"unpublished type"** badge. Strict mode (`[render] require_published =
true`) refuses to render it. Private registries count as published within
that instance. (See open question Q3.)

---

## WS5. D2 rendering (`polargraph-render`)

### 5.1 Crate

Pure library (no I/O), consistent with embeddability. Input: a subgraph
(quads + RDF-star annotations) and a resolved style set. Output: D2 source.

```rust
pub struct RenderRequest {
    pub quads: Vec<Quad>,
    pub annotations: Vec<EdgeAnnotation>,
    pub styles: StyleSet,               // resolved from active TypeGraphs
    pub strategy: Strategy,
    pub options: RenderOptions,         // direction, max_nodes, show_confidence, …
}

pub enum Strategy {
    Flat,
    GroupByType,
    GroupByGraph,                       // one container per named graph
    Containment,                        // uses cbr:containerBy predicates
    Diff { base: GraphId, proposal: GraphId },
}
```

### 5.2 Behaviour

- **Identifiers.** D2 keys are short stable IDs (base32 of the first 8 bytes
  of `NodeId`, collision-checked); labels come from `cbr:labelTemplate`.
  Escaping for D2 reserved characters is centralized.
- **Confidence.** With `show_confidence`, edges below 1.0 render dashed with
  opacity proportional to confidence.
- **Provenance.** Tooltips carry graph, attributed agent, and review status.
- **Diff rendering.** Added quads green, retracted red, conflicts amber — the
  review UI for proposals.
- **Time travel.** Any render request accepts `as_of_valid_time` /
  `as_of_tx_time`.
- **Limits.** `max_nodes` (default 150) with a "+N more" collapse node per
  container, to keep diagrams legible.

### 5.3 Endpoint and runtime

`POST /render` with one of `{cypher, sparql, graph, subject+depth}` plus
`strategy`, `view`, `format`.

- `format = d2` (default): returns D2 source.
- `format = svg`: requires a `d2` binary configured
  (`[render] d2_binary = "/usr/local/bin/d2"`); the server shells out with a
  timeout. D2 is written in Go, so there is no in-process Rust renderer.
- The management UI renders D2 client-side via D2's WASM build, so SVG output
  on the server is optional.

### 5.4 Views as render presets

Existing `View` structs become render presets: `node_filter` and
`visible_predicates` select the subgraph; `edge_presentations` override
labels; a new `strategy` field picks the layout. Views are stored as quads in
`urn:pg:graph:meta` so they are versioned and shareable.

### 5.5 Management UI

- Replace the Mermaid schema diagram with a D2 render of active type graphs.
- Add a **Render** tab: query box + strategy picker + D2 canvas.

---

## WS6. Change feed

### 6.1 `Subscribe` RPC

Server-streaming, built on the existing WAL stream but filtered and
authorized per caller.

```protobuf
message SubscribeRequest {
  repeated string graphs      = 1;   // empty = all visible
  repeated string predicates  = 2;
  repeated string types       = 3;   // subjects whose rdf:type matches
  uint64 resume_from_seq      = 4;   // 0 = now
  bool include_values         = 5;
}

message ChangeEvent {
  uint64 seq            = 1;
  int64  commit_ts      = 2;
  string graph          = 3;
  ChangeKind kind       = 4;         // INSERT | CLOSE_VALID_TIME | DROP_GRAPH | PROMOTE
  Quad quad             = 5;
  string user_id        = 6;
}
```

- ACL applied per event using the caller's graph bitmap.
- Resume tokens are WAL sequence numbers; if the requested seq is older than
  WAL retention, the server returns `OUT_OF_RANGE` and the client re-syncs.
- REST: `GET /subscribe` as Server-Sent Events.

### 6.2 Consumers

| Consumer | Uses |
|----------|------|
| Renderer / UI | Live diagram updates |
| Embedding worker | Re-embed changed labels, chunks |
| Materializer | Incremental OWL RL (WS10) |
| Notification service | "Commitment due", "your service's dependency changed" |
| Search cache | Invalidate context-assembly caches |

---

## WS7. Ingestion

### 7.1 Ingestion ladder

| Level | Stored | Default for |
|-------|--------|-------------|
| 0 Blob | Content-addressed bytes + metadata quads | Attachments, archives |
| 1 Envelope | `Record` node, provenance, `rec:mentions` links | Every record |
| 2 Retrievable | Chunks with spans + embeddings | Conversations, documents, plans, tickets |
| 3 Claims | Extracted decisions/facts/commitments in a `ProposalGraph`, each pointing at a chunk span | Records whose type policy lists extractors |
| 4 Promoted | Approved claims merged into living state | Only after `PromoteGraph` |

The level for each record type comes from `ingest.ttl` in its type package
(WS4.3), so granularity is a published, versioned decision.

### 7.2 Pipeline

Workers run outside the engine (keeps the core free of model calls) and
communicate through the engine's RPCs plus `Subscribe`.

```
connector ──► normalize ──► store ──► chunk ──► embed ──► link ──► extract ──► propose
  (source)    (Record)     (blob +   (spans)   (vectors) (mentions) (claims)   (ProposalGraph)
                           envelope)
```

| Stage | Detail |
|-------|--------|
| Connector | Pulls from source system; emits raw payload + source IDs + ACL hints |
| Normalize | Maps to a `rec:` type; derives `import_id` from `(source, source_id, content_hash)` for idempotency |
| Store | Blob via out-of-line storage (WS1.3); envelope quads into a `RecordGraph` per source |
| Chunk | Structure-aware: speaker turns for conversations, headings for docs, fields for tickets. Target 300–800 tokens. Chunk IRI `rec/<id>#c<n>`, span `[start,end)` in chars |
| Embed | Model per vector space from the registry's `VectorSpaceDef`; batch via `BatchInsertVectors` |
| Link | Entity mentions: exact external-ID match → value index (`POS` on value-hashed keys, §1.2); name match → trigram; ambiguous → vector over candidate set (`SearchVectorInSet`). Confidence ≥ 0.9 writes `rec:mentions` directly into the record graph; lower becomes a proposal |
| Extract | LLM with structured output. The **JSON schema is generated from SHACL shapes** of the target types, so outputs are valid-by-construction for field kinds and enums |
| Propose | `ProposeGraph` with `cb:ModelAgent` provenance and per-fact `cb:sourceSpan` + `cb:confidence` |

**Re-ingestion.** Same `import_id` → the record graph is replaced (bitemporal
close + rewrite); its prior proposals are marked `cb:Superseded` unless
already approved.

### 7.3 Entity resolution

The entity spine only works if "Acme", "ACME Inc." and CRM account `0014…`
resolve to one node.

1. **Candidate generation** (per new or changed entity):
   - exact `ExternalId` match (value index, §1.2) — deterministic merge
   - normalized-name trigram similarity above threshold
   - embedding similarity of label + key properties
   - neighbourhood overlap (Jaccard of linked accounts/contacts/services)
2. **Scoring**: weighted combination; weights per type in `ingest.ttl`.
3. **Decision**:
   - deterministic matches → `owl:sameAs` in the approved graph automatically
   - probable matches → review queue (proposal with both sides rendered side by side)
   - below threshold → nothing
4. **Canonical representative**: the node with the most authoritative
   `ExternalId` (system priority list in `core`), used for display and new edges.
   The existing `eq-sym`/`eq-trans` rules propagate identity.
5. **Unmerge** is a supported operation: close the `sameAs` quad's valid time;
   history shows the period during which they were considered one entity.

### 7.4 Connector priorities

Driven by what gives the most cross-department value first:

| Order | Source | Contributes |
|-------|--------|-------------|
| 1 | CRM | Customers, accounts, contacts, contracts (IDs + key fields), owners |
| 2 | Service catalog / code hosting | Services, systems, repos, owning teams, dependencies |
| 3 | Ticketing (CS + eng) | Tickets as records; links to accounts, services, features |
| 4 | Meetings / chat | Conversations as records; decisions and commitments as claims |
| 5 | Billing / ERP | **References only**: account IDs, plan, renewal date — not revenue lines |
| 6 | Docs / wiki | Documents as records; glossary candidates |

---

## WS8. LLM interface

### 8.1 MCP server (`contxtbroker-mcp`)

A separate service (Rust, reusing `polargraph-core` types; see Q6) that talks
to `polargraphd` over gRPC. Every call carries the end user's identity, so
graph ACLs apply to what the model can see.

| Tool | Signature (abridged) | Notes |
|------|----------------------|-------|
| `search` | `(query, types?, graphs?, as_of?, k?)` | Hybrid: vector + trigram + exact ID, fused by reciprocal rank |
| `describe` | `(entity, depth=1, as_of?, include_records=3)` | Entity card: key properties, owners, top relations, recent records |
| `neighbors` | `(entity, predicates?, direction?, limit)` | Typed expansion |
| `find_records` | `(entity, query?, since?, limit)` | Records mentioning an entity, ranked by vector similarity to `query` within the mention set |
| `query` | `(sparql \| cypher, as_of?)` | Read-only; approved graphs by default |
| `impact` | `(entity, max_hops=3)` | Precomputed traversal over `dependsOn`/`uses`/`governedBy` to customers, contracts, commitments |
| `explain_fact` | `(s, p, o)` | Provenance chain: graph, agent, review, source span text |
| `list_types` / `describe_type` | `(package?)` / `(class)` | From active type graphs, incl. shapes |
| `propose` | `(assertions[], sources[], rationale)` | Always creates a `ProposalGraph`; returns validation report |

No tool writes to approved graphs.

### 8.2 Context assembly

The core value for LLMs: a complete, current, permission-filtered subgraph
plus the right passages, within a token budget.

1. **Seed**: explicit entity mentions in the request (linker from WS7) ∪
   `search` top-k.
2. **Expand**: typed hops. Each predicate carries a traversal weight in its
   type package (`cbi:contextWeight`), e.g. `owns` 0.9, `dependsOn` 0.8,
   `mentions` 0.3. Expansion stops at a cumulative-weight floor.
3. **Filter**: ACL bitmap; approved graphs only unless `include_proposed`;
   `as_of` applied uniformly.
4. **Rank**: personalized PageRank from the seeds over the expanded subgraph,
   boosted by recency and review status.
5. **Attach text**: for top entities, the best-matching chunks from
   `find_records`.
6. **Pack**: greedy by score into the budget, reserving ~40% for passages.
7. **Serialize**: compact, prefix-abbreviated, with provenance footnotes.

```
# context as_of 2026-09-29 · graphs: approved/eng, approved/sales
org:AuthService  owner=@team-identity  dependsOn=[org:UserDB, org:Kafka]
  exposes=[org:APIv1 (deprecated 2026-03), org:APIv2]
org:Acme  account=SF-0042  renewal=2027-01-15  uses=[org:APIv1]
  commitment[c1]: "SSO GA by Q2 2027" (sales call 2026-08-12, reviewed)
passages:
  [c1] …we told Acme SSO would be GA before their Q2 renewal… (rec/meet-8812#c4)
```

8. **Cache**: keyed by `(seed set, as_of, ACL bitmap hash)`; invalidated by
   `Subscribe` events touching included nodes.

**Budget targets**: p95 ≤ 300 ms for a 4K-token context at company scale
(§ Capacity).

### 8.3 Evaluation harness

- Golden question sets per department ("Who owns X?", "What did we promise
  Acme?", "Is it safe to deprecate APIv1?") with expected entities and facts.
- Metrics: fact recall/precision in packed context, stale-fact rate
  (superseded facts included), ACL leak rate (must be 0), latency.
- Runs in CI against a synthetic `cb-bench` dataset (see § Capacity).

---

## WS9. Collaboration UX

### 9.1 Bidirectional D2 editor

- The canvas renders D2 from WS5; edits are captured as D2 source changes.
- The server parses **only the dialect the renderer emits** (keys, labels,
  edges, style overrides, containers) — a small hand-written parser, not a
  full D2 implementation. Unknown constructs are preserved but ignored.
- A D2 diff against the last render becomes quad adds/retractions.
- **Human edits go through the same proposal flow** as LLM writes. Owners get
  auto-approval via the target graph's promotion policy, so the common case
  still feels immediate.
- Style-only edits (colour, position hints) become instance-level `cbr:`
  overrides in a per-user or per-view presentation graph, never in approved
  data.

### 9.2 Entity pages

One page per entity: key properties, owners, relation panels grouped by
predicate, a D2 neighbourhood diagram, linked records (ranked), open
proposals touching the entity, and a history timeline (bitemporal).

### 9.3 Glossary

SKOS concepts from the `glossary` package, each with owner, scope, and
competing definitions per department shown side by side. Linking a metric or
report to a concept makes the definition travel with it into LLM context.

### 9.4 Review queue

Proposals grouped by target graph and owner, sorted by impact (number of
approved facts touched, commitments involved). Batch approve/reject with
keyboard navigation; diff renders from WS5.

---

## WS10. Maintenance and feedback

### 10.1 Incremental materialization

- Move from full forward-chaining runs to **DRed (delete and re-derive)**:
  on retraction, over-delete derived facts depending on it, then re-derive
  those still supported.
- One `DerivedGraph` per approved graph (plus a cross-graph derived graph for
  rules whose premises span graphs).
- Triggered by `Subscribe` events; batched per commit window (default 1 s).
- Type-package upgrades trigger a scoped re-run only for rules whose schema
  premises changed.

### 10.2 Staleness

- `cb:reviewDue` per fact class (e.g. owners every 90 days, commitments at
  due date). Overdue facts are flagged in context assembly output.
- Supersession never deletes; `vt_end` + `cb:supersedes` keep the trail.

### 10.3 Usage and feedback

- Record which facts and passages were packed into contexts and whether the
  answer was accepted or corrected.
- Store counters in a dedicated `STATS` CF (not as triples) to avoid MVCC churn.
- Feed back into ranking (frequently useful facts rank higher) and into the
  review queue (heavily used, unreviewed facts get reviewed first).

---

## Capacity and performance

### Per-quad cost (after WS1 + WS2)

| Item | Raw bytes |
|------|-----------|
| Relation quad: 8 CFs × (48-byte key + 33-byte value) | ~650 |
| Short property quad (≈24-byte payload) | ~710 |
| Long text property | ~650 + text stored **once** in `BLOB` |
| Trigrams for a 40-char label (~38 keys × 27 bytes) | ~1,000 |
| Vector, 384-dim f32 + HNSW links | ~2,000 (int8 quantized: ~900) |

RocksDB block compression plus shared key prefixes typically cut the index
bytes 2–3×; vectors don't compress.

### Per-object estimates (compressed)

| Object | Composition | Size |
|--------|-------------|------|
| Living entity | ~20 quads, 2–3 trigram-indexed fields, 1 vector | ~8 KB |
| Record (conversation/doc) | envelope + mentions ~20 quads; 20 chunks × 3 quads; text in `BLOB`; 20 chunk vectors; ~15 claim quads | ~80 KB (~50 KB with int8 vectors) |
| Plan as record (this plan) | same as record | ~80 KB |
| Plan promoted to full structure | ~600 quads | ~200 KB |

### Scenarios

| | Team (~50 people) | Company (~1,000 people) |
|---|---|---|
| Living entities | 20K | 200K |
| Records (a few years) | 100K | 2M |
| Quads | ~10M | ~195M |
| Chunk vectors | 2M | 40M |
| Disk | ~9 GB | ~165 GB (~100 GB int8 + doc-level vectors for old records) |
| HNSW RAM (memory mode) | ~4 GB | too large — mmap + quantization required |

Records, and specifically their chunk vectors, dominate. Two levers matter
most: **vector quantization** (new engine work, see below) and **tiered
embedding** (chunk-level vectors for recent/active records, one document-level
vector for records older than a configurable age).

### Expected hot spots

| Hot spot | Why | Mitigation |
|----------|-----|------------|
| High-degree entities | A major customer is mentioned by 50K records; `OSPG` fan-out is large | `find_records` always ranks within the mention set via `SearchVectorInSet` with a limit; never materialize the full fan-out |
| Full re-materialization | Millions of derived facts per run | DRed (WS10.1) |
| Initial backfill | ~195M quads × 8 CF writes | N-Quads via SST bulk import (WS2.7), never through gRPC |
| Per-node ACL | `HashSet<NodeId>` per user at 10M+ nodes | Graph bitmaps (WS2.8) |
| Vector memory | 40M × 384 × 4 bytes | Quantization + mmap + tiering |
| Compaction | 8× write amplification | Tune per `docs/scaling.md`; separate compaction threads for record CFs later |

### New engine work implied

- **int8 / PQ vector quantization** in `hnsw.rs` with re-ranking on full
  vectors from mmap. Not in the current roadmap; required for company scale.
- **`cb-bench` scenario** in `polargraph-bench`: generator for spine entities,
  records with chunks, mention edges with realistic skew (Zipf), proposals, and
  ACL graphs.

### Performance targets (company scenario, single node, NVMe)

| Operation | Target |
|-----------|--------|
| Load one graph (1K quads, `GSPO` scan) | p50 ≤ 2 ms |
| `describe(entity)` across visible graphs | p95 ≤ 10 ms |
| Hybrid search, k=20 | p95 ≤ 25 ms |
| Context assembly, 4K tokens | p95 ≤ 300 ms |
| Promote a 500-quad proposal incl. SHACL | p95 ≤ 200 ms |
| Incremental materialization lag | ≤ 2 s after commit |
| Sustained ingestion | ≥ 50 records/s through the full pipeline (model latency excluded) |

---

## Milestones

| Milestone | Contents | Exit criteria |
|-----------|----------|---------------|
| **M1 Foundations** | WS1 all (incl. term identity, §1.6); WS2 key layout + migration v3, Datalog/SPARQL graph terms, N-Quads/TriG | W3C SPARQL dataset subset passes; bench regression within budget; existing tests green on migrated data |
| **M2 Trust** | WS2 ACL + graph RPCs; WS3 all | Proposal → validate → promote works end to end with provenance; SHACL core subset passes |
| **M3 Types + render** | WS4 registry repo, seed packages, instance CLI; WS5 renderer + UI tab | Install/upgrade/rollback `org` package on a populated instance; diagrams render from active types |
| **M4 LLM** | WS6; WS8 MCP + context assembly + eval harness | Golden-question recall ≥ 0.8 on `cb-bench`; zero ACL leaks |
| **M5 Ingestion** | WS7 pipeline, entity resolution, CRM + ticketing connectors | 100K records ingested; ER precision ≥ 0.95 on labelled set |
| **M6 Collaboration + upkeep** | WS9; WS10; quantization | Editor round-trips emitted dialect; DRed lag within target; company-scale bench within targets |

---

## Open design questions

| # | Question | Leaning |
|---|----------|---------|
| Q1 | Should record graphs be one per record or one per source/day? | One per record (clean replace/drop); revisit if graph count exceeds ~10M |
| Q2 | Tenant isolation: named graphs in one DB, or one DB per tenant? | Graphs for departments; separate DBs for separate customers of ContxtBroker |
| Q3 | How strict is "published = rendered"? | Badge by default, strict mode opt-in; private registries count as published |
| Q4 | Server-side SVG rendering? | Optional via `d2` binary; UI uses WASM |
| Q5 | Embedding model and dimension defaults | 384-dim default for chunks; allow per-space override; decide after `cb-bench` recall tests |
| Q6 | MCP server language | Rust for type reuse and single-binary deployment; TS acceptable if SDK velocity matters more |
| Q7 | Default promotion policy for human edits | Auto-approve for owners of the target graph; review for everyone else |
| Q8 | `sh:sparql` constraints | Defer; revisit when a seed package needs one |
| Q9 | Example data in public packages | Must be synthetic; CI scans for emails/phone numbers/known customer names |
| Q10 | Registry curation tiers | `core` (maintained), `verified` (reviewed orgs), `community` (anyone); catalog shows tier |

---

## Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Key-layout migration corrupts or slows existing deployments | High | Offline SST path, backup before migrate, dry-run + checksum comparison of quad counts |
| Review queue fatigue — humans stop reviewing | High | Confidence thresholds, owner auto-approval, batch review, impact-sorted queue |
| Extraction quality too low to trust | High | Shape-derived JSON schemas, span-grounded claims, eval harness gating model/prompt changes |
| Ontology sprawl in the open registry | Medium | Curation tiers, strong seed packages, semver discipline enforced in CI |
| Write amplification hurts ingestion | Medium | Bulk SST path for backfill; measure and tune compaction |
| Scope — ten workstreams | High | Critical path first (WS1→WS2→WS3→WS8); ship M2 + M4 before broad ingestion |
