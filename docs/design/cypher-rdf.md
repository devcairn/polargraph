# Cypher over RDF — labels as `rdf:type`, names as IRIs, and Cypher's future — design note

Status: decisions A–F **approved** (Mark, 2026-10-01) with the hard
requirement in §2.5 and G2 for the legacy conversion (§2.4). **PR 1 built**
on `db/cypher-rdf` (vocabulary, canonical names, `ConvertLegacyData`,
`rdf:type` labels, change-log type index; upgrade guide
`docs/upgrade-cypher-rdf.md`). PR 2 (Cypher write deprecation, SDKs) next.

**As built (PR 1).** Bare names in Cypher are left for the store to place
under the base; only `prefix:local` names and labels are expanded at compile
time (plan cache keyed by the vocabulary fingerprint). The type index is
keyed by class node (`iri_to_node_id(class IRI)`) rather than the IRI
string, and recomputes each touched subject's classes from the store rather
than replaying assert / close events. Prefix names that are URI schemes
(`http`, `urn`, …) are rejected.

## 1. How Cypher stores things today

| Cypher | Stored as | Where it's used |
|---|---|---|
| Label `(n:Person)` | **Text property** `n __type "Person"` | Cypher MATCH (value filter on `__type`); node type registry and schema-aware pruning (`eval.rs`, `planner.rs`: domain/range = `__type`); server **type cache** (`SearchVectorFiltered` NodeType filter, `VectorSeedQuery` filter, `Subscribe` `types` filter, `HAS_ACCESS_TYPE` node ACL); management UI type list |
| Relationship type `-[:KNOWS]->` | Predicate string `"KNOWS"` | everywhere |
| Property key `n.name` | Predicate string `"name"` | everywhere |

The RDF side types nodes with **`rdf:type` relations to class IRIs**:
OWL 2 RL (`rdfs9`, `rdfs11`, …), SHACL (`sh:targetClass`, `sh:class`), and
SPARQL (`?x a ex:Person`). Consequences today:

- A node created with `CREATE (n:Person)` is **invisible** to SPARQL
  `?x a ex:Person`, to SHACL class targets and to OWL RL type inference; and
  RDF-imported `rdf:type` data is invisible to Cypher labels, the type
  registry, typed vector filters and `HAS_ACCESS_TYPE` grants.
- **Exports produce invalid RDF** for Cypher data: predicates are written as
  `<{predicate}>`, so `__type`, `name`, `KNOWS` become relative IRIs
  (`<name> "Ann"`) that N-Triples / Turtle parsers reject. The same applies
  to any bare predicate written via `Insert` / REST `/insert` / Datalog.

So the fix has two halves: **types** (labels ↔ `rdf:type`) and **names**
(bare labels / relationship types / property keys ↔ IRIs).

## 2. Proposal

### 2.1 A store-wide vocabulary

- A **vocabulary base** IRI per store (default `urn:pg:vocab:`), plus a
  **prefix map** (`ex = http://ex/`, …). Both live in the **system graph**
  and are managed at runtime by RPCs (§2.5); there is no config-only setting.
- A bare name `Person` ↔ `<vocab_base>Person`. A name containing `:` is a
  CURIE (`ex:Person` → `http://ex/Person`) or a full IRI. In Cypher such
  labels go in backticks — `` (n:`ex:Person`) ``,
  `` (n:`http://schema.org/Person`) `` — because `:` already separates
  multiple labels.
- Reverse (result keys, relationship and property names in responses): an
  IRI under the vocabulary base renders as the bare name, one under a
  declared prefix as a CURIE, anything else as the full IRI.

### 2.2 Labels become `rdf:type`

- `CREATE (n:Person)` / `MERGE (n:Person …)` write
  `n rdf:type <vocab:Person>` (a relation, in the write's graph). (Cypher
  here has no `REMOVE` / `SET n:Label` / `labels()` today; none are added.)
- `MATCH (n:Person)` matches `n rdf:type <vocab:Person>` — a pattern, not a
  post-filter, so it can drive the scan. Optionally including
  `rdfs:subClassOf` instances (decision D).
- Everything keyed on `__type` switches to `rdf:type`: the type cache (keyed
  by class IRI), the node type registry (type names become class IRIs via the
  same mapping, so `RegisterNodeType("Person")` describes `<vocab:Person>`),
  schema-aware pruning, `HAS_ACCESS_TYPE`, `Subscribe` `types`, typed vector
  filters, the UI type list.

### 2.3 Names become IRIs (decision B)

Two ways to make exports and SPARQL see one spelling:

- **B1 — store IRIs** (recommended): predicates are IRIs in storage. Cypher
  (and the bare-name APIs: REST `/insert`, Datalog patterns, Cypher-style
  property maps) expand bare names through the vocabulary on the way in and
  compact them on the way out. RDF paths (SPARQL, imports, exports, SHACL,
  OWL) need no change. Existing bare predicates are renamed once (see
  migration): the predicate intern table maps id → string, so a rename is a
  **metadata update, not a quad rewrite** — unless the IRI is already
  interned separately, in which case those quads are merged.
- **B2 — map at the RDF boundary**: storage keeps bare names; every RDF
  consumer (SPARQL translate, imports, exports, SHACL path IRIs, OWL RL
  vocabulary) maps `<vocab_base>x` ↔ `x`. No migration, but every RDF path
  carries the mapping forever and must never miss it.

### 2.4 Legacy-data conversion (decision G2: operator-triggered, online)

There is **no automatic migration at startup** (Mark, 2026-10-01, G2). The
operator first sets the vocabulary base (`SetVocabularyBase`, plus any
prefixes), then triggers the one-time conversion explicitly with
`ConvertLegacyData` (REST `POST /vocabulary/convert`; `dry_run` reports
what would change):

1. **Predicate renames (B1).** Every interned bare predicate `P` becomes
   `base + P`. Normally this is a rename of the intern-table entry — a
   metadata update, no quad rewrite. If `base + P` is already interned
   separately, the live quads of `P` are copied to it and closed under `P`,
   and `P`'s entry is renamed to `__legacy__/P` (its history stays
   queryable there).
2. **Labels.** Every live `__type "X"` property (all graphs) becomes
   `s rdf:type <expand(X)>` in the same graph with the same valid time; the
   property is closed (bitemporal — history keeps it).
3. Caches (type, access, registry) are rebuilt; class IRIs are recorded in
   the IRI dictionary.

The node type registry keeps the names it was given (`Person`,
`ex:Widget`); they resolve through the vocabulary when used, so registry
entries need no rewrite.

**Idempotent and resumable**: both steps work from what is still legacy
(bare entries in the intern table, live `__type` properties), in chunked
commits; re-running after an interruption continues, and running it again
when nothing is left is a no-op.

**Pending state is visible, not silent.** While legacy data exists:

- the server logs a **startup warning** with the counts and the command to
  run;
- `GetVocabulary`, `ShowStats` and the management `/health` JSON report
  `legacy_bare_predicates` / `legacy_type_labels` counts and
  `legacy_conversion_pending: true`.

**The window** (documented in the upgrade guide): from the upgrade until the
conversion has run, data stored under bare predicate names and `__type`
labels **is not reachable by bare name** — bare names now resolve under the
vocabulary base, and Cypher labels match `rdf:type`. It stays reachable by
its stored name, e.g. in exports.

### 2.5 Hard requirement — runtime type and vocabulary changes

Registering new types, their properties and their relationships must work
on a running server, **with no restart or reload**, and be usable
immediately from Cypher, SPARQL and SHACL. This covers
`RegisterNodeType` / `RegisterEdgeType`, plain `rdf:type` writes with a new
class (any write path: `Insert`, `ApplyChanges`, Cypher, SPARQL Update,
imports), later WS4 type-package installs, and prefix / vocabulary-base
changes. Nothing runs at startup beyond loading the vocabulary and caches;
the one-time legacy conversion (§2.4) is triggered by the operator on a
running server.

How the design meets it:

| Change | Mechanism | Visible to |
|---|---|---|
| New prefix / vocabulary base | `PutPrefix`, `RemovePrefix`, `SetVocabularyBase`, `GetVocabulary` RPCs (+ REST `/vocabulary`). Stored as triples on `urn:pg:vocab` in the system graph; the in-memory vocabulary is swapped atomically on commit (replicas refresh from the change log). Changing the base later affects only future expansions / compactions; stored IRIs are never rewritten. | Cypher immediately (names resolve per request); SPARQL / SHACL work on full IRIs and don't need it |
| `RegisterNodeType` / `RegisterEdgeType` | Already runtime: registry triples + in-process cache update. Type names go through the current vocabulary to class / property IRIs. Replicas refresh the registry cache from the change log. | Schema hints, validation, Cypher, `ListPredicatesBetween` |
| `rdf:type` with a new class, on any write path | The type cache (typed vector filters, `Subscribe` types, `HAS_ACCESS_TYPE`) is driven by **one in-process consumer of the change log** instead of per-RPC hooks — today only `Insert` and `ApplyChanges` update it, so Cypher writes and replicas miss changes. | Cypher labels and SPARQL / SHACL read the store directly — immediate |
| WS4 type packages | Installs are ordinary quad writes + registry RPCs, so the rows above apply | Everything |

**Acceptance test** (gRPC, on a running server, no restart): put prefix
`ex = http://ex/`, register node type `` `ex:Widget` `` with a property and
an edge type, then (1) Cypher `` CREATE (w:`ex:Widget` {name: "w1"}) `` and
`` MATCH (w:`ex:Widget`) `` returns it; (2) SPARQL `SELECT ?w WHERE { ?w a
ex:Widget }` returns it; (3) a SHACL shape with `sh:targetClass ex:Widget`
validates it; (4) a typed vector filter and `Subscribe` `types` see it; (5)
the same holds for a class first introduced by a plain `rdf:type` insert.
Run on a replica as well for the change-log-driven caches.

**Finding while building (2026-10-01).** Canonicalising bare predicate
names at the storage layer (B1) makes data **already stored** under a bare
predicate (`name`, `KNOWS`, any Cypher / REST `/insert` data) unreachable by
that name until the predicate-rename migration (§2.4 step 2) has run. So
canonicalisation and the rename migration must ship together and the
migration must complete before the server serves requests — which makes G
blocking for PR 1.

**Decision G — G2** (Mark, 2026-10-01): no startup migration; the operator
sets the base by RPC and triggers the conversion (§2.4). Considered: G1, an
initial-value setting used by an automatic startup migration; G3, migrate
with the default base and re-map later.

## 3. Should we keep Cypher?

### Evidence

| | |
|---|---|
| Code | `cypher.rs` 4 516 + `aggregation.rs` 854 lines = **~36 %** of `polargraph-query` (14 796); ~700 lines of server handlers (`CypherQuery`, `CypherQueryStream`, `CypherWrite`); 3 REST endpoints |
| Tests | 87 + 21 unit tests, ~22 gRPC tests, e2e cases |
| Clients | Python / Go / JS SDKs: ~530 lines of Cypher API; management UI: **none** (uses Datalog) |
| Plan dependencies | Cypher `USE GRAPH` (done, step 5); WS5 renderer `POST /render` accepts `cypher`; WS8 MCP `query` tool takes `sparql \| cypher` — neither built yet |
| Unique capability | `VECTOR_NEAR` (ANN-seeded graph queries) exists **only** in Cypher (SPARQL has no vector function; `VectorSeedQuery` is the RPC form) |
| Consistency cost so far | Graph scoping, `USE GRAPH`, ACL, write-graph semantics and authors each needed separate Cypher work; two real bugs (Cypher `DELETE` closing named-graph facts in the default graph; writes outside graph scope) were Cypher-only. Known gaps remain: property filters read every graph, text predicates don't use the trigram index, labels aren't RDF types |
| Model fit | Property-graph names (labels, rel types, keys) are bare strings; RDF needs IRIs. Edge properties map to RDF-star annotations only partly. Reads map well (MATCH ≈ BGP); writes (CREATE/MERGE/SET/DELETE) are where graph, ACL, typing and multi-value semantics diverge |
| Value | LLMs and app developers write Cypher fluently and compactly; for an MCP query tool it's the friendlier language. SPARQL is the standard for RDF data and already covers datasets, updates, federation-style patterns |

### Options

1. **Keep as-is** (full read + write): no breakage; every engine feature
   (graphs, ACL, changesets, validation, typing) keeps needing a Cypher
   variant and tests; the write path keeps diverging.
2. **Keep a thin, read-mostly layer over RDF**: Cypher becomes a query
   syntax over the RDF model — labels = `rdf:type`, names via the
   vocabulary, MATCH/WHERE/RETURN/WITH/aggregations/`VECTOR_NEAR`/`USE GRAPH`
   kept. Writes are **deprecated**: kept one release with a warning, then
   removed in favour of `ApplyChanges` / SPARQL Update (one write path for
   ACL, authors, change feed, validation).
3. **Deprecate Cypher entirely**: remove after a deprecation release; SPARQL +
   Datalog only. Needs a SPARQL vector function (e.g. `pg:vectorNear`) to
   replace `VECTOR_NEAR`; SDK and MCP plans change to SPARQL only.

### Recommendation

**Option 2.** Keep Cypher as a read-mostly query language over the RDF
model, because it's the most usable query surface for LLMs and app
developers and carries `VECTOR_NEAR`; stop maintaining a second write path,
which is where almost all of the divergence and bugs have been. Do the
labels/names change (2.1–2.4) as part of it, so Cypher and SPARQL see the
same data.

## 4. Decisions needed

| # | Question | Recommendation |
|---|----------|----------------|
| A | Cypher's future | **Option 2** — read-mostly layer over RDF; writes deprecated for one release, then removed. |
| B | Names: store IRIs (B1) or map at the RDF boundary (B2) | **B1** — store IRIs; one rename migration (metadata-only in the common case), no mapping in RDF paths. Also applies to bare predicates written by REST `/insert` and Datalog. |
| C | Vocabulary base default | `urn:pg:vocab:`, changed at runtime via `SetVocabularyBase`; prefixes via `PutPrefix` (§2.5). Operators set a real namespace before the `__type` migration runs. |
| D | `MATCH (n:Person)` and subclasses | Exact `rdf:type` by default (fast, predictable); subclass instances via OWL RL materialization, which already derives `rdf:type` from `rdfs:subClassOf`. |
| E | Migration timing | Superseded by **G2**: online, operator-triggered `ConvertLegacyData` (resumable, dry run, visible pending state). Breaking for clients reading `__type` or bare predicate names — release note + upgrade guide. |
| F | Sequencing | Its own step before the plan's step 9; ships in two PRs: (1) vocabulary + labels/names + migration, (2) Cypher write deprecation + SDK/doc updates. |
