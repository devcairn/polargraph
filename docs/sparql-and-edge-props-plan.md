# Implementation Plan: Cypher Edge Annotation Filters + SPARQL 1.1 Endpoint

**Date**: 2026-06-27  
**Status**: Draft  
**Scope**: Two independent features; edge annotations first (small), SPARQL second (large)

---

## Feature 1: Cypher Edge Annotation Property Access

### Background

`MATCH (a)-[r:knows]->(b) WHERE r.since > 2020` should filter on an edge
annotation property stored in the `EPA` RocksDB column family. The plumbing
for this is almost entirely done — it just isn't wired together in the service
layer.

### Current state (what research found)

| Layer | Status |
|-------|--------|
| Lexer: `r.prop` tokenized correctly | ✅ Done |
| Parser: dot-property on edge var → `WhereClause::PropertyEq` | ✅ Done |
| Compiler: checks `edge_vars` set, routes to `EdgeAnnotationFilter` | ✅ Done |
| `CompiledQuery.edge_annotation_filters: Vec<EdgeAnnotationFilter>` | ✅ Done |
| `apply_edge_annotation_filters()` in `polargraph-query::cypher` | ✅ Done |
| Storage: `scan_edge_annotations`, `get_edge_annotation` on `TripleStore` | ✅ Done |
| **`apply_edge_annotation_filters()` called in `service.rs`** | ❌ Missing |
| `r.prop` projections in RETURN clause | ❌ Missing |
| `cypher_query_stream` wired the same way | ❌ Check |

The core fix is a single call insertion in `service.rs`. The remaining work is
edge property projections in RETURN (e.g. `RETURN r.since`) and ensuring the
streaming path has the same call.

### Data flow today vs. desired

```
Today:
  execute_query() → raw bindings
    → filter_bindings() (AC)
    → apply_value_filters()    (n.prop = val)
    → apply_text_filters()     (n.prop CONTAINS "x")
    → apply_aggregations()
    → build response

Desired:
  execute_query() → raw bindings
    → filter_bindings() (AC)
    → apply_value_filters()
    → apply_text_filters()
    → apply_edge_annotation_filters()   ← INSERT HERE
    → apply_aggregations()
    → build response
```

### Key types (no changes needed)

```rust
// cypher.rs — already exists, no changes
pub struct EdgeAnnotationFilter {
    pub var: String,       // "r" — the edge variable name
    pub predicate: String, // "since" — the annotation key
    pub op: ComparisonOp,  // Gt, Gte, Lt, Lte, Eq, Ne
    pub value: Value,      // the RHS literal value
}
```

`apply_edge_annotation_filters` already handles all `ComparisonOp` variants
for scalar `EdgeAnnotationValue::Scalar(v)`.

### Changes required

#### 1. `crates/polargraph-server/src/service.rs` — `cypher_query()`

After the `apply_text_filters()` call, add:

```rust
let filtered = polargraph_query::cypher::apply_edge_annotation_filters(
    filtered,
    &compiled.edge_annotation_filters,
    &snapshot,
)?;
```

Pass `snapshot_ts` to the storage call inside `apply_edge_annotation_filters`
(verify the function signature already accepts a `&Snapshot` which carries `ts`
— research confirmed it does).

#### 2. `crates/polargraph-server/src/service.rs` — `cypher_query_stream()`

Apply the same insertion in the streaming variant. Both paths share the same
filter chain; the stream version chunks and sends results incrementally but
filters the same way before chunking.

#### 3. Edge property projections in RETURN (e.g. `RETURN r.since`)

Currently, RETURN property projections (`prop_projections: Vec<(String, String)>`)
are resolved in the service via `snapshot.scan_by_subject_predicate(&node_id, prop)`.
This works for nodes because `NodeId` maps directly to a triple subject. For
edge variables, the binding value is the `EdgeId` reinterpreted as a `NodeId`.

Two sub-changes:

**a. In the compiler** (`cypher.rs`): When building `prop_projections`, record
whether the variable is an edge var. The cleanest approach is to add a parallel
list `edge_prop_projections: Vec<(String, String)>` to `CompiledQuery` — same
shape as `prop_projections` but for edges. In `compile()`, check `edge_vars`
when iterating RETURN projections and route accordingly.

**b. In the service** (`service.rs`): For each `(var, prop)` in
`edge_prop_projections`, reinterpret the binding `NodeId` as an `EdgeId` and
call `store.get_edge_annotation(edge_id, &prop, snapshot_ts)`. Serialize the
result value to JSON string and add to the binding row under key `r.since`.

#### 4. `crates/polargraph-query/src/cypher.rs` — text filters on edge annotations

Currently `TextFilter` (CONTAINS / STARTS WITH / =~) only applies to node
properties. If we want `WHERE r.label CONTAINS "foo"`, a similar routing
through `edge_vars` check is needed, producing an `EdgeTextFilter` variant.
**Defer this to phase 2** — the comparison operators (=, >, <, >=, <=, !=)
cover the common cases.

### Proto/REST changes

None required. This is a pure execution-layer change. The proto schema for
`CypherQueryRequest` / `CypherQueryResponse` is unchanged; the binding values
in the response already carry arbitrary string-encoded values.

### Tests to write

#### Unit tests in `crates/polargraph-query/src/cypher.rs`

1. `edge_annotation_filter_eq` — compile `WHERE r.weight = 5`, assert one
   `EdgeAnnotationFilter{op: Eq, value: Int(5)}` in `compiled.edge_annotation_filters`.
2. `edge_annotation_filter_gt` — `WHERE r.since > 2020`, assert `op: Gt`.
3. `edge_annotation_filter_node_prop_not_confused` — `WHERE n.age > 30` must
   produce a `ValueFilter`, not an `EdgeAnnotationFilter`.

#### Storage integration tests in `crates/polargraph-storage/tests/`

Already covered by existing `annotation.rs` tests.

#### gRPC integration tests in `crates/polargraph-server/tests/`

4. `cypher_edge_prop_filter_eq` — Insert two edges with `since` annotations
   (2019, 2022). Query `WHERE r.since > 2020`, expect only the 2022 edge in
   results.
5. `cypher_edge_prop_filter_no_match` — Filter that matches nothing returns
   empty bindings.
6. `cypher_edge_prop_return_projection` — `RETURN r.since` includes the
   annotation value in the response row.
7. `cypher_stream_edge_prop_filter` — Same as (4) via `CypherQueryStream`.

### Estimated effort

Small — 1–2 days. The hard work (storage, parsing, compilation, filter
function) is already done. This is wiring + projection support + tests.

---

## Feature 2: SPARQL 1.1 + SPARQL-star Endpoint

### Background

PolarGraph has no SPARQL support. SPARQL is the W3C standard query language
for RDF triple stores; SPARQL-star extends it to query edge annotations
(RDF-star). Adding a SPARQL endpoint makes PolarGraph interoperable with the
broad RDF/linked-data tooling ecosystem.

### Parser: `spargebra` crate

**`spargebra`** (crates.io: `spargebra`, part of the Oxigraph project) is a
standalone SPARQL 1.1 + SPARQL-star parser that emits an algebra AST without
pulling in a full triple store or evaluation engine. It parses:

- SELECT / CONSTRUCT / ASK / DESCRIBE queries
- SPARQL-star `<<?s ?p ?o>>` quoted triple syntax
- SPARQL Update (`INSERT DATA` / `DELETE DATA` / `MODIFY`)
- Property paths (`+`, `*`, `?`, sequences `a/b`, alternates `a|b`)
- FILTER, OPTIONAL, UNION, MINUS, SERVICE, GRAPH, VALUES

The API entry point is `spargebra::Query::parse(query_str, base_iri)` which
returns `Result<Query, ParseError>`. The `Query` enum has variants for
`SelectQuery`, `ConstructQuery`, `AskQuery`, `DescribeQuery`. Each contains
an `Algebra` tree built from types like `GraphPattern::Join`, `Filter`,
`LeftJoin`, `Union`, `TriplePattern`, `PathPattern`, etc.

`spargebra` has **no** evaluation engine — it is purely a parser/AST library,
which is exactly what we need.

### Architecture decision: new crate vs. extend polargraph-rest

**Option A: New `polargraph-sparql` binary crate**

Pros:
- Clean separation of concerns; polargraph-rest stays focused on REST
- Independent versioning and deployment
- Can be omitted from minimal deployments
- Avoids pulling `spargebra` (and any transitive deps) into polargraph-rest

Cons:
- Another port to expose and manage
- Two HTTP servers to configure in Helm/Docker

**Option B: Add `/sparql` to `polargraph-rest`**

Pros:
- Single HTTP server; single port; single binary
- REST gateway already has the gRPC channel, auth interceptor, and TLS setup
- Simpler Helm chart (no new Service/Deployment)

Cons:
- polargraph-rest currently has no query compilation logic — adding a
  SPARQL→Datalog compiler pulls in `polargraph-query` (and its deps) which
  breaks the clean "rest depends only on proto" architecture

**Recommendation: Option B with a thin compilation proxy.**

The SPARQL→Datalog compiler should live in a new library crate
`polargraph-sparql` (not a binary), and `polargraph-rest` depends on it.
`polargraph-query` already exists as a dep path; adding it to polargraph-rest's
Cargo.toml is a one-line change. The translation layer in `polargraph-sparql`
converts SPARQL algebra to gRPC `QueryRequest` / `CypherQueryRequest` structs
(the protobuf types), so polargraph-rest just calls `translate_sparql()` and
forwards the resulting request to its existing gRPC client.

This gives us one HTTP server while keeping the compiler in a testable library.

### Crate structure

```
crates/
└── polargraph-sparql/          ← NEW library crate
    ├── Cargo.toml              (deps: spargebra, polargraph-core, prost-generated proto types)
    ├── src/
    │   ├── lib.rs
    │   ├── translate.rs        SPARQL algebra → QueryRequest
    │   ├── paths.rs            Property path → Datalog rules
    │   ├── filter.rs           SPARQL FILTER → ValueFilter / TextFilter
    │   ├── results.rs          Bindings → SPARQL JSON/XML/CSV serializers
    │   └── error.rs            SparqlError enum
```

`polargraph-rest` adds:
```
crates/polargraph-rest/
└── src/
    └── sparql.rs               axum handlers for GET/POST /sparql
```

### SPARQL algebra → PolarGraph mapping

#### Basic graph patterns

A SPARQL triple pattern `?s :knows ?o` maps directly to a `Pattern`:

```
spargebra: TriplePattern { subject: Variable("s"), predicate: NamedNode("knows"), object: Variable("o") }
→ polargraph: Pattern::Relation { subject: Var("s"), predicate: Bound("knows"), object: Var("o") }
```

SPARQL uses IRIs (e.g. `<http://schema.org/name>`) as predicates. Strategy:
strip the IRI to its local name for predicate lookup, or store the full IRI as
the predicate string. **Phase 1**: use full IRI string — PolarGraph stores
predicates as arbitrary strings, so this works without changes. Document that
SPARQL clients should use full IRIs in INSERT and SPARQL queries consistently.

Variable binding: SPARQL variables (`?s`, `$s`) map 1:1 to Datalog variable
names. Blank nodes (`_:b1`) generate fresh variable names at translation time.

#### FILTER expressions

| SPARQL FILTER | PolarGraph equivalent |
|---|---|
| `FILTER(?age > 30)` | `ValueFilter { var: "age", pred: "age", op: Gt, val: Int(30) }` |
| `FILTER(regex(?name, "^A"))` | `TextFilter { var: "name", kind: Regex }` |
| `FILTER(contains(?name, "foo"))` | `TextFilter { kind: Contains }` |
| `FILTER(strstarts(?name, "A"))` | `TextFilter { kind: StartsWith }` |
| `FILTER(?x = ?y)` | post-filter: equality check between two bound variables |
| `FILTER(bound(?x))` | post-filter: check variable present in bindings |
| `FILTER(!bound(?x))` | requires OPTIONAL (left-join) — see below |

FILTER predicates that reference two variables (rather than a variable + literal)
require a post-processing step — iterate bindings and drop rows where the
condition fails. A `BinaryFilter { left_var, op, right_var }` type captures this.

#### OPTIONAL (LEFT JOIN)

PolarGraph's `execute_query` performs inner joins only. OPTIONAL requires
left-join semantics: return all bindings from the left side, with right-side
variables set to `null` (absent) when the optional pattern doesn't match.

**Phase 1**: Translate `OPTIONAL { ?s :prop ?o }` as a best-effort inner join
with documentation noting the semantic gap.  
**Phase 2**: Add `execute_left_join(left_query, right_query, snapshot)` to
`polargraph-query::datalog` — evaluate right side against each left binding,
keep left binding regardless, merge right bindings when they exist.

#### UNION

```
GraphPattern::Union(left, right)
→ execute_query(left) ++ execute_query(right), deduplicate
```

Evaluate each branch independently and concatenate results. Already supported
by running two `execute_query` calls and merging `Vec<Bindings>`.

#### Property paths

| SPARQL path | Translation |
|---|---|
| `:knows+` (one-or-more) | `Rule` with recursive TC + `execute_recursive` |
| `:knows*` (zero-or-more) | Same, plus identity binding for zero hops |
| `:knows?` (zero-or-one) | Inner join OR identity binding — union of both |
| `(:a/:b)` (sequence) | Two chained `Pattern`s |
| `(:a\|:b)` (alternate) | Two branches → UNION |
| `^:knows` (inverse) | Swap subject/object in the `Pattern` |
| `(:a/:b)+` (complex) | Unroll to Datalog rules recursively |

The `paths.rs` module in `polargraph-sparql` builds `Vec<Rule>` from path
expressions and hands them to `execute_recursive`. This mirrors how Cypher
`[r*]` already compiles to recursive Datalog.

#### Named graphs → Views

SPARQL `GRAPH <uri> { ... }` selects a named graph. Map `<uri>` to a
PolarGraph `ViewId` by treating the IRI as the view name. If no View with that
name exists, return `UNKNOWN_GRAPH` error. Phase 1 may omit named graph
support and return `NOT_SUPPORTED` for `GRAPH` patterns.

#### SPARQL-star: quoted triples

```sparql
<<?s :knows ?o>> :since ?year .
```

`spargebra` represents this as `GraphPattern::Extend` with a `TriplePattern`
inside a quoted triple expression. The translation:

1. The inner triple `?s :knows ?o` identifies an edge — look up the `EdgeId`
   from the hexastore (SPO lookup on `?s`, `:knows`, `?o` returns the edge UUID
   from the stored triple's `edge_id` field, if present).
2. The outer triple `<<?s ?p ?o>> :since ?year` → call `scan_edge_annotations`
   with the resolved `EdgeId`, filter by predicate `:since`, bind `?year` to the
   result value.

This requires a new evaluation strategy: resolve the quoted triple to an
`EdgeId` binding, then evaluate annotation patterns against it. A new
`EvaluationStep::EdgeAnnotationPattern` type in the translation layer captures
this — it is not a `VarPattern` but a post-join lookup.

**Phase 2** covers SPARQL-star end-to-end.

### SPARQL protocol

Two endpoints:

```
GET  /sparql?query=<encoded>&format=json
POST /sparql   (Content-Type: application/sparql-query — raw SPARQL body)
POST /sparql   (Content-Type: application/x-www-form-urlencoded — query=...)
```

Handler logic:

1. Extract query string from GET param or POST body (handle both content types).
2. Parse with `spargebra::Query::parse()`.
3. Translate algebra → `QueryRequest` via `translate.rs`.
4. Forward to gRPC (using the existing tonic channel in polargraph-rest).
5. Receive `Vec<Bindings>` results.
6. Serialize to requested format (content negotiation via `Accept` header).

#### Response formats

**`application/sparql-results+json`** (default):
```json
{
  "head": { "vars": ["s", "p", "o"] },
  "results": {
    "bindings": [
      { "s": { "type": "uri", "value": "urn:uuid:..." },
        "p": { "type": "uri", "value": "http://schema.org/knows" },
        "o": { "type": "uri", "value": "urn:uuid:..." } }
    ]
  }
}
```

PolarGraph `NodeId` values serialize as `urn:uuid:<uuid>` URIs. String `Value`
serializes as `{ "type": "literal", "value": "..." }`. Typed literals include
`"datatype"` per XSD.

**`application/sparql-results+xml`** — Same data, W3C XML format. Lower
priority; implement after JSON.

**`text/csv`** — RFC 4180, header row = variable names, one row per binding.
Easiest to implement; good for debugging.

**`application/sparql-results+json` for ASK** — `{ "boolean": true/false }`.

**CONSTRUCT / DESCRIBE** — Returns RDF (Turtle / N-Triples / JSON-LD). Defer
to phase 3; these require an RDF serializer.

### What we cannot support initially (honest gap list)

| Feature | Gap | Phase |
|---|---|---|
| `INSERT DATA` / `DELETE DATA` | SPARQL Update — polargraph writes go through typed Insert RPC, not RDF INSERT | Phase 3 |
| `DELETE WHERE` / `MODIFY` | Complex update semantics | Out of scope |
| `OPTIONAL` with full left-join semantics | No left-join in current evaluator | Phase 2 |
| Federated `SERVICE` | Requires outbound HTTP from query engine | Out of scope |
| OWL entailment / RDFS reasoning | No inference engine | Out of scope |
| SPARQL-star annotations | Requires EdgeId resolution step | Phase 2 |
| `DESCRIBE` / `CONSTRUCT` | Requires RDF serializer (Turtle, N-Triples) | Phase 3 |
| `GROUP BY` / `HAVING` | Cypher aggregation exists but not wired to SPARQL | Phase 2 |
| `VALUES` inline data | Feasible but low priority | Phase 2 |
| Named graphs (`GRAPH`) | Requires View name resolution | Phase 2 |

### Work breakdown by phase

#### Phase 1: Core SELECT queries

**Step 1: New `polargraph-sparql` crate**

- `Cargo.toml`: deps `spargebra = "0.3"`, `polargraph-core`, `polargraph-query`,
  proto-generated types
- `error.rs`: `SparqlError` enum (ParseError, TranslationError, UnsupportedFeature)
- `translate.rs`: `translate_select(SelectQuery) -> Result<QueryRequest, SparqlError>`
  - Walk `GraphPattern` recursively
  - Collect `TriplePattern`s → `VarPattern`s
  - `FILTER` → `ValueFilter` / `TextFilter`
  - `UNION` → two separate `QueryRequest`s (union result sets in handler)
  - `DISTINCT` flag → dedup bindings after evaluation
  - `LIMIT` / `OFFSET` → pass through to aggregation
- `results.rs`: `serialize_json`, `serialize_csv` functions
  - `NodeId` → `urn:uuid:<uuid>`
  - `Value::Text(s)` → `{ "type": "literal", "value": s }`
  - `Value::Int(n)` → `{ "type": "literal", "value": "n", "datatype": "xsd:integer" }`

**Step 2: Add `/sparql` to `polargraph-rest`**

- `sparql.rs`: two axum handlers (GET + POST)
  - Content-type negotiation for POST body
  - Accept header → response format
  - Call `translate_select()`, forward to gRPC, serialize
- Wire into `build_router()` in `main.rs`
- Auth: reuse existing `AuthInterceptor` (API key forwarded to upstream)

**Step 3: Basic property paths**

- `paths.rs`: `translate_path(subject, path, object) -> (Vec<VarPattern>, Vec<Rule>)`
  - Sequence: chain patterns
  - Alternate: emit two pattern groups → UNION
  - Plus/Star: emit recursive Rule, call `execute_recursive`
  - Inverse: swap subject/object

**Tests (Phase 1)**

Unit (in `polargraph-sparql`):
1. `translate_bgp_simple` — three-pattern BGP → correct `VarPattern`s
2. `translate_filter_comparison` — `FILTER(?age > 30)` → `ValueFilter`
3. `translate_filter_text` — `FILTER(contains(?n, "foo"))` → `TextFilter`
4. `translate_union` — two branches → two queries
5. `translate_path_plus` — `:knows+` → Rule with TC predicate
6. `serialize_json_bindings` — binding map → SPARQL JSON format
7. `serialize_csv_bindings` — binding map → CSV

Integration (in `crates/polargraph-rest/tests/` or new test binary):
8. `sparql_select_basic` — insert two triples, GET /sparql?query=SELECT, assert JSON result
9. `sparql_select_filter` — FILTER on integer property
10. `sparql_select_union` — UNION of two patterns
11. `sparql_select_path_plus` — property path across 3-hop chain
12. `sparql_ask_true` / `sparql_ask_false` — ASK query returns boolean
13. `sparql_content_negotiation` — Accept: text/csv returns CSV
14. `sparql_auth_rejected` — missing API key → 401

#### Phase 2: SPARQL-star, OPTIONAL, aggregations

**OPTIONAL / left-join**

Add `execute_left_join(left: &Query, right: &Query, snapshot: &Snapshot) -> Vec<Bindings>`
to `polargraph-query::datalog`. Algorithm:
1. Evaluate `left` → `left_bindings`.
2. For each left binding, attempt to evaluate `right` with pre-bound variables.
3. If right succeeds, merge; if not, return left binding with right vars absent.

Wire into SPARQL translation: `GraphPattern::LeftJoin` → `execute_left_join`.

**SPARQL-star**

Add `EvaluationStep::EdgeAnnotation { edge_var, predicate_var, value_var }` to
the translation model:
1. After evaluating the inner quoted triple pattern, resolve to `EdgeId`.
2. Call `store.scan_edge_annotations(edge_id, snapshot_ts)`.
3. Bind result variables.

This requires passing the `TripleStore` (not just `Snapshot`) to the evaluation
path, or adding a new helper on `Snapshot` that delegates to `scan_edge_annotations`.

**Aggregations**

`SELECT (COUNT(?s) AS ?n) GROUP BY ?type` maps to existing
`polargraph-query::aggregation` types. `translate.rs` needs to extract
`Aggregate` expressions from the `SelectQuery.dataset.projection` list and
build `AggregationSpec` values.

**Named graphs / Views**

`GRAPH <urn:view:my-view> { ... }` → look up `ViewId` from string, apply
`apply_view()` projection after evaluation.

#### Phase 3: SPARQL Update + RDF serializers

**SPARQL Update**

`INSERT DATA { ?s :knows ?o }` maps to an `InsertRequest` gRPC call.
`DELETE DATA { ... }` has no current PolarGraph equivalent (we append, not
delete); this would require a "soft delete" mechanism (set `vt_end` = now).
Classify as future work pending a delete/retraction feature.

**RDF serializers** for CONSTRUCT / DESCRIBE:
- Turtle (preferred) — use `rio_turtle` crate from Oxigraph ecosystem
- N-Triples — simpler, implement first
- JSON-LD — complex, phase 4

### Helm / deployment changes

- No new Service or Deployment for Phase 1/2 (endpoint added to polargraph-rest).
- `values.yaml`: add `sparql.enabled: true` flag; when false, the handler
  returns 404 immediately.
- REST gateway readiness probe can remain `GET /health`.

### Dependency additions

In `Cargo.toml` workspace:
```toml
spargebra = "0.3"
```

In `crates/polargraph-sparql/Cargo.toml`:
```toml
[dependencies]
spargebra = { workspace = true }
polargraph-core = { path = "../polargraph-core" }
polargraph-query = { path = "../polargraph-query" }
# proto types (prost-generated) from polargraph-server build output
```

In `crates/polargraph-rest/Cargo.toml`:
```toml
polargraph-sparql = { path = "../polargraph-sparql" }
polargraph-query = { path = "../polargraph-query" }
```

Note: `polargraph-query` pulling in `polargraph-storage` (and therefore
RocksDB) is acceptable — polargraph-rest is already a fairly heavy binary.
The alternative (proto-only REST gateway) is already broken by the SPARQL
translator needing `VarPattern` and `Rule` types from `polargraph-query`.

### Estimated effort

| Phase | Work | Est. |
|---|---|---|
| Feature 1: edge annotation wiring | 1–2 days |
| Phase 1: core SPARQL SELECT | 2–3 weeks |
| Phase 2: OPTIONAL, SPARQL-star, aggs | 2–3 weeks |
| Phase 3: SPARQL Update, RDF serializers | 3–4 weeks |

Feature 1 should be implemented first — it is a prerequisite for SPARQL-star
(Phase 2) because the annotation lookup path is the same.

---

---

## Feature 3: OWL 2 RL Entailment (Materialization)

### Background and value

OWL 2 RL is the "rule-friendly" profile of OWL 2 — it restricts the language
to axioms that can be compiled to Datalog rules, guaranteeing polynomial-time
materialization. Adding RL materialization gives PolarGraph:

- **Semantic interoperability** with published ontologies (FOAF, Schema.org,
  SKOS, Dublin Core, etc.) without hand-translating their axioms
- **Automatic inference** — insert `owl:subClassOf` / `owl:inverseOf` axioms
  and derived triples appear without touching the application layer
- **Property characteristics**: transitivity, symmetry, inverse, functional,
  domain/range entailment
- **Class hierarchy propagation**: subClass chains, disjointness, restrictions
- **SPARQL query completeness**: SPARQL queries over the materialized graph
  return inferred triples without any SPARQL reasoning extension

### Design philosophy: forward-chaining materialization at insert time

The alternative approaches and why we reject them:

| Approach | Pros | Cons |
|---|---|---|
| Live backward-chaining at query time | No storage overhead | Unbounded latency; breaks query timeout guarantees |
| Forward-chain on demand (lazy) | Smaller write amplification | Cache invalidation complexity |
| **Forward-chain at insert time (chosen)** | Queries see derived facts at no extra cost | Write amplification; re-materialization needed after axiom changes |

The chosen approach stores derived triples as first-class facts in a dedicated
`DRV` column family. Queries can include or exclude derived facts with a flag.
This is consistent with how OWL 2 RL reasoners like RDFox and ELK work.

### Integration with the existing Datalog engine

OWL 2 RL's ~40 entailment rules are expressed in Datalog. We already have
`execute_recursive` which takes `Vec<Rule>` and iterates to fixpoint. The
entire materialization engine is therefore:

1. Express every applicable RL rule as a `polargraph_query::datalog::Rule`.
2. Collect the current axiom triples from the store as seed bindings.
3. Run `execute_recursive` to fixpoint.
4. Write the derived bindings as `Triple::Relation` / `Triple::Property` into
   the `DRV` CF via a `WriteBatch`.

A `BUILTIN_OWL_RL_RULES: &[Rule]` constant (generated at compile time from a
rules DSL or written by hand) replaces the user-supplied `Vec<Rule>`. No new
evaluation engine required.

### The OWL 2 RL rule tables

OWL 2 RL defines rules across five tables. Below is the full rule inventory
organized by implementation phase.

#### Table 4: Semantic conditions (cls-* / thing/nothing)

| Rule | Head | Body |
|---|---|---|
| `cls-thing` | `?x rdf:type owl:Thing` | `?x rdf:type ?C` |
| `cls-nothing1` | *(inconsistency flag)* | `?x rdf:type owl:Nothing` |
| `cls-nothing2` | *(inconsistency flag)* | `?x rdf:type ?C`, `?C owl:complementOf ?D`, `?x rdf:type ?D` |

#### Table 5: Equality (eq-*)

| Rule | Head | Body |
|---|---|---|
| `eq-ref` | `?x owl:sameAs ?x` | `?x rdf:type owl:Thing` |
| `eq-sym` | `?y owl:sameAs ?x` | `?x owl:sameAs ?y` |
| `eq-trans` | `?x owl:sameAs ?z` | `?x owl:sameAs ?y`, `?y owl:sameAs ?z` |
| `eq-rep-s` | `?s2 ?p ?o` | `?s1 owl:sameAs ?s2`, `?s1 ?p ?o` |
| `eq-rep-p` | `?s ?p2 ?o` | `?p1 owl:sameAs ?p2`, `?s ?p1 ?o` |
| `eq-rep-o` | `?s ?p ?o2` | `?o1 owl:sameAs ?o2`, `?s ?p ?o1` |

`owl:sameAs` propagation (`eq-rep-*`) causes high fan-out on large datasets.
**Phase 3 only.** Phases 1 and 2 skip eq-* rules entirely; derive a flag to
detect `owl:sameAs` presence and warn at startup.

#### Table 6: Property axioms (prp-*)  — Phase 1 priority

| Rule | Head | Body | Priority |
|---|---|---|---|
| `prp-dom` | `?x rdf:type ?C` | `?p rdfs:domain ?C`, `?x ?p ?y` | Phase 1 |
| `prp-rng` | `?y rdf:type ?C` | `?p rdfs:range ?C`, `?x ?p ?y` | Phase 1 |
| `prp-fp` | `?y1 owl:sameAs ?y2` | `?p rdf:type owl:FunctionalProperty`, `?x ?p ?y1`, `?x ?p ?y2` | Phase 3 |
| `prp-ifp` | `?x1 owl:sameAs ?x2` | `?p rdf:type owl:InverseFunctionalProperty`, `?x1 ?p ?y`, `?x2 ?p ?y` | Phase 3 |
| `prp-irp` | *(inconsistency)* | `?p rdf:type owl:IrreflexiveProperty`, `?x ?p ?x` | Phase 1 |
| `prp-symp` | `?y ?p ?x` | `?p rdf:type owl:SymmetricProperty`, `?x ?p ?y` | Phase 1 |
| `prp-asyp` | *(inconsistency)* | `?p rdf:type owl:AsymmetricProperty`, `?x ?p ?y`, `?y ?p ?x` | Phase 1 |
| `prp-trp` | `?x ?p ?z` | `?p rdf:type owl:TransitiveProperty`, `?x ?p ?y`, `?y ?p ?z` | Phase 1 |
| `prp-spo1` | `?x ?q ?y` | `?p rdfs:subPropertyOf ?q`, `?x ?p ?y` | Phase 1 |
| `prp-spo2` | `?x ?q ?z` | `?p owl:propertyChainAxiom (?q1 ?q2)`, `?x ?q1 ?y`, `?y ?q2 ?z` | Phase 2 |
| `prp-eqp1` | `?x ?q ?y` | `?p owl:equivalentProperty ?q`, `?x ?p ?y` | Phase 1 |
| `prp-eqp2` | `?x ?p ?y` | `?p owl:equivalentProperty ?q`, `?x ?q ?y` | Phase 1 |
| `prp-inv1` | `?y ?q ?x` | `?p owl:inverseOf ?q`, `?x ?p ?y` | Phase 1 |
| `prp-inv2` | `?y ?p ?x` | `?p owl:inverseOf ?q`, `?x ?q ?y` | Phase 1 |

#### Table 7: Class axioms (cls-*) — Phase 2

| Rule | Meaning |
|---|---|
| `cls-int1` | If `?C owl:intersectionOf (?C1 ?C2)` and `?x` is in both, derive `?x rdf:type ?C` |
| `cls-int2` | Converse: if `?x rdf:type ?C` (intersection), derive member of each component |
| `cls-uni` | If `?C owl:unionOf (?C1 ...)` and `?x` is in any component, derive `?x rdf:type ?C` |
| `cls-com` | If `?C1 owl:complementOf ?C2`, `?x` cannot be in both (inconsistency) |
| `cls-svf1` | `owl:someValuesFrom` restriction satisfaction → class membership |
| `cls-svf2` | Class membership → `owl:someValuesFrom` witness |
| `cls-avf` | `owl:allValuesFrom` constraint propagation |
| `cls-hv1/2` | `owl:hasValue` restriction |
| `cls-maxc1/2` | `owl:maxCardinality 0/1` constraints |
| `cls-maxqc1/2` | `owl:maxQualifiedCardinality` |

#### Table 8: Schema entailment (scm-*) — Phase 2/3

| Rule | Meaning |
|---|---|
| `scm-cls` | Every class is subclass of `owl:Thing` |
| `scm-sco` | `rdfs:subClassOf` is transitive |
| `scm-op/dp` | Object/data properties are subproperties of themselves |
| `scm-eqc1/2` | `owl:equivalentClass` → bidirectional `rdfs:subClassOf` |
| `scm-eqp1/2` | `owl:equivalentProperty` → bidirectional `rdfs:subPropertyOf` |
| `scm-dom1/2` | Domain propagation through `rdfs:subPropertyOf` |
| `scm-rng1/2` | Range propagation through `rdfs:subPropertyOf` |
| `scm-spo` | `rdfs:subPropertyOf` is transitive |
| `scm-int` | Intersection component → superclass |
| `scm-uni` | Union → superclass of components |

### RDFS entailment rules (Phase 1 prerequisite)

RDFS rules are a subset that OWL 2 RL extends. We implement these first as
they cover the most common inference patterns:

| RDFS rule | Derived triple |
|---|---|
| `rdfs2` | `?x rdf:type ?C` from `?p rdfs:domain ?C`, `?x ?p ?y` |
| `rdfs3` | `?y rdf:type ?C` from `?p rdfs:range ?C`, `?x ?p ?y` |
| `rdfs5` | `?p rdfs:subPropertyOf ?r` from transitivity chain |
| `rdfs7` | `?x ?q ?y` from `?p rdfs:subPropertyOf ?q`, `?x ?p ?y` |
| `rdfs9` | `?x rdf:type ?D` from `?C rdfs:subClassOf ?D`, `?x rdf:type ?C` |
| `rdfs11` | `rdfs:subClassOf` transitivity |

`rdfs2` and `rdfs3` are identical to `prp-dom` / `prp-rng` — implement once,
cite both rule names.

### New storage: `DRV` column family

Derived triples share the same logical structure as asserted triples but live
in a separate CF so queries can choose to include or exclude them.

**Key layout**: identical to SPO — `[subject:16][pred_id:4][object:16][tt:8]`
for relations; `[subject:16][pred_id:4][sentinel:16][tt:8]` for properties.
This means `decode_spo_key` works unchanged on DRV entries.

**Value layout**: identical to SPO value bytes — `[vt_start:8][vt_end:8]`.
Materialization sets `vt_start = now`, `vt_end = END_OF_TIME`, `tt = commit_ts`.

**Write path**: `TripleStore::materialize_batch(derived: Vec<Triple>)` writes
to `DRV` only (not to the 6 hexastore CFs). This avoids polluting the primary
index with derived facts while still allowing fast lookup.

**Query path**: `Snapshot` gains a `include_derived: bool` field (default
`true`). When `true`, `snapshot_scan_cf` also scans `DRV` and merges results
(interleaved iteration, same MVCC filter logic). When `false`, derived triples
are invisible — useful for debugging or audit queries.

**Re-materialization**: when axioms change (new `owl:subClassOf` inserted),
the entire `DRV` CF must be rebuilt. `purge_derived()` issues a RocksDB
`delete_range` on `DRV` then re-runs `RunMaterialization`. This is a
potentially slow operation on large datasets; document it as an offline admin
operation (similar to `RunRetention`).

### BUILTIN_OWL_RL_RULES constant

```rust
// crates/polargraph-query/src/owl_rl.rs (new file)
use crate::datalog::{Rule, VarPattern, Term};

pub const BUILTIN_OWL_RL_RULES: &[RlRule] = &[
    // prp-dom: ?p rdfs:domain ?C, ?x ?p ?y  →  ?x rdf:type ?C
    RlRule {
        name: "prp-dom",
        body: &[
            VarPattern { subject: Term::Var("p"), predicate: Some("rdfs:domain"), object: Term::Var("C"), .. },
            VarPattern { subject: Term::Var("x"), predicate: Some_var("p"),        object: Term::Var("y"), .. },
        ],
        head: VarPattern { subject: Term::Var("x"), predicate: Some("rdf:type"), object: Term::Var("C"), .. },
    },
    // prp-rng: ?p rdfs:range ?C, ?x ?p ?y  →  ?y rdf:type ?C
    // prp-symp: ?p a owl:SymmetricProperty, ?x ?p ?y  →  ?y ?p ?x
    // prp-trp: ?p a owl:TransitiveProperty, ?x ?p ?y, ?y ?p ?z  →  ?x ?p ?z
    // prp-inv1: ?p owl:inverseOf ?q, ?x ?p ?y  →  ?y ?q ?x
    // prp-spo1: ?p rdfs:subPropertyOf ?q, ?x ?p ?y  →  ?x ?q ?y
    // rdfs9: ?C rdfs:subClassOf ?D, ?x rdf:type ?C  →  ?x rdf:type ?D
    // scm-sco: ?A rdfs:subClassOf ?B, ?B rdfs:subClassOf ?C  →  ?A rdfs:subClassOf ?C
    // ... (full set compiled in)
];
```

The `Term::Some_var("p")` notation above is illustrative — the actual
implementation uses the existing `Term` enum in `datalog.rs` where predicate
slots accept either a bound string or a variable. If the current `VarPattern`
does not support variable predicates (the `predicate` field is `Option<String>`
today), rules like `prp-spo1` that require `?x ?p ?y` (variable predicate)
need a **predicate-variable extension** to `VarPattern`.

**Required change to `VarPattern`**: Add `predicate_var: Option<String>` field
alongside the existing `predicate: Option<String>`. When `predicate_var` is
set, the evaluator iterates over all predicates for the bound subject (PSO scan)
and binds the predicate variable. This is necessary for rules that quantify
over properties.

### Materialization trigger points

| Event | Action |
|---|---|
| Any `Insert` RPC | Check if inserted triples include OWL axiom predicates; if so, run incremental materialization |
| `RunMaterialization` RPC | Purge DRV CF, run full forward-chain from scratch |
| Server startup | If `DRV` CF is empty and OWL axioms present, auto-run materialization |
| Bulk import (`polargraph-import`) | Accept `--run-materialization` flag to trigger after SST ingestion |

**Incremental materialization** (insert-triggered): extract the newly inserted
triples, find all rules whose body could be satisfied by these triggers, run
only those rules from the new seeds. This avoids re-materializing the entire
graph on every insert. Implementation via a `trigger_map: HashMap<&str, Vec<&RlRule>>`
that indexes rules by the predicates that appear in their body.

**Full materialization** (`RunMaterialization` RPC): `execute_recursive` is
already iterative-to-fixpoint. Pass all `BUILTIN_OWL_RL_RULES` + all existing
asserted triples as seeds. Write results to `DRV` CF.

### API surface

#### New gRPC RPCs in `polargraph.proto`

```protobuf
rpc RunMaterialization(RunMaterializationRequest) returns (RunMaterializationResponse);
rpc MaterializationStatus(google.protobuf.Empty) returns (MaterializationStatusResponse);
```

```protobuf
message RunMaterializationRequest {
    bool dry_run = 1;       // count rules fired without writing to DRV
    bool incremental = 2;   // only re-derive from changed triples (ignored for now, reserved)
}
message RunMaterializationResponse {
    uint64 triples_derived = 1;
    uint64 duration_ms = 2;
    bool inconsistency_detected = 3;
}
message MaterializationStatusResponse {
    bool enabled = 1;
    uint64 derived_triple_count = 2;
    int64 last_run_ts = 3;
    bool stale = 4;         // true if axioms changed since last run
}
```

#### REST endpoints (in `polargraph-rest`)

```
POST /ontology/import         Accept: text/turtle or application/n-triples
                              Body: OWL/RDFS ontology file
                              Action: parse, insert axiom triples, trigger materialization
GET  /ontology/export         Accept: text/turtle
                              Response: all triples with OWL/RDFS predicates
POST /materialization/run     Body: { "dry_run": false }
GET  /materialization/status
```

The `/ontology/import` handler in polargraph-rest needs a Turtle/N-Triples
parser to extract axiom triples from the uploaded file, then converts them to
`InsertRequest` proto messages. Use the `rio_turtle` crate (from Oxigraph) for
parsing — same dep we'll add for SPARQL Feature 2 Phase 3.

#### Configuration

```toml
[storage]
owl_materialization = true          # enable/disable (default: true if OWL axioms present)
materialization_on_startup = true   # re-run on server start if stale
```

CLI flags: `--owl-materialization` / `--no-owl-materialization`,
`POLARGRAPH_OWL_MATERIALIZATION` env var.

Server startup sequence with materialization enabled:
1. Open RocksDB.
2. Run schema migrations.
3. Check `MaterializationStatus` — if `stale = true`, log a warning.
4. If `materialization_on_startup = true` and stale, run `RunMaterialization`.
5. Accept gRPC connections.

### Inconsistency detection

OWL 2 RL defines several "unsatisfiability" conditions (e.g., `cls-nothing2`:
a node that is both a member of `?C` and `?C owl:complementOf ?D` and member
of `?D`). When the materializer detects such a condition it:

1. Sets an `inconsistency_detected` flag in `MaterializationStatusResponse`.
2. Emits a `warn!` log entry with the offending triples.
3. Does **not** abort — inconsistent ontologies are common in practice and we
   surface the issue without crashing.

Full inconsistency handling (blocking writes, returning errors) is deferred
to Phase 3.

### Prometheus metrics (new)

```
polargraph_materialization_derived_total       # total derived triples in DRV CF
polargraph_materialization_last_run_ts         # unix µs of last full run
polargraph_materialization_duration_seconds    # histogram of run duration
polargraph_materialization_rules_fired_total   # rule firings per run
polargraph_materialization_stale               # gauge: 1 if stale, 0 if current
```

### Phases

#### Phase 1: RDFS + property characteristics (highest ROI)

Rules: `rdfs2`, `rdfs3`, `rdfs5`, `rdfs7`, `rdfs9`, `rdfs11`, `prp-dom`,
`prp-rng`, `prp-symp`, `prp-trp`, `prp-inv1`, `prp-inv2`, `prp-spo1`,
`prp-eqp1`, `prp-eqp2`, `prp-irp`, `prp-asyp`, `scm-sco`, `scm-spo`.

These cover the most practical use cases: `FOAF` property chains,
`Schema.org` domain/range, SKOS `skos:broader` transitivity,
inverse properties (`:parent` / `:child`), symmetric properties (`:sibling`).

Deliverables:
- `DRV` CF added to `store.rs` with `materialize_batch()` + `purge_derived()`
- `include_derived: bool` on `Snapshot`
- `polargraph-query::owl_rl` module with Phase 1 rule set
- `RunMaterialization` + `MaterializationStatus` gRPC RPCs
- Incremental trigger on `Insert` for axiom predicates
- `POST /materialization/run`, `GET /materialization/status` REST endpoints
- Tests (see below)

Extension to `VarPattern` for variable predicates (needed by `prp-spo1`):
add `predicate_var: Option<String>` and update the evaluator's PSO scan path.

#### Phase 2: Class axioms + schema entailment

Rules: `cls-int1/2`, `cls-uni`, `cls-svf1/2`, `cls-avf`, `cls-hv1/2`,
`scm-cls`, `scm-eqc1/2`, `scm-eqp1/2`, `scm-dom1/2`, `scm-rng1/2`,
`scm-int`, `scm-uni`.

Deliverables:
- Full `cls-*` and `scm-*` rule set in `owl_rl.rs`
- `POST /ontology/import` REST endpoint with `rio_turtle` parser
- `GET /ontology/export` REST endpoint
- `cls-com` / `cls-nothing*` inconsistency detection
- `polargraph_materialization_stale` metric

#### Phase 3: `owl:sameAs` + functional properties + cardinality

Rules: `eq-sym`, `eq-trans`, `eq-rep-s/p/o`, `prp-fp`, `prp-ifp`,
`cls-maxc1/2`, `cls-maxqc1/2`, `cls-nothing2`.

`owl:sameAs` equality propagation is the most expensive part of OWL 2 RL
because it causes an n² explosion of derived triples. Mitigations:
- **Union-Find structure**: maintain a union-find for sameAs equivalence
  classes; store the canonical representative per class rather than all
  pairwise sameAs triples
- **Query-time expansion**: instead of materializing all eq-rep-* triples,
  expand sameAs sets at query time (hybrid approach)

Cardinality rules (`cls-maxc1`, `cls-maxqc1`) detect violations but do not
enforce constraints (enforcement is a write-path concern).

### Tests to write

#### Unit tests in `crates/polargraph-query/src/owl_rl.rs`

1. `prp_dom_fires` — body matches, derive `rdf:type` triple
2. `prp_trp_chain` — 3-hop transitive chain collapses to direct triple
3. `prp_symp_derives_inverse` — symmetric property produces reverse triple
4. `prp_inv1_inv2` — inverseOf both directions
5. `scm_sco_transitivity` — 3-level subclass chain propagates
6. `rdfs9_class_membership` — subClassOf propagates rdf:type
7. `inconsistency_cls_nothing` — complementOf + membership sets inconsistency flag

#### Storage integration tests

8. `materialize_and_scan_derived` — insert axioms + data, run materialize,
   scan DRV CF, assert derived triples present
9. `purge_derived_clears_cf` — after purge, DRV CF is empty
10. `include_derived_false_hides_derived` — snapshot with `include_derived=false`
    does not return DRV entries
11. `incremental_materialize_on_axiom_insert` — inserting a new `rdfs:domain`
    triple auto-triggers materialization for affected subjects

#### gRPC integration tests

12. `run_materialization_derives_domain` — insert domain axiom + data, call
    `RunMaterialization`, then `Query` returns the derived type triple
13. `run_materialization_dry_run` — `dry_run=true` reports count but DRV stays empty
14. `materialization_status_stale` — insert new axiom, check status shows `stale=true`
15. `materialization_on_startup` — stop/restart server with `materialization_on_startup=true`,
    verify DRV is rebuilt
16. `prp_trp_query` — insert transitive property axiom + chain of 4 triples,
    query the indirect connection, get a hit from DRV

### Estimated effort

| Phase | Work | Est. |
|---|---|---|
| Phase 1: RDFS + prp-* rules | 3–4 weeks |
| Phase 2: cls-* + scm-* + ontology import | 2–3 weeks |
| Phase 3: owl:sameAs + cardinality | 3–4 weeks |

Phase 1 has a hard prerequisite: the `predicate_var` extension to `VarPattern`
(needed for `prp-spo1` and similar rules that quantify over predicates). This
should be scoped and delivered before or alongside the rule engine work.

### Dependency additions

```toml
# workspace Cargo.toml — already planned for Feature 2 Phase 3:
rio_turtle = "0.8"       # Turtle/N-Triples parser for /ontology/import

# No new evaluation engine dep — reuses execute_recursive
```

---

## Open questions

1. **IRI strategy**: Should PolarGraph store predicates as full IRIs
   (`http://schema.org/knows`) or local names (`knows`) when data is inserted
   via the SPARQL endpoint? Full IRIs are correct RDF but verbose; local names
   are what the Cypher layer uses. Recommendation: store full IRIs and document
   that Cypher users can use either form (predicate interning is string-keyed,
   so both coexist in the same store without conflict).

2. **Blank node identity**: SPARQL blank nodes within a session should be
   scoped to the query. Across sessions they are opaque. PolarGraph uses UUID v7
   for all node IDs — map SPARQL blank nodes to `NodeId::new()` on INSERT (out
   of scope for Phase 1 which covers SELECT only).

3. **RDF type system**: SPARQL literals carry XSD datatypes. PolarGraph `Value`
   is typed internally. The serializer must map `Value::Int → xsd:integer`,
   `Value::Float → xsd:double`, `Value::Bool → xsd:boolean`, `Value::Text →
   plain literal`. Confirm whether typed literals need to round-trip through
   the SPARQL endpoint (yes for interop, no for Phase 1).

4. **`spargebra` version**: Verify the current crates.io version of `spargebra`
   (0.3.x as of mid-2025) compiles with our MSRV (Rust 1.78). Oxigraph targets
   recent stable Rust; check their CI matrix.
