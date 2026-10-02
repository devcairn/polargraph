# Value bindings — query variables that hold literals — design note

Status: decisions A–G **approved as recommended** (Mark, 2026-10-02; A: a
separate value map). Building as the two PRs in G on `db/value-bindings`.

## 1. The gap

A query variable can only hold a **node**. `Bindings` is
`HashMap<String, NodeId>` (`polargraph-query::datalog`), and when a pattern's
object variable meets a property triple, `extend_full` drops the match
("object variable can't bind to a scalar"). So:

| Query | Today |
|---|---|
| SPARQL `?p :name "Alice"` (literal written in the query) | works — `Term::Literal`, a value-index lookup |
| SPARQL `SELECT ?n WHERE { ?p :name ?n }` | **no rows** |
| `FILTER(?age > 30)`, `GROUP BY ?city`, `SUM(?price)` over property values | no rows (the comparison / aggregate code exists but never sees a value) |
| `ORDER BY ?date` | **silently ignored** by the translator |
| SPARQL Update `DELETE WHERE { <n> ?p ?o }` | closes relations only; property values aren't bound |
| Datalog / `Query` RPC `?p name ?n` | no rows; the proto `Binding` has no place for a value |
| Cypher `RETURN n.name` | works, by a per-row lookup after the query (`prop_projections`) |

This is core SPARQL 1.1 (not SPARQL-star, which is already supported). It
also blocks removing Cypher writes: `DELETE n` and pattern-based value
updates have no replacement until variables can hold values
(`docs/upgrade-cypher-rdf.md`, "Gap before removal").

What's already in place: storage returns decoded values on every scan
(out-of-line values included); a value bound before a pattern can drive a
`posg` value-index lookup (`resolve_term` already hashes `Term::Literal`);
the SPARQL side already has literal result types (`SparqlValue::Literal*`)
and value comparisons, used today only by aggregates.

## 2. Proposal

### 2.1 Engine (`polargraph-query::datalog`)

A **solution** carries node, predicate and value bindings:

```rust
pub type Bindings = HashMap<String, NodeId>;        // unchanged
pub type PredBindings = HashMap<String, String>;    // unchanged
pub type ValueBindings = HashMap<String, Value>;    // new
```

- An object variable that meets a **property** triple binds to its value
  (today: the row is dropped); one that meets a relation binds the node, as
  now. A variable lives in exactly one map; the same variable bound as a
  node in one pattern and meeting a value in another simply doesn't join
  (an IRI never equals a literal).
- A value variable bound **before** a pattern is substituted like
  `Term::Literal` — so `?p :age ?a . ?q :age ?a` joins through the value
  index, not a scan.
- A value variable in subject position, as a graph, or as a path endpoint
  (`max_hops`) matches nothing (literals aren't subjects or graphs).
- Rule heads stay node-to-node (derived facts are relations). Rule bodies
  may use value variables; a body solution whose head variable holds a
  value derives nothing. (As built: the proposal said `INVALID_ARGUMENT`,
  but Cypher's `[:p*]` closure rules would then fail whenever some node also
  has a `p` property; skipping keeps today's behaviour.)
- The pending-write overlay (wire transactions), timeouts and the graph ACL
  (value rows pass the node filter; their subject was already checked by the
  scan) apply unchanged.
- Public API: the existing `execute_query*` functions keep their signatures
  and node-only results; `*_full` variants (already used by the server for
  predicate variables) return `(Bindings, PredBindings, ValueBindings)`.
  Internally a `Solution` struct replaces the tuple. Callers that only want
  nodes — Cypher, the bench — are untouched.

### 2.2 Wire format (additive)

```protobuf
message Binding     { map<string, NodeId> vars = 1; map<string, string> predicates = 2;
                      map<string, Value> values = 3; }   // new
message QueryResult { map<string, NodeId> vars = 1;
                      map<string, Value> values = 2; }   // new
```

Old clients ignore `values`; nothing they received before changes. REST
`/query` and `/query/stream` add a `values` object per row (JSON value
encoding as `/insert`). The SDKs decode it (`query()` rows gain values).

### 2.3 SPARQL (`polargraph-sparql`, `polargraph-rest`)

- SELECT / ASK / CONSTRUCT / DESCRIBE results include literal bindings,
  serialised with their datatype or language (`SparqlValue` gains
  `LangText` and `Typed`, mirroring `Value`), in JSON, CSV and RDF output.
- `FILTER` comparisons, `BOUND`, `isLiteral`, `GROUP BY`, aggregates and
  `HAVING` see real values (the code exists; it starts receiving them).
- **`ORDER BY`** (ascending / descending, multiple keys, SPARQL's ordering
  of unbound < nodes < literals) — implemented, since ignoring it silently
  is worse than not supporting it.
- SPARQL Update `DELETE` / `INSERT ... WHERE` templates instantiate value
  variables, so `DELETE WHERE { <n> ?p ?o }` closes every live quad of `n`
  — the replacement for Cypher `DELETE n`.
- `OPTIONAL` (`left_join`) and `UNION` work on value columns unchanged.

### 2.4 Cypher

Unchanged in this step. `RETURN n.prop` keeps its per-row lookup; moving it
(and `WHERE n.prop > x`, which today only supports equality on nodes) onto
value bindings is a later optimisation, not needed for correctness.

### 2.5 What this unblocks

Removing Cypher writes (a later release): every Cypher write then has a
one-call replacement — `ApplyChanges` for known quads, SPARQL Update for
pattern-based ones, including `DELETE WHERE { <n> ?p ?o }`.

## 3. Decisions needed

| # | Question | Recommendation |
|---|----------|----------------|
| A | How solutions carry values | **A separate `ValueBindings` map** threaded alongside node and predicate bindings (the pattern predicate variables already use), behind a `Solution` struct. The alternative — one map of an enum `{Node, Value}` — is cleaner in theory but rewrites every consumer of `Bindings` (about 100 sites across query, server, REST, SPARQL, bench) for no behavioural gain. |
| B | Join equality | **RDF term equality**: two values join only if they are the same term — `Int(1)` ≠ `Float(1.0)`, `"a"` ≠ `"a"@en`, typed literals compare lexical form + datatype. This is what SPARQL joins use, and it's exactly the value index's hash. `FILTER(?a = ?b)` gets value semantics (numeric `1 = 1.0` true), as SPARQL specifies — today it compares strictly. |
| C | Wire format | **Additive** `values` maps on `Binding` and `QueryResult`, a `values` object in REST rows. No breaking change. |
| D | Which values bind | **All property values except vectors** (`Value::Vector` is an embedding, not an RDF literal; it can be large and SPARQL has no form for it). A pattern meeting a vector property skips it, documented. Out-of-line values bind in full. |
| E | Rules | Heads stay node-to-node; value variables allowed in bodies (as built: a solution whose head variable holds a value derives nothing — see 2.1). |
| F | Scope of this step | Engine + `Query` / `QueryStream` RPCs + REST + SPARQL (results, FILTER / GROUP BY / aggregates seeing values, `ORDER BY`, Update templates) + SDK decoding. Cypher unchanged (2.4). |
| G | Delivery | **Two PRs**: (1) engine, proto, `Query` / `QueryStream`, REST `/query`, SDK decoding; (2) SPARQL — literal results, `ORDER BY`, Update with value variables, tests from the W3C-style cases we already run. Cypher-write removal stays a separate, later step. |

## 4. Risks and costs

- **Result size**: a `?s ?p ?o` over property-heavy data now returns every
  value. Existing limits apply (`LIMIT`, query timeout, streaming); no new
  cap proposed.
- **Behaviour change for SPARQL users**: queries that returned nothing now
  return rows (the intended fix). Queries that relied on an object variable
  silently skipping literals — e.g. `?s ?p ?o` used to list only relations —
  now also return property rows. Release note.
- **Performance**: no extra storage work (values are already decoded); one
  more map per solution. Bench `polargraph-bench bsbm` before / after to
  confirm.
- **No storage change, no migration.**
