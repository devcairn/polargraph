# Value bindings — release note and upgrade guide

Design: `docs/design/value-bindings.md`.

## Release note

Query variables can now hold **property values** (literals), not only nodes.
`?p name ?n` binds `?n` to each name; before, a variable that met a property
value dropped the row.

| Surface | What's new |
|---|---|
| gRPC `Query`, `QueryStream` | `Binding.values` / `QueryResult.values`: variables bound to values (`map<string, Value>`). A variable is in `vars` or `values`, never both. |
| REST `/query`, `/query/stream` | Rows carry `"@values": {var: value}` when any variable holds a value (the JSON value encoding of `/insert`; language-tagged and typed literals as `{"@value", "@language" \| "@type"}`). |
| Management UI | Query results show values. |
| SDKs | Python / TypeScript query rows include value variables; Go adds `QueryRows` (`Row.Nodes`, `Row.Values`), `Query` unchanged. |

Joins on value variables use RDF term equality (`1` and `1.0` are different
values; `"a"` and `"a"@en` too). Vector properties don't bind.

### Behaviour change: `?s ?p ?o` and other object variables

A pattern whose object is a variable now matches **properties as well as
relations**:

- `?s ?p ?o` (predicate variable, object variable) used to list relations
  only; it now also returns one row per live property value, with `?o` in
  `values`.
- `?s :name ?o` used to return nothing for a property predicate; it now
  returns the values.
- A pattern on a predicate used both as a relation and as a property returns
  both kinds of rows.

If you relied on object variables skipping properties, either keep only the
rows whose variable is in `vars` (gRPC) / not in `@values` (REST), or put
the object in a node-only position (a variable that is also a subject of a
later pattern can only be a node).

Not affected: Cypher (relationship patterns still match relations only)
and Datalog rules (a rule-body row whose head variable holds a value
derives nothing, as before).

### SPARQL

SPARQL now returns literals, and several results change:

- **Object variables bind values** — `SELECT ?n WHERE { ?p :name ?n }`
  returns the names; `?s ?p ?o` returns property rows as well as relations;
  `DESCRIBE` includes property values.
- **Predicate variables bind** — `?p` in `?s ?p ?o` is now in results (it
  was never bound).
- **`ORDER BY` is applied** (it was silently ignored). Only variables are
  supported as keys; other expressions are rejected (HTTP 501) instead of
  ignored.
- **`FILTER` errors drop rows** — an unbound variable or an incomparable
  comparison is an error, and `!error` is still an error. Before,
  `FILTER(!(?x > 3))` kept rows where `?x` was unbound or a string. `!=`
  between different non-numeric types (`"a" != 1`) is an error too.
- **`=` compares values** — `?a = ?b` is true for `1` and `1.0` (it used
  to require the same term); `sameTerm(?a, ?b)` still tests identity.
- **Aggregates** — `MIN` / `MAX` return the actual least / greatest value
  (mixed integers and doubles used to return their sum); `MIN` / `MAX` /
  `SAMPLE` of an empty group are unbound (were `0` / `""`); `SUM` / `AVG`
  over a non-numeric value are unbound (non-numbers were skipped); `AVG` of
  an empty group is `0`.
- **Updates** — `DELETE` / `INSERT ... WHERE` templates use value and
  predicate variables, so `DELETE WHERE { <n> ?p ?o }` closes every live
  quad of `n` (it skipped variable predicates).
- **Output** — literal results carry `datatype` / `xml:lang` in JSON; CSV
  quotes fields containing commas, quotes or line breaks.
- **New `FILTER` functions** — comparisons between variables (`?a < ?b`)
  and over `STR` / `LANG` / `DATATYPE`; `CONTAINS`, `STRSTARTS`, `STRENDS`,
  `LANGMATCHES`, `REGEX`. Queries using them were rejected before (HTTP 501).

## Upgrade

No storage change, no migration, no configuration. Clients built against
the old proto keep working: `values` is a new field they ignore — but they
will see more rows for `?s ?p ?o`-style patterns (above), with the value
variable missing from `vars`.
