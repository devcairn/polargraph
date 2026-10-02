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

Not affected: Cypher (relationship patterns still match relations only),
Datalog rules (a rule-body row whose head variable holds a value derives
nothing, as before), and SPARQL, which keeps its current results until the
SPARQL step adds literal results, `ORDER BY` and value-aware updates.

## Upgrade

No storage change, no migration, no configuration. Clients built against
the old proto keep working: `values` is a new field they ignore — but they
will see more rows for `?s ?p ?o`-style patterns (above), with the value
variable missing from `vars`.
