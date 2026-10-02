# Cypher over RDF — release note and upgrade guide

Design: `docs/design/cypher-rdf.md` (decisions A–F, G2).

## Release note (breaking)

PolarGraph now stores names as RDF IRIs and Cypher labels as `rdf:type`, so
Cypher, SPARQL, SHACL, imports and exports all see one spelling of the same
data.

- **A store-wide vocabulary.** A base IRI (default `urn:pg:vocab:`) and a
  prefix map, kept in the system graph and changed at runtime with
  `SetVocabularyBase`, `PutPrefix` and `RemovePrefix` (REST `/vocabulary*`).
  No restart or reload is ever needed.
- **Bare predicate names are IRIs.** A predicate written without a `:` —
  `name`, `knows`, anything from REST `/insert`, Datalog patterns or Cypher —
  is stored as `<base><name>` (`urn:pg:vocab:name`). APIs that *take* a bare
  name resolve it the same way, so they keep working; **responses that
  return predicate strings now return the IRI** (exports, `Subscribe` quads,
  bound predicate variables, edge annotations).
  Internal names (`__…`, `MEMBER_OF`, `HAS_ACCESS`, `HAS_ACCESS_TYPE`,
  `HAS_GRAPH_ACCESS`, `GRAPH_ACCESS_LEVEL`) are unchanged.
- **Cypher labels are `rdf:type`.** `CREATE (n:Person)` writes
  `n rdf:type <urn:pg:vocab:Person>`; `MATCH (n:Person)` matches it (exact
  class; subclasses via OWL RL materialization). The `__type` property is no
  longer written or read anywhere: typed vector filters (`NodeTypeFilter`),
  `Subscribe` `types`, `HAS_ACCESS_TYPE` grants, schema-aware pruning and the
  management UI all use `rdf:type`.
- **Prefixed and full names in Cypher** go in backticks:
  `` (n:`ex:Widget`) ``, `` -[:`http://schema.org/knows`]-> ``,
  `` n.`ex:sku` ``. Result keys keep the name as written (`n.name`).
- **Type names resolve through the vocabulary** wherever a type is named
  (`NodeTypeFilter.type_name`, `Subscribe.types`, `HAS_ACCESS_TYPE` values,
  registry domain / range): `Person` → `<base>Person`, `ex:Widget` →
  namespace + `Widget`, or a full IRI.

### The window: legacy data until conversion

Existing data is **not** rewritten at startup. Until the operator runs the
one-time conversion below, data stored under bare predicate names and
`__type` labels **is not reachable by bare name**: `name` now resolves to
`urn:pg:vocab:name`, and Cypher labels match `rdf:type`. The data is intact
and still visible under its stored name (exports, `?p` predicate variables).
Plan the conversion right after the upgrade.

While legacy data remains:

- the server logs a warning at startup with the counts and what to run;
- `GetVocabulary` (REST `GET /vocabulary` → `legacy`), `ShowStats` and the
  management UI's `/health` report `legacy_conversion_pending: true`,
  `legacy_bare_predicates` and `legacy_type_labels`.

## Upgrade guide

### 1. Upgrade and check the status

Deploy the new binary as usual. Then:

```bash
curl http://rest:8000/vocabulary
```

```json
{"base": "urn:pg:vocab:", "prefixes": {},
 "legacy": {"conversion_pending": true, "bare_predicates": ["knows", "name"],
            "type_labels": 1200, "pending_merges": []}}
```

A store created on this version has nothing to convert
(`conversion_pending: false`) — skip to step 5.

### 2. Choose the vocabulary base (and prefixes)

Bare names are converted under the base **in effect when the conversion
runs**, and new bare names keep resolving under it. Set a namespace you own
before converting; changing it later affects only names resolved from then
on — stored IRIs are never rewritten, so bare names would stop finding data
stored under the old base.

```bash
curl -X PUT http://rest:8000/vocabulary/base -H 'content-type: application/json' \
  -d '{"base":"https://example.com/ns/"}'
curl -X POST http://rest:8000/vocabulary/prefixes -H 'content-type: application/json' \
  -d '{"name":"schema","namespace":"http://schema.org/"}'
```

gRPC: `SetVocabularyBase`, `PutPrefix` (service calls — no user id — on the
primary).

### 3. Dry run

```bash
curl -X POST http://rest:8000/vocabulary/convert -H 'content-type: application/json' \
  -d '{"dry_run":true}'
```

The report lists every predicate rename (`from` → `to`), predicates whose IRI
already existed (`merged: true`, with the number of live quads to move) and
the number of `__type` labels. Nothing changes.

### 4. Convert

```bash
curl -X POST http://rest:8000/vocabulary/convert
```

(gRPC `ConvertLegacyData`.) The conversion runs online on the primary:

1. **Predicate renames.** Each bare predicate `P` becomes `<base>P` — a
   rename of its intern-table entry, so no quads are rewritten. If
   `<base>P` was already in use, `P`'s live quads are copied to it and
   closed under `P`, whose entry becomes `__legacy__/P` (its history stays
   queryable there).
2. **Labels.** Every live `s __type "X"` (in every graph) becomes
   `s rdf:type <expand(X)>` in the same graph with the same valid time, and
   the property is closed — history keeps it. Class IRIs are recorded in the
   IRI dictionary. These commits appear in the change feed with author
   `urn:pg:legacy-conversion`.

Work is committed in chunks of 10 000 quads. The call is **idempotent and
resumable**: if it is interrupted, run it again and it continues from what
is still legacy; once nothing is left it is a no-op. One conversion runs at
a time (`ABORTED` for a second concurrent call).

Replicas receive the conversion through the WAL; their status fields catch up
within a minute.

### 5. Update clients

- Code that reads or writes `__type` should use `rdf:type`
  (`http://www.w3.org/1999/02/22-rdf-syntax-ns#type`) relations to class
  IRIs, or Cypher labels.
- Code that compares predicate strings from responses should expect IRIs
  (`urn:pg:vocab:name` or your base), or compact them with the vocabulary
  from `GET /vocabulary`.
- Cypher using a name with `:` must quote it in backticks.
- `HAS_ACCESS_TYPE` grants, `NodeTypeFilter` and `Subscribe` `types` keep
  taking type names; they now resolve through the vocabulary.

## Cypher writes are deprecated

Cypher stays as a **read** language (MATCH / WHERE / RETURN / WITH,
aggregations, `VECTOR_NEAR`, `USE GRAPH`). Cypher **writes** — the
`CypherWrite` RPC and REST `POST /cypher/write` (CREATE, MERGE, SET,
DELETE) — are deprecated: they still run in this release and are removed in
the next one. Meanwhile:

- gRPC responses carry a `warning` header, the server logs a warning on the
  first call, and `polargraph_deprecated_rpc_total{rpc="CypherWrite"}`
  counts calls (use it to find remaining callers);
- REST responses carry `Deprecation: true` and a `Warning` header;
- the SDKs mark `cypher_write` / `CypherWrite` / `cypherWrite` deprecated
  (Python `DeprecationWarning`, Go `Deprecated:`, TypeScript `@deprecated`
  plus a one-time `DeprecationWarning`).

Write with an atomic changeset — `ApplyChanges` (REST `POST /changes`, SDK
`apply_changes` / `ApplyChanges` / `applyChanges`) — or SPARQL Update. One
write path keeps graph ACLs, authors, the change feed and SHACL validation
consistent.

| Cypher | Replacement |
|---|---|
| `CREATE (n:Person {name: "Alice"})` | `ApplyChanges` adds: `n rdf:type` with `object_iri: "Person"` (REST: `"object": "Person"`) + `n name "Alice"`; the client picks the node UUID |
| `MERGE (n:Person {name: "Alice"})` | Query first (Cypher `MATCH`), then `ApplyChanges` with `read_ts` set to the query's read time — `ABORTED` if someone wrote in between |
| `MATCH … SET n.age = 31` | Add `n age 31` (the default write mode replaces the current value), or SPARQL `DELETE { … } INSERT { … } WHERE { … }` |
| `CREATE (a)-[:knows]->(b)` | Add the relation `a knows b` |
| `DELETE n` | Retract each of the node's live quads (`ApplyChanges` retractions; see the gap below) |
| `USE GRAPH <g> CREATE …` | Adds grouped under graph `g` |

SDK `insert_node` / `InsertNode` / `insertNode` now write
`rdf:type` (by class name) instead of `__type`.

**Gap before removal.** Two Cypher write patterns have no one-call
replacement yet, because query variables bind nodes, not property values:
deleting a node with all its properties (`DELETE n`), and SPARQL updates
whose `WHERE` binds a literal (`DELETE WHERE { <n> ?p ?o }`). Today a client
lists the node's values (Cypher `RETURN n.prop`, `GetPropertyHistory`,
`ExportGraph`) and retracts them. Removing Cypher writes is planned only
after one of these lands: literal-valued variables in Datalog / SPARQL, or a
"close every quad of these subjects" option on `ApplyChanges`.

## Notes

- The node and edge type registries keep the names they were registered
  with; names resolve through the vocabulary when used.
- Predicate renames are metadata-only and don't produce change-feed events.
- A prefix can't be named like a URI scheme (`http`, `https`, `urn`, …).
