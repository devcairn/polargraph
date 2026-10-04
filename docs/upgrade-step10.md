# Step 10 — release note and upgrade guide

Design: `docs/design/type-packages-boundary.md` (P2 and the graph-selection
gaps).

## Release note

- **SPARQL Update is atomic.** A request made of data operations
  (`INSERT DATA`, `DELETE DATA`, `DELETE` / `INSERT … WHERE`) is compiled to
  one changeset and committed as **one transaction** (`ApplyChanges`): it
  applies entirely or not at all, and is one change-feed entry. Before, each
  operation and each triple was its own write, so a failure part-way left
  the request half-applied.
  - `?dry_run=true` returns the changeset (in the `POST /changes` format)
    without applying it.
  - `?read_ts=N`: the commit fails (HTTP 409) if any quad it adds or
    retracts changed after commit time N.
  - **Behaviour change:** every `WHERE` clause reads the data as of the
    request's read point — `read_ts`, else the start of the request — not
    the result of earlier operations in the same request. (Within the
    changeset, a later `DELETE` still undoes an earlier `INSERT` of the same
    quad, and a later `INSERT` re-asserts a deleted one.) A request with a
    `WHERE` clause and no `read_ts` commits conditionally on its read point,
    so a concurrent change to a quad it touches returns 409; retry.
  - **Behaviour change:** graph operations (`CLEAR`, `DROP`, `CREATE`,
    `ADD` / `COPY` / `MOVE`, `LOAD`, `DELETE DATA` of SPARQL-star
    annotations) can't be combined with data operations in one request
    (HTTP 400); send them separately. Graph-operation requests run as
    before (not atomic).
  - `INSERT DATA { << s p o >> :a v }` annotates an edge that exists before
    the request; an edge inserted in the same request can't be annotated
    (counted in `failed`).
  - A rejected quad (e.g. a write to an inferred graph, or a graph the
    caller can't write) now fails the whole request with its status (403…)
    instead of being counted in `failed`.
  - The response adds `commit_ts` and `retractions_not_found`.
- **`ApplyChanges`**: `QuadRef.all_graphs` retracts a quad in every graph
  where it is live (and writable); `edge_annotations` adds RDF-star
  annotations in the same transaction. `ValidateShapes` overlays honour
  `all_graphs`.
- **Inference schema graphs.** `SetInferenceSettings { schema_graphs }`
  (REST `PUT /inference/settings`) names the graphs whose schema axioms
  (`rdfs:subClassOf`, `subPropertyOf`, `domain`, `range`, `owl:inverseOf`,
  symmetric / transitive declarations) drive OWL RL inference; axioms in
  other graphs are ordinary data. Unset = every graph (unchanged). Changing
  it recomputes the inferred graphs. `GetInferenceSettings` /
  `GET /inference/settings` read it. Service calls only.
- **Graph-scoped vector search.** `SearchVector`, `SearchVectorFiltered`,
  `SearchVectorInSet` and `VectorSeedQuery` take `graphs`: only nodes with
  a live quad in one of them ("" = default graph) are returned;
  `VectorSeedQuery`'s patterns read only those graphs. REST
  `/vector/search` accepts `"graphs"`; SDKs: `graphs` option.
  `SearchVectorInSet` now drops invisible nodes before ranking, so it
  returns up to k visible nodes.
- **Cypher reads stay in the dataset.** With `USE GRAPH` or `graphs`,
  property filters, text predicates, projections and aggregates read only
  the dataset's graphs (before, only the `MATCH` patterns did).
- **Security fix:** `Query`, `QueryStream`, `CypherQuery`,
  `CypherQueryStream` and `VectorSeedQuery` with `as_of_tx_time` /
  `snapshot_ts` read every graph regardless of the caller's graph grants.
  They now apply the graph ACL like any other read.
- Inference runs (manual, settings change, background `--inference`) are
  serialized.

## Upgrade

No migration. Clients that send mixed graph and data operations in one
SPARQL Update request must split them. Clients relying on a later `WHERE`
seeing an earlier operation's writes in the same request must send the
operations as separate requests.
