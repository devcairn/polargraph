# WS4 type packages — application-only (superseded)

Status: **superseded** (Mark, 2026-10-03): "I actually think the
application should handle what schemas get queried too. I don't think we
should do anything with type packages." The earlier proposals — graph roles
(P1), registry derivation from shapes (P3), `DiffGraphs` with `as_of` (P4)
and a graph digest (P5) — are **dropped**. History: this file's git log.

## What stays in the engine

Type packages are entirely an application concern: manifest, registry,
fetching, resolution, lock files, the CLI, version pointers, migrations
orchestration, rendering. The application decides which graphs and schemas
each query reads, using the engine's existing graph scoping:

- `FROM` / `FROM NAMED` / `GRAPH` (SPARQL), `USE GRAPH` / `graphs`
  (Cypher), `graphs` / `GraphScope` (Datalog `Query`), explicit
  `shapes_graphs` / `data_graphs` (`ValidateShapes`).
- Installing a package is `CreateGraph` + `ApplyChanges` / `/import/rdf`.

Engine work kept from this analysis (general-purpose, not package-specific):

| | Work | Why the engine |
|---|---|---|
| P2 | **Atomic SPARQL Update** — one transaction per request, `dry_run` returning the changeset, optional `read_ts` precondition | REST SPARQL Update applies operations and triples one by one today; any caller can be left half-applied. |
| — | **Inference schema graphs** — a setting naming which graphs supply schema axioms (empty = all) | Inference is the one place the engine picks graphs by itself (it reads axioms from every graph). |
| — | **Graph-scoped search** — a `graphs` filter on vector search; Cypher reads (filters, projections, text predicates, aggregates) confined to the query's dataset | The other places the engine ranges over every graph regardless of the caller's scope. |

## Dropped, and whether the engine needs any for itself

- **P1 graph roles** — no: the application scopes queries.
- **P3 registry from shapes** — no. The advisory registry stays as is
  (`RegisterNodeType` / `RegisterEdgeType`); an application that keeps
  SHACL as its source of truth can call those RPCs.
- **P4 `DiffGraphs`** — no; diffs can be computed from `ExportGraph`. (Was
  the optional step 8c.)
- **P5 graph digest** — not now. It would help verify replicas or backups
  against a primary; revisit if that need appears.
