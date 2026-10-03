# Step 9 — release note and upgrade guide

Design: `docs/design/step9-inference-vectors-stats.md`.

## Release note

- **Inferred facts are queryable.** OWL 2 RL inference now writes ordinary
  quads into inferred graphs (`urn:pg:inferred:<source graph>`,
  `urn:pg:inferred:default`, service-only `urn:pg:inferred:cross`), so
  Query, SPARQL, Cypher, SHACL, exports and `Subscribe` see them **by
  default**. Before, materialized facts went to the `drvg` column family,
  which no query read. Opt out per request with `exclude_inferred` (gRPC),
  `"inferred": false` (REST bodies) or `?inferred=false` (SPARQL).
- **Results change for stores that run inference**: a class's instances
  include its subclasses' (`SELECT ?a WHERE { ?a a :Animal }`, Cypher
  `MATCH (a:Animal)`), sub-properties imply super-properties, and so on.
  `Subscribe` streams include inference commits (author
  `urn:pg:inference`, graphs `urn:pg:inferred:*`).
- **Inference now fires on RDF-loaded schema.** The materializer hashed
  property and class IRIs differently from RDF import, so schema loaded as
  RDF matched no rule. Fixed.
- **Incremental inference**: `--inference` keeps the inferred graphs
  current (about one second behind) with DRed. `--auto-materialize` implies
  it.
- **`RunMaterialization`** recomputes and diffs (idempotent); `clear_first`
  is ignored. New response fields `asserted` / `closed`; `rules_fired` now
  equals `asserted`, `iterations` is 1 when anything changed.
- **int8 vectors**: `VectorSpaceDef.quantization = "int8"` — ~4× less
  vector RAM, exact returned scores. Opt-in per space.
- **Counters**: `IncrementCounters` / `GetCounters`, REST `/counters`.
- Inferred graphs can't be written, granted, copied, moved or dropped
  (`PERMISSION_DENIED`).

## Upgrade

No migration. The `sts` CF is created on open. If you used
materialization before, the old `drvg` contents are ignored: run
`RunMaterialization` (REST `POST /materialize`) once, or start with
`--auto-materialize` / `--inference`.
