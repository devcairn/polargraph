# Step 9 — queryable inference, DRed, int8 vectors, STATS — design note

Status: decisions B–G **approved as recommended** (Mark, 2026-10-02); A
changed: **one PR**, parts 9a → 9b → 9c → 9d as separate commits, in order,
on `db/step9-design`.

## 0. Finding

OWL 2 RL materialization (`polargraph-storage::owl_rl`) writes derived facts
to the `drvg` CF, and **no query path reads that CF** — Datalog, SPARQL,
Cypher and SHACL never see inferred facts; only the materializer does. The
derived facts also carry no graph (so the graph ACL can't apply), and each
run rescans the whole store per fixpoint iteration. So inferred facts must
become queryable (9a) before incremental maintenance (9b) is worth having.
It also means the Cypher / RDF decision D ("subclass instances via OWL RL")
doesn't hold yet; 9a makes it hold.

## 9a — Inferred facts are queryable

**Inferred graphs.** Inferred facts are ordinary quads in **inferred
graphs**, written through the normal commit path (all 8 orders, change log,
WAL), so every reader — scans, Datalog, SPARQL, Cypher, SHACL, exports,
`Subscribe`, the type index — sees them with no new read path.

- Source graph `g` (IRI `G`, or the default graph) has the companion
  inferred graph `urn:pg:inferred:<G>` (`urn:pg:inferred:default`), created
  on first use, recorded in graph metadata (`urn:pg:inferredFrom <G>`).
- A fact goes to the inferred graph of its **instance (A-box) premises'**
  graph. Schema (T-box) premises — `rdfs:subClassOf`, `subPropertyOf`,
  `domain`, `range`, `owl:inverseOf`, `owl:SymmetricProperty`,
  `owl:TransitiveProperty` declarations — don't
  decide the graph, so a schema kept in an ontology graph still yields
  per-graph inferences. (Consequence, documented: a user who can read a data
  graph sees inferences that used schema from a graph they can't read.)
- Instance premises from **more than one graph** (`prp-trp`, `eq-trans`,
  `prp-symp`/`inv` chains across graphs) → the cross-graph inferred graph
  `urn:pg:inferred:cross`, which has no grants and so is **service-only**
  (deny-by-default does the rest).
- **ACL** (decision B): an inferred graph is readable by exactly those who can
  read its source graph (`GraphAccessIndex` adds each readable graph's
  companion to the user's readable bitmap). Nobody but the engine writes to
  inferred graphs: user writes to them are `PERMISSION_DENIED`, and graph
  admin operations (copy / move / drop) are refused on them.
- **Opt-out** (decision C): requests include inferred facts by default; a
  per-request `inferred: false` (Query / QueryStream, SPARQL `?inferred=false`,
  Cypher, `ValidateShapes`) excludes the inferred graphs from the snapshot
  (`Snapshot::without_graphs`). A store that never ran materialization has
  no inferred graphs, so nothing changes for it.
- **Materializer**: the rules move to a premise-lookup interface (store
  snapshot + in-memory deltas) so full and incremental runs share them; full
  materialization computes the closure, then **diffs** against the live
  inferred graphs and commits only the changes (closing facts no longer
  derived — decision E — and asserting new ones). Author
  `urn:pg:inference`. `RunMaterialization` keeps its API (`clear_first` now
  means "recompute and diff", the result being the same).
- The legacy `drvg` CF is no longer written or read; existing contents are
  ignored (re-run materialization). No migration.

**As built (9a).** Two more findings fixed: the materializer named
properties and classes with a hash in a different byte order from
`term::iri_to_node_id` (so schema loaded as RDF never matched any rule), and
re-scanned the whole store per fixpoint iteration. Rules now take the
schema closed up front (rdfs5 / rdfs11 by labelled transitive closure), run
semi-naive once, and name properties via interned predicates or the IRI
dictionary. `owl:sameAs` facts are instance data. Opt-out field:
`exclude_inferred` (proto booleans default to false); REST bodies
`"inferred": false`, SPARQL `?inferred=false`.

## 9b — DRed (incremental maintenance)

Decision D: a background task on the primary consumes the change log
(`changes_after`), batching commits in a **1 s window**:

1. Ignore commits by `urn:pg:inference` (its own writes) and quads in
   inferred graphs.
2. **Schema change** (a T-box quad asserted or closed) → full recompute-and-
   diff (9a). (The plan's "scoped re-run" is approximated by the diff: only
   facts whose truth changed are written.)
3. Otherwise **DRed** over the batch's instance changes:
   - *Over-delete*: from each closed base fact, find inferred facts with a
     derivation using it (one rule step), transitively through inferred
     facts.
   - *Re-derive*: keep over-deleted facts that still have a derivation from
     the remaining facts.
   - *Insert*: semi-naive from asserted base facts and re-derived facts.
   - Close what's gone (`vt_end`), assert what's new — one commit.
4. Resume point (last processed commit ts) persists in META; below the
   change-log floor → full recompute-and-diff.

Enabled with `--inference` (`POLARGRAPH_INFERENCE`, `[storage] inference`);
`--auto-materialize` keeps its meaning (full run at startup) and implies
`--inference`. Replicas receive inferred quads by WAL. Metrics:
`polargraph_inference_lag_seconds`, `…_batches_total`, `…_closed_total`,
`…_asserted_total`. The 12 current rules stay.

## 9c — int8 vector quantization

Decision F: per-space opt-in, `VectorSpaceDef.quantization = "int8"`.

- Each vector is scaled independently: `scale = max|x_i| / 127`, codes
  `round(x_i / scale)` as `i8`; no calibration pass, inserts stay
  incremental.
- HNSW traversal computes distances on the codes (integer dot products;
  cosine from the per-vector norms of the codes).
- Search re-ranks the top `ef` candidates against the full f32 vectors —
  kept on disk (the mmap `.vecs` file; memory-mode spaces gain one) — and
  returns exact cosine scores.
- Codes persist alongside graph topology in the `hnsw` CF; ~4× less vector
  RAM (384-dim: 1,536 → 388 bytes per vector).
- Existing spaces stay f32. Re-registering a space with `quantization`
  converts it on its next insert (full vectors move to the `.vecs` file,
  every node gets codes). PQ later.
- Recall checked in `tests/vector_int8.rs`: recall@10 vs brute-force exact
  cosine ≥ 0.97 (measured 1.0 on 2,000 × 64-dim random vectors, `ef` 100),
  with exact returned scores.

**As built (9c).** `SpaceOptions { mode, int8 }` (from `StorageMode` for
existing callers) on `insert_vector` / `batch_insert_vectors`; codes under
`<space>/q/<id>`, marker `<space>/__q`; missing codes are recomputed on
open. Traversal compares int8 codes (integer dot product over the codes'
norms — per-vector scales cancel in cosine); `search` re-ranks its
`max(ef, k)` candidates exactly; `search_in_set` is exact.

## 9d — `STATS` CF: counters

Decision G: a small, generic primitive; how counters feed ranking is
ContxtBroker's.

- New CF `sts` (created on open, like `chg`): key `[namespace][0x00][node:16]`
  → `i64`, updated with a RocksDB **merge operator** (atomic add, no read,
  no MVCC versions).
- RPCs `IncrementCounters { namespace, increments: [{node, delta}] }` and
  `GetCounters { namespace, nodes }` (plus REST `POST /counters`,
  `GET /counters`); service-only (user callers `PERMISSION_DENIED`); writes on
  the primary, replicated by WAL.
- Not versioned, not in the change log, not covered by retention.

## Delivery

One PR, commits in order: 9a (inferred graphs, opt-out, ACL, diffing
materializer), 9b (DRed task), 9c (int8), 9d (counters), then docs. Full
pre-PR checks including e2e.

## Out of scope

Per-fact ACL for cross-graph inferences, more OWL RL rules, PQ, counter
retention / decay.
