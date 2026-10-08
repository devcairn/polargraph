# Hybrid search and default `ef` — release note

Design: `docs/design/hybrid-search.md` (decisions A, B, C, F).

## Release note

- **`SearchVectorInSet` takes candidates as patterns.** Set
  `candidate_patterns` (the same `VarPattern`s as `Query`) and `rank_var`
  instead of `node_ids`; the server evaluates them as the caller (graph
  ACL applies, `graphs` is the dataset) and ranks the nodes bound to
  `rank_var`. Passing ids still works; using both is `INVALID_ARGUMENT`.
- **Faster graph-ACL checks on vector results.** Every vector RPC now
  checks candidates against an in-memory node → graphs index (built in the
  background at startup, kept current from the change log) instead of a
  store scan per node. Results are unchanged; until the index is built
  (seconds to a minute on large stores) the old per-node check is used.
- **int8 spaces** rank in-set candidates by their codes and re-rank the
  best `4k` exactly, so returned scores stay exact.
- **Default `ef` is 400 (was 50)**, and the effective `ef` is never below
  `2·k`. Better recall on large spaces (recall@20 at 2M vectors: 0.52 →
  0.96) for ~2–3 ms more per search there; small spaces barely change.
  `--default-vector-ef` / `POLARGRAPH_DEFAULT_VECTOR_EF` / `[query]
  default_vector_ef`, request `ef` and Cypher's inline `ef=` still
  override it (but not below `2·k`).

## Upgrade

No migration. To keep the old behaviour, start the server with
`--default-vector-ef 50`.
