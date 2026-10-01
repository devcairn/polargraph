# Trust-layer primitives — changesets, SHACL validation, graph diff (plan step 8, WS3) — design note

Status: **proposal, awaiting decisions A–F** (branch `db/ws3-proposals`).

## The split (approved by Mark, 2026-10-01)

The proposal / review / promotion workflow of plan §WS3 (3.1–3.3) is
ContxtBroker **product policy**: proposal status, who may approve, "model
proposals need human review", confidence thresholds, reviewer bookkeeping,
required provenance per graph kind. It will change with the product, so it
lives in a **ContxtBroker service** above the engine.

The engine provides general primitives that service — and any other client —
builds on:

| Engine (this step) | ContxtBroker service (not in this repo) |
|---|---|
| **Atomic changeset RPC** — adds + retractions across graphs, one transaction, optimistic precondition | Proposal graphs, `cb:status`, `cb:targetGraph`, retraction lists |
| **SHACL validation** over a graph, a dataset, or a dataset with an uncommitted overlay | Which shapes apply to which target; blocking vs advisory results |
| **Graph diff** (optional) | Conflict / supersession rules (cardinality-one handling) |
| Existing: named graphs, graph metadata, graph ACL (`propose` level stored), change feed with authors | Promotion policy (`autoApproveFor`, `requireReviewFor`, `minConfidence`), `cb:reviewedBy`, reject reasons |
| | Provenance profile enforcement (`prov:wasAttributedTo`, … on `CreateGraph` of its graph kinds) |

A promotion in the service then reads: diff (or compute changes) → validate
the target with the changes overlaid → `ApplyChanges` with the read time as
precondition → update proposal metadata. Concurrent writers can't half-apply
or interleave: the changeset commits atomically or fails `ABORTED`.

## 8a — `ApplyChanges`

```protobuf
message QuadRef {           // an exact quad to retract
  NodeId subject = 1; string predicate = 2;
  oneof object { NodeId node = 3; Value value = 4; }
  string graph = 5;         // "" = default graph
}
message ApplyChangesRequest {
  repeated GraphTriples adds = 1;      // { graph, repeated Triple }
  repeated QuadRef retractions = 2;    // closed at vt_end = now
  int64 read_ts = 3;                   // precondition (0 = none)
  bool strict = 4;                     // every retraction must match a live quad
  repeated string iris = 5;            // IRI dictionary bindings
  string user_id = 6;                  // author + graph ACL
}
message ApplyChangesResponse {
  int64 commit_ts = 1; uint64 added = 2; uint64 retracted = 3;
  uint64 retractions_not_found = 4; repeated bytes edge_ids = 5;
}
```

- One transaction, one commit timestamp, one change-feed entry (author =
  caller), graph ACL `write` on every graph touched (unknown graphs are
  created only for service calls, as for `Insert`).
- **Precondition**: with `read_ts`, the transaction reads at `read_ts`, so the
  existing MVCC write-write check fails the commit (`ABORTED`) if any quad it
  adds or retracts — or, for single-valued (`Replace`) property adds, any
  value of that `(s, p, g)` — was committed after `read_ts`. This is the
  "nothing I looked at changed" guarantee a promotion needs, with no new
  versioning machinery.
- Retractions close exactly the named quad in exactly the named graph.
  Also closes the gap that `DeleteTriples` can't join a wire transaction.

## 8b — `polargraph-shacl` + `ValidateShapes`

New crate (depends on `polargraph-core` and `polargraph-query`): shapes are
read from quads, compiled to violation checks over a `Snapshot`, and produce a
report. Supported subset (v1), as plan §3.4: node/property shapes;
`targetClass` / `targetNode` / `targetSubjectsOf` / `targetObjectsOf`;
predicate, inverse and sequence paths; `minCount` / `maxCount`; `datatype`,
`class`, `nodeKind`; `min/maxInclusive`, `min/maxExclusive`; `pattern`,
`min/maxLength`; `in`; `node`, `closed` + `ignoredProperties`; severities.
Deferred: `sh:sparql`, `qualifiedValueShape`, full path algebra.

```protobuf
message ValidateShapesRequest {
  repeated string shapes_graphs = 1;   // graphs holding the shapes
  repeated string data_graphs = 2;     // dataset to validate ("" = default; empty = union)
  repeated GraphTriples overlay_adds = 3;       // uncommitted changes to
  repeated QuadRef overlay_retractions = 4;     //   validate as if applied
  int64 read_ts = 5; string user_id = 6;
}
message ValidateShapesResponse {
  bool conforms = 1; repeated ValidationResult results = 2;
}
// ValidationResult: focus_node, path, value, source_shape,
//   constraint_component, severity, message
```

With an overlay, only focus nodes the overlay touches (subjects, objects, and
nodes whose targets may change) are validated, so cost scales with the
change, not the graph.

## 8c — `DiffGraphs` (optional)

`DiffGraphs(source, target)` → quads only in `source`, quads only in
`target`, and `(s, p)` pairs present in both with different values. Purely
set-based; what counts as a conflict stays the service's call.

## Decisions needed

| # | Question | Recommendation |
|---|----------|----------------|
| A | Delivery | Two PRs: **8a** `ApplyChanges`, **8b** SHACL crate + `ValidateShapes`. `DiffGraphs` as a small **8c** only if the service wants it (it can compute diffs itself from `ExportGraph`). Registry derivation from shapes (§3.5) deferred to a later step. |
| B | Precondition semantics | `read_ts` + the existing MVCC write-write check (per touched quad, plus `(s, p, g)` for single-valued property adds), as above. No separate per-graph version counters. |
| C | Retraction that matches nothing | Counted in `retractions_not_found`, not an error; `strict = true` makes it `FAILED_PRECONDITION` and nothing is applied. |
| D | Changeset size | At most **100 000** adds + retractions per call (`RESOURCE_EXHAUSTED` beyond), keeping it one bounded transaction. |
| E | Validation report format | Structured results in the RPC response (and JSON over REST `POST /validate`); a Turtle `sh:ValidationReport` via `Accept: text/turtle`. The engine never stores reports — the service decides where they go. |
| F | Provenance enforcement | **Dropped from the engine** — the service requires provenance on the graphs it creates. The engine keeps recording authors in the change feed. |

No storage migration in any part.
