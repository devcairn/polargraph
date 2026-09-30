# Graph-level access control (plan step 6, §2.8) — design note

Status: **proposal, awaiting decisions A–F** (branch `db/ws2-graph-acl`).

## Today

- Identity is `QueryRequest.user_id` / `x-polargraph-user-id` (REST
  `X-User-Id`). It is **asserted by the client**: the API key authenticates
  the caller as a service, not the user.
- `AccessCache: HashMap<user, HashSet<NodeId>>` built from `MEMBER_OF`,
  `HAS_ACCESS`, `HAS_ACCESS_TYPE`; results are post-filtered so every bound
  node must be allowed.
- Applied only to `Query`, `CypherQuery`, `VectorSeedQuery`,
  `SearchVectorFiltered`. Not applied to `QueryStream`, `CypherQueryStream`,
  SPARQL (REST doesn't forward the user), `ExportGraph`, `/export/*`,
  `GraphStats`, `ListGraphs`.
- A request with no user, **or a user with no grants**, sees everything.

## Proposal

1. **Grants.** `(principal) -[HAS_GRAPH_ACCESS]-> (graph IRI node)` where the
   principal is a `User` or `Group`, stored in the system graph
   `urn:pg:graph:meta`. The level is a property of the grant edge,
   `GRAPH_ACCESS_LEVEL` ∈ `read` < `propose` < `write` < `admin` (each level
   implies the ones below it). New RPCs `GrantGraphAccess` /
   `RevokeGraphAccess` / `GetGraphAccess` (+ REST) write and read these, so
   clients never hand-craft grant triples.
2. **Cache.** `GraphAccessCache: HashMap<user, GraphGrants>` with one
   `RoaringBitmap` per level (graph ids are `u32`), rebuilt like today's
   cache when grant, membership or graph triples change.
3. **Enforcement in the scan.** `Snapshot` gains an optional
   `Arc<RoaringBitmap>` of readable graphs. `snapshot_scan_keyed` checks the
   4-byte graph slot of each key before decoding the value; graph-leading
   scans (`gspo`/`gpos`) skip whole disallowed graphs. Because every read
   path goes through a `Snapshot`, this covers Datalog, Cypher (including
   property filters/projections), streaming, SPARQL, exports and graph
   stats in one place. Union reads de-duplicate after filtering.
4. **Vector search.** HNSW isn't graph-aware: hits are kept only if the node
   has a live quad in a readable graph (k small, so one scoped lookup each).
5. **Writes.** `user_id` added to `Insert`, `CypherWrite` and the graph RPCs;
   when present, writing into a graph requires `write`, and
   create/copy-into/move/drop/grant require `admin` on the target.
   `propose` is stored now and used by the proposal RPCs in step 8.
6. **Node-level grants** keep working as a post-filter on top, as §2.8
   says.

## Decisions needed

| # | Question | Recommendation |
|---|----------|----------------|
| A | Enforcement switch | Server flag `--graph-acl off\|enforce` (TOML `[auth] graph_acl`), **default `off`** so existing deployments are unchanged. |
| B | Requests **without** a user id when enforcing | Treated as trusted service calls (full access), because the user id is client-asserted anyway; operators who need more should bind users to API keys (later). |
| C | A user **with no graph grants** when enforcing | Sees only the readable-by-all graphs (D) — deny by default. (Today's node ACL is allow-by-default; the graph ACL shouldn't copy that.) |
| D | Default graph and system graph | Default graph readable by everyone (legacy/untagged data); writes to it need no grant. System graph readable only for metadata of graphs the user can read; grant triples readable by `admin`s of that graph. |
| E | Grant storage shape | As in 1: grant edge in the system graph with a `GRAPH_ACCESS_LEVEL` property (not four separate predicates), managed through RPCs. |
| F | Scope of this step | Reads everywhere (3, 4) + write/admin checks when a user id is present (5). `propose` semantics wait for step 8. Binding user identity to API keys stays out of scope. |

New dependency: `roaring` (pure Rust, widely used).
