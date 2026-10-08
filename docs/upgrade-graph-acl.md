# Graph-level access control — release note and upgrade guide

## Trust model

**The engine trusts the user id the calling application sends** (`user_id`,
`x-polargraph-user-id`, REST `X-User-Id`) and enforces graph access for that
id. It does not authenticate users: **the application is responsible for
authentication** and must forward only ids it has verified. The API key
authenticates the application; a call without a user id is a trusted
service call with full access. Verified identity (JWT from an IdP) is the
first item on the "Later / nice-to-have" list in [`docs/STATUS.md`](STATUS.md)
(Mark, 2026-10-08: "for now we trust what the app sends").

## Release note (breaking)

PolarGraph now enforces **graph-level access control** on every request that
carries a user id (`user_id` field, `x-polargraph-user-id` gRPC metadata, or
`X-User-Id` on the REST gateway). There is no switch to turn it off.

- **Deny by default.** A user reads the default graph plus the named graphs it
  (or one of its groups) has been granted. A user with no grants sees only the
  default graph.
- **Levels.** `read` < `propose` < `write` < `admin`; each level includes the
  ones below it. Writing into a named graph needs `write`; changing a graph's
  metadata, copying or moving into it, dropping it and managing its grants
  need `admin`. The default graph is readable and writable by everyone.
- **Everywhere.** Enforcement happens inside the storage scan, so it covers
  Datalog, Cypher (including property filters and projections), streaming
  queries, SPARQL (queries and updates), vector search, exports, graph
  listings and stats, edge annotations, property history and edge-id lookups.
- **Service calls are unchanged.** Requests without a user id are trusted
  service calls with full access, as before.

### What changes for user-scoped callers

| Before | Now |
|---|---|
| A user saw every graph (only node-level `HAS_ACCESS` post-filters applied, and only on four RPCs) | Named graphs are invisible until granted |
| REST forwarded `X-User-Id` only on `/query` and `/cypher` | Forwarded on every endpoint, so SPARQL, exports and imports are restricted too |
| A user insert could create a named graph | Users write only into existing graphs they have `write` on; unknown graphs are `PERMISSION_DENIED` (create them with `CreateGraph`, which makes the caller its admin) |
| A user's Cypher write / `DeleteTriples` touched every graph | Cypher writes without `USE GRAPH` stay in the default graph; `DeleteTriples` closes copies only in graphs the user can write |
| Users could write `MEMBER_OF` / `HAS_ACCESS*` triples and call `GrantAccess`, `RevokeAccess`, `AddUserToGroup` | Access-control triples and those RPCs are service-only (`PERMISSION_DENIED` for users) |
| Vector search returned any node | Users only get hits on nodes with a live quad in a readable graph (nodes that have only a vector are hidden from users) |

## Upgrade guide

### 1. Find your user-scoped callers

Anything that sends a user id is affected: application back ends that pass
`user_id` / `x-polargraph-user-id`, and HTTP clients that send `X-User-Id` to
`polargraph-rest`. Callers that never send a user id are unaffected.

### 2. Decide who should see which graphs

Data in the **default graph stays visible to everyone**, so a deployment that
doesn't use named graphs keeps working unchanged. For each named graph, pick
the users or (preferably) groups that need `read`, `write` or `admin`.

### 3. Bootstrap the first grants as a service

Only a service call (no user id) or an `admin` of the graph can grant access,
so the operator makes the first grants with the API key and **without** a
user id. Principals are node UUIDs or IRIs.

REST (no `X-User-Id` header; the gateway authenticates upstream with its
own `--api-key`):

```bash
# Make a group admin of a graph
curl -X POST http://rest:8000/graphs/access -H 'content-type: application/json' \
  -d '{"principal":"urn:group:kb-admins","graph":"https://cb.ai/graph/approved","level":"admin"}'
# Give a team read access
curl -X POST http://rest:8000/graphs/access -H 'content-type: application/json' \
  -d '{"principal":"urn:group:analysts","graph":"https://cb.ai/graph/approved","level":"read"}'
```

gRPC (`grpcurl`, no `x-polargraph-user-id`):

```bash
grpcurl -H 'authorization: Bearer <api-key>' -d \
  '{"principal":"urn:group:kb-admins","graph":"https://cb.ai/graph/approved","level":"admin"}' \
  polargraphd:50051 polargraph.v1.PolarGraphService/GrantGraphAccess
```

To grant a group on every existing graph, list them as a service
(`GET /graphs`) and loop over the IRIs. Put users in groups with
`AddUserToGroup` / `POST /access/add-user` (service-only).

From then on, graph admins can manage their own graphs' grants by calling the
same endpoints with their user id.

### 4. Verify

```bash
curl 'http://rest:8000/graphs/access?principal=urn:user:alice'   # effective access
curl -H 'X-User-Id: urn:user:alice' http://rest:8000/graphs         # what alice sees
```

### 5. Know the trust model

The user id is **asserted by the caller**; the API key authenticates the
service, not the user. Only give API keys to trusted services that set user
ids from their own authentication. The same applies to `polargraph-rest`: it
has no HTTP authentication of its own, so a client that reaches it and omits
`X-User-Id` makes a full-access service call — keep the gateway behind a
trusted front end that sets (and strips client-supplied) `X-User-Id`. Binding
users to API keys is planned separately.

### Rollback

Grants are ordinary bitemporal triples in the system graph
`urn:pg:graph:meta`; an older server simply ignores them. Rolling back to a
version without graph access control restores the old visibility.
