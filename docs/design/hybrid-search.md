# Hybrid search over large candidate sets (finding #2) and the default `ef` (finding #5)

Status: **draft for decisions** (Mark picked #2 with #5 folded in,
2026-10-07). No code until the decisions in §7 are made.

## 1. The problem

The plan's retrieval shape is "rank the chunks of records that mention an
entity" (plan § Capacity, hot spots: `find_records` ranks within the
mention set via `SearchVectorInSet`). cb-bench measures it as: mention set
by query (`?r cb:mentions <e> . ?c cb:chunkOf ?r`, as the user), then
`SearchVectorInSet` k = 20 over those chunks.

| | Scale 0.05 | Scale 1 (team) | Target |
|---|---|---|---|
| Hybrid search k=20, p95 | 256 ms | 2,681 ms (f32), 2,959 ms (int8) | 25 ms |
| Context assembly (describe + hybrid + text), p95 | 260 ms | 2,253 ms | 300 ms |
| Mention-set size, median / max | 9,800 / 28,760 | 13,720–24,860 / 86,020 | — |

Mentions are Zipf-skewed (s = 1.1), so p95 is set by popular entities with
tens of thousands of chunks.

## 2. Where the time goes (team scale)

Measured 2026-10-07 (cb-bench `--scale 1`, Apple M4 Pro, 48 GiB): the
same in-set call timed as the user and again as a service call (no
visibility check), over the same mention sets.

| | f32 p50 / p95 | int8 p50 / p95 |
|---|---|---|
| Whole hybrid search (set + search) | 798 / 3,683 ms | 382 / 2,412 ms |
| Building the set (`Query`, mention → chunks) | — / 333 ms | — / 179 ms |
| `SearchVectorInSet` as the user | 782 / 3,220 ms | 343 / 2,181 ms |
| `SearchVectorInSet` without the visibility check | 6.4 / 65 ms | 9.0 / 60 ms |

So the visibility check is **~95 % of the search** and ~85 % of the whole
request; building the set is most of the rest; scoring 86K candidates
costs at most ~60 ms (and that includes converting 86K ids from proto).

Three costs, in order:

1. **Per-node visibility check in `SearchVectorInSet`.** For every
   candidate, `node_visible_in` (`crates/polargraph-server/src/service.rs:1010`)
   opens a snapshot and scans every quad of the subject to see whether one
   is in a graph the caller may read. It runs even though the candidates
   came from a query that the same graph ACL already filtered.
2. **Building the set.** The mention → chunk join runs as a separate
   `Query`, and up to 86K bindings are converted to proto and back before
   the search sees them.
3. **Scoring** is not the problem: an exact scan of 86K vectors is a few
   milliseconds (f32 dot products, or int8 codes with exact re-ranking).

## 3. Options

### A. Visibility

- **A1 — batch the check.** One snapshot per request and an early-exit
  seek per node instead of a full subject scan. Still O(set) seeks:
  roughly a few µs per node, ~0.2–0.4 s for 86K. Not enough alone.
- **A2 — don't re-check sets the engine built.** If the candidate set is
  produced inside the engine by an ACL-scoped evaluation (B2), every member
  is already visible; no check at all. Caller-supplied ids
  (`SearchVectorInSet` today) still need one.
- **A3 — graph membership bitmaps per vector space.** Give every vector
  node a dense `u32` id in its space; keep, per graph, a `RoaringBitmap`
  of the dense ids whose node has a live quad in that graph. Visible set
  for a caller = OR of the readable graphs' bitmaps (cached per readable
  set); checking a node is a bitmap lookup (nanoseconds). Maintained from
  the change log the way `TypeIndex` is (catch up on read; rebuild by scan
  below the log floor; works on replicas). Memory: compressed bitmaps,
  well under 1 byte per vector per graph for clustered ids.

### B. Building the candidate set

- **B1 — as today**: the caller runs a `Query`, then passes ids.
- **B2 — candidates as patterns, evaluated in the engine.** A search
  request carries `candidate_patterns` (the same `VarPattern`s as `Query`)
  and the variable to rank (`?c`). The server evaluates them against the
  caller's snapshot straight into a set of dense ids — no proto round
  trip, no visibility re-check (A2) — then scores. This is the plan's
  "rank within the mention set" as one RPC, and stays generic (any pattern
  shape, not mention-specific).
- **B3 — cached candidate bitmaps.** Cache B2's result keyed by (patterns
  with bound values, dataset, readable-graph set), invalidated through the
  change log when a quad with one of the patterns' predicates changes
  (per-predicate epochs). Popular entities are asked about repeatedly, so
  the hot tail becomes cache hits. Adds memory and invalidation
  complexity; only worth it if B2 alone misses the target.

### C. Scoring

- **C1 — brute force over the set**: exact for f32; for int8, integer dot
  products over codes, then exact re-ranking of the top `4k` with full
  vectors. Linear in the set: measured ≤ 65 ms p95 for up to 86K candidates today (including proto
conversion and a `HashMap` lookup per id); a flat array indexed by dense
id (A3) should bring an 86K int8 scan to single-digit milliseconds.
- **C2 — filtered HNSW traversal**: search the graph normally but only
  admit nodes in the allowed bitmap, widening `ef` as the filter gets
  selective. Wins when the set is a large fraction of the space (e.g.
  "ANN within these graphs", or the `graphs` filter, which today
  over-fetches and post-filters). For small sets, traversal wastes work
  and recall degrades.
- **Rule**: brute force when `|set| ≤ T`, filtered traversal above; `T`
  from measurement (expected in the 100K–500K range for 384-dim int8).

### D. Limiting the candidate set

Capping candidates (most recent N records, importance ranking) is a
retrieval policy, so it belongs to the application (plan: "never
materialize the full fan-out"). The engine can offer `max_candidates` with
an ordering key on B2 so the cap is applied before scoring, but the
choice of key is the app's.

## 4. Interactions

- **Graph ACL.** Semantics unchanged: a node is visible iff it has a live
  quad in a readable graph. A3's per-graph bitmaps intersect the same
  readable-graph bitmap scans use (inferred companion graphs included).
  B2 evaluates under the caller's snapshot, so it can't see more than
  `Query` would. Caller-supplied ids keep a check (A3 makes it cheap).
- **int8.** C1 over codes with exact re-ranking from the mmap file; dense
  ids (A3) also give codes a flat array layout, which speeds scans.
- **Change feed / caching.** A3 and B3 catch up from `chg` on read, like
  `TypeIndex`: correct for every write path and on replicas, with a scan
  rebuild below the change-log floor. Time-travel reads (`as_of`) bypass
  the caches and use the slow path.
- **Vector writes.** Inserting a vector assigns its dense id; deleting a
  node's last quad in a graph clears its bit (via the change log), the
  vector itself stays (as today).

## 5. The default `ef` (finding #5)

Measured at 2M clustered 384-dim vectors (team scale), k = 20, 40
queries, recall@20 against exact search; latency is the search alone
(in-process):

| `ef` | f32 recall | f32 p50 / p95 | int8 recall | int8 p50 / p95 |
|---|---|---|---|---|
| 50 (today's default) | 0.52 | 0.68 / 1.40 ms | 0.50 | 0.50 / 0.85 ms |
| 100 | 0.68 | 1.02 / 1.75 ms | 0.69 | 0.79 / 1.29 ms |
| 200 | 0.88 | 1.55 / 2.62 ms | 0.84 | 1.30 / 2.52 ms |
| 400 | 0.96 | 2.49 / 3.94 ms | 0.94 | 2.10 / 3.38 ms |
| 800 | 0.98 | 3.53 / 5.21 ms | 0.98 | 3.12 / 4.78 ms |

At 100K vectors every `ef` ≥ 100 already gives 1.0, so a fixed default
sized for the large spaces costs small spaces only fractions of a
millisecond.

Proposal: **default `ef` 400** (recall ≥ 0.94 at 2M for under 4 ms p95),
and an effective `ef` of `max(ef, 2·k)` so large `k` never searches with a
beam narrower than the result. Scaling `ef` with space size is not worth
the extra rule: the latency difference at small sizes is negligible.
Requests and the Cypher inline `ef=` keep overriding it; `ef` 800 is the
setting for recall-critical callers.

## 6. Expected impact

Estimates for the cb-bench hybrid scenario at team scale; to be
confirmed by cb-bench in the PR.

| Step | Today p95 | With A3 + B2 + C1 (est.) | With B3 too (est., hot entities cached) |
|---|---|---|---|
| Visibility | ~2,200–3,200 ms | ~0 (A2 / A3) | ~0 |
| Building the set | 179–333 ms | ~50–150 ms (in-engine join, no proto round trip) | ~1 ms on a hit |
| Scoring 86K | ≤ 65 ms | ~5–10 ms | ~5–10 ms |
| **Hybrid search p95** (target 25 ms) | **2.4–3.7 s** | **~60–160 ms** ❌ | **~10–20 ms** ✅ (cold misses still ~60–160 ms) |
| Context assembly p95 (target 300 ms) | 2.4 s | ~100–200 ms ✅ | ~40–60 ms ✅ |

The visibility fix alone takes hybrid search from seconds to the low
hundreds of milliseconds and brings context assembly under its target.
Hybrid search's 25 ms target at the hottest entities needs either B3
(cached candidate sets) or an application cap on candidates (D) — the
popular entities' mention sets are simply large (86K chunks at team
scale, ~20× that at company scale).

## 7. Decisions

| # | Decision | Recommendation |
|---|---|---|
| A | Visibility for vector candidates | **A3**: dense ids + per-graph bitmaps per vector space, caught up from the change log (`TypeIndex` pattern); plus **A2** (no check for engine-built sets). A1 alone isn't enough. |
| B | Candidate sets | **B2**: `candidate_patterns` + `rank_var` on `SearchVectorInSet` (ids still accepted), evaluated in the engine under the caller's snapshot. |
| C | Scoring | **C1** now (flat dense-id arrays; int8 codes + exact re-rank of the top `4k`). **C2** filtered traversal later, when the `graphs` filter or a type filter needs it. |
| D | Cache (B3) | **Measure first**: build B3 only if cb-bench after A + B still misses the 25 ms target on popular entities (expected), as a follow-up PR with its own numbers. |
| E | Candidate caps | Application policy; the engine doesn't cap. (Optional later: `max_candidates` with an ordering key on B2.) |
| F | Default `ef` | **400**, effective `max(ef, 2·k)`; same flag / env / TOML key. |
| G | Delivery | One PR: A + B + C1 + F, with cb-bench before/after (hybrid search, context assembly, recall/latency at the new `ef`); B3 separately if D says so. |
