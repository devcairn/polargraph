# Design: v3 storage layout (engine step 3)

**Status:** Approved 2026-09-30 (decisions A–E as recommended in §9) — in implementation
**Date:** 2026-09-29
**Plan refs:** `docs/contxtbroker-platform-plan.md` §1.2 (D1), §1.3, §2.2–2.4, §2.9 (D3)

Step 3 is the only change in the plan that rewrites every key on disk. This
note pins down the exact formats and the migration so they can be reviewed
once, before code. Everything here lands together as migration **v3**.

---

## 1. What changes

| | v2 (today) | v3 |
|---|---|---|
| Hexastore key | `[a][b][c][tt]`, 44 B | `[a][b][c][g][tt]`, 48 B |
| Property object slot | `0xFF×16` sentinel | `value_hash` (16 B) |
| Values per `(s,p)` | one | any number |
| Graph-leading CFs | — | `gspo`, `gpos` |
| Large values | inline, ×6 | out of line in `blob`, once |
| CF writes per triple | 6 | 8 |

## 2. Graph identity

- Graph IRIs are interned to `u32` in `meta`, exactly like predicates:
  `graph_fwd:{iri} → id`, `graph_rev:{id} → iri`, `graph_ctr → next`.
- `g = 0` is the **default graph**; every v2 triple migrates there, and every
  existing API that doesn't name a graph reads and writes it.
- Graph IDs are big-endian in keys so graph-leading scans sort numerically.

## 3. Key layouts

`g` sits immediately before `tt`, so all versions of one quad still sort
together and snapshot/MVCC/retention logic only moves its offsets.

```
spog  [s16][p4][o16][g4][tt8]   48
sopg  [s16][o16][p4][g4][tt8]   48
psog  [p4][s16][o16][g4][tt8]   48
posg  [p4][o16][s16][g4][tt8]   48
ospg  [o16][s16][p4][g4][tt8]   48
opsg  [o16][p4][s16][g4][tt8]   48
gspo  [g4][s16][p4][o16][tt8]   48   "everything in g", "g's view of s"
gpos  [g4][p4][o16][s16][tt8]   48   "in g, who has p = o"
```

- Tuple prefix (all versions of one quad) = first **40** bytes in every CF.
- For properties, `o` is the `value_hash` (§4).
- Union (all-graph) reads use the six non-`g`-leading CFs and filter on the
  4 `g` bytes at offset 36 — the same offset in all six. Results are
  de-duplicated on `(s,p,o)` unless the query binds a graph variable.

Ancillary CFs gain `g` before `tt` (or before the subject for `tri`, so graph
ACL applies to text-search candidates):

```
drv   [s16][p4][o16][g4][tt8]           48  (g = the derived graph)
epa   [edge16][p4][g4][tt8]             32
epo   [edge16][p4][o16][g4][tt8]        48
pea   [p4][edge16][g4][tt8]             32
tri   [trigram3][p4][g4][s16]           27  (no tt, as today)
blob  [value_hash16] → payload          (new, §5)
```

`meta`, `hnsw` and `iri` are unchanged.

## 4. Value hashes

`value_hash = xxh3_128(canonical(value))`, stored little-endian like
`NodeId::from_iri`. `canonical` is a fixed binary encoding, **not**
`serde_json` (whose float and escaping output isn't a stable contract):

| Value | Canonical bytes |
|---|---|
| `Null` | `00` |
| `Bool(b)` | `01 b` |
| `Int(i)` | `02` + i64 BE |
| `Float(f)` | `03` + f64 bits BE; `-0.0` → `0.0`, every NaN → one canonical NaN |
| `Text(s)` | `04` + UTF-8 |
| `Blob(b)` | `05` + bytes |
| `Vector(v)` | `06` + f32 LE each |
| `LangText{text,lang}` | `07` + lowercase(lang) + `00` + UTF-8 text |
| `Typed{lexical,dt}` | `08` + dt + `00` + lexical |

- Language tags hash lowercased (RDF 1.1 compares them case-insensitively)
  but are stored as written.
- **Relation vs property is decided by the value discriminant byte**, which is
  read with every key already. No key bit is reserved. A value hash equal to
  a real NodeId (2⁻¹²⁸) is caught because readers check the discriminant, and
  value lookups verify the decoded value.
- Value lookup `(?s, p, literal)` → `posg` prefix `[p][value_hash]`, or
  `gpos` `[g][p][value_hash]` within a graph.
- Current value(s) of `s.p` → `spog` prefix `[s][p]`; within one graph,
  `gspo` prefix `[g][s][p]`.

## 5. Values and out-of-line storage

| Disc | Layout | Bytes |
|---|---|---|
| `01` Relation | `[01][edge_id16][vt_start8][vt_end8]` | 33 (unchanged) |
| `02` Property | `[02][vt_start8][vt_end8][json]` | 17+N (unchanged) |
| `03` Vector | `[03][vt_start8][vt_end8][len4][f32…]` | unchanged |
| `04` **PropertyRef** | `[04][vt_start8][vt_end8]` | **17** |

- A property whose encoded payload exceeds `inline_value_max_bytes`
  (default **256**, `[storage]` config) is written once to
  `blob[value_hash] = payload` and every index entry carries a `04`
  PropertyRef. The hash is already in the key's object slot, so the ref
  needs no pointer of its own.
- Identical large values (boilerplate, repeated chunks) share one blob.
- Refs resolve lazily, only when a value is projected.
- **GC:** mark-and-sweep inside the retention run — collect every
  `value_hash` still referenced by a `04` entry in `spog`, delete other
  `blob` keys. Blobs are only orphaned when retention deletes their last
  version, so no refcounts to keep consistent.
- Vectors over the threshold (all realistic embeddings) go out of line too,
  which removes the 6× copy flagged in the audit.

## 6. Writes, conflicts and "replace"

With one key per value, replacing a property is explicit:

| Mode | Behaviour | Default for |
|---|---|---|
| `Replace` | close `vt_end` on every live value of `(s,p,g)` (same `vt_start`, `vt_end = new.vt_start`), then insert | `Insert` RPC properties (today's behaviour), Cypher `SET`, REST `/insert` |
| `Add` | insert alongside existing values | RDF import, SPARQL `INSERT DATA`, bulk import |

- Proto: optional `PropertyWriteMode mode` on `PropertyTriple` (default
  `REPLACE`), `string graph` on `InsertRequest` (empty = default graph).
- Conflict detection: `Add` and relations check the exact 40-byte tuple;
  `Replace` checks the `gspo` prefix `[g][s][p]`, so two concurrent
  replaces of one property conflict while writes to different graphs don't.
- Retention's same-`vt_start` shadowing rule is unchanged; a replaced value's
  closing version shadows its original.

## 7. Migration v3

**Offline and explicit**, never automatic at startup:

```bash
polargraphd migrate --data-dir /data --backup-dir /backups   # server stopped
```

1. Refuse unless the store is at v2 and no server holds the lock; take a
   `BackupEngine` backup first (mandatory flag, like `--i-have-a-backup`
   when `--backup-dir` is absent).
2. Stream each v2 hexastore CF in key order; rewrite keys with `g = 0`,
   sentinel → `value_hash`, large values → `blob` + PropertyRef; build
   `gspo`/`gpos` from `spo`. Same for `drv`, `epa`, `epo`, `pea`, `tri`.
   Write via `SstImporter`-style sorted SST files into **new** CFs (`spog`, …).
3. Verify: per-CF entry counts equal across all eight new CFs and match the
   v2 `spo` count; checksum of decoded `(s,p,o|value,tt,vt)` tuples equal
   between `spo` and `spog`.
4. Commit point: set `__migrations__/version = 3` in `meta`. Then drop the
   v2 CFs. A crash before the version write leaves v2 intact and the new CFs
   are discarded on the next attempt (rerunnable); after it, only the drop
   remains and is retried on open.
5. A v3 binary opening v2 data refuses to start with a message pointing at
   `polargraphd migrate`. Replicas re-bootstrap from a post-migration backup.

No splitting is needed: v2 could only hold one value per `(s,p)` at a given
`tt`, and distinct historical values simply become distinct keys.

## 8. Rollout inside the codebase

Order of PRs on the branch (each green on its own):

1. `keys`: v3 encoders/decoders + canonical value hash, behind the existing
   `keys::` accessors (step 1 already routed widths through them).
2. `codec` PropertyRef + `blob` CF + lazy resolution.
3. Store/transaction write path (8 CFs, `Replace`/`Add`, graph interning,
   conflict rules) and read paths (snapshot scans, property history,
   text search, retention, SST import) on v3 keys.
4. `polargraphd migrate` + v2 reader + verification + tests on a v2 fixture.
5. Benchmarks vs v2: insert throughput (budget ≤ 30% regression), union-query
   latency (≤ 10%), single-graph `gspo` load of 1K quads (p50 ≤ 2 ms).

Graph-aware query surfaces (`GraphTerm`, graph RPCs, SPARQL datasets, ACL
bitmaps) follow in steps 4–6 and don't change the on-disk format again.

## 9. Decisions (approved 2026-09-30 as recommended)

| # | Question | Decision |
|---|---|---|
| A | Default property write mode for the existing `Insert` RPC | `Replace` — keeps current client behaviour; RDF paths use `Add` |
| B | Migration trigger | Explicit offline `polargraphd migrate`, never automatic |
| C | Inline threshold | 256 bytes, configurable |
| D | Blob GC | Mark-and-sweep during retention |
| E | CF names | New names (`spog`…) so v2 and v3 data can coexist during migration and a store's version is visible from its CFs |
