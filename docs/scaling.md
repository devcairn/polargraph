# PolarGraph Scaling Guide

This document covers strategies for scaling PolarGraph DB, from single-node vertical scaling to read-replica horizontal scale-out.

---

## Single-node vertical scaling

The first and often sufficient path to more throughput is tuning the single instance.

### RocksDB block cache

By default, PolarGraph opens RocksDB with no explicit block cache configuration. For read-heavy workloads, add a block cache in `TripleStore::open`:

```rust
let mut table_opts = BlockBasedOptions::default();
table_opts.set_block_cache(&Cache::new_lru_cache(2 << 30)); // 2 GiB
```

Size the cache to the working set of hot triples. Rule of thumb: 20–50 % of the dataset for a read-heavy graph. Larger block caches cut disk I/O dramatically for repeated pattern scans.

### Bloom filters

A 10-bit bloom filter per SST file reduces point-lookup disk reads to near zero when the key is absent. Enable on the hexastore column families:

```rust
table_opts.set_bloom_filter(10.0, false);
```

The impact is highest on the SPO, PSO, and POS CFs that are hit by most subject/predicate/object-bound pattern queries.

### Compaction threads

Heavy write amplification from 6-CF fan-out can saturate the compaction thread pool. Raise it:

```rust
db_opts.increase_parallelism(8); // match core count
db_opts.set_max_background_jobs(8);
```

### Write buffer size

Each CF gets its own memtable. The default write buffer (64 MiB) may flush too frequently under bulk-import load. Increase with:

```rust
cf_opts.set_write_buffer_size(256 << 20); // 256 MiB
```

This trades peak memory for fewer flushes and less write stall.

---

## Read replicas (WAL streaming)

PolarGraph supports true network replication via **WAL streaming**. The primary exposes a server-streaming gRPC RPC (`StreamWal`) that tails the RocksDB write-ahead log. Each replica connects to the primary over gRPC, receives write batches, and applies them to its own independent RocksDB instance.

This replaces the earlier same-filesystem secondary-instance approach and enables true multi-host deployments with no shared storage requirement.

### Architecture

```
primary (polargraphd)
  │  opens DB read-write
  │  exposes StreamWal RPC
  │  WAL retention: 1 h / 512 MB
  │
  ├──[StreamWal stream]──→ replica-1 (polargraphd)
  │                          opens DB read-write internally
  │                          applies write batches via apply_replicated_batch
  │                          persists last_applied_seq to META CF
  │
  └──[StreamWal stream]──→ replica-2 (polargraphd)
```

**Sequence numbers** are RocksDB WAL sequence numbers. Every committed write batch has a monotonically increasing sequence number. Replicas persist `last_applied_seq` so they can resume exactly where they left off after a restart.

**At-least-once delivery.** The replica re-requests from `last_applied_seq` on reconnect. RocksDB write batches are idempotent when applied at the same sequence number (duplicate apply is harmless since MVCC deduplication takes the latest version).

**WAL retention.** The primary keeps WAL files for up to 1 hour / 512 MB (set at `TripleStore::open`). A replica that falls more than 1 hour behind will need to be re-bootstrapped from a backup.

### Consistency model

**Eventual, not linearizable.** A replica's view lags the primary by the time it takes a WAL entry to traverse the network and be applied (typically < 100 ms under normal conditions). This is appropriate for:

- Analytical queries over graph snapshots
- Vector similarity search workloads
- Read traffic that doesn't require seeing the very latest writes

It is **not** appropriate for:
- Read-your-own-writes patterns
- Any operation that must observe a specific committed timestamp

### Running a replica

```bash
# Primary (standard startup):
polargraphd --data-dir /data/primary --listen 0.0.0.0:50051

# Replica (WAL streaming mode):
polargraphd \
  --data-dir /data/replica1 \
  --listen 0.0.0.0:50052 \
  --replica-of http://primary-host:50051
```

`--replica-of URL` is the primary's **gRPC address**. No shared filesystem is required — the replica and primary can be on completely different machines.

`--data-dir` for the replica is a **local** directory where the replica's independent RocksDB instance stores its data.

### Environment variables

| Flag | Env variable | Default |
|---|---|---|
| `--replica-of URL` | `POLARGRAPH_REPLICA_OF` | *(none; primary mode)* |

### Reconnection and backoff

The WAL replication client (`WalReplicationClient`) reconnects automatically on any network error, using exponential backoff starting at 1 second and capped at 30 seconds. The reconnection loop runs indefinitely; the replica will resume replication as soon as the primary is reachable again.

After each successful reconnect the replica calls `StreamWal(since_seq = last_applied_seq)` so it picks up exactly where it left off, with no data loss.

### In-memory state refresh

When a replica applies a write batch it also refreshes its in-memory structures:
- Predicate interning maps (`fwd`/`rev`) — needed for query planning
- MVCC oracle timestamp — so reads at `snapshot_ts=0` reflect the new data

This refresh happens synchronously inside `apply_replicated_batch`, so a query issued immediately after a batch is applied will see the new data.

### Checking replica status

Use the `ReplicaStatus` RPC:

```
grpcurl -plaintext localhost:50052 polargraph.v1.PolarGraphService/ReplicaStatus
```

Response:
```json
{
  "isReplica": true,
  "primaryAddress": "http://primary-host:50051",
  "lastCatchupAt": 1718000000000000,
  "catchupCount": "4823",
  "lastAppliedSeq": "9182",
  "replicationLagEntries": "3"
}
```

- `lastCatchupAt` — Unix microsecond timestamp of the most recent successfully applied WAL batch.
- `catchupCount` — total WAL batches applied since server start.
- `lastAppliedSeq` — the RocksDB WAL sequence number last applied. Persisted across restarts.
- `replicationLagEntries` — approximate number of WAL entries the replica is behind the primary (computed as `primary_latest_seq - last_applied_seq`).

### Write operations on a replica

All write RPCs (`Insert`, `InsertVector`, `BatchInsertVectors`, `RegisterNodeType`, `RegisterEdgeType`, `CreateBackup`, `RunRetention`) return `FAILED_PRECONDITION` on a replica:

```
status: FailedPrecondition
message: "write operations are not supported on a read replica"
```

`StreamWal` itself also returns `FAILED_PRECONDITION` on a replica — only the primary can serve WAL streams.

---

## Deployment patterns

### Primary + N replicas behind a load balancer

```
clients
  │
  ├──[write]──→ polargraphd (primary)   :50051
  │
  └──[read]──→ load balancer            :50060
                  ├── polargraphd (replica-1)  :50052
                  ├── polargraphd (replica-2)  :50053
                  └── polargraphd (replica-3)  :50054
```

Route writes explicitly to the primary. Route reads to any replica (or the primary). The load balancer should health-check using gRPC reflection or the `ReplicaStatus` RPC.

### CQRS (command/query responsibility segregation)

For applications that already separate the write path from the read path:

- **Command service** → `polargraphd` primary on a write-optimised instance.
- **Query service** → `polargraphd` replicas on read-optimised instances (more RAM for block cache, SSD for low-latency scans).

This decouples scaling dimensions: write throughput scales with the primary's single-node capacity; read throughput scales horizontally with the replica count.

### Docker Compose example

```yaml
services:
  polargraphd-primary:
    image: polargraph:latest
    volumes:
      - primary_data:/data
    ports:
      - "50051:50051"

  polargraphd-replica:
    image: polargraph:latest
    volumes:
      - replica_data:/data
    command: >
      polargraphd
        --data-dir /data
        --listen 0.0.0.0:50052
        --replica-of http://polargraphd-primary:50051
    ports:
      - "50052:50052"
    depends_on:
      - polargraphd-primary

volumes:
  primary_data:
  replica_data:
```

No shared volume is needed. Each service has its own independent data volume.

---

## Failure modes and limitations

### Replica falls behind WAL retention window

If a replica is offline for more than the WAL retention period (1 hour by default), its `last_applied_seq` will no longer be available in the primary's WAL files. `StreamWal` will return an error when the replica reconnects.

**Recovery**: restore the replica from a backup (see `docs/architecture.md`), then restart it. The replica will start replication from the backup's sequence number.

### Primary restart

The primary's WAL TTL persists across restarts. Replicas will reconnect and resume from their `last_applied_seq` seamlessly as long as the gap is within the retention window.

### No automatic promotion

If the primary goes down, replicas continue serving reads from the last applied state but cannot be automatically promoted to primary. Failover requires:

1. Stop the replica.
2. Open its `--data-dir` as a primary (`polargraphd --data-dir /data/replica ...` without `--replica-of`).

Any writes committed to the original primary after the last applied WAL batch will be lost.

### Write amplification with 6 CFs

Every triple write goes to 6 column families (hexastore). This is by design but means write throughput is roughly 1/6 of a single-CF store at the same hardware. Replicas inherit this — each WAL batch applied on the replica performs the same 6-CF fan-out that was done on the primary.

---

## Why graph sharding is hard

Horizontal write scaling via sharding is significantly more complex for graph workloads than for document or key-value stores:

**The edge-cut problem.** Any partition strategy splits nodes across shards. Edges that cross the partition boundary require cross-shard lookups. A traversal query that follows 3 hops may touch all N shards in the worst case, negating the benefit of distribution.

**Traversal penalty.** SPO/PSO/OSP scans follow the index structure. Once a traversal crosses a shard boundary, latency spikes from microseconds (local RocksDB read) to milliseconds (network round-trip). Multi-hop queries compound this badly.

**UUID v7 IDs don't help.** The IDs are time-ordered for storage locality within a single instance, but a consistent hash ring over `NodeId` still cuts edges freely.

**When sharding is worth it.** Sharding makes sense only when:
- The dataset exceeds single-node capacity (tens of billions of triples or several TiB).
- The workload is dominated by 1–2 hop queries or aggregations, not deep traversals.
- The graph has a natural partition structure (e.g., tenants, time windows) that avoids edge cuts.

Until those conditions are met, read replicas provide the most practical scale-out path.
