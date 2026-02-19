# Replication

## Goals

- Single-primary with streaming read replicas.
- Zero external dependencies for failover — no ZooKeeper, etcd, Patroni.
- Minimal configuration: list your nodes, pg2 handles the rest.
- Continuous backups to object storage with point-in-time recovery.
- Instant database branching for development and CI/CD workflows.

## Non-Goals

- **Multi-primary / multi-writer.** Single-primary eliminates write conflicts, distributed deadlocks, and conflict resolution. For the target workloads (web applications, SaaS, multi-tenant), one write node with read replicas covers the vast majority of scaling needs.
- **Sharding or partitioning across nodes.** All data lives on every node. This eliminates distributed query planning, cross-shard transactions, and resharding operations.
- **Logical replication.** Replication is physical (WAL streaming). No selective table sync, no schema-aware filtering. This keeps the replication path simple and identical to crash recovery.

## Architecture

A pg2 cluster consists of:

- **One primary** — accepts all writes, serves reads, streams WAL to replicas.
- **One or more replicas** — receive and apply the WAL stream, serve read queries.

Each replica has a priority (1 = highest). Priority determines promotion order during failover.

```
                    ┌──────────────┐
     writes ──────▶ │   Primary    │
                    │   (epoch 3)  │
                    └──┬───────┬───┘
                  WAL  │       │  WAL
                stream │       │  stream
                    ┌──▼──┐ ┌──▼──┐
     reads ───────▶ │ R1  │ │ R2  │
                    │ p=1 │ │ p=2 │
                    └─────┘ └─────┘
```

### WAL Streaming

The primary streams WAL records to replicas over the Network interface as they are written. Each record carries its LSN, transaction ID, record type, affected page ID, and payload — the same format used for local crash recovery.

Protocol:

1. Replica connects to primary and reports its current applied LSN.
2. Primary begins streaming WAL records from that LSN forward (via `readFromInto`).
3. Replica applies records to local storage and acknowledges its new applied LSN.
4. Primary tracks each replica's acknowledged position.

Streaming is continuous — records flow as they are appended. No polling, no batch interval.

### Synchronous Replication

Replication is synchronous. The primary waits for at least one replica to acknowledge each WAL record before confirming the commit to the client. This guarantees zero data loss on failover — every confirmed transaction exists on at least two nodes.

The cost is one network round-trip added to write latency. On a local network this is typically sub-millisecond. This is the right default for the target workloads: web applications and SaaS services where losing confirmed transactions is unacceptable.

When no replicas are connected (standalone mode or all replicas in maintenance), the primary confirms commits after local WAL flush only.

## Failover

### The Problem

Automatic failover traditionally requires a consensus protocol (Raft, Paxos) to agree on the new leader. These protocols are notoriously difficult to implement correctly. A buggy consensus layer is worse than no automation at all.

pg2 avoids consensus entirely using a **lease-based mechanism with epoch fencing**.

### Leases

The primary holds a lease. To maintain the lease, it must receive a heartbeat acknowledgment from at least one replica within the lease period (default: 10 seconds).

If the primary cannot renew its lease — because it is partitioned from all replicas, or all replicas are down — it **fences itself**: stops accepting writes and returns errors to clients. This is the critical property. The primary voluntarily gives up writes when it cannot prove replica connectivity.

Replicas in planned maintenance (clean disconnect) are excluded from the lease calculation. Taking a replica offline for upgrades does not affect the primary.

If no replicas are configured or all are in maintenance, the primary operates in standalone mode without lease requirements.

### Automatic Promotion

When a replica detects the primary is unresponsive (no heartbeat received for 2× the lease period):

1. The highest-priority caught-up replica promotes itself.
2. It increments the cluster **epoch** — a monotonic counter carried in every WAL record.
3. It begins accepting writes under the new epoch.
4. Other replicas detect the new epoch and follow the new primary.

The 2× wait is deliberate: it guarantees the old primary's lease has expired before the new primary starts writing. This ensures **at most one writable primary at any time** without requiring a vote.

### Epoch Fencing

Every WAL record carries the cluster epoch. Replicas reject WAL records from a stale epoch. When the old primary recovers from a partition or crash:

1. It attempts to contact replicas and discovers a higher epoch.
2. It steps down to replica role.
3. It replays WAL from the new primary to converge. Because replication is synchronous, no confirmed transactions are lost — the old primary cannot have confirmed a commit without at least one replica acknowledging it.

Epoch fencing is the safety net. Even if timing assumptions are violated (clock skew, long GC pauses), the epoch prevents a stale primary from corrupting the cluster.

### Planned Failover

For maintenance (OS upgrades, hardware changes):

```
pg2 cluster failover --to replica-1
```

Graceful handover:

1. Primary stops accepting new transactions.
2. Drains in-flight transactions to completion.
3. Waits for the target replica to apply all remaining WAL.
4. Target replica increments epoch and becomes primary.
5. Old primary steps down to replica.

Zero downtime for reads, zero data loss. Write unavailability is bounded by the time to drain in-flight transactions (typically milliseconds).

### Manual Promotion

If automatic failover is disabled:

```
pg2 cluster promote replica-1
```

This performs the same epoch increment and fencing, but is operator-initiated rather than timeout-driven.

### Failure Scenarios

| Scenario | Behavior |
|----------|----------|
| Primary crashes | Replicas detect after 2× lease timeout, highest-priority replica promotes. Zero data loss (sync replication). Write unavailability: ~20 seconds. |
| Network partition (primary isolated) | Primary's lease expires, it stops writes. Replica promotes. Clients reconnect to new primary. |
| Replica crashes | Primary continues operating. Remaining replicas (if any) continue serving reads. |
| All replicas down | Primary enters standalone mode (no lease requirement). Writes continue. No failover target until a replica rejoins. |
| Primary + replica crash simultaneously | On restart, nodes exchange epochs. Higher epoch wins. Same epoch: higher LSN wins. Tie: priority wins. |
| Flapping network | Primary's lease may expire during flaps, causing brief write pauses. Lease renewal on reconnection resumes writes without full failover. |

### Why This Works Without Consensus

Traditional consensus (Raft) solves the problem of *agreement among N nodes* on who the leader is. pg2 sidesteps this:

- **The primary fences itself** when it loses connectivity. No other node needs to tell it to stop.
- **Promotion order is predetermined** by priority. No election or voting needed.
- **Epochs provide total ordering.** Any node can determine which primary is authoritative by comparing epoch numbers.

The trade-off: failover takes ~20 seconds (2× lease timeout) instead of the sub-second failover that Raft can achieve. For web applications, this is an acceptable trade-off for dramatically simpler operations.

### Simulation Testing

All failover scenarios — partitions, crashes, flapping, clock skew, simultaneous failures — are testable in pg2's deterministic simulator. The `SimulatedDisk`, `SimulatedClock`, and `Network` interfaces allow injecting exact failure sequences and replaying them from a seed. This is the primary mechanism for validating correctness of the lease and epoch protocol.

## Client Routing

Clients connect using a multi-node connection string:

```
pg2://node1:5432,node2:5432,node3:5432/mydb
```

During the connection handshake, the node reports its role (primary or replica) and the current epoch. The client caches this topology.

- **Writes** are sent to the primary.
- **Reads** can be sent to any replica (or the primary).
- A **write sent to a replica** receives a redirect response with the current primary's address.
- On **connection failure**, the client tries the next node in the list, discovering the new topology.

No external proxy, load balancer, or DNS changes required. Failover is transparent to the application once promotion completes.

## Replica Bootstrapping

When a new replica joins or an existing replica has fallen too far behind:

**With object storage configured (preferred):**

1. Replica restores from the latest base backup in object storage.
2. Connects to the primary and requests WAL from the backup's LSN.
3. Primary streams WAL forward; replica applies and catches up.

This is preferred because it avoids loading the primary with a full data transfer.

**Without object storage:**

1. Replica requests a base copy directly from the primary.
2. Primary takes a checkpoint and streams all pages to the replica.
3. Once complete, the replica switches to live WAL streaming from the checkpoint LSN.

In both cases, the transition from bootstrapping to live streaming is automatic. The replica is available for reads as soon as it is within acceptable lag.

### WAL Retention

The primary retains WAL segments until all connected replicas have applied them. Segments are also retained for a configurable period (`wal_retain`) to allow replicas that temporarily disconnect to catch up without a full re-bootstrap.

If a replica reconnects with an applied LSN older than the oldest retained WAL segment, it must re-bootstrap.

## Backups

### Continuous WAL Archiving

pg2 ships completed WAL segments to object storage as they are produced:

```toml
[backup]
destination = "s3://my-bucket/myapp/"
```

This runs continuously in the background. Combined with synchronous replication, RPO is zero for any failure where at least one replica survives. WAL archiving provides an additional safety net for catastrophic scenarios (all nodes lost).

### Base Backups

Periodic full snapshots of all data pages:

```toml
[backup]
destination = "s3://my-bucket/myapp/"
schedule = "daily"
retention = "7d"
```

A base backup plus all WAL segments since that backup enables point-in-time recovery to any moment within the retention window.

Manual trigger:

```
pg2 backup now
```

### Point-in-Time Recovery

Restore to any point within the retention window:

```
pg2 restore --from s3://my-bucket/myapp/ --target-time "2025-01-15T14:30:00Z"
```

pg2 finds the nearest base backup before the target time, restores it, then replays WAL to the exact target LSN.

### Retention

```toml
[backup]
retention = "7d"
```

Expired base backups and WAL segments are garbage-collected automatically. The oldest base backup within the retention window is always preserved as a restore anchor.

## Database Branching

A branch is a writable fork of the database at a specific point in time.

### Creating a Branch

From current state:

```
pg2 branch create pr-123
```

From a specific point in time:

```
pg2 branch create pr-123 --at "2025-01-15T14:30:00Z"
```

### How It Works

**Local branches (same machine):**

pg2 creates a copy-on-write fork at the page level. The branch shares all unmodified pages with the parent. Writes in the branch allocate new pages; reads that miss fall through to the parent's snapshot.

Creation is instant regardless of database size. Storage overhead is proportional to the changes made in the branch, not the size of the database.

**Remote branches (separate machine, CI runner):**

pg2 restores from the nearest base backup in object storage, replays WAL to the requested point in time, and runs as an independent pg2 instance.

### Management

```
pg2 branch list
pg2 branch drop pr-123
```

Branches are ephemeral by design — for development, testing, and preview environments. Not for production traffic.

### Use Cases

- **PR preview environments.** Branch with production data for each pull request. Run migrations and tests against real data. Drop on merge.
- **Migration testing.** Validate schema changes against a real data fork before applying to production.
- **Debugging.** Fork from a specific point in time to investigate an issue without affecting production.

## Replica Reads

Replicas serve read queries. Because replication is synchronous, replicas are at most one in-flight transaction behind the primary.

### Read-Your-Writes Consistency

For applications that need causal consistency after a write:

1. After a write, the primary returns the commit LSN to the client.
2. When reading from a replica, the client includes this LSN.
3. The replica waits until it has applied past that LSN before executing the query.

This is opt-in per query. Applications that tolerate eventual consistency skip the LSN for lower read latency.

### Lag Monitoring

```
pg2 cluster status

NODE     ROLE      EPOCH   LSN              LAG
node1    primary   3       000000001A3F     —
node2    replica   3       000000001A3D     0.4ms
node3    replica   3       000000001A3B     1.1ms
```

## Configuration

### Minimal (Primary + One Replica)

**node1:**
```toml
[cluster]
name = "myapp"
role = "primary"
listen = "0.0.0.0:5432"
nodes = ["node2:5432"]
```

**node2:**
```toml
[cluster]
name = "myapp"
role = "replica"
listen = "0.0.0.0:5432"
primary = "node1:5432"
```

The replica connects, bootstraps automatically, and begins serving reads. No manual initialization step.

### With Backups and Automatic Failover

```toml
[cluster]
name = "myapp"
role = "primary"
listen = "0.0.0.0:5432"
nodes = ["node2:5432", "node3:5432"]

[replication]
lease_timeout = "10s"
failover = "automatic"

[backup]
destination = "s3://my-bucket/myapp/"
schedule = "daily"
retention = "7d"
```

### Reference

```toml
[cluster]
name = "<cluster-name>"             # Cluster identifier (must match across all nodes)
role = "primary" | "replica"        # Initial role
listen = "<bind>:<port>"            # Address to listen on
primary = "<host>:<port>"           # Replica only: primary to connect to
nodes = ["<host>:<port>", ...]      # Primary only: known replicas
priority = 1                        # Replica only: failover priority (1 = highest)

[replication]
lease_timeout = "10s"               # Primary lease period
failover = "automatic" | "manual"   # Failover mode
wal_retain = "1h"                   # WAL retention for replica catch-up

[backup]
destination = "s3://..."            # Object storage for WAL + base backups
schedule = "daily" | "hourly" | "none"
retention = "7d"                    # Backup retention period
```
