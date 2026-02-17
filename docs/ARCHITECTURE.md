# Architecture Overview

## Build Order

The project is built bottom-up. Each layer depends only on the layers below it. The simulation harness is built alongside the storage engine, not after.

```
Phase 1: Foundation
  ├── I/O abstraction interfaces (Storage, Network, Clock)
  ├── Deterministic simulation harness
  └── Page manager + buffer pool

Phase 2: Storage Engine
  ├── Write-ahead log (WAL)
  ├── B-tree indexes
  ├── Heap storage (in-place update with undo log)
  └── Undo-log MVCC + transaction manager

Phase 3: Query Layer
  ├── pg2 query language parser
  ├── Executor (runtime-adaptive)
  ├── Catalog stats (row counts, avg row size, index stats)
  └── Schema / catalog management

Phase 4: Server
  ├── Connection handling (custom wire protocol)
  └── Runtime statistics + introspection

Phase 5: Replication
  ├── WAL streaming to replicas
  ├── Replica read path
  └── Promotion / failover
```

## Core Abstractions

Every subsystem is parameterized over these interfaces so that the simulation harness can replace them.

### Storage Interface

```zig
const Storage = struct {
    read: *const fn (offset: u64, buf: []u8) void,
    write: *const fn (offset: u64, data: []const u8) void,
    fsync: *const fn () void,
};
```

Production: wraps `preadv`/`pwritev` + `fdatasync`.
Simulation: in-memory byte array with fault injection (partial writes, bit flips, delayed fsync).

### Clock Interface

```zig
const Clock = struct {
    now: *const fn () u64,  // monotonic nanoseconds
};
```

Production: `std.time.nanoTimestamp`.
Simulation: manually advanced by the scheduler.

### Network Interface

```zig
const Network = struct {
    send: *const fn (peer: PeerId, msg: []const u8) void,
    recv: *const fn () ?Message,
};
```

Production: TCP sockets.
Simulation: in-memory message queue with configurable latency, reordering, and drops.

## Data Flow

```
Client Query (text)
  │
  ▼
Parser ──► Logical Plan (AST)
  │
  ▼
Executor ──► walks the plan, optimizes physical execution
  │          ├── find → B-tree → Buffer Pool → Storage
  │          ├── Table Read → Heap → Buffer Pool → Storage
  │          ├── where → evaluates predicate on each row
  │          └── join → picks strategy and order at runtime
  │
  ▼
Result stream back to client
```

There is no cost-based optimizer. The developer writes the **logical query** — which tables to access, which filters to apply, which joins to make. The DB handles the **physical execution** — join order, join strategy, projection pushdown, and other mechanical optimizations. Every automatic decision is visible in the stats output.

The DB uses **runtime-adaptive** selection for physical decisions: it observes actual row counts as they flow through the pipeline and commits to strategies based on reality, not estimates. For decisions that depend only on table-level properties (e.g., dataflow for unfiltered `let` bindings), it uses O(1) catalog stats.

This is analogous to a CPU pipelining your instructions — the semantic behavior matches what you wrote, but the physical execution is optimized. The developer controls what data to get; the DB controls how to get it.

## Concurrency Model

Single-writer, multiple-reader. One thread handles all writes (mutations are serialized through the WAL). Read-only queries can run concurrently using MVCC snapshots. This simplifies the storage engine dramatically and is sufficient for the learning goals of this project.

If the single-writer becomes a bottleneck later, the design can evolve toward partitioned writers, but start simple.

## Catalog Stats

The DB maintains exact, O(1) statistics for every table and index. These are not stale estimates — they are updated on every mutation. No `ANALYZE` command, no sampling.

| Stat | Scope | Updated on |
|---|---|---|
| `row_count` | per table, per index | insert, delete |
| `avg_row_size` | per table | insert, update (running average) |
| `total_pages` | per table | page alloc/dealloc |
| `min` / `max` | per indexed column | insert; lazy-corrected on delete |
| `distinct_count` | per indexed column | insert (HyperLogLog, approximate) |

These stats serve two purposes:
1. **Developer visibility** — queryable via `stats(table)` and `stats(index)` in the query language.
2. **Automatic dataflow decisions** — for unfiltered inputs, the executor uses `row_count * avg_row_size` vs `buffer_pool_size` as a baseline for dataflow and join strategy decisions. When inputs have been transformed by filters or other operators, the executor uses **runtime observation** of actual row counts instead. All decisions are deterministic and reported in stats output.
