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
    ptr: *anyopaque,
    vtable: *const VTable,

    const VTable = struct {
        read:  *const fn (ptr: *anyopaque, page_id: u64, buf: *[8192]u8) StorageError!void,
        write: *const fn (ptr: *anyopaque, page_id: u64, data: *const [8192]u8) StorageError!void,
        fsync: *const fn (ptr: *anyopaque) StorageError!void,
    };
};
```

Production: wraps `preadv`/`pwritev` + `fdatasync`.
Simulation: `SimulatedDisk` — in-memory pages with pending/durable write separation and crash semantics.

### Clock Interface

```zig
const Clock = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    const VTable = struct {
        now: *const fn (ptr: *anyopaque) u64,
    };
};
```

Production: `RealClock` wrapping `std.time.nanoTimestamp`.
Simulation: `SimulatedClock` — manually advanced by the scheduler.

### Network Interface

```zig
const Network = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    const VTable = struct {
        send: *const fn (ptr: *anyopaque, to: PeerId, data: []const u8) void,
        recv: *const fn (ptr: *anyopaque) ?Message,
    };
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

Single-writer, multiple-reader. One thread handles all writes; read-only queries run concurrently using MVCC snapshots.

This is a deliberate architectural choice, not a simplification:

- **Correctness.** Serialized writes eliminate write-write conflicts, lock ordering deadlocks, and phantom reads during DDL. The WAL is a natural serialization point — there is no concurrency benefit to having multiple writers contend on it.
- **Performance.** A single writer serializing through the WAL can sustain 100K+ TPS on modern hardware. The bottleneck in OLTP is disk I/O (fsync latency), not CPU serialization. Multiple writers would add lock contention without improving throughput.
- **Simplicity.** The buffer pool, WAL, and undo-log MVCC are dramatically simpler without concurrent writers. Less code means fewer bugs, and fewer bugs matter more in a database than in any other kind of software.
- **Deterministic simulation.** Single-writer execution is inherently more deterministic, which strengthens the simulation testing model.

This model is used successfully in production by SQLite (WAL mode), LMDB, and TigerBeetle. Read scalability comes from async replicas, not from concurrent writers on the primary.

## Memory Model

All memory is statically allocated at startup. See CLAUDE.md "Tiger Style — Static memory allocation" for the full specification.

Summary: on startup, pg2 `mmap`s a single contiguous region sized by the `--memory` flag (default: 512 MiB). All subsystem pools, buffers, and arenas are bump-allocated from this region. After initialization, the allocator is sealed — any allocation attempt is a panic.

The total budget is subdivided by fixed compile-time ratios:

| Component | Share | Purpose |
|-----------|-------|---------|
| Buffer pool | ~70% | Page cache |
| WAL buffers | ~5% | Write-ahead log |
| Connection arenas | ~10% | Per-query execution memory |
| Undo log pool | ~10% | MVCC version storage |
| Catalog/metadata | ~5% | Schema, indexes metadata |

Per-query memory uses arena allocators carved from the connection pool at startup. Arenas are reset (not freed) after each query.

## Limits

Every resource has a fixed upper bound, configured at startup:

| Resource | Default | Configurable |
|----------|---------|-------------|
| Total memory | 512 MiB | `--memory` |
| Buffer pool frames | derived from memory budget | no (fixed ratio) |
| Max connections | 64 | `--max-connections` |
| Max tables | 1024 | compile-time |
| Max columns per table | 256 | compile-time |
| Max indexes per table | 32 | compile-time |
| Max query result rows | bounded by connection arena | no |
| WAL segment size | 64 MiB | compile-time |
| WAL retention segments | 16 | `--wal-retention` |
| Undo log entries | bounded by undo log pool | no |

Exceeding any limit produces an explicit error or panic (for invariant violations), never silent degradation.

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
2. **Automatic dataflow decisions** — for unfiltered inputs, the executor uses `row_count * avg_row_size` vs the query memory budget as a baseline for dataflow and join strategy decisions. When inputs have been transformed by filters or other operators, the executor uses **runtime observation** of actual row counts instead. All decisions are deterministic and reported in stats output.
