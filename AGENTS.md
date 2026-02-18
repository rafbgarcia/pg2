# pg2

A database built from scratch in Zig, focused on developer experience and transparency.

## Project Philosophy

- **Transparent optimization.** The developer writes the logical query — which tables, filters, and joins. The DB handles physical execution — join order, join strategy, and mechanical optimizations — using runtime-adaptive decisions based on actual row counts, not cost estimates. Every automatic decision is visible in always-on stats output, and the developer can override any of them.
- **Undo-log MVCC.** Updates happen in-place; old versions live in an undo log. No vacuum, no heap bloat.
- **Deterministic simulation testing.** All I/O is abstracted. A seeded PRNG controls scheduling. Any execution can be replayed exactly from a seed. Fault injection (disk corruption, partial writes, OOM, network partitions) is built in from day one.
- **Async read replicas.** Primary handles all writes; replicas stream the WAL and serve reads.

## Project Structure

```
src/
  storage/       # Page manager, WAL, buffer pool, undo log
  mvcc/          # Transaction management, snapshot isolation, undo-log versioning
  executor/      # Query executor — runtime-adaptive physical planning and execution
  parser/        # Parser for the pg2 query language
  simulator/     # Deterministic simulation harness, fault injection
  replication/   # WAL streaming, replica sync
  server/        # Wire protocol, connection handling
docs/
  ARCHITECTURE.md
  STORAGE_ENGINE.md
  QUERY_LANGUAGE.md
  SIMULATION_TESTING.md
  REPLICATION.md
```

## Build & Test

```bash
zig build              # Build the project
zig build test         # Run unit tests
zig build sim          # Run deterministic simulation tests (takes a seed argument)
```

## Conventions

- **Zig version**: Use latest stable Zig (0.13.x+ at time of writing).
- **No libc dependency** unless absolutely necessary. Prefer Zig's std library.
- **No system clock access in core code.** Time is injected explicitly. Core logic uses a `Clock` interface — real clock in production, deterministic clock in simulation.
- **No threads in core code.** Concurrency is managed through an event loop abstraction — real async I/O in production, deterministic scheduler in simulation.
- **All I/O through interfaces.** Disk I/O goes through a `Storage` interface. Network I/O goes through a `Network` interface. Simulation provides fake implementations.
- **Error handling**: Use Zig's error unions. No panics except for true invariant violations (use `@panic` with a message, never `unreachable` for things that can actually happen).
- **Tests**: Every module has inline tests. Simulation tests live in `src/simulator/`. Prefer property-based checks over example-based checks where possible.
- **Page size**: 8KB (matching PostgreSQL, allows direct comparison and reuse of research).
- **File format**: All on-disk formats are explicitly versioned from day one.

## Tiger Style

Adapted from [TigerBeetle's Tiger Style](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md). These principles are mandatory for all pg2 code.

### Static memory allocation

All memory is statically allocated at startup. No heap allocation after initialization.

- On startup, pg2 `mmap`s a single contiguous region sized by the `--memory` flag (default: 512 MiB). All subsystem pools, buffers, and arenas are bump-allocated from this region.
- After init, the allocator is **sealed** — any allocation attempt after seal is a panic. This is enforced by a `StaticAllocator` that tracks `.init` vs `.static` state.
- The total budget is subdivided by fixed compile-time ratios:
  - ~70% buffer pool (page cache)
  - ~5% WAL buffers
  - ~10% connection arenas (per-query execution memory)
  - ~10% undo log pool (MVCC version storage)
  - ~5% catalog/metadata
- Per-query memory uses arena allocators carved from the connection pool at startup. Arenas are **reset** (not freed) after each query — no OS allocator calls during query execution.
- The `--memory` value is always a fixed number set by the operator, never derived from machine hardware. This preserves deterministic simulation: test configs use a small fixed budget (e.g. 8 MiB), production uses whatever the operator specifies.

### Put a limit on everything

Every queue, buffer, loop, and batch has an explicit upper bound. No unbounded data structures.

- All loops have a maximum iteration count. Exceeding it is a panic — it means the bound was wrong, not that the loop should keep going.
- All queues and ring buffers have fixed capacities allocated at startup.
- All batches have a maximum size. If a batch fills up, it is flushed — it does not grow.
- Client connections are capped at `max_connections` (configured at startup).
- Result sets are bounded. Queries that would exceed the per-query arena fail with an explicit error rather than silently allocating more memory.

### Assertions

Use assertions aggressively. They are the first line of defense and primary debugging tool.

- **Minimum two assertions per function** for any function with non-trivial logic. Assert preconditions at entry. Assert postconditions or invariants before return.
- Assert across boundaries — when data crosses a module boundary (e.g. page read from disk, message received from network), assert its structural validity immediately.
- Use `std.debug.assert` for invariants that must hold in all builds. Use `if (!condition) @panic("message")` when you need a descriptive message.
- Never use `unreachable` for conditions that can actually occur. If a switch case "shouldn't happen," use `@panic` with a message explaining why.
- Pair assertions: if you assert `x < limit` on insert, assert `x > 0` on remove. Assertions should cover both the positive and negative space.

### Naming

- Use `snake_case` for all identifiers.
- Include units in names: `timeout_ms`, `size_bytes`, `offset_pages`. Never leave units ambiguous.
- Suffixes for bounds: `_max`, `_min`, `_limit`, `_count`.
- Name for the reader, not the writer. If a name requires a comment to explain, the name is wrong.
- No abbreviations unless universally understood (`wal`, `mvcc`, `lsn` are fine; `buf_mgr`, `txn_ctx` are not — write `buffer_manager`, `transaction_context`).

### No recursion

Do not use recursion. Use explicit fixed-capacity stacks allocated at startup. Recursion hides resource usage in the call stack, which cannot be bounded or asserted on. An explicit stack has a visible capacity, can be asserted against, and fails predictably when the bound is wrong.

### Function and code structure

- **70-line hard limit per function.** If a function exceeds this, it must be split. No exceptions.
- Declare variables in the smallest possible scope. No declarations at function top "for later."
- One statement per line. No chained operations that obscure control flow.
- Centralize control flow — avoid distributing related branching logic across multiple functions when it can live in one place.

### Performance thinking

- Optimize the slowest resource first: **network > disk > memory > CPU.** Do not optimize CPU at the expense of extra disk I/O.
- **Batch aggressively.** Amortize syscalls, disk writes, and network round-trips. One write of N items beats N writes of one item.
- Think about performance at design time, not after. If a data structure choice forces O(n) scans, fix the data structure — don't add caching later.
- Do back-of-the-envelope resource sketches before implementing any new subsystem: how many pages, how many bytes on the wire, how many disk seeks.

## Progress

- [x] **Phase 1: Foundation**
  - [x] I/O abstraction interfaces (`Storage`, `Clock`, `Network`) — `src/storage/io.zig`
  - [x] Deterministic simulation harness (`SimulatedDisk`, `SimulatedClock`) — `src/simulator/`
  - [x] Page struct with CRC-32C checksums — `src/storage/page.zig`
  - [x] Buffer pool with clock-sweep eviction — `src/storage/buffer_pool.zig`
  - [x] Build system (`zig build`, `zig build test`, `zig build sim`)
- [x] **Phase 2: Storage Engine**
  - [x] Write-ahead log (WAL) — `src/storage/wal.zig`
  - [x] B-tree indexes — `src/storage/btree.zig`
  - [x] Heap storage (slotted pages, in-place update) — `src/storage/heap.zig`
  - [x] Undo-log MVCC + transaction manager — `src/mvcc/`
  - [x] Buffer pool enforces WAL protocol (page LSN must be flushed before page flush)
- [ ] **Phase 3: Query Layer**
  - [x] pg2 query language parser (tokenizer, AST, expression parser, statement parser)
  - [x] Schema / catalog management (catalog metadata store, schema loader)
  - [x] Executor: scan, filter, mutations — `src/executor/`
  - [ ] Executor: sort, aggregation, joins
- [ ] **Phase 4: Server**
  - [ ] Connection handling (custom wire protocol)
  - [ ] Runtime statistics + introspection
- [ ] **Phase 5: Replication**
  - [ ] WAL streaming to replicas
  - [ ] Replica read path
  - [ ] Promotion / failover

## Context7

- For Zig 0.15.2 queries, always use `context7 - query-docs (MCP)(libraryId: "/websites/ziglang_0_15_2", query: "<YOUR QUERY>")`
