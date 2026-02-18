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
  TIGER_STYLE.md
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

All Tiger Style and database-specific robustness rules live only in `docs/TIGER_STYLE.md`.

### PR Gate (Summary)

For any PR touching core DB code, complete the mandatory checklist in `docs/TIGER_STYLE.md`:

- Invariant changes.
- Crash-consistency contract.
- Error class changes.
- Persistent format/protocol impact.
- Deterministic crash/fault tests.
- Performance baseline/threshold impact.

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

# Future ideas

- Columnar storage: user defines which columns
- Built-in online migrations
- Redis-like features: cache specific queries, subscribe to data mutations (real-time use cases)
- Use in browser (builtin online migrations could be useful)
