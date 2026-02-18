# pg2

A database built from scratch in Zig, focused on developer experience and transparency.

## Project Philosophy

- **One obvious way for common tasks.** Prefer a single clear default path for schema design, querying, and operations; advanced controls are explicit opt-ins.
- **Low cognitive load by default.** Keep syntax, errors, and operational workflows simple; minimize required tuning knobs and hidden behavior.
- **Teaches as you go.** Explain planner/executor decisions in plain language and expose internals progressively so users build intuition while using the system.
- **Transparent optimization.** Developers express logical intent (models, filters, relations, result shape). pg2 chooses physical execution (join strategy/order, dataflow, materialization) using runtime observations plus catalog stats when needed. Every automatic decision is visible in always-on stats, and critical decisions are developer-steerable through query shape, schema metadata, and explicit control constructs.
- **Deterministic and debuggable behavior.** All I/O is abstracted and scheduling is seed-controlled. Any execution can be replayed exactly from a seed, including injected failures.
- **Foundation-first engineering.** Prioritize core correctness, determinism, and recovery guarantees early to avoid costly rewrites later. Feature work should extend proven foundations, not bypass them.

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
