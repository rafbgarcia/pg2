# pg2

An experimental database built from scratch in Zig, focused on developer experience and transparency.

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
- **Custom allocators everywhere.** Every subsystem takes an `Allocator` parameter. This is critical for simulation testing (OOM injection, tracking).
- **No system clock access in core code.** Time is injected explicitly. Core logic uses a `Clock` interface — real clock in production, deterministic clock in simulation.
- **No threads in core code.** Concurrency is managed through an event loop abstraction — real async I/O in production, deterministic scheduler in simulation.
- **All I/O through interfaces.** Disk I/O goes through a `Storage` interface. Network I/O goes through a `Network` interface. Simulation provides fake implementations.
- **Error handling**: Use Zig's error unions. No panics except for true invariant violations (use `@panic` with a message, never `unreachable` for things that can actually happen).
- **Tests**: Every module has inline tests. Simulation tests live in `src/simulator/`. Prefer property-based checks over example-based checks where possible.
- **Page size**: 8KB (matching PostgreSQL, allows direct comparison and reuse of research).
- **File format**: All on-disk formats are explicitly versioned from day one.

## Context7

- For Zig 0.15.2 queries, always use `context7 - query-docs (MCP)(libraryId: "/websites/ziglang_0_15_2", query: "<YOUR QUERY>")`
