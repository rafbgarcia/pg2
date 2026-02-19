# Tiger Style

Adapted from [TigerBeetle's Tiger Style](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md), with pg2-specific database rules.

These principles are mandatory for all pg2 code.

## Static Memory Allocation

All memory is statically allocated at startup. No heap allocation after initialization.

- On startup, pg2 `mmap`s a single contiguous region sized by the `--memory` flag (default: 512 MiB). All subsystem pools, buffers, and arenas are bump-allocated from this region.
- After init, the allocator is sealed. Any allocation attempt after seal is a panic. This is enforced by a `StaticAllocator` that tracks `.init` vs `.static` state.
- Per-query memory uses arena allocators carved from the connection pool at startup. Arenas are reset (not freed) after each query.
- The `--memory` value is always a fixed number set by the operator, never derived from machine hardware.

## Put a Limit on Everything

Every queue, buffer, loop, and batch has an explicit upper bound. No unbounded data structures.

- All loops have a maximum iteration count. Exceeding it is a panic.
- All queues and ring buffers have fixed capacities allocated at startup.
- All batches have a maximum size. If a batch fills up, it is flushed.
- Client connections are capped at `max_connections` (configured at startup).
- Result sets are bounded. Queries that would exceed the per-query arena fail with an explicit error.

## Assertions

Use assertions aggressively. They are the first line of defense and primary debugging tool.

- Minimum two assertions per function for any function with non-trivial logic.
- Assert across module boundaries (page read from disk, message from network, WAL record decode).
- Use `std.debug.assert` for invariants that must hold in all builds.
- Use `if (!condition) @panic("message")` when you need a descriptive message.
- Never use `unreachable` for conditions that can occur.
- Pair assertions to cover both insert and remove sides of invariants.

## Naming

- Use `snake_case` for all identifiers.
- Include units in names: `timeout_ms`, `size_bytes`, `offset_pages`.
- Use `_max`, `_min`, `_limit`, `_count` suffixes for bounds.
- Name for the reader, not the writer.
- No obscure abbreviations (`buffer_manager`, not `buf_mgr`).

## No Recursion

Do not use recursion. Use explicit fixed-capacity stacks allocated at startup.

## Function and Code Structure

- Declare variables in the smallest possible scope.
- One statement per line.
- Keep related control flow centralized.

## Performance Thinking

- Optimize the slowest resource first: network > disk > memory > CPU.
- Batch aggressively to amortize syscalls, disk writes, and network round-trips.
- Design for performance up front.
- Do resource sketches before implementing major subsystems.

## Crash-Consistency Contracts

Every persistent subsystem must define its crash contract.

- Each module with on-disk state must document write ordering requirements, flush boundaries, durable point, and post-crash invariants.
- Code paths must assert preconditions and postconditions around durability boundaries.
- Recovery must not depend on unspecified storage behavior.

Required tests:

- Deterministic crash injection at each write protocol step.
- Replay after every injected crash point.
- Invariant validation after replay.

## On-Disk Format Discipline

All persistent bytes are a public contract.

- Every on-disk structure must define version, endianness, field sizes, and checksum coverage.
- No implicit struct layout persistence.
- No raw memory reinterpretation persistence unless explicitly versioned and tested.
- Parse with bounds checks before reading fields.

Required tests:

- Golden encode/decode vectors.
- Corrupt/truncated input tests.
- Forward/backward compatibility tests for supported versions.

## Recovery Idempotence and Determinism

Recovery must be safe to rerun and reproducible.

- Redo/undo/replay operations must be idempotent.
- Recovery order must be deterministic for a given durable state.
- Recovery code must not read wall-clock time or external mutable state.

Required tests:

- Run recovery twice and assert byte-identical resulting state.
- Seeded simulation asserting same seed yields same recovery outcome.

## Corruption Policy and Boundary Validation

Corruption handling must be explicit and consistent.

- Validate checksums and structural invariants at trust boundaries.
- Classify corruption outcomes per component:
  - fail closed
  - quarantine and continue read-only
  - operator-required repair path
- Never silently drop corrupted records/pages.

Required tests:

- Single-bit and multi-bit corruption injection.
- Partial write and torn-write scenarios.
- Assertions for error class and containment behavior.

## Error Taxonomy

Errors must be machine-actionable.

- All public module errors must map to one of:
  - `retryable`
  - `resource_exhausted`
  - `corruption`
  - `fatal`
- Error mapping must be stable and documented.
- New error cases require classification at introduction.

Required tests:

- Force each class in each major subsystem.
- Assert callers implement class-specific handling.

## Integer, Bounds, and Units Safety

Storage code fails on arithmetic mistakes.

- Use checked arithmetic for offsets, lengths, page indexes, and LSN math.
- Every narrowing cast must be preceded by an assertion on value range.
- Units must be encoded in names (`*_bytes`, `*_pages`, `*_lsn`).
- Cross-unit arithmetic requires explicit conversion helpers.

Required tests:

- Overflow/underflow edge tests.
- Boundary tests at max page count, max tuple size, and max WAL record size.

## Compatibility Contract

Compatibility must be intentional.

- Define stability guarantees for data files, WAL format, replication protocol, and client wire protocol.
- Define supported upgrade paths.
- Refuse unsupported versions with explicit errors.

Required tests:

- Upgrade tests from each supported prior version.
- Downgrade behavior tests where supported.
- Mixed-version replica handshake tests.

## Performance Regression Gates

Correctness is required. Predictable performance is also required.

- Maintain fixed-seed benchmark workloads for core operations.
- Track and gate throughput, p50 latency, p99 latency, write amplification, and recovery time.
- Regressions beyond threshold fail CI unless explicitly approved.

Required tests:

- Repeatable benchmark harness using deterministic seeds.
- Per-metric threshold checks with documented baselines.

## Invariant-Driven Testing Matrix

Example-based tests are not enough for a database.

- Every subsystem must include property-based or metamorphic tests.
- Parser and executor should include fuzz inputs with invariant checks.
- Storage and MVCC should include long-running seeded simulations with injected faults.
- Prefer differential checks against a reference implementation where semantics overlap.

Required tests:

- Property suites for parser, expressions, and row operations.
- Simulator stress runs with deterministic replay on failure.

## Operational Guardrails

Unsafe runtime states must be blocked early.

- Validate all startup configuration with explicit limits and units.
- Fail startup when memory partition budgets do not sum correctly.
- Enforce bounded recovery progress with progress counters and watchdog limits.
- Expose always-on internal counters for WAL, buffer pool, MVCC, and replication lag.

Required tests:

- Startup rejection tests for invalid configurations.
- Recovery progress tests under degraded I/O simulation.
- Introspection tests verifying counter monotonicity and bounds.

## Replication Safety Rules

Replication must preserve durability semantics and order.

- Replica apply order must follow WAL order.
- Commit visibility on replicas must align with durable commit boundaries.
- Divergence detection must use explicit LSN checks and checksums.
- Promotion must include a defined fencing step to prevent split-brain.

Required tests:

- Partition/rejoin simulations with deterministic schedules.
- Lag, drop, duplicate, and reorder message injection.
- Promotion/failover simulations with invariant checks.

## Catalog and Schema Change Safety

Metadata corruption can invalidate user data access.

- Schema changes must be atomic and versioned.
- Catalog updates must follow the same WAL and recovery guarantees as user data.
- In-flight query behavior during DDL must be explicitly specified.

Required tests:

- Crash during each step of schema change.
- Replay correctness of partially applied DDL.
- Concurrent read behavior under catalog version changes.

## PR Checklist (Mandatory)

Every PR touching core DB code must answer:

- What invariant was added or changed?
- What is the crash-consistency contract for the modified path?
- Which error classes can now be returned?
- Does this change modify any persistent format or protocol?
- Which deterministic crash/fault tests were added?
- Which performance baseline or threshold was updated (if any)?

If any answer is `none`, state why explicitly.
