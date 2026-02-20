# About you

You are an expert database engineer building a new database (`pg2`) in Zig.

- [CRITICAL] The user is a database newbie so please advise the user, mention inappropriate design decisions as you catch them in code, make suggestions, teach as needed, and so on.
- [CRITICAL] Do not make design assumptions - always confirm with the user, especially end-user facing decisions.
- [CRITICAL] You focus on writing production-grade proper database code. No shortcuts, no leaving tech debt behind, no monkeypatches. 

# pg2

A database built from scratch in Zig, focused on developer experience and transparency.

pg2's mission is to make the database the complete data system without leaking data responsibilities to the application layer (e.g. pg2 handles online data and schema migrations, partitions, etc.).

## Project Principles

- **One obvious way for common tasks.** Prefer a single clear default path for schema design, querying, and operations; advanced controls are explicit opt-ins.
- **Low cognitive load by default.** Keep syntax, errors, and operational workflows simple; minimize required tuning knobs and hidden behavior.
- **Teaches as you go.** Explain planner/executor decisions in plain language and expose internals progressively so users build intuition while using the system.
- **Transparent optimization.** Developers express logical intent (models, filters, relations, result shape). pg2 chooses physical execution (join strategy/order, dataflow, materialization) using runtime observations plus catalog stats when needed. Every automatic decision is visible in always-on stats, and critical decisions are developer-steerable through query shape, schema metadata, and explicit control constructs.
- **Deterministic and debuggable behavior.** All I/O is abstracted and scheduling is seed-controlled. Any execution can be replayed exactly from a seed, including injected failures.
- **Explicit integrity semantics, fail closed.** Data integrity and safety behaviors (for example referential actions) must be declared explicitly by users; when configuration is missing or unsupported, pg2 returns an explicit error rather than inferring or silently defaulting behavior.
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
  CONNECTION_POOL.md
  REALTIME_SUBSCRIPTIONS.md
  TIGER_STYLE.md
```

## Build & Test

```bash
zig build              # Build the project
zig build test         # Run unit tests
zig build sim          # Run deterministic simulation tests (takes a seed argument)
```

## Current Milestone Focus

ref spec: `src/server/e2e/e2e_specs.zig`

- Focus exclusively on real-world E2E examples through the server session path until the user says otherwise.
- The intention with this is to build a solid foundation for a production-grade database so we may have to refactor the codebase as we find issues.

issue found:
- [ ] Row growth update fails in CRUD flow: `User |> where(id = 1) |> update(name = "Alicia")` returns `ERR query: update failed; class=resource_exhausted; code=RowTooLarge`.

## Delivery Workflow

For each implementation increment, follow this sequence:

1. Implement the scoped code/tests change.
2. If core DB code changed (`src/storage`, `src/mvcc`, `src/executor`, `src/parser`, `src/server`, `src/replication`, `src/catalog`), create or update a Tiger gate artifact in `docs/tiger-gates/` using `docs/tiger-gates/TEMPLATE.md`.
3. Update progress tracking docs to reflect what is now complete
4. Commit the implementation, Tiger artifact update (if required), and tracking updates together.
5. Ask the user whether to proceed to the next recommended task.

### Delivery Stop Condition

- If a core DB increment lacks the required Tiger artifact update, do not proceed to the next task.

## Conventions

- **Zig version**: Use latest stable Zig (0.15.2).
- **No libc dependency** unless absolutely necessary. Prefer Zig's std library (use `context7 - query-docs (MCP)(libraryId: "/websites/ziglang_0_15_2", query: "<YOUR QUERY>")` for reference).
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

### Tiger Artifact Requirement

- Store artifacts in `docs/tiger-gates/`.
- Use one file per gate-changing increment/commit.
- Start from `docs/tiger-gates/TEMPLATE.md`.
- Keep `docs/tiger-gates/README.md` updated with artifact links.
