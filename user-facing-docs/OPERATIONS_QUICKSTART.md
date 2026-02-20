# Operations Quickstart (Current)

This is the minimum operator-facing path for running pg2 during the current milestone.

## Build And Test

```bash
zig build
zig build test
zig build sim
```

## Server Runtime Scope

- Production runtime target: Linux.
- Non-Linux `--listen` path fails closed with an explicit Linux-only message.
- Linux listen path expects `io_uring` availability.

## Development Workflow

- Use server-path E2E examples in `src/server/e2e/` as executable behavior references.
- For macOS local validation of Linux server path, use Docker-based flow documented in `docs/SERVER_RUNTIME.md`.

## Query Session Output Contract

- Requests return one text response per query.
- Success starts with
  `OK returned_rows=<n> inserted_rows=<n> updated_rows=<n> deleted_rows=<n>`.
- Errors start with `ERR ...`.
- `inspect` adds deterministic diagnostic lines after row output.
  - includes overflow reclaim queue depth and reclaim counters.

## Known Early-Stage Constraints

- Query/features are still being hardened through v1 E2E specification coverage.
- Treat current docs as living reference for implemented behavior, not final long-term compatibility guarantees.
