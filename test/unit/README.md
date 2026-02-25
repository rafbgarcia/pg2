# Unit Lane

This directory tracks migration of legacy inline `src/*` tests toward explicit
`test/unit/*` ownership.

Current state:
- `zig build unit` compiles inline `src/*` unit tests via `src/pg2.zig`.
- `zig build unit-run` executes inline `src/*` unit tests.

Policy direction:
- New unit tests should prefer `test/unit/*` files.
- Inline `src/*` tests are considered legacy until migration is complete.
