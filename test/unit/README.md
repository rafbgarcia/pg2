# Unit Lane

This directory tracks migration of legacy inline `src/*` tests toward explicit
`test/unit/*` ownership.

Current state:
- `zig build unit` compiles explicit `test/unit/*` lane tests.
- `zig build unit-run` executes `test/unit/*` plus legacy inline `src/*` tests.
- `zig build unit-legacy` compiles legacy inline `src/*` tests only.
- `zig build unit-legacy-run` executes legacy inline `src/*` tests only.

Policy direction:
- New unit tests should prefer `test/unit/*` files.
- Inline `src/*` tests are considered legacy until migration is complete.
