## Focus Item

Statement-level transactional composition:
- one user statement executes as a self-contained transaction,
- a statement may mix reads and mutations in deterministic order,
- `let` variables can bind intermediate results and feed subsequent operators.

## Design Tradeoffs To Remember

1. Long statements hold locks/resources longer.
2. Retry semantics must be explicit and deterministic.
3. `let` evaluation order and visibility rules must be deterministic.
