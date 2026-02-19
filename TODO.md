# TODO

Remaining follow-up items from the code review that we should still address.

## High Priority

- [x] Buffer pool: fail-closed handling for all-zero pages based on allocation metadata.
  - Why: avoid silently accepting zeroed corruption for previously allocated pages.
  - Files: `src/storage/buffer_pool.zig`, likely allocator/freelist metadata owner.

- [x] Expression parser: remove recursion fully (convert list/function/aggregate paths to iterative stack-based parsing).
  - Why: Tiger Style requires no recursion (current depth cap is only a partial mitigation).
  - Files: `src/parser/expression.zig`.

- [x] B-tree split panic paths: add actionable context or convert to recoverable errors where appropriate.
  - Why: current panic strings are low-diagnostic during field failures.
  - Files: `src/storage/btree.zig`.

- [x] Error taxonomy enforcement at all public boundaries.
  - Why: taxonomy exists but is not consistently enforced at return boundaries.
  - Files: `src/tiger/error_taxonomy.zig`, plus module boundary call sites in executor/server/storage.

## Medium Priority

- [ ] Assertion-density systematic pass for remaining modules.
  - Why: we improved several modules, but coverage is still uneven.
  - Targets: `src/mvcc/undo.zig`, `src/runtime/bootstrap.zig`, remaining `src/server/session.zig` paths, broader `src/storage/wal.zig`.

- [ ] Golden encode/decode vectors for on-disk formats.
  - Why: guard against accidental format drift.
  - Targets: `src/storage/row.zig`, `src/storage/wal.zig`, and any other persistent format modules.

- [ ] B-tree split crash-consistency matrix expansion.
  - Why: current fault-matrix coverage is useful but not exhaustive per write protocol step.
  - Files: `src/simulator/fault_matrix.zig`, `src/storage/btree.zig`.

## Lower Priority / Cleanup

- [ ] Reduce split-logic duplication in B-tree (`splitAndInsert` vs `splitInternal`).
  - Why: simplify maintenance and reduce divergence risk.
  - Files: `src/storage/btree.zig`.

- [ ] Introduce scoped pin/unpin helper in mutation paths.
  - Why: reduce repetitive unpin error paths and improve reliability/readability.
  - Files: `src/executor/mutation.zig`.
