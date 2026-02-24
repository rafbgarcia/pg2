# Workfront 02: Self-Tune Memory and Concurrency

## Objective

Turn `--memory` into a planner input that derives runtime capacities automatically, with explicit overrides.

## User Decisions Locked

- Override flag name: `--concurrency`.
- Default pool overload policy: `queue`.
- Default queue timeout: `30s`.

## Phase 1: Planner Module

### Scope

- Add planner module that derives:
  - `max_query_slots` (effective concurrency)
  - buffer/WAL/undo/query arena budgets
  - parser budgets per query slot:
    - `parse_tokens_bytes_per_slot`
    - `parse_ast_bytes_per_slot`
  - parser effective capacities from those budgets:
    - `max_tokens_effective`
    - `max_ast_nodes_effective`
  - parser global hard caps (independent safety ceilings):
    - `hard_max_tokens`
    - `hard_max_ast_nodes`
  - `work_memory_bytes_per_slot` — per-query byte budget governing in-memory result accumulation before spill (consumed by Workfront 03 Phase 2's spill threshold)
  - `temp_pages_per_query_slot` — per-slot temp page budget for operator spill (consumed by Workfront 03 Phases 2-3). Derived from remaining disk budget after shared structures.
  - `max_active_transactions` — concurrency-derived limit on simultaneously executing transactions. Default: `max(max_query_slots * 4, 256)`. Currently hardcoded at 256 in `TxManager`. The planner makes this a runtime value so higher `--concurrency` configurations don't hit transaction admission errors under load.
  - `max_tx_states` — sliding window size for transaction state history (active/committed/aborted tracking). Default: `max_active_transactions * 256`. Currently hardcoded at 65,536 in `TxManager`. The window must be large enough that cleanup can advance `base_tx_id` before the window fills under sustained throughput. The planner scales it proportionally to active transaction capacity.
  - pool config defaults
- **Migration note**: `max_active_transactions` and `max_tx_states` are currently compile-time constants backing fixed-size arrays in `TxManager`. The planner phase must convert these to runtime-sized allocations (allocated at bootstrap from the memory budget). The memory cost is small (~1 byte per tx state + 8 bytes per active tx slot) so even large values have negligible impact on the overall budget.
- Inputs:
  - memory budget from `--memory`
  - detected vCPU count
  - optional overrides (`--concurrency`, future knobs)

### Initial Rule

- Default effective concurrency = `min(vcpus, memory_limited_slots)` with min 1.
- `work_memory_bytes_per_slot` = remaining per-slot budget after reserving shared structures (buffer pool, WAL, undo log). Before WF02 lands, Workfront 03 uses a hardcoded default (4 MB, matching PostgreSQL's `work_mem`).
- `temp_pages_per_query_slot` must be threaded through **all** runtime spill constructors (root collector temp manager and nested spill temp managers). No execution path may fall back to `default_pages_per_query_slot` once planned config is active.
- parser capacities are primarily memory-bound (`max_*_effective`), but must also be bounded by global hard caps:
  - `max_tokens_effective <= hard_max_tokens`
  - `max_ast_nodes_effective <= hard_max_ast_nodes`
- recommended initial hard caps:
  - `hard_max_tokens = 32768`
  - `hard_max_ast_nodes = 65535` (u16 `NodeIndex` upper bound)

### Gate

- Unit tests for planner outputs across memory/vCPU combinations.
- `work_memory_bytes_per_slot` scales correctly with `--memory` and `--concurrency` (more memory or less concurrency → larger per-slot budget).
- `max_active_transactions` and `max_tx_states` scale with effective concurrency (higher `--concurrency` → larger limits).
- `temp_pages_per_query_slot` is derived and scales inversely with concurrency (fewer slots → more temp pages per slot).
  - This is a hard dependency for Workfront 13 nested per-parent spill capacity and partition fanout.
- Coverage proves both root and nested spill paths honor configured `temp_pages_per_query_slot`.
- parser effective capacities scale with memory/concurrency (more memory or fewer slots → larger `max_tokens_effective` / `max_ast_nodes_effective`).
- parser effective capacities never exceed global hard caps.
- tokenizer/parser diagnostics distinguish budget exhaustion from hard-cap exhaustion.
- `TxManager` accepts runtime-sized capacity (no longer compile-time constants). Existing tests pass with the default-derived values.

### Parser Runtime Migration (Phase 1 extension)

#### Scope

- Convert parser/tokenizer fixed compile-time capacities to runtime slot-local capacities:
  - tokenizer token buffer size derived from `max_tokens_effective`
  - AST node buffer size derived from `max_ast_nodes_effective`
- Allocate parser buffers per query slot at bootstrap from planned memory budgets.
- Keep fail-closed semantics and deterministic behavior with explicit error messages.

#### Gate

- Existing parser/feature tests pass under default planned capacities.
- New boundary tests:
  - `effective_limit - 1` parses successfully.
  - `effective_limit + 1` fails with capacity diagnostics.
  - `hard_cap + 1` fails with hard-cap diagnostics even if memory budget is large.

#### Statement Size Policy and Coverage

- Keep a strict global statement-size safety boundary, but make normal parser capacity memory-planned per query slot.
- Document two distinct contracts:
  - parser effective-budget exhaustion (derived from `--memory` + `--concurrency`)
  - parser hard-cap exhaustion (global ceiling)
- Short-term coverage while tokenizer is fixed-capacity:
  - keep token-bound multi-row insert regressions in [insert_test.zig](/Users/rafa/github.com/rafbgarcia/pg2/test/features/expressions/insert_test.zig)
- Post migration coverage (when parser/tokenizer are runtime-sized):
  - add `> scan_batch_size` single-statement bulk-insert coverage in [insert_test.zig](/Users/rafa/github.com/rafbgarcia/pg2/test/features/expressions/insert_test.zig) and stress extension in [stress_mutations_test.zig](/Users/rafa/github.com/rafbgarcia/pg2/test/stress/mutations/stress_mutations_test.zig)
- Long-term ingest posture: for very large payloads, prefer bounded/streamed batching paths over unbounded single-statement growth.

## Phase 2: Startup Admission Semantics

### Scope

- Validate planned config against budget before full bootstrap.
- Return explicit startup diagnostics with required/provided bytes.

### Gate

- Boundary tests (`min-1` fail, `min` pass) for multiple profiles.

## Phase 3: CLI Integration

### Scope

- Wire planner into startup path.
- Add `--concurrency` CLI parsing and validation.
- Keep deterministic override behavior for tests.

### Gate

- CLI tests:
  - valid memory+concurrency starts
  - invalid concurrency for budget fails with clear message.

# Open questions (non-blocking for Phases 1-3)

- Do we derive the concurrency (number of query executors) based on flag? Or could internal statistics also support this decision? Does the database know better than the user how much concurrency it should have?
