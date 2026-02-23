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

### Gate

- Unit tests for planner outputs across memory/vCPU combinations.
- `work_memory_bytes_per_slot` scales correctly with `--memory` and `--concurrency` (more memory or less concurrency → larger per-slot budget).
- `max_active_transactions` and `max_tx_states` scale with effective concurrency (higher `--concurrency` → larger limits).
- `temp_pages_per_query_slot` is derived and scales inversely with concurrency (fewer slots → more temp pages per slot).
- `TxManager` accepts runtime-sized capacity (no longer compile-time constants). Existing tests pass with the default-derived values.

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

# Open questions

- Do we derive the concurrency (number of query executors) based on flag? Or could internal statistics also support this decision? Does the database know better than the user how much concurrency it should have?
