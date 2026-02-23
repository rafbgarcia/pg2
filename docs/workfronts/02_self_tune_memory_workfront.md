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
  - pool config defaults
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
