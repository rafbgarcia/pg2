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
  - pool config defaults
- Inputs:
  - memory budget from `--memory`
  - detected vCPU count
  - optional overrides (`--concurrency`, future knobs)

### Initial Rule
- Default effective concurrency = `min(vcpus, memory_limited_slots)` with min 1.

### Gate
- Unit tests for planner outputs across memory/vCPU combinations.

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
