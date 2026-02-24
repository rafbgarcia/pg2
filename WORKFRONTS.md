# pg2 Workfronts

This file is the top-level index for multi-session delivery work.
Each workfront has phased gates so fresh Codex sessions can resume safely.

## Strict Sequential Lane (One Workfront at a Time)

Follow this order strictly. Start the next item only after the current item's gates are green (or the item is already marked complete/deferred).

| Order | Workfront | Why This Position |
| --- | --- | --- |
| 1 | `docs/workfronts/01_server_concurrency_workfront.md` (complete) | Runtime request scheduling foundation; later runtime/storage behavior assumes this path is stable. |
| 2 | `docs/workfronts/02_self_tune_memory_workfront.md` | Turns memory/concurrency into explicit runtime budgets consumed by spill/planner/runtime gates. |
| 3 | `docs/workfronts/03_degrade_spill_workfront.md` (core complete; guardrail tracking) | Spill/degrade correctness is a prerequisite safety contract for user-facing growth work. |
| 4 | `docs/workfronts/11_write_performance_workfront.md` (phases 1-6 complete) | Establishes index/WAL/write-path patterns that storage reclamation builds on directly. |
| 5 | `docs/workfronts/12_storage_reclamation_workfront.md` (**recommended next**) | Reclaims dead heap/overflow/index storage to prevent long-running correctness/perf debt. |
| 6 | `docs/workfronts/14_runtime_storage_backend_workfront.md` (active follow-through) | Enforces real file-backed runtime boundaries so memory limits are production-real, not simulation-only. |
| 7 | `docs/workfronts/06_variables_and_multi_statement_workfront.md` | User-facing execution semantics come after reclamation/storage foundations are reliable. |
| 8 | `docs/workfronts/07_adaptive_planning_workfront.md` | Planner policy hardening after multi-statement semantics are in place and measurable. |
| 9 | `docs/workfronts/04_advisor_observability_workfront.md` | Advisor quality depends on stable metrics emitted by execution/planning/runtime paths above. |
| 10 | `docs/workfronts/08_expression_language_v1_workfront.md` | Expression work is mostly parity/diagnostic closeout and is safer after core runtime contracts settle. |
| 11 | `docs/workfronts/05_test_matrix_workfront.md` | Cross-profile reliability matrix should validate the consolidated behavior from prior workfronts. |
| 12 | `docs/workfronts/13_nested_spill_hash_join_workfront.md` (core complete; refinement track) | Keep as targeted refinement/hard-boundary expansion once primary foundations are stable. |
| 13 | `docs/workfronts/09_module_decomposition_workfront.md` (complete) | Mechanical extraction track; keep as maintenance-only unless new large modules regress clarity. |
| 14 | `docs/workfronts/10_iterator_execution_model_workfront.md` (deferred) | Structural executor rewrite; only start when subqueries/CTEs/window functions become active priorities. |

## Cross-Workfront Rules

1. Core code continues to use `Storage`/`Network` abstractions.
2. HARD STOP on ambiguous design decisions, compromises, or test flaws; lock decisions in the active workfront before coding.
3. Keep commits in logical slices aligned to a single phase task and its gate.

## 1.0.0 Release Hardening (Post-Sequence)

Complete this section after the strict sequential lane before tagging `v1.0.0`.

| Item | Why It Is Required For 1.0.0 |
| --- | --- |
| Crash/Recovery Proof | Proves no silent corruption or data loss across kill/restart and WAL replay boundaries. |
| Backup/Restore Tooling | Ensures you can recover from operator mistakes and host failures in real usage. |
| On-Disk Format Versioning | Prevents unsafe boot on incompatible data formats and enables explicit upgrade policy. |
| Operational Safety Contracts | Makes failure modes explicit: deterministic errors, limits, and fail-closed boundaries. |
| Soak + Performance Baseline | Validates long-running stability (memory/storage growth, reclaim behavior, latency envelopes). |
| Security Baseline (Context-Dependent) | Local single-user only: minimal auth may be optional. Any shared/remote access: require auth + role separation before `v1.0.0`. |

### Security Baseline Details

If the server is reachable by other users/processes/machines, minimum required before `v1.0.0`:
1. Authentication enabled by default (no anonymous access).
2. At least two roles: read-only and read-write (admin retained for schema/ops).
3. Audit trail for auth failures and schema-changing operations.
