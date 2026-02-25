# pg2 Workfronts

This file is the top-level index for multi-session delivery work.
Each workfront has phased gates so fresh Codex sessions can resume safely.

## Strict Sequential Lane (One Workfront at a Time)

Suggested implementation order.

| Order | Workfront                                                                         | Why This Position                                                                                       |
| ----- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| 6     | `docs/workfronts/14_runtime_storage_backend_workfront.md` (complete) | Enforces real file-backed runtime boundaries so memory limits are production-real, not simulation-only. |
| 7     | `docs/workfronts/06_variables_and_multi_statement_workfront.md`                   | User-facing execution semantics come after reclamation/storage foundations are reliable.                |
| 8     | `docs/workfronts/07_adaptive_planning_workfront.md`                               | Planner policy hardening after multi-statement semantics are in place and measurable.                   |
| 9     | `docs/workfronts/04_advisor_observability_workfront.md`                           | Advisor quality depends on stable metrics emitted by execution/planning/runtime paths above.            |
| 10    | `docs/workfronts/08_expression_language_v1_workfront.md`                          | Expression work is mostly parity/diagnostic closeout and is safer after core runtime contracts settle.  |
| 11    | `docs/workfronts/05_test_matrix_workfront.md`                                     | Cross-profile reliability matrix should validate the consolidated behavior from prior workfronts.       |
| 14    | `docs/workfronts/10_iterator_execution_model_workfront.md` (deferred)             | Structural executor rewrite; only start when subqueries/CTEs/window functions become active priorities. |
| 15    | `docs/workfronts/15_module_decomposition_drift_workfront.md` (maintenance-only)   | Tracks post-completion drift from decomposition gates without re-opening original extraction history.   |

## 1.0.0 Release Hardening (Post-Sequence)

Complete this section after the strict sequential lane before tagging `v1.0.0`.

| Item                                  | Why It Is Required For 1.0.0                                                                                                    |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Crash/Recovery Proof                  | Proves no silent corruption or data loss across kill/restart and WAL replay boundaries.                                         |
| Backup/Restore Tooling                | Ensures you can recover from operator mistakes and host failures in real usage.                                                 |
| On-Disk Format Versioning             | Prevents unsafe boot on incompatible data formats and enables explicit upgrade policy.                                          |
| Operational Safety Contracts          | Makes failure modes explicit: deterministic errors, limits, and fail-closed boundaries.                                         |
| Soak + Performance Baseline           | Validates long-running stability (memory/storage growth, reclaim behavior, latency envelopes).                                  |
| Security Baseline (Context-Dependent) | Local single-user only: minimal auth may be optional. Any shared/remote access: require auth + role separation before `v1.0.0`. |

### Security Baseline Details

If the server is reachable by other users/processes/machines, minimum required before `v1.0.0`:

1. Authentication enabled by default (no anonymous access).
2. At least two roles: read-only and read-write (admin retained for schema/ops).
3. Audit trail for auth failures and schema-changing operations.
