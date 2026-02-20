• Prioritize integrity and determinism before type breadth.

  1. Constraints first (highest impact)

  - NOT NULL, PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY
  - Why first: they define correctness guarantees and force clear engine semantics (validation timing, error behavior, rollback).
  - For pg2 principles, especially implement FK with explicit referential actions only (fail closed if unspecified/unsupported).

  2. Core field types second (small, solid set)

  - Start with: BOOL, INT64, FLOAT64, DECIMAL, TEXT, BYTEA, DATE, TIMESTAMP
  - Why: enough for real apps, but small enough to harden encoding, comparison, null semantics, and versioned on-disk format.

  3. Indexes + constraint integration third

  - Backing indexes for PRIMARY KEY/UNIQUE
  - Ensure constraint checks use indexes correctly and deterministically under MVCC.

  4. DDL/migration safety fourth

  - ALTER TABLE ADD COLUMN, SET/DROP NOT NULL, ADD/DROP CONSTRAINT
  - Focus on deterministic, replayable behavior and explicit unsupported-case errors.

  5. Then expand type surface

  - UUID, JSON, arrays, enums, collation-aware text behavior, etc.

  If you want, I can draft a concrete test roadmap under test/features/ (one file per feature) with edge cases and execution order.

-----------


• Findings (ordered by severity)

  1. No coverage for constraint enforcement on update/delete paths
     test/features/constraints/not_null_test.zig:5, test/features/constraints/duplicate_key_test.zig:5, test/features/constraints/foreign_key_violation_test.zig:5 only exercise insert.
     Risk: engine can pass all constraint tests while allowing illegal update/delete mutations in production.
  2. Foreign-key tests miss the behavior actually declared in schema (onDeleteRestrict, onUpdateCascade)
     test/features/constraints/foreign_key_violation_test.zig:18 declares referential actions, but test only checks missing-parent insert failure.
     Risk: referential actions regress silently (restrict/cascade semantics not verified).
  3. Failed-write atomicity is not asserted after constraint errors
     Examples: test/features/constraints/duplicate_key_test.zig:26, test/features/constraints/not_null_test.zig:18, test/features/constraints/foreign_key_violation_test.zig:22 validate error text but do not query table state after
     failure.
     Risk: partial writes/side effects could exist unnoticed.
  4. No test coverage for UNIQUE (non-PK) or CHECK constraints
     Constraints suite imports only: default_values, duplicate_key, foreign_key_violation, nullable, not_null (test/features/features_specs_test.zig:7).
     Risk: major integrity features are unverified.
  5. Null/default semantics are only tested on inserts, single-row happy paths
     test/features/constraints/nullable_test.zig:57, test/features/constraints/default_values_test.zig:5.
     Missing real-world cases: update-to-null, update-from-null, explicit value overriding default on update, and repeated-row behavior.
  6. Test harness forces single query slot, so no contention/concurrency constraint cases are covered
     test/features/test_env_test.zig:125 sets .max_query_slots = 1.
     Residual risk: duplicate-key/FK race behavior under concurrent writers is untested.

  Recommended next test files (highest value first)

  1. test/features/constraints/foreign_key_actions_test.zig
  2. test/features/constraints/constraint_atomicity_test.zig
  3. test/features/constraints/update_constraint_enforcement_test.zig
  4. test/features/constraints/unique_constraint_test.zig
  5. test/features/constraints/check_constraint_test.zig
