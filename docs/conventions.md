Project-wide conventions for tests. Optimize for fast, trustworthy feedback during development.

# `test/end_user_docs` is documentation-like and user-facing.

e.g. show end-users how to use features.

# `test/core` is exhaustive and implementation-focused.

e.g. prove fail-closed behavior and deep correctness invariants, cover edge cases and fault paths.

# `test/stress` is heavy and optional for routine local loops.

e.g. catch regressions in runtime envelope (time, memory, churn) under heavier load.
