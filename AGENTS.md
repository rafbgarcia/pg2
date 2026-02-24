project tags: database engineering, Zig 0.15.2, Tiger Style (TigerBeetle), useful assertions, deterministic simulation testing, static allocation.
project stage: greenfield, not live, no users, zero backwards compatibility concerns.
behaviors: write solid foundation production-grade database code, commit in logical slices.
paths: `./test/features` for 1-1 supported user-facing features; `./WORKFRONTS.md`
commands: `zig build test --summary all`

- HARD STOP on ambiguious decisions, design choices, compromises.
- HARD STOP when you notice design flaws, inappropriate production implementation, tests flaws.
