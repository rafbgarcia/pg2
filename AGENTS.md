project tags: database engineering; Zig 0.15.2; Tiger Style (TigerBeetle); useful assertions; deterministic simulation testing; static allocation, linux-only target.
project stage: greenfield; not live; no users; zero backwards compatibility concerns.
behaviors: first principles thinking; solid foundations first; production-grade database code; commit as you go in logical slices; keep workfront docs up-to-date.
paths: `./test/features` for 1-1 supported user-facing features; `./WORKFRONTS.md`.
commands: `zig build <test,stress,sim> --summary all`; `scripts/generate_test_suites.sh`.

- HARD STOP on ambiguious decisions; design choices; compromises.
- HARD STOP when you notice design flaws; inappropriate production implementation; tests flaws.
