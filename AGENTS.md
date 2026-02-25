project tags: database engineering; Zig 0.15.2; Tiger Style (TigerBeetle); useful assertions; deterministic simulation testing; static allocation, linux-only target.
project stage: greenfield; not live; no users; zero backwards compatibility concerns.
behaviors: write solid foundation production-grade database code; commit as you go in logical slices; keep workfront up-to-date.
paths: `./test/features` for 1-1 supported user-facing features; `./WORKFRONTS.md`
commands: `zig build test --summary all`

- HARD STOP on ambiguious decisions; design choices; compromises.
- HARD STOP when you notice design flaws; inappropriate production implementation; tests flaws.
