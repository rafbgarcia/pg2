# TODO

1. Wire transport-level server connection handling into `server.session.Session.handleRequest`.
   - Add the accept/read/write loop and route inbound request bytes through the existing session handler.
   - Keep request/response buffers bounded and fail closed on overflow.

2. Complete memory budget plumbing from CLI to runtime bootstrap.
   - Use `--memory` from `src/main.zig` to allocate the runtime memory region.
   - Initialize `runtime.bootstrap.BootstrappedRuntime` with that region.
   - Surface explicit startup errors for `error.InsufficientMemoryBudget` and `error.InvalidConfig`.

3. Wire Tiger error taxonomy at subsystem boundaries.
   - Apply `src/tiger/error_taxonomy.zig` classification where errors cross runtime/server boundaries.
   - Ensure public-facing error paths consistently emit mapped error classes.

4. Continue expanding deterministic fault-matrix coverage as new durability/recovery paths land.
   - Grow seeded interleaving breadth in CI (larger seed sets and longer schedules) while keeping bounded budgets.

5. Before each handoff, run validation and update tracking docs.
   - Run `zig build test`.
