# TODO

1. Add a concrete server transport implementation.
   - Implement real accept/read/write wiring (TCP/socket-backed) against `src/server/transport.zig`.
   - Hook the concrete accept loop into runtime/server startup flow.
   - Keep bounded request/response behavior and fail closed on overflow.

2. Continue expanding deterministic fault-matrix coverage as new durability/recovery paths land.
   - Grow seeded interleaving breadth in CI (larger seed sets and longer schedules) while keeping bounded budgets.
