# TODO

1. Add a concrete server transport implementation.
   - Replace the temporary blocking socket loop in `src/main.zig` with the planned Linux `io_uring` event-loop backend.
   - Preserve deterministic/fake transport path for simulation and unit tests.
   - Keep bounded request/response behavior and fail closed on overflow.

2. Continue expanding deterministic fault-matrix coverage as new durability/recovery paths land.
   - Grow seeded interleaving breadth in CI (larger seed sets and longer schedules) while keeping bounded budgets.
