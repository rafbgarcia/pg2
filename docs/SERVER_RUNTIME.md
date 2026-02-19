# Server Runtime Notes

## Target

Server runtime is Linux-only for production execution.
On macOS development machines, run server-path tests inside Docker.

## Backend Strategy

- Core DB logic remains transport-agnostic and deterministic.
- Server I/O backend is Linux-only and moves to `io_uring` for production.
- Deterministic tests keep using fake/test-double transport implementations.

## Current State

- `src/server/transport.zig` defines transport interfaces (`Acceptor`, `Connection`).
- `src/server/tcp_transport.zig` provides a concrete blocking TCP implementation.
- `src/main.zig` supports `--listen <host:port>` to enter accept-loop mode.
- Non-Linux `--listen` exits with an explicit Linux-only message.

## macOS Dev/Test Path

Use Docker for Linux-path validation:

```bash
scripts/test-linux-docker.sh
```

You can override the image:

```bash
ZIG_DOCKER_IMAGE=ziglang/zig:0.15.2 scripts/test-linux-docker.sh
```
