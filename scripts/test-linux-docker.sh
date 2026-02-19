#!/usr/bin/env bash
set -euo pipefail

# Linux-only server integration path helper for macOS development.
# Override ZIG_DOCKER_IMAGE if you need a different image/tag.
IMAGE="${ZIG_DOCKER_IMAGE:-ziglang/zig:0.15.2}"

docker run --rm \
  -v "$PWD:/work" \
  -w /work \
  "$IMAGE" \
  zig build test
