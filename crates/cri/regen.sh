#!/bin/sh
# Regenerate src/generated/runtime.v1.rs from proto/api.proto. protoc is
# vendored by the helper crate, so nothing needs installing.
set -eu
cd "$(dirname "$0")"
mkdir -p src/generated
cargo run --manifest-path gen/Cargo.toml
