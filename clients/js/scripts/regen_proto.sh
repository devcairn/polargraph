#!/usr/bin/env bash
# Regenerate TypeScript types from polargraph.proto using ts-proto.
# Run from the clients/js directory: scripts/regen_proto.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROTO_DIR="$(cd "$JS_DIR/../../crates/polargraph-server/proto" && pwd)"

echo "Regenerating TypeScript proto types from $PROTO_DIR/polargraph.proto"

protoc \
  --plugin=protoc-gen-ts_proto="$JS_DIR/node_modules/.bin/protoc-gen-ts_proto" \
  --ts_proto_out="$JS_DIR/src/proto" \
  --ts_proto_opt=outputServices=grpc-js \
  --ts_proto_opt=esModuleInterop=true \
  --ts_proto_opt=env=node \
  --ts_proto_opt=useOptionals=messages \
  --proto_path="$PROTO_DIR" \
  "$PROTO_DIR/polargraph.proto"

echo "Done — src/proto/polargraph.ts regenerated"
