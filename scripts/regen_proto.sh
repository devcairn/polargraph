#!/usr/bin/env bash
# Regenerate client proto stubs from crates/polargraph-server/proto/polargraph.proto.
# Run from the repo root.
set -euo pipefail

PROTO_SRC="crates/polargraph-server/proto"
PROTO_FILE="polargraph.proto"

echo "==> Regenerating Python stubs..."
python3 -m grpc_tools.protoc \
  -I "$PROTO_SRC" \
  --python_out="clients/python/polargraph/_proto" \
  --grpc_python_out="clients/python/polargraph/_proto" \
  "$PROTO_SRC/$PROTO_FILE"

echo "==> Regenerating Go stubs..."
GO_PACKAGE="github.com/polarops/polargraph-go/polargraph/proto"
protoc \
  -I "$PROTO_SRC" \
  "--go_out=M${PROTO_FILE}=${GO_PACKAGE}:clients/go/polargraph/proto" \
  --go_opt=paths=source_relative \
  "--go-grpc_out=M${PROTO_FILE}=${GO_PACKAGE}:clients/go/polargraph/proto" \
  --go-grpc_opt=paths=source_relative \
  "$PROTO_SRC/$PROTO_FILE"

echo "Done. Commit any changed files in clients/*/."
