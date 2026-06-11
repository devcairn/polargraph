#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

cleanup() {
  echo "Tearing down containers..."
  docker compose -f docker-compose.e2e.yml down -v
}
trap cleanup EXIT

# Start services (build images if needed).
docker compose -f docker-compose.e2e.yml up -d --build

# Resolve the dynamically-assigned host port for the REST gateway.
GATEWAY_PORT=$(docker compose -f docker-compose.e2e.yml port rest-gateway 8000 | cut -d: -f2)
BASE_URL="http://localhost:${GATEWAY_PORT}"

echo "Waiting for REST gateway at ${BASE_URL}/health ..."
for i in $(seq 1 30); do
  if curl -sf "${BASE_URL}/health" > /dev/null 2>&1; then
    echo "Gateway ready."
    break
  fi
  sleep 2
  if [ "$i" -eq 30 ]; then
    echo "ERROR: timed out waiting for REST gateway after 60s"
    docker compose -f docker-compose.e2e.yml logs
    exit 1
  fi
done

export BASE_URL

echo "Running E2E tests..."
python3 tests.py

# Exit code propagated by set -e; cleanup runs via trap.
