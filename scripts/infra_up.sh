#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${ROOT}/infra"
docker compose up -d redpanda redis qdrant

echo "waiting for kafka (9092 internal / 19092 host) ..."
for _ in $(seq 1 30); do
  if nc -z localhost 19092 2>/dev/null; then
    echo "kafka up"
    break
  fi
  sleep 1
done

if ! nc -z localhost 19092 2>/dev/null; then
  echo "Kafka not reachable on localhost:19092" >&2
  exit 1
fi

echo "creating Hermes Kafka topics if missing ..."
docker compose exec -T redpanda rpk topic create \
  hermes.tasks.inbound \
  hermes.tasks.dispatch \
  hermes.tasks.results \
  hermes.tasks.dlq \
  -p 1 -r 1 2>/dev/null || true

if ! nc -z localhost 6333 2>/dev/null; then
  echo "Qdrant not reachable on localhost:6333" >&2
  exit 1
fi

echo "infra ready"
