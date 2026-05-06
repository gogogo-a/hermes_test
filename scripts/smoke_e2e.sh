#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

"${ROOT}/scripts/infra_up.sh"

if [[ ! -f "${ROOT}/.env" ]]; then
  echo "MISSING ${ROOT}/.env — copy .env.example and fill ZHIPU_API_KEY." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source "${ROOT}/.env"
set +a

export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:19092}"
export TASK_SCHEMA_PATH="${TASK_SCHEMA_PATH:-${ROOT}/contracts/task-envelope.schema.json}"

if [[ "${GATEWAY_ADDR:-}" =~ :([0-9]+)$ ]]; then
  PORT="${BASH_REMATCH[1]}"
else
  PORT="8080"
fi

(cd "${ROOT}/gateway" && go run ./cmd/gateway) &
GW_PID="$!"

cleanup() {
  kill "${GW_PID}" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 40); do
  if curl -fsS "http://127.0.0.1:${PORT}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

BODY='{"message":"请把下面需求拆成多步：写一段中文广告文案并补充事实核查要点，同时检索 readme 中与架构相关的段落。"}'

curl -fsS \
  -H "Content-Type: application/json" \
  -X POST \
  --data-binary "${BODY}" \
  "http://127.0.0.1:${PORT}/api/v1/tasks"

echo
echo "[smoke] inbound task POST ok; ensure router/workers/RAG runs separately to consume."
