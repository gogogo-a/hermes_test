#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "${ROOT}/.env" ]]; then
  echo "Create ${ROOT}/.env from .env.example first." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source "${ROOT}/.env"
set +a

export PYTHONPATH="${ROOT}/shared/python:${PYTHONPATH:-}"

exec "${ROOT}/.venv/rag/bin/uvicorn" app:app \
  --app-dir "${ROOT}/services/rag" \
  --host 127.0.0.1 \
  --port "${RAG_HTTP_PORT:-8090}"
