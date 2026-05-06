#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "${ROOT}/.env" ]]; then
  echo "Create ${ROOT}/.env from .env.example first (ZHIPU_API_KEY)." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
source "${ROOT}/.env"
set +a

export PYTHONPATH="${ROOT}/shared/python:${PYTHONPATH:-}"

"${ROOT}/.venv/router/bin/python" "${ROOT}/services/router/main.py"
