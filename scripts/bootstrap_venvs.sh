#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 -m venv "${ROOT}/.venv/router"
"${ROOT}/.venv/router/bin/pip" install -r "${ROOT}/services/router/requirements.txt"

python3 -m venv "${ROOT}/.venv/workers"
"${ROOT}/.venv/workers/bin/pip" install -r "${ROOT}/services/workers/requirements.txt"

python3 -m venv "${ROOT}/.venv/rag"
"${ROOT}/.venv/rag/bin/pip" install -r "${ROOT}/services/rag/requirements.txt"
