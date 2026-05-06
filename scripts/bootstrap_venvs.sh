#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

pick_python() {
  for cmd in python3.12 python3.13 python3.11 python3; do
    if command -v "$cmd" >/dev/null 2>&1 && "$cmd" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null; then
      echo "$cmd"
      return 0
    fi
  done
  echo "error: need Python >= 3.11 (Nous hermes-agent dependency). Install e.g. python3.12." >&2
  exit 1
}

PY="$(pick_python)"
echo "using $PY ($($PY -c 'import sys; print(sys.version)'))"

"$PY" -m venv "${ROOT}/.venv/router"
"${ROOT}/.venv/router/bin/pip" install -U pip
"${ROOT}/.venv/router/bin/pip" install -r "${ROOT}/services/router/requirements.txt"

"$PY" -m venv "${ROOT}/.venv/workers"
"${ROOT}/.venv/workers/bin/pip" install -U pip
"${ROOT}/.venv/workers/bin/pip" install -r "${ROOT}/services/workers/requirements.txt"

"$PY" -m venv "${ROOT}/.venv/rag"
"${ROOT}/.venv/rag/bin/pip" install -U pip
"${ROOT}/.venv/rag/bin/pip" install -r "${ROOT}/services/rag/requirements.txt"
