#!/usr/bin/env bash
set -euo pipefail

ENV_FILE=""
if [[ -f "POLYMARKET_ENV.sh" ]]; then
  ENV_FILE="POLYMARKET_ENV.sh"
elif [[ -f ".env.polymarket" ]]; then
  ENV_FILE=".env.polymarket"
else
  echo "Missing POLYMARKET_ENV.sh or .env.polymarket" >&2
  exit 1
fi

echo "[runner] using_env=$ENV_FILE"
source "./$ENV_FILE"

PY_BIN="${PY_BIN:-python3}"
if [[ "$PY_BIN" == "python3" ]] && command -v /Users/killdead/opt/anaconda3/bin/python3 >/dev/null 2>&1; then
  PY_BIN="/Users/killdead/opt/anaconda3/bin/python3"
fi
echo "[runner] using_python=$PY_BIN"

"$PY_BIN" test_polymarket_relayer_api.py "$@"
