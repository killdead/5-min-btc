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

if [[ $# -lt 1 ]]; then
  echo "Usage: ./run_relayer_redeem.sh --condition-id 0x... --up-shares N --down-shares N [--execute] [--wait]" >&2
  exit 1
fi

if ! command -v npx >/dev/null 2>&1; then
  echo "npx not found (Node.js required)" >&2
  exit 1
fi

npx tsx relayer_redeem_proxy.ts "$@"
