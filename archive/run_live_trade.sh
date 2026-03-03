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

source "./$ENV_FILE"

PY_BIN="/Users/killdead/opt/anaconda3/bin/python3"
if [[ ! -x "$PY_BIN" ]]; then
  PY_BIN="python3"
fi

DB="${BOT_DB:-polymarket_collector.db}"
TARGET="${BOT_TARGET_PRICE:-0.49}"
SIZE="${BOT_SIZE_SHARES:-5}"

if [[ $# -ge 1 ]]; then
  SLUG="$1"
  shift || true
  exec "$PY_BIN" btc5m_trading_bot.py \
    --db "$DB" \
    --slug "$SLUG" \
    --mode live \
    --target-price "$TARGET" \
    --size-shares "$SIZE" \
    --live-confirm \
    --live-poll-seconds 1 \
    "$@"
else
  exec "$PY_BIN" btc5m_trading_bot.py \
    --db "$DB" \
    --probe-next \
    --mode live \
    --target-price "$TARGET" \
    --size-shares "$SIZE" \
    --live-confirm \
    --live-poll-seconds 1
fi
