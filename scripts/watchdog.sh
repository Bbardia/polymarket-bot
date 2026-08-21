#!/usr/bin/env bash
set -euo pipefail

# Opt-in local watchdog. It never starts live trading unless .env explicitly has
# ENABLE_LIVE_TRADING=true and PAPER_TRADING=false.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
mkdir -p logs

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

if ! curl -s --max-time 5 -o /dev/null "${POLY_CLOB_HOST:-https://clob.polymarket.com}/time"; then
  echo "$(date -Is): Polymarket API unreachable" >> logs/watchdog.log
fi

if pgrep -f "run_full_loop.py" > /dev/null; then
  echo "$(date -Is): bot already running" >> logs/watchdog.log
  exit 0
fi

if [ "${ENABLE_LIVE_TRADING:-false}" = "true" ] && [ "${PAPER_TRADING:-true}" = "false" ]; then
  echo "$(date -Is): bot down; restarting live loop" >> logs/watchdog.log
  venv/bin/python run_full_loop.py --live >> logs/watchdog.log 2>&1 &
  echo "$(date -Is): restarted with PID $!" >> logs/watchdog.log
else
  echo "$(date -Is): bot down; not restarting because live trading is not explicitly enabled" >> logs/watchdog.log
fi
