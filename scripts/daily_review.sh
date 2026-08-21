#!/usr/bin/env bash
set -euo pipefail

# Dry-run local review helper. This script summarizes ignored runtime files when
# present. It does not edit code, update private agent memory, or restart bots.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
mkdir -p logs

REVIEW_LOG="logs/daily_review_$(date +%Y%m%d_%H%M).log"

{
  echo "# Polymarket Bot Local Review"
  echo "timestamp=$(date -Is)"
  echo
  echo "## Process"
  if pgrep -f "run_full_loop.py" >/dev/null; then
    pgrep -af "run_full_loop.py"
  else
    echo "run_full_loop.py is not running"
  fi
  echo
  echo "## Runtime files"
  for path in data/portfolio_positions.json data/portfolio_history.jsonl data/loop_v2_trades.jsonl data/paper_performance.json data/calibration.json; do
    if [ -f "$path" ]; then
      echo "present: $path"
    else
      echo "missing: $path"
    fi
  done
  echo
  echo "## Recent notable logs"
  if compgen -G 'logs/*.log' >/dev/null; then
    grep -hE "SIGNAL|BUY|SOLD|STOP_LOSS|TAKE_PROFIT|ERROR|spread=" logs/*.log 2>/dev/null | tail -50 || true
  else
    echo "no logs found"
  fi
} | tee "$REVIEW_LOG"
