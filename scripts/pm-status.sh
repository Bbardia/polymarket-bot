#!/usr/bin/env bash
set -euo pipefail

# Local portfolio/status helper. It reads only local `.env` and ignored runtime
# files. It does not print private keys and does not place orders.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

echo "=== Polymarket Bot Status ==="
if pgrep -f "run_full_loop.py" > /dev/null; then
  PID=$(pgrep -f "run_full_loop.py" | head -1)
  UPTIME=$(ps -o etime= -p "$PID" 2>/dev/null | xargs || true)
  echo "Bot: RUNNING (PID $PID, uptime ${UPTIME:-unknown})"
else
  echo "Bot: DOWN"
fi

echo "Mode flags: PAPER_TRADING=${PAPER_TRADING:-true}, ENABLE_LIVE_TRADING=${ENABLE_LIVE_TRADING:-false}"

if [ -n "${POLY_FUNDER_ADDRESS:-}" ] && command -v polymarket >/dev/null 2>&1; then
  echo ""
  echo "=== On-Chain Positions ==="
  polymarket -o json data positions "$POLY_FUNDER_ADDRESS" 2>/dev/null | python3 - <<'PY' || true
import json, sys
try:
    positions = json.load(sys.stdin)
except Exception:
    print("  Failed to fetch positions")
    raise SystemExit
active = [p for p in positions if float(p.get("current_value", 0) or 0) > 0]
total_value = sum(float(p.get("current_value", 0) or 0) for p in positions)
total_pnl = sum(float(p.get("cash_pnl", 0) or 0) for p in positions)
print(f"  Active: {len(active)} positions")
print(f"  On-chain value: ${total_value:.2f}")
print(f"  Unrealized P&L: ${total_pnl:.2f}")
PY
else
  echo "On-chain positions: skipped (missing POLY_FUNDER_ADDRESS or polymarket CLI)"
fi

echo ""
echo "=== Local Portfolio Tracker ==="
python3 - <<'PY'
import json
from pathlib import Path
positions_path = Path("data/portfolio_positions.json")
history_path = Path("data/portfolio_history.jsonl")
if not positions_path.exists() and not history_path.exists():
    print("  No local runtime data found")
    raise SystemExit
if positions_path.exists():
    positions = json.loads(positions_path.read_text() or "{}")
    print(f"  Local open positions: {len(positions)}")
if history_path.exists():
    trades = [json.loads(line) for line in history_path.read_text().splitlines() if line.strip()]
    total_pnl = sum(float(t.get("pnl", 0) or 0) for t in trades)
    wins = sum(1 for t in trades if float(t.get("pnl", 0) or 0) > 0)
    print(f"  Closed: {len(trades)} trades ({wins}W/{len(trades)-wins}L), P&L: ${total_pnl:.2f}")
PY
