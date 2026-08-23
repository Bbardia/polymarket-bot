#!/usr/bin/env bash
set -euo pipefail

# Legacy name retained for compatibility. V3 intentionally has no autonomous
# restart path. This script can only report state; it never launches Python.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
mkdir -p logs

if pgrep -f "[r]un_full_loop.py" >/dev/null; then
  echo "$(date -Is): legacy process detected; manual investigation required" >> logs/watchdog.log
  exit 1
fi

echo "$(date -Is): no bot process; automatic restart is disabled for V3" >> logs/watchdog.log
exit 0
