#!/usr/bin/env bash
set -euo pipefail

# Local, network-free V3 configuration status. Does not print secrets, create an
# authenticated client, inspect positions, or submit account actions.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

venv/bin/python run_v3.py validate-config
venv/bin/python run_v3.py paper-status

if pgrep -f "[r]un_full_loop.py" >/dev/null; then
  echo "Legacy process: DETECTED (manual investigation required)"
else
  echo "Legacy process: DOWN"
fi
