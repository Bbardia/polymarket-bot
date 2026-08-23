#!/usr/bin/env bash
set -euo pipefail

# Local, network-free V3 configuration status. Does not print secrets, create an
# authenticated client, inspect positions, or submit account actions.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

venv/bin/python run_v3.py validate-config

if pgrep -f "[r]un_full_loop.py" >/dev/null; then
  echo "Legacy process: DETECTED (manual investigation required)"
else
  echo "Bot process: DOWN (expected; V3 execution is not enabled)"
fi
