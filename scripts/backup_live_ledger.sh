#!/usr/bin/env bash
# Daily consistent backup of the live V7 ledger and its paired live_state.json.
#
# ledger.sqlite and live_state.json are a PAIR: resolution validators replay the
# ledger against the baselines/IDs in live_state.json, so always restore both
# files from the same YYYYMMDD directory, with the bot stopped.
#
# The sqlite3 CLI is not installed, so scripts/backup_live_ledger.py uses
# Python's sqlite3 backup API on a read-only (mode=ro) connection, which yields a
# consistent snapshot while the bot writes. live_state.json is read before and
# after the ledger snapshot and must be byte-identical (up to 3 attempts).
# A same-day rerun builds a tmp dir, moves the old day aside, swaps the new one
# in, then deletes the old one. Keeps 30 days; stale work dirs are cleaned.
#
# Writes only under data/backups/live-v7/. The live files are never modified,
# but opening a WAL database (even mode=ro) can create its -wal/-shm sidecar
# files in data/live-v7 if they do not exist yet (the running bot has them).
set -euo pipefail
umask 077  # backups hold order/condition IDs and balances: owner-only

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIR="${LIVE_DATA_DIR:-$ROOT_DIR/data/live-v7}"
DEST_ROOT="${BACKUP_ROOT:-$ROOT_DIR/data/backups/live-v7}"
KEEP_DAYS="${KEEP_DAYS:-30}"
PYTHON="${PYTHON:-python3}"

exec "$PYTHON" "$ROOT_DIR/scripts/backup_live_ledger.py" "$SRC_DIR" "$DEST_ROOT" "$KEEP_DAYS"
