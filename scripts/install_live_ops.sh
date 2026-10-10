#!/usr/bin/env bash
# Idempotently install the live-ops user units (log rotation, backups)
# and the repo copy of polymarket-v7-live.service into ~/.config/systemd/user.
#
# It NEVER starts, stops or restarts polymarket-v7-live. The new logging and
# restart settings take effect only after the operator restarts the unit.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT_SRC="$ROOT_DIR/deploy/systemd"
UNIT_DST="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
TIMERS=(polymarket-logrotate.timer polymarket-ledger-backup.timer)

command -v logrotate >/dev/null || [ -x /usr/sbin/logrotate ] || {
  echo "logrotate is not installed (apt install logrotate)" >&2; exit 1; }

# StandardOutput=append: needs the directory to exist before the unit starts.
mkdir -p "$ROOT_DIR/logs/live" "$ROOT_DIR/data/backups/live-v7"
chmod 700 "$ROOT_DIR/data/backups" "$ROOT_DIR/data/backups/live-v7"
chmod +x "$ROOT_DIR/scripts/backup_live_ledger.sh"

mkdir -p "$UNIT_DST"
for unit in "$UNIT_SRC"/*.service "$UNIT_SRC"/*.timer; do
  name="$(basename "$unit")"
  if [ "$name" = "polymarket-v7-live.service" ] && [ -f "$UNIT_DST/$name" ] \
     && ! cmp -s "$unit" "$UNIT_DST/$name"; then
    cp "$UNIT_DST/$name" "$UNIT_DST/$name.bak-$(date +%Y%m%d%H%M%S)"
  fi
  install -m 0644 "$unit" "$UNIT_DST/$name"
  echo "installed $UNIT_DST/$name"
done

systemctl --user daemon-reload
systemctl --user enable --now "${TIMERS[@]}"
systemctl --user list-timers "${TIMERS[@]}" --no-pager || true

cat <<EOF

Done. polymarket-v7-live was NOT restarted. To pick up file logging
(logs/live/live.log), RestartSec=30s, the DNS wait and StartLimitIntervalSec=0, run:

  systemctl --user restart polymarket-v7-live

Then check: tail -f $ROOT_DIR/logs/live/live.log
EOF
