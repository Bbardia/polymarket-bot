#!/usr/bin/env python3
"""Consistent backup of the live V7 ledger and its paired live_state.json (stdlib only).

Called by scripts/backup_live_ledger.sh. See that file for the operational notes.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ATTEMPTS = 3
RETRY_SLEEP_SECONDS = 2.0
STALE_WORKDIR_SECONDS = 3600


class PairChanged(RuntimeError):
    pass


def snapshot_pair(src: Path, workdir: Path, *, attempts: int = ATTEMPTS,
                  sleep: float = RETRY_SLEEP_SECONDS) -> int:
    """Ledger backup bracketed by two byte-identical reads of live_state.json."""
    for attempt in range(1, attempts + 1):
        before = (src / "live_state.json").read_bytes()
        # mode=ro never modifies the database, but opening a WAL database may
        # create its -wal/-shm sidecars if they do not exist yet.
        source = sqlite3.connect(f"file:{src / 'ledger.sqlite'}?mode=ro", uri=True)
        target = sqlite3.connect(workdir / "ledger.sqlite")
        try:
            with target:
                source.backup(target)
        finally:
            source.close()
        after = (src / "live_state.json").read_bytes()
        if before == after:
            try:
                target.execute("PRAGMA journal_mode=DELETE")
                check = target.execute("PRAGMA integrity_check").fetchone()[0]
                events = target.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            finally:
                target.close()
            if check != "ok":
                raise RuntimeError(f"backup integrity_check failed: {check}")
            json.loads(before.decode("utf-8"))
            (workdir / "live_state.json").write_bytes(before)
            return events
        target.close()
        (workdir / "ledger.sqlite").unlink()
        print(f"live_state.json changed during attempt {attempt}; retrying", file=sys.stderr)
        if attempt < attempts:
            time.sleep(sleep)
    raise PairChanged(f"live_state.json kept changing during {attempts} attempts; no backup written")


def backup(src: Path, dest_root: Path, *, keep_days: int = 30, now: datetime | None = None,
           attempts: int = ATTEMPTS, sleep: float = RETRY_SLEEP_SECONDS) -> Path:
    now = now or datetime.now(timezone.utc)
    stamp = f"{now.strftime('%Y%m%d%H%M%S')}-{os.getpid()}"
    dest = dest_root / now.strftime("%Y%m%d")
    tmp = dest_root / f".tmp-{stamp}"
    old = dest_root / f".old-{stamp}"
    dest_root.mkdir(parents=True, exist_ok=True)
    tmp.mkdir()
    try:
        events = snapshot_pair(src, tmp, attempts=attempts, sleep=sleep)
        # Same-day rerun: move the previous copy aside, put the new one in place,
        # then delete the old one, so a complete day directory always exists.
        if dest.exists():
            dest.rename(old)
        tmp.rename(dest)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        if old.exists() and not dest.exists():
            old.rename(dest)
        raise
    shutil.rmtree(old, ignore_errors=True)
    print(f"backup ok: {dest} ({events} ledger events)")
    prune(dest_root, keep_days=keep_days, now=now)
    return dest


def prune(dest_root: Path, *, keep_days: int, now: datetime) -> None:
    cutoff = (now - timedelta(days=keep_days)).strftime("%Y%m%d")
    for child in sorted(dest_root.iterdir()):
        if not child.is_dir():
            continue
        if len(child.name) == 8 and child.name.isdigit() and child.name < cutoff:
            shutil.rmtree(child)
            print(f"pruned {child}")
        elif (child.name.startswith((".tmp-", ".old-"))
              and time.time() - child.stat().st_mtime > STALE_WORKDIR_SECONDS):
            shutil.rmtree(child, ignore_errors=True)
            print(f"removed stale work dir {child}")


def main(argv: list[str]) -> int:
    src, dest_root, keep_days = Path(argv[0]), Path(argv[1]), int(argv[2])
    try:
        backup(src, dest_root, keep_days=keep_days)
    except (PairChanged, RuntimeError, OSError, sqlite3.Error, ValueError) as exc:
        print(f"backup failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
