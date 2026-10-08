"""Offline tests for scripts/backup_live_ledger.{sh,py} against a throwaway ledger."""
import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
spec = importlib.util.spec_from_file_location("backup_live_ledger", SCRIPTS / "backup_live_ledger.py")
bk = importlib.util.module_from_spec(spec)
sys.modules["backup_live_ledger"] = bk
spec.loader.exec_module(bk)

NOW = datetime(2026, 10, 8, 3, 15, tzinfo=timezone.utc)


def make_live(tmp_path, rows=("a", "b"), state=None):
    src = tmp_path / "live-v7"
    src.mkdir(exist_ok=True)
    db = sqlite3.connect(src / "ledger.sqlite")
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE IF NOT EXISTS events (sequence INTEGER PRIMARY KEY, event_id TEXT)")
    db.executemany("INSERT INTO events(event_id) VALUES (?)", [(row,) for row in rows])
    db.commit()
    (src / "live_state.json").write_text(json.dumps(state or {"baseline_cash": "1"}))
    return src, db


def count(path):
    with sqlite3.connect(path) as db:
        return db.execute("SELECT COUNT(*) FROM events").fetchone()[0]


def test_shell_wrapper_copies_pair_and_prunes_old_days(tmp_path):
    src, db = make_live(tmp_path)  # left open: WAL content must still be captured
    dest_root = tmp_path / "backups"
    (dest_root / "20000101").mkdir(parents=True)
    (dest_root / "keep-me").mkdir()

    result = subprocess.run(["bash", str(SCRIPTS / "backup_live_ledger.sh")], capture_output=True, text=True,
                            check=True, env={**os.environ, "LIVE_DATA_DIR": str(src),
                                             "BACKUP_ROOT": str(dest_root), "PYTHON": sys.executable})
    db.close()

    days = sorted(p.name for p in dest_root.iterdir())
    assert "20000101" not in days and "keep-me" in days
    day = next(dest_root / name for name in days if name.isdigit())
    assert count(day / "ledger.sqlite") == 2
    assert json.loads((day / "live_state.json").read_text()) == {"baseline_cash": "1"}
    assert "backup ok" in result.stdout


def test_same_day_rerun_replaces_atomically(tmp_path):
    src, db = make_live(tmp_path)
    dest_root = tmp_path / "backups"
    first = bk.backup(src, dest_root, now=NOW)
    db.execute("INSERT INTO events(event_id) VALUES ('c')")
    db.commit()
    (src / "live_state.json").write_text(json.dumps({"baseline_cash": "2"}))
    second = bk.backup(src, dest_root, now=NOW)
    db.close()
    assert first == second
    assert count(second / "ledger.sqlite") == 3
    assert json.loads((second / "live_state.json").read_text()) == {"baseline_cash": "2"}
    assert [p.name for p in dest_root.iterdir()] == ["20261008"]  # no .tmp-/.old- left


def test_pair_retries_until_live_state_is_stable(tmp_path, monkeypatch):
    src, db = make_live(tmp_path)
    db.close()
    real_connect = sqlite3.connect
    calls = {"n": 0}

    def connect(path, *args, **kwargs):
        # Simulate the bot rewriting live_state.json during the first ledger snapshot.
        if "mode=ro" in str(path):
            calls["n"] += 1
            if calls["n"] == 1:
                (src / "live_state.json").write_text(json.dumps({"baseline_cash": "changed"}))
        return real_connect(path, *args, **kwargs)

    monkeypatch.setattr(bk.sqlite3, "connect", connect)
    dest = bk.backup(src, tmp_path / "backups", now=NOW, sleep=0)
    assert calls["n"] == 2
    assert json.loads((dest / "live_state.json").read_text()) == {"baseline_cash": "changed"}


def test_pair_gives_up_after_three_unstable_attempts(tmp_path, monkeypatch):
    src, db = make_live(tmp_path)
    db.close()
    real_connect = sqlite3.connect
    calls = {"n": 0}

    def connect(path, *args, **kwargs):
        if "mode=ro" in str(path):
            calls["n"] += 1
            (src / "live_state.json").write_text(json.dumps({"n": calls["n"]}))
        return real_connect(path, *args, **kwargs)

    monkeypatch.setattr(bk.sqlite3, "connect", connect)
    dest_root = tmp_path / "backups"
    (dest_root / "20261008").mkdir(parents=True)
    (dest_root / "20261008" / "marker").write_text("previous")
    with pytest.raises(bk.PairChanged):
        bk.backup(src, dest_root, now=NOW, sleep=0)
    assert calls["n"] == 3
    # The previous same-day backup is untouched and no work dir is left behind.
    assert sorted(p.name for p in dest_root.iterdir()) == ["20261008"]
    assert (dest_root / "20261008" / "marker").read_text() == "previous"


def test_prune_removes_stale_work_dirs_only(tmp_path):
    root = tmp_path / "backups"
    for name in (".tmp-old", ".old-old", ".tmp-fresh", "20261001"):
        (root / name).mkdir(parents=True)
    past = time.time() - 2 * bk.STALE_WORKDIR_SECONDS
    for name in (".tmp-old", ".old-old"):
        os.utime(root / name, (past, past))
    bk.prune(root, keep_days=30, now=NOW)
    assert sorted(p.name for p in root.iterdir()) == [".tmp-fresh", "20261001"]
