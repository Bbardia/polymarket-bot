"""Offline tests for scripts/live_health_check.py with a fake status file and sender."""
import importlib.util
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "live_health_check.py"
spec = importlib.util.spec_from_file_location("live_health_check", SCRIPT)
hc = importlib.util.module_from_spec(spec)
sys.modules["live_health_check"] = hc
spec.loader.exec_module(hc)

NOW = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
STEP = timedelta(minutes=5)


class FakeSender:
    def __init__(self, fail=False):
        self.mails = []
        self.fail = fail

    def __call__(self, mail):
        if self.fail:
            raise RuntimeError("Resend HTTP 500: boom")
        self.mails.append(mail)


def write_status(data_dir, *, age=60, healthy=True, entry_block=None, lifecycle=False, now=NOW):
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "status.json").write_text(json.dumps({
        "cycle_finished_at": (now - timedelta(seconds=age)).isoformat(),
        "healthy": healthy,
        "entry_block_reason": entry_block,
        "reconciliation": {"lifecycle_reconciliation_required": lifecycle,
                           "lifecycle_reconciliation_reasons": ["x"] if lifecycle else []},
    }))


def write_failed_status(data_dir, *, now=NOW):
    """Shape live_runner writes when a cycle raises: no cycle_finished_at, no reconciliation."""
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "status.json").write_text(json.dumps({
        "mode": "LIVE", "healthy": False, "at": now.isoformat(), "last_error": "RateLimitError: x",
        "cycle": 7, "data_dir": str(data_dir),
    }))


def check(tmp_path, sender, *, now=NOW, unit="active", persist=True):
    return hc.run(data_dir=tmp_path / "live", state_file=tmp_path / "alerts-state.json", sender=sender,
                  now=now, max_age_seconds=600, unit_active_state=unit, persist=persist)


def test_healthy_status_sends_nothing(tmp_path):
    write_status(tmp_path / "live")
    sender = FakeSender()
    assert check(tmp_path, sender) is None
    assert sender.mails == []


@pytest.mark.parametrize("kwargs,unit,key", [
    ({"age": 601}, "active", "stale"),
    ({"lifecycle": True}, "active", "reconciliation"),
    ({}, "failed", "unit_inactive"),
    ({}, "inactive", "unit_inactive"),
])
def test_immediate_conditions_alert_on_first_check(tmp_path, kwargs, unit, key):
    write_status(tmp_path / "live", **kwargs)
    sender = FakeSender()
    mail = check(tmp_path, sender, unit=unit)
    assert sender.mails == [mail]
    assert "ALERT" in mail.subject and key in mail.subject


@pytest.mark.parametrize("kwargs,key", [
    ({"healthy": False}, "unhealthy"),
    ({"entry_block": "daily loss limit"}, "entry_blocked"),
])
def test_debounced_conditions_need_two_consecutive_checks(tmp_path, kwargs, key):
    sender = FakeSender()
    write_status(tmp_path / "live", **kwargs)
    assert check(tmp_path, sender) is None
    write_status(tmp_path / "live", now=NOW + STEP, **kwargs)
    mail = check(tmp_path, sender, now=NOW + STEP)
    assert "ALERT" in mail.subject and key in mail.subject


@pytest.mark.parametrize("kwargs", [{"healthy": False}, {"entry_block": "x"}])
def test_single_transient_debounced_problem_is_silent(tmp_path, kwargs):
    sender = FakeSender()
    write_status(tmp_path / "live", **kwargs)
    check(tmp_path, sender)
    write_status(tmp_path / "live", now=NOW + STEP)
    assert check(tmp_path, sender, now=NOW + STEP) is None
    assert sender.mails == []  # no alert and no "recovered" for something never mailed


def test_restarting_unit_is_not_an_alert(tmp_path):
    write_status(tmp_path)
    observation = hc.check_status(tmp_path / "status.json", NOW, 600, "activating")
    assert not [p for p in observation.problems if p.key == "unit_inactive"]
    assert "unit_inactive" not in observation.cleared


def test_stale_threshold_is_exclusive(tmp_path):
    write_status(tmp_path / "live", age=600)
    assert check(tmp_path, FakeSender()) is None


def test_missing_status_file_alerts(tmp_path):
    (tmp_path / "live").mkdir()
    mail = check(tmp_path, FakeSender())
    assert "status_unreadable" in mail.subject


def test_failed_cycle_is_not_stale_and_only_unhealthy_after_debounce(tmp_path):
    sender = FakeSender()
    write_status(tmp_path / "live", age=30)
    assert check(tmp_path, sender) is None  # remembers the last good cycle
    write_failed_status(tmp_path / "live", now=NOW + STEP)
    assert check(tmp_path, sender, now=NOW + STEP) is None
    write_status(tmp_path / "live", age=30, now=NOW + 2 * STEP)
    assert check(tmp_path, sender, now=NOW + 2 * STEP) is None
    assert sender.mails == []


def test_repeated_failed_cycles_go_stale_from_last_good_cycle(tmp_path):
    sender = FakeSender()
    write_status(tmp_path / "live", age=0)
    check(tmp_path, sender)
    for step in (1, 2):
        write_failed_status(tmp_path / "live", now=NOW + step * STEP)
        mail = check(tmp_path, sender, now=NOW + step * STEP)
    assert mail is not None and "unhealthy" in mail.subject and "stale" not in mail.subject
    write_failed_status(tmp_path / "live", now=NOW + 3 * STEP)
    mail = check(tmp_path, sender, now=NOW + 3 * STEP)
    assert "stale" in mail.subject and "last successful cycle" in mail.body


def test_stale_falls_back_to_status_mtime_without_known_cycle(tmp_path):
    write_failed_status(tmp_path / "live")
    old = (NOW - timedelta(hours=1)).timestamp()
    os.utime(tmp_path / "live" / "status.json", (old, old))
    mail = check(tmp_path, FakeSender())
    assert "stale" in mail.subject and "status.json last written" in mail.body


def test_incomplete_status_does_not_mark_recovered(tmp_path):
    sender = FakeSender()
    write_status(tmp_path / "live", lifecycle=True)
    assert "reconciliation" in check(tmp_path, sender).subject
    # Failed cycle: no reconciliation block, so the latch is not known to be clear.
    write_failed_status(tmp_path / "live", now=NOW + STEP)
    mail = check(tmp_path, sender, now=NOW + STEP)
    assert mail is None or "RECOVERED" not in mail.subject
    # Unreadable status: same.
    (tmp_path / "live" / "status.json").write_text("{not json")
    mail = check(tmp_path, sender, now=NOW + 2 * STEP)
    assert "status_unreadable" in mail.subject and "RECOVERED" not in mail.body
    # Explicitly clear: recovered.
    write_status(tmp_path / "live", now=NOW + 3 * STEP)
    mail = check(tmp_path, sender, now=NOW + 3 * STEP)
    assert "RECOVERED" in mail.subject and "reconciliation" in mail.body and "status_unreadable" in mail.body


def test_dedup_realert_after_6h_and_recovery(tmp_path):
    sender = FakeSender()
    write_status(tmp_path / "live", lifecycle=True)
    assert check(tmp_path, sender) is not None
    for minutes in (5, 60, 359):
        at = NOW + timedelta(minutes=minutes)
        write_status(tmp_path / "live", lifecycle=True, now=at)
        assert check(tmp_path, sender, now=at) is None
    at = NOW + timedelta(hours=6)
    write_status(tmp_path / "live", lifecycle=True, now=at)
    mail = check(tmp_path, sender, now=at)
    assert "STILL FAILING" in mail.body and "reconciliation" in mail.subject
    at = NOW + timedelta(hours=6, minutes=5)
    write_status(tmp_path / "live", lifecycle=True, now=at)
    assert check(tmp_path, sender, now=at) is None
    later = NOW + timedelta(hours=7)
    write_status(tmp_path / "live", now=later)
    mail = check(tmp_path, sender, now=later)
    assert "RECOVERED" in mail.subject and "reconciliation" in mail.body
    assert check(tmp_path, sender, now=later + timedelta(minutes=5)) is None
    assert len(sender.mails) == 3


def test_new_problem_alerts_while_other_persists(tmp_path):
    sender = FakeSender()
    write_status(tmp_path / "live", lifecycle=True)
    check(tmp_path, sender)
    write_status(tmp_path / "live", lifecycle=True, now=NOW + STEP)
    mail = check(tmp_path, sender, now=NOW + STEP, unit="failed")
    assert "unit_inactive" in mail.subject and "reconciliation" not in mail.subject
    assert "Also still active" in mail.body


def append(path, *lines):
    with path.open("a") as handle:
        handle.write("".join(line + "\n" for line in lines))


def test_errors_baseline_then_debounced_new_lines(tmp_path):
    write_status(tmp_path / "live")
    errors = tmp_path / "live" / "errors.jsonl"
    errors.write_text('{"at": "old"}\n')
    sender = FakeSender()
    assert check(tmp_path, sender) is None  # first run records the baseline
    append(errors, '{"at": "new1"}')
    write_status(tmp_path / "live", now=NOW + STEP)
    assert check(tmp_path, sender, now=NOW + STEP) is None  # one batch: debounced
    append(errors, '{"at": "new2"}')
    write_status(tmp_path / "live", now=NOW + 2 * STEP)
    mail = check(tmp_path, sender, now=NOW + 2 * STEP)
    assert "errors" in mail.subject
    assert "new1" in mail.body and "new2" in mail.body and "old" not in mail.body
    # No new lines: cleared silently (no recovered mail for events).
    write_status(tmp_path / "live", now=NOW + 3 * STEP)
    assert check(tmp_path, sender, now=NOW + 3 * STEP) is None
    assert len(sender.mails) == 1


def test_single_error_batch_is_silent(tmp_path):
    write_status(tmp_path / "live")
    errors = tmp_path / "live" / "errors.jsonl"
    errors.write_text("")
    sender = FakeSender()
    check(tmp_path, sender)
    append(errors, '{"at": "blip"}')
    for step in (1, 2):
        write_status(tmp_path / "live", now=NOW + step * STEP)
        check(tmp_path, sender, now=NOW + step * STEP)
    assert sender.mails == []


def test_errors_inode_change_reads_whole_new_file(tmp_path):
    errors = tmp_path / "errors.jsonl"
    errors.write_text("a\nb\nc\n")
    _, cursor = hc.read_new_error_lines(errors, {})
    replacement = tmp_path / "errors.new"
    replacement.write_text("x\n")
    os.replace(replacement, errors)
    assert errors.stat().st_ino != cursor["inode"]
    lines, new_cursor = hc.read_new_error_lines(errors, {"errors": cursor})
    assert lines == ["x"] and new_cursor["lines"] == 1


def test_errors_truncated_in_place_resets_cursor(tmp_path):
    errors = tmp_path / "errors.jsonl"
    errors.write_text("a\nb\n")
    _, cursor = hc.read_new_error_lines(errors, {})
    errors.write_text("c\n")  # same inode, shorter
    lines, new_cursor = hc.read_new_error_lines(errors, {"errors": cursor})
    assert lines == [] and new_cursor["lines"] == 1


def test_failed_send_does_not_advance_state(tmp_path):
    write_status(tmp_path / "live", lifecycle=True)
    with pytest.raises(RuntimeError):
        check(tmp_path, FakeSender(fail=True))
    assert not (tmp_path / "alerts-state.json").exists()
    assert check(tmp_path, FakeSender(), now=NOW + STEP) is not None


def test_dry_run_does_not_persist(tmp_path):
    write_status(tmp_path / "live", lifecycle=True)
    check(tmp_path, FakeSender(), persist=False)
    assert not (tmp_path / "alerts-state.json").exists()


def test_env_file_parsing(tmp_path):
    env = tmp_path / ".env.alerts"
    env.write_text('# comment\nRESEND_API_KEY="re_x"  # quoted then comment\n'
                   "export ALERT_FROM=Bot <bot@example.ch> # inline comment\n"
                   "ALERT_TO=\nHASH='a # b'\nFRAG=abc#def\n")
    assert hc.load_env_file(env) == {"RESEND_API_KEY": "re_x", "ALERT_FROM": "Bot <bot@example.ch>",
                                     "ALERT_TO": "", "HASH": "a # b", "FRAG": "abc#def"}


@pytest.mark.parametrize("content,expected", [
    ("OTHER_SECRET=zzz\nV3_LIVE_SCAN_INTERVAL_SECONDS=120 # fast\n", 120.0),
    ('V3_LIVE_SCAN_INTERVAL_SECONDS="450"\n', 450.0),
    ("OTHER=1\n", 300.0),
    ("V3_LIVE_SCAN_INTERVAL_SECONDS=-5\n", 300.0),
    ("V3_LIVE_SCAN_INTERVAL_SECONDS=abc\n", 300.0),
])
def test_scan_interval_from_live_env(tmp_path, capsys, content, expected):
    env = tmp_path / ".env.live"
    env.write_text(content)
    assert hc.scan_interval_from_live_env(env) == expected
    captured = capsys.readouterr()
    assert "zzz" not in captured.out + captured.err and "abc" not in captured.out + captured.err


def test_scan_interval_missing_live_env_falls_back(tmp_path):
    assert hc.scan_interval_from_live_env(tmp_path / "absent") == 300.0


def test_main_uses_live_env_interval(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(hc, "unit_state", lambda unit=hc.UNIT: "active")
    live_env = tmp_path / ".env.live"
    live_env.write_text("PRIVATE_KEY=never-print-me\nV3_LIVE_SCAN_INTERVAL_SECONDS=60\n")
    data = tmp_path / "live"
    data.mkdir()
    (data / "status.json").write_text(json.dumps({
        "cycle_finished_at": (datetime.now(timezone.utc) - timedelta(seconds=200)).isoformat(),
        "healthy": True, "entry_block_reason": None,
        "reconciliation": {"lifecycle_reconciliation_required": False}}))
    assert hc.main(["--dry-run", "--env-file", str(tmp_path / "missing"), "--live-env-file", str(live_env),
                    "--data-dir", str(data), "--state-file", str(tmp_path / "s.json")]) == 0
    out = capsys.readouterr().out
    assert "stale" in out and "threshold 120s" in out and "never-print-me" not in out


def test_main_refuses_without_credentials(tmp_path, capsys, monkeypatch):
    for name in ("RESEND_API_KEY", "ALERT_FROM", "ALERT_TO"):
        monkeypatch.delenv(name, raising=False)
    env = tmp_path / ".env.alerts"
    env.write_text("RESEND_API_KEY=\nALERT_FROM=\nALERT_TO=\n")
    assert hc.main(["--env-file", str(env), "--live-env-file", str(tmp_path / "absent"), "--data-dir",
                    str(tmp_path), "--state-file", str(tmp_path / "s.json")]) == 2
    assert "RESEND_API_KEY" in capsys.readouterr().err


def test_main_dry_run_prints(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(hc, "unit_state", lambda unit=hc.UNIT: "inactive")
    write_status(tmp_path / "live", now=datetime.now(timezone.utc))
    assert hc.main(["--dry-run", "--env-file", str(tmp_path / "missing"), "--live-env-file",
                    str(tmp_path / "absent"), "--data-dir", str(tmp_path / "live"),
                    "--state-file", str(tmp_path / "s.json")]) == 0
    out = capsys.readouterr().out
    assert "Subject: [polymarket-v7 ALERT]" in out and "unit_inactive" in out
    assert not (tmp_path / "s.json").exists()


def test_resend_request_shape(monkeypatch):
    captured = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            return b'{"id": "1"}'

    def fake_urlopen(request, timeout):
        captured["request"] = request
        return Response()

    monkeypatch.setattr(hc.urllib.request, "urlopen", fake_urlopen)
    hc.resend_sender("re_key", "Bot <bot@bardia-amiryavari.ch>", ["a@example.ch"])(hc.Mail("s", "b"))
    request = captured["request"]
    assert request.full_url == "https://api.resend.com/emails"
    assert request.get_header("Authorization") == "Bearer re_key"
    assert json.loads(request.data) == {"from": "Bot <bot@bardia-amiryavari.ch>", "to": ["a@example.ch"],
                                        "subject": "s", "text": "b"}
