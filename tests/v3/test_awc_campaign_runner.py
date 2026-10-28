"""Offline recurrence and durable journal failure-window contracts."""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import json
import os

import pytest

from src.v3 import awc_campaign as tape
from src.v3 import awc_campaign_runner as runner

K = ZoneInfo('Asia/Karachi')
START = datetime(2026, 10, 28, 0, tzinfo=K).astimezone(timezone.utc)


@pytest.fixture(autouse=True)
def root(tmp_path, monkeypatch):
    monkeypatch.setattr(tape, 'APPROVED_OUTPUT_ROOT', tmp_path)


def at(day, hour, minute=0):
    return (START + timedelta(days=day, hours=hour, minutes=minute)).astimezone(timezone.utc)


def invoke(tmp_path, now, collect=None, start=START):
    return runner.once(tmp_path / 'recurring', start.isoformat(), clock=lambda: now,
                       collect=collect or (lambda day, start, directory: {'status': 'captured'}))


def rows(tmp_path):
    return runner.replay(tmp_path / 'recurring' / 'journal', START.isoformat())


def test_start_precedes_get_and_crashed_start_is_not_retried(tmp_path):
    calls = []
    def crash(day, start, directory):
        calls.append(rows(tmp_path)['attempted'])
        raise KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        invoke(tmp_path, at(0, 8), crash)
    assert calls == [1]
    assert invoke(tmp_path, at(0, 8, 1), crash)['status'] == 'already_recorded'
    assert calls == [1] and rows(tmp_path)['attempted'] == 1


def test_failure_fixed_code_and_no_retry(tmp_path):
    def fail(*args):
        raise ValueError('secret=do-not-journal')
    assert invoke(tmp_path, at(0, 8), fail)['status'] == 'failed'
    assert invoke(tmp_path, at(0, 8, 1), fail)['status'] == 'already_recorded'
    assert rows(tmp_path)['attempted'] == 1
    assert 'secret=' not in ''.join(p.read_text() for p in (tmp_path / 'recurring' / 'journal').glob('*.json'))


def test_outage_records_missed_no_get_and_skips_empty_revisit(tmp_path):
    calls = []
    invoke(tmp_path, at(0, 8), lambda *a: calls.append(1) or {'status': 'captured'})
    result = invoke(tmp_path, at(1, 14), lambda *a: calls.append(1) or {'status': 'captured'})
    assert result['status'] == 'captured'
    state = rows(tmp_path)
    assert state['attempted'] == 5  # day 0 14/20; day 1 08/10 missed, plus two starts
    assert len(calls) == 2
    assert invoke(tmp_path, at(1, 14, 30), lambda *a: calls.append(1))['status'] == 'outside_window'


def test_forty_fifth_day_and_no_forty_sixth_day_get(tmp_path):
    assert invoke(tmp_path, at(44, 20))['status'] == 'captured'
    calls = []
    assert invoke(tmp_path, at(45, 8), lambda *a: calls.append(1))['status'] == 'stopped'
    assert rows(tmp_path)['stop_reason'] == 'calendar_days'
    assert not calls


def test_slot_cap_and_terminal_stop_no_get(tmp_path):
    # Preseed 179 distinct missed slot records, then the last allowed attempt.
    journal = tmp_path / 'recurring' / 'journal'
    for slot in runner.slots(START):
        if slot[0] >= at(44, 20):
            break
        runner.append(journal, START.isoformat(), 'missed', slot[0], slot[1])
    assert rows(tmp_path)['attempted'] == 179
    calls = []
    assert invoke(tmp_path, at(44, 20), lambda *a: calls.append(1) or {'status': 'captured'})['status'] == 'captured'
    assert rows(tmp_path)['attempted'] == 180
    assert rows(tmp_path)['stop_reason'] == 'attempted_slots'
    assert invoke(tmp_path, at(44, 20), lambda *a: calls.append(1))['status'] == 'stopped'
    assert calls == [1]


def test_resolved_cap_no_network(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'tape_state', lambda directory: (30, set()))
    calls = []
    assert invoke(tmp_path, at(0, 8), lambda *a: calls.append(1))['status'] == 'stopped'
    assert rows(tmp_path)['stop_reason'] == 'resolved_days' and not calls


def test_stale_start_and_lock_and_symlink(tmp_path):
    assert invoke(tmp_path, at(0, 8))['status'] == 'captured'
    with pytest.raises(ValueError, match='start drift'):
        invoke(tmp_path, at(0, 14), start=START + timedelta(minutes=1))
    import fcntl
    lock = tmp_path / 'recurring' / '.lock'
    with open(lock, 'rb') as f:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert invoke(tmp_path, at(0, 14))['status'] == 'locked'
    journal = tmp_path / 'recurring' / 'journal'
    (journal / '000002.json').symlink_to('/etc/passwd')
    with pytest.raises(ValueError):
        invoke(tmp_path, at(0, 14))


def test_no_credentials_and_no_retroactive_fetch(tmp_path):
    calls = []
    assert invoke(tmp_path, at(0, 8, 15), lambda *a: calls.append(1))['status'] == 'outside_window'
    assert invoke(tmp_path, at(0, 14), lambda day, start, directory: calls.append((day, directory)) or {'status': 'captured'})['status'] == 'captured'
    assert calls == [('2026-10-28', tmp_path / 'recurring' / 'tape')]
    assert not any('credential' in p.name for p in (tmp_path / 'recurring').rglob('*'))


def test_revisit_only_after_captured_pending_prior_even_after_two_day_outage(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, 'tape_state', lambda directory: (0, {'2026-10-28'}))
    calls = []
    invoke(tmp_path, at(2, 14), lambda day, *_: calls.append(day) or {'status': 'captured'})
    state = rows(tmp_path)
    # Day 1 10:00 was a pending-prior-day revisit, missed during outage.
    assert at(1, 10).isoformat() in state['slots']
    assert state['slots'][at(1, 10).isoformat()] == 'missed'
    assert calls == ['2026-10-30']


def test_missing_gamma_and_rule_failures_consume_slot_without_tape(tmp_path, monkeypatch):
    calls = []
    def absent(self):
        calls.append(self.target_date)
        raise ValueError('Gamma missing/rule mismatch secret=never-journal')
    monkeypatch.setattr(tape.DailyCampaign, 'once', absent)
    directory = tmp_path / 'recurring'
    assert runner.once(directory, START.isoformat(), clock=lambda: at(0, 8))['failure_code'] == 'invalid_response'
    assert runner.once(directory, START.isoformat(), clock=lambda: at(0, 14))['failure_code'] == 'invalid_response'
    assert len(calls) == 2 and not list((directory / 'tape').glob('*.json'))
    assert 'secret=' not in ''.join(p.read_text() for p in (directory / 'journal').glob('*.json'))


def test_collector_last_local_day_not_utc_45_day_cutoff(tmp_path):
    late_start = at(0, 13)
    late = at(44, 20)
    calls = []
    def fetch(*args):
        calls.append(1)
        raise ValueError('absent Gamma')
    campaign = tape.DailyCampaign(tmp_path / 'recurring' / 'tape',
                                  (late.astimezone(K)).date().isoformat(),
                                  late_start.isoformat(), fetch=fetch, clock=lambda: late)
    with pytest.raises(ValueError, match='absent Gamma'):
        campaign.once()
    assert calls == [1]
    expired = tape.DailyCampaign(tmp_path / 'recurring' / 'tape',
                                 (late + timedelta(days=1)).astimezone(K).date().isoformat(),
                                 late_start.isoformat(), fetch=fetch, clock=lambda: late + timedelta(days=1))
    assert expired.once()['status'] == 'deadline' and calls == [1]


def test_stop_immediately_after_thirtieth_verified_capture(tmp_path, monkeypatch):
    visits = []
    def state(directory):
        visits.append(1)
        return (30 if len(visits) == 2 else 29, set())
    monkeypatch.setattr(runner, 'tape_state', state)
    calls = []
    assert invoke(tmp_path, at(0, 8), lambda *a: calls.append(1) or {'status': 'captured'})['status'] == 'captured'
    assert rows(tmp_path)['stop_reason'] == 'resolved_days'
    assert invoke(tmp_path, at(0, 14), lambda *a: calls.append(1))['status'] == 'stopped'
    assert calls == [1]


def test_expired_window_during_preflight_is_missed_without_get(tmp_path):
    ticks = iter((at(0, 8), at(0, 8, 11)))
    calls = []
    result = runner.once(tmp_path / 'recurring', START.isoformat(),
                         clock=lambda: next(ticks, at(0, 8, 11)),
                         collect=lambda *a: calls.append(1))
    assert result['status'] == 'outside_window'
    assert rows(tmp_path)['slots'][at(0, 8).isoformat()] == 'missed'
    assert not calls


def test_start_must_be_utc(tmp_path):
    with pytest.raises(ValueError, match='pinned UTC'):
        runner.once(tmp_path / 'recurring', at(0, 8).astimezone(K).isoformat(),
                    clock=lambda: at(0, 8), collect=lambda *a: pytest.fail('GET'))


@pytest.mark.parametrize('failure_call,expected_attempts', [(1, 0), (2, 1)])
def test_journal_fsync_fail_windows_never_get(tmp_path, monkeypatch, failure_call, expected_attempts):
    original = os.fsync
    calls = []
    def fail(fd):
        calls.append(fd)
        if len(calls) == failure_call:
            raise OSError('injected fsync failure')
        return original(fd)
    monkeypatch.setattr(runner.os, 'fsync', fail)
    try:
        with pytest.raises(OSError, match='injected'):
            invoke(tmp_path, at(0, 8), lambda *a: pytest.fail('GET before durable START'))
    finally:
        monkeypatch.setattr(runner.os, 'fsync', original)
    assert rows(tmp_path)['attempted'] == expected_attempts
    assert not list((tmp_path / 'recurring' / 'journal').glob('*.tmp'))
    if expected_attempts:
        assert invoke(tmp_path, at(0, 8, 1), lambda *a: pytest.fail('retry'))['status'] == 'already_recorded'
