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
    monkeypatch.setattr(runner, 'tape_state', lambda directory, start: (30, set()))
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
    monkeypatch.setattr(runner, 'tape_state', lambda directory, start: (0, {'2026-10-28'}))
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
    def state(directory, start):
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


def test_delayed_finality_revisits_older_pending_and_replays_after_restart(tmp_path, monkeypatch):
    pending = {'2026-10-28'}
    monkeypatch.setattr(runner, 'tape_state', lambda directory, start: (0, pending.copy()))
    calls = []
    for day in (1, 2, 3):
        invoke(tmp_path, at(day, 10), lambda target, *_: calls.append(target) or {'status': 'captured'})
    assert calls == ['2026-10-28'] * 3
    assert rows(tmp_path)['revisits']['2026-10-28'] == 3
    assert invoke(tmp_path, at(4, 10), lambda *_: pytest.fail('fourth revisit'))['status'] == 'outside_window'
    assert rows(tmp_path)['revisits']['2026-10-28'] == 3


def test_pending_selection_least_visits_then_oldest(tmp_path, monkeypatch):
    pending = {'2026-10-28'}
    monkeypatch.setattr(runner, 'tape_state', lambda directory, start: (0, pending.copy()))
    calls = []
    invoke(tmp_path, at(1, 10), lambda target, *_: calls.append(target) or {'status': 'captured'})
    pending.update({'2026-10-29', '2026-10-30'})
    for day in (2, 3, 4):
        invoke(tmp_path, at(day, 10), lambda target, *_: calls.append(target) or {'status': 'captured'})
    assert calls == ['2026-10-28', '2026-10-29', '2026-10-30', '2026-10-28']


def test_replay_rejects_invalid_prior_target_and_excess_revisits(tmp_path):
    journal = tmp_path / 'recurring' / 'journal'
    with pytest.raises(ValueError, match='target'):
        runner.append(journal, START.isoformat(), 'started', at(1, 10), '2026-10-30')
    for day in (1, 2, 3):
        runner.append(journal, START.isoformat(), 'started', at(day, 10), '2026-10-28')
        runner.append(journal, START.isoformat(), 'finished', at(day, 10), '2026-10-28', 'captured')
    with pytest.raises(ValueError, match='revisit'):
        runner.append(journal, START.isoformat(), 'started', at(4, 10), '2026-10-28')


@pytest.mark.parametrize('bad_date', ['2026-10-27', '2026-12-12', 'not-a-date'])
def test_tape_identity_and_local_date_rejected_before_get(tmp_path, monkeypatch, bad_date):
    monkeypatch.setattr(tape, '_iter_records', lambda directory: iter([({'campaign_start_at': START.isoformat(),
        'target_date': bad_date, 'captured_at': at(0, 8).isoformat(),
        'status': 'captured', 'station': 'OPKC', 'zone': 'Asia/Karachi'}, 'digest')]))
    with pytest.raises(ValueError, match='tape'):
        invoke(tmp_path, at(0, 8), lambda *_: pytest.fail('GET'))


def test_foreign_start_tape_rejected_before_get(tmp_path, monkeypatch):
    monkeypatch.setattr(tape, '_iter_records', lambda directory: iter([({'campaign_start_at':
        (START + timedelta(minutes=1)).isoformat(), 'target_date': '2026-10-28',
        'captured_at': at(0, 8).isoformat(),
        'status': 'captured', 'station': 'OPKC', 'zone': 'Asia/Karachi'}, 'digest')]))
    with pytest.raises(ValueError, match='tape'):
        invoke(tmp_path, at(0, 8), lambda *_: pytest.fail('GET'))


def test_stale_capture_timestamp_rejected_before_get(tmp_path, monkeypatch):
    monkeypatch.setattr(tape, '_iter_records', lambda directory: iter([({
        'campaign_start_at': START.isoformat(), 'target_date': '2026-10-28',
        'captured_at': (START - timedelta(seconds=1)).isoformat(),
        'status': 'captured', 'station': 'OPKC', 'zone': 'Asia/Karachi'}, 'digest')]))
    with pytest.raises(ValueError, match='tape'):
        invoke(tmp_path, at(0, 8), lambda *_: pytest.fail('GET'))


@pytest.mark.parametrize('exc,code', [(ValueError('bad response'), 'invalid_response'),
                                      (OSError('storage secret'), 'storage_failure'),
                                      (TimeoutError('late secret'), 'timeout'),
                                      (tape.CycleWallExceeded('cycle secret'), 'timeout')])
def test_fixed_collector_failure_classes(tmp_path, exc, code):
    def fail(*_):
        raise exc
    assert invoke(tmp_path, at(0, 8), fail)['failure_code'] == code
    assert 'secret' not in ''.join(p.read_text() for p in (tmp_path / 'recurring' / 'journal').glob('*.json'))


def test_typed_source_failure_only_journals_allowlisted_stage(tmp_path):
    def fail(*_):
        raise tape.SourceStageError('forecast') from ValueError('secret=private-url')
    assert invoke(tmp_path, at(0, 8), fail)['failure_code'] == 'invalid_response'
    journal = tmp_path / 'recurring' / 'journal'
    finished = json.loads((journal / '000001.json').read_text())
    assert finished['stage'] == 'forecast'
    assert 'secret=' not in (journal / '000001.json').read_text()
    assert rows(tmp_path)['attempted'] == 1
    assert not list((tmp_path / 'recurring' / 'tape').glob('*.json'))


def test_runner_real_daily_discovery_failure_attributed_without_tape(tmp_path, monkeypatch):
    calls = []
    def fetch(url, limit):
        calls.append(url)
        raise ValueError('secret=private-url')
    monkeypatch.setattr(runner, '_collect', lambda day, start, directory:
                        tape.DailyCampaign(directory, day, start, fetch=fetch,
                                           clock=lambda: at(0, 8), stage_failures=True).once())
    result = runner.once(tmp_path / 'recurring', START.isoformat(), clock=lambda: at(0, 8))
    assert result['failure_code'] == 'invalid_response'
    finished = json.loads((tmp_path / 'recurring' / 'journal' / '000001.json').read_text())
    assert finished['stage'] == 'discovery'
    assert len(calls) == 1 and 'secret=' not in json.dumps(finished)
    assert not list((tmp_path / 'recurring' / 'tape').glob('*.json'))


def test_legacy_two_row_replay_and_stage_schema_rejects_spoofing(tmp_path):
    journal = tmp_path / 'recurring' / 'journal'
    runner.append(journal, START.isoformat(), 'started', at(0, 8), '2026-10-28')
    runner.append(journal, START.isoformat(), 'finished', at(0, 8), '2026-10-28', 'invalid_response')
    legacy = [(journal / f'{n:06d}.json').read_bytes() for n in range(2)]
    assert rows(tmp_path)['slots'][at(0, 8).isoformat()] == 'finished'
    assert [(journal / f'{n:06d}.json').read_bytes() for n in range(2)] == legacy
    with pytest.raises(ValueError, match='stage'):
        runner.append(journal, START.isoformat(), 'started', at(0, 14), '2026-10-28', stage='gamma')
    runner.append(journal, START.isoformat(), 'started', at(0, 14), '2026-10-28')
    for code, stage in [('invalid_response', 'private-url'), ('captured', 'gamma'),
                        ('invalid_response', ['gamma'])]:
        with pytest.raises(ValueError, match='stage'):
            runner.append(journal, START.isoformat(), 'finished', at(0, 14), '2026-10-28',
                          code, stage=stage)
    row = runner.append(journal, START.isoformat(), 'finished', at(0, 14), '2026-10-28',
                        'invalid_response', stage='gamma')
    path = journal / '000003.json'
    for forged in ({**row, 'stage': 'private-url'}, {**row, 'code': 'captured'}):
        path.write_bytes(runner._body(forged))
        with pytest.raises(ValueError):
            rows(tmp_path)
