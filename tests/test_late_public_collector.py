"""Synthetic fixtures only; no settlement or performance labels."""
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from src.v3 import late_public_collector as collector


def event(day, markets=None):
    slug = collector.event_slug(day)
    markets = markets if markets is not None else [
        {'id': '12', 'active': True, 'closed': False, 'acceptingOrders': True},
        {'id': '2', 'active': True, 'closed': False, 'acceptingOrders': True},
        {'id': '3', 'active': True, 'closed': False, 'acceptingOrders': True}]
    for market in markets:
        market.setdefault('endDate', day.isoformat() + 'T12:00:00Z')
        market.setdefault('question', f'Will the highest temperature in New York City be between 58-59°F on {day.strftime("%B")} {day.day}?')
    return {'slug': slug, 'active': True, 'closed': False, 'markets': markets}


def test_deterministic_local_date_and_exact_bounded_selection():
    instant = datetime(2026, 10, 16, 2, tzinfo=timezone.utc)
    day = instant.astimezone(ZoneInfo('America/New_York')).date()
    assert collector.event_slug(day) == 'highest-temperature-in-nyc-on-october-15-2026'
    assert collector.select_ids(event(day), collector.event_slug(day)) == ['2', '3']
    with pytest.raises(ValueError):
        collector.select_ids(event(day) | {'slug': 'other'}, collector.event_slug(day))
    with pytest.raises(ValueError):
        collector.select_ids(event(day, [{'id': '2', 'active': True, 'closed': False,
                                          'acceptingOrders': True}] * 2), collector.event_slug(day))
    assert collector.select_ids(event(day, [{'id': '2', 'active': True, 'closed': False,
                                             'acceptingOrders': False}]), collector.event_slug(day)) == []
    wrong_day = event(day, [{'id': '2', 'active': True, 'closed': False,
                             'acceptingOrders': True, 'question': 'Will the highest temperature in New York City be between 58-59°F on October 14?'}])
    with pytest.raises(ValueError):
        collector.select_ids(wrong_day, collector.event_slug(day))


def test_cycle_rejections_and_atomic_status(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    calls = []
    class Session:
        headers = {}
        def close(self): pass
    monkeypatch.setattr(collector.requests, 'Session', Session)
    monkeypatch.setattr(collector, '_public_json', lambda *a, **k: (event(datetime.now(ZoneInfo('America/New_York')).date()), a[1]))
    def capture(slug, mid, output):
        calls.append(mid)
        output.write_text(json.dumps({'evidence': {'gamma': {'data': {}}, 'metar': {'data': []}, 'book': {'data': {}}},
                                      'input': {'market_id': mid},
                                      'result': {'status': 'rejected', 'paper_only': True,
                                                 'market_id': None, 'reason': 'missing official publication time'}}))
    monkeypatch.setattr(collector, 'run_probe', capture)
    status = collector.run_cycle(tmp_path)
    assert calls == ['2', '3']
    assert status['attempted'] == 2 and status['captured'] == 2 and status['rejected'] == 2
    assert status['errors'] == [] and status['provider_health']['gamma'] == 'ok'
    assert status['provider_health']['probe'] == 'rejected'
    assert status['completed_at'] and json.loads((tmp_path / 'status.json').read_text()) == status
    assert len(list(tmp_path.glob('capture-*.json'))) == 2


def test_discovery_failure_no_probe_and_no_leak(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    monkeypatch.setattr(collector, '_public_json', lambda *a, **k: (_ for _ in ()).throw(ValueError('secret')))
    monkeypatch.setattr(collector, 'run_probe', lambda *a: pytest.fail('probe should not run'))
    status = collector.run_cycle(tmp_path)
    assert status['attempted'] == status['captured'] == 0
    assert status['provider_health']['gamma'] == 'error'
    assert 'secret' not in json.dumps(status)


def test_lock_exclusive_retention_and_path(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    with collector.worker_lock(tmp_path):
        with pytest.raises(RuntimeError, match='already running'):
            with collector.worker_lock(tmp_path): pass
    for i in range(collector.MAX_CAPTURES + 3):
        (tmp_path / f'capture-{i:05d}.json').write_text('{}')
    collector.prune(tmp_path)
    assert len(list(tmp_path.glob('capture-*.json'))) == collector.MAX_CAPTURES
    monkeypatch.undo()
    with pytest.raises(ValueError):
        collector.validate_directory(tmp_path)

def test_eligible_capture_retained_and_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    monkeypatch.setattr(collector, '_public_json', lambda *a, **k: (event(datetime.now(ZoneInfo('America/New_York')).date()), a[1]))
    def synthetic_eligible(slug, mid, output):
        output.write_text(json.dumps({'evidence': {'gamma': {'data': {}}, 'metar': {'data': []}, 'book': {'data': {}}},
            'input': {'market_id': mid}, 'result': {
            'status': 'eligible', 'paper_only': True, 'market_id': mid}}))
    monkeypatch.setattr(collector, 'run_probe', synthetic_eligible)
    status = collector.run_cycle(tmp_path)
    assert status['captured'] == status['eligible'] == 2
    assert status['rejected'] == 0 and len(status['capture_files']) == 2
    assert status['request_budget'] == {'gamma': 1 + collector.MAX_MARKETS,
                                        'metar': collector.MAX_MARKETS, 'clob': collector.MAX_MARKETS}

def test_no_response_and_invalid_discovery_distinct(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    import requests
    monkeypatch.setattr(collector, '_public_json', lambda *a, **k: (_ for _ in ()).throw(requests.ConnectionError('secret')))
    status = collector.run_cycle(tmp_path)
    assert status['provider_health']['gamma'] == 'no_response'
    assert status['errors'] == ['Gamma discovery request failed without a response']
    assert 'secret' not in json.dumps(status)


def test_probe_no_response_is_retained_but_probe_error_is_distinct(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    monkeypatch.setattr(collector, '_public_json', lambda *a, **k: (event(datetime.now(ZoneInfo('America/New_York')).date(), [
        {'id': '2', 'active': True, 'closed': False, 'acceptingOrders': True}]), a[1]))
    def no_response(slug, mid, output):
        output.write_text(json.dumps({'provider_health': {'gamma': 'no_response'},
            'result': {'market_id': mid, 'status': 'rejected',
            'paper_only': True, 'reason': 'Gamma request failed without a response'}}))
    monkeypatch.setattr(collector, 'run_probe', no_response)
    status = collector.run_cycle(tmp_path)
    assert status['provider_health']['probe'] == 'request_failure'
    assert status['provider_health']['gamma_probe'] == 'no_response'
    assert status['captured'] == 0 and status['rejected'] == status['request_failure'] == 1
    assert len(status['capture_files']) == 0 and len(status['rejection_files']) == 1
    monkeypatch.setattr(collector, 'run_probe', lambda *a: (_ for _ in ()).throw(OSError('secret')))
    status = collector.run_cycle(tmp_path)
    assert status['provider_health']['probe'] == 'error' and status['captured'] == 0
    assert 'secret' not in json.dumps(status)


def test_running_status_is_interrupted_on_restart(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    completed = datetime.now(timezone.utc).isoformat()
    (tmp_path / 'status.json').write_text(json.dumps({'state': 'running', 'last_completed_at': completed}))
    monkeypatch.setattr(sys, 'argv', ['collector', '--once', '--state-dir', str(tmp_path)])
    monkeypatch.setattr(collector, '_public_json', lambda *a, **k: pytest.fail('network during restart skip'))
    collector.main()
    status = json.loads((tmp_path / 'status.json').read_text())
    assert status['state'] == 'skipped' and status['last_completed_at'] == completed
    assert status['previous_cycle_state'] == 'interrupted'


def test_interrupt_during_probe_preserves_previous_completion(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    completed = datetime.now(timezone.utc).isoformat()
    (tmp_path / 'status.json').write_text(json.dumps({'state': 'completed', 'completed_at': completed}))
    monkeypatch.setattr(collector, '_public_json', lambda *a, **k: (event(datetime.now(ZoneInfo('America/New_York')).date(), [
        {'id': '2', 'active': True, 'closed': False, 'acceptingOrders': True}]), a[1]))
    monkeypatch.setattr(collector, 'run_probe', lambda *a: (_ for _ in ()).throw(collector.Shutdown()))
    with pytest.raises(collector.Shutdown):
        collector.run_cycle(tmp_path)
    status = json.loads((tmp_path / 'status.json').read_text())
    assert status['state'] == 'interrupted' and status['completed_at'] is None
    assert status['last_completed_at'] == completed and status['attempted'] == 1


def test_collector_session_disables_environment(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    seen = []
    class Session:
        trust_env = True
        headers = {}
        def close(self): pass
    monkeypatch.setattr(collector.requests, 'Session', Session)
    def discovery(session, url, **kwargs):
        seen.append(session.trust_env)
        return event(datetime.now(ZoneInfo('America/New_York')).date(), []), url
    monkeypatch.setattr(collector, '_public_json', discovery)
    collector.run_cycle(tmp_path)
    assert seen == [False]


def test_probe_session_disables_environment_and_private_capture(tmp_path, monkeypatch):
    from src.v3 import late_public_probe as probe
    seen = []
    class Session:
        trust_env = True
        headers = {}
    monkeypatch.setattr(probe.requests, 'Session', Session)
    monkeypatch.setattr(probe, '_audit_path', lambda p: p)
    def failed_request(session, url, **kwargs):
        seen.append(session.trust_env)
        raise __import__('requests').ConnectionError('secret')
    monkeypatch.setattr(probe, '_public_json', failed_request)
    monkeypatch.setattr(sys, 'argv', ['probe', '--slug', 'test-event', '--market-id', '2',
                                     '--output', str(tmp_path / 'capture.json')])
    probe.main()
    assert seen == [False]
    assert (tmp_path / 'capture.json').stat().st_mode & 0o777 == 0o600

def test_restart_cadence_skips_without_network(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    monkeypatch.setattr(collector, '_public_json', lambda *a, **k: (event(datetime.now(ZoneInfo('America/New_York')).date(), []), a[1]))
    monkeypatch.setattr(sys, 'argv', ['collector', '--once', '--state-dir', str(tmp_path)])
    collector.main()
    before = json.loads((tmp_path / 'status.json').read_text())
    monkeypatch.setattr(collector, '_public_json', lambda *a, **k: pytest.fail('network during skip'))
    collector.main()
    after = json.loads((tmp_path / 'status.json').read_text())
    assert after['state'] == 'skipped' and after['last_completed_at'] == before['completed_at']
    assert after['request_budget']['gamma'] == 0
    assert before['state'] == 'completed'

def test_private_files_and_environment(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    monkeypatch.setattr(collector, '_public_json', lambda *a, **k: (event(datetime.now(ZoneInfo('America/New_York')).date(), []), a[1]))
    monkeypatch.setenv('POLYMARKET_PRIVATE_KEY', 'secret')
    monkeypatch.setenv('HTTPS_PROXY', 'http://proxy.invalid')
    collector.run_cycle(tmp_path)
    with collector.worker_lock(tmp_path):
        assert (tmp_path / 'worker.lock').stat().st_mode & 0o777 == 0o600
    assert tmp_path.stat().st_mode & 0o777 == 0o700
    assert (tmp_path / 'status.json').stat().st_mode & 0o777 == 0o600
    assert 'POLYMARKET_PRIVATE_KEY' not in collector.probe_environment()
    assert 'HTTPS_PROXY' not in collector.probe_environment()
    assert 'HOME' not in collector.probe_environment()
    assert 'PATH' in collector.probe_environment()

def test_symlink_status_and_lock_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    victim = tmp_path / 'victim'
    victim.write_text('untouched')
    (tmp_path / 'worker.lock').symlink_to(victim)
    with pytest.raises(ValueError):
        with collector.worker_lock(tmp_path): pass
    (tmp_path / 'worker.lock').unlink()
    (tmp_path / 'status.json').symlink_to(victim)
    with pytest.raises(ValueError):
        collector.run_cycle(tmp_path)
    assert victim.read_text() == 'untouched'

def test_sigterm_kills_probe_group_before_lock_release(tmp_path):
    # Exercise the real process-group cleanup with a deterministic sleeping child.
    script = '''import sys, subprocess\nfrom pathlib import Path\nfrom src.v3 import late_public_collector as c\nimport signal\nsignal.signal(signal.SIGTERM, c.shutdown_signal)\nreal = subprocess.Popen\ndef sleeping(*args, **kwargs):\n return real([sys.executable, '-c', 'import time; time.sleep(60)'], **kwargs)\nc.subprocess.Popen = sleeping\nwith c.worker_lock(Path(sys.argv[1])):\n c.run_probe("highest-temperature-in-nyc-on-october-15-2026", "2", Path(sys.argv[1]) / "capture-test.json")\n'''
    env = {**os.environ, 'PYTHONPATH': str(collector.Path(collector.__file__).resolve().parents[2])}
    proc = subprocess.Popen([sys.executable, '-c', script, str(tmp_path)], env=env)
    try:
        # No public network in this fixture; signal after the child exists.
        deadline = time.monotonic() + 10
        children = ''
        while time.monotonic() < deadline:
            children = subprocess.run(['pgrep', '-P', str(proc.pid)], capture_output=True, text=True).stdout.strip()
            if children:
                break
            time.sleep(.05)
        assert children
        child_pid = int(children.splitlines()[0])
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
        with pytest.raises(ProcessLookupError):
            os.kill(child_pid, 0)
        with collector.worker_lock(tmp_path): pass
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()

def test_discovery_interrupted_and_http_health(tmp_path, monkeypatch):
    from src.v3.late_public_probe import ResponseInterrupted, PublicHTTPError
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    for exc, health in [(ResponseInterrupted(), 'interrupted'), (PublicHTTPError(), 'http_error')]:
        monkeypatch.setattr(collector, '_public_json', lambda *a, **k: (_ for _ in ()).throw(exc))
        status = collector.run_cycle(tmp_path)
        assert status['provider_health']['gamma'] == health
        assert status['captured'] == 0

def test_prune_failure_finalizes_status(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    monkeypatch.setattr(collector, '_public_json', lambda *a, **k: (event(datetime.now(ZoneInfo('America/New_York')).date(), []), a[1]))
    monkeypatch.setattr(collector, 'prune', lambda *a: (_ for _ in ()).throw(OSError('secret')))
    with pytest.raises(OSError):
        collector.run_cycle(tmp_path)
    status = json.loads((tmp_path / 'status.json').read_text())
    assert status['state'] == 'error' and status['completed_at'] is None
    assert any('prune' in error for error in status['errors']) and 'secret' not in json.dumps(status)

def test_prune_failure_during_shutdown_preserves_interrupted(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, 'validate_directory', lambda p: p.resolve())
    monkeypatch.setattr(collector, '_public_json', lambda *a, **k: (_ for _ in ()).throw(collector.Shutdown()))
    monkeypatch.setattr(collector, 'prune', lambda *a: (_ for _ in ()).throw(OSError('secret')))
    with pytest.raises(collector.Shutdown):
        collector.run_cycle(tmp_path)
    status = json.loads((tmp_path / 'status.json').read_text())
    assert status['state'] == 'interrupted' and status['completed_at'] is None
    assert any('prune' in error for error in status['errors'])

def test_sigterm_at_popen_return_boundary_kills_child(tmp_path):
    script = '''import sys, subprocess, os, signal
from pathlib import Path
from src.v3 import late_public_collector as c
signal.signal(signal.SIGTERM, c.shutdown_signal)
real = subprocess.Popen
def interrupted(*args, **kwargs):
 child = real([sys.executable, '-c', 'import time; time.sleep(60)'], **kwargs)
 Path(sys.argv[1], 'child.pid').write_text(str(child.pid))
 os.kill(os.getpid(), signal.SIGTERM)
 return child
c.subprocess.Popen = interrupted
with c.worker_lock(Path(sys.argv[1])):
 c.run_probe('test-event', '2', Path(sys.argv[1]) / 'capture-test.json')
'''
    env = {**os.environ, 'PYTHONPATH': str(collector.Path(collector.__file__).resolve().parents[2])}
    proc = subprocess.run([sys.executable, '-c', script, str(tmp_path)], env=env, capture_output=True, timeout=10)
    child_pid = int((tmp_path / 'child.pid').read_text())
    assert proc.returncode != 0
    with pytest.raises(ProcessLookupError):
        os.kill(child_pid, 0)
    with collector.worker_lock(tmp_path): pass
