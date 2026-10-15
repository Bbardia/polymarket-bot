"""Public-only, paper-only periodic evidence capture. No account or trading interfaces.

Discovery uses exactly one bounded Gamma event-by-slug GET per cycle. At most two
exact market IDs are passed to the existing fail-closed one-shot probe. Evidence
is NOT a resolver observation, settlement label, trading signal, or P&L.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import signal
import stat
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

from .late_observation import _audit_path, late_contract
from .late_public_probe import GAMMA_LIMIT, _public_json, ResponseInterrupted, PublicHTTPError

INTERVAL_SECONDS = 1800
MAX_MARKETS = 2
MAX_CAPTURES = 96
MAX_STORAGE_BYTES = 128 * 1024 * 1024
PROBE_TIMEOUT = 75
NY = ZoneInfo('America/New_York')


def event_slug(day: date) -> str:
    return f'highest-temperature-in-nyc-on-{day.strftime("%B").lower()}-{day.day}-{day.year}'


def select_ids(event: object, slug: str) -> list[str]:
    if (not isinstance(event, dict) or event.get('slug') != slug
            or event.get('active') is not True or event.get('closed') is not False
            or not isinstance(event.get('markets'), list)):
        raise ValueError('event identity or availability invalid')
    # Slug identity alone is insufficient: verify each selected market targets
    # the same local calendar day. An invalid open market invalidates discovery.
    match = re.fullmatch(r'highest-temperature-in-nyc-on-([a-z]+)-(\d{1,2})-(\d{4})', slug)
    if match is None:
        raise ValueError('unexpected event slug')
    try:
        day = datetime.strptime(
            f'{match[1]}-{match[2]}-{match[3]}', '%B-%d-%Y').date()
    except ValueError as exc:
        raise ValueError('invalid event date') from exc
    ids = []
    for market in event['markets']:
        if not isinstance(market, dict):
            raise ValueError('malformed event market')
        if (market.get('active') is True and market.get('closed') is False
                and market.get('acceptingOrders') is True):
            mid = market.get('id')
            try:
                end = datetime.fromisoformat(market['endDate'].replace('Z', '+00:00'))
                contract = late_contract(market['question'], end_date=end)
                if end.utcoffset() is None or contract is None or contract.city != 'new york' or contract.target_date != day.isoformat():
                    raise ValueError('wrong question or event day')
            except (KeyError, TypeError, AttributeError) as exc:
                raise ValueError('malformed open market date') from exc
            if not isinstance(mid, str) or not mid.isascii() or not re.fullmatch(r'[0-9]{1,32}', mid):
                raise ValueError('invalid exact market ID')
            ids.append(mid)
    if len(ids) != len(set(ids)):
        raise ValueError('duplicate exact market ID')
    return sorted(ids, key=lambda mid: (int(mid), mid))[:MAX_MARKETS]


def validate_directory(path: Path) -> Path:
    # Reject symlinks even if they happen to resolve inside the isolated tree.
    path = path.absolute()
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError('state directory must not contain symlinks')
    resolved = _audit_path(path)
    data_root = Path(__file__).resolve().parents[2] / 'data'
    if not resolved.is_relative_to(data_root) or resolved == data_root:
        raise ValueError('state directory must be under ignored worktree data/')
    return resolved


def prune(directory: Path) -> None:
    """Bound both count and total capture bytes; never follow capture symlinks."""
    files = sorted((p for p in directory.glob('capture-*.json') if p.is_file() or p.is_symlink()),
                   key=lambda p: p.name)
    total = sum(p.lstat().st_size for p in files)
    while files and (len(files) > MAX_CAPTURES or total > MAX_STORAGE_BYTES):
        oldest = files.pop(0)
        total -= oldest.lstat().st_size
        oldest.unlink()


def private_directory(directory: Path) -> None:
    if directory.is_symlink():
        raise ValueError('state directory symlink refused')
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not directory.is_dir():
        raise ValueError('state directory not a directory')
    directory.chmod(0o700)
    for path in directory.iterdir():
        if path.is_symlink():
            raise ValueError('state file symlink refused')
        if not path.is_file() or path.stat().st_nlink != 1:
            raise ValueError('invalid state file')
        path.chmod(0o600)


def probe_environment() -> dict[str, str]:
    # No HOME/.netrc, proxies, credentials or Python startup hooks in the child.
    return {'PATH': os.defpath, 'PYTHONNOUSERSITE': '1'}


class Shutdown(BaseException):
    pass


def shutdown_signal(signum, frame):
    raise Shutdown()


@contextmanager
def worker_lock(directory: Path):
    private_directory(directory)
    lock_path = directory / 'worker.lock'
    if lock_path.is_symlink():
        raise ValueError('lock symlink refused')
    # File remains for cross-process flock; no unlink race at shutdown.
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'r+') as lock:
        info = os.fstat(lock.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError('invalid lock file')
        os.fchmod(lock.fileno(), 0o600)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError('collector already running') from exc
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def run_probe(slug: str, market_id: str, output: Path) -> None:
    blocked = {signal.SIGINT, signal.SIGTERM}
    previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, blocked)
    child = None
    try:
        child = subprocess.Popen([sys.executable, '-m', 'src.v3.late_public_probe', '--slug', slug,
                                  '--market-id', market_id, '--output', str(output)],
                                 cwd=Path(__file__).resolve().parents[2], env=probe_environment(),
                                 start_new_session=True, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL,
                                 preexec_fn=lambda: signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask))
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        code = child.wait(timeout=PROBE_TIMEOUT)
        if code:
            raise subprocess.CalledProcessError(code, child.args)
    finally:
        signal.pthread_sigmask(signal.SIG_BLOCK, blocked)
        try:
            if child is not None and child.poll() is None:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)

def _status(directory: Path, status: dict) -> dict:
    target = directory / 'status.json'
    if target.is_symlink():
        raise ValueError('status symlink refused')
    temporary = directory / ('.status-' + uuid.uuid4().hex + '.tmp')
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(status, stream, sort_keys=True, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    return status


def previous_status(directory: Path) -> dict:
    target = directory / 'status.json'
    if target.is_symlink():
        raise ValueError('status symlink refused')
    if not target.exists():
        return {}
    info = target.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 100_000:
        raise ValueError('invalid status file')
    target.chmod(0o600)
    try:
        value = json.loads(target.read_text())
    except (ValueError, UnicodeError) as exc:
        raise ValueError('invalid status JSON') from exc
    if not isinstance(value, dict):
        raise ValueError('invalid status JSON')
    return value


def due_at(status: dict) -> datetime | None:
    value = status.get('last_completed_at') or status.get('completed_at')
    if not isinstance(value, str):
        return None
    try:
        completed = datetime.fromisoformat(value)
        if completed.utcoffset() is None:
            return None
        return completed + timedelta(seconds=INTERVAL_SECONDS)
    except ValueError:
        return None


def run_cycle(directory: Path) -> dict:
    """One discovery request, at most two probes (three GETs each); errors fail closed."""
    directory = validate_directory(directory)
    private_directory(directory)
    old = previous_status(directory)
    started = datetime.now(timezone.utc)
    day = started.astimezone(NY).date()
    slug = event_slug(day)
    status: dict[str, Any] = dict(paper_only=True, event_date=day.isoformat(), event_slug=slug,
                  started_at=started.isoformat(), completed_at=None,
                  last_completed_at=old.get('last_completed_at') or old.get('completed_at'),
                  state='running', pid=os.getpid(), heartbeat_at=started.isoformat(),
                  attempted=0, captured=0, eligible=0, rejected=0, request_failure=0, errors=[],
                  provider_health={'gamma': 'not_requested', 'metar': 'not_verified',
                                   'clob': 'not_verified', 'probe': 'not_requested'},
                  request_budget={'gamma': 1 + MAX_MARKETS, 'metar': MAX_MARKETS, 'clob': MAX_MARKETS},
                  capture_files=[], rejection_files=[])
    _status(directory, status)
    session = requests.Session()
    session.trust_env = False
    session.headers['User-Agent'] = 'paper-late-observation-research/1.0'
    try:
        url = 'https://gamma-api.polymarket.com/events/slug/' + slug
        event, response_url = _public_json(session, url, limit=GAMMA_LIMIT)
        if response_url != url:
            raise ValueError('discovery URL mismatch')
        ids = select_ids(event, slug)
        status['provider_health']['gamma'] = 'ok'
        for mid in ids:
            # Lexically sortable UTC prefix, random suffix; never overwrite evidence.
            name = f'capture-{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")}-{mid}-{uuid.uuid4().hex[:8]}.json'
            output = directory / name
            status['attempted'] += 1
            status['heartbeat_at'] = datetime.now(timezone.utc).isoformat()
            _status(directory, status)
            try:
                run_probe(slug, mid, output)
                if (output.is_symlink() or not output.is_file() or output.stat().st_nlink != 1
                        or output.stat().st_size > 2_000_000):
                    raise ValueError('invalid capture file')
                output.chmod(0o600)
                capture = json.loads(output.read_text())
                result = capture.get('result') if isinstance(capture, dict) else None
                # Evaluator deliberately redacts IDs from rejected results; the
                # probe's bound input retains the exact selected market ID.
                bound_input = capture.get('input') if isinstance(capture, dict) else None
                bound_id = (bound_input.get('market_id') if isinstance(bound_input, dict)
                            else result.get('market_id') if isinstance(result, dict) else None)
                if (not isinstance(result, dict) or result.get('paper_only') is not True
                        or result.get('status') not in ('rejected', 'eligible') or str(bound_id) != mid):
                    raise ValueError('probe did not produce a matching paper-only result')
                status[result['status']] += 1
                evidence = capture.get('evidence')
                health = capture.get('provider_health')
                if isinstance(health, dict):
                    for provider, value in health.items():
                        if provider in ('gamma', 'metar', 'clob') and value in (
                                'ok', 'no_response', 'interrupted', 'http_error', 'malformed'):
                            status['provider_health']['gamma_probe' if provider == 'gamma' else provider] = value
                if isinstance(evidence, dict):
                    if not isinstance(health, dict) and isinstance(evidence.get('metar'), dict):
                        status['provider_health']['metar'] = 'ok'
                    if not isinstance(health, dict) and isinstance(evidence.get('book'), dict):
                        status['provider_health']['clob'] = 'ok'
                verified = (isinstance(bound_input, dict) and isinstance(evidence, dict)
                            and all(isinstance(evidence.get(key), dict) and 'data' in evidence[key]
                                    for key in ('gamma', 'metar', 'book')))
                failure = (not verified and ((isinstance(health, dict) and any(
                    health.get(key) in ('no_response', 'interrupted', 'http_error')
                    for key in ('gamma', 'metar', 'clob')))
                    or result.get('reason') in ('Gamma request failed without a response',
                                                 'public ancillary request failed without a response')))
                if verified:
                    status['captured'] += 1
                    status['capture_files'].append(name)
                else:
                    status['rejection_files'].append(name)
                if failure:
                    status['request_failure'] += 1
                status['provider_health']['probe'] = 'request_failure' if failure else result['status']
            except (subprocess.SubprocessError, OSError, ValueError, TypeError, UnicodeError, json.JSONDecodeError):
                status['errors'].append('probe failed or produced invalid evidence for market ' + mid)
                status['provider_health']['probe'] = 'error'
                output.unlink(missing_ok=True)
    except ResponseInterrupted:
        status['provider_health']['gamma'] = 'interrupted'
        status['errors'].append('Gamma discovery response interrupted')
    except PublicHTTPError:
        status['provider_health']['gamma'] = 'http_error'
        status['errors'].append('Gamma discovery HTTP status error')
    except requests.RequestException:
        status['provider_health']['gamma'] = 'no_response'
        status['errors'].append('Gamma discovery request failed without a response')
    except (ValueError, TypeError, UnicodeError):
        status['provider_health']['gamma'] = 'error'
        status['errors'].append('Gamma discovery failed or event invalid')
    finally:
        session.close()
        interrupted = sys.exc_info()[0] is not None
        prune_error = None
        try:
            prune(directory)
        except OSError as exc:
            prune_error = exc
            status['errors'].append('capture prune failed')
        for field in ('capture_files', 'rejection_files'):
            status[field] = [name for name in status[field]
                             if (directory / name).is_file() and not (directory / name).is_symlink()]
        status['state'] = 'interrupted' if interrupted else 'error' if prune_error else 'completed'
        status['heartbeat_at'] = datetime.now(timezone.utc).isoformat()
        if status['state'] == 'completed':
            status['completed_at'] = status['heartbeat_at']
            status['last_completed_at'] = status['completed_at']
        _status(directory, status)
        if prune_error is not None and not interrupted:
            raise prune_error
    return status


def main() -> None:
    parser = argparse.ArgumentParser(description='public-only paper observation collector; no trading or settlement labels')
    parser.add_argument('--state-dir', type=Path, default=Path('data/late-observation-worker'))
    parser.add_argument('--once', action='store_true', help='one bounded cycle then exit')
    args = parser.parse_args()
    directory = validate_directory(args.state_dir)
    try:
        with worker_lock(directory):
            previous_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
            for sig in previous_handlers:
                signal.signal(sig, shutdown_signal)
            try:
                while True:
                    old = previous_status(directory)
                    due = due_at(old)
                    now = datetime.now(timezone.utc)
                    if old.get('state') == 'running':
                        old['state'] = 'interrupted'
                        old['heartbeat_at'] = now.isoformat()
                        _status(directory, old)
                    if due is not None and now < due:
                        status = dict(state='skipped', paper_only=True, pid=os.getpid(),
                                      heartbeat_at=now.isoformat(), last_completed_at=(old.get('last_completed_at') or old.get('completed_at')),
                                      previous_cycle_state=old.get('previous_cycle_state', old.get('state')),
                                      capture_files=old.get('capture_files', []),
                                      event_date=old.get('event_date'), event_slug=old.get('event_slug'),
                                      next_due_at=due.isoformat(), request_budget={'gamma': 0, 'metar': 0, 'clob': 0},
                                      attempted=0, captured=0, eligible=0, rejected=0,
                                      request_failure=0, errors=[], rejection_files=old.get('rejection_files', []))
                        _status(directory, status)
                    else:
                        status = run_cycle(directory)
                    print(json.dumps(status, sort_keys=True), flush=True)
                    if args.once:
                        break
                    next_due = due_at(status)
                    time.sleep(max(1, (next_due - datetime.now(timezone.utc)).total_seconds()) if next_due else INTERVAL_SECONDS)
            finally:
                for sig, handler in previous_handlers.items():
                    signal.signal(sig, handler)
    except (KeyboardInterrupt, Shutdown):
        pass
    except (RuntimeError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == '__main__':
    main()
