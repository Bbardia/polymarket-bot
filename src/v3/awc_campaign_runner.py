"""One-shot, credential-free AWC recurrence. No service, trading or retries.

Journal entries are immutable fsynced files. A START consumes a slot before any
public GET; a crash after START (including after tape publication) never retries.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import secrets
import stat
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from . import awc_campaign as tape

ZONE = ZoneInfo('Asia/Karachi')
MAX_ENTRIES = 2 * tape.MAX_CYCLES + 1
MAX_ENTRY_BYTES = 2048
MAX_REVISITS_PER_DAY = 3
WINDOW = timedelta(minutes=10)
CODES = {'timeout', 'invalid_response', 'storage_failure', 'capture_failed', 'internal_failure', 'collector_refused'}
REASONS = {'resolved_days', 'calendar_days', 'attempted_slots'}
RESULTS = {'captured', 'captured_partial'}


def slots(start: datetime):
    """45 Karachi dates, not a UTC duration. Times are scheduled instants."""
    first = start.astimezone(ZONE).date()
    for offset in range(tape.MAX_DAYS):
        day = first + timedelta(days=offset)
        for hour in (8, 10, 14, 20):
            stamp = datetime.combine(day, time(hour), ZONE).astimezone(timezone.utc)
            if stamp >= start:
                target = max(first, day - timedelta(days=1)) if hour == 10 else day
                yield stamp, target.isoformat()


def _body(record):
    return (json.dumps(record, sort_keys=True, separators=(',', ':'), ensure_ascii=True) + '\n').encode()


def _valid_target(start: datetime, slot: str, target: str, kind: str) -> bool:
    """Bound a journal target to its slot's Karachi campaign date."""
    try:
        instant = tape._aware(slot)
        day = date.fromisoformat(target)
        first = start.astimezone(ZONE).date()
        local = instant.astimezone(ZONE)
        if target != day.isoformat() or not first <= day < first + timedelta(days=tape.MAX_DAYS):
            return False
        if local.hour == 10:
            return day < local.date() or (kind == 'missed' and day == first == local.date())
        return day == local.date()
    except (ValueError, TypeError):
        return False


def replay(directory: Path, start_at: str):
    """Validate bounded canonical journal and START/terminal transitions."""
    state = {'attempted': 0, 'stop_reason': None, 'slots': {}, 'targets': {},
             'revisits': {}, 'next': 0, 'digest': None}
    start = tape._aware(start_at)
    valid_slots = {s.isoformat() for s, _ in slots(start)}
    with tape._directory_fd(directory) as fd:
        names = sorted(n for n in os.listdir(fd) if n.endswith('.json')) if fd is not None else []
        if len(names) > MAX_ENTRIES:
            raise ValueError('journal entry cap exceeded')
        for i, name in enumerate(names):
            if name != f'{i:06d}.json':
                raise ValueError('journal sequence mismatch')
            try:
                handle = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            except OSError as exc:
                raise ValueError('invalid journal entry') from exc
            with os.fdopen(handle, 'rb') as file:
                info = os.fstat(file.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_ENTRY_BYTES:
                    raise ValueError('invalid journal entry')
                body = file.read(MAX_ENTRY_BYTES + 1)
            if len(body) > MAX_ENTRY_BYTES:
                raise ValueError('journal entry too large')
            row = json.loads(body)
            if (not isinstance(row, dict) or body != _body(row) or row.get('sequence') != i or
                    row.get('previous_sha256') != state['digest'] or row.get('start_at') != start_at or
                    row.get('paper_only') is not True):
                raise ValueError('journal continuity or start drift')
            kind = row.get('kind')
            if state['stop_reason'] is not None:
                raise ValueError('entries after terminal stop')
            if kind == 'stop':
                if set(row) != {'sequence', 'previous_sha256', 'start_at', 'paper_only', 'kind', 'reason'} or row.get('reason') not in REASONS:
                    raise ValueError('invalid stop')
                state['stop_reason'] = row['reason']
            else:
                required = {'sequence', 'previous_sha256', 'start_at', 'paper_only', 'kind', 'slot', 'target_date', 'code'}
                if (set(row) not in (required, required | {'stage'}) or
                        ('stage' in row and (kind != 'finished' or row.get('code') != 'invalid_response' or
                                             not isinstance(row['stage'], str) or
                                             row['stage'] not in tape.SOURCE_STAGES))):
                    raise ValueError('invalid attempt entry')
                slot, target = row['slot'], row['target_date']
                if not isinstance(slot, str) or not isinstance(target, str):
                    raise ValueError('invalid slot')
                instant = tape._aware(slot)
                if (slot != instant.isoformat() or
                        slot not in valid_slots or not isinstance(kind, str) or
                        not _valid_target(start, slot, target, kind)):
                    raise ValueError('slot outside campaign')
                previous = state['slots'].get(slot)
                code = row['code']
                if kind in ('started', 'missed') and previous is None and code is None:
                    if kind == 'started' and instant.astimezone(ZONE).hour == 10:
                        count = state['revisits'].get(target, 0)
                        if count >= MAX_REVISITS_PER_DAY:
                            raise ValueError('revisit cap exceeded')
                        state['revisits'][target] = count + 1
                    state['slots'][slot] = kind
                    state['targets'][slot] = target
                    state['attempted'] += 1
                elif (kind == 'finished' and previous == 'started' and
                      target == state['targets'][slot] and code in RESULTS | CODES):
                    state['slots'][slot] = 'finished'
                else:
                    raise ValueError('duplicate or invalid slot transition')
            state['digest'] = hashlib.sha256(body).hexdigest()
            state['next'] = i + 1
    if state['attempted'] > tape.MAX_CYCLES:
        raise ValueError('attempt cap exceeded')
    return state


def append(directory: Path, start_at: str, kind: str, slot=None, target_date=None, code=None, *, stage=None, state=None):
    """Publish a complete fsynced entry atomically; caller owns singleton lock."""
    state = state if state is not None else replay(directory, start_at)
    if state['next'] >= MAX_ENTRIES or state['stop_reason']:
        raise ValueError('journal closed or full')
    if stage is not None and (kind != 'finished' or code != 'invalid_response' or
                              not isinstance(stage, str) or stage not in tape.SOURCE_STAGES):
        raise ValueError('invalid failure stage')
    row = {'sequence': state['next'], 'previous_sha256': state['digest'],
           'start_at': start_at, 'paper_only': True, 'kind': kind}
    if kind == 'stop':
        if code not in REASONS:
            raise ValueError('invalid stop reason')
        row['reason'] = code
    else:
        if kind not in ('started', 'missed', 'finished'):
            raise ValueError('invalid entry kind')
        if not isinstance(slot, datetime) or not isinstance(target_date, str):
            raise ValueError('invalid target date for slot')
        row.update(slot=slot.isoformat(), target_date=target_date, code=code)
        if stage is not None:
            row['stage'] = stage
    body = _body(row)
    if len(body) > MAX_ENTRY_BYTES:
        raise ValueError('journal entry too large')
    # Validate transition before writing; an unfinished START remains an attempt.
    if kind != 'stop':
        prior = state['slots'].get(row['slot'])
        if not isinstance(target_date, str) or not _valid_target(tape._aware(start_at), row['slot'], target_date, kind):
            raise ValueError('invalid target date for slot')
        if kind == 'started' and slot.astimezone(ZONE).hour == 10 and state['revisits'].get(target_date, 0) >= MAX_REVISITS_PER_DAY:
            raise ValueError('revisit cap exceeded')
        if ((kind in ('started', 'missed') and (prior is not None or code is not None or state['attempted'] >= tape.MAX_CYCLES)) or
                (kind == 'finished' and (prior != 'started' or state['targets'].get(row['slot']) != target_date or code not in RESULTS | CODES))):
            raise ValueError('invalid attempt transition')
    with tape._directory_fd(directory, create=True) as fd:
        name = f'{state["next"]:06d}.json'
        temp = f'.{name}.{secrets.token_hex(16)}.tmp'
        handle = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
        try:
            with os.fdopen(handle, 'wb') as file:
                file.write(body)
                file.flush()
                os.fsync(file.fileno())
            os.link(temp, name, src_dir_fd=fd, dst_dir_fd=fd, follow_symlinks=False)
            os.fsync(fd)
        finally:
            os.unlink(temp, dir_fd=fd)
    state['next'] += 1
    state['digest'] = hashlib.sha256(body).hexdigest()
    if kind == 'stop':
        state['stop_reason'] = code
    elif kind in ('started', 'missed'):
        state['slots'][row['slot']] = kind
        state['targets'][row['slot']] = target_date
        if kind == 'started' and slot.astimezone(ZONE).hour == 10:
            state['revisits'][target_date] = state['revisits'].get(target_date, 0) + 1
        state['attempted'] += 1
    else:
        state['slots'][row['slot']] = 'finished'
    return row


@contextmanager
def _lock(directory: Path):
    with tape._directory_fd(directory, create=True) as fd:
        handle = os.open('.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=fd)
        try:
            if not stat.S_ISREG(os.fstat(handle).st_mode):
                raise ValueError('invalid lock file')
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                yield False
            else:
                try:
                    yield True
                finally:
                    fcntl.flock(handle, fcntl.LOCK_UN)
        finally:
            os.close(handle)


def tape_state(directory: Path, start_at: str):
    """Read each bounded tape row once; retain only day keys, not raw bodies."""
    resolved, captured = set(), set()
    first = tape._aware(start_at).astimezone(ZONE).date()
    for row, _ in tape._iter_records(directory):
        try:
            day = date.fromisoformat(row['target_date'])
            captured_at = tape._aware(row['captured_at'])
            valid = (row.get('campaign_start_at') == start_at and row['target_date'] == day.isoformat()
                     and first <= day < first + timedelta(days=tape.MAX_DAYS)
                     and captured_at >= tape._aware(start_at)
                     and first <= captured_at.astimezone(ZONE).date() < first + timedelta(days=tape.MAX_DAYS)
                     and row.get('station') == 'OPKC' and row.get('zone') == 'Asia/Karachi')
        except (KeyError, TypeError, ValueError):
            valid = False
        if not valid:
            raise ValueError('tape campaign identity or local date drift')
        good = tape._corroborated_row(row)
        if good:
            resolved.add(row['target_date'])
        if row.get('status') in ('captured', 'captured_partial'):
            captured.add(row['target_date'])
    return len(resolved), captured - resolved


def _collect(day: str, start_at: str, directory: Path):
    return tape.DailyCampaign(directory, day, start_at, stage_failures=True).once()


def once(directory: Path, start_at: str, *, clock=None, collect=None):
    """Timer-safe one-shot invocation. No catch-up GET and no credentials."""
    if not start_at.endswith(('+00:00', 'Z')):
        raise ValueError('start_at must be a pinned UTC timestamp')
    start = tape._aware(start_at)
    get_time = clock or (lambda: datetime.now(timezone.utc))
    now = get_time().astimezone(timezone.utc)
    if now < start:
        raise ValueError('campaign has not started')
    root = tape.APPROVED_OUTPUT_ROOT / 'recurring'
    if Path(os.path.abspath(directory)) != Path(os.path.abspath(root)):
        raise ValueError('runner output must be approved recurring directory')
    with _lock(directory) as locked:
        if not locked:
            return {'status': 'locked', 'paper_only': True}
        journal, evidence = directory / 'journal', directory / 'tape'
        state = replay(journal, start_at)
        if state['stop_reason']:
            return {'status': 'stopped', 'reason': state['stop_reason'], 'paper_only': True}
        day = now.astimezone(ZONE).date()
        first = start.astimezone(ZONE).date()
        resolved, pending_days = tape_state(evidence, start_at)
        def stop(reason):
            append(journal, start_at, 'stop', code=reason, state=state)
            return {'status': 'stopped', 'reason': reason, 'paper_only': True}
        if resolved >= tape.MAX_RESOLVED_DAYS:
            return stop('resolved_days')
        if state['attempted'] >= tape.MAX_CYCLES:
            return stop('attempted_slots')
        for stamp, target in slots(start):
            # Replay and missed-slot writes can outlast the arrival window.
            # Recheck immediately before START rather than using invocation time.
            now = get_time().astimezone(timezone.utc)
            if stamp > now:
                break
            if stamp.isoformat() in state['slots']:
                continue
            if stamp.astimezone(ZONE).hour == 10:
                eligible = (d for d in pending_days if d < stamp.astimezone(ZONE).date().isoformat()
                            and state['revisits'].get(d, 0) < MAX_REVISITS_PER_DAY)
                target = min(eligible, key=lambda d: (state['revisits'].get(d, 0), d), default=None)
                if target is None:
                    continue  # no eligible captured pending prior date
            if state['attempted'] >= tape.MAX_CYCLES:
                return stop('attempted_slots')
            if (now >= stamp + WINDOW or
                    now.astimezone(ZONE).date() >= first + timedelta(days=tape.MAX_DAYS)):
                append(journal, start_at, 'missed', stamp, target, state=state)
                continue
            append(journal, start_at, 'started', stamp, target, state=state)
            stage = None
            try:
                result = (collect or _collect)(target, start_at, evidence)
                code = result['status'] if result.get('status') in RESULTS else 'collector_refused'
            except (TimeoutError, tape.CycleWallExceeded):
                code = 'timeout'
            except tape.SourceStageError as exc:
                code, stage = 'invalid_response', exc.stage
            except (ValueError, TypeError, KeyError, UnicodeError, json.JSONDecodeError):
                code = 'invalid_response'
            except OSError:
                code = 'storage_failure'
            except Exception:
                code = 'internal_failure'
            append(journal, start_at, 'finished', stamp, target, code, stage=stage, state=state)
            if state['attempted'] >= tape.MAX_CYCLES:
                stop('attempted_slots')
            elif code in RESULTS and tape_state(evidence, start_at)[0] >= tape.MAX_RESOLVED_DAYS:
                stop('resolved_days')
            return {'status': code if code in RESULTS else 'failed', 'failure_code': None if code in RESULTS else code,
                    'paper_only': True}
        if get_time().astimezone(ZONE).date() >= first + timedelta(days=tape.MAX_DAYS):
            return stop('calendar_days')
        if state['attempted'] >= tape.MAX_CYCLES:
            return stop('attempted_slots')
        return {'status': 'already_recorded' if any(s <= now < s + WINDOW and s.isoformat() in state['slots'] for s, _ in slots(start)) else 'outside_window',
                'paper_only': True}


def main():
    parser = argparse.ArgumentParser(description='Public-only AWC one-shot runner; no scheduler or trading')
    parser.add_argument('--start-at', required=True, help='fixed UTC timestamp, identical on every invocation')
    parser.add_argument('--directory', type=Path, default=tape.APPROVED_OUTPUT_ROOT / 'recurring')
    args = parser.parse_args()
    print(json.dumps(once(args.directory, args.start_at), sort_keys=True))


if __name__ == '__main__':
    main()
