"""Finite, public-only AWC evidence tape. No credentials, trades or scheduler.

Run one explicitly identified event per invocation. Raw HTTP response bodies are
immutable JSON-escaped bytes (UTF-8 only); the tape never retroactively claims a
forecast issue time or reconstructs a past snapshot from a later API read.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import signal
import stat
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from zoneinfo import ZoneInfo

from .awc_metar_observation import inspect_awc_source, parse_raw_observations

MAX_CYCLES = 180  # independent hard disk bound; no recurrence is installed
MAX_RESOLVED_DAYS = 30
MAX_DAYS = 45
LIMITS = {'gamma': 1_000_000, 'forecast': 300_000, 'book': 512_000, 'awc': 256_000,
          'tokens': 128_000, 'resolution': 128_000}
# Worst-case JSON escape expansion is six bytes per input byte (e.g. NUL).
# 6 * sum(LIMITS) = 13,944,000; the remainder covers bounded row metadata.
# Check the serialized row before publishing, including untrusted headers.
MAX_TAPE_ROW_BYTES = 16_000_000
APPROVED_OUTPUT_ROOT = Path(__file__).absolute().parents[2] / 'data' / 'awc-campaign'
REQUEST_WALL_SECONDS = 12
CYCLE_WALL_SECONDS = 60
_deadlines = []


def _alarm(_signum, _frame):
    now = time.monotonic()
    for deadline, error in _deadlines:
        if now >= deadline:
            raise error
    if _deadlines:
        signal.setitimer(signal.ITIMER_REAL, max(0.000001, min(d for d, _ in _deadlines) - now))


@contextmanager
def _wall_bound(seconds, error):
    """Interrupt blocked reads, including slow trickles. Main-thread POSIX only."""
    if (threading.current_thread() is not threading.main_thread() or
            not all(hasattr(signal, name) for name in ('setitimer', 'pthread_sigmask', 'sigpending', 'sigwait'))):
        raise ValueError('wall deadline requires POSIX main thread')
    previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
    outer = not _deadlines
    previous = None
    added = False
    try:
        if signal.SIGALRM in previous_mask:
            raise ValueError('blocked alarm prevents safe wall deadline')
        if outer:
            if signal.getitimer(signal.ITIMER_REAL)[0]:
                raise ValueError('pre-existing alarm prevents safe wall deadline')
            previous = signal.signal(signal.SIGALRM, _alarm)
        deadline = time.monotonic() + seconds
        _deadlines.append((deadline, error))
        added = True
        signal.setitimer(signal.ITIMER_REAL, max(0.000001, min(d for d, _ in _deadlines) - time.monotonic()))
        # An overdue outer alarm can be delivered while unmasking, before
        # contextmanager has yielded. Roll it back through the same path.
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
    except BaseException:
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
        try:
            if added:
                signal.setitimer(signal.ITIMER_REAL, 0)
                _deadlines.pop()
            if signal.SIGALRM in signal.sigpending():
                signal.sigwait({signal.SIGALRM})
            if outer and previous is not None:
                signal.signal(signal.SIGALRM, previous)
            elif added:
                signal.setitimer(signal.ITIMER_REAL, max(0.000001, min(d for d, _ in _deadlines) - time.monotonic()))
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        raise
    try:
        yield
    finally:
        try:
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
        except BaseException:
            # A callback can run immediately after the mask syscall returns;
            # still finish teardown rather than leaving a live timer/handler.
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
        try:
            # Disarm before changing the stack, and consume an already pending
            # timer before restoring the caller's handler on the outer exit.
            signal.setitimer(signal.ITIMER_REAL, 0)
            expired = next((exc for d, exc in _deadlines if time.monotonic() >= d), None)
            _deadlines.pop()
            if signal.SIGALRM in signal.sigpending():
                signal.sigwait({signal.SIGALRM})
            if outer:
                signal.signal(signal.SIGALRM, previous)
            else:
                signal.setitimer(signal.ITIMER_REAL, max(0.000001, min(d for d, _ in _deadlines) - time.monotonic()))
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        if expired is not None:
            raise expired


@contextmanager
def _directory_fd(directory: Path, *, create=False):
    """Walk from / using directory handles; never follow a replaced component."""
    root = Path(os.path.abspath(APPROVED_OUTPUT_ROOT))
    target = Path(os.path.abspath(directory))
    if target != root and root not in target.parents:
        raise ValueError('output outside approved research root')
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        missing = False
        for component in target.parts[1:]:
            if missing:
                break
            try:
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            except FileNotFoundError:
                if not create:
                    missing = True
                    continue
                os.mkdir(component, mode=0o700, dir_fd=fd)
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            except OSError as exc:
                raise ValueError('symlink or invalid output directory component') from exc
            os.close(fd)
            fd = child
        yield None if missing else fd
    finally:
        os.close(fd)


_SLUG = re.compile(r'highest-temperature-in-[a-z]+(?:-[a-z]+)*-on-[a-z]+-\d{1,2}-20\d{2}')
_STATION = re.compile(r'[A-Z]{4}')
_ID = re.compile(r'[0-9]{1,32}')
_TOKEN = re.compile(r'[0-9]{1,78}')  # decimal uint256 token IDs
_CONDITION = re.compile(r'0x[0-9a-fA-F]{64}')


@dataclass(frozen=True)
class Config:
    slug: str
    event_id: str
    target_date: str
    station: str
    zone: str
    latitude: float
    longitude: float
    market_id: str
    start_at: str
    rule_sha256: str

    def __post_init__(self):
        if not _SLUG.fullmatch(self.slug) or not _STATION.fullmatch(self.station):
            raise ValueError('unsupported event slug or station')
        if not _ID.fullmatch(self.event_id) or not _ID.fullmatch(self.market_id):
            raise ValueError('invalid exact numeric IDs')
        day = date.fromisoformat(self.target_date)
        if day.isoformat() != self.target_date or not ZoneInfo(self.zone):
            raise ValueError('invalid target date or zone')
        if not (-90 <= self.latitude <= 90 and -180 <= self.longitude <= 180):
            raise ValueError('invalid coordinates')
        # This minimal campaign supports one verified airport only. Other
        # stations require an independently reviewed coordinate/zone registry.
        if (self.station, self.zone, self.latitude, self.longitude) != ('OPKC', 'Asia/Karachi', 24.9065, 67.1608):
            raise ValueError('unsupported station coordinates or zone')
        if not re.fullmatch(r'[0-9a-f]{64}', self.rule_sha256):
            raise ValueError('exact rule fingerprint required')
        start = _aware(self.start_at)
        if start > datetime.now(timezone.utc) + timedelta(days=1):
            raise ValueError('campaign starts in future')


def _aware(value: str) -> datetime:
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.utcoffset() is None:
        raise ValueError('timezone-aware timestamp required')
    return stamp.astimezone(timezone.utc)


def _array(value):
    result = json.loads(value) if isinstance(value, str) else value
    if not isinstance(result, list):
        raise ValueError('expected array')
    return result


def validate_event(config: Config, event: dict) -> dict:
    """No city-only matching: bind Gamma ID, slug, date, URL, rules and rung."""
    if not isinstance(event, dict) or str(event.get('id')) != config.event_id or event.get('slug') != config.slug or event.get('eventDate') != config.target_date:
        raise ValueError('exact event identity mismatch')
    inspection = inspect_awc_source(event.get('resolutionSource', ''), station=config.station,
                                    target_date=date.fromisoformat(config.target_date),
                                    timezone_name=config.zone, description=event.get('description', ''))
    if not inspection.observation_only:
        raise ValueError('AWC source/rule mismatch: ' + inspection.reason)
    if hashlib.sha256(event['description'].encode()).hexdigest() != config.rule_sha256:
        raise ValueError('rule fingerprint changed')
    markets = event.get('markets')
    if not isinstance(markets, list) or len(markets) > 32:
        raise ValueError('unbounded or invalid markets')
    found = [m for m in markets if isinstance(m, dict) and str(m.get('id')) == config.market_id]
    if len(found) != 1:
        raise ValueError('market ID not unique')
    market = found[0]
    if market.get('resolutionSource') != event['resolutionSource'] or market.get('description') != event['description']:
        raise ValueError('market rule differs from event')
    if not _CONDITION.fullmatch(market.get('conditionId', '')):
        raise ValueError('invalid condition')
    tokens = _array(market.get('clobTokenIds'))
    if len(tokens) != 2 or len(set(tokens)) != 2 or any(not isinstance(t, str) or not _TOKEN.fullmatch(t) or int(t) >= 2**256 for t in tokens):
        raise ValueError('invalid outcome tokens')
    if _array(market.get('outcomes')) != ['Yes', 'No']:
        raise ValueError('unsupported outcomes')
    if not isinstance(market.get('question'), str) or not market['question']:
        raise ValueError('market question missing')
    return {'market': market, 'token_id': tokens[0]}


def _market_pin(market: dict) -> tuple[str, tuple[str, str]]:
    return market['conditionId'], tuple(_array(market['clobTokenIds']))


def _previous_pin(rows: list[dict], config: Config):
    """Recover even legacy pins from immutable raw Gamma, rejecting missing/drifted evidence."""
    pins = []
    for row in rows:
        if row.get('target_date') != config.target_date:
            continue
        if (row.get('event_id'), row.get('market_id'), row.get('rule_sha256')) != (
                config.event_id, config.market_id, config.rule_sha256):
            raise ValueError('prior daily manifest pin drift')
        try:
            capture = row['captures']['gamma']
            raw = capture['body'].encode('utf-8')
            if hashlib.sha256(raw).hexdigest() != capture['sha256']:
                raise ValueError('Gamma digest mismatch')
            market = validate_event(config, json.loads(raw))['market']
            pins.append(_market_pin(market))
        except (KeyError, TypeError, ValueError, UnicodeError) as exc:
            raise ValueError('prior daily manifest pin evidence invalid') from exc
    if len(set(pins)) > 1:
        raise ValueError('prior daily manifest pin drift')
    return pins[0] if pins else None



def discover_daily_manifest(event: dict, target_date: str, start_at: str,
                            *, pinned: Config | None = None, pinned_identity=None) -> Config:
    """Discover the unique lower-tail rung; pin every identity before collection.

    The first raw Gamma response may supply a candidate, not an operator-reviewed
    30-day schedule. Subsequent reads must match the exact immutable candidate.
    """
    day = date.fromisoformat(target_date)
    if day.isoformat() != target_date:
        raise ValueError('invalid day')
    slug = f'highest-temperature-in-karachi-on-{day.strftime("%B").lower()}-{day.day}-{day.year}'
    if not isinstance(event, dict) or event.get('slug') != slug or event.get('eventDate') != target_date:
        raise ValueError('daily event identity mismatch')
    markets = event.get('markets')
    if not isinstance(markets, list) or not 1 <= len(markets) <= 32 or any(not isinstance(m, dict) for m in markets):
        raise ValueError('invalid daily markets')
    ids = [str(m.get('id')) for m in markets]
    if len(set(ids)) != len(ids) or any(not _ID.fullmatch(v) for v in ids):
        raise ValueError('ambiguous daily market IDs')
    question = re.compile(r'Will the highest temperature in Karachi be -?\d+°C or below on ' +
                          re.escape(day.strftime('%B')) + r' ' + str(day.day) + r'\?')
    rungs = [m for m in markets if isinstance(m.get('question'), str) and question.fullmatch(m['question'])]
    if len(rungs) != 1:
        raise ValueError('ambiguous daily lower-tail rung')
    if not isinstance(event.get('description'), str):
        raise ValueError('missing daily rule')
    config = Config(slug=slug, event_id=str(event.get('id')), target_date=target_date,
                    station='OPKC', zone='Asia/Karachi', latitude=24.9065, longitude=67.1608,
                    market_id=str(rungs[0]['id']), start_at=start_at,
                    rule_sha256=hashlib.sha256(event['description'].encode()).hexdigest())
    identity = validate_event(config, event)
    if pinned is not None and config != pinned:
        raise ValueError('daily manifest pin drift')
    if pinned_identity is not None and _market_pin(identity['market']) != pinned_identity:
        raise ValueError('daily condition/token pin drift')
    return config


def crosscheck_resolution(config: Config, market: dict, gamma_label: dict,
                          tokens: dict, resolution: dict) -> dict:
    """Data API on-chain token payout and exact resolved condition must agree."""
    failure = {'status': 'unverified', 'reason': 'Data API finality/identity mismatch'}
    if gamma_label.get('status') != 'official_gamma_binary':
        return failure
    try:
        expected = _array(market['clobTokenIds'])
        rows = tokens['data']
        settlements = resolution['data']
        if (not isinstance(rows, list) or len(rows) != 2 or
                not isinstance(settlements, list) or len(settlements) != 1 or
                not isinstance(settlements[0], dict) or
                settlements[0].get('condition_id') != market['conditionId'] or
                settlements[0].get('status') != 'resolved'):
            return failure
        prices = []
        for index, row in enumerate(rows):
            if (not isinstance(row, dict) or row.get('token_id') != expected[index] or
                    row.get('opposite_token_id') != expected[1-index] or
                    row.get('condition_id') != market['conditionId'] or
                    str(row.get('event_id')) != config.event_id or
                    row.get('event_slug') != config.slug or row.get('module') != 'v1_ctf' or
                    row.get('outcome_index') != index or row.get('outcome') != ('Yes', 'No')[index] or
                    row.get('resolved') is not True or row.get('closed') is not True):
                return failure
            price = row.get('final_price')
            if isinstance(price, bool) or not isinstance(price, (str, int, float)):
                return failure
            parsed = Decimal(str(price))
            if parsed not in (Decimal(0), Decimal(1)):
                return failure
            prices.append(parsed)
        if prices != ([Decimal(1), Decimal(0)] if gamma_label['winner'] == 'Yes'
                      else [Decimal(0), Decimal(1)]):
            return failure
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return failure
    return {'status': 'venue_corroborated_final_binary', 'winner': gamma_label['winner'],
            'basis': 'same-venue Gamma payout + Polymarket Data API token final prices and resolved condition; not independent UMA/on-chain verification'}

class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError('redirect refused')


def public_fetch(url: str, limit: int) -> tuple[bytes, dict]:
    """Hard response, redirect, and per-request timeout bounds; no auth headers."""
    from urllib.parse import urlsplit
    parts = urlsplit(url)
    if (parts.scheme != 'https' or parts.hostname not in
            {'gamma-api.polymarket.com', 'clob.polymarket.com', 'data-api.polymarket.com',
             'aviationweather.gov', 'api.open-meteo.com'}
            or parts.username or parts.password or parts.port or parts.fragment):
        raise ValueError('non-public endpoint')
    # Never inherit proxy configuration (which can contain credentials).
    with _wall_bound(REQUEST_WALL_SECONDS, TimeoutError('public request timed out')):
        opener = build_opener(ProxyHandler({}), _NoRedirect)
        return _fetch_response(opener, url, parts, limit)


def _fetch_response(opener, url, parts, limit):
    try:
        with opener.open(Request(url, headers={'User-Agent': 'awc-paper-observation/1.0', 'Accept': 'application/json, text/plain'}), timeout=10) as response:
            if response.status != 200 and not (parts.hostname == 'aviationweather.gov' and response.status == 204):
                raise ValueError('public HTTP non-200')
            length = response.headers.get('Content-Length')
            if length and (not length.isdecimal() or int(length) > limit):
                raise ValueError('response exceeds byte limit')
            chunks = bytearray()
            while True:
                chunk = response.read(min(16384, limit + 1 - len(chunks)))
                chunks.extend(chunk)
                if len(chunks) > limit:
                    raise ValueError('response exceeds byte limit')
                if not chunk:
                    break
            body = bytes(chunks)
            if len(body) > limit:
                raise ValueError('response exceeds byte limit')
            if response.status == 204 and body:
                raise ValueError('204 with unexpected body')
            return body, {'Status': response.status, 'Date': response.headers.get('Date'),
                          'Content-Type': response.headers.get('Content-Type')}
    except TimeoutError as exc:
        raise TimeoutError('public request timed out') from exc
    except URLError as exc:
        if not isinstance(exc, HTTPError) and isinstance(exc.reason, TimeoutError):
            raise TimeoutError('public request timed out') from exc
        raise ValueError('public request failed') from exc


def _canonical(record: dict) -> bytes:
    return (json.dumps(record, sort_keys=True, separators=(',', ':'), ensure_ascii=False) + '\n').encode()


def _records(directory: Path) -> list[tuple[dict, str]]:
    with _directory_fd(directory) as dirfd:
        paths = sorted(name for name in os.listdir(dirfd) if name.endswith('.json')) if dirfd is not None else []
        if len(paths) > MAX_CYCLES:
            raise ValueError('cycle cap exceeded')
        records = []
        prev = None
        for i, name in enumerate(paths):
            if name != f'{i:06d}.json':
                raise ValueError('tape sequence mismatch')
            try:
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dirfd)
            except OSError as exc:
                raise ValueError('symlink or invalid tape entry') from exc
            with os.fdopen(fd, 'rb') as file:
                info = os.fstat(file.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_TAPE_ROW_BYTES:
                    raise ValueError('tape sequence or size mismatch')
                body = file.read(MAX_TAPE_ROW_BYTES + 1)
            if len(body) > MAX_TAPE_ROW_BYTES:
                raise ValueError('tape size mismatch')
            record = json.loads(body)
            if body != _canonical(record) or record.get('sequence') != i or record.get('previous_sha256') != prev:
                raise ValueError('tape continuity mismatch')
            prev = hashlib.sha256(body).hexdigest()
            records.append((record, prev))
        return records


def report(directory: Path) -> dict:
    rows = [row for row, _ in _records(directory)]
    resolved = {row['target_date'] for row in rows if _corroborated_row(row)}
    return {'cycles': len(rows), 'resolved_event_days': len(resolved), 'last_at': rows[-1]['captured_at'] if rows else None,
            'pending_labels': sum(not _corroborated_row(row) for row in rows),
            'stop_at_resolved_days': MAX_RESOLVED_DAYS, 'stop_after_calendar_days': MAX_DAYS,
            'paper_only': True, 'live_eligible': False}


def _corroborated_row(row: dict) -> bool:
    """Count only replayable same-venue corroboration, never a claimed status alone."""
    if row.get('status') != 'captured' or row.get('label', {}).get('status') != 'venue_corroborated_final_binary':
        return False
    try:
        config = Config(slug=row['captures']['gamma']['url'].split('/events/slug/')[1],
                        event_id=row['event_id'], market_id=row['market_id'],
                        target_date=row['target_date'], station=row['station'], zone=row['zone'],
                        latitude=24.9065, longitude=67.1608, start_at=row['campaign_start_at'],
                        rule_sha256=row['rule_sha256'])
        documents = {}
        for kind in ('gamma', 'awc', 'tokens', 'resolution'):
            capture = row['captures'][kind]
            raw = capture['body'].encode('utf-8')
            if capture['status'] != 200 or hashlib.sha256(raw).hexdigest() != capture['sha256']:
                return False
            if kind != 'awc':
                documents[kind] = json.loads(raw)
        event = documents['gamma']
        market = validate_event(config, event)['market']
        gamma_label = _label(market, event=event, target_date=config.target_date,
                             fetched_at=row['captures']['gamma']['first_available_at'])
        checked = crosscheck_resolution(config, market, gamma_label, documents['tokens'], documents['resolution'])
        return (checked['status'] == 'venue_corroborated_final_binary' and
                checked['winner'] == row['label'].get('winner') and
                gamma_label == row.get('gamma_label'))
    except (KeyError, TypeError, ValueError, IndexError, UnicodeError):
        return False


def _label(market: dict, *, event: dict | None = None,
           target_date: str | None = None, fetched_at: str | None = None) -> dict:
    """Gamma's exact closed binary payout, not an inferred weather winner."""
    if market.get('closed') is not True:
        return {'status': 'unverified', 'reason': 'market not closed'}
    try:
        # Historical Gamma automatically-resolved events expose closedTime but
        # no resolvedAt. Require both market and event terminal flags there.
        if market.get('resolvedAt'):
            resolved = _aware(market['resolvedAt'])
            basis = 'Gamma closed + resolvedAt + binary outcomePrices'
        elif (event and event.get('closed') is True and event.get('automaticallyResolved') is True
              and market.get('automaticallyResolved') is True):
            resolved = _aware(event['closedTime'])
            market_closed = _aware(market['closedTime'])
            if market_closed > resolved:
                raise ValueError('market close after event resolution')
            basis = 'Gamma event+market automaticallyResolved, closedTime, binary outcomePrices'
        else:
            raise ValueError('no explicit resolution metadata')
        if (target_date and resolved.date() < date.fromisoformat(target_date)) or (fetched_at and resolved > _aware(fetched_at)):
            raise ValueError('resolution outside available window')
        prices = _array(market.get('outcomePrices'))
        if prices not in (['1', '0'], ['0', '1'], [1, 0], [0, 1]):
            raise ValueError('non-binary payout')
    except (ValueError, TypeError, KeyError):
        return {'status': 'unverified', 'reason': 'ambiguous official payout'}
    return {'status': 'official_gamma_binary', 'winner': 'Yes' if str(prices[0]) == '1' else 'No',
            'resolved_at': resolved.isoformat(), 'basis': basis}


class Campaign:
    def __init__(self, directory: Path, config: Config, *, fetch=public_fetch, clock=None):
        self.directory, self.config, self.fetch = directory, config, fetch
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def once(self) -> dict:
        with _wall_bound(CYCLE_WALL_SECONDS, ValueError('cycle wall budget exhausted')):
            return self._once()

    def _once(self, *, gamma_response=None) -> dict:
        now = self.clock().astimezone(timezone.utc)
        rows = _records(self.directory)
        if rows and any(r.get('campaign_start_at') != self.config.start_at or
                        r.get('station') != self.config.station for r, _ in rows):
            raise ValueError('campaign identity/start drift')
        if now < _aware(self.config.start_at) or now >= _aware(self.config.start_at) + timedelta(days=MAX_DAYS):
            return {'status': 'deadline', 'paper_only': True}
        if len(rows) >= MAX_CYCLES or report(self.directory)['resolved_event_days'] >= MAX_RESOLVED_DAYS:
            return {'status': 'cap', 'paper_only': True}
        # One immutable snapshot per event/UTC hour; no implicit retries.
        if any(r['event_id'] == self.config.event_id and r['target_date'] == self.config.target_date
               and r['captured_at'][:13] == now.isoformat()[:13] for r, _ in rows):
            return {'status': 'already_captured', 'paper_only': True}
        urls = {'gamma': 'https://gamma-api.polymarket.com/events/slug/' + self.config.slug}
        captures = {}
        def get(kind, url):
            start = self.clock().astimezone(timezone.utc).isoformat()
            try:
                body, headers = (gamma_response if kind == 'gamma' and gamma_response is not None
                                 else self.fetch(url, LIMITS[kind]))
            except TimeoutError:
                if kind != 'awc':
                    raise
                captures[kind] = {'url': url, 'request_at': start,
                                  'failed_at': self.clock().astimezone(timezone.utc).isoformat(),
                                  'status': 'timeout', 'failure_code': 'timeout'}
                return None
            if not isinstance(body, bytes) or len(body) > LIMITS[kind]:
                raise ValueError('oversized or invalid response')
            status = headers.get('Status', 200)
            if status != 200 and not (kind == 'awc' and status == 204 and not body):
                raise ValueError('unexpected source status or 204 body')
            end = self.clock().astimezone(timezone.utc).isoformat()
            text = body.decode('utf-8', errors='strict')
            captures[kind] = {'url': url, 'request_at': start, 'first_available_at': end,
                              'status': status,
                              'response_date': headers.get('Date'), 'content_type': headers.get('Content-Type'),
                              'sha256': hashlib.sha256(body).hexdigest(), 'body': text}
            if status == 204:
                captures[kind]['failure_code'] = 'no_data'
            if kind == 'forecast':
                captures[kind]['issue_at'] = None  # provider omitted run issuance
            return text
        event = json.loads(get('gamma', urls['gamma']))
        identity = validate_event(self.config, event)
        market = identity['market']
        prior_pin = _previous_pin([row for row, _ in rows], self.config)
        if prior_pin is not None and _market_pin(market) != prior_pin:
            raise ValueError('daily condition/token pin drift')
        day = date.fromisoformat(self.config.target_date)
        forecast_url = 'https://api.open-meteo.com/v1/forecast?' + urlencode({
            'latitude': self.config.latitude, 'longitude': self.config.longitude,
            'hourly': 'temperature_2m', 'timezone': self.config.zone,
            'start_date': day.isoformat(), 'end_date': day.isoformat()})
        forecast = json.loads(get('forecast', forecast_url))
        if not isinstance(forecast, dict) or not isinstance(forecast.get('hourly'), dict):
            raise ValueError('invalid forecast')
        # Open-Meteo response lacks a reliable run/issue timestamp. Never infer one.
        book = json.loads(get('book', 'https://clob.polymarket.com/book?' + urlencode({'token_id': identity['token_id']})))
        if not isinstance(book, dict) or book.get('market') != market['conditionId'] or book.get('asset_id') != identity['token_id'] or not isinstance(book.get('bids'), list) or not isinstance(book.get('asks'), list) or not re.fullmatch(r'\d{13}', str(book.get('timestamp', ''))):
            raise ValueError('book identity/timestamp mismatch')
        book_at = datetime.fromtimestamp(int(book['timestamp']) / 1000, timezone.utc)
        if book_at > _aware(captures['book']['first_available_at']) + timedelta(seconds=5):
            raise ValueError('book timestamp ahead of acquisition')
        raw = get('awc', event['resolutionSource'])
        awc_reason = captures['awc'].get('failure_code')
        if awc_reason:
            obs = {'status': 'unavailable', 'reason': awc_reason,
                   'correction_cutoff_proven': False, 'wu_fallback_proven': False}
        else:
            observations = parse_raw_observations(raw, station=self.config.station, target_date=day,
                                                   timezone_name=self.config.zone)
            obs = {'status': 'future_day' if now.astimezone(ZoneInfo(self.config.zone)).date() < day else 'snapshot_not_settlement',
                   'count': len(observations), 'maximum_c': str(max((o.temperature_c for o in observations), default=None)) if observations else None,
                   'rounded_max_c': max((o.rounded_c for o in observations), default=None),
                   'report_times': [o.observed_at.isoformat() for o in observations],
                   'correction_cutoff_proven': False, 'wu_fallback_proven': False}
        status = 'captured_partial' if awc_reason else 'captured'
        gamma_label = _label(market, event=event, target_date=self.config.target_date,
                             fetched_at=captures['gamma']['first_available_at'])
        label = {'status': 'unverified', 'reason': 'AWC unavailable: ' + awc_reason} if awc_reason else {
            'status': 'unverified', 'reason': 'same-venue finality corroboration unavailable'}
        if not awc_reason and gamma_label['status'] == 'official_gamma_binary':
            tokens = _array(market['clobTokenIds'])
            checks = (
                ('tokens', 'https://data-api.polymarket.com/v2/tokens?' +
                 urlencode({'token_id': ','.join(tokens)})),
                ('resolution', 'https://data-api.polymarket.com/v2/resolutions?' +
                 urlencode({'condition': market['conditionId']})),
            )
            documents = {}
            for kind, url in checks:
                try:
                    raw = get(kind, url)
                    if raw is None:
                        raise ValueError('missing Data API body')
                    documents[kind] = json.loads(raw)
                except (TimeoutError, ValueError, TypeError) as exc:
                    failure_code = 'timeout' if isinstance(exc, TimeoutError) else 'invalid_or_unavailable'
                    captures.setdefault(kind, {'url': url, 'request_at': self.clock().astimezone(timezone.utc).isoformat(),
                                               'status': 'unavailable'})['failure_code'] = failure_code
                    label = {'status': 'unverified', 'reason': 'Data API ' + kind + ' unavailable'}
                    break
            else:
                label = crosscheck_resolution(self.config, market, gamma_label,
                                              documents['tokens'], documents['resolution'])
        record = {'sequence': len(rows), 'previous_sha256': rows[-1][1] if rows else None,
                  'status': status, 'paper_only': True, 'live_eligible': False,
                  'event_id': self.config.event_id, 'market_id': self.config.market_id,
                  'target_date': self.config.target_date, 'station': self.config.station, 'zone': self.config.zone,
                  'campaign_start_at': self.config.start_at, 'rule_sha256': self.config.rule_sha256,
                  'captured_at': now.isoformat(), 'captures': captures, 'observation': obs,
                  'gamma_label': gamma_label, 'label': label}
        # Publish only a complete, fsynced row; a signal during creation must
        # never leave a numbered entry that blocks the sequence permanently.
        body = _canonical(record)
        if len(body) > MAX_TAPE_ROW_BYTES:
            raise ValueError('serialized tape row exceeds byte limit')
        with _directory_fd(self.directory, create=True) as dirfd:
            name = f'{len(rows):06d}.json'
            temp = f'.{name}.{secrets.token_hex(16)}.tmp'
            try:
                fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=dirfd)
                with os.fdopen(fd, 'wb') as f:
                    f.write(body)
                    f.flush()
                    os.fsync(f.fileno())
                # Hard-link is atomic and exclusive: rename could replace a
                # concurrent writer's numbered entry. Keep the alarm masked
                # only for this short publish/cleanup critical section.
                previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
                try:
                    now = time.monotonic()
                    for deadline, wall_error in _deadlines:
                        if now >= deadline:
                            raise wall_error
                    os.link(temp, name, src_dir_fd=dirfd, dst_dir_fd=dirfd, follow_symlinks=False)
                    os.unlink(temp, dir_fd=dirfd)
                finally:
                    signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            finally:
                previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
                try:
                    try:
                        os.unlink(temp, dir_fd=dirfd)
                    except FileNotFoundError:
                        pass
                finally:
                    signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        return {'status': status, 'sequence': record['sequence'], 'label': record['label']['status'], 'paper_only': True}


class DailyCampaign:
    """Manual daily discovery; reuse its Gamma read within the six-GET cycle."""
    def __init__(self, directory: Path, target_date: str, start_at: str, *, fetch=public_fetch, clock=None):
        self.directory, self.target_date, self.start_at, self.fetch = directory, target_date, start_at, fetch
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def once(self) -> dict:
        with _wall_bound(CYCLE_WALL_SECONDS, ValueError('cycle wall budget exhausted')):
            now = self.clock().astimezone(timezone.utc)
            rows = [row for row, _ in _records(self.directory)]
            if now < _aware(self.start_at) or now >= _aware(self.start_at) + timedelta(days=MAX_DAYS):
                return {'status': 'deadline', 'paper_only': True}
            if len(rows) >= MAX_CYCLES or report(self.directory)['resolved_event_days'] >= MAX_RESOLVED_DAYS:
                return {'status': 'cap', 'paper_only': True}
            if any(row.get('campaign_start_at') != self.start_at or row.get('station') != 'OPKC' for row in rows):
                raise ValueError('campaign start/station drift')
            day = date.fromisoformat(self.target_date)
            if day.isoformat() != self.target_date:
                raise ValueError('invalid target date')
            if not (_aware(self.start_at).astimezone(ZoneInfo('Asia/Karachi')).date() <= day <=
                    (_aware(self.start_at) + timedelta(days=MAX_DAYS)).astimezone(ZoneInfo('Asia/Karachi')).date()):
                raise ValueError('target date outside campaign')
            slug = f'highest-temperature-in-karachi-on-{day.strftime("%B").lower()}-{day.day}-{day.year}'
            previous = [r for r in rows if r.get('target_date') == self.target_date]
            if previous and any(r.get('captured_at', '')[:13] == now.isoformat()[:13] for r in previous):
                return {'status': 'already_captured', 'paper_only': True}
            url = 'https://gamma-api.polymarket.com/events/slug/' + slug
            response = self.fetch(url, LIMITS['gamma'])
            body, _headers = response
            if not isinstance(body, bytes) or len(body) > LIMITS['gamma']:
                raise ValueError('invalid daily Gamma response')
            event = json.loads(body.decode('utf-8'))
            pinned = None
            if previous:
                first = previous[0]
                pinned = Config(slug=slug, event_id=first['event_id'], market_id=first['market_id'],
                                target_date=self.target_date, station='OPKC', zone='Asia/Karachi',
                                latitude=24.9065, longitude=67.1608, start_at=self.start_at,
                                rule_sha256=first['rule_sha256'])
                if any((r['event_id'], r['market_id'], r['rule_sha256']) !=
                       (pinned.event_id, pinned.market_id, pinned.rule_sha256) for r in previous):
                    raise ValueError('prior daily manifest pin drift')
            prior_pin = _previous_pin(previous, pinned) if pinned is not None else None
            config = discover_daily_manifest(event, self.target_date, self.start_at,
                                             pinned=pinned, pinned_identity=prior_pin)
            return Campaign(self.directory, config, fetch=self.fetch, clock=self.clock)._once(gamma_response=response)


def main():
    p = argparse.ArgumentParser(description='public-only AWC one-cycle evidence collector; no scheduler or trading')
    p.add_argument('--directory', required=True, type=Path)
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--config', type=Path, help='explicit event identity JSON')
    group.add_argument('--discover-date', help='manual OPKC YYYY-MM-DD discovery')
    p.add_argument('--start-at', help='fixed campaign start, required with --discover-date')
    p.add_argument('--report', action='store_true')
    args = p.parse_args()
    # Fail closed on unknown config keys (Config constructor) and runtime-only state.
    if args.discover_date and not args.start_at:
        p.error('--start-at required for daily discovery')
    if args.config and args.start_at:
        p.error('--start-at only applies to daily discovery')
    if args.report:
        result = report(args.directory)
    elif args.config:
        result = Campaign(args.directory, Config(**json.loads(args.config.read_text()))).once()
    else:
        result = DailyCampaign(args.directory, args.discover_date, args.start_at).once()
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
