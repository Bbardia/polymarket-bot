"""Offline AWC raw-METAR observations; NOT a settlement or trading resolver.

Corrections, publication cutoffs, fallback and discretionary clarifications are
not reconstructible from an unversioned raw snapshot. No function here grants
market eligibility. Invalid or ambiguous input raises instead of guessing.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_FLOOR
from urllib.parse import parse_qsl, urlsplit
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class AWCInspection:
    station: str | None
    observation_only: bool
    reason: str


_REQUIRED_RULES = (
    'hourly temperature remark of any routine (METAR) observation',
    'A report whose line begins with SPECI is a special observation and does not count.',
    'temperature group in the body of that observation governs',
    'station\'s local time',
    'whether or not it is marked as a correction (COR)',
    'Weather Underground Daily Observations table will be used',
)


def inspect_awc_source(source: str, *, station: str, target_date: date,
                       timezone_name: str, description: str) -> AWCInspection:
    """Validate a narrowly scoped research URL and rule fingerprint, never eligibility."""
    def refuse(reason: str) -> AWCInspection:
        return AWCInspection(None, False, reason)

    try:
        p = urlsplit(source)
        if (p.scheme != 'https' or p.netloc != 'aviationweather.gov'
                or p.path != '/api/data/metar' or p.fragment):
            return refuse('unrecognized AWC endpoint')
        pairs = parse_qsl(p.query, keep_blank_values=True, strict_parsing=True)
        params = dict(pairs)
        if len(pairs) != 4 or set(params) != {'ids', 'format', 'date', 'hours'}:
            return refuse('ambiguous AWC query')
        if not re.fullmatch(r'[A-Z]{4}', station) or params['ids'] != station or params['format'] != 'raw':
            return refuse('station or format mismatch')
        if not re.fullmatch(r'[1-9][0-9]?', params['hours']):
            return refuse('invalid window')
        hours = int(params['hours'])
        if hours > 48 or not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ', params['date']):
            return refuse('invalid AWC time')
        end = datetime.fromisoformat(params['date'].replace('Z', '+00:00'))
        zone = ZoneInfo(timezone_name)
        start_local = datetime(target_date.year, target_date.month, target_date.day, tzinfo=zone)
        stop_local = datetime.combine(target_date + timedelta(days=1), datetime.min.time(), zone)
        if end - timedelta(hours=hours) > start_local.astimezone(timezone.utc) or end < stop_local.astimezone(timezone.utc):
            return refuse('window does not cover the local observation day')
        if not description or any(marker not in description for marker in _REQUIRED_RULES):
            return refuse('unrecognized or changed observation/settlement rules')
        return AWCInspection(station, True, 'observation-only: settlement and live eligibility unverified')
    except (ValueError, OverflowError):
        return refuse('invalid source parameters')


@dataclass(frozen=True)
class AWCRawObservation:
    observed_at: datetime
    temperature_c: Decimal
    rounded_c: int
    raw: str


_REPORT = re.compile(r'^(?:METAR )?(?:COR )?(?P<station>[A-Z]{4}) (?P<day>\d{2})(?P<hour>\d{2})(?P<minute>\d{2})Z\b')
_BODY = re.compile(r'^(?:M?\d{2})/(?:M?\d{2}|//)$')
_REMARK = re.compile(r'^T([01])(\d{3})([01])(\d{3})$')


def parse_raw_observations(raw: str, *, station: str, target_date: date,
                           timezone_name: str) -> tuple[AWCRawObservation, ...]:
    """Parse a snapshot, but never infer its publication or correction history."""
    if not re.fullmatch(r'[A-Z]{4}', station):
        raise ValueError('invalid station')
    zone = ZoneInfo(timezone_name)
    rows: dict[datetime, AWCRawObservation] = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith('SPECI '):
            continue
        match = _REPORT.match(line)
        if match is None or match['station'] != station:
            raise ValueError('unrecognized or mismatched METAR report')
        day, hour, minute = (int(match[key]) for key in ('day', 'hour', 'minute'))
        if hour > 23 or minute > 59:
            raise ValueError('invalid report time')
        # DDHHMMZ has no month: accept only a unique candidate near the target day.
        stamps = []
        for shift in (-1, 0, 1):
            month_index = target_date.year * 12 + target_date.month - 1 + shift
            year, month_zero = divmod(month_index, 12)
            try:
                stamp = datetime(year, month_zero + 1, day, hour, minute, tzinfo=timezone.utc)
            except ValueError:
                continue
            if abs((stamp.date() - target_date).days) <= 2:
                stamps.append(stamp)
        if len(stamps) != 1:
            raise ValueError('ambiguous report month')
        stamp = stamps[0]
        if stamp.astimezone(zone).date() != target_date:
            continue
        tokens = line.split()
        remark_positions = [i for i, token in enumerate(tokens) if token == 'RMK']
        if len(remark_positions) > 1:
            raise ValueError('ambiguous remark')
        body = tokens[:remark_positions[0]] if remark_positions else tokens
        remarks = tokens[remark_positions[0] + 1:] if remark_positions else []
        t_tokens = [token for token in remarks if token.startswith('T') and len(token) > 1 and token[1].isdigit()]
        if any(_REMARK.fullmatch(token) is None for token in t_tokens) or len(t_tokens) > 1:
            raise ValueError('malformed or ambiguous hourly T remark')
        if t_tokens:
            t = _REMARK.fullmatch(t_tokens[0])
            assert t is not None
            temperature = Decimal(-1 if t[1] == '1' else 1) * Decimal(int(t[2])) / 10
        else:
            values = [token for token in body if _BODY.fullmatch(token)]
            if len(values) > 1:
                raise ValueError('ambiguous body temperature')
            if not values:
                continue
            first = values[0].split('/')[0]
            temperature = Decimal(int(first[1:] if first.startswith('M') else first) * (-1 if first.startswith('M') else 1))
        item = AWCRawObservation(stamp, temperature, int((temperature + Decimal('0.5')).to_integral_value(rounding=ROUND_FLOOR)), line)
        if stamp in rows and rows[stamp].raw != line:
            raise ValueError('multiple report versions for observation time; correction order unknown')
        rows[stamp] = item
    return tuple(rows[key] for key in sorted(rows))
