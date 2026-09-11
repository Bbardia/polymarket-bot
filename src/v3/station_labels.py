"""Offline METAR/SPECI daily labels, separate from traded-position selection.

IEM report_type=3,4 acquisition excludes the 5-minute feed. Archive labels do
not establish when a report was available to a trading decision.
"""
from __future__ import annotations
import csv
import io
import re
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from .resolver import parse_resolver_identity

T_GROUP = re.compile(r'\bT([01])(\d{3})[01]\d{3}\b')
BODY_TEMP = re.compile(r'\s(M?\d{2})/(?:M?\d{2}|//)(?:\s|$)')


def temperature_c(raw: str, *, use_tenths: bool) -> Decimal:
    if use_tenths:
        match = T_GROUP.search(raw)
        if match:
            return Decimal(match[2]) / 10 * (-1 if match[1] == '1' else 1)
    match = BODY_TEMP.search(raw.split(' RMK ')[0])
    if not match:
        raise ValueError('routine temperature group missing')
    return Decimal(match[1].replace('M', '-'))


def local_day_window(day: date, zone: str) -> tuple[datetime, datetime]:
    tz = ZoneInfo(zone)
    return (datetime.combine(day, time(), tz).astimezone(timezone.utc),
            datetime.combine(day+timedelta(days=1), time(), tz).astimezone(timezone.utc))


def iem_url(station: str, day: date, zone: str) -> str:
    start, end = local_day_window(day, zone)
    # Request surrounding UTC dates; exact DST-aware day is filtered below.
    stop = end.date()+timedelta(days=1)
    station = station[1:] if station.startswith('K') and len(station) == 4 else station
    return 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?' + urlencode({
        'station': station, 'data': ['metar'], 'year1':start.year, 'month1':start.month,
        'day1':start.day, 'year2':stop.year, 'month2':stop.month, 'day2':stop.day,
        'tz':'Etc/UTC', 'format':'onlycomma', 'latlon':'no', 'elev':'no',
        'missing':'M', 'trace':'T', 'direct':'no', 'report_type':[3,4]}, doseq=True)


def parse_iem(text: str, station: str) -> list[dict]:
    rows = []
    for row in csv.DictReader(line for line in text.splitlines() if not line.startswith('#')):
        raw = row.get('metar', '')
        if raw in ('', 'M'):
            continue
        tokens = raw.split()
        actual = tokens[1] if tokens[0] in ('METAR', 'SPECI') else tokens[0]
        if actual != station:
            raise ValueError('IEM report station differs from resolver')
        stamp = datetime.fromisoformat(row['valid']).replace(tzinfo=timezone.utc)
        rows.append({'observed_at':stamp, 'raw':raw, 'report_type':'SPECI' if tokens[0]=='SPECI' else 'METAR',
                     'source':'iem-asos-report-types-3-4'})
    return rows


def daily_label(rows: list[dict], *, station: str, day: date, zone: str, unit: str,
                acquired_at: datetime) -> dict:
    if unit not in ('C','F'):
        raise ValueError('unsupported market unit')
    start, end = local_day_window(day, zone)
    accepted = {}
    rejected = 0
    for row in rows:
        if row.get('report_type') not in ('METAR','SPECI') or row.get('source') not in {'iem-asos-report-types-3-4', 'aviationweather-metar'}:
            rejected += 1
            continue
        tokens = row['raw'].split()
        actual = tokens[1] if tokens and tokens[0] in ('METAR', 'SPECI') else tokens[0] if tokens else ''
        if actual != station:
            raise ValueError('observation station mismatch')
        stamp = row['observed_at']
        if stamp.tzinfo is None:
            raise ValueError('observation timestamp must be aware')
        if start <= stamp < end:
            try:
                value = temperature_c(row['raw'], use_tenths=unit=='F')
            except ValueError:
                rejected += 1
                continue
            # Conflicting revisions are refused until report-publication ordering is known.
            if stamp in accepted and accepted[stamp] != value:
                raise ValueError('conflicting temperatures at identical observation time')
            accepted[stamp] = value
    ordered = sorted(accepted)
    gaps = [(b-a).total_seconds() for a,b in zip([start]+ordered, ordered+[end])]
    max_gap = max(gaps)
    complete = acquired_at >= end and len(accepted) >= 18 and max_gap <= 7200
    maximum = max(accepted.values()) if accepted else None
    display = maximum if unit=='C' or maximum is None else maximum*9/5+32
    return {'station':station,'target_date':day.isoformat(),'timezone':zone,'unit':unit,
            'window_start':start.isoformat(),'window_end':end.isoformat(),
            'observations':len(accepted),'rejected_rows':rejected,'max_gap_seconds':max_gap,
            'maximum_c':str(maximum) if maximum is not None else None,
            'label':str(display.quantize(Decimal('1'),rounding=ROUND_HALF_UP)) if display is not None else None,
            'status':'label_available' if complete else 'insufficient_data',
            'historical_publication_times_available':False, 'acquired_at':acquired_at.isoformat()}


def collect_label(event: dict, cache) -> dict:
    identity = parse_resolver_identity(event.get('resolutionSource'), event.get('description'))
    if not identity.supported:
        return {'event_id':event.get('event_id'),'status':'unsupported_resolver','reason':identity.reason}
    day = date.fromisoformat(event['target_date'])
    url = iem_url(identity.station, day, event['timezone'])
    if event.get('source', 'iem') == 'aviationweather':
        url = 'https://aviationweather.gov/api/data/metar?' + urlencode({'ids':identity.station, 'format':'json', 'hours':48})
        rows = parse_aviation(cache.get_json(url), identity.station)
    else:
        rows = parse_iem(cache.get_text(url), identity.station)
    label = daily_label(rows, station=identity.station, day=day,
                        zone=event['timezone'], unit=event['unit'], acquired_at=datetime.fromisoformat(cache.provenance(url)['acquired_at']))
    label.update(event_id=event.get('event_id'), source_url=url, provenance=cache.provenance(url), resolver=identity.as_dict())
    # Optional winner validation requires genuine declared winner and market bounds.
    label['winner_match'] = None
    if label['status']=='label_available' and event.get('declared_winner') in ('YES','NO'):
        value = Decimal(label['label'])
        inside = ((event.get('lower') is None or value >= Decimal(str(event['lower']))) and
                  (event.get('upper') is None or value <= Decimal(str(event['upper']))))
        label['winner_match'] = inside == (event['declared_winner']=='YES')
    return label


def parse_aviation(payload: list[dict], station: str) -> list[dict]:
    rows = []
    for report in payload:
        if report.get('icaoId') != station:
            raise ValueError('AviationWeather station mismatch')
        if report.get('metarType') not in ('METAR', 'SPECI'):
            continue
        rows.append({'observed_at':datetime.fromtimestamp(report['obsTime'],timezone.utc),
                     'raw':report['rawOb'], 'report_type':report['metarType'],
                     'source':'aviationweather-metar'})
    return rows


def validate_manifest(events: list[dict], *, max_events: int = 100) -> None:
    if not isinstance(events, list) or not 0 < len(events) <= max_events:
        raise ValueError('manifest must be a bounded nonempty list')
    seen = set()
    for event in events:
        if not isinstance(event, dict) or not all(event.get(k) for k in ('event_id','target_date','timezone','unit')):
            raise ValueError('manifest missing required fields')
        if event['event_id'] in seen:
            raise ValueError('duplicate event id')
        seen.add(event['event_id'])
        date.fromisoformat(event['target_date'])
        ZoneInfo(event['timezone'])
        if event.get('declared_winner') is not None and (event['declared_winner'] not in ('YES','NO') or event.get('lower') is None and event.get('upper') is None):
            raise ValueError('declared winner requires explicit rung bounds')
        if event['unit'] not in ('C','F') or event.get('source','iem') not in ('iem','aviationweather'):
            raise ValueError('unsupported unit or source')
        identity = parse_resolver_identity(event.get('resolutionSource'), event.get('description'))
        if not identity.supported:
            raise ValueError('unsupported resolver')
