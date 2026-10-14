"""Bounded public-only Gamma/CLOB/METAR capture, never an executable signal.

Aviation METAR has no proved publication time for the exact resolver's Hourly
Temp column. This probe records raw evidence, but intentionally cannot fill
that field or supply a remaining-day model.
"""
import argparse
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from .late_observation import _audit_path, evaluate, late_contract
from .paper_weather import CITY_STATIONS
from .resolver_registry import registry_entry_from_gamma

# Bound decoded JSON bytes, not merely Content-Length (which servers may omit/lie about).
GAMMA_LIMIT = 1_000_000
METAR_LIMIT = 256_000
BOOK_LIMIT = 512_000


def _public_json(session, url: str, *, limit: int, params: dict | None = None):
    with session.get(url, params=params, timeout=(5, 15), stream=True,
                     allow_redirects=False) as response:
        try:
            if 300 <= response.status_code < 400:
                raise ValueError('public API redirect refused')
            response.raise_for_status()
            length = response.headers.get('Content-Length')
            if length is not None and (not length.isdecimal() or int(length) > limit):
                raise ValueError('public API response exceeds byte limit')
            chunks = bytearray()
            for chunk in response.iter_content(chunk_size=16_384):
                if len(chunks) + len(chunk) > limit:
                    raise ValueError('public API response exceeds byte limit')
                chunks.extend(chunk)
            return json.loads(chunks), response.url
        except requests.RequestException as exc:
            raise ValueError('public API response interrupted') from exc


def _normalize_book(raw: object, *, condition_id: str, token_id: str, fetched_at: str) -> dict:
    """Convert native CLOB book fields without replacing the preserved raw response."""
    if not isinstance(raw, dict) or raw.get('market') != condition_id or raw.get('asset_id') != token_id:
        raise ValueError('native CLOB book identity mismatch')
    milliseconds = raw.get('timestamp')
    if not isinstance(milliseconds, str) or not re.fullmatch(r'\d{13}', milliseconds):
        raise ValueError('native CLOB millisecond timestamp missing')
    try:
        changed = (datetime(1970, 1, 1, tzinfo=timezone.utc)
                   + timedelta(milliseconds=int(milliseconds))).isoformat()
    except OverflowError as exc:
        raise ValueError('native CLOB timestamp out of range') from exc
    if not isinstance(raw.get('bids'), list) or not isinstance(raw.get('asks'), list):
        raise ValueError('native CLOB bid/ask arrays required')
    return dict(condition_id=condition_id, token_id=token_id, timestamp=changed,
                fetched_at=fetched_at, bids=raw.get('bids'), asks=raw.get('asks'),
                tick_size=raw.get('tick_size'), min_order_size=raw.get('min_order_size'))


def _event_url(slug: str) -> str:
    if not isinstance(slug, str) or not re.fullmatch(r'[a-z0-9]+(?:-[a-z0-9]+)*', slug):
        raise ValueError('event slug must be a bare lowercase hyphenated identifier')
    return 'https://gamma-api.polymarket.com/events/slug/' + slug


def _select_market(markets: object, market_id: str) -> dict:
    if not isinstance(markets, list) or not market_id or not str(market_id).isdigit():
        raise ValueError('event markets and exact numeric market ID required')
    matches = [m for m in markets if isinstance(m, dict) and str(m.get('id')) == str(market_id)]
    if (len(matches) != 1 or matches[0].get('active') is not True
            or matches[0].get('closed') is not False or matches[0].get('acceptingOrders') is not True):
        raise ValueError('exact unique open market not available')
    return matches[0]


def _save_capture(path: Path, capture: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x') as f:
        json.dump(capture, f, indent=2, sort_keys=True)
        f.write('\n')


def main():
    p = argparse.ArgumentParser(description='paper-only public evidence probe; no trading')
    p.add_argument('--output', required=True, type=Path)
    p.add_argument('--slug', required=True, help='exact public Gamma event slug')
    p.add_argument('--market-id', required=True, help='exact numeric market ID within event')
    a = p.parse_args()
    a.output = _audit_path(a.output)
    if a.output.exists():
        p.error('output exists')
    event_url = _event_url(a.slug)
    if not a.market_id.isascii() or not a.market_id.isdecimal() or len(a.market_id) > 32:
        p.error('exact bounded numeric market ID required')
    session = requests.Session()
    session.headers['User-Agent'] = 'paper-late-observation-research/1.0'
    evidence: dict[str, dict] = {}
    evidence['gamma'] = dict(url=event_url, fetched_at=datetime.now(timezone.utc).isoformat())
    def reject(reason: str, *, no_response: bool = False) -> None:
        # No raw provider data, exception text, or replayable input on invalid records.
        evidence.clear()
        if not no_response:
            evidence['gamma'] = dict(url=event_url, fetched_at=gamma_at)
        result = dict(status='rejected', paper_only=True, market_id=a.market_id,
                      reason=reason, decision_at=gamma_at)
        _save_capture(a.output, dict(provenance=('Gamma request failed; no provider response'
                                               if no_response else 'public Gamma only; market not verified'),
                                     evidence=evidence, result=result))
        print(json.dumps(result, sort_keys=True))
    gamma_at = evidence['gamma']['fetched_at']
    try:
        gamma, _ = _public_json(session, event_url, limit=GAMMA_LIMIT)
    except requests.RequestException:
        reject('Gamma request failed without a response', no_response=True)
        return
    except (ValueError, TypeError, UnicodeError):
        reject('malformed, redirected or oversized Gamma response')
        return
    evidence['gamma']['fetched_at'] = datetime.now(timezone.utc).isoformat()
    try:
        if not isinstance(gamma, dict) or not isinstance(gamma.get('markets'), list):
            reject('malformed Gamma event')
            return
        market = _select_market(gamma['markets'], a.market_id)
    except (ValueError, TypeError, KeyError):
        reject('exact unique open market not available')
        return
    try:
        entry = registry_entry_from_gamma(market, fetched_at=evidence['gamma']['fetched_at'])
        end_date = market['endDate']
        if not isinstance(end_date, str):
            raise ValueError('endDate must be a timezone-aware string')
        end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        if end.utcoffset() is None:
            raise ValueError('endDate must be timezone-aware')
        contract = late_contract(entry.question, end_date=end)
        if contract is None or CITY_STATIONS.get(contract.city) != entry.resolver.station:
            raise ValueError('unmatched resolver')
        station = entry.resolver.station
        if not station or not re.fullmatch(r'[A-Z]{4}', station):
            raise ValueError('unsupported station')
        if entry.outcomes != ('Yes', 'No'):
            raise ValueError('unsupported outcomes')
        token = entry.token_ids[0]
    except (ValueError, TypeError, KeyError, IndexError, AttributeError):
        reject('invalid open-market metadata or resolver')
        return
    evidence['gamma']['data'] = gamma
    try:
        metar, metar_url = _public_json(session, 'https://aviationweather.gov/api/data/metar',
                                       params={'ids': station, 'format': 'json', 'hours': 6}, limit=METAR_LIMIT)
        # METAR is retained as ancillary evidence, never as resolver Hourly Temp.
        if not isinstance(metar, list) or any(not isinstance(row, dict) for row in metar):
            raise ValueError('METAR reports must be an array of objects')
        evidence['metar'] = dict(url=metar_url, fetched_at=datetime.now(timezone.utc).isoformat(), data=metar)
        book, book_url = _public_json(session, 'https://clob.polymarket.com/book',
                                     params={'token_id': token}, limit=BOOK_LIMIT)
        evidence['book'] = dict(url=book_url, fetched_at=datetime.now(timezone.utc).isoformat(), data=book)
        normalized_book = _normalize_book(book, condition_id=entry.condition_id,
                                          token_id=token, fetched_at=evidence['book']['fetched_at'])
    except requests.RequestException:
        reject('public ancillary request failed without a response')
        return
    except (ValueError, TypeError, UnicodeError):
        reject('malformed or oversized public ancillary response')
        return
    at = datetime.now(timezone.utc).isoformat()
    payload = dict(decision_at=at, market=market, market_fetched_at=evidence['gamma']['fetched_at'],
                   market_id=entry.market_id, condition_id=entry.condition_id, token_id=token,
                   outcome='Yes', rule_sha256=entry.raw_sha256,
                   rule_kind='hourly_temp_whole_degree', station_id=station,
                   target_date=contract.target_date, target_end_date=market['endDate'],
                   primary_source_available=False, market_active=market['active'],
                   market_closed=market['closed'], observations=[], remaining_day_model=None,
                   book=normalized_book)
    result = evaluate(payload)
    _save_capture(a.output, dict(provenance='public read-only; METAR not resolver Hourly Temp; no publication timestamp or model',
                                 evidence=evidence, input=payload, result=result))
    print(json.dumps(dict(market_id=entry.market_id, station=station,
                          metar_reports=len(evidence['metar']['data']),
                          book_asks=len(evidence['book']['data'].get('asks', [])),
                          book_bids=len(evidence['book']['data'].get('bids', [])),
                          result=result), default=str))


if __name__ == '__main__':
    main()
