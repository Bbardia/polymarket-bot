"""Offline, deterministic paper-only campaign contract tests."""
import json
import hashlib
import os
import signal
import time
import tracemalloc
from urllib.error import HTTPError, URLError
from io import BytesIO
from email.message import Message
from urllib.parse import parse_qs, urlsplit
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.v3.awc_campaign import Campaign, Config, validate_event, report, _canonical, _label, public_fetch
from src.v3.awc_metar_observation import _REQUIRED_RULES
from src.v3 import awc_campaign

@pytest.fixture(autouse=True)
def reviewed_test_root(tmp_path, monkeypatch):
    monkeypatch.setattr('src.v3.awc_campaign.APPROVED_OUTPUT_ROOT', tmp_path)


RULES = '\n'.join(_REQUIRED_RULES)
SLUG = 'highest-temperature-in-karachi-on-october-28-2026'
SOURCE = 'https://aviationweather.gov/api/data/metar?ids=OPKC&format=raw&date=2026-10-28T22:00:00Z&hours=30'
NOW = datetime(2026, 10, 27, 12, tzinfo=timezone.utc)


def fixture():
    config = Config(slug=SLUG, event_id='1257298', target_date='2026-10-28',
                    station='OPKC', zone='Asia/Karachi', latitude=24.9065,
                    longitude=67.1608, market_id='6239073', start_at=NOW.isoformat(),
                    rule_sha256=hashlib.sha256(RULES.encode()).hexdigest())
    market = {'id': '6239073', 'question': 'Will the highest temperature in Karachi be 32°C or below on October 28?',
              'conditionId': '0x' + 'a'*64, 'clobTokenIds': json.dumps(['123', '456']),
              'outcomes': '["Yes", "No"]', 'description': RULES,
              'resolutionSource': SOURCE, 'closed': False}
    event = {'id': '1257298', 'slug': SLUG, 'eventDate': '2026-10-28',
             'description': RULES, 'resolutionSource': SOURCE, 'markets': [market]}
    return config, event

def daily_fixture(day='2026-10-28'):
    """Offline 11-rung shape: lower tail, nine degrees, upper tail."""
    base, event = fixture()
    edge = 32 if day == '2026-10-28' else 31
    event['eventDate'] = day
    event['slug'] = f'highest-temperature-in-karachi-on-october-{int(day[-2:])}-2026'
    event['resolutionSource'] = SOURCE.replace('2026-10-28', day)
    questions = ([f'{edge}°C or below'] + [f'{n}°C' for n in range(edge + 1, edge + 10)] +
                 [f'{edge + 10}°C or higher'])
    markets = []
    for i, bracket in enumerate(questions):
        market = {**event['markets'][0], 'id': str(6239073 + i),
                  'question': f'Will the highest temperature in Karachi be {bracket} on October {int(day[-2:])}?',
                  'conditionId': '0x' + f'{i + 1:064x}',
                  'clobTokenIds': json.dumps([str(100 + i * 2), str(101 + i * 2)]),
                  'resolutionSource': event['resolutionSource']}
        markets.append(market)
    event['markets'] = markets
    selected = markets[5]
    config = Config(**{**base.__dict__, 'slug': event['slug'], 'target_date': day,
                       'market_id': selected['id']})
    return config, event


def _book_for(url, event, *, bids=None, asks=None, at=NOW):
    token = parse_qs(urlsplit(url).query)['token_id'][0]
    market = next((m for m in event['markets'] if token in json.loads(m['clobTokenIds'])), event['markets'][0])
    return json.dumps({'market': market['conditionId'], 'asset_id': token,
                       'timestamp': str(int(at.timestamp() * 1000)), 'bids': bids if bids is not None else [],
                       'asks': asks if asks is not None else []}).encode(), {'Status': 200}


def test_two_exact_books_and_strict_seven_get_ceiling(tmp_path):
    config, event = daily_fixture()
    calls = []
    def fetch(url, limit):
        calls.append((url, limit))
        if 'gamma-api' in url: return json.dumps(event).encode(), {}
        if 'open-meteo' in url: return b'{"hourly":{}}', {}
        if '/book?' in url:
            return _book_for(url, event, bids=[{'price': '0.2', 'size': '50'}],
                             asks=[{'price': '0.3', 'size': '20'}])
        if 'aviationweather' in url: return b'', {'Status': 204}
        raise AssertionError(url)
    result = awc_campaign.DailyCampaign(tmp_path, config.target_date, config.start_at,
                                        fetch=fetch, clock=lambda: NOW).once()
    assert result['status'] == 'captured_partial'
    assert len(calls) == 5 and len(calls) <= 7
    assert [parse_qs(urlsplit(u).query)['token_id'][0] for u, _ in calls if '/book?' in u] == ['110', '111']
    row = json.loads((tmp_path / '000000.json').read_text())
    assert row['book_evidence'] == {'Yes': 'verified_book', 'No': 'verified_book'}
    assert row['captures']['book_no']['body'] != row['captures']['book']['body']
    assert row['captures']['book_no']['sha256'] == hashlib.sha256(row['captures']['book_no']['body'].encode()).hexdigest()
    assert row['captures']['book_no']['first_available_at'] == NOW.isoformat()
    assert sum(awc_campaign.LIMITS.values()) * 6 < awc_campaign.MAX_TAPE_ROW_BYTES


@pytest.mark.parametrize('bad', [
    {'asset_id': '999'}, {'asset_id': '123'}, {'market': '0x' + 'b'*64},
    {'timestamp': '179310240000'}, {'timestamp': '1793102400001.0'},
    {'timestamp': '1793102406000'}, {'timestamp': '1793000000000'},
])
def test_no_book_identity_and_time_fail_closed(tmp_path, bad):
    config, event = fixture()
    def fetch(url, limit):
        if 'gamma-api' in url: return json.dumps(event).encode(), {}
        if 'open-meteo' in url: return b'{"hourly":{}}', {}
        if '/book?' in url:
            body, headers = _book_for(url, event)
            if '456' in url: return json.dumps({**json.loads(body), **bad}).encode(), headers
            return body, headers
        raise AssertionError('should stop at invalid No book')
    with pytest.raises(ValueError, match='book identity/timestamp'):
        Campaign(tmp_path, config, fetch=fetch, clock=lambda: NOW).once()
    assert not list(tmp_path.glob('*.json'))


@pytest.mark.parametrize('level', [{'price': 'NaN', 'size': '1'},
                                    {'price': '1.1', 'size': '1'},
                                    {'price': '0.5', 'size': '0'},
                                    {'price': '0.5', 'size': 'Infinity'},
                                    {'price': '0.5'}])
def test_malformed_no_ask_cannot_claim_book(tmp_path, level):
    config, event = fixture()
    def fetch(url, limit):
        if 'gamma-api' in url: return json.dumps(event).encode(), {}
        if 'open-meteo' in url: return b'{"hourly":{}}', {}
        if '/book?' in url: return _book_for(url, event, asks=[level] if '456' in url else [])
        raise AssertionError(url)
    with pytest.raises(ValueError, match='book level'):
        Campaign(tmp_path, config, fetch=fetch, clock=lambda: NOW).once()
    assert not list(tmp_path.glob('*.json'))


@pytest.mark.parametrize('no_result,code', [
    ((b'{"error":"No orderbook exists for the requested token id"}', {'Status': 404}), 'no_book'),
    (TimeoutError('timed out'), 'timeout'),
])
def test_no_book_unavailable_retains_upstream_without_executable_claim(tmp_path, no_result, code):
    config, event = fixture()
    calls = []
    def fetch(url, limit):
        calls.append(url)
        if 'gamma-api' in url: return json.dumps(event).encode(), {}
        if 'open-meteo' in url: return b'{"hourly":{}}', {}
        if '/book?' in url:
            if '456' in url:
                if isinstance(no_result, Exception): raise no_result
                return no_result
            return _book_for(url, event)
        if 'aviationweather' in url: return b'', {'Status': 200}
        raise AssertionError(url)
    assert Campaign(tmp_path, config, fetch=fetch, clock=lambda: NOW).once()['status'] == 'captured'
    assert len(calls) == 5
    row = json.loads((tmp_path / '000000.json').read_text())
    assert row['book_evidence'] == {'Yes': 'verified_book', 'No': 'unverified_' + code}
    assert row['captures']['book_no']['failure_code'] == code
    assert row['captures']['book']['status'] == 200
    assert row['label']['status'] == 'unverified' and report(tmp_path)['resolved_event_days'] == 0
    if code == 'no_book':
        assert row['captures']['book_no']['status'] == 404
        assert row['captures']['book_no']['body'] == no_result[0].decode()
    else:
        assert 'body' not in row['captures']['book_no']


@pytest.mark.parametrize('response', [(b'{"error":"unknown token"}', {'Status': 404}),
                                      (b'{"error":"No orderbook exists for the requested token id"}', {'Status': 200}),
                                      (b'{"error":"No orderbook exists for the requested token id"}', {'Status': 503})])
def test_unknown_no_token_or_unexpected_status_fails_closed(tmp_path, response):
    config, event = fixture()
    def fetch(url, limit):
        if 'gamma-api' in url: return json.dumps(event).encode(), {}
        if 'open-meteo' in url: return b'{"hourly":{}}', {}
        if '/book?' in url: return response if '456' in url else _book_for(url, event)
        raise AssertionError(url)
    with pytest.raises(ValueError):
        Campaign(tmp_path, config, fetch=fetch, clock=lambda: NOW).once()
    assert not list(tmp_path.glob('*.json'))


def test_no_ask_book_keeps_raw_snapshot_but_cannot_claim_executable_no(tmp_path):
    config, event = fixture()
    def fetch(url, limit):
        if 'gamma-api' in url: return json.dumps(event).encode(), {}
        if 'open-meteo' in url: return b'{"hourly":{}}', {}
        if '/book?' in url: return _book_for(url, event, bids=[])
        if 'aviationweather' in url: return b'', {'Status': 204}
        raise AssertionError(url)
    Campaign(tmp_path, config, fetch=fetch, clock=lambda: NOW).once()
    row = json.loads((tmp_path / '000000.json').read_text())
    assert row['book_evidence']['No'] == 'unverified_no_ask'
    assert row['captures']['book_no']['status'] == 200
    assert json.loads(row['captures']['book_no']['body'])['asks'] == []


def test_public_fetch_bounded_clob_404_preserves_body_and_rejects_other_http(monkeypatch):
    calls = []
    def open_response(request, timeout):
        calls.append(request.full_url)
        headers = Message()
        headers['Date'] = 'test'
        raise HTTPError(request.full_url, 404, 'Not Found', headers,
                        BytesIO(b'{"error":"No orderbook exists for the requested token id"}'))
    monkeypatch.setattr(awc_campaign, 'build_opener', lambda *args: type('Opener', (), {'open': staticmethod(open_response)})())
    body, headers = public_fetch('https://clob.polymarket.com/book?token_id=456', 100)
    assert headers['Status'] == 404 and body.startswith(b'{"error"') and len(calls) == 1
    with pytest.raises(ValueError):
        public_fetch('https://gamma-api.polymarket.com/events/slug/' + SLUG, 100)
    with pytest.raises(ValueError, match='byte limit'):
        public_fetch('https://clob.polymarket.com/book?token_id=456', 5)


def test_identity_rejects_modified_rule_station_source_and_duplicate():
    config, event = fixture()
    assert validate_event(config, event)['token_id'] == '123'
    for change in ({'description': RULES.replace('SPECI', 'SPECX')},
                   {'resolutionSource': SOURCE.replace('OPKC', 'KJFK')},
                   {'slug': SLUG + '-other'}, {'eventDate': '2026-10-29'},
                   {'markets': event['markets'] * 2}):
        with pytest.raises(ValueError):
            validate_event(config, {**event, **change})
    event['markets'][0]['description'] = 'different'
    with pytest.raises(ValueError):
        validate_event(config, event)


def test_coordinates_and_rule_hash_cannot_drift():
    config, event = fixture()
    with pytest.raises(ValueError):
        Config(**{**config.__dict__, 'latitude': 1.0})
    changed = {**event, 'description': RULES + '\nchanged'}
    changed['markets'] = [{**event['markets'][0], 'description': changed['description']}]
    with pytest.raises(ValueError):
        validate_event(config, changed)


def test_label_requires_explicit_final_settlement_not_just_price_or_close():
    market = {'closed': True, 'outcomePrices': '["1", "0"]'}
    assert _label(market)['status'] == 'unverified'
    market['resolvedAt'] = '2026-10-29T13:00:00Z'
    assert _label(market, target_date='2026-10-28', fetched_at='2026-10-30T00:00:00Z')['winner'] == 'Yes'
    assert _label(market, target_date='2026-10-28', fetched_at=NOW.isoformat())['status'] == 'unverified'
    market['outcomePrices'] = '["0.999", "0.001"]'
    assert _label(market)['status'] == 'unverified'


def test_cycle_is_bounded_append_only_and_stops_on_deadline(tmp_path):
    config, event = fixture()
    calls = []
    def fetch(url, limit):
        calls.append(url)
        if 'gamma-api' in url:
            return json.dumps(event).encode(), {'Date': 'Tue, 27 Oct 2026 12:00:00 GMT'}
        if 'open-meteo' in url:
            return b'{"hourly":{"time":[],"temperature_2m":[]}}', {}
        if 'clob.polymarket.com' in url:
            return _book_for(url, event)
        return b'METAR OPKC 280600Z 00000KT 9999 FEW020 32/23 Q1010 RMK T03200230\n', {}
    campaign = Campaign(tmp_path, config, fetch=fetch, clock=lambda: NOW)
    result = campaign.once()
    assert result['status'] == 'captured' and len(calls) == 5
    files = list(tmp_path.glob('*.json'))
    assert len(files) == 1
    saved = json.loads(files[0].read_text())
    assert saved['captures']['awc']['body'].startswith('METAR OPKC')
    assert saved['captures']['forecast']['issue_at'] is None
    assert saved['captures']['forecast']['first_available_at'] == NOW.isoformat()
    assert saved['observation']['status'] == 'future_day'
    assert saved['label']['status'] == 'unverified'
    assert campaign.once()['status'] == 'already_captured'
    assert len(calls) == 5 and len(list(tmp_path.glob('*.json'))) == 1
    assert report(tmp_path)['cycles'] == 1
    expired = Config(**{**config.__dict__, 'start_at': '2026-09-01T00:00:00+00:00'})
    assert Campaign(tmp_path / 'expired', expired, fetch=fetch, clock=lambda: NOW).once()['status'] == 'deadline'


def test_failed_fetch_does_not_write_partial_and_oversize_is_rejected(tmp_path):
    config, event = fixture()
    def fail(url, limit):
        if 'gamma-api' in url:
            return json.dumps(event).encode(), {}
        raise ValueError('offline')
    with pytest.raises(ValueError):
        Campaign(tmp_path, config, fetch=fail, clock=lambda: NOW).once()
    assert not list(tmp_path.glob('*.json'))
    with pytest.raises(ValueError):
        Campaign(tmp_path, config, fetch=lambda url, limit: b'x' * (limit + 1), clock=lambda: NOW).once()
    assert not list(tmp_path.glob('*.json'))


def test_report_refuses_tampered_chain(tmp_path):
    config, event = fixture()
    path = tmp_path / '000000.json'
    path.write_bytes(_canonical({'sequence': 0, 'previous_sha256': None, 'status': 'captured', 'event_id': config.event_id,
                                 'target_date': config.target_date, 'captured_at': NOW.isoformat(),
                                 'label': {'status': 'unverified'}}))
    assert report(tmp_path)['cycles'] == 1
    path.write_text(path.read_text().replace('captured', 'changed'))
    (tmp_path / '000001.json').write_text(json.dumps({'sequence': 1, 'previous_sha256': 'wrong'}))
    with pytest.raises(ValueError):
        report(tmp_path)


def test_claimed_status_without_raw_resolution_cannot_trigger_cap(tmp_path):
    config, _ = fixture()
    previous = None
    for n in range(30):
        row = {'sequence': n, 'previous_sha256': previous,
               'event_id': str(n + 1), 'target_date': '2026-10-' + str(n + 1).zfill(2),
               'campaign_start_at': config.start_at, 'station': config.station,
               'captured_at': NOW.isoformat(), 'label': {'status': 'venue_corroborated_final_binary'}}
        body = _canonical(row)
        (tmp_path / f'{n:06d}.json').write_bytes(body)
        previous = hashlib.sha256(body).hexdigest()
    assert report(tmp_path)['resolved_event_days'] == 0


def test_public_fetch_rejects_non_allowlisted_hosts_without_network():
    from src.v3.awc_campaign import public_fetch
    for url in ('https://gamma-api.polymarket.com.evil.test/events/slug/x',
                'http://gamma-api.polymarket.com/events/slug/x',
                'https://user:pass@gamma-api.polymarket.com/events/slug/x'):
        with pytest.raises(ValueError):
            public_fetch(url, 100)


def test_native_clob_token_ids_can_be_full_uint256_width():
    config, event = fixture()
    token = '17260578725842273431782519405472345891170315157094821855212554575505590763842'
    event['markets'][0]['clobTokenIds'] = json.dumps([token, '456'])
    assert validate_event(config, event)['token_id'] == token


def test_real_gamma_closed_time_and_auto_resolution_give_exact_payout_label():
    event = {'closed': True, 'automaticallyResolved': True, 'closedTime': '2026-10-29T22:29:47Z'}
    market = {'closed': True, 'automaticallyResolved': True, 'closedTime': '2026-10-29 20:10:50+00',
              'outcomePrices': '["0", "1"]', 'umaResolutionStatuses': '["proposed", "proposed"]'}
    label = _label(market, event=event, target_date='2026-10-28', fetched_at='2026-10-30T00:00:00Z')
    assert label['status'] == 'official_gamma_binary' and label['winner'] == 'No'
    assert _label(market, event={**event, 'automaticallyResolved': False},
                  target_date='2026-10-28', fetched_at='2026-10-30T00:00:00Z')['status'] == 'unverified'

@pytest.mark.parametrize('prices', ('[true,false]', '[false,true]', [True, False], [False, True],
                                          '[1.0,0.0]', [1.0, 0.0]))
def test_gamma_non_integer_binary_payout_is_not_accepted(prices):
    market = {'closed': True, 'resolvedAt': '2026-10-29T13:00:00Z', 'outcomePrices': prices}
    assert _label(market)['status'] == 'unverified'

@pytest.mark.parametrize('prices,winner', [('["1","0"]', 'Yes'), ('["0","1"]', 'No'),
                                               ([1, 0], 'Yes'), ([0, 1], 'No')])
def test_gamma_documented_binary_numeric_representations_remain_accepted(prices, winner):
    market = {'closed': True, 'resolvedAt': '2026-10-29T13:00:00Z', 'outcomePrices': prices}
    assert _label(market)['winner'] == winner


@pytest.mark.parametrize('awc_result,reason', [((b'', {'Status': 204, 'Date': 'Tue, 27 Oct 2026 12:00:00 GMT'}), 'no_data'),
                                             (TimeoutError('timed out'), 'timeout')])
def test_awc_unavailable_preserves_verified_upstream_evidence_without_label(tmp_path, awc_result, reason):
    config, event = fixture()
    # Even terminal Gamma metadata cannot make an absent AWC observation a settlement.
    market = event['markets'][0]
    market.update(closed=True, resolvedAt='2026-10-27T11:30:00Z', outcomePrices='["1","0"]')
    calls = []
    def fetch(url, limit):
        calls.append(url)
        if 'gamma-api' in url:
            return json.dumps(event).encode(), {'Status': 200}
        if 'open-meteo' in url:
            return b'{"hourly":{"time":[],"temperature_2m":[]}}', {'Status': 200}
        if 'clob.polymarket.com' in url:
            return _book_for(url, event)
        if isinstance(awc_result, Exception):
            raise awc_result
        return awc_result
    campaign = Campaign(tmp_path, config, fetch=fetch, clock=lambda: NOW)
    result = campaign.once()
    assert result['status'] == 'captured_partial' and result['label'] == 'unverified'
    assert len(calls) == 5
    saved = json.loads((tmp_path / '000000.json').read_text())
    assert saved['status'] == 'captured_partial'
    assert set(saved['captures']) == {'gamma', 'forecast', 'book', 'book_no', 'awc'}
    for source in ('gamma', 'forecast', 'book', 'book_no'):
        capture = saved['captures'][source]
        assert capture['status'] == 200 and capture['body'] and len(capture['sha256']) == 64
        assert capture['first_available_at'] == NOW.isoformat()
    assert saved['captures']['forecast']['issue_at'] is None
    awc = saved['captures']['awc']
    assert awc['url'] == SOURCE and awc['failure_code'] == reason
    if reason == 'no_data':
        assert awc['status'] == 204 and awc['body'] == '' and len(awc['sha256']) == 64
        assert awc['response_date'] == 'Tue, 27 Oct 2026 12:00:00 GMT'
    else:
        assert awc['status'] == 'timeout' and 'body' not in awc and 'sha256' not in awc
    assert saved['observation'] == {'status': 'unavailable', 'reason': reason,
                                     'correction_cutoff_proven': False, 'wu_fallback_proven': False}
    assert saved['label']['status'] == 'unverified' and 'winner' not in saved['label']
    assert campaign.once()['status'] == 'already_captured' and len(calls) == 5
    assert report(tmp_path)['pending_labels'] == 1
    assert report(tmp_path)['resolved_event_days'] == 0


def test_invalid_gamma_identity_still_blocks_partial_capture(tmp_path):
    config, event = fixture()
    event['slug'] = SLUG + '-other'
    calls = []
    def fetch(url, limit):
        calls.append(url)
        return json.dumps(event).encode(), {'Status': 200}
    with pytest.raises(ValueError, match='identity'):
        Campaign(tmp_path, config, fetch=fetch, clock=lambda: NOW).once()
    assert len(calls) == 1 and not list(tmp_path.glob('*.json'))


@pytest.mark.parametrize('failure', [ValueError('public request failed'),
                                          (b'unexpected', {'Status': 204}),
                                          (b'', {'Status': 503})])
def test_only_awc_no_data_or_timeout_can_write_partial(tmp_path, failure):
    config, event = fixture()
    def fetch(url, limit):
        if 'gamma-api' in url:
            return json.dumps(event).encode(), {}
        if 'open-meteo' in url:
            return b'{"hourly":{}}', {}
        if 'clob.polymarket.com' in url:
            return _book_for(url, event)
        if isinstance(failure, Exception):
            raise failure
        return failure
    with pytest.raises(ValueError):
        Campaign(tmp_path, config, fetch=fetch, clock=lambda: NOW).once()
    assert not list(tmp_path.glob('*.json'))


def test_upstream_timeout_does_not_write_partial(tmp_path):
    config, _ = fixture()
    with pytest.raises(TimeoutError):
        Campaign(tmp_path, config, fetch=lambda *_: (_ for _ in ()).throw(TimeoutError()),
                 clock=lambda: NOW).once()
    assert not list(tmp_path.glob('*.json'))


def test_public_fetch_distinguishes_awc_204_and_timeout_without_retry(monkeypatch):
    class Response:
        status = 204
        headers = {'Date': 'Tue, 27 Oct 2026 12:00:00 GMT'}
        def __enter__(self): return self
        def __exit__(self, *args): return None
        def read(self, size): return b''
    class Opener:
        def __init__(self): self.calls = 0
        def open(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 2:
                raise URLError(TimeoutError('socket timed out'))
            return Response()
    opener = Opener()
    monkeypatch.setattr('src.v3.awc_campaign.build_opener', lambda *args: opener)
    body, headers = public_fetch(SOURCE, 256_000)
    assert body == b'' and headers['Status'] == 204
    with pytest.raises(TimeoutError):
        public_fetch(SOURCE, 256_000)
    assert opener.calls == 2


def test_public_fetch_204_other_sources_is_failure(monkeypatch):
    class Response:
        status = 204
        headers = {}
        def __enter__(self): return self
        def __exit__(self, *args): return None
        def read(self, size): return b''
    monkeypatch.setattr('src.v3.awc_campaign.build_opener', lambda *args: type('Opener', (), {'open': lambda *args, **kwargs: Response()})())
    with pytest.raises(ValueError, match='non-200'):
        public_fetch('https://gamma-api.polymarket.com/events/slug/' + SLUG, 100)

def test_unapproved_or_symlinked_directory_refused_before_fetch(tmp_path):
    config, _ = fixture()
    outside = tmp_path.parent / 'unapproved-output'
    outside.mkdir(exist_ok=True)
    (tmp_path / 'link').symlink_to(outside, target_is_directory=True)
    def no_fetch(*_):
        raise AssertionError('network before path validation')
    for directory in (outside, tmp_path / 'link', tmp_path / 'link' / 'nested',
                      tmp_path / '..' / outside.name):
        with pytest.raises(ValueError, match='output|symlink'):
            Campaign(directory, config, fetch=no_fetch, clock=lambda: NOW).once()
        with pytest.raises(ValueError, match='output|symlink'):
            report(directory)
    assert not (outside / 'nested').exists()

def test_symlinked_tape_entry_refused_without_reading_target_or_fetch(tmp_path):
    config, _ = fixture()
    target = tmp_path.parent / 'external-tape.json'
    target.write_text('private')
    (tmp_path / '000000.json').symlink_to(target)
    def no_fetch(*_):
        raise AssertionError('network before tape validation')
    with pytest.raises(ValueError, match='symlink|tape'):
        Campaign(tmp_path, config, fetch=no_fetch, clock=lambda: NOW).once()
    with pytest.raises(ValueError, match='symlink|tape'):
        report(tmp_path)
    assert target.read_text() == 'private'

def test_public_fetch_slow_trickle_is_interrupted_by_wall_deadline(monkeypatch):
    class Response:
        status = 200
        headers = {}
        def __enter__(self): return self
        def __exit__(self, *args): return None
        def read(self, size):
            time.sleep(0.06)
            return b'x'
    class Opener:
        def open(self, *args, **kwargs): return Response()
    monkeypatch.setattr('src.v3.awc_campaign.build_opener', lambda *args: Opener())
    monkeypatch.setattr('src.v3.awc_campaign.REQUEST_WALL_SECONDS', 0.12)
    started = time.monotonic()
    with pytest.raises(TimeoutError, match='timed out'):
        public_fetch(SOURCE, 10000)
    assert time.monotonic() - started < 0.6

def test_cycle_wall_deadline_interrupts_blocked_fetch_without_partial_row(tmp_path, monkeypatch):
    config, _ = fixture()
    monkeypatch.setattr('src.v3.awc_campaign.CYCLE_WALL_SECONDS', 0.12)
    def blocked(*_):
        time.sleep(0.8)
        raise AssertionError('not interrupted')
    started = time.monotonic()
    with pytest.raises(ValueError, match='cycle wall budget exhausted'):
        Campaign(tmp_path, config, fetch=blocked, clock=lambda: NOW).once()
    assert time.monotonic() - started < 0.6
    assert not list(tmp_path.glob('*.json'))

def test_symlinked_parent_within_approved_root_is_refused(tmp_path):
    config, _ = fixture()
    (tmp_path / 'real').mkdir()
    (tmp_path / 'alias').symlink_to(tmp_path / 'real', target_is_directory=True)
    with pytest.raises(ValueError, match='symlink'):
        Campaign(tmp_path / 'alias', config, fetch=lambda *_: pytest.fail('unexpected fetch'),
                 clock=lambda: NOW).once()

def test_directory_replaced_after_preflight_cannot_redirect_append(tmp_path):
    config, event = fixture()
    inside = tmp_path / 'capture'
    inside.mkdir()
    outside = tmp_path.parent / 'redirected-capture'
    outside.mkdir(exist_ok=True)
    def fetch(url, limit):
        if 'gamma-api' in url:
            inside.rename(tmp_path / 'original')
            inside.symlink_to(outside, target_is_directory=True)
            return json.dumps(event).encode(), {}
        if 'open-meteo' in url:
            return b'{"hourly":{}}', {}
        if 'clob.polymarket.com' in url:
            return _book_for(url, event)
        return b'', {'Status': 204}
    with pytest.raises(ValueError, match='symlink'):
        Campaign(inside, config, fetch=fetch, clock=lambda: NOW).once()
    assert not list(outside.glob('*.json'))

def test_cycle_deadline_takes_priority_over_awc_request_timeout(tmp_path, monkeypatch):
    config, event = fixture()
    monkeypatch.setattr('src.v3.awc_campaign.CYCLE_WALL_SECONDS', 0.12)
    monkeypatch.setattr('src.v3.awc_campaign.REQUEST_WALL_SECONDS', 0.5)
    class Response:
        status = 200
        headers = {}
        def __enter__(self): return self
        def __exit__(self, *args): return None
        def read(self, size):
            time.sleep(0.8)
            return b'x'
    def fetch(url, limit):
        if 'gamma-api' in url:
            return json.dumps(event).encode(), {}
        if 'open-meteo' in url:
            return b'{"hourly":{}}', {}
        if 'clob.polymarket.com' in url:
            return _book_for(url, event)
        return public_fetch(SOURCE, limit)
    monkeypatch.setattr('src.v3.awc_campaign.build_opener', lambda *args: type('Opener', (), {'open': lambda *a, **kw: Response()})())
    with pytest.raises(ValueError, match='cycle wall budget exhausted'):
        Campaign(tmp_path, config, fetch=fetch, clock=lambda: NOW).once()
    assert not list(tmp_path.glob('*.json'))


def _offline_fetch(event, *, at=NOW):
    def fetch(url, limit):
        if 'gamma-api' in url:
            return json.dumps(event).encode(), {}
        if 'open-meteo' in url:
            return b'{"hourly":{}}', {}
        if 'clob.polymarket.com' in url:
            return _book_for(url, event, at=at)
        return b'', {'Status': 204}
    return fetch


@pytest.mark.parametrize('interruption', ['open_after_creation', 'fsync'])
def test_alarm_during_append_never_publishes_incomplete_sequence(tmp_path, monkeypatch, interruption):
    config, event = fixture()
    campaign = Campaign(tmp_path, config, fetch=_offline_fetch(event), clock=lambda: NOW)
    real_open, real_fsync = os.open, os.fsync
    fired = False
    def interrupted_open(path, flags, *args, **kwargs):
        nonlocal fired
        fd = real_open(path, flags, *args, **kwargs)
        if interruption == 'open_after_creation' and str(path).endswith('.tmp') and not fired:
            fired = True
            os.close(fd)
            raise ValueError('cycle wall budget exhausted')
        return fd
    def interrupted_fsync(fd):
        nonlocal fired
        if interruption == 'fsync' and not fired:
            fired = True
            raise ValueError('cycle wall budget exhausted')
        return real_fsync(fd)
    with monkeypatch.context() as patch:
        patch.setattr(awc_campaign.os, 'open', interrupted_open)
        patch.setattr(awc_campaign.os, 'fsync', interrupted_fsync)
        with pytest.raises(ValueError, match='cycle wall budget exhausted'):
            campaign.once()
    assert fired
    assert report(tmp_path)['cycles'] == 0
    assert not list(tmp_path.glob('*.json'))
    assert not list(tmp_path.glob('*.tmp'))
    assert campaign.once()['sequence'] == 0
    assert report(tmp_path)['cycles'] == 1


def test_exclusive_publish_cannot_replace_competing_sequence(tmp_path, monkeypatch):
    config, event = fixture()
    campaign = Campaign(tmp_path, config, fetch=_offline_fetch(event), clock=lambda: NOW)
    real_link = os.link
    competing = _canonical({'sequence': 0, 'previous_sha256': None, 'event_id': 'other',
                            'target_date': config.target_date, 'captured_at': NOW.isoformat(),
                            'label': {'status': 'unverified'}})
    def race(src, dst, *args, **kwargs):
        (tmp_path / dst).write_bytes(competing)
        return real_link(src, dst, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(awc_campaign.os, 'link', race)
        with pytest.raises(FileExistsError):
            campaign.once()
    assert (tmp_path / '000000.json').read_bytes() == competing
    assert report(tmp_path)['cycles'] == 1
    assert not list(tmp_path.glob('*.tmp'))


def test_directory_fsync_failure_never_reports_success(tmp_path, monkeypatch):
    config, event = fixture()
    campaign = Campaign(tmp_path, config, fetch=_offline_fetch(event), clock=lambda: NOW)
    original = os.fsync
    calls = []
    def fail_directory(fd):
        calls.append(fd)
        if len(calls) == 2:
            raise OSError('directory fsync failed')
        return original(fd)
    with monkeypatch.context() as patch:
        patch.setattr(awc_campaign.os, 'fsync', fail_directory)
        with pytest.raises(OSError, match='directory fsync failed'):
            campaign.once()
    assert len(calls) == 2
    assert report(tmp_path)['cycles'] == 1
    assert not list(tmp_path.glob('*.tmp'))


def test_real_cycle_alarm_during_temp_fsync_leaves_tape_retryable(tmp_path, monkeypatch):
    config, event = fixture()
    campaign = Campaign(tmp_path, config, fetch=_offline_fetch(event), clock=lambda: NOW)
    real_fsync = os.fsync
    monkeypatch.setattr(awc_campaign, 'CYCLE_WALL_SECONDS', 0.02)
    def slow_fsync(fd):
        time.sleep(0.08)
        return real_fsync(fd)
    with monkeypatch.context() as patch:
        patch.setattr(awc_campaign.os, 'fsync', slow_fsync)
        with pytest.raises(ValueError, match='cycle wall budget exhausted'):
            campaign.once()
    assert report(tmp_path)['cycles'] == 0
    assert not list(tmp_path.glob('*.tmp'))
    monkeypatch.setattr(awc_campaign, 'CYCLE_WALL_SECONDS', 60)
    assert campaign.once()['sequence'] == 0


def test_alarm_during_wall_cleanup_restores_handler_and_timer(monkeypatch):
    if not hasattr(signal, 'setitimer'):
        pytest.skip('POSIX timer required')
    previous = signal.getsignal(signal.SIGALRM)
    real_setitimer = signal.setitimer
    triggered = False
    def interrupt_disarm(which, seconds, *args):
        nonlocal triggered
        if seconds == 0 and not triggered:
            triggered = True
            awc_campaign._alarm(signal.SIGALRM, None)
        return real_setitimer(which, seconds, *args)
    with monkeypatch.context() as patch:
        patch.setattr(awc_campaign.signal, 'setitimer', interrupt_disarm)
        with awc_campaign._wall_bound(1, ValueError('outer expired')):
            with awc_campaign._wall_bound(0.5, TimeoutError('inner expired')):
                pass
    assert triggered
    assert awc_campaign._deadlines == []
    assert signal.getsignal(signal.SIGALRM) is previous
    assert signal.getitimer(signal.ITIMER_REAL)[0] == 0


def test_pending_alarm_during_disarm_is_not_delivered_to_previous_handler(monkeypatch):
    previous = signal.getsignal(signal.SIGALRM)
    delivered = []
    signal.signal(signal.SIGALRM, lambda *_: delivered.append('stale'))
    real_setitimer = signal.setitimer
    sent = False
    def pending_disarm(which, seconds, *args):
        nonlocal sent
        if seconds == 0 and not sent:
            sent = True
            signal.raise_signal(signal.SIGALRM)
        return real_setitimer(which, seconds, *args)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(awc_campaign.signal, 'setitimer', pending_disarm)
            with awc_campaign._wall_bound(1, ValueError('expired')):
                pass
        assert sent and not delivered
        assert awc_campaign._deadlines == []
        assert signal.getitimer(signal.ITIMER_REAL)[0] == 0
    finally:
        signal.signal(signal.SIGALRM, previous)


def test_alarm_on_nested_entry_unmask_rolls_back_both_bounds(monkeypatch):
    previous = signal.getsignal(signal.SIGALRM)
    real_sigmask = signal.pthread_sigmask
    sent = False
    def alarm_at_unmask(how, mask):
        nonlocal sent
        if how == signal.SIG_SETMASK and len(awc_campaign._deadlines) == 2 and not sent:
            sent = True
            d, error = awc_campaign._deadlines[0]
            awc_campaign._deadlines[0] = (time.monotonic() - 1, error)
            signal.raise_signal(signal.SIGALRM)
        return real_sigmask(how, mask)
    with monkeypatch.context() as patch:
        patch.setattr(awc_campaign.signal, 'pthread_sigmask', alarm_at_unmask)
        with pytest.raises(ValueError, match='outer expired'):
            with awc_campaign._wall_bound(1, ValueError('outer expired')):
                with awc_campaign._wall_bound(1, TimeoutError('inner expired')):
                    pytest.fail('should not enter inner bound')
    assert sent and awc_campaign._deadlines == []
    assert signal.getsignal(signal.SIGALRM) is previous
    assert signal.getitimer(signal.ITIMER_REAL)[0] == 0


@pytest.mark.parametrize('day,median', [('2026-10-28', 37), ('2026-10-29', 36)])
def test_daily_manifest_selects_sorted_median_and_pins_rule(day, median):
    config, event = daily_fixture(day)
    assert awc_campaign.discover_daily_manifest(event, day, config.start_at) == config
    assert f'{median}°C on' in event['markets'][5]['question']
    for reordered in (list(reversed(event['markets'])), event['markets'][::2] + event['markets'][1::2]):
        assert awc_campaign.discover_daily_manifest({**event, 'markets': reordered}, day, config.start_at) == config
    for changed in (
        {**event, 'eventDate': '2026-10-30'},
        {**event, 'slug': event['slug'] + '-wrong'},
        {**event, 'markets': event['markets'] * 2},
        {**event, 'resolutionSource': SOURCE.replace('OPKC', 'KJFK')},
    ):
        with pytest.raises(ValueError):
            awc_campaign.discover_daily_manifest(changed, day, config.start_at)
    changed = {**event, 'description': RULES + '\nchanged'}
    changed['markets'] = [{**m, 'description': changed['description']} for m in event['markets']]
    assert awc_campaign.discover_daily_manifest(changed, day, config.start_at).rule_sha256 != config.rule_sha256
    with pytest.raises(ValueError, match='pin'):
        awc_campaign.discover_daily_manifest(changed, day, config.start_at, pinned=config)

@pytest.mark.parametrize('mutation', [
    lambda e: e['markets'].pop(),
    lambda e: e['markets'].append({**e['markets'][-1], 'id': '99999'}),
    lambda e: e['markets'].__setitem__(4, {**e['markets'][4], 'question': e['markets'][3]['question']}),
    lambda e: e['markets'].__setitem__(4, {**e['markets'][4], 'question': e['markets'][4]['question'].replace('36°C', '50°C')}),
    lambda e: e['markets'].__setitem__(0, {**e['markets'][0], 'question': e['markets'][0]['question'].replace('or below', 'or lower')}),
    lambda e: e['markets'].__setitem__(10, {**e['markets'][10], 'question': e['markets'][10]['question'].replace('or higher', 'or above')}),
    lambda e: e['markets'].__setitem__(5, {**e['markets'][5], 'question': e['markets'][5]['question'].replace('37°C', '37.0°C')}),
    lambda e: e['markets'].__setitem__(5, {**e['markets'][5], 'question': e['markets'][5]['question'].replace('Karachi', 'Lahore')}),
    lambda e: e['markets'].__setitem__(1, {**e['markets'][1], 'resolutionSource': SOURCE.replace('OPKC', 'KJFK')}),
    lambda e: e['markets'].__setitem__(1, {**e['markets'][1], 'description': RULES + ' changed'}),
    lambda e: e['markets'].__setitem__(1, {**e['markets'][1], 'conditionId': 'bad'}),
    lambda e: e['markets'].__setitem__(1, {**e['markets'][1], 'conditionId': e['markets'][2]['conditionId']}),
    lambda e: e['markets'].__setitem__(1, {**e['markets'][1], 'clobTokenIds': e['markets'][2]['clobTokenIds']}),
    lambda e: e['markets'].__setitem__(1, {**e['markets'][1], 'outcomes': '["No","Yes"]'}),
    lambda e: e['markets'].__setitem__(1, {**e['markets'][1], 'question': e['markets'][1]['question'].replace('October 28', 'October 29')}),
])
def test_daily_discovery_refuses_incomplete_ambiguous_or_drifting_brackets(mutation):
    config, event = daily_fixture()
    mutation(event)
    with pytest.raises(ValueError):
        awc_campaign.discover_daily_manifest(event, config.target_date, config.start_at)

def test_daily_rejects_invalid_bracket_before_any_books_or_append(tmp_path):
    config, event = daily_fixture()
    event['markets'][1]['resolutionSource'] = SOURCE.replace('OPKC', 'KJFK')
    requests = []
    def fetch(url, limit):
        requests.append(url)
        return json.dumps(event).encode(), {'Status': 200}
    with pytest.raises(ValueError):
        awc_campaign.DailyCampaign(tmp_path, config.target_date, config.start_at,
                                   fetch=fetch, clock=lambda: NOW).once()
    assert len(requests) == 1 and not list(tmp_path.glob('*.json'))

def test_daily_pinned_selection_ignores_reordered_books_and_forecast(tmp_path):
    config, event = daily_fixture()
    calls = []
    def fetch(url, limit):
        calls.append(url)
        if 'gamma-api' in url: return json.dumps(event).encode(), {}
        if 'open-meteo' in url: return b'{"hourly":{"temperature_2m":[42]}}', {}
        if '/book?' in url: return _book_for(url, event, at=NOW.replace(hour=13) if len(calls) > 5 else NOW)
        if 'aviationweather' in url: return b'', {'Status': 204}
        raise AssertionError(url)
    daily = awc_campaign.DailyCampaign(tmp_path, config.target_date, config.start_at, fetch=fetch, clock=lambda: NOW)
    assert daily.once()['sequence'] == 0
    event['markets'].reverse()
    daily = awc_campaign.DailyCampaign(tmp_path, config.target_date, config.start_at, fetch=fetch,
                                       clock=lambda: NOW.replace(hour=13))
    assert daily.once()['sequence'] == 1
    rows = [json.loads((tmp_path / f'{n:06d}.json').read_text()) for n in range(2)]
    assert [row['market_id'] for row in rows] == [config.market_id] * 2
    assert [row['rule_sha256'] for row in rows] == [config.rule_sha256] * 2
    assert [parse_qs(urlsplit(url).query)['token_id'][0] for url in calls if '/book?' in url] == ['110', '111'] * 2
    assert len(calls) == 10

@pytest.mark.parametrize('field,replacement', [('conditionId', '0x' + 'b'*64),
                                                 ('clobTokenIds', '["999","456"]'),
                                                 ('clobTokenIds', '["456","123"]')])
def test_daily_legacy_raw_gamma_pins_exact_condition_and_ordered_tokens(tmp_path, field, replacement):
    from src.v3.awc_campaign import DailyCampaign
    config, event = daily_fixture()
    original = _offline_fetch(event)
    assert DailyCampaign(tmp_path, config.target_date, config.start_at,
                         fetch=original, clock=lambda: NOW).once()['status'] == 'captured_partial'
    # Existing row has no explicit condition/token pin: recover it from raw Gamma.
    row = json.loads((tmp_path / '000000.json').read_text())
    assert 'condition_id' not in row
    event['markets'][5][field] = replacement
    calls = []
    def fetch(url, limit):
        calls.append(url)
        return _offline_fetch(event)(url, limit)
    with pytest.raises(ValueError, match='pin'):
        DailyCampaign(tmp_path, config.target_date, config.start_at, fetch=fetch,
                      clock=lambda: NOW.replace(hour=13)).once()
    assert len(calls) == 1 and len(list(tmp_path.glob('*.json'))) == 1

def test_direct_cycle_also_refuses_legacy_pin_drift(tmp_path):
    config, event = fixture()
    Campaign(tmp_path, config, fetch=_offline_fetch(event), clock=lambda: NOW).once()
    event['markets'][0]['conditionId'] = '0x' + 'b'*64
    with pytest.raises(ValueError, match='pin'):
        Campaign(tmp_path, config, fetch=_offline_fetch(event),
                 clock=lambda: NOW.replace(hour=13)).once()
    assert report(tmp_path)['cycles'] == 1

def test_tape_row_bound_includes_escaping_and_metadata_and_is_readable(tmp_path):
    limit = awc_campaign.MAX_TAPE_ROW_BYTES
    assert limit > sum(awc_campaign.LIMITS.values()) * 6
    row = {'sequence': 0, 'previous_sha256': None, 'target_date': '2026-10-28',
           'captured_at': NOW.isoformat(), 'captures': {
        source: {'body': '\x00' * size, 'response_date': 'header'}
        for source, size in awc_campaign.LIMITS.items()}}
    body = _canonical(row)
    assert len(body) <= limit
    (tmp_path / '000000.json').write_bytes(body)
    assert report(tmp_path)['cycles'] == 1
    row['captures']['gamma']['response_date'] = 'x' * limit
    (tmp_path / '000000.json').write_bytes(_canonical(row))
    with pytest.raises(ValueError, match='size'):
        report(tmp_path)

def test_oversized_serialized_row_refused_before_publish(tmp_path, monkeypatch):
    config, event = fixture()
    monkeypatch.setattr(awc_campaign, 'MAX_TAPE_ROW_BYTES', 100)
    with pytest.raises(ValueError, match='row.*limit'):
        Campaign(tmp_path, config, fetch=_offline_fetch(event), clock=lambda: NOW).once()
    assert not list(tmp_path.glob('*.json')) and not list(tmp_path.glob('*.tmp'))


def test_data_api_requires_exact_both_tokens_condition_event_and_terminal_resolution():
    from src.v3.awc_campaign import crosscheck_resolution
    config, event = fixture()
    m = event['markets'][0]
    m.update(closed=True, resolvedAt='2026-10-29T13:00:00Z', outcomePrices='["1","0"]')
    label = _label(m, target_date=config.target_date, fetched_at='2026-10-30T00:00:00Z')
    base = {'condition_id': m['conditionId'], 'event_id': int(config.event_id), 'event_slug': config.slug,
            'resolved': True, 'closed': True, 'module': 'v1_ctf'}
    tokens = {'data': [{**base, 'token_id': '123', 'opposite_token_id': '456',
                        'outcome_index': 0, 'outcome': 'Yes', 'final_price': 1},
                       {**base, 'token_id': '456', 'opposite_token_id': '123',
                        'outcome_index': 1, 'outcome': 'No', 'final_price': 0}]}
    resolution = {'data': [{'condition_id': m['conditionId'], 'status': 'resolved'}]}
    assert crosscheck_resolution(config, m, label, tokens, resolution)['status'] == 'venue_corroborated_final_binary'
    for key, value in [('condition_id', '0x'+'b'*64), ('event_id', 999), ('resolved', False),
                       ('final_price', .5), ('outcome', 'No'), ('opposite_token_id', '999'), ('module', 'v2')]:
        bad = json.loads(json.dumps(tokens)); bad['data'][0][key] = value
        assert crosscheck_resolution(config, m, label, bad, resolution)['status'] == 'unverified'
    for bad in ({'data': tokens['data'][:1]}, {'data': tokens['data']*2}, {'data': 'error'}):
        assert crosscheck_resolution(config, m, label, bad, resolution)['status'] == 'unverified'
    for bad in ({'data': []}, {'data': [{'condition_id': m['conditionId'], 'status': 'proposed'}]},
                {'data': [{'condition_id': '0x'+'b'*64, 'status': 'resolved'}]}):
        assert crosscheck_resolution(config, m, label, tokens, bad)['status'] == 'unverified'
    assert crosscheck_resolution(config, m, {'status': 'unverified'}, tokens, resolution)['status'] == 'unverified'


@pytest.mark.parametrize('slot,invalid', [(0, False), (1, True), (0, 0.0), (1, 1.0)])
def test_data_api_outcome_index_requires_integer_slot_not_boolean_or_float(slot, invalid):
    config, event = fixture()
    market = event['markets'][0]
    market.update(closed=True, resolvedAt='2026-10-29T13:00:00Z', outcomePrices='["1","0"]')
    gamma_label = _label(market)
    base = {'condition_id': market['conditionId'], 'event_id': int(config.event_id),
            'event_slug': config.slug, 'resolved': True, 'closed': True, 'module': 'v1_ctf'}
    tokens = {'data': [
        {**base, 'token_id': '123', 'opposite_token_id': '456', 'outcome_index': 0,
         'outcome': 'Yes', 'final_price': 1},
        {**base, 'token_id': '456', 'opposite_token_id': '123', 'outcome_index': 1,
         'outcome': 'No', 'final_price': 0},
    ]}
    resolution = {'data': [{'condition_id': market['conditionId'], 'status': 'resolved'}]}
    assert awc_campaign.crosscheck_resolution(config, market, gamma_label, tokens, resolution)['status'] == 'venue_corroborated_final_binary'
    tokens['data'][slot]['outcome_index'] = invalid
    assert awc_campaign.crosscheck_resolution(config, market, gamma_label, tokens, resolution)['status'] == 'unverified'


def test_forged_boolean_gamma_label_cannot_count_venue_corroborated_day(tmp_path):
    config, event = fixture()
    market = event['markets'][0]
    market.update(closed=True, resolvedAt='2026-10-29T13:00:00Z', outcomePrices='[true,false]')
    base = {'condition_id': market['conditionId'], 'event_id': int(config.event_id),
            'event_slug': config.slug, 'resolved': True, 'closed': True, 'module': 'v1_ctf'}
    tokens = {'data': [
        {**base, 'token_id': '123', 'opposite_token_id': '456', 'outcome_index': 0,
         'outcome': 'Yes', 'final_price': 0},
        {**base, 'token_id': '456', 'opposite_token_id': '123', 'outcome_index': 1,
         'outcome': 'No', 'final_price': 1},
    ]}
    resolution = {'data': [{'condition_id': market['conditionId'], 'status': 'resolved'}]}
    available_at = '2026-10-30T00:00:00Z'
    captures = {}
    for kind, document in [('gamma', event), ('awc', ''), ('tokens', tokens), ('resolution', resolution)]:
        raw = document if kind == 'awc' else json.dumps(document)
        captures[kind] = {'body': raw, 'sha256': hashlib.sha256(raw.encode()).hexdigest(), 'status': 200}
    captures['gamma'].update(url='https://gamma-api.polymarket.com/events/slug/' + config.slug,
                             first_available_at=available_at)
    forged_gamma = {'status': 'official_gamma_binary', 'winner': 'No',
                    'resolved_at': '2026-10-29T13:00:00+00:00',
                    'basis': 'Gamma closed + resolvedAt + binary outcomePrices'}
    row = {'sequence': 0, 'previous_sha256': None, 'status': 'captured',
           'captured_at': available_at,
           'event_id': config.event_id, 'market_id': config.market_id, 'target_date': config.target_date,
           'station': config.station, 'zone': config.zone, 'campaign_start_at': config.start_at,
           'rule_sha256': config.rule_sha256, 'captures': captures, 'gamma_label': forged_gamma,
           'label': {'status': 'venue_corroborated_final_binary', 'winner': 'No'}}
    (tmp_path / '000000.json').write_bytes(_canonical(row))
    assert report(tmp_path)['resolved_event_days'] == 0


def test_report_does_not_count_unsubstantiated_finality_claims(tmp_path):
    config, _ = fixture()
    row = {'sequence': 0, 'previous_sha256': None, 'event_id': config.event_id,
           'target_date': config.target_date, 'captured_at': NOW.isoformat(),
           'label': {'status': 'official_gamma_binary'}}
    (tmp_path / '000000.json').write_bytes(_canonical(row))
    assert report(tmp_path)['resolved_event_days'] == 0
    row['label']['status'] = 'venue_corroborated_final_binary'
    (tmp_path / '000000.json').write_bytes(_canonical(row))
    assert report(tmp_path)['resolved_event_days'] == 0


def test_cycle_crosscheck_is_bounded_and_outage_cannot_count_day(tmp_path):
    config, event = fixture()
    m = event['markets'][0]
    m.update(closed=True, resolvedAt='2026-10-29T00:00:00Z', outcomePrices='["1","0"]')
    future = datetime(2026, 10, 30, tzinfo=timezone.utc)
    base = {'condition_id': m['conditionId'], 'event_id': int(config.event_id),
            'event_slug': config.slug, 'resolved': True, 'closed': True, 'module': 'v1_ctf'}
    tokens = {'data': [{**base, 'token_id': '123', 'opposite_token_id': '456',
                       'outcome_index': 0, 'outcome': 'Yes', 'final_price': 1},
                      {**base, 'token_id': '456', 'opposite_token_id': '123',
                       'outcome_index': 1, 'outcome': 'No', 'final_price': 0}]}
    resolution = {'data': [{'condition_id': m['conditionId'], 'status': 'resolved'}]}
    calls = []
    def fetch(url, limit):
        calls.append(url)
        if 'gamma-api' in url: return json.dumps(event).encode(), {}
        if 'open-meteo' in url: return b'{"hourly":{}}', {}
        if '/book?' in url:
            if 'token_id=456' in url:
                return b'{"error":"No orderbook exists for the requested token id"}', {'Status': 404}
            return _book_for(url, event, at=future if len(calls) <= 7 else future.replace(hour=1))
        if 'aviationweather' in url: return b'', {'Status': 200}
        if '/v2/tokens?' in url: return json.dumps(tokens).encode(), {}
        if '/v2/resolutions?' in url: return json.dumps(resolution).encode(), {}
        raise AssertionError(url)
    result = Campaign(tmp_path, config, fetch=fetch, clock=lambda: future).once()
    assert result['label'] == 'venue_corroborated_final_binary' and len(calls) == 7
    assert report(tmp_path)['resolved_event_days'] == 1
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(awc_campaign, 'MAX_RESOLVED_DAYS', 1)
        assert Campaign(tmp_path, config, fetch=lambda *_: pytest.fail('cap fetched network'),
                        clock=lambda: future.replace(hour=1)).once()['status'] == 'cap'
    saved = json.loads((tmp_path / '000000.json').read_text())
    assert set(saved['captures']) == {'gamma', 'forecast', 'book', 'book_no', 'awc', 'tokens', 'resolution'}
    assert saved['book_evidence']['No'] == 'unverified_no_book'
    assert saved['gamma_label']['status'] == 'official_gamma_binary'
    # The reporting cap must replay both APIs, not trust a status string or digest.
    for kind, mutation in [('tokens', lambda d: d['data'][0].update(token_id='999')),
                           ('tokens', lambda d: d['data'][0].update(final_price=0.5)),
                           ('resolution', lambda d: d['data'][0].update(status='proposed')),
                           ('resolution', lambda d: d['data'][0].update(condition_id='0x'+'b'*64))]:
        forged = json.loads(json.dumps(saved))
        payload = json.loads(forged['captures'][kind]['body'])
        mutation(payload)
        raw = json.dumps(payload)
        forged['captures'][kind]['body'] = raw
        forged['captures'][kind]['sha256'] = hashlib.sha256(raw.encode()).hexdigest()
        (tmp_path / '000000.json').write_bytes(_canonical(forged))
        assert report(tmp_path)['resolved_event_days'] == 0
    (tmp_path / '000000.json').write_bytes(_canonical(saved))
    duplicate = {**saved, 'sequence': 1, 'previous_sha256': hashlib.sha256(_canonical(saved)).hexdigest(),
                 'captured_at': future.replace(hour=1).isoformat()}
    (tmp_path / '000001.json').write_bytes(_canonical(duplicate))
    assert report(tmp_path)['resolved_event_days'] == 1
    (tmp_path / '000001.json').unlink()
    # A new hour cannot let an outage or disagreement claim another final day.
    def outage(url, limit):
        if '/v2/tokens?' in url: raise TimeoutError('offline')
        return fetch(url, limit)
    later = future.replace(hour=1)
    result = Campaign(tmp_path, config, fetch=outage, clock=lambda: later).once()
    assert result['label'] == 'unverified'
    row = json.loads((tmp_path / '000001.json').read_text())
    assert row['captures']['tokens']['failure_code'] == 'timeout'
    assert 'resolution' not in row['captures']
    assert len(calls) <= 13


def test_daily_cycle_uses_one_gamma_and_pins_manifest_across_hours(tmp_path):
    from src.v3.awc_campaign import DailyCampaign
    config, event = daily_fixture()
    calls = []
    def fetch(url, limit):
        calls.append(url)
        if 'gamma-api' in url: return json.dumps(event).encode(), {'Status': 200}
        if 'open-meteo' in url: return b'{"hourly":{}}', {}
        if '/book?' in url:
            return _book_for(url, event)
        if 'aviationweather' in url: return b'', {'Status': 204}
        raise AssertionError(url)
    daily = DailyCampaign(tmp_path, config.target_date, config.start_at, fetch=fetch, clock=lambda: NOW)
    assert daily.once()['status'] == 'captured_partial'
    assert len(calls) == 5 and sum('gamma-api' in u for u in calls) == 1
    assert daily.once()['status'] == 'already_captured' and len(calls) == 5
    event['description'] += '\nchanged'
    for market in event['markets']:
        market['description'] = event['description']
    daily = DailyCampaign(tmp_path, config.target_date, config.start_at, fetch=fetch,
                          clock=lambda: NOW.replace(hour=13))
    with pytest.raises(ValueError, match='pin'):
        daily.once()
    assert len(calls) == 6 and len(list(tmp_path.glob('*.json'))) == 1
    expired = DailyCampaign(tmp_path, config.target_date, '2026-08-01T00:00:00Z',
                            fetch=lambda *_: (_ for _ in ()).throw(AssertionError('no GET')),
                            clock=lambda: NOW)
    assert expired.once()['status'] == 'deadline'


def test_daily_discovery_failure_and_data_api_invalid_response_never_promote(tmp_path):
    from src.v3.awc_campaign import DailyCampaign
    config, event = daily_fixture()
    event['resolutionSource'] = SOURCE.replace('OPKC', 'KJFK')
    requests = []
    def fetch(url, limit):
        requests.append(url)
        return json.dumps(event).encode(), {'Status': 200}
    with pytest.raises(ValueError, match='AWC'):
        DailyCampaign(tmp_path, config.target_date, config.start_at,
                      fetch=fetch, clock=lambda: NOW).once()
    assert len(requests) == 1 and not list(tmp_path.glob('*.json'))

    config, event = daily_fixture()
    event['markets'][5].update(closed=True, resolvedAt='2026-10-29T00:00:00Z',
                               outcomePrices='["1","0"]')
    future = datetime(2026, 10, 30, tzinfo=timezone.utc)
    def invalid(url, limit):
        if 'gamma-api' in url: return json.dumps(event).encode(), {}
        if 'open-meteo' in url: return b'{"hourly":{}}', {}
        if '/book?' in url:
            return _book_for(url, event, at=future)
        if 'aviationweather' in url: return b'', {'Status': 200}
        return b'{bad json', {'Status': 200}
    result = DailyCampaign(tmp_path, config.target_date, config.start_at,
                           fetch=invalid, clock=lambda: future).once()
    assert result['label'] == 'unverified'
    row = json.loads((tmp_path / '000000.json').read_text())
    assert row['captures']['tokens']['body'] == '{bad json'
    assert row['captures']['tokens']['failure_code'] == 'invalid_or_unavailable'
    assert report(tmp_path)['resolved_event_days'] == 0


def test_event_day_cap_does_not_trust_unbacked_status_even_if_ids_differ(tmp_path):
    previous = None
    for i in range(2):
        row = {'sequence': i, 'previous_sha256': previous, 'event_id': str(i),
               'target_date': '2026-10-28', 'captured_at': NOW.isoformat(),
               'label': {'status': 'venue_corroborated_final_binary'}}
        body = _canonical(row)
        (tmp_path / f'{i:06d}.json').write_bytes(body)
        previous = hashlib.sha256(body).hexdigest()
    assert report(tmp_path)['resolved_event_days'] == 0


def test_preflight_replays_once_without_materializing_rows(tmp_path, monkeypatch):
    config, event = daily_fixture()
    assert awc_campaign.DailyCampaign(tmp_path, config.target_date, config.start_at,
                                      fetch=_offline_fetch(event), clock=lambda: NOW).once()['sequence'] == 0
    visits = []
    original = awc_campaign._canonical
    def counted(record):
        if 'sequence' in record:
            visits.append(record['sequence'])
        return original(record)
    monkeypatch.setattr(awc_campaign, '_canonical', counted)
    assert awc_campaign.DailyCampaign(tmp_path, config.target_date, config.start_at,
                                      fetch=_offline_fetch(event, at=NOW.replace(hour=13)),
                                      clock=lambda: NOW.replace(hour=13)).once()['sequence'] == 1
    assert visits == [0, 1]  # one replay, then the new row's serialization


def test_replay_memory_and_time_for_max_row_and_180_rows(tmp_path):
    # One near-ceiling row plus a full chain of small rows: never build a 2.88 GB fixture.
    row = {'sequence': 0, 'previous_sha256': None, 'target_date': '2026-10-28',
           'captured_at': NOW.isoformat(), 'label': {'status': 'unverified'},
           'captures': {'forecast': {'body': '\x00' * 2_200_000}}}
    body = _canonical(row)
    assert 13_000_000 < len(body) < awc_campaign.MAX_TAPE_ROW_BYTES
    (tmp_path / '000000.json').write_bytes(body)
    previous = hashlib.sha256(body).hexdigest()
    for i in range(1, 180):
        body = _canonical({'sequence': i, 'previous_sha256': previous,
                           'target_date': '2026-10-28', 'captured_at': NOW.isoformat(),
                           'label': {'status': 'unverified'}})
        (tmp_path / f'{i:06d}.json').write_bytes(body)
        previous = hashlib.sha256(body).hexdigest()
    del row, body
    tracemalloc.start()
    start = time.monotonic()
    result = report(tmp_path)
    elapsed = time.monotonic() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f'AWC replay: peak={peak/1_000_000:.1f} MB elapsed={elapsed:.2f}s')
    assert result['cycles'] == 180 and result['pending_labels'] == 180
    assert peak < 130_000_000 and elapsed < 30


def test_replay_preflight_180_rows_with_large_raw_capture(tmp_path):
    config, event = fixture()
    Campaign(tmp_path, config, fetch=_offline_fetch(event), clock=lambda: NOW).once()
    row = json.loads((tmp_path / '000000.json').read_bytes())
    row['captures']['forecast']['body'] = '\x00' * 2_200_000
    row['captures']['forecast']['sha256'] = hashlib.sha256(row['captures']['forecast']['body'].encode()).hexdigest()
    body = _canonical(row)
    assert 13_000_000 < len(body) <= awc_campaign.MAX_TAPE_ROW_BYTES
    (tmp_path / '000000.json').write_bytes(body)
    previous = hashlib.sha256(body).hexdigest()
    row['captures']['forecast']['body'] = ''
    row['captures']['forecast']['sha256'] = hashlib.sha256(b'').hexdigest()
    for i in range(1, 180):
        row['sequence'], row['previous_sha256'] = i, previous
        row['captured_at'] = NOW.replace(hour=13).isoformat()
        body = _canonical(row)
        (tmp_path / f'{i:06d}.json').write_bytes(body)
        previous = hashlib.sha256(body).hexdigest()
    del row, body
    tracemalloc.start()
    start = time.monotonic()
    result = Campaign(tmp_path, config, fetch=lambda *_: pytest.fail('cap fetched'),
                      clock=lambda: NOW.replace(hour=14)).once()
    elapsed = time.monotonic() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f'AWC preflight: peak={peak/1_000_000:.1f} MB elapsed={elapsed:.2f}s')
    assert result['status'] == 'cap'
    assert peak < 130_000_000 and elapsed < 30


def test_replay_rejects_rechained_prior_pin_drift_before_fetch(tmp_path):
    config, event = fixture()
    Campaign(tmp_path, config, fetch=_offline_fetch(event), clock=lambda: NOW).once()
    Campaign(tmp_path, config, fetch=_offline_fetch(event, at=NOW.replace(hour=13)),
             clock=lambda: NOW.replace(hour=13)).once()
    second = json.loads((tmp_path / '000001.json').read_bytes())
    gamma = second['captures']['gamma']
    changed = json.loads(gamma['body'])
    changed['markets'][0]['conditionId'] = '0x' + 'b' * 64
    gamma['body'] = json.dumps(changed)
    gamma['sha256'] = hashlib.sha256(gamma['body'].encode()).hexdigest()
    (tmp_path / '000001.json').write_bytes(_canonical(second))
    # Canonical bytes, correct previous link and correct Gamma digest are not enough.
    assert report(tmp_path)['cycles'] == 2
    with pytest.raises(ValueError, match='pin drift'):
        Campaign(tmp_path, config, fetch=lambda *_: pytest.fail('fetched'),
                 clock=lambda: NOW.replace(hour=14)).once()


def test_replay_rejects_middle_chain_before_fetch(tmp_path):
    config, event = fixture()
    Campaign(tmp_path, config, fetch=_offline_fetch(event), clock=lambda: NOW).once()
    first = (tmp_path / '000000.json').read_bytes()
    second = _canonical({'sequence': 1, 'previous_sha256': hashlib.sha256(first).hexdigest(),
                         'target_date': config.target_date, 'captured_at': NOW.isoformat(),
                         'label': {'status': 'unverified'}})
    (tmp_path / '000001.json').write_bytes(second)
    (tmp_path / '000000.json').write_bytes(first.replace(b'captured_partial', b'captured_changed'))
    for operation in (lambda: report(tmp_path),
                      lambda: Campaign(tmp_path, config, fetch=lambda *_: pytest.fail('fetched'),
                                       clock=lambda: NOW.replace(hour=13)).once()):
        with pytest.raises(ValueError, match='continuity'):
            operation()
