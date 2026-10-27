"""Offline, deterministic paper-only campaign contract tests."""
import json
import hashlib
from urllib.error import URLError
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.v3.awc_campaign import Campaign, Config, validate_event, report, _canonical, _label, public_fetch
from src.v3.awc_metar_observation import _REQUIRED_RULES


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
            return json.dumps({'market': '0x'+'a'*64, 'asset_id': '123',
                               'timestamp': '1793102400000', 'bids': [], 'asks': []}).encode(), {}
        return b'METAR OPKC 280600Z 00000KT 9999 FEW020 32/23 Q1010 RMK T03200230\n', {}
    campaign = Campaign(tmp_path, config, fetch=fetch, clock=lambda: NOW)
    result = campaign.once()
    assert result['status'] == 'captured' and len(calls) == 4
    files = list(tmp_path.glob('*.json'))
    assert len(files) == 1
    saved = json.loads(files[0].read_text())
    assert saved['captures']['awc']['body'].startswith('METAR OPKC')
    assert saved['captures']['forecast']['issue_at'] is None
    assert saved['captures']['forecast']['first_available_at'] == NOW.isoformat()
    assert saved['observation']['status'] == 'future_day'
    assert saved['label']['status'] == 'unverified'
    assert campaign.once()['status'] == 'already_captured'
    assert len(calls) == 4 and len(list(tmp_path.glob('*.json'))) == 1
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


def test_resolved_day_cap_blocks_all_network(tmp_path):
    config, _ = fixture()
    previous = None
    for n in range(30):
        row = {'sequence': n, 'previous_sha256': previous,
               'event_id': str(n + 1), 'target_date': config.target_date,
               'campaign_start_at': config.start_at, 'station': config.station,
               'captured_at': NOW.isoformat(), 'label': {'status': 'official_gamma_binary'}}
        body = _canonical(row)
        (tmp_path / f'{n:06d}.json').write_bytes(body)
        previous = hashlib.sha256(body).hexdigest()
    assert report(tmp_path)['resolved_event_days'] == 30
    def no_network(*_):
        raise AssertionError('cap must block fetch')
    assert Campaign(tmp_path, config, fetch=no_network, clock=lambda: NOW).once()['status'] == 'cap'


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
            return json.dumps({'market': market['conditionId'], 'asset_id': '123',
                               'timestamp': '1793102400000', 'bids': [], 'asks': []}).encode(), {'Status': 200}
        if isinstance(awc_result, Exception):
            raise awc_result
        return awc_result
    campaign = Campaign(tmp_path, config, fetch=fetch, clock=lambda: NOW)
    result = campaign.once()
    assert result['status'] == 'captured_partial' and result['label'] == 'unverified'
    assert len(calls) == 4
    saved = json.loads((tmp_path / '000000.json').read_text())
    assert saved['status'] == 'captured_partial'
    assert set(saved['captures']) == {'gamma', 'forecast', 'book', 'awc'}
    for source in ('gamma', 'forecast', 'book'):
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
    assert campaign.once()['status'] == 'already_captured' and len(calls) == 4
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
            return json.dumps({'market': event['markets'][0]['conditionId'], 'asset_id': '123',
                               'timestamp': '1793102400000', 'bids': [], 'asks': []}).encode(), {}
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
