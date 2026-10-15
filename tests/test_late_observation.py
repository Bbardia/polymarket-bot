"""Synthetic unit fixtures; never interpreted as observed market performance."""
import copy
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import pytest
import requests

from src.v3.late_observation import evaluate, main as audit_main
from src.v3.late_public_probe import _event_url, _select_market, _public_json, _normalize_book, main as probe_main


def test_public_json_rejects_redirect_and_oversized_stream():
    class Response:
        url = 'https://gamma-api.polymarket.com/events/slug/test-event'
        headers = {}
        def __init__(self, status, chunks):
            self.status_code, self.chunks, self.closed = status, chunks, False
        def __enter__(self): return self
        def __exit__(self, *args): self.closed = True
        def raise_for_status(self): pass
        def iter_content(self, chunk_size): yield from self.chunks
    class Session:
        def __init__(self, response): self.response = response
        def get(self, url, **kwargs):
            assert kwargs['allow_redirects'] is False and kwargs['stream'] is True
            assert kwargs['timeout']
            return self.response
    for response in (Response(302, [b'{}']), Response(200, [b'1234', b'5'])):
        with pytest.raises(ValueError):
            _public_json(Session(response), response.url, limit=4)
        assert response.closed
    response = Response(200, [b'{"ok": true}'])
    data, url = _public_json(Session(response), response.url, limit=12)
    assert data == {'ok': True} and url == response.url and response.closed
    response = Response(200, [b'{}'])
    response.headers = {'Content-Length': '999'}
    with pytest.raises(ValueError):
        _public_json(Session(response), response.url, limit=4)

def test_public_json_distinguishes_transport_phase_and_http_status():
    from src.v3.late_public_probe import ResponseInterrupted, PublicHTTPError
    class Response:
        url = 'https://example.org/json'
        headers = {}
        def __init__(self, failure): self.failure = failure; self.status_code = 503 if failure == 'http' else 200
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def raise_for_status(self):
            if self.failure == 'http': raise requests.HTTPError('secret')
        def iter_content(self, chunk_size):
            yield b'{'
            raise requests.Timeout('secret')
    class Session:
        def __init__(self, failure): self.failure = failure
        def get(self, *args, **kwargs):
            if self.failure == 'connect': raise requests.Timeout('secret')
            return Response(self.failure)
    for failure, expected in [('connect', requests.Timeout), ('stream', ResponseInterrupted),
                              ('http', PublicHTTPError)]:
        with pytest.raises(expected) as caught:
            _public_json(Session(failure), 'https://example.org/json', limit=100)
        if failure != 'connect':
            assert 'secret' not in str(caught.value)

@pytest.mark.parametrize('failure', [b'\xff', b'{"markets":', requests.Timeout('secret-token'),
                                   ('stream', requests.Timeout('secret-token'))])
def test_probe_malformed_bytes_or_no_response_audited(tmp_path, monkeypatch, failure):
    import sys
    from src.v3 import late_public_probe, late_observation
    calls = []
    class Response:
        status_code = 200
        headers = {}
        url = 'https://gamma-api.polymarket.com/events/slug/test-event'
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def raise_for_status(self): pass
        def iter_content(self, chunk_size):
            if isinstance(failure, tuple): raise failure[1]
            yield failure
    class Session:
        headers = {}
        def get(self, url, **kwargs):
            calls.append(url)
            if isinstance(failure, Exception): raise failure
            return Response()
    monkeypatch.setattr(late_public_probe.requests, 'Session', Session)
    monkeypatch.setattr(late_observation, '__file__', str(tmp_path / 'src/v3/late_observation.py'))
    output = tmp_path / 'rejected.json'
    monkeypatch.setattr(sys, 'argv', ['probe', '--slug', 'test-event', '--market-id', '123', '--output', str(output)])
    probe_main()
    capture = json.loads(output.read_text())
    assert len(calls) == 1 and len(output.read_bytes()) < 1024
    assert 'input' not in capture and 'secret-token' not in output.read_text()
    assert capture['result']['status'] == 'rejected'
    if isinstance(failure, tuple):
        assert capture['result']['reason'] == 'Gamma response interrupted'
        assert capture['provider_health']['gamma'] == 'interrupted'
    elif isinstance(failure, Exception):
        assert capture['result']['reason'] == 'Gamma request failed without a response'
        assert 'gamma' not in capture['evidence']
        assert capture['provider_health']['gamma'] == 'no_response'
    else:
        assert capture['result']['reason'] == 'malformed, redirected or oversized Gamma response'
        assert 'gamma' in capture['evidence']


@pytest.mark.parametrize('status,chunks', [(302, [b'{}']), (200, [b'x' * 1_000_001])])
def test_probe_refuses_redirect_or_large_gamma_and_persists_small_rejection(
        tmp_path, monkeypatch, status, chunks):
    import sys
    from src.v3 import late_public_probe, late_observation
    calls = []
    class Response:
        status_code = status
        headers = {}
        url = 'https://gamma-api.polymarket.com/events/slug/test-event'
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def raise_for_status(self): pass
        def iter_content(self, chunk_size): yield from chunks
    class Session:
        headers = {}
        def get(self, url, **kwargs):
            calls.append(url)
            return Response()
    monkeypatch.setattr(late_public_probe.requests, 'Session', Session)
    monkeypatch.setattr(late_observation, '__file__', str(tmp_path / 'src/v3/late_observation.py'))
    output = tmp_path / 'rejection.json'
    monkeypatch.setattr(sys, 'argv', ['probe', '--slug', 'test-event', '--market-id', '123',
                                     '--output', str(output)])
    probe_main()
    record = json.loads(output.read_text())
    assert len(calls) == 1 and record['result']['status'] == 'rejected'
    assert 'input' not in record and len(output.read_bytes()) < 1024


def test_native_clob_book_conversion_preserves_raw_timestamp_and_identity():
    raw = {'market': '0xabc', 'asset_id': 'Y', 'timestamp': '1791820000123',
           'tick_size': '0.01', 'min_order_size': '5', 'bids': [], 'asks': []}
    fetched = '2026-10-12T12:00:00+00:00'
    converted = _normalize_book(raw, condition_id='0xabc', token_id='Y', fetched_at=fetched)
    assert converted['condition_id'] == '0xabc' and converted['token_id'] == 'Y'
    assert converted['timestamp'] == datetime.fromtimestamp(1791820000.123, timezone.utc).isoformat()
    assert converted['fetched_at'] == fetched
    assert raw['timestamp'] == '1791820000123' and 'condition_id' not in raw
    for mutated in (dict(raw, market='0xother'), dict(raw, asset_id='N'),
                    dict(raw, timestamp='bad'), dict(raw, timestamp=None),
                    dict(raw, bids=None), dict(raw, asks=None)):
        with pytest.raises(ValueError):
            _normalize_book(mutated, condition_id='0xabc', token_id='Y', fetched_at=fetched)


def test_october14_closed_capture_is_bounded_and_honestly_rejected():
    path = Path(__file__).resolve().parents[1] / 'experiments/late-observation/public-20261014-closed-rejection.json'
    capture = json.loads(path.read_text())
    assert path.stat().st_size < 2048
    assert 'input' not in capture
    gamma = capture['evidence']['gamma']
    assert 'data' not in gamma
    assert set(capture['evidence']) == {'gamma'}
    assert gamma['url'].endswith('/highest-temperature-in-nyc-on-october-13-2026')
    assert gamma['fetched_at'] == capture['result']['decision_at']
    assert len(gamma['event_sha256']) == 64
    assert gamma['selected_market']['id'] == capture['result']['market_id'] == '5540790'
    assert gamma['selected_market']['active'] is True
    assert gamma['selected_market']['closed'] is True
    assert gamma['selected_market']['acceptingOrders'] is False
    assert capture['result']['status'] == 'rejected'
    assert capture['result']['reason'] == 'exact unique open market not available'

def test_city_normalization_is_late_lane_only():
    from src.v3.paper_weather import parse_high_temperature_contract
    from src.v3.late_observation import late_contract
    question = 'Will the highest temperature in New York City be between 62-63°F on October 12?'
    end = datetime.fromisoformat('2026-10-12T12:00:00+00:00')
    assert parse_high_temperature_contract(question, end_date=end) is None
    assert late_contract(question, end_date=end).city == 'new york'

def test_public_probe_requires_exact_market_and_safe_slug():
    assert _event_url('highest-temperature-in-nyc-on-october-13-2026').endswith(
        '/events/slug/highest-temperature-in-nyc-on-october-13-2026')
    for slug in ('../wallet', 'a?x=1', 'https://evil.invalid/', ''):
        with pytest.raises(ValueError):
            _event_url(slug)
    markets = [dict(id='111', active=True, closed=False, acceptingOrders=True),
               dict(id='222', active=True, closed=False, acceptingOrders=True)]
    assert _select_market(markets, '222')['id'] == '222'
    for bad in ('333', ''):
        with pytest.raises(ValueError):
            _select_market(markets, bad)
    with pytest.raises(ValueError):
        _select_market(markets + [dict(markets[1])], '222')
    with pytest.raises(ValueError):
        _select_market([dict(id='222', active=True, closed=True)], '222')
    for flag in (False, None, 'true', 1):
        with pytest.raises(ValueError):
            _select_market([dict(markets[1], acceptingOrders=flag)], '222')


@pytest.mark.parametrize('active,closed,accepting', [(True, True, True), (False, False, True),
                                                    (True, False, False), (True, False, None)])
def test_nonopen_gamma_capture_persists_rejection_without_other_requests(tmp_path, monkeypatch, active, closed, accepting):
    import sys
    from src.v3 import late_public_probe

    class Response:
        url = 'https://gamma-api.polymarket.com/events/slug/test-event'
        status_code = 200
        headers = {}
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def raise_for_status(self):
            pass
        def json(self):
            return {'markets': [{'id': '123', 'active': active, 'closed': closed, 'acceptingOrders': accepting}]}
        def iter_content(self, chunk_size): yield json.dumps(self.json()).encode()

    class Session:
        headers = {}
        def get(self, url, **kwargs):
            assert url == Response.url, 'rejected snapshot must not fetch METAR or CLOB'
            return Response()

    monkeypatch.setattr(late_public_probe.requests, 'Session', Session)
    monkeypatch.setattr(__import__('src.v3.late_observation', fromlist=['x']), '__file__',
                        str(tmp_path / 'src/v3/late_observation.py'))
    output = tmp_path / 'capture.json'
    monkeypatch.setattr(sys, 'argv', ['probe', '--slug', 'test-event', '--market-id', '123', '--output', str(output)])
    probe_main()
    capture = json.loads(output.read_text())
    assert capture['evidence']['gamma']['url'] == Response.url
    assert 'data' not in capture['evidence']['gamma']
    assert capture['result']['status'] == 'rejected'
    assert capture['result']['paper_only'] is True
    assert 'open market' in capture['result']['reason']
    assert capture['result']['market_id'] == '123'
    assert 'metar' not in capture['evidence'] and 'book' not in capture['evidence']
    assert 'input' not in capture  # no partial candidate can be mistaken for replayable evidence

@pytest.mark.parametrize('data', [None, [], {}, {'markets': 'bad'},
    {'markets': [{'id': '123', 'active': True, 'closed': False, 'description': 'secret-token'}]},
    {'markets': [{'id': '123', 'active': True, 'closed': False,
                  'description': 'secret-token' * 10000}]}])
def test_malformed_gamma_and_invalid_open_market_are_minimally_persisted(tmp_path, monkeypatch, data):
    import sys
    from src.v3 import late_public_probe
    class Response:
        url = 'https://gamma-api.polymarket.com/events/slug/test-event'
        status_code = 200
        headers = {}
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def raise_for_status(self): pass
        def json(self):
            if data is None:
                raise json.JSONDecodeError('secret-token', 'secret-token', 0)
            return data
        def iter_content(self, chunk_size):
            yield b'broken JSON' if data is None else json.dumps(data).encode()
    class Session:
        headers = {}
        def get(self, url, **kwargs):
            assert url == Response.url
            return Response()
    monkeypatch.setattr(late_public_probe.requests, 'Session', Session)
    monkeypatch.setattr(__import__('src.v3.late_observation', fromlist=['x']), '__file__',
                        str(tmp_path / 'src/v3/late_observation.py'))
    output = tmp_path / 'rejection.json'
    monkeypatch.setattr(sys, 'argv', ['probe', '--slug', 'test-event', '--market-id', '123', '--output', str(output)])
    probe_main()
    capture = json.loads(output.read_text())
    assert capture['result']['status'] == 'rejected'
    assert capture['result']['paper_only'] is True
    assert capture['result']['market_id'] == '123'
    assert 'input' not in capture
    assert 'data' not in capture['evidence']['gamma']
    assert len(output.read_bytes()) < 1024
    assert 'secret-token' not in output.read_text()


@pytest.mark.parametrize('end_date', ['2026-10-12T12:00:00',
                                          '2026-10-12T12:00:00+99:00',
                                          '2026-10-12T12:00:00+00:bad',
                                          None, 123, ['2026-10-12T12:00:00Z']])
def test_probe_rejects_invalid_gamma_end_date_before_ancillary_fetch(tmp_path, monkeypatch, end_date):
    import sys
    from src.v3 import late_public_probe

    market = cast(dict, copy.deepcopy(example()['market']))
    market['endDate'] = end_date
    calls = []
    event_url = 'https://gamma-api.polymarket.com/events/slug/test-event'

    class Response:
        url = event_url
        status_code = 200
        headers = {}
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def raise_for_status(self): pass
        def json(self): return {'markets': [market]}
        def iter_content(self, chunk_size): yield json.dumps(self.json()).encode()

    class Session:
        headers = {}
        def get(self, url, **kwargs):
            calls.append(url)
            assert url == event_url, 'invalid endDate must not fetch METAR or CLOB'
            return Response()

    monkeypatch.setattr(late_public_probe.requests, 'Session', Session)
    monkeypatch.setattr(__import__('src.v3.late_observation', fromlist=['x']), '__file__',
                        str(tmp_path / 'src/v3/late_observation.py'))
    output = tmp_path / 'rejection.json'
    monkeypatch.setattr(sys, 'argv', ['probe', '--slug', 'test-event', '--market-id', '123', '--output', str(output)])
    probe_main()
    capture = json.loads(output.read_text())
    assert calls == [event_url]
    assert set(capture['evidence']) == {'gamma'}
    assert 'data' not in capture['evidence']['gamma']
    assert 'input' not in capture
    assert len(output.read_bytes()) < 1024
    assert capture['result']['status'] == 'rejected'
    assert capture['result']['reason'] == 'invalid open-market metadata or resolver'


@pytest.mark.parametrize('end_date', ['2026-10-12T12:00:00Z', '2026-10-12T08:00:00-04:00'])
def test_probe_accepts_timezone_aware_gamma_end_date(tmp_path, monkeypatch, end_date):
    import sys
    from src.v3 import late_public_probe

    market = cast(dict, copy.deepcopy(example()['market']))
    market['endDate'] = end_date
    calls = []

    class Response:
        status_code = 200
        headers = {}
        def __init__(self, url, data):
            self.url, self.data = url, data
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def raise_for_status(self): pass
        def json(self): return self.data
        def iter_content(self, chunk_size): yield json.dumps(self.data).encode()

    class Session:
        headers = {}
        def get(self, url, **kwargs):
            calls.append(url)
            if 'gamma-api.polymarket.com' in url:
                return Response(url, {'markets': [market]})
            if 'aviationweather.gov' in url:
                return Response(url, [{'icaoId': 'KLGA'}])
            if url == 'https://clob.polymarket.com/book':
                return Response(url + '?token_id=Y', {'market': '0xabc', 'asset_id': 'Y',
                    'timestamp': '1791820000123', 'tick_size': '0.01', 'min_order_size': '5',
                    'asks': [], 'bids': []})
            pytest.fail(f'unexpected URL: {url}')

    monkeypatch.setattr(late_public_probe.requests, 'Session', Session)
    monkeypatch.setattr(__import__('src.v3.late_observation', fromlist=['x']), '__file__',
                        str(tmp_path / 'src/v3/late_observation.py'))
    output = tmp_path / 'capture.json'
    monkeypatch.setattr(sys, 'argv', ['probe', '--slug', 'test-event', '--market-id', '123', '--output', str(output)])
    probe_main()
    capture = json.loads(output.read_text())
    assert calls == ['https://gamma-api.polymarket.com/events/slug/test-event',
                     'https://aviationweather.gov/api/data/metar',
                     'https://clob.polymarket.com/book']
    assert capture['input']['target_end_date'] == end_date
    assert capture['input']['book']['timestamp'] == datetime.fromtimestamp(1791820000.123, timezone.utc).isoformat()
    assert capture['input']['book']['fetched_at'] == capture['evidence']['book']['fetched_at']
    assert capture['evidence']['book']['data']['timestamp'] == '1791820000123'
    assert capture['evidence']['metar']['data'] == [{'icaoId': 'KLGA'}]

@pytest.mark.parametrize('book_failure', [None, requests.Timeout('secret-token'),
                                         ('stream', requests.Timeout('secret-token')), b'\xff'])
def test_probe_invalid_clob_payload_or_timeout_is_nonreplayable(tmp_path, monkeypatch, book_failure):
    import sys
    from src.v3 import late_public_probe, late_observation
    market = example()['market']
    calls = []
    class Response:
        status_code = 200
        headers = {}
        def __init__(self, url, body): self.url, self.body = url, body
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def raise_for_status(self): pass
        def iter_content(self, chunk_size):
            if isinstance(self.body, tuple):
                yield b'{'
                raise self.body[1]
            yield self.body
    class Session:
        headers = {}
        def get(self, url, **kwargs):
            calls.append(url)
            if 'gamma-api' in url: return Response(url, json.dumps({'markets': [market]}).encode())
            if 'aviationweather' in url: return Response(url, b'[]')
            if isinstance(book_failure, Exception): raise book_failure
            body = (book_failure if isinstance(book_failure, (bytes, tuple)) else json.dumps(dict(
                market='0xabc', asset_id='Y', timestamp='1791820000123', bids=None,
                asks=None, tick_size='0.01', min_order_size='5')).encode())
            return Response(url, body)
    monkeypatch.setattr(late_public_probe.requests, 'Session', Session)
    monkeypatch.setattr(late_observation, '__file__', str(tmp_path / 'src/v3/late_observation.py'))
    output = tmp_path / 'rejected.json'
    monkeypatch.setattr(sys, 'argv', ['probe', '--slug', 'test-event', '--market-id', '123', '--output', str(output)])
    probe_main()
    capture = json.loads(output.read_text())
    assert len(calls) == 3
    assert capture['result']['status'] == 'rejected'
    assert 'input' not in capture and 'secret-token' not in output.read_text()
    assert len(output.read_bytes()) < 1024
    assert capture['result']['reason'] == ('public ancillary response interrupted' if isinstance(book_failure, tuple)
        else 'public ancillary request failed without a response' if isinstance(book_failure, Exception)
        else 'malformed or oversized public ancillary response')
    assert capture['provider_health']['clob'] == ('interrupted' if isinstance(book_failure, tuple)
        else 'no_response' if isinstance(book_failure, Exception) else 'malformed')

@pytest.mark.parametrize('stage,shape', [
    (stage, shape) for stage in ('gamma', 'metar', 'clob')
    for shape in ('null', 'dict', 'list', 'scalar')
])
def test_probe_malformed_json_top_level_is_bounded_nonreplayable(
        tmp_path, monkeypatch, stage, shape):
    import sys
    from src.v3 import late_public_probe, late_observation

    market = example()['market']
    valid = {
        'gamma': {'markets': [market]},
        'metar': [],
        'clob': {'market': '0xabc', 'asset_id': 'Y', 'timestamp': '1791820000123',
                 'tick_size': '0.01', 'min_order_size': '5', 'bids': [], 'asks': []},
    }
    malformed = {'null': None, 'dict': {}, 'list': [None], 'scalar': 42}
    calls = []

    class Session:
        headers = {}

    def public_json(session, url, *, limit, params=None):
        source = ('gamma' if 'gamma-api' in url else
                  'metar' if 'aviationweather' in url else 'clob')
        calls.append(source)
        return (malformed[shape] if source == stage else valid[source]), url

    monkeypatch.setattr(late_public_probe.requests, 'Session', Session)
    monkeypatch.setattr(late_public_probe, '_public_json', public_json)
    monkeypatch.setattr(late_observation, '__file__', str(tmp_path / 'src/v3/late_observation.py'))
    output = tmp_path / 'rejected.json'
    monkeypatch.setattr(sys, 'argv', ['probe', '--slug', 'test-event', '--market-id', '123',
                                     '--output', str(output)])
    probe_main()
    capture = json.loads(output.read_text())
    assert calls == {'gamma': ['gamma'], 'metar': ['gamma', 'metar'],
                     'clob': ['gamma', 'metar', 'clob']}[stage]
    assert capture['result']['status'] == 'rejected'
    assert capture['result']['paper_only'] is True
    assert 'input' not in capture
    assert set(capture['evidence']) == {'gamma'}
    assert 'data' not in capture['evidence']['gamma']
    assert len(output.read_bytes()) < 1024


def test_probe_output_outside_worktree_refused_before_network_or_write(tmp_path, monkeypatch):
    import sys
    from src.v3 import late_public_probe
    monkeypatch.setattr(late_public_probe.requests, 'Session', lambda: pytest.fail('network called'))
    output = tmp_path / 'outside.json'
    monkeypatch.setattr(sys, 'argv', ['probe', '--slug', 'test-event', '--market-id', '123', '--output', str(output)])
    with pytest.raises(ValueError, match='inside isolated worktree'):
        probe_main()
    assert not output.exists()

def test_probe_symlink_escape_refused_before_network(tmp_path, monkeypatch):
    import sys
    from src.v3 import late_public_probe
    root = tmp_path / 'root'
    root.mkdir()
    (root / 'escape').symlink_to(tmp_path, target_is_directory=True)
    monkeypatch.setattr(late_public_probe.requests, 'Session', lambda: pytest.fail('network called'))
    monkeypatch.setattr(__import__('src.v3.late_observation', fromlist=['x']), '__file__',
                        str(root / 'src/v3/late_observation.py'))
    output = root / 'escape' / 'outside.json'
    monkeypatch.setattr(sys, 'argv', ['probe', '--slug', 'test-event', '--market-id', '123', '--output', str(output)])
    with pytest.raises(ValueError, match='inside isolated worktree'):
        probe_main()
    assert not (tmp_path / 'outside.json').exists()


def example():
    rule = ('The highest reading under the "Temp" column; Show Hourly Data; '
            'whole degrees Fahrenheit. https://www.weather.gov/wrh/timeseries?site=klga')
    market = dict(id='123', slug='nyc', question='Will the highest temperature in New York City be between 62-63°F on October 12?',
                  conditionId='0xabc', active=True, closed=False, acceptingOrders=True,
                  endDate='2026-10-12T12:00:00Z', outcomes='["Yes","No"]', clobTokenIds='["Y","N"]',
                  resolutionSource='https://www.weather.gov/wrh/timeseries?site=klga', description=rule)
    digest = hashlib.sha256(json.dumps(market, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
    return dict(decision_at='2026-10-12T19:00:00Z', market=market,
                market_fetched_at='2026-10-12T18:59:55Z', market_id='123', condition_id='0xabc',
                token_id='Y', outcome='Yes', rule_sha256=digest, rule_kind='hourly_temp_whole_degree',
                station_id='KLGA', target_date='2026-10-12', target_end_date='2026-10-12T12:00:00Z',
                primary_source_available=True, market_active=True, market_closed=False,
                observations=[dict(station_id='KLGA', observed_at='2026-10-12T18:00:00Z',
                                   published_at='2026-10-12T18:05:00Z', temperature_f=62,
                                   source_url=market['resolutionSource'], column='Temp', hourly=True)],
                remaining_day_model=dict(station_id='KLGA', target_date='2026-10-12',
                                         market_id='123', condition_id='0xabc', token_id='Y', outcome='Yes',
                                         bucket=market['question'], rule_sha256=digest,
                                         issued_at='2026-10-12T18:00:00Z', published_at='2026-10-12T18:20:00Z',
                                         source_url='https://example.org/model', probability_lower='0.8', plausible_remaining_max_f=68),
                book=dict(condition_id='0xabc', token_id='Y', timestamp='2026-10-12T18:59:50Z',
                          fetched_at='2026-10-12T18:59:58Z', bids=[dict(price='0.29',size='100')],
                          asks=[dict(price='0.30',size='100')], tick_size='0.01', min_order_size='5'),
                fee_rate='0.05', fee_verified_at='2026-10-12T18:59:00Z',
                fee_source_url='https://docs.polymarket.com/trading/fees', shares='10', latency_seconds='5')


def test_eligible_is_not_settled():
    result = evaluate(example())
    assert result['status'] == 'eligible', result
    assert result['running_high'] == '62' and result['final_high'] is None
    assert result['depth_haircut'] == '0.5' and result['fee'] != '0'

@pytest.mark.parametrize('field,value', [
    ('bucket', '90°F or higher'), ('market_id', '456'), ('condition_id', '0xwrong'),
    ('token_id', 'N'), ('outcome', 'No'), ('rule_sha256', '0' * 64),
    ('station_id', 'KJFK'), ('target_date', '2026-10-13'),
])
def test_model_must_bind_exact_candidate_and_rule(field, value):
    p = example()
    p['remaining_day_model'][field] = value
    result = evaluate(p)
    assert result['status'] == 'rejected', result
    assert 'model identity mismatch' in result['reason']

@pytest.mark.parametrize('field', ['bucket', 'market_id', 'condition_id', 'token_id',
                                    'outcome', 'rule_sha256', 'station_id', 'target_date'])
def test_model_missing_binding_rejects(field):
    p = example()
    del p['remaining_day_model'][field]
    assert evaluate(p)['status'] == 'rejected'

@pytest.mark.parametrize('candidate', [None, [], 42, {'observations': [None]},
                                        {'observations': [42]}])
def test_malformed_candidate_returns_structured_rejection(candidate):
    p = example() if isinstance(candidate, dict) else candidate
    if isinstance(candidate, dict):
        p.update(candidate)
    result = evaluate(p)
    assert result['status'] == 'rejected' and result['paper_only'] is True
    assert isinstance(result['reason'], str) and result['reason']

@pytest.mark.parametrize('value', ['1e999999999', '-1e999999999', '9' * 1000, 'NaN',
                                   'Infinity', {}, [], None, True, 'secret-token'])
@pytest.mark.parametrize('location', ['shares', 'ask_size', 'ask_price', 'fee_rate',
                                      'tick_size', 'probability_lower', 'temperature_f'])
def test_candidate_numeric_adversarial_rejection_is_bounded(location, value):
    p = example()
    if location in ('shares', 'fee_rate'): p[location] = value
    elif location in ('ask_size', 'ask_price'): p['book']['asks'][0][location[4:]] = value
    elif location == 'tick_size': p['book'][location] = value
    elif location == 'probability_lower': p['remaining_day_model'][location] = value
    else: p['observations'][0][location] = value
    result = evaluate(p)
    assert result['status'] == 'rejected', (location, value, result)
    assert result['paper_only'] is True
    assert len(json.dumps(result)) < 2048
    assert 'secret-token' not in json.dumps(result)

@pytest.mark.parametrize('value', [None, False, 7, 'wrong', {}, [None],
                                   [{'price': '0.3', 'size': '1e999999999'}]])
@pytest.mark.parametrize('field', ['asks', 'bids'])
def test_book_array_adversarial_types_reject(field, value):
    p = example()
    p['book'][field] = value
    result = evaluate(p)
    assert result['status'] == 'rejected' and len(json.dumps(result)) < 2048

def test_deterministic_json_candidate_field_type_sweep():
    # Mutate each consumed candidate branch independently; never fuzz live data.
    for path in [('decision_at',), ('market',), ('market_fetched_at',),
                 ('observations',), ('observations', 0), ('remaining_day_model',),
                 ('book',), ('book', 'asks'), ('book', 'bids'),
                 ('book', 'asks', 0), ('book', 'bids', 0),
                 ('book', 'asks', 0, 'price'), ('book', 'asks', 0, 'size'),
                 ('remaining_day_model', 'probability_lower'), ('shares',),
                 ('fee_verified_at',), ('latency_seconds',)]:
        for value in (None, False, 12, 'secret-token', [], {}, '1e999999999'):
            p = example()
            node = p
            for part in path[:-1]: node = node[part]
            node[path[-1]] = value
            result = evaluate(json.loads(json.dumps(p)))
            if path in (('shares',), ('latency_seconds',)) and value == 12:
                assert result['status'] == 'eligible'
                continue
            assert result['status'] == 'rejected', (path, value, result)
            assert 'secret-token' not in json.dumps(result)
            assert len(json.dumps(result)) < 2048

def test_unencodable_market_snapshot_is_bounded_rejection():
    p = example()
    p['market']['description'] = '\udcffsecret-token'
    result = evaluate(p)
    assert result['status'] == 'rejected'
    assert 'secret-token' not in json.dumps(result)

def test_overflow_cli_persists_bounded_nonreplayable_rejection(tmp_path, monkeypatch):
    import sys
    from src.v3 import late_observation
    p = example()
    p['book']['asks'][0]['size'] = '1e999999999'
    source = tmp_path / 'candidate.json'
    source.write_text(json.dumps(p))
    monkeypatch.setattr(late_observation, '__file__', str(tmp_path / 'src/v3/late_observation.py'))
    audit = tmp_path / 'audit.json'
    monkeypatch.setattr(sys, 'argv', ['audit', '--input', str(source), '--audit', str(audit)])
    audit_main()
    record = json.loads(audit.read_text())
    assert record['result']['status'] == 'rejected'
    assert 'input' not in record and len(audit.read_bytes()) < 2048


@pytest.mark.parametrize('candidate', [None, {'observations': [None]}])
def test_cli_persists_malformed_candidate_rejection(tmp_path, monkeypatch, candidate):
    import sys
    from src.v3 import late_observation
    payload = example() if isinstance(candidate, dict) else candidate
    if isinstance(candidate, dict):
        payload.update(candidate)
    source = tmp_path / 'candidate.json'
    source.write_text(json.dumps(payload))
    monkeypatch.setattr(late_observation, '__file__', str(tmp_path / 'src/v3/late_observation.py'))
    audit = tmp_path / 'audit.json'
    monkeypatch.setattr(sys, 'argv', ['audit', '--input', str(source), '--audit', str(audit)])
    audit_main()
    record = json.loads(audit.read_text())
    assert 'input' not in record
    assert record['result']['status'] == 'rejected'


@pytest.mark.parametrize('change,reason', [
    (lambda p: p['observations'][0].update(station_id='KJFK'), 'wrong station'),
    (lambda p: p['observations'][0].pop('published_at'), 'published_at'),
    (lambda p: p['observations'][0].update(observed_at='2026-10-12T12:00:00Z'), 'stale observed'),
    (lambda p: p['observations'][0].update(published_at='2026-10-12T19:05:00Z'), 'not available'),
    (lambda p: p['observations'].append(dict(p['observations'][0], observed_at='2026-10-12T18:50:00Z', published_at='2026-10-12T18:55:00Z', temperature_f=65)), 'already exceeds'),
    (lambda p: p['market'].update(active=False), 'market snapshot not open'),
    (lambda p: p['market'].update(active=True, closed=True), 'market snapshot not open'),
    (lambda p: p['market'].update(acceptingOrders=False), 'market snapshot not accepting orders'),
    (lambda p: p['book'].update(bids=[]), 'missing bid or ask'),
    (lambda p: p['book'].update(asks=[]), 'missing bid or ask'),
    (lambda p: p.update(shares='2'), 'fee/minimum invalid'),
    (lambda p: p.update(fee_rate='-0.1'), 'fee/minimum invalid'),
    (lambda p: p.pop('fee_verified_at'), 'fee_verified_at'),
    (lambda p: p['book']['asks'][0].update(size='2'), 'insufficient haircut'),
    (lambda p: p['remaining_day_model'].update(probability_lower='0.2'), 'no conservative edge'),
    (lambda p: p['book'].update(timestamp='2026-10-12T18:00:00Z'), 'stale/future'),
])
def test_reject(change, reason):
    p = example()
    change(p)
    if reason.startswith('market snapshot'):
        p['rule_sha256'] = hashlib.sha256(json.dumps(p['market'], sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
    assert reason in evaluate(p)['reason']


@pytest.mark.parametrize('end_date', [None, '', '2026-10-13T12:00:00Z',
                                       '2026-10-12T11:59:59Z', '2026-10-12T12:00:00',
                                       'not-a-date'])
def test_market_end_date_must_exist_and_match_target_instant(end_date):
    p = example()
    if end_date is None:
        del p['market']['endDate']
    else:
        p['market']['endDate'] = end_date
    p['rule_sha256'] = hashlib.sha256(json.dumps(p['market'], sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
    result = evaluate(p)
    assert result['status'] == 'rejected', result
    assert 'endDate' in result['reason'], result


@pytest.mark.parametrize('flag', [None, 'true', 1, 0])
def test_market_accepting_orders_requires_literal_true(flag):
    p = example()
    if flag is None:
        del p['market']['acceptingOrders']
    else:
        p['market']['acceptingOrders'] = flag
    p['rule_sha256'] = hashlib.sha256(json.dumps(p['market'], sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
    result = evaluate(p)
    assert result['status'] == 'rejected', result
    assert 'accepting orders' in result['reason'], result


def test_market_end_date_equivalent_timezone_instant_is_allowed():
    p = example()
    p['market']['endDate'] = '2026-10-12T08:00:00-04:00'
    p['rule_sha256'] = hashlib.sha256(json.dumps(p['market'], sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
    p['remaining_day_model']['rule_sha256'] = p['rule_sha256']
    assert evaluate(p)['status'] == 'eligible'


def test_dst_local_day_not_utc_date():
    p = example()
    # NY midnight on autumn fallback day is UTC-4; 01:30 occurs twice.
    p['target_date'] = '2026-11-01'
    p['market']['question'] = p['market']['question'].replace('October 12', 'November 1')
    p['target_end_date'] = '2026-11-01T12:00:00Z'
    p['market']['endDate'] = p['target_end_date']
    p['decision_at'] = '2026-11-01T06:45:00Z'
    p['market_fetched_at'] = '2026-11-01T06:44:55Z'
    p['observations'][0].update(observed_at='2026-11-01T05:30:00Z', published_at='2026-11-01T05:35:00Z')
    p['observations'].append(dict(p['observations'][0], observed_at='2026-11-01T06:30:00Z', published_at='2026-11-01T06:35:00Z'))
    p['remaining_day_model'].update(target_date='2026-11-01', issued_at='2026-11-01T06:00:00Z', published_at='2026-11-01T06:20:00Z')
    p['fee_verified_at'] = '2026-11-01T06:40:00Z'
    p['book'].update(timestamp='2026-11-01T06:44:50Z', fetched_at='2026-11-01T06:44:58Z')
    p['rule_sha256'] = hashlib.sha256(json.dumps(p['market'],sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
    p['remaining_day_model'].update(bucket=p['market']['question'], rule_sha256=p['rule_sha256'])
    assert evaluate(p)['status'] == 'eligible', evaluate(p)
    p['observations'][0]['observed_at'] = '2026-11-01T03:30:00Z'  # preceding local date
    assert 'outside target local day' in evaluate(p)['reason']
