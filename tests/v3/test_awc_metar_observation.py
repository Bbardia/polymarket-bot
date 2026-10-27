"""AWC rule research is deliberately not an eligibility gate."""
import asyncio
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.v3.awc_metar_observation import inspect_awc_source, parse_raw_observations
from src.v3.paper_weather import _verified_resolution_station, parse_high_temperature_contract, evaluate_weather_universe, WeatherPaperPolicy
from src.v3.resolver import parse_resolver_identity

URL = 'https://aviationweather.gov/api/data/metar?ids=OPKC&format=raw&date=2026-10-28T22:00:00Z&hours=30'
RULES = '''The qualifying value is the highest temperature reported in the hourly temperature remark of any routine (METAR) observation for Jinnah International Airport (OPKC) on that date, in tenths of a degree Celsius as reported. A report whose line begins with SPECI is a special observation and does not count. Every other report is routine, including one issued off the nominal hour and one that carries no METAR or SPECI label.
If a routine observation does not carry the hourly temperature remark, the temperature group in the body of that observation governs for that observation, in whole degrees Celsius as reported.
An observation counts if its time stamp, converted to the station's local time (including any daylight-saving shift in effect), falls on the observation date.
The report the resolution source shows for a given observation time is the one that counts, whether or not it is marked as a correction (COR).
In the event that the Aviation Weather Center has published no routine observation reporting a temperature for the observation date by 11:59 PM ET on the day following the observation date, the Weather Underground Daily Observations table will be used as the resolution source.'''


def test_sampled_karachi_is_observation_only_not_live_eligible():
    identity = parse_resolver_identity(URL)
    assert identity.station == 'OPKC' and not identity.supported
    contract = parse_high_temperature_contract('Will the highest temperature in Karachi be 32°C or below on October 28?', end_date=__import__('datetime').datetime.fromisoformat('2026-10-28T12:00:00+00:00'))
    assert contract is not None
    assert _verified_resolution_station(contract, URL) is None
    assert inspect_awc_source(URL, station='OPKC', target_date=date(2026, 10, 28), timezone_name='Asia/Karachi', description=RULES).observation_only


@pytest.mark.parametrize('url', [
    URL.replace('aviationweather.gov', 'evilaviationweather.gov'),
    URL.replace('https:', 'http:'),
    URL.replace('gov/', 'gov:443/'),
    URL.replace('ids=OPKC', 'ids=OPKC,KHI'),
    URL + '&ids=OPKC', URL + '&extra=yes',
    URL.replace('format=raw', 'format=json'),
    URL.replace('hours=30', 'hours=2'),
    URL.replace('2026-10-28', '2026-10-29'),
    URL + '#fragment',
    'https://user@aviationweather.gov/api/data/metar?ids=OPKC&format=raw&date=2026-10-28T22:00:00Z&hours=30',
])
def test_url_rejection(url):
    assert not inspect_awc_source(url, station='OPKC', target_date=date(2026, 10, 28), timezone_name='Asia/Karachi', description=RULES).observation_only


def test_station_and_rule_drift_rejected():
    assert not inspect_awc_source(URL, station='KHI', target_date=date(2026, 10, 28), timezone_name='Asia/Karachi', description=RULES).observation_only
    assert not inspect_awc_source(URL, station='OPKC', target_date=date(2026, 10, 28), timezone_name='Asia/Karachi', description=RULES.replace('does not count', 'does count')).observation_only


@pytest.mark.parametrize(('source', 'observation_only', 'unverified'), [
    (URL, 1, 0),
    (URL.replace('format=raw', 'format=json'), 0, 1),  # AWC identity is unparsed.
    (URL.replace('hours=30', 'hours=2'), 0, 1),  # Parsed URL, insufficient day coverage.
])
def test_discovery_funnel_counts_awc_but_never_fetches_books_or_forecasts(source, observation_only, unverified):
    class Client:
        async def get_tag(self, *, slug):
            return SimpleNamespace(id=84)
        def list_markets(self, **kwargs):
            class Paginator:
                async def iter_items(self):
                    yield market
            return Paginator()
        async def get_order_books(self, *, token_ids):
            raise AssertionError('book fetch must not run for AWC')

    market = SimpleNamespace(
        question='Will the highest temperature in Karachi be 32°C or below on October 28?',
        description=RULES, resolution=SimpleNamespace(source=source),
        state=SimpleNamespace(end_date=datetime(2026, 10, 28, 12, tzinfo=timezone.utc)),
        outcomes=SimpleNamespace(yes=SimpleNamespace(token_id='yes'), no=SimpleNamespace(token_id='no')),
        metrics=SimpleNamespace(liquidity_num=Decimal('10000')),
    )
    class NoForecast:
        async def forecast(self, *_args, **_kwargs):
            raise AssertionError('forecast must not run for AWC')

    result = asyncio.run(evaluate_weather_universe(
        client=Client(), forecast=NoForecast(), policy=WeatherPaperPolicy(),
        now=datetime(2026, 10, 27, tzinfo=timezone.utc),
    ))
    assert result.awc_observation_only == observation_only
    assert result.awc_rule_unverified == unverified
    assert result.markets_discovered == result.markets_evaluated == 0


def test_raw_t_remark_body_fallback_special_local_day_and_rounding():
    raw = '''METAR OPKC 271859Z 00000KT 18/12 RMK T01801000
OPKC 280005Z 00000KT 18/12 RMK T01390133
SPECI OPKC 281200Z 00000KT 43/20 RMK T04300000
METAR OPKC 281300Z 00000KT M08/M17 RMK T10851000
METAR OPKC 281400Z 00000KT 16/12
METAR OPKC 281500Z 00000KT 17/10 RMK T01750000
METAR OPKC 281600Z 00000KT 20/10 RMK T02000000
METAR OPKC 281900Z 00000KT 50/20 RMK T05000000'''
    rows = parse_raw_observations(raw, station='OPKC', target_date=date(2026, 10, 28), timezone_name='Asia/Karachi')
    assert [r.temperature_c for r in rows] == [Decimal('13.9'), Decimal('-8.5'), Decimal('16'), Decimal('17.5'), Decimal('20')]
    assert [r.rounded_c for r in rows] == [14, -8, 16, 18, 20]


def test_negative_half_tie_follows_awc_market_rule_not_generic_round_half_up():
    rows = parse_raw_observations(
        'OPKC 281200Z 00000KT M08/M17 RMK T10851000',
        station='OPKC', target_date=date(2026, 10, 28), timezone_name='Asia/Karachi',
    )
    assert rows[0].temperature_c == Decimal('-8.5')
    assert rows[0].rounded_c == -8  # Rules say next higher whole degree, not away from zero (-9).


def test_corrected_snapshot_and_daylight_saving_boundary_are_observation_only():
    rows = parse_raw_observations(
        'METAR COR KLGA 010430Z 00000KT M01/M03 RMK T10050030\n'
        'KLGA 010030Z 00000KT 01/00',
        station='KLGA', target_date=date(2026, 11, 1), timezone_name='America/New_York',
    )
    assert len(rows) == 1  # 00:30Z is Oct 31 local; 04:30Z is Nov 1 local.
    assert rows[0].rounded_c == 0


@pytest.mark.parametrize('raw', [
    'OPKC 281200Z 00000KT 19/10 RMK T01901000 T02001000',
    'OPKC 281200Z 00000KT 19/10 RMK T019010000',
    'OPKC 281200Z 00000KT 19/10 RMK T21901000',
    'OPKC 281200Z 00000KT 19/10 RMK T/1901000',
    'OPKC 281200Z 00000KT 19/10 RMK T01901000\nOPKC 281200Z 00000KT 20/10 RMK T02001000',
    'KHI 281200Z 00000KT 19/10 RMK T01901000',
])
def test_ambiguous_or_malformed_reports_fail_closed(raw):
    with pytest.raises(ValueError):
        parse_raw_observations(raw, station='OPKC', target_date=date(2026, 10, 28), timezone_name='Asia/Karachi')
