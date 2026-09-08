import asyncio
import json
import time
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest
import requests

from src.v3.paper_weather import (
    EnsembleForecast,
    ForecastUnavailableError,
    HighTemperatureContract,
    JMAForecast,
    MetNoLocationForecast,
    NOAAStationObservations,
    NWSGridForecast,
    OpenMeteoEnsemble,
    ObservationBoundResult,
    ObservationUnavailableError,
    OffsetWeatherPublicClient,
    _ensemble_probability,
    ProbabilityCalibration,
    ResilientForecastEnsemble,
    SevenTimerForecast,
    StationObservation,
    WeatherPaperPolicy,
    apply_observation_bounds,
    evaluate_weather_universe,
    parse_exact_high_contract,
    parse_high_temperature_contract,
)


def D(value: str) -> Decimal:
    return Decimal(value)


def weather_market(*, market_id: str = "weather-1", question: str | None = None):
    question = question or "Will the highest temperature in Singapore be 32°C on August 25?"
    return SimpleNamespace(
        id=market_id,
        condition_id=f"condition-{market_id}",
        question=question,
        slug=market_id,
        state=SimpleNamespace(
            active=True,
            closed=False,
            archived=False,
            accepting_orders=True,
            neg_risk=True,
            end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
        ),
        outcomes=SimpleNamespace(
            yes=SimpleNamespace(label="Yes", token_id=f"yes-{market_id}", price=D("0.10")),
            no=SimpleNamespace(label="No", token_id=f"no-{market_id}", price=D("0.90")),
        ),
        metrics=SimpleNamespace(liquidity_num=D("10000")),
        trading=SimpleNamespace(
            minimum_tick_size=D("0.01"),
            minimum_order_size=D("5"),
            fees_enabled=False,
            fee_schedule=None,
        ),
        resolution=SimpleNamespace(
            source="https://www.weather.gov/wrh/timeseries?site=wsss",
            uma_resolution_status=None,
        ),
    )


def book(
    token_id: str,
    *,
    bid: str,
    ask: str,
    ask_size: str = "100",
    condition_id: str = "condition-weather-1",
):
    return SimpleNamespace(
        market=condition_id,
        condition_id=condition_id,
        token_id=token_id,
        bids=(SimpleNamespace(price=D(bid), size=D("100")),),
        asks=(SimpleNamespace(price=D(ask), size=D(ask_size)),),
        min_order_size=D("5"),
        tick_size=D("0.01"),
        neg_risk=True,
    )


class FakePaginator:
    def __init__(self, items):
        self.items = tuple(items)

    def iter_items(self):
        async def iterate():
            for item in self.items:
                yield item

        return iterate()


class FakePublicClient:
    def __init__(self, markets, books):
        self.markets = tuple(markets)
        self.books = {str(item.token_id): item for item in books}
        self.list_kwargs = {}

    async def get_tag(self, *, slug):
        assert slug == "weather"
        return SimpleNamespace(id="84")

    def list_markets(self, **kwargs):
        self.list_kwargs = kwargs
        return FakePaginator(self.markets)

    async def get_order_books(self, *, token_ids):
        return tuple(self.books[str(token_id)] for token_id in token_ids)


class FakeForecast:
    async def forecast(self, contract, *, now=None):
        return EnsembleForecast(
            raw_probability=D("0.80"),
            ensemble_mean_c=D("32"),
            ensemble_std_c=D("1"),
            n_members=100,
            lead_days=1,
        )


def test_exact_high_parser_accepts_celsius_and_rejects_uncalibrated_structures():
    end = datetime(2026, 8, 25, 12, tzinfo=timezone.utc)
    parsed = parse_exact_high_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=end,
    )
    assert parsed is not None
    assert parsed.city == "singapore"
    assert parsed.target_date == "2026-08-25"
    assert parsed.target_c == D("32")
    assert parsed.event_key == "weather:singapore:2026-08-25"

    alias = parse_exact_high_contract(
        "Will the highest temperature in NYC be 80°F on August 25?",
        end_date=end,
    )
    assert alias is not None
    assert alias.city == "new york"
    assert alias.event_key == "weather:new york:2026-08-25"

    new_year = parse_exact_high_contract(
        "Will the highest temperature in New York be 40°F on December 31?",
        end_date=datetime(2027, 1, 1, 1, tzinfo=timezone.utc),
    )
    assert new_year is not None
    assert new_year.target_date == "2026-12-31"

    assert parse_exact_high_contract(
        "Will the highest temperature in Austin be between 100-101°F on August 25?",
        end_date=end,
    ) is None
    assert parse_exact_high_contract(
        "Will the lowest temperature in Miami be 80°F on August 25?",
        end_date=end,
    ) is None


def test_weather_discovery_rejects_a_resolution_station_mismatch():
    market = weather_market()
    market.resolution.source = "https://www.weather.gov/wrh/timeseries?site=wrong"
    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [market],
            [
                book("yes-weather-1", bid="0.09", ask="0.10"),
                book("no-weather-1", bid="0.89", ask="0.90"),
            ],
        ),
        forecast=FakeForecast(),
        policy=WeatherPaperPolicy(),
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))
    assert result.markets_discovered == 0
    assert result.evaluations == ()


def test_weather_policy_caps_discovery_positions_and_kelly_inputs():
    with pytest.raises(ValueError, match="position cap"):
        WeatherPaperPolicy(max_open_positions=21)
    assert WeatherPaperPolicy(max_open_positions=20).max_open_positions == 20
    with pytest.raises(ValueError, match="discovery limit"):
        WeatherPaperPolicy(discovery_limit=5_001)
    with pytest.raises(ValueError, match="fractional Kelly"):
        WeatherPaperPolicy(fractional_kelly=D("1.1"))


def test_weather_universe_allows_neg_risk_only_for_directional_paper_evaluation():
    market = weather_market()
    public = FakePublicClient(
        [market],
        [
            book("yes-weather-1", bid="0.09", ask="0.10"),
            book("no-weather-1", bid="0.89", ask="0.90"),
        ],
    )
    policy = WeatherPaperPolicy(
        enabled=True,
        horizon_days=3,
        discovery_limit=100,
        market_limit=20,
        min_liquidity=D("1000"),
        min_price=D("0.03"),
        max_price=D("0.20"),
        max_order_notional=D("1"),
        base_edge=D("0.03"),
        intraclass_correlation=D("0.05"),
        prior_strength=D("10"),
        fractional_kelly=D("0.05"),
    )

    result = asyncio.run(evaluate_weather_universe(
        client=public,
        forecast=FakeForecast(),
        policy=policy,
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))

    assert result.errors == ()
    assert result.markets_discovered == 1
    assert len(result.evaluations) == 1
    evaluation = result.evaluations[0]
    assert evaluation.strategy == "weather_directional"
    assert evaluation.side == "YES"
    assert evaluation.event_key == "weather:singapore:2026-08-25"
    assert evaluation.paper_tradeable
    assert evaluation.all_in_cost <= D("1")
    assert evaluation.decision.net_edge >= evaluation.decision.minimum_edge
    assert public.list_kwargs["tag_id"] == 84
    assert public.list_kwargs["liquidity_num_min"] == 1000.0


def test_weather_universe_rejects_non_negative_risk_markets():
    market = weather_market()
    market.state.neg_risk = False
    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [market],
            [
                book("yes-weather-1", bid="0.09", ask="0.10"),
                book("no-weather-1", bid="0.89", ask="0.90"),
            ],
        ),
        forecast=FakeForecast(),
        policy=WeatherPaperPolicy(),
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))
    assert result.markets_discovered == 0


def test_no_side_telemetry_uses_no_probability_polarity():
    market = weather_market()
    market.outcomes.yes.price = D("0.95")
    market.outcomes.no.price = D("0.05")

    class LowYesForecast:
        async def forecast(self, contract, *, now=None):
            return EnsembleForecast(
                raw_probability=D("0.05"),
                ensemble_mean_c=D("29"),
                ensemble_std_c=D("1"),
                n_members=100,
                lead_days=1,
            )

    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [market],
            [
                book("yes-weather-1", bid="0.94", ask="0.95"),
                book("no-weather-1", bid="0.04", ask="0.05"),
            ],
        ),
        forecast=LowYesForecast(),
        policy=WeatherPaperPolicy(),
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))
    assert len(result.evaluations) == 1
    evaluation = result.evaluations[0]
    assert evaluation.side == "NO"
    assert evaluation.forecast.raw_probability == D("0.05")
    assert evaluation.raw_probability == D("0.95")
    assert evaluation.paper_tradeable


def test_weather_evaluation_requires_executable_minimum_depth():
    market = weather_market()
    public = FakePublicClient(
        [market],
        [
            book("yes-weather-1", bid="0.09", ask="0.10", ask_size="1"),
            book("no-weather-1", bid="0.89", ask="0.90", ask_size="100"),
        ],
    )
    result = asyncio.run(evaluate_weather_universe(
        client=public,
        forecast=FakeForecast(),
        policy=WeatherPaperPolicy(max_price=D("0.20")),
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))
    assert result.errors == ()
    assert result.evaluations == ()
    assert result.markets_discovered == 1
    assert result.markets_modeled == 1
    assert result.markets_forecast_unavailable == 0
    assert result.markets_side_evaluable == 0
    assert result.forecast_status == "available"


def test_weather_universe_coalesces_global_forecast_outage_and_counts_skipped_markets():
    first = weather_market(market_id="outage-1")
    second = weather_market(
        market_id="outage-2",
        question="Will the highest temperature in Singapore be 33°C on August 25?",
    )

    class UnavailableForecast:
        async def forecast(self, contract, *, now=None):
            raise ForecastUnavailableError(
                "Open-Meteo global rate-limit backoff active",
                provider_global=True,
            )

    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient([first, second], []),
        forecast=UnavailableForecast(),
        policy=WeatherPaperPolicy(observations_enabled=False),
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))

    assert result.markets_discovered == 2
    assert result.markets_forecast_unavailable == 2
    assert result.markets_modeled == 0
    assert result.markets_side_evaluable == 0
    assert result.forecast_status == "unavailable"
    assert len(result.forecast_errors) == 1
    assert len(result.errors) == 1


def test_weather_multi_level_fee_matches_depth_summed_all_in_cost_and_decision():
    item = weather_market()
    item.trading.fees_enabled = True
    item.trading.fee_schedule = SimpleNamespace(rate=D("0.05"))
    yes_book = book("yes-weather-1", bid="0.09", ask="0.10")
    yes_book.asks = (
        SimpleNamespace(price=D("0.10"), size=D("2")),
        SimpleNamespace(price=D("0.40"), size=D("3")),
    )
    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [item],
            [yes_book, book("no-weather-1", bid="0.89", ask="0.90")],
        ),
        forecast=FakeForecast(),
        policy=WeatherPaperPolicy(
            observations_enabled=False,
            max_price=D("0.98"),
            prior_strength=D("0"),
            base_edge=D("0"),
            uncertainty_z=D("0"),
        ),
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))

    evaluation = result.evaluations[0]
    expected_fee = (
        D("2") * D("0.05") * D("0.10") * D("0.90")
        + D("3") * D("0.05") * D("0.40") * D("0.60")
    )
    assert evaluation.ask == D("0.28")
    assert evaluation.fee == expected_fee
    assert evaluation.all_in_cost == D("1.40") + expected_fee
    assert evaluation.decision.fee_per_share == expected_fee / D("5")


def test_generic_high_parser_models_exact_ranges_and_tails_in_celsius():
    end = datetime(2026, 8, 25, 12, tzinfo=timezone.utc)

    exact = parse_high_temperature_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=end,
    )
    assert exact is not None
    assert exact.display_lower == D("32")
    assert exact.display_upper == D("32")
    assert exact.probability_lower_c == D("31.5")
    assert exact.probability_upper_c == D("32.5")
    assert exact.target_c == D("32")
    assert exact.display_temperature == D("32")

    bounded = parse_high_temperature_contract(
        "Will the highest temperature in Singapore be between 30-31°C on August 25?",
        end_date=end,
    )
    assert bounded is not None
    assert bounded.display_lower == D("30")
    assert bounded.display_upper == D("31")
    assert bounded.probability_lower_c == D("29.5")
    assert bounded.probability_upper_c == D("31.5")

    higher = parse_high_temperature_contract(
        "Will the highest temperature in Singapore be 32°C or higher on August 25?",
        end_date=end,
    )
    assert higher is not None
    assert higher.display_lower == D("32")
    assert higher.display_upper is None
    assert higher.probability_lower_c == D("31.5")
    assert higher.probability_upper_c is None

    below = parse_high_temperature_contract(
        "Will the highest temperature in Singapore be 31°C or below on August 25?",
        end_date=end,
    )
    assert below is not None
    assert below.display_lower is None
    assert below.display_upper == D("31")
    assert below.probability_lower_c is None
    assert below.probability_upper_c == D("31.5")


def test_generic_high_parser_converts_fahrenheit_bin_edges_and_keeps_exact_wrapper_strict():
    end = datetime(2026, 8, 25, 12, tzinfo=timezone.utc)
    bounded = parse_high_temperature_contract(
        "Will the highest temperature in Austin be between 80-81°F on August 25?",
        end_date=end,
    )
    assert bounded is not None
    assert bounded.display_lower == D("80")
    assert bounded.display_upper == D("81")
    assert bounded.probability_lower_c == (D("79.5") - D("32")) * D("5") / D("9")
    assert bounded.probability_upper_c == (D("81.5") - D("32")) * D("5") / D("9")
    assert parse_exact_high_contract(
        "Will the highest temperature in Austin be between 80-81°F on August 25?",
        end_date=end,
    ) is None

    lower_alias = parse_high_temperature_contract(
        "Will the highest temperature in Austin be 80°F or lower on August 25?",
        end_date=end,
    )
    assert lower_alias is not None
    assert lower_alias.display_lower is None
    assert lower_alias.display_upper == D("80")


@pytest.mark.parametrize("question", [
    "Will the lowest temperature in Miami be 80°F on August 25?",
    "Will the highest temperature in Miami be between 81-80°F on August 25?",
    "Will the highest temperature in Miami be 80.5°F on August 25?",
    "Will it rain in Miami on August 25?",
])
def test_generic_high_parser_rejects_unmodeled_or_non_whole_degree_structures(question):
    assert parse_high_temperature_contract(
        question,
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    ) is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"display_lower": D("30.5"), "probability_lower_c": D("30")},
        {"display_upper": D("NaN"), "probability_upper_c": D("31.5")},
        {"probability_lower_c": D("NaN")},
        {"probability_lower_c": D("29.4")},
        {"probability_upper_c": D("31.6")},
    ],
)
def test_high_temperature_contract_rejects_noncanonical_bounds(overrides):
    values = {
        "city": "singapore",
        "target_date": "2026-08-25",
        "unit": "C",
        "display_lower": D("30"),
        "display_upper": D("31"),
        "probability_lower_c": D("29.5"),
        "probability_upper_c": D("31.5"),
    }
    values.update(overrides)
    with pytest.raises(ValueError, match="bound"):
        HighTemperatureContract(**values)


def test_open_meteo_probability_supports_bounded_and_one_sided_contracts():
    end = datetime(2026, 8, 25, 12, tzinfo=timezone.utc)

    def contract(outcome):
        parsed = parse_high_temperature_contract(
            f"Will the highest temperature in Singapore be {outcome} on August 25?",
            end_date=end,
        )
        assert parsed is not None
        return parsed

    members = tuple((30.0, 31.0, 32.0, 33.0) for _ in range(4))
    exact = _ensemble_probability(contract("32°C"), members, 0)
    bounded = _ensemble_probability(contract("between 31-32°C"), members, 0)
    higher = _ensemble_probability(contract("32°C or higher"), members, 0)
    below = _ensemble_probability(contract("31°C or below"), members, 0)

    assert bounded.raw_probability > exact.raw_probability
    assert abs((higher.raw_probability + below.raw_probability) - D("1")) < D("0.000000000001")


def test_noaa_observations_use_station_only_request_filter_local_date_and_cache():
    calls = []

    def fetch_json(url, *, params, headers, timeout):
        calls.append((url, params, headers, timeout))
        return [
            {"icaoId": "WSSS", "reportTime": "2026-08-24T15:59:00Z", "temp": 29},
            {"icaoId": "WSSS", "reportTime": "2026-08-24T16:00:00Z", "temp": 31.5},
            {"icaoId": "WSSS", "reportTime": "2026-08-25T15:59:00Z", "temp": 32.5},
            {"icaoId": "WSSS", "reportTime": "2026-08-25T16:00:00Z", "temp": 33},
        ]

    contract = parse_high_temperature_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    client = NOAAStationObservations(
        fetch_json=fetch_json,
        user_agent="paper-v4-tests/1.0",
    )
    first = asyncio.run(client.observations(contract, station_id="WSSS"))
    second = asyncio.run(client.observations(contract, station_id="WSSS"))

    assert first is not None
    assert tuple(item.temperature_c for item in first) == (D("31.5"), D("32.5"))
    assert tuple(item.display_temperature for item in first) == (D("32"), D("33"))
    assert second == first
    assert len(calls) == 1
    assert calls[0][0] == "https://aviationweather.gov/api/data/metar"
    assert calls[0][1] == {"ids": "WSSS", "format": "json", "hours": 24}
    assert calls[0][2] == {"User-Agent": "paper-v4-tests/1.0"}
    assert calls[0][3] == 20


def test_noaa_observations_skip_missing_temperature_readings():
    def fetch_json(*_args, **_kwargs):
        return [
            {"icaoId": "KMIA", "reportTime": "2026-08-31T17:00:00Z", "temp": None},
            {"icaoId": "KMIA", "reportTime": "2026-08-31T18:00:00Z", "temp": 31.2},
        ]

    contract = parse_high_temperature_contract(
        "Will the highest temperature in Miami be 90°F on August 31?",
        end_date=datetime(2026, 8, 31, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    observations = asyncio.run(
        NOAAStationObservations(fetch_json=fetch_json).observations(
            contract,
            station_id="KMIA",
        )
    )
    assert tuple(item.temperature_c for item in observations) == (D("31.2"),)


def test_noaa_observations_convert_celsius_readings_to_fahrenheit_before_rounding():
    def fetch_json(*_args, **_kwargs):
        return [
            {"icaoId": "WSSS", "reportTime": "2026-08-25T00:00:00Z", "temp": 10.3},
        ]

    contract = parse_high_temperature_contract(
        "Will the highest temperature in Singapore be 51°F on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    observations = asyncio.run(
        NOAAStationObservations(fetch_json=fetch_json).observations(
            contract,
            station_id="WSSS",
        )
    )
    assert observations is not None
    assert observations[0].temperature_c == D("10.3")
    assert observations[0].display_temperature == D("51")


def test_noaa_observations_round_half_degree_ties_away_from_zero():
    def fetch_json(*_args, **_kwargs):
        return [
            {"icaoId": "WSSS", "reportTime": "2026-08-25T01:00:00Z", "temp": 10.5},
            {"icaoId": "WSSS", "reportTime": "2026-08-25T02:00:00Z", "temp": -10.5},
        ]

    contract = parse_high_temperature_contract(
        "Will the highest temperature in Singapore be 11°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    observations = asyncio.run(NOAAStationObservations(fetch_json=fetch_json).observations(
        contract,
        station_id="WSSS",
    ))
    assert tuple(item.display_temperature for item in observations) == (D("11"), D("-11"))


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ([{"icaoId": "WRONG", "reportTime": "2026-08-25T01:00:00Z", "temp": 30}], "station"),
        ([{"icaoId": "WSSS", "reportTime": "not-a-time", "temp": 30}], "timestamp"),
        ([{"icaoId": "WSSS", "reportTime": "2026-08-25T01:00:00Z", "temp": True}], "numeric"),
        ([{"icaoId": "WSSS", "reportTime": "2026-08-25T01:00:00Z", "temp": float("nan")}], "finite"),
    ],
)
def test_noaa_observations_reject_malformed_fields(payload, message):
    contract = parse_high_temperature_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    client = NOAAStationObservations(fetch_json=lambda *_args, **_kwargs: payload)
    with pytest.raises(ValueError, match=message):
        asyncio.run(client.observations(contract, station_id="WSSS"))


def test_noaa_observations_require_the_resolver_verified_station_identity():
    contract = parse_high_temperature_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    client = NOAAStationObservations(fetch_json=lambda *_args, **_kwargs: [])
    with pytest.raises(ValueError, match="verified station"):
        asyncio.run(client.observations(contract, station_id="KLGA"))


def test_noaa_observations_deduplicate_concurrent_requests_per_station_event():
    calls = 0

    def fetch_json(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        time.sleep(0.05)
        return [
            {"icaoId": "WSSS", "reportTime": "2026-08-25T01:00:00Z", "temp": 30},
        ]

    contract = parse_high_temperature_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    client = NOAAStationObservations(fetch_json=fetch_json)

    async def gather():
        return await asyncio.gather(*(
            client.observations(contract, station_id="WSSS")
            for _ in range(5)
        ))

    results = asyncio.run(gather())
    assert calls == 1
    assert all(result == results[0] for result in results)


def test_noaa_observations_reject_malformed_payload_and_back_off():
    calls = 0

    def malformed(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return {"icaoId": "WSSS"}

    contract = parse_high_temperature_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    client = NOAAStationObservations(fetch_json=malformed)
    with pytest.raises(ValueError, match="list"):
        asyncio.run(client.observations(contract, station_id="WSSS"))
    with pytest.raises(ObservationUnavailableError, match="backoff"):
        asyncio.run(client.observations(contract, station_id="WSSS"))
    assert calls == 1


def test_noaa_observations_failure_backoff_prevents_request_storm():
    calls = 0

    def fail(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise ConnectionError("observations unavailable")

    contract = parse_high_temperature_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    client = NOAAStationObservations(fetch_json=fail)
    with pytest.raises(ConnectionError, match="observations unavailable"):
        asyncio.run(client.observations(contract, station_id="WSSS"))
    with pytest.raises(ObservationUnavailableError, match="backoff"):
        asyncio.run(client.observations(contract, station_id="WSSS"))
    assert calls == 1


def test_noaa_observations_prune_expired_cache_and_failure_entries():
    contract = parse_exact_high_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    client = NOAAStationObservations(
        fetch_json=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ConnectionError("current failure")
        )
    )
    stale = ("WSSS", "2020-01-01", "C")
    client._cache[stale] = (time.monotonic() - 1, ())
    client._failure_until[stale] = time.monotonic() - 1
    client._failure_messages[stale] = "stale failure"

    with pytest.raises(ConnectionError, match="current failure"):
        asyncio.run(client.observations(contract, station_id="WSSS"))

    assert stale not in client._cache
    assert stale not in client._failure_until
    assert stale not in client._failure_messages


def test_noaa_observation_hours_are_bounded():
    with pytest.raises(ValueError, match="hours"):
        NOAAStationObservations(hours=0)
    with pytest.raises(ValueError, match="hours"):
        NOAAStationObservations(hours=25)


def observation(display_temperature: str) -> StationObservation:
    return StationObservation(
        station_id="WSSS",
        observed_at=datetime(2026, 8, 25, tzinfo=timezone.utc),
        temperature_c=D("0"),
        display_temperature=D(display_temperature),
    )


@pytest.mark.parametrize(
    ("outcome", "current_high", "expected"),
    [
        ("32°C", "33", D("0")),
        ("between 31-32°C", "33", D("0")),
        ("32°C or below", "33", D("0")),
        ("32°C or higher", "32", D("1")),
        ("32°C", "31", D("0.37")),
        ("between 31-32°C", "31", D("0.37")),
        ("32°C or below", "32", D("0.37")),
        ("32°C or higher", "31", D("0.37")),
    ],
)
def test_observation_bounds_apply_only_hard_resolver_logic(outcome, current_high, expected):
    contract = parse_high_temperature_contract(
        f"Will the highest temperature in Singapore be {outcome} on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    adjusted = apply_observation_bounds(contract, D("0.37"), (observation(current_high),))
    assert adjusted.probability == expected
    assert adjusted.same_day_observation_available
    assert adjusted.current_high_display == D(current_high)


def test_observation_bounds_represent_missing_same_day_observations_without_changing_forecast():
    contract = parse_high_temperature_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    adjusted = apply_observation_bounds(contract, D("0.37"), ())
    assert adjusted.probability == D("0.37")
    assert not adjusted.same_day_observation_available
    assert adjusted.current_high_display is None


class FakeObservations:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.calls = []

    async def adjust_probability(self, contract, base_probability, *, station_id):
        self.calls.append((contract, base_probability, station_id))
        if self.error is not None:
            raise self.error
        assert self.result is not None
        return self.result


def test_weather_universe_discovers_generic_contracts_and_preserves_contract_metadata():
    market = weather_market(
        question="Will the highest temperature in Singapore be between 31-32°C on August 25?"
    )
    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [market],
            [
                book("yes-weather-1", bid="0.09", ask="0.10"),
                book("no-weather-1", bid="0.89", ask="0.90"),
            ],
        ),
        forecast=FakeForecast(),
        policy=WeatherPaperPolicy(observations_enabled=False),
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))

    assert result.errors == ()
    assert result.markets_discovered == 1
    assert len(result.evaluations) == 1
    evaluation = result.evaluations[0]
    assert evaluation.contract_kind == "range"
    assert evaluation.unit == "C"
    assert evaluation.display_lower == D("31")
    assert evaluation.display_upper == D("32")
    assert evaluation.target_c is None
    assert evaluation.maker_shadow is not None
    assert evaluation.maker_shadow.execution_status == "not_submitted"


def test_widened_weather_bounds_allow_a_high_probability_no_favorite():
    market = weather_market()

    class LowYesForecast:
        async def forecast(self, contract, *, now=None):
            return EnsembleForecast(
                raw_probability=D("0.01"),
                ensemble_mean_c=D("29"),
                ensemble_std_c=D("1"),
                n_members=100,
                lead_days=1,
            )

    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [market],
            [
                book("yes-weather-1", bid="0.09", ask="0.10"),
                book("no-weather-1", bid="0.89", ask="0.90"),
            ],
        ),
        forecast=LowYesForecast(),
        policy=WeatherPaperPolicy(observations_enabled=False),
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))

    assert len(result.evaluations) == 1
    evaluation = result.evaluations[0]
    assert evaluation.side == "NO"
    assert evaluation.ask == D("0.90")
    assert evaluation.raw_probability == D("0.99")
    assert evaluation.all_in_cost == D("4.50")
    assert evaluation.paper_tradeable


def test_weather_rejects_missing_fee_schedule_and_reports_market_as_scanned():
    market = weather_market()
    market.trading.fees_enabled = True
    market.trading.fee_schedule = None
    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [market],
            [
                book("yes-weather-1", bid="0.09", ask="0.10"),
                book("no-weather-1", bid="0.89", ask="0.90"),
            ],
        ),
        forecast=FakeForecast(),
        policy=WeatherPaperPolicy(observations_enabled=False),
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))

    assert result.markets_discovered == 1
    assert result.markets_evaluated == 1
    assert result.markets_modeled == 1
    assert result.markets_side_evaluable == 0
    assert result.evaluations == ()


def test_weather_surface_rejects_a_component_with_mismatched_book_condition():
    markets = [
        weather_market(
            market_id="low",
            question="Will the highest temperature in Singapore be 29°C or below on August 25?",
        ),
        weather_market(
            market_id="middle",
            question="Will the highest temperature in Singapore be between 30-31°C on August 25?",
        ),
        weather_market(
            market_id="high",
            question="Will the highest temperature in Singapore be 32°C or higher on August 25?",
        ),
    ]
    books = []
    for market in markets:
        condition_id = f"condition-{market.id}"
        yes_condition = "wrong-condition" if market.id == "middle" else condition_id
        books.extend((
            book(
                f"yes-{market.id}",
                bid="0.19",
                ask="0.20",
                condition_id=yes_condition,
            ),
            book(
                f"no-{market.id}",
                bid="0.79",
                ask="0.80",
                condition_id=condition_id,
            ),
        ))

    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(markets, books),
        forecast=FakeForecast(),
        policy=WeatherPaperPolicy(observations_enabled=False),
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))

    assert len(result.events) == 1
    assert not result.events[0].surface.executable
    assert not result.events[0].surface.tradeable
    assert "incomplete" in result.events[0].surface.reason


def test_same_day_weather_refuses_trade_when_required_observation_is_missing():
    observations = FakeObservations(ObservationBoundResult(D("0.80"), False, None))
    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [weather_market()],
            [
                book("yes-weather-1", bid="0.09", ask="0.10"),
                book("no-weather-1", bid="0.89", ask="0.90"),
            ],
        ),
        forecast=FakeForecast(),
        observation_provider=observations,
        policy=WeatherPaperPolicy(
            observations_enabled=True,
        ),
        now=datetime(2026, 8, 25, 1, tzinfo=timezone.utc),
    ))

    assert result.errors == ()
    assert len(observations.calls) == 1
    assert len(result.evaluations) == 1
    evaluation = result.evaluations[0]
    assert evaluation.same_day_contract
    assert not evaluation.same_day_observation_available
    assert not evaluation.paper_tradeable
    assert evaluation.paper_reason == "required same-day station observation unavailable"
    assert evaluation.same_day_observation_status == "unavailable"
    assert result.observations_available == 0
    assert result.observation_errors == 0


def test_same_day_weather_fails_closed_when_observations_are_disabled():
    observations = FakeObservations(
        ObservationBoundResult(D("0.80"), True, D("32"))
    )
    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [weather_market()],
            [
                book("yes-weather-1", bid="0.09", ask="0.10"),
                book("no-weather-1", bid="0.89", ask="0.90"),
            ],
        ),
        forecast=FakeForecast(),
        observation_provider=observations,
        policy=WeatherPaperPolicy(
            observations_enabled=False,
        ),
        now=datetime(2026, 8, 25, 1, tzinfo=timezone.utc),
    ))

    evaluation = result.evaluations[0]
    assert observations.calls == []
    assert not evaluation.paper_tradeable
    assert evaluation.paper_reason == "same-day station observations are disabled"
    assert evaluation.same_day_observation_status == "disabled"


def test_same_day_weather_fails_closed_when_observation_provider_is_missing():
    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [weather_market()],
            [
                book("yes-weather-1", bid="0.09", ask="0.10"),
                book("no-weather-1", bid="0.89", ask="0.90"),
            ],
        ),
        forecast=FakeForecast(),
        observation_provider=None,
        policy=WeatherPaperPolicy(
            observations_enabled=True,
        ),
        now=datetime(2026, 8, 25, 1, tzinfo=timezone.utc),
    ))

    evaluation = result.evaluations[0]
    assert not evaluation.paper_tradeable
    assert evaluation.paper_reason == "same-day station observation provider unavailable"
    assert evaluation.same_day_observation_status == "provider_unavailable"


def test_same_day_weather_fails_closed_on_empty_exact_station_response_without_flag():
    observations = FakeObservations(ObservationBoundResult(D("0.80"), False, None))
    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [weather_market()],
            [
                book("yes-weather-1", bid="0.09", ask="0.10"),
                book("no-weather-1", bid="0.89", ask="0.90"),
            ],
        ),
        forecast=FakeForecast(),
        observation_provider=observations,
        policy=WeatherPaperPolicy(
            observations_enabled=True,
        ),
        now=datetime(2026, 8, 25, 1, tzinfo=timezone.utc),
    ))

    evaluation = result.evaluations[0]
    assert not evaluation.paper_tradeable
    assert evaluation.paper_reason == "required same-day station observation unavailable"
    assert evaluation.same_day_observation_status == "unavailable"


def test_same_day_hard_observation_bound_sets_directional_probability_before_shrinkage():
    observations = FakeObservations(ObservationBoundResult(D("0"), True, D("33")))
    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [weather_market()],
            [
                book("yes-weather-1", bid="0.89", ask="0.90"),
                book("no-weather-1", bid="0.09", ask="0.10"),
            ],
        ),
        forecast=FakeForecast(),
        observation_provider=observations,
        policy=WeatherPaperPolicy(
            observations_enabled=True,
        ),
        now=datetime(2026, 8, 25, 1, tzinfo=timezone.utc),
    ))

    assert result.errors == ()
    assert result.observations_available == 1
    evaluation = result.evaluations[0]
    assert evaluation.side == "NO"
    assert evaluation.raw_probability == D("1")
    assert evaluation.decision.calibrated_probability == D("1")
    assert evaluation.current_high_display == D("33")
    assert evaluation.same_day_observation_status == "available"
    assert evaluation.paper_tradeable


def test_future_weather_stays_eligible_without_calling_observation_provider():
    observations = FakeObservations(error=AssertionError("future observations must not be read"))
    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [weather_market()],
            [
                book("yes-weather-1", bid="0.09", ask="0.10"),
                book("no-weather-1", bid="0.89", ask="0.90"),
            ],
        ),
        forecast=FakeForecast(),
        observation_provider=observations,
        policy=WeatherPaperPolicy(
            observations_enabled=True,
        ),
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))

    assert observations.calls == []
    assert result.errors == ()
    assert result.evaluations[0].same_day_observation_status == "not_applicable"
    assert result.evaluations[0].paper_tradeable


def test_noaa_error_is_per_market_and_blocks_same_day_trade_without_crashing_discovery():
    observations = FakeObservations(error=ConnectionError("NOAA unavailable"))
    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [weather_market()],
            [
                book("yes-weather-1", bid="0.09", ask="0.10"),
                book("no-weather-1", bid="0.89", ask="0.90"),
            ],
        ),
        forecast=FakeForecast(),
        observation_provider=observations,
        policy=WeatherPaperPolicy(
            observations_enabled=True,
        ),
        now=datetime(2026, 8, 25, 1, tzinfo=timezone.utc),
    ))

    assert result.markets_discovered == 1
    assert result.observation_errors == 1
    assert len(result.errors) == 1
    assert "observation error" in result.errors[0]
    assert len(result.evaluations) == 1
    assert not result.evaluations[0].paper_tradeable
    assert result.evaluations[0].observation_error == "ConnectionError: NOAA unavailable"
    assert result.evaluations[0].same_day_observation_status == "error"


def test_same_day_uses_station_local_date_at_a_utc_date_boundary():
    observations = FakeObservations(ObservationBoundResult(D("0.80"), False, None))
    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient(
            [weather_market()],
            [
                book("yes-weather-1", bid="0.09", ask="0.10"),
                book("no-weather-1", bid="0.89", ask="0.90"),
            ],
        ),
        forecast=FakeForecast(),
        observation_provider=observations,
        policy=WeatherPaperPolicy(
            observations_enabled=True,
        ),
        # August 24 UTC is already August 25 at the Singapore resolver station.
        now=datetime(2026, 8, 24, 17, tzinfo=timezone.utc),
    ))

    assert len(observations.calls) == 1
    assert result.evaluations[0].same_day_contract
    assert not result.evaluations[0].paper_tradeable


def test_parser_supported_city_without_verified_station_is_not_discoverable():
    market = weather_market(
        question="Will the highest temperature in Hong Kong be 32°C on August 25?"
    )
    market.resolution.source = "https://www.weather.gov/wrh/timeseries?site=vhhh"
    result = asyncio.run(evaluate_weather_universe(
        client=FakePublicClient([market], []),
        forecast=FakeForecast(),
        policy=WeatherPaperPolicy(observations_enabled=False),
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))
    assert result.markets_discovered == 0
    assert result.evaluations == ()


def test_probability_calibration_persists_and_shrinks_with_resolved_outcomes(tmp_path):
    path = tmp_path / "weather_calibration.json"
    calibration = ProbabilityCalibration(path, min_samples=2)
    for _ in range(2):
        calibration.record("met-no", "singapore", 1, D("0.20"), 1)
    adjusted = calibration.calibrate("met-no", "singapore", 1, D("0.20"))
    assert adjusted > D("0.20")
    reloaded = ProbabilityCalibration(path, min_samples=2)
    assert reloaded.samples() == 2
    assert reloaded.calibrate("met-no", "singapore", 1, D("0.20")) == adjusted


def test_met_no_forecast_parses_target_local_date_and_returns_source_metadata():
    def fetch_json(url, *, params, headers, timeout):
        assert url.endswith("/compact")
        assert params["lat"] == 1.3644
        assert headers["User-Agent"]
        return {
            "properties": {
                "timeseries": [
                    {
                        "time": "2026-08-25T00:00:00Z",
                        "data": {"instant": {"details": {"air_temperature": 30.0}}},
                    },
                    {
                        "time": "2026-08-25T06:00:00Z",
                        "data": {"instant": {"details": {"air_temperature": 33.0}}},
                    },
                    {
                        "time": "2026-08-26T00:00:00Z",
                        "data": {"instant": {"details": {"air_temperature": 99.0}}},
                    },
                ],
            },
        }

    contract = parse_exact_high_contract(
        "Will the highest temperature in Singapore be 33°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    result = asyncio.run(MetNoLocationForecast(fetch_json=fetch_json).forecast(contract))
    assert result.source == "met-no"
    assert result.provider_names == ("met-no",)
    assert result.ensemble_mean_c == D("33.0")


def test_seven_timer_forecast_parses_global_daily_maximum_and_caches():
    calls = []

    def fetch_json(url, *, params, headers, timeout):
        calls.append((url, dict(params)))
        assert url == "https://www.7timer.info/bin/api.pl"
        assert params["product"] == "civillight"
        assert params["unit"] == "metric"
        assert headers["User-Agent"]
        return {
            "dataseries": [
                {"date": 20260825, "temp2m": {"max": 33, "min": 25}},
                {"date": 20260826, "temp2m": {"max": 99, "min": 25}},
            ],
        }

    contract = parse_exact_high_contract(
        "Will the highest temperature in Singapore be 33°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    provider = SevenTimerForecast(fetch_json=fetch_json, min_request_interval_seconds=0)
    first = asyncio.run(provider.forecast(contract, now=datetime(2026, 8, 24, tzinfo=timezone.utc)))
    second = asyncio.run(provider.forecast(contract, now=datetime(2026, 8, 24, tzinfo=timezone.utc)))
    assert first.source == "seven-timer"
    assert first.provider_names == ("seven-timer",)
    assert first.ensemble_mean_c == D("33")
    assert second.raw_probability == first.raw_probability
    assert len(calls) == 1


def test_nws_forecast_parses_fahrenheit_hourly_grid_data():
    def fetch_json(url, *, params, headers, timeout):
        if "/points/" in url:
            return {"properties": {"forecastHourly": "https://api.weather.gov/hourly"}}
        return {
            "properties": {
                "periods": [
                    {"startTime": "2026-08-25T12:00:00-04:00", "temperature": 86, "temperatureUnit": "F"},
                    {"startTime": "2026-08-25T16:00:00-04:00", "temperature": 91, "temperatureUnit": "F"},
                ],
            },
        }

    contract = parse_exact_high_contract(
        "Will the highest temperature in Chicago be 33°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    result = asyncio.run(NWSGridForecast(fetch_json=fetch_json).forecast(contract))
    assert result.source == "nws"
    assert result.ensemble_mean_c == D("32.77777777777778")


def test_open_meteo_forecast_parses_models_and_persists_cache_and_quota(tmp_path):
    calls = []
    contract = parse_exact_high_contract(
        "Will the highest temperature in Tokyo be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None

    def fetch_json(url, *, params, timeout):
        calls.append((url, dict(params), timeout))
        daily: dict[str, object] = {"time": [contract.target_date]}
        units: dict[str, object] = {"time": "iso8601"}
        for name, marker in {
            "ecmwf": "ecmwf_ifs025_ensemble",
            "gfs": "ncep_gefs_seamless",
            "icon": "icon_seamless_eps",
            "gem": "gem_global_ensemble",
        }.items():
            key = f"temperature_2m_max_{marker}"
            daily[key] = [30.0]
            units[key] = "°C"
            for member in range(1, 3):
                member_key = f"temperature_2m_max_member{member:02d}_{marker}"
                daily[member_key] = [30.0 + member]
                units[member_key] = "°C"
        return {"daily": daily, "daily_units": units}

    quota_path = tmp_path / "open_meteo_quota.json"
    provider = OpenMeteoEnsemble(
        fetch_json=fetch_json,
        quota_path=quota_path,
        max_requests_per_day=1,
    )
    first = asyncio.run(provider.forecast(contract, now=datetime(2026, 8, 24, tzinfo=timezone.utc)))
    second = asyncio.run(provider.forecast(contract, now=datetime(2026, 8, 24, tzinfo=timezone.utc)))
    assert first.source == "open-meteo"
    assert first.provider_names == ("open-meteo",)
    assert first.model_count == 4
    assert second.raw_probability == first.raw_probability
    assert len(calls) == 1

    reloaded = OpenMeteoEnsemble(
        fetch_json=fetch_json,
        quota_path=quota_path,
        max_requests_per_day=1,
    )
    cached = asyncio.run(reloaded.forecast(contract, now=datetime(2026, 8, 24, tzinfo=timezone.utc)))
    assert cached.raw_probability == first.raw_probability
    assert len(calls) == 1


def test_open_meteo_enforces_persistent_daily_request_cap(tmp_path):
    contract = parse_exact_high_contract(
        "Will the highest temperature in Tokyo be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None

    def fetch_json(url, *, params, timeout):
        raise RuntimeError("not reached")

    quota_path = tmp_path / "open_meteo_quota.json"
    quota_path.write_text(json.dumps({"request_times": [datetime.now().timestamp()], "cache": {}}))
    provider = OpenMeteoEnsemble(
        fetch_json=fetch_json,
        quota_path=quota_path,
        max_requests_per_day=1,
    )
    with pytest.raises(ForecastUnavailableError, match="application quota exhausted"):
        asyncio.run(provider.forecast(contract))


def test_jma_forecast_parses_tokyo_daily_maximum_and_rejects_other_cities():
    contract = parse_exact_high_contract(
        "Will the highest temperature in Tokyo be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None

    def fetch_json(url, *, params, headers, timeout):
        assert url.endswith("/130000.json")
        assert headers["User-Agent"]
        return [{
            "timeSeries": [
                {
                    "timeDefines": [
                        "2026-08-25T00:00:00+09:00",
                        "2026-08-25T09:00:00+09:00",
                    ],
                    "areas": [{"area": {"code": "44132"}, "temps": ["24", "32"]}],
                },
                {
                    "timeDefines": ["2026-08-25T00:00:00+09:00"],
                    "areas": [{"area": {"code": "44132"}, "tempsMax": ["32"]}],
                },
            ],
        }]

    result = asyncio.run(JMAForecast(fetch_json=fetch_json).forecast(contract))
    assert result.source == "jma"
    assert result.ensemble_mean_c == D("32")
    other = parse_exact_high_contract(
        "Will the highest temperature in Seoul be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert other is not None
    with pytest.raises(ForecastUnavailableError, match="no configured coverage"):
        asyncio.run(JMAForecast(fetch_json=fetch_json).forecast(other))


def test_resilient_ensemble_selects_weights_by_city_continent():
    tokyo = parse_exact_high_contract(
        "Will the highest temperature in Tokyo be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert tokyo is not None
    results = {
        "jma": D("0.90"),
        "open-meteo": D("0.60"),
        "met-no": D("0.30"),
        "seven-timer": D("0.01"),
    }

    class Provider:
        def __init__(self, name):
            self.name = name

        async def forecast(self, contract, *, now=None):
            return EnsembleForecast(
                raw_probability=results[self.name],
                ensemble_mean_c=D("30"),
                ensemble_std_c=D("1"),
                n_members=4,
                lead_days=1,
                model_count=1,
                source=self.name,
            )

    result = asyncio.run(ResilientForecastEnsemble([
        Provider("jma"), Provider("open-meteo"), Provider("seven-timer"), Provider("met-no"),
    ]).forecast(tokyo))
    expected = (D("0.90") * D("0.40") + D("0.60") * D("0.35") + D("0.30") * D("0.20")) / D("0.95")
    assert result.raw_probability == expected
    assert "seven-timer" not in result.provider_names


def test_resilient_ensemble_uses_remaining_sources_when_one_provider_is_down():
    contract = parse_exact_high_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    healthy = EnsembleForecast(
        raw_probability=D("0.70"),
        ensemble_mean_c=D("32"),
        ensemble_std_c=D("1"),
        n_members=20,
        lead_days=1,
        model_count=1,
        source="met-no",
    )

    class Provider:
        def __init__(self, name, result=None):
            self.name = name
            self.result = result

        async def forecast(self, contract, *, now=None):
            if self.result is None:
                raise ForecastUnavailableError(f"{self.name} down", provider_global=True)
            return self.result

    result = asyncio.run(ResilientForecastEnsemble([
        Provider("down"),
        Provider("met-no", healthy),
        Provider("nws", healthy),
    ]).forecast(contract))
    assert result.provider_count == 2
    assert set(result.provider_names) == {"met-no", "nws"}
    assert result.provider_failures[0][0] == "down"
    assert D("0") < result.raw_probability < D("1")


def test_resilient_ensemble_reports_total_outage():
    contract = parse_exact_high_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )

    class Down:
        name = "down"

        async def forecast(self, contract, *, now=None):
            raise ForecastUnavailableError("down", provider_global=True)

    assert contract is not None
    with pytest.raises(ForecastUnavailableError, match="all forecast providers unavailable"):
        asyncio.run(ResilientForecastEnsemble([Down()]).forecast(contract))


def test_offset_weather_client_traverses_following_pages(monkeypatch):
    calls = []
    payloads = {
        0: [{"id": "weather-1"}, {"id": "weather-2"}],
        2: [{"id": "weather-3"}],
    }

    def fetch_json(url, *, params, headers, timeout):
        assert url == "https://gamma-api.polymarket.com/markets"
        calls.append(dict(params))
        return payloads.get(int(params["offset"]), [])

    def parse_response_list(cls, data):
        return tuple(SimpleNamespace(id=item["id"]) for item in data)

    monkeypatch.setattr(
        "src.v3.paper_weather.Market.parse_response_list",
        classmethod(parse_response_list),
    )
    wrapper = OffsetWeatherPublicClient(
        SimpleNamespace(),
        fetch_json=fetch_json,
        page_size=2,
    )

    async def collect():
        return [item.id async for item in wrapper.list_markets(
            tag_id=84,
            closed=False,
            page_size=2,
        ).iter_items()]

    assert asyncio.run(collect()) == ["weather-1", "weather-2", "weather-3"]
    assert [call["offset"] for call in calls] == [0, 2]
    assert all(call["tag_id"] == 84 for call in calls)
    assert all(call["closed"] == "false" for call in calls)
