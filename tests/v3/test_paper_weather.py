import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.v3.paper_weather import (
    EnsembleForecast,
    OpenMeteoEnsemble,
    WeatherPaperPolicy,
    evaluate_weather_universe,
    parse_exact_high_contract,
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


def book(token_id: str, *, bid: str, ask: str, ask_size: str = "100"):
    return SimpleNamespace(
        market="condition-weather-1",
        condition_id="condition-weather-1",
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


def test_open_meteo_forecast_uses_public_ensemble_members_and_cache():
    calls = []

    def fetch_json(url, *, params, timeout):
        calls.append((url, params, timeout))
        temperatures = {
            "temperature_2m_max_ecmwf_ifs025_ensemble": [31.5],
            "temperature_2m_max_member01_ecmwf_ifs025_ensemble": [32.0],
            "temperature_2m_max_ncep_gefs_seamless": [31.8],
            "temperature_2m_max_member01_ncep_gefs_seamless": [32.2],
            "temperature_2m_max_icon_seamless_eps": [31.7],
            "temperature_2m_max_member01_icon_seamless_eps": [32.3],
            "temperature_2m_max_gem_global_ensemble": [31.6],
            "temperature_2m_max_member01_gem_global_ensemble": [32.4],
        }
        return {
            "daily": {"time": ["2026-08-25"], **temperatures},
            "daily_units": {
                "time": "iso8601",
                **{key: "°C" for key in temperatures},
            },
        }

    contract = parse_exact_high_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    client = OpenMeteoEnsemble(fetch_json=fetch_json, min_members=8)
    first = asyncio.run(client.forecast(contract, now=datetime(2026, 8, 24, tzinfo=timezone.utc)))
    second = asyncio.run(client.forecast(contract, now=datetime(2026, 8, 24, tzinfo=timezone.utc)))

    assert first is not None
    assert D("0") < first.raw_probability < D("1")
    assert first.n_members == 8
    assert first.model_count == 4
    assert first.lead_days == 1
    assert second == first
    assert len(calls) == 1
    assert calls[0][1]["models"] == "ecmwf_ifs025,gfs_seamless,icon_seamless,gem_global"
    assert calls[0][1]["timezone"] == "auto"


def test_open_meteo_failure_backoff_prevents_request_storm():
    calls = 0

    def fail(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise ConnectionError("forecast unavailable")

    contract = parse_exact_high_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    client = OpenMeteoEnsemble(fetch_json=fail, min_members=3)
    with pytest.raises(ConnectionError, match="forecast unavailable"):
        asyncio.run(client.forecast(contract))
    assert asyncio.run(client.forecast(contract)) is None
    assert calls == 1


def test_open_meteo_rejects_wrong_response_date_and_units():
    def wrong_date(*_args, **_kwargs):
        return {
            "daily": {
                "time": ["2026-08-26"],
                "temperature_2m_max_ecmwf_ifs025_ensemble": [32],
            },
            "daily_units": {
                "time": "iso8601",
                "temperature_2m_max_ecmwf_ifs025_ensemble": "°F",
            },
        }

    contract = parse_exact_high_contract(
        "Will the highest temperature in Singapore be 32°C on August 25?",
        end_date=datetime(2026, 8, 25, 12, tzinfo=timezone.utc),
    )
    assert contract is not None
    with pytest.raises(ValueError, match="date does not match"):
        asyncio.run(OpenMeteoEnsemble(fetch_json=wrong_date).forecast(contract))


def test_weather_policy_caps_discovery_positions_and_kelly_inputs():
    with pytest.raises(ValueError, match="position cap"):
        WeatherPaperPolicy(max_open_positions=6)
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
