import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.v3.paper import (
    PaperSettings,
    PaperState,
    PaperStore,
    PaperWorker,
    paper_status,
    run_paper,
)
from src.v3.paper_weather import EnsembleForecast, WeatherPaperPolicy


def D(value: str) -> Decimal:
    return Decimal(value)


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
        self.books = {str(book.token_id): book for book in books}
        self.list_calls = 0
        self.book_calls = 0
        self.last_list_kwargs = {}
        self.get_market_error: Exception | None = None

    async def get_tag(self, *, slug):
        return SimpleNamespace(id="84")

    def list_markets(self, **kwargs):
        self.list_calls += 1
        self.last_list_kwargs = kwargs
        return FakePaginator(self.markets)

    async def get_order_books(self, *, token_ids):
        self.book_calls += 1
        return tuple(self.books[str(token_id)] for token_id in token_ids)

    async def get_market(self, *, id):
        if self.get_market_error is not None:
            raise self.get_market_error
        return next(market for market in self.markets if str(market.id) == str(id))


def market(*, closed=False):
    return SimpleNamespace(
        id="market-1",
        condition_id="condition-1",
        question="Will the paper test pass?",
        slug="paper-test",
        state=SimpleNamespace(
            active=not closed,
            closed=closed,
            archived=False,
            accepting_orders=not closed,
            neg_risk=False,
        ),
        outcomes=SimpleNamespace(
            yes=SimpleNamespace(label="Yes", token_id="yes-token", price=D("0.45")),
            no=SimpleNamespace(label="No", token_id="no-token", price=D("0.45")),
        ),
        metrics=SimpleNamespace(liquidity_num=D("10000")),
        trading=SimpleNamespace(
            minimum_tick_size=D("0.01"),
            minimum_order_size=D("5"),
            fees_enabled=False,
            fee_schedule=None,
        ),
        resolution=SimpleNamespace(
            source="https://example.test/rules",
            uma_resolution_status=None,
        ),
    )


def book(
    token_id: str,
    *,
    ask: str,
    bid: str = "0.40",
    condition_id: str = "condition-1",
    neg_risk: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        market=condition_id,
        condition_id=condition_id,
        token_id=token_id,
        timestamp=None,
        bids=(SimpleNamespace(price=D(bid), size=D("100")),),
        asks=(SimpleNamespace(price=D(ask), size=D("100")),),
        min_order_size=D("5"),
        tick_size=D("0.01"),
        neg_risk=neg_risk,
        last_trade_price=D("0.42"),
        hash=f"hash-{token_id}",
    )


def settings(tmp_path, **overrides) -> PaperSettings:
    values = {
        "data_dir": tmp_path,
        "paper_trading": True,
        "live_enabled": False,
        "account_reads_enabled": False,
        "scan_interval_seconds": 60.0,
        "market_limit": 5,
        "min_liquidity": D("1000"),
        "min_net_return": D("0.005"),
        "max_capital": D("50"),
        "reserve_fraction": D("0.25"),
        "max_order_notional": D("5"),
        "max_open_positions": 5,
    }
    values.update(overrides)
    return PaperSettings(**values)


def test_paper_settings_require_all_authenticated_paths_to_remain_disabled(tmp_path):
    safe = settings(tmp_path)
    assert safe.safety_errors() == ()

    unsafe = settings(
        tmp_path,
        paper_trading=False,
        live_enabled=True,
        account_reads_enabled=True,
    )
    assert unsafe.safety_errors() == (
        "PAPER_TRADING must be true",
        "ENABLE_V3_LIVE_TRADING must be false",
        "ENABLE_V3_ACCOUNT_READS must be false",
    )


def test_worker_records_public_scan_and_opens_one_capped_paper_position(tmp_path):
    client = FakePublicClient(
        [market()],
        [book("yes-token", ask="0.45"), book("no-token", ask="0.45")],
    )
    store = PaperStore(tmp_path)
    worker = PaperWorker(client=client, settings=settings(tmp_path), store=store)

    first = asyncio.run(worker.run_cycle())
    second = asyncio.run(worker.run_cycle())

    assert first.markets_scanned == 1
    assert first.paper_trades == 1
    assert second.paper_trades == 0
    state = store.load_state()
    assert state.cash == D("33.00")
    assert tuple(state.open_positions) == ("condition-1",)
    assert len(store.read_records(store.trades_path)) == 1
    assert len(store.read_records(store.scans_path)) == 2
    status = paper_status(settings(tmp_path))
    assert status["public_data_only"] is True
    assert status["authenticated_client_initialized"] is False
    assert client.last_list_kwargs["order"] == "liquidityNum"


def test_complete_set_discovery_paginates_beyond_the_old_200_market_cutoff(tmp_path):
    rejected = []
    for index in range(210):
        item = market()
        item.id = f"rejected-{index}"
        item.state.neg_risk = True
        rejected.append(item)
    eligible = market()
    client = FakePublicClient(
        [*rejected, eligible],
        [book("yes-token", ask="0.55"), book("no-token", ask="0.55")],
    )
    store = PaperStore(tmp_path)
    worker = PaperWorker(
        client=client,
        settings=settings(tmp_path, market_limit=1, discovery_max_markets=250),
        store=store,
    )

    result = asyncio.run(worker.run_cycle())

    assert result.markets_discovered == 1
    assert result.markets_scanned == 1


def test_worker_records_candidate_but_refuses_paper_order_above_cap(tmp_path):
    client = FakePublicClient(
        [market()],
        [book("yes-token", ask="0.45"), book("no-token", ask="0.45")],
    )
    paper_settings = settings(tmp_path, max_order_notional=D("2"))
    store = PaperStore(tmp_path)
    worker = PaperWorker(client=client, settings=paper_settings, store=store)

    result = asyncio.run(worker.run_cycle())

    assert result.candidates == 1
    assert result.paper_trades == 0
    candidate = store.read_records(store.candidates_path)[0]
    assert candidate["paper_executed"] is False
    assert candidate["paper_reason"] == "paper order cap exceeded"
    assert store.load_state().cash == D("37.50")


def test_complete_set_position_settles_from_public_resolution_state(tmp_path):
    resolved_market = market()
    client = FakePublicClient(
        [resolved_market],
        [book("yes-token", ask="0.45"), book("no-token", ask="0.45")],
    )
    store = PaperStore(tmp_path)
    worker = PaperWorker(client=client, settings=settings(tmp_path), store=store)
    asyncio.run(worker.run_cycle())

    resolved_market.state.active = False
    resolved_market.state.closed = True
    resolved_market.state.accepting_orders = False
    resolved_market.outcomes.yes.price = D("0.6")
    resolved_market.outcomes.no.price = D("0.4")
    unresolved = asyncio.run(worker.run_cycle())
    assert unresolved.settlements == 0
    assert tuple(store.load_state().open_positions) == ("condition-1",)

    resolved_market.outcomes.yes.price = D("1")
    resolved_market.outcomes.no.price = D("0")
    result = asyncio.run(worker.run_cycle())

    assert result.settlements == 1
    state = store.load_state()
    assert state.cash == D("38.00")
    assert state.realized_pnl == D("0.50")
    assert state.open_positions == {}
    assert len(store.read_records(store.settlements_path)) == 1


def test_settlement_read_error_marks_cycle_unhealthy(tmp_path):
    client = FakePublicClient(
        [market()],
        [book("yes-token", ask="0.45"), book("no-token", ask="0.45")],
    )
    store = PaperStore(tmp_path)
    worker = PaperWorker(client=client, settings=settings(tmp_path), store=store)
    asyncio.run(worker.run_cycle())
    client.get_market_error = ConnectionError("public resolution unavailable")

    result = asyncio.run(worker.run_cycle())

    assert result.errors == 1
    assert paper_status(settings(tmp_path))["healthy"] is False
    settlement = store.read_records(store.settlements_path)[0]
    assert settlement["status"] == "settlement_error"


def test_worker_opens_only_one_tiny_weather_position_per_city_date(tmp_path):
    weather_one = market()
    weather_one.id = "weather-1"
    weather_one.condition_id = "weather-condition-1"
    weather_one.question = "Will the highest temperature in Singapore be 32°C on August 25?"
    weather_one.state.neg_risk = True
    weather_one.state.end_date = datetime(2026, 8, 25, 12, tzinfo=timezone.utc)
    weather_one.outcomes.yes.token_id = "weather-yes-1"
    weather_one.outcomes.yes.price = D("0.10")
    weather_one.outcomes.no.token_id = "weather-no-1"
    weather_one.outcomes.no.price = D("0.90")
    weather_one.resolution.source = "https://www.weather.gov/wrh/timeseries?site=wsss"

    weather_two = market()
    weather_two.id = "weather-2"
    weather_two.condition_id = "weather-condition-2"
    weather_two.question = "Will the highest temperature in Singapore be 33°C on August 25?"
    weather_two.state.neg_risk = True
    weather_two.state.end_date = datetime(2026, 8, 25, 12, tzinfo=timezone.utc)
    weather_two.outcomes.yes.token_id = "weather-yes-2"
    weather_two.outcomes.yes.price = D("0.10")
    weather_two.outcomes.no.token_id = "weather-no-2"
    weather_two.outcomes.no.price = D("0.90")
    weather_two.resolution.source = "https://www.weather.gov/wrh/timeseries?site=wsss"

    weather_three = market()
    weather_three.id = "weather-3"
    weather_three.condition_id = "weather-condition-3"
    weather_three.question = "Will the highest temperature in Tokyo be 30°C on August 25?"
    weather_three.state.neg_risk = True
    weather_three.state.end_date = datetime(2026, 8, 25, 12, tzinfo=timezone.utc)
    weather_three.outcomes.yes.token_id = "weather-yes-3"
    weather_three.outcomes.yes.price = D("0.10")
    weather_three.outcomes.no.token_id = "weather-no-3"
    weather_three.outcomes.no.price = D("0.90")
    weather_three.resolution.source = "https://www.weather.gov/wrh/timeseries?site=rjtt"

    class WeatherClient(FakePublicClient):
        async def get_tag(self, *, slug):
            assert slug == "weather"
            return SimpleNamespace(id="84")

        def list_markets(self, **kwargs):
            self.list_calls += 1
            self.last_list_kwargs = kwargs
            if kwargs.get("tag_id") == 84:
                return FakePaginator([weather_one, weather_two, weather_three])
            return FakePaginator([])

    class WeatherForecast:
        async def forecast(self, contract, *, now=None):
            return EnsembleForecast(
                raw_probability=D("0.80"),
                ensemble_mean_c=contract.target_c,
                ensemble_std_c=D("1"),
                n_members=100,
                lead_days=1,
            )

    client = WeatherClient(
        [],
        [
            book(
                "weather-yes-1", ask="0.10", bid="0.09",
                condition_id="weather-condition-1", neg_risk=True,
            ),
            book(
                "weather-no-1", ask="0.90", bid="0.89",
                condition_id="weather-condition-1", neg_risk=True,
            ),
            book(
                "weather-yes-2", ask="0.10", bid="0.09",
                condition_id="weather-condition-2", neg_risk=True,
            ),
            book(
                "weather-no-2", ask="0.90", bid="0.89",
                condition_id="weather-condition-2", neg_risk=True,
            ),
            book(
                "weather-yes-3", ask="0.10", bid="0.09",
                condition_id="weather-condition-3", neg_risk=True,
            ),
            book(
                "weather-no-3", ask="0.90", bid="0.89",
                condition_id="weather-condition-3", neg_risk=True,
            ),
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
        max_open_positions=1,
        base_edge=D("0.03"),
        intraclass_correlation=D("0.05"),
        prior_strength=D("10"),
        fractional_kelly=D("0.05"),
    )
    store = PaperStore(tmp_path)
    worker = PaperWorker(
        client=client,
        settings=settings(tmp_path, weather_policy=policy),
        store=store,
        forecast=WeatherForecast(),
    )

    result = asyncio.run(worker.run_cycle(now=datetime(2026, 8, 24, tzinfo=timezone.utc)))

    assert result.weather_markets_scanned == 3
    assert result.weather_candidates == 2
    assert result.paper_trades == 1
    state = store.load_state()
    assert len(state.open_positions) == 1
    position = next(iter(state.open_positions.values()))
    assert position["strategy"] == "weather_directional"
    assert position["event_key"] == "weather:singapore:2026-08-25"
    assert Decimal(position["all_in_cost"]) <= D("1")
    assert len(store.read_records(store.weather_scans_path)) == 3

    client.markets = (weather_one,)
    weather_one.state.active = False
    weather_one.state.closed = True
    weather_one.state.accepting_orders = False
    weather_one.outcomes.yes.price = D("1")
    weather_one.outcomes.no.price = D("0")
    restarted_worker = PaperWorker(
        client=client,
        settings=settings(tmp_path, weather_policy=policy),
        store=store,
        forecast=WeatherForecast(),
    )
    settled = asyncio.run(
        restarted_worker.run_cycle(now=datetime(2026, 8, 26, tzinfo=timezone.utc))
    )

    assert settled.settlements == 1
    final_state = store.load_state()
    assert final_state.open_positions == {}
    assert final_state.weather_resolved == 1
    assert final_state.weather_brier_sum > D("0")
    settlement = store.read_records(store.settlements_path)[0]
    assert settlement["strategy"] == "weather_directional"
    assert settlement["directional_outcome"] == 1
    assert settlement["brier_score"] is not None


def test_no_weather_position_loss_settles_with_correct_brier_and_no_duplicate(tmp_path):
    resolved = market(closed=True)
    resolved.id = "weather-no-loss"
    resolved.condition_id = "weather-no-condition"
    resolved.outcomes.yes.price = D("1")
    resolved.outcomes.no.price = D("0")
    client = FakePublicClient([resolved], [])
    store = PaperStore(tmp_path)
    state = PaperState.new(D("37.50"))
    state.cash = D("37.00")
    state.open_positions["weather-no-condition"] = {
        "strategy": "weather_directional",
        "event_key": "weather:singapore:2026-08-25",
        "market_id": "weather-no-loss",
        "condition_id": "weather-no-condition",
        "side": "NO",
        "shares": "5",
        "all_in_cost": "0.50",
        "model_probability": "0.80",
    }
    store.save_state(state)

    first_worker = PaperWorker(client=client, settings=settings(tmp_path), store=store)
    first = asyncio.run(first_worker.run_cycle())
    second_worker = PaperWorker(client=client, settings=settings(tmp_path), store=store)
    second = asyncio.run(second_worker.run_cycle())

    assert first.settlements == 1
    assert second.settlements == 0
    final_state = store.load_state()
    assert final_state.cash == D("37.00")
    assert final_state.realized_pnl == D("-0.50")
    assert final_state.weather_resolved == 1
    assert final_state.weather_brier_sum == D("0.64")
    settlements = store.read_records(store.settlements_path)
    assert len(settlements) == 1
    assert settlements[0]["directional_outcome"] == 0


def test_run_paper_refuses_unsafe_settings_before_client_construction(tmp_path):
    constructed = False

    def client_factory():
        nonlocal constructed
        constructed = True
        raise AssertionError("public client must not be constructed")

    unsafe = settings(tmp_path, paper_trading=False)
    with pytest.raises(RuntimeError, match="Paper worker refused"):
        asyncio.run(run_paper(unsafe, cycles=1, client_factory=client_factory))
    assert not constructed


def test_run_paper_releases_pid_if_public_client_construction_fails(tmp_path):
    def broken_factory():
        raise RuntimeError("public client construction failed")

    with pytest.raises(RuntimeError, match="public client construction failed"):
        asyncio.run(run_paper(settings(tmp_path), cycles=1, client_factory=broken_factory))
    assert not (tmp_path / "worker.pid").exists()


def test_store_uses_atomic_json_state_and_ignores_blank_jsonl_lines(tmp_path):
    store = PaperStore(tmp_path)
    worker = PaperWorker(
        client=FakePublicClient(
            [market()],
            [book("yes-token", ask="0.55"), book("no-token", ask="0.55")],
        ),
        settings=settings(tmp_path),
        store=store,
    )
    asyncio.run(worker.run_cycle())
    payload = json.loads(store.state_path.read_text())
    assert payload["cycles"] == 1
    store.scans_path.write_text(store.scans_path.read_text() + "\n")
    assert len(store.read_records(store.scans_path)) == 1


def test_store_refuses_a_second_live_worker_pid(tmp_path):
    store = PaperStore(tmp_path)
    store.acquire()
    try:
        with pytest.raises(RuntimeError, match="already running"):
            PaperStore(tmp_path).acquire()
    finally:
        store.release()
