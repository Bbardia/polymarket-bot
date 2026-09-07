import asyncio
import json
from dataclasses import replace
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
from src.v3.paper_weather import (
    EnsembleForecast,
    ForecastUnavailableError,
    ObservationBoundResult,
    ResilientForecastEnsemble,
    WeatherPaperPolicy,
)


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


def test_default_weather_ensemble_uses_four_capped_providers(tmp_path):
    worker = PaperWorker(
        client=FakePublicClient([], []),
        settings=settings(
            tmp_path,
            weather_policy=WeatherPaperPolicy(enabled=True),
        ),
        store=PaperStore(tmp_path),
    )

    assert isinstance(worker.forecast, ResilientForecastEnsemble)
    assert tuple(provider.name for provider in worker.forecast.providers) == (
        "open-meteo",
        "met-no",
        "nws",
        "jma",
    )
    assert worker.forecast.weights == {
        "open-meteo": D("0.35"),
        "met-no": D("0.30"),
        "nws": D("0.20"),
        "jma": D("0.15"),
    }


def test_paper_can_disable_complete_set_lane(tmp_path):
    client = FakePublicClient(
        [market()],
        [book("yes-token", ask="0.45"), book("no-token", ask="0.45")],
    )
    worker = PaperWorker(
        client=client,
        settings=settings(tmp_path, complete_set_enabled=False),
        store=PaperStore(tmp_path),
    )

    result = asyncio.run(worker.run_cycle())

    assert result.markets_discovered == 0
    assert result.markets_scanned == 0
    assert client.list_calls == 0


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


def test_paper_entry_gate_disables_complete_set_orders(tmp_path):
    client = FakePublicClient(
        [market()],
        [book("yes-token", ask="0.45"), book("no-token", ask="0.45")],
    )
    store = PaperStore(tmp_path)
    worker = PaperWorker(
        client=client,
        settings=settings(tmp_path, entries_enabled=False),
        store=store,
    )

    result = asyncio.run(worker.run_cycle())

    assert result.paper_trades == 0
    candidate = store.read_records(store.candidates_path)[0]
    assert candidate["paper_executed"] is False
    assert candidate["paper_reason"] == "paper entries disabled by profile"
    assert store.load_state().cash == D("37.50")


def test_realized_loss_breaker_disables_complete_set_orders(tmp_path):
    client = FakePublicClient(
        [market()],
        [book("yes-token", ask="0.45"), book("no-token", ask="0.45")],
    )
    store = PaperStore(tmp_path)
    worker = PaperWorker(
        client=client,
        settings=settings(tmp_path, max_realized_loss=D("1")),
        store=store,
    )
    worker.state.realized_pnl = D("-1")
    worker.state.cash = D("37.50")
    store.save_state(worker.state)

    result = asyncio.run(worker.run_cycle())

    assert result.paper_trades == 0
    candidate = store.read_records(store.candidates_path)[0]
    assert candidate["paper_reason"] == "paper realized-loss breaker reached"
    assert store.load_state().cash == D("37.50")


def test_degraded_weather_forecast_blocks_new_entries(tmp_path):
    item = event_weather_market("degraded", "31°C", "0.50")
    worker, store = event_worker(
        tmp_path,
        [item],
        {(D("31"), D("31")): D("0.90")},
    )
    worker.settings = replace(
        worker.settings,
        weather_policy=replace(
            worker.settings.weather_policy,
            base_edge=D("0"),
            require_healthy_forecast=True,
            minimum_provider_count=2,
        ),
    )
    worker.forecast = EventForecast(
        {(D("31"), D("31")): D("0.90")},
        provider_failures=(("open-meteo", "quota exhausted"),),
    )

    result = asyncio.run(worker.run_cycle(
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))

    assert result.weather_forecast_status == "degraded"
    assert result.weather_candidates == 0
    assert result.paper_trades == 0
    evaluation = next(
        row
        for row in store.read_records(store.weather_scans_path)
        if row.get("market_id") == "degraded"
    )
    assert evaluation["tradeable"] is False
    assert evaluation["reason"] == (
        "weather forecast health gate requires at least 2 providers (got 1)"
    )
    assert store.read_status()["paper_entry_block_reason"] is None
    assert store.read_status()["paper_weather_min_provider_count"] == 2


def test_degraded_weather_forecast_with_minimum_providers_can_trade(tmp_path):
    item = event_weather_market("degraded-two", "31°C", "0.50")
    worker, store = event_worker(
        tmp_path,
        [item],
        {(D("31"), D("31")): D("0.90")},
    )
    worker.settings = replace(
        worker.settings,
        weather_policy=replace(
            worker.settings.weather_policy,
            base_edge=D("0"),
            require_healthy_forecast=True,
            minimum_provider_count=2,
        ),
    )
    worker.forecast = EventForecast(
        {(D("31"), D("31")): D("0.90")},
        provider_names=("met-no", "nws"),
        provider_failures=(("open-meteo", "quota exhausted"),),
    )

    result = asyncio.run(worker.run_cycle(
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))

    assert result.weather_forecast_status == "degraded"
    assert result.weather_candidates == 1
    assert result.paper_trades == 1
    trade = store.read_records(store.trades_path)[0]
    assert trade["paper_executed"] is True


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


def test_entry_state_and_pending_audit_recover_when_audit_append_fails(
    tmp_path,
    monkeypatch,
):
    client = FakePublicClient(
        [market()],
        [book("yes-token", ask="0.45"), book("no-token", ask="0.45")],
    )
    store = PaperStore(tmp_path)
    original_append = store.append_unique_record
    failed = False

    def fail_first_trade_audit(path, payload, *, id_field):
        nonlocal failed
        if path == store.trades_path and not failed:
            failed = True
            raise OSError("simulated audit append crash")
        return original_append(path, payload, id_field=id_field)

    monkeypatch.setattr(store, "append_unique_record", fail_first_trade_audit)
    worker = PaperWorker(client=client, settings=settings(tmp_path), store=store)

    first = asyncio.run(worker.run_cycle())

    crashed_state = store.load_state()
    assert first.errors == 1
    assert crashed_state.cash == D("33.00")
    assert tuple(crashed_state.open_positions) == ("condition-1",)
    assert len(crashed_state.pending_audits) == 1
    assert store.read_records(store.trades_path) == ()

    restarted_store = PaperStore(tmp_path)
    restarted = PaperWorker(
        client=client,
        settings=settings(tmp_path),
        store=restarted_store,
    )
    assert restarted.state.pending_audits == {}
    assert len(restarted_store.read_records(restarted_store.trades_path)) == 1

    second = asyncio.run(restarted.run_cycle())
    assert second.paper_trades == 0
    assert restarted_store.load_state().cash == D("33.00")
    assert len(restarted_store.read_records(restarted_store.trades_path)) == 1


def test_entry_audit_restart_dedupes_crash_after_append_before_outbox_clear(
    tmp_path,
    monkeypatch,
):
    client = FakePublicClient(
        [market()],
        [book("yes-token", ask="0.45"), book("no-token", ask="0.45")],
    )
    store = PaperStore(tmp_path)
    original_save = store.save_state
    save_calls = 0

    def fail_outbox_clear_once(state):
        nonlocal save_calls
        save_calls += 1
        if save_calls == 2:
            raise OSError("simulated crash before outbox clear")
        original_save(state)

    monkeypatch.setattr(store, "save_state", fail_outbox_clear_once)
    worker = PaperWorker(client=client, settings=settings(tmp_path), store=store)
    result = asyncio.run(worker.run_cycle())

    assert result.errors == 1
    assert len(store.read_records(store.trades_path)) == 1
    assert len(store.load_state().pending_audits) == 1

    restarted_store = PaperStore(tmp_path)
    restarted = PaperWorker(
        client=client,
        settings=settings(tmp_path),
        store=restarted_store,
    )
    assert restarted.state.pending_audits == {}
    assert len(restarted_store.read_records(restarted_store.trades_path)) == 1
    assert restarted.state.cash == D("33.00")


def test_pending_audit_recovers_from_a_torn_final_jsonl_append(tmp_path):
    store = PaperStore(tmp_path)
    state = PaperState.new(D("37.50"))
    audit_id = "weather:condition-1:YES:2026-08-25T00:00:00+00:00"
    state.pending_audits[audit_id] = {
        "stream": "paper_trades",
        "payload": {"audit_id": audit_id, "paper_executed": True},
    }
    store.save_state(state)
    store.trades_path.write_bytes(b'{"audit_id":"torn"')

    restarted_store = PaperStore(tmp_path)
    worker = PaperWorker(
        client=FakePublicClient([], []),
        settings=settings(tmp_path),
        store=restarted_store,
    )

    assert worker.state.pending_audits == {}
    assert restarted_store.read_records(restarted_store.trades_path) == (
        {"audit_id": audit_id, "paper_executed": True},
    )
    assert restarted_store.trades_path.with_suffix(".jsonl.torn").is_file()


def test_jsonl_corruption_at_a_clean_record_boundary_remains_fatal(tmp_path):
    store = PaperStore(tmp_path)
    store.trades_path.write_bytes(b'{"audit_id":"broken"\n')
    with pytest.raises(ValueError, match="corrupt paper JSONL"):
        store.read_records(store.trades_path)


def test_valid_unterminated_final_jsonl_record_is_delimited_before_next_append(tmp_path):
    store = PaperStore(tmp_path)
    store.trades_path.write_bytes(b'{"audit_id":"first"}')

    assert store.read_records(store.trades_path) == ({"audit_id": "first"},)
    store.append_record(store.trades_path, {"audit_id": "second"})

    assert store.read_records(store.trades_path) == (
        {"audit_id": "first"},
        {"audit_id": "second"},
    )
    assert store.trades_path.read_bytes().count(b"\n") == 2


def test_settlement_state_and_pending_audit_recover_without_duplicate_credit(
    tmp_path,
    monkeypatch,
):
    resolved = market(closed=True)
    resolved.outcomes.yes.price = D("1")
    resolved.outcomes.no.price = D("0")
    store = PaperStore(tmp_path)
    state = PaperState.new(D("37.50"))
    state.cash = D("33.00")
    state.open_positions["condition-1"] = {
        "strategy": "complete_set",
        "event_key": "condition-1",
        "market_id": "market-1",
        "condition_id": "condition-1",
        "shares": "5",
        "all_in_cost": "4.50",
    }
    store.save_state(state)
    original_append = store.append_unique_record
    failed = False

    def fail_first_settlement_audit(path, payload, *, id_field):
        nonlocal failed
        if path == store.settlements_path and not failed:
            failed = True
            raise OSError("simulated settlement audit crash")
        return original_append(path, payload, id_field=id_field)

    monkeypatch.setattr(store, "append_unique_record", fail_first_settlement_audit)
    client = FakePublicClient([resolved], [])
    worker = PaperWorker(client=client, settings=settings(tmp_path), store=store)

    first = asyncio.run(worker.run_cycle())

    crashed_state = store.load_state()
    assert first.settlements == 0
    assert first.errors == 1
    assert crashed_state.cash == D("38.00")
    assert crashed_state.realized_pnl == D("0.50")
    assert crashed_state.open_positions == {}
    assert len(crashed_state.pending_audits) == 1

    restarted_store = PaperStore(tmp_path)
    restarted = PaperWorker(
        client=client,
        settings=settings(tmp_path),
        store=restarted_store,
    )
    assert restarted.state.pending_audits == {}
    audits = [
        row
        for row in restarted_store.read_records(restarted_store.settlements_path)
        if row.get("audit_id")
    ]
    assert len(audits) == 1
    asyncio.run(restarted.run_cycle())
    assert restarted_store.load_state().cash == D("38.00")
    assert len([
        row
        for row in restarted_store.read_records(restarted_store.settlements_path)
        if row.get("audit_id")
    ]) == 1


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


def test_paper_early_exit_uses_full_bid_depth_fees_and_persists_audit(tmp_path):
    opened = market()
    opened.trading.fees_enabled = True
    opened.trading.fee_schedule = SimpleNamespace(rate=D("0.05"))
    exit_book = book("yes-token", ask="0.50", bid="0.40")
    exit_book.bids = (
        SimpleNamespace(price=D("0.40"), size=D("3")),
        SimpleNamespace(price=D("0.50"), size=D("2")),
    )
    client = FakePublicClient([opened], [exit_book])
    store = PaperStore(tmp_path)
    state = PaperState.new(D("37.50"))
    state.cash = D("36.00")
    state.open_positions[opened.condition_id] = {
        "strategy": "weather_directional",
        "event_key": "weather:singapore:2026-08-25",
        "opened_at": "2026-08-24T00:00:00+00:00",
        "market_id": opened.id,
        "condition_id": opened.condition_id,
        "side": "YES",
        "token_id": "yes-token",
        "shares": "5",
        "all_in_cost": "1.50",
    }
    store.save_state(state)
    worker = PaperWorker(
        client=client,
        settings=settings(
            tmp_path,
            early_exit_enabled=True,
            early_exit_target_return=D("0.25"),
            early_exit_min_profit=D("0.10"),
        ),
        store=store,
    )

    assert asyncio.run(worker._exit_positions("2026-08-25T00:00:00+00:00")) == (1, 0)
    updated = store.load_state()
    assert updated.open_positions == {}
    assert updated.cash == D("38.139000")
    assert updated.realized_pnl == D("0.639000")
    exits = store.read_records(store.exits_path)
    assert len(exits) == 1
    assert exits[0]["exit_vwap"] == "0.44"
    assert exits[0]["exit_fee"] == "0.061000"
    assert exits[0]["net_proceeds"] == "2.139000"
    assert exits[0]["realized_pnl"] == "0.639000"


def test_paper_early_exit_refuses_shallow_bid_books_without_mutation(tmp_path):
    opened = market()
    shallow = book("yes-token", ask="0.50", bid="0.40")
    shallow.bids = (SimpleNamespace(price=D("0.90"), size=D("4")),)
    client = FakePublicClient([opened], [shallow])
    store = PaperStore(tmp_path)
    state = PaperState.new(D("37.50"))
    state.cash = D("36.00")
    state.open_positions[opened.condition_id] = {
        "strategy": "weather_directional",
        "market_id": opened.id,
        "condition_id": opened.condition_id,
        "side": "YES",
        "token_id": "yes-token",
        "shares": "5",
        "all_in_cost": "1.50",
    }
    store.save_state(state)
    worker = PaperWorker(
        client=client,
        settings=settings(tmp_path, early_exit_enabled=True),
        store=store,
    )

    assert asyncio.run(worker._exit_positions("2026-08-25T00:00:00+00:00")) == (0, 0)
    updated = store.load_state()
    assert tuple(updated.open_positions) == (opened.condition_id,)
    assert updated.cash == D("36.00")
    assert updated.realized_pnl == D("0")
    assert not store.exits_path.exists()


def test_paper_hybrid_exit_keeps_runner_then_exits_at_higher_target(tmp_path):
    opened = market()
    opened.trading.fees_enabled = True
    opened.trading.fee_schedule = SimpleNamespace(rate=D("0.05"))
    exit_book = book("yes-token", ask="0.50", bid="0.40")
    client = FakePublicClient([opened], [exit_book])
    store = PaperStore(tmp_path)
    state = PaperState.new(D("37.50"))
    state.cash = D("36.00")
    state.open_positions[opened.condition_id] = {
        "strategy": "weather_directional",
        "event_key": "weather:singapore:2026-08-25",
        "opened_at": "2026-08-24T00:00:00+00:00",
        "market_id": opened.id,
        "condition_id": opened.condition_id,
        "side": "YES",
        "token_id": "yes-token",
        "shares": "5",
        "all_in_cost": "1.50",
    }
    store.save_state(state)
    worker = PaperWorker(
        client=client,
        settings=settings(
            tmp_path,
            early_exit_enabled=True,
            early_exit_target_return=D("0.25"),
            early_exit_min_profit=D("0.10"),
            hybrid_exit_enabled=True,
            hybrid_exit_fraction=D("0.75"),
            hybrid_runner_target_return=D("0.50"),
        ),
        store=store,
    )

    assert asyncio.run(worker._exit_positions("2026-08-25T00:00:00+00:00")) == (1, 0)
    partial_state = store.load_state()
    partial = partial_state.open_positions[opened.condition_id]
    assert partial["shares"] == "1.25"
    assert partial["all_in_cost"] == "0.3750"
    assert partial["hybrid_exit_done"] is True
    assert partial_state.cash == D("37.455000")
    assert partial_state.realized_pnl == D("0.330000")
    partial_exit = store.read_records(store.exits_path)[0]
    assert partial_exit["shares"] == "3.75"
    assert partial_exit["remaining_shares"] == "1.25"
    assert partial_exit["reason"] == "paper hybrid partial-exit profit target"

    exit_book.bids = (SimpleNamespace(price=D("0.80"), size=D("100")),)
    assert asyncio.run(worker._exit_positions("2026-08-25T01:00:00+00:00")) == (1, 0)
    final_state = store.load_state()
    assert final_state.open_positions == {}
    assert final_state.cash == D("38.445000")
    assert final_state.realized_pnl == D("0.945000")
    exits = store.read_records(store.exits_path)
    assert len(exits) == 2
    assert exits[1]["shares"] == "1.25"
    assert exits[1]["target_return"] == "0.50"
    assert exits[1]["reason"] == "paper hybrid runner profit target"


def test_weather_settlement_calibrates_provider_probability_against_yes_outcome(tmp_path):
    resolved = market(closed=True)
    resolved.id = "weather-calibration"
    resolved.condition_id = "weather-calibration-condition"
    resolved.outcomes.yes.price = D("1")
    resolved.outcomes.no.price = D("0")
    client = FakePublicClient([resolved], [])
    store = PaperStore(tmp_path)
    state = PaperState.new(D("37.50"))
    state.cash = D("37.00")
    state.open_positions[resolved.condition_id] = {
        "strategy": "weather_directional",
        "event_key": "weather:singapore:2026-08-25",
        "market_id": resolved.id,
        "condition_id": resolved.condition_id,
        "side": "NO",
        "shares": "5",
        "all_in_cost": "0.50",
        "model_probability": "0.80",
        "provider_probabilities": {"met-no": "0.20"},
        "city": "singapore",
        "lead_days": 1,
    }
    store.save_state(state)

    class RecordingForecast:
        def __init__(self):
            self.calls = []

        def record_outcome(self, **kwargs):
            self.calls.append(kwargs)

    forecast = RecordingForecast()
    worker = PaperWorker(
        client=client,
        settings=settings(tmp_path),
        store=store,
        forecast=forecast,
    )

    assert asyncio.run(worker._settle_positions("2026-08-26T00:00:00+00:00")) == (1, 0)
    assert forecast.calls == [{
        "city": "singapore",
        "lead_days": 1,
        "outcome": 1,
        "provider_probabilities": (("met-no", D("0.20")),),
    }]


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


def test_run_paper_closes_async_public_client_after_success(tmp_path):
    class ClosableClient(FakePublicClient):
        def __init__(self):
            super().__init__([], [])
            self.closed = False

        async def close(self):
            self.closed = True

    client = ClosableClient()
    asyncio.run(run_paper(settings(tmp_path), cycles=1, client_factory=lambda: client))

    assert client.closed
    assert not (tmp_path / "worker.pid").exists()


def test_run_paper_closes_async_public_client_after_worker_failure(
    tmp_path,
    monkeypatch,
):
    class ClosableClient(FakePublicClient):
        def __init__(self):
            super().__init__([], [])
            self.closed = False

        async def close(self):
            self.closed = True

    async def fail_cycle(self, *, now=None):
        raise RuntimeError("simulated worker failure")

    monkeypatch.setattr(PaperWorker, "run_cycle", fail_cycle)
    client = ClosableClient()

    with pytest.raises(RuntimeError, match="simulated worker failure"):
        asyncio.run(run_paper(
            settings(tmp_path),
            cycles=1,
            client_factory=lambda: client,
        ))

    assert client.closed
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


def test_unique_event_ids_are_loaded_once_per_store_and_dedupe_after_restart(
    tmp_path,
    monkeypatch,
):
    store = PaperStore(tmp_path)
    original_read = store.read_records
    read_calls = 0

    def count_reads(path):
        nonlocal read_calls
        read_calls += 1
        return original_read(path)

    monkeypatch.setattr(store, "read_records", count_reads)
    for index in range(10):
        assert store.append_unique_record(
            store.weather_events_path,
            {"event_id": f"event-{index}"},
            id_field="event_id",
        )
    assert read_calls == 1

    restarted = PaperStore(tmp_path)
    assert not restarted.append_unique_record(
        restarted.weather_events_path,
        {"event_id": "event-5"},
        id_field="event_id",
    )
    assert restarted.append_unique_record(
        restarted.weather_events_path,
        {"event_id": "event-10"},
        id_field="event_id",
    )
    assert len(restarted.read_records(restarted.weather_events_path)) == 11


def test_store_refuses_a_second_live_worker_pid(tmp_path):
    store = PaperStore(tmp_path)
    store.acquire()
    try:
        with pytest.raises(RuntimeError, match="already running"):
            PaperStore(tmp_path).acquire()
    finally:
        store.release()


def test_weather_environment_defaults_enable_resolver_observations_and_widen_paper_bounds(
    tmp_path,
    monkeypatch,
):
    for name in (
        "V3_PAPER_WEATHER_OBSERVATIONS_ENABLED",
        "V3_PAPER_WEATHER_MIN_PRICE",
        "V3_PAPER_WEATHER_MAX_PRICE",
        "V3_PAPER_WEATHER_MAX_ORDER_NOTIONAL",
    ):
        monkeypatch.delenv(name, raising=False)
    policy = PaperSettings.from_env(tmp_path).weather_policy
    assert policy.observations_enabled
    assert policy.min_price == D("0.02")
    assert policy.max_price == D("0.98")
    assert policy.max_order_notional == D("5")


def event_weather_market(market_id: str, outcome: str, yes_price: str):
    item = market()
    item.id = market_id
    item.condition_id = f"condition-{market_id}"
    item.question = (
        f"Will the highest temperature in Singapore be {outcome} on August 25?"
    )
    item.state.neg_risk = True
    item.state.end_date = datetime(2026, 8, 25, 12, tzinfo=timezone.utc)
    item.outcomes.yes.token_id = f"yes-{market_id}"
    item.outcomes.yes.price = D(yes_price)
    item.outcomes.no.token_id = f"no-{market_id}"
    item.outcomes.no.price = D("1") - D(yes_price)
    item.resolution.source = "https://www.weather.gov/wrh/timeseries?site=wsss"
    return item


class EventWeatherClient(FakePublicClient):
    def list_markets(self, **kwargs):
        self.list_calls += 1
        self.last_list_kwargs = kwargs
        if kwargs.get("tag_id") == 84:
            return FakePaginator(self.markets)
        return FakePaginator(())


class EventForecast:
    def __init__(
        self,
        probabilities,
        *,
        provider_names=("met-no",),
        provider_failures=(),
    ):
        self.probabilities = probabilities
        self.provider_names = tuple(provider_names)
        self.provider_failures = tuple(provider_failures)

    async def forecast(self, contract, *, now=None):
        key = (contract.display_lower, contract.display_upper)
        probability = self.probabilities[key]
        return EnsembleForecast(
            raw_probability=probability,
            distribution_probability=probability,
            ensemble_mean_c=D("31"),
            ensemble_std_c=D("1"),
            n_members=100,
            lead_days=0,
            provider_count=len(self.provider_names),
            provider_names=self.provider_names,
            provider_probabilities=tuple(
                (name, probability) for name in self.provider_names
            ),
            provider_failures=self.provider_failures,
        )


def event_worker(
    tmp_path,
    markets,
    probabilities,
    *,
    observation_provider=None,
    observations_enabled=False,
):
    books = []
    for item in markets:
        yes_price = D(str(item.outcomes.yes.price))
        no_price = D(str(item.outcomes.no.price))
        books.extend((
            book(
                str(item.outcomes.yes.token_id),
                ask=str(yes_price),
                bid=str(yes_price - D("0.01")),
                condition_id=str(item.condition_id),
                neg_risk=True,
            ),
            book(
                str(item.outcomes.no.token_id),
                ask=str(no_price),
                bid=str(no_price - D("0.01")),
                condition_id=str(item.condition_id),
                neg_risk=True,
            ),
        ))
    policy = WeatherPaperPolicy(
        enabled=True,
        observations_enabled=observations_enabled,
        min_price=D("0.02"),
        max_price=D("0.98"),
        max_order_notional=D("5"),
        base_edge=D("1"),
    )
    store = PaperStore(tmp_path)
    worker = PaperWorker(
        client=EventWeatherClient(markets, books),
        settings=settings(tmp_path, weather_policy=policy),
        store=store,
        forecast=EventForecast(probabilities),
        observation_provider=observation_provider,
    )
    return worker, store


class RecordingObservationProvider:
    def __init__(self, *, error=None):
        self.error = error
        self.calls = []

    async def adjust_probability(self, contract, base_probability, *, station_id):
        self.calls.append((contract.event_key, station_id))
        if self.error is not None:
            raise self.error
        return ObservationBoundResult(base_probability, True, D("31"))


def test_worker_uses_injected_observations_and_reports_availability_and_errors(tmp_path):
    item = event_weather_market("observed", "31°C", "0.50")
    probabilities = {(D("31"), D("31")): D("0.50")}
    available_provider = RecordingObservationProvider()
    available_worker, available_store = event_worker(
        tmp_path / "available",
        [item],
        probabilities,
        observation_provider=available_provider,
        observations_enabled=True,
    )
    available = asyncio.run(available_worker.run_cycle(
        now=datetime(2026, 8, 25, 1, tzinfo=timezone.utc),
    ))
    assert available_provider.calls == [
        ("weather:singapore:2026-08-25", "WSSS")
    ]
    assert available.weather_observations_available == 1
    assert available.weather_observation_errors == 0
    assert available_store.read_status()[
        "weather_observations_available_this_cycle"
    ] == 1

    error_provider = RecordingObservationProvider(
        error=ConnectionError("NOAA unavailable"),
    )
    error_worker, error_store = event_worker(
        tmp_path / "error",
        [item],
        probabilities,
        observation_provider=error_provider,
        observations_enabled=True,
    )
    failed = asyncio.run(error_worker.run_cycle(
        now=datetime(2026, 8, 25, 1, tzinfo=timezone.utc),
    ))
    assert failed.weather_observations_available == 0
    assert failed.weather_observation_errors == 1
    assert failed.errors == 1
    assert error_store.read_status()["weather_observation_errors_this_cycle"] == 1
    weather_rows = error_store.read_records(error_store.weather_scans_path)
    evaluation = next(row for row in weather_rows if row.get("market_id") == "observed")
    assert evaluation["tradeable"] is False
    assert evaluation["observation_error"] == "ConnectionError: NOAA unavailable"


def test_forecast_outage_is_unhealthy_and_distinct_from_zero_candidates(tmp_path):
    markets = [
        event_weather_market("outage-1", "31°C", "0.40"),
        event_weather_market("outage-2", "32°C", "0.40"),
    ]
    worker, store = event_worker(
        tmp_path,
        markets,
        {
            (D("31"), D("31")): D("0.40"),
            (D("32"), D("32")): D("0.40"),
        },
    )

    class UnavailableForecast:
        async def forecast(self, contract, *, now=None):
            raise ForecastUnavailableError(
                "Open-Meteo global rate-limit backoff active",
                provider_global=True,
            )

    worker.forecast = UnavailableForecast()
    summary = asyncio.run(worker.run_cycle(
        now=datetime(2026, 8, 24, tzinfo=timezone.utc),
    ))

    assert summary.weather_markets_discovered == 2
    assert summary.weather_forecast_unavailable == 2
    assert summary.weather_markets_modeled == 0
    assert summary.weather_side_evaluable == 0
    assert summary.weather_candidates == 0
    assert summary.weather_forecast_status == "unavailable"
    assert summary.errors == 1
    status = store.read_status()
    assert status["healthy"] is False
    assert status["weather_forecast_status"] == "unavailable"
    assert status["weather_forecast_unavailable_this_cycle"] == 2
    forecast_errors = [
        row
        for row in store.read_records(store.weather_scans_path)
        if row.get("status") == "weather_forecast_error"
    ]
    assert len(forecast_errors) == 1


def test_weather_maker_shadow_telemetry_never_changes_paper_state(tmp_path):
    item = event_weather_market("maker", "31°C", "0.50")
    worker, store = event_worker(tmp_path, [item], {(D("31"), D("31")): D("0.50")})
    before = worker.state.to_json()

    summary = asyncio.run(worker.run_cycle(now=datetime(2026, 8, 24, tzinfo=timezone.utc)))

    after = store.load_state().to_json()
    assert summary.paper_trades == 0
    assert after["cash"] == before["cash"]
    assert after["open_positions"] == before["open_positions"]
    assert after["traded_conditions"] == before["traded_conditions"]
    assert after["traded_strategy_keys"] == before["traded_strategy_keys"]
    assert after["total_paper_trades"] == before["total_paper_trades"]
    row = store.read_records(store.weather_scans_path)[0]
    assert row["maker_shadow"]["execution_status"] == "not_submitted"
    assert row["maker_shadow"]["cash_delta"] == "0"
    assert row["maker_shadow"]["inventory_delta"] == "0"
    assert store.read_records(store.candidates_path) == ()
    assert store.read_records(store.trades_path) == ()


def test_weather_event_store_writes_one_deterministic_row_per_event_cycle(tmp_path):
    item = event_weather_market("single", "31°C", "0.40")
    worker, store = event_worker(tmp_path, [item], {(D("31"), D("31")): D("0.40")})
    asyncio.run(worker.run_cycle(now=datetime(2026, 8, 24, tzinfo=timezone.utc)))

    rows = store.read_records(store.weather_events_path)
    assert len(rows) == 1
    assert rows[0]["event_id"] == (
        f"{worker.state.started_at}:1:weather:singapore:2026-08-25:C"
    )
    assert rows[0]["execution_status"] == "not_executed"
    assert rows[0]["public_data_only"] is True


def test_weather_event_store_dedupes_an_existing_event_cycle_id(tmp_path):
    item = event_weather_market("dedupe", "31°C", "0.40")
    worker, store = event_worker(tmp_path, [item], {(D("31"), D("31")): D("0.40")})
    event_id = f"{worker.state.started_at}:1:weather:singapore:2026-08-25:C"
    store.append_record(store.weather_events_path, {"event_id": event_id})

    asyncio.run(worker.run_cycle(now=datetime(2026, 8, 24, tzinfo=timezone.utc)))

    assert len(store.read_records(store.weather_events_path)) == 1


def test_incomplete_weather_partition_never_reports_basket_profit(tmp_path):
    markets = [
        event_weather_market("low", "29°C or below", "0.20"),
        event_weather_market("high", "31°C or higher", "0.20"),
    ]
    worker, store = event_worker(
        tmp_path,
        markets,
        {(None, D("29")): D("0.40"), (D("31"), None): D("0.60")},
    )
    asyncio.run(worker.run_cycle(now=datetime(2026, 8, 24, tzinfo=timezone.utc)))

    row = store.read_records(store.weather_events_path)[0]
    assert row["complete_partition"] is False
    assert row["indicative_profitable_basket"] is False
    assert row["basket_net_profit"] == "0"
    assert row["execution_status"] == "not_executed"


def test_complete_weather_partition_records_only_unverified_cross_market_hypothesis(tmp_path):
    markets = [
        event_weather_market("low", "29°C or below", "0.10"),
        event_weather_market("middle", "between 30-31°C", "0.20"),
        event_weather_market("high", "32°C or higher", "0.20"),
    ]
    worker, store = event_worker(
        tmp_path,
        markets,
        {
            (None, D("29")): D("0.20"),
            (D("30"), D("31")): D("0.30"),
            (D("32"), None): D("0.50"),
        },
    )
    before = worker.state.to_json()
    summary = asyncio.run(worker.run_cycle(now=datetime(2026, 8, 24, tzinfo=timezone.utc)))

    row = store.read_records(store.weather_events_path)[0]
    assert row["complete_partition"] is True
    assert row["model_probability_sum"] == "1.00"
    assert row["model_probability_residual"] == "0.00"
    assert D(row["basket_gross_cost"]) == D("2.50")
    assert D(row["basket_fees"]) == D("0")
    assert D(row["basket_payout"]) == D("5")
    assert D(row["basket_net_profit"]) == D("2.50")
    assert row["indicative_profitable_basket"] is False
    assert row["unverified_cross_market_hypothesis"] is True
    assert row["negative_risk_verified"] is True
    assert row["event_membership_verified"] is False
    assert row["execution_status"] == "not_executed"
    assert len(row["contracts"]) == 3
    assert len(row["maker_shadows"]) == 3
    assert all(
        shadow["execution_status"] == "not_submitted"
        for shadow in row["maker_shadows"]
    )
    after = store.load_state().to_json()
    assert after["cash"] == before["cash"]
    assert after["open_positions"] == before["open_positions"]
    assert after["traded_strategy_keys"] == before["traded_strategy_keys"]
    assert after["total_paper_trades"] == before["total_paper_trades"]
    assert summary.weather_events_observed == 1
    assert summary.weather_complete_partitions == 1
    assert summary.weather_indicative_profitable_baskets == 0
    status = store.read_status()
    assert status["weather_events_observed_this_cycle"] == 1
    assert status["weather_complete_partitions_this_cycle"] == 1
    assert status["weather_indicative_profitable_baskets_this_cycle"] == 0
    assert status["weather_observations_available_this_cycle"] == 0
    assert status["weather_observation_errors_this_cycle"] == 0
    assert row["partition_violations"] == []
    assert row["monotonic_violations"] == []
