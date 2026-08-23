import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.v3.paper import (
    PaperSettings,
    PaperStore,
    PaperWorker,
    paper_status,
    run_paper,
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


def book(token_id: str, *, ask: str) -> SimpleNamespace:
    return SimpleNamespace(
        market="condition-1",
        condition_id="condition-1",
        token_id=token_id,
        timestamp=None,
        bids=(SimpleNamespace(price=D("0.40"), size=D("100")),),
        asks=(SimpleNamespace(price=D(ask), size=D("100")),),
        min_order_size=D("5"),
        tick_size=D("0.01"),
        neg_risk=False,
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
