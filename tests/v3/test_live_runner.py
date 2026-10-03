import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.v3 import live_shadow
from src.v3.config import V3Settings
from src.v3.execution import ExecutionResult
from src.v3.ledger import EventLedger, LedgerEvent
from src.v3.live_runner import (
    LiveRunnerSettings,
    LiveStore,
    LiveTradingRunner,
    baseline_from,
    local_snapshot,
    record_expired_orders,
)
from src.v3.live_shadow import LiveShadowSettings
from src.v3.orders import OrderAggregate
from src.v3.reconciliation import Reconciler, RemoteOrder, RemotePosition, RemoteSnapshot
from src.v3.streaming import StreamEventProcessor

D = Decimal
NOW = datetime(2026, 10, 3, 14, 0, tzinfo=timezone.utc)


def _accept(ledger, order_id, *, token="tok", expiration=None, size="10"):
    ledger.append(LedgerEvent.create("order.submission.started", {"client_order_id": f"c-{order_id}"}))
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": f"c-{order_id}", "order_id": order_id, "status": "live",
        "condition_id": "cond", "token_id": token, "side": "BUY", "price": "0.19",
        "requested_size": size, "post_only": True,
        "expiration": expiration if expiration is not None else int(NOW.timestamp()) + 960,
    }))


def test_local_snapshot_excludes_reported_taker_rate_on_maker_fills():
    processor = SimpleNamespace(orders={}, active_order_ids=frozenset())
    order = OrderAggregate.new(client_order_id="c", token_id="tok", side="BUY", requested_size=D("10"))
    order.confirmed_size, order.confirmed_notional, order.confirmed_fees = D("10"), D("1.90"), D("0.15")
    processor.orders["o1"] = order
    snap = local_snapshot(processor, D("100"))
    assert snap.cash == D("98.10")
    assert snap.position_quantities == {"tok": D("10")}
    assert snap.position_cost_basis == {"tok": D("1.90")}


def test_baseline_marks_existing_positions_external_and_refuses_open_orders():
    remote = RemoteSnapshot(D("237.13"), (RemotePosition("old", "t", D("5"), D("0"), D("2"), True),), ())
    state = baseline_from(remote, NOW, frozenset({"manual"}))
    assert state["external_condition_ids"] == ["manual", "old"]
    assert state["baseline_cash"] == "237.13" and state["baseline_epoch"] == int(NOW.timestamp())
    with pytest.raises(RuntimeError):
        baseline_from(RemoteSnapshot(D("1"), (), (RemoteOrder("o", "c", "t", D("1")),)), NOW, frozenset())


def test_expired_orders_marked_only_after_grace_and_when_absent(tmp_path):
    ledger = EventLedger(tmp_path / "ledger.sqlite")
    expiry = int(NOW.timestamp())
    _accept(ledger, "gone", expiration=expiry)
    _accept(ledger, "resting", expiration=expiry)
    _accept(ledger, "recent", expiration=expiry + 3_000)
    remote = RemoteSnapshot(D("100"), (), (RemoteOrder("resting", "cond", "tok", D("1.9")),))
    processor = StreamEventProcessor(ledger)
    assert record_expired_orders(processor, ledger, remote, NOW + timedelta(seconds=300),
                                 grace_seconds=600) == []
    processor = StreamEventProcessor(ledger)
    marked = record_expired_orders(processor, ledger, remote, NOW + timedelta(seconds=700),
                                   grace_seconds=600)
    assert marked == ["gone"]
    replayed = StreamEventProcessor(ledger)
    assert replayed.active_order_ids == frozenset({"resting", "recent"})
    assert not replayed.reconciliation_required
    # Idempotent on the next cycle.
    assert record_expired_orders(replayed, ledger, remote, NOW + timedelta(seconds=800),
                                 grace_seconds=600) == []


class _Service:
    def __init__(self, *, accept=True):
        self.accept = accept
        self.submitted = []

    async def recover_trade_history(self, *, max_items, page_limit):
        return {"imported_count": 0, "lifecycle_clear": True}

    async def submit(self, intent, local, context):
        self.submitted.append((intent, local, context))
        order = OrderAggregate.new(client_order_id="c1", token_id=intent.token_id, side="BUY",
                                   requested_size=intent.shares)
        if self.accept:
            order.accept(order_id="ord-1", status="live")
            return ExecutionResult(True, "live", order)
        return ExecutionResult(False, "post_only_would_cross: crosses", order)


class _API:
    def __init__(self, cash="237.13", positions=(), orders=()):
        self.remote = RemoteSnapshot(D(cash), tuple(positions), tuple(orders))

    async def fetch_remote_snapshot(self):
        return self.remote

    async def get_verified_market_context(self, condition_id, token_id):
        return SimpleNamespace(book_hash="h1", tick_size=D("0.01"), min_order_size=D("5"),
                               accepting_orders=True, rules_verified=True, disputed=False)


def _evaluation(event_key="toronto:2026-10-04", condition="cond-new"):
    return SimpleNamespace(
        strategy="weather_directional", event_key=event_key, paper_tradeable=True,
        decision=SimpleNamespace(net_edge=D("0.10"), calibrated_probability=D("0.30"),
                                 minimum_edge=D("0.05")),
        condition_id=condition, token_id=f"tok-{condition}", question="Will it be 18C?",
        side="YES", city="toronto", shares=D("10"), ask=D("0.20"), bid=D("0.18"),
        maker_shadow=SimpleNamespace(best_bid=D("0.18"), best_ask=D("0.20")),
        book_timestamp=NOW, book_hash="h1", decision_timestamp=NOW,
    )


def _runner(tmp_path, monkeypatch, *, api=None, service=None, evaluations=None, state=None):
    from src.v3 import v7_weather_intent
    monkeypatch.setattr(live_shadow, "station_metadata_reason", lambda path, city: None)
    monkeypatch.setattr(live_shadow, "propose_v7_weather_order", lambda *a, **k: (
        v7_weather_intent.V7WeatherOrderProposal(True, "ok", price=D("0.19"), shares=D("10"),
                                                 expected_edge=D("0.11"), quote_age_seconds=5)))

    async def universe(**kwargs):
        return SimpleNamespace(evaluations=tuple(evaluations or (_evaluation(),)),
                               markets_evaluated=1, forecast_status="available", errors=())

    from src.v3 import live_runner
    monkeypatch.setattr(live_runner, "evaluate_weather_universe", universe)
    store = LiveStore(tmp_path / "live")
    store.save_state(state or {
        "baseline_at": NOW.isoformat(), "baseline_epoch": int(NOW.timestamp()),
        "baseline_cash": "237.13", "baseline_equity": "237.13",
        "external_condition_ids": ["cond-old"], "peak_equity": "237.13", "event_orders": {},
    })
    ledger = EventLedger(store.ledger_path)
    settings = V3Settings(max_capital=D("100"), max_order_notional=D("2"), reserve_fraction=D("0.25"),
                          max_daily_loss=D("10"), max_drawdown_amount=D("10"))
    shadow = LiveShadowSettings(data_dir=store.data_dir)
    runner = LiveTradingRunner(
        service=service or _Service(), ledger=ledger,
        reconciler=Reconciler(external_condition_ids={"cond-old"}, cost_tolerance=D("0.01"),
                              allow_cash_inflows=True),
        runner_settings=LiveRunnerSettings(shadow=shadow),
        api=api or _API(), settings=settings, shadow=shadow, store=store,
        weather_client=None, forecast=None, observation_provider=None,
    )
    return runner, store


def test_cycle_submits_through_service_and_records_event_order(tmp_path, monkeypatch):
    service = _Service()
    runner, store = _runner(tmp_path, monkeypatch, service=service)
    status = asyncio.run(runner.run_cycle(now=NOW))
    assert status["outcomes_this_cycle"] == {"accepted": 1}
    intent, local, context = service.submitted[0]
    assert intent.post_only and intent.side == "BUY" and intent.all_in_notional == D("1.90")
    assert context.daily_pnl == D("0") and context.peak_equity == D("237.13")
    state = json.loads(store.state_path.read_text())
    assert state["event_orders"]["toronto:2026-10-04"][0]["order_id"] == "ord-1"


def test_resting_order_blocks_requote_of_same_event(tmp_path, monkeypatch):
    service = _Service()
    api = _API(orders=(RemoteOrder("ord-1", "cond-new", "tok-cond-new", D("1.9")),))
    runner, store = _runner(tmp_path, monkeypatch, service=service, api=api)
    runner.state["event_orders"] = {"toronto:2026-10-04": [
        {"at": NOW.isoformat(), "order_id": "ord-1", "token_id": "tok-cond-new"}]}
    _accept(runner.ledger, "ord-1", token="tok-cond-new")
    status = asyncio.run(runner.run_cycle(now=NOW))
    assert service.submitted == []
    assert status["outcomes_this_cycle"] == {"skipped": 1}


def test_external_condition_is_never_traded(tmp_path, monkeypatch):
    service = _Service()
    runner, _ = _runner(tmp_path, monkeypatch, service=service,
                        evaluations=(_evaluation(condition="cond-old"),))
    status = asyncio.run(runner.run_cycle(now=NOW))
    assert service.submitted == [] and status["outcomes_this_cycle"] == {"skipped": 1}


def test_unknown_position_blocks_all_entries(tmp_path, monkeypatch):
    service = _Service()
    api = _API(positions=(RemotePosition("cond-x", "tok-x", D("5"), D("1"), D("1")),))
    runner, _ = _runner(tmp_path, monkeypatch, service=service, api=api)
    status = asyncio.run(runner.run_cycle(now=NOW))
    assert service.submitted == []
    assert status["entry_block_reason"] == "account reconciliation blocked entries"


def test_unexplained_cash_outflow_blocks_but_inflow_does_not(tmp_path, monkeypatch):
    service = _Service()
    runner, _ = _runner(tmp_path, monkeypatch, service=service, api=_API(cash="237.50"))
    assert asyncio.run(runner.run_cycle(now=NOW))["entry_block_reason"] is None
    service2 = _Service()
    runner2, _ = _runner(tmp_path / "b", monkeypatch, service=service2, api=_API(cash="236.00"))
    status = asyncio.run(runner2.run_cycle(now=NOW))
    assert service2.submitted == [] and status["entry_block_reason"] == "account reconciliation blocked entries"


def test_drawdown_from_peak_blocks_entries(tmp_path, monkeypatch):
    service = _Service()
    state = {"baseline_at": NOW.isoformat(), "baseline_epoch": int(NOW.timestamp()),
             "baseline_cash": "227.00", "baseline_equity": "237.13",
             "external_condition_ids": [], "peak_equity": "237.13", "event_orders": {},
             "day": NOW.date().isoformat(), "day_start_equity": "227.00"}
    runner, _ = _runner(tmp_path, monkeypatch, service=service, api=_API(cash="227.00"), state=state)
    status = asyncio.run(runner.run_cycle(now=NOW))
    assert service.submitted == [] and status["entry_block_reason"] == "maximum drawdown reached"


def test_rejected_order_is_logged_not_recorded(tmp_path, monkeypatch):
    runner, store = _runner(tmp_path, monkeypatch, service=_Service(accept=False))
    status = asyncio.run(runner.run_cycle(now=NOW))
    assert status["outcomes_this_cycle"] == {"rejected": 1}
    assert json.loads(store.state_path.read_text())["event_orders"] == {}


class _SdkAPI:
    """Fake API behind the real LiveOrderService, executor and SDK adapter."""

    def __init__(self, settings, remote):
        self.settings = settings
        self.remote = remote
        self.trades = ()
        self.trade_after = []
        self.created = []

    async def initialize_secure_client(self):
        return self

    async def fetch_remote_snapshot(self):
        return self.remote

    async def fetch_account_trades(self, *, max_items, page_limit, after=None):
        self.trade_after.append(after)
        return tuple(t for t in self.trades if after is None or t.matched_at.timestamp() >= after)

    async def get_verified_market_context(self, condition_id, token_id):
        return SimpleNamespace(
            condition_id=condition_id, token_id=token_id, condition_matches=True,
            token_matches=True, tick_size=D("0.01"), min_order_size=D("5"), fee_rate=D("0.05"),
            fee_exponent=D("1"), taker_only=True, accepting_orders=True, rules_verified=True,
            disputed=False, book_timestamp=datetime.now(timezone.utc), book_hash="h1",
        )

    async def create_limit_order(self, **kwargs):
        self.created.append(kwargs)
        return {"signed": kwargs}

    async def post_order(self, signed):
        return SimpleNamespace(ok=True, order_id="sdk-ord-1", status="live")


def test_end_to_end_baseline_submit_fill_and_reconcile(tmp_path, monkeypatch):
    from src.v3.live_runner import _start
    from src.v3 import live_runner
    from src.v3.reconciliation import RemoteTrade, RemoteTradeMaker

    old = tuple(RemotePosition(f"old-{i}", f"t-old-{i}", D("5"), D("0"), D("0.4"), True)
                for i in range(126))
    settings = V3Settings(
        live_enabled=True, paper_trading=False, live_confirmation="I_UNDERSTAND_REAL_MONEY",
        private_key="0x" + "1" * 64, wallet_address="0x" + "2" * 40,
        max_capital=D("100"), max_order_notional=D("2"), reserve_fraction=D("0.25"),
        max_daily_loss=D("10"), max_drawdown_amount=D("10"),
    )
    api = _SdkAPI(settings, RemoteSnapshot(D("237.13"), old, ()))
    monkeypatch.setattr(live_runner, "UnifiedPolymarketAPI", lambda settings: api)

    async def fake_account_client():
        return None
    api.initialize_account_client = fake_account_client
    api._authenticated_client = lambda: SimpleNamespace(close=_async_none)
    runner_settings = LiveRunnerSettings(shadow=LiveShadowSettings(data_dir=tmp_path / "live"))

    store, _, ledger, reconciler, service = asyncio.run(_start(settings, runner_settings))
    state = json.loads(store.state_path.read_text())
    assert len(state["external_condition_ids"]) == 126
    assert api.trade_after == [state["baseline_epoch"]]

    runner, _ = _runner_with(tmp_path, monkeypatch, store, ledger, reconciler, service, api, settings,
                             runner_settings)
    status = asyncio.run(runner.run_cycle())
    assert status["outcomes_this_cycle"] == {"accepted": 1}, status
    created = api.created[0]
    assert created["post_only"] is True and created["side"] == "BUY"
    assert created["price"] == D("0.19") and created["size"] == D("10")
    assert isinstance(created["expiration"], int)

    # Order rests: same event is not re-quoted.
    api.remote = RemoteSnapshot(D("237.13"), old, (RemoteOrder("sdk-ord-1", "cond-new", "tok-cond-new", D("1.9")),))
    status = asyncio.run(runner.run_cycle())
    assert status["outcomes_this_cycle"] == {"skipped": 1} and len(api.created) == 1
    assert status["reconciliation"]["safe_to_trade"] is True

    # Maker fill confirmed: position and cash reconcile despite the reported taker rate.
    api.trades = (RemoteTrade(
        trade_id="tr-1", condition_id="cond-new", token_id="tok-cond-new", taker_order_id="other",
        side="SELL", trader_side="MAKER", price=D("0.19"), size=D("10"), status="CONFIRMED",
        matched_at=datetime.now(timezone.utc), updated_at=None, fee_rate_bps=D("1000"),
        transaction_hash=None,
        maker_orders=(RemoteTradeMaker("sdk-ord-1", "tok-cond-new", "BUY", D("0.19"), D("10"), D("1000")),),
    ),)
    fill_position = RemotePosition("cond-new", "tok-cond-new", D("10"), D("2.1"), D("1.90"))
    api.remote = RemoteSnapshot(D("235.23"), old + (fill_position,), ())
    status = asyncio.run(runner.run_cycle())
    assert status["reconciliation"]["safe_to_trade"] is True, status["reconciliation"]
    assert status["bot_position_tokens"] == 1 and status["expected_bot_cash"] == D("235.23")
    assert status["outcomes_this_cycle"] == {"skipped": 1}


async def _async_none():
    return None


def _runner_with(tmp_path, monkeypatch, store, ledger, reconciler, service, api, settings, runner_settings):
    from src.v3 import live_runner, v7_weather_intent
    monkeypatch.setattr(live_shadow, "station_metadata_reason", lambda path, city: None)
    monkeypatch.setattr(live_shadow, "propose_v7_weather_order", lambda *a, **k: (
        v7_weather_intent.V7WeatherOrderProposal(True, "ok", price=D("0.19"), shares=D("10"),
                                                 expected_edge=D("0.11"), quote_age_seconds=5)))

    async def universe(**kwargs):
        return SimpleNamespace(evaluations=(_evaluation(),), markets_evaluated=1,
                               forecast_status="available", errors=())

    monkeypatch.setattr(live_runner, "evaluate_weather_universe", universe)
    runner = LiveTradingRunner(
        service=service, ledger=ledger, reconciler=reconciler, runner_settings=runner_settings,
        api=api, settings=settings, shadow=runner_settings.shadow, store=store,
        weather_client=None, forecast=None, observation_provider=None,
    )
    return runner, store
