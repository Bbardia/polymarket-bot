import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.v3.config import V3Settings
from src.v3.execution import V3OrderExecutor
from src.v3.ledger import EventLedger, LedgerEvent
from src.v3.live_service import LiveOrderService, LiveRiskContext
from src.v3.reconciliation import LocalSnapshot, RemoteOrder, RemotePosition, RemoteSnapshot, Reconciler
from src.v3.risk import AccountRiskState, OrderIntent, RiskEngine, RiskLimits
from src.v3.sdk_execution_adapter import SDKExecutionAdapter
from src.v3.reconciliation import RemoteTrade


def D(value: str) -> Decimal:
    return Decimal(value)


def _authorized_service_for_test(*args, **kwargs) -> LiveOrderService:
    service = LiveOrderService(*args, **kwargs)
    service._factory_authorized = True
    return service


def live_settings(**limits) -> V3Settings:
    return V3Settings(
        live_enabled=True, paper_trading=False,
        live_confirmation="I_UNDERSTAND_REAL_MONEY",
        private_key="0x" + "1" * 64, wallet_address="0x" + "2" * 40,
        max_capital=limits.get("max_capital", D("50")),
        max_order_notional=limits.get("max_order_notional", D("2")),
        reserve_fraction=limits.get("reserve_fraction", D("0.25")),
        max_daily_loss=limits.get("max_daily_loss", D("3")),
        max_drawdown_amount=limits.get("max_drawdown_amount", D("10")),
    )


def risk_engine(**overrides) -> RiskEngine:
    values = dict(
        max_capital=D("50"), reserve_fraction=D("0.25"), max_order_notional=D("2"),
        max_event_exposure=D("10"), max_open_orders=4, max_positions=5,
        daily_loss_limit=D("3"), max_drawdown_amount=D("10"),
        max_quote_age_seconds=120, max_order_ttl_seconds=300,
    )
    values.update(overrides)
    return RiskEngine(RiskLimits(**values))


def remote_snapshot(*, cash="10", positions=(), orders=()) -> RemoteSnapshot:
    return RemoteSnapshot(
        cash=D(cash),
        positions=tuple(positions),
        open_orders=tuple(orders),
    )


def local_snapshot(*, cash="10", tokens=(), orders=(), quantities=None, costs=None) -> LocalSnapshot:
    return LocalSnapshot(
        D(cash), frozenset(tokens), frozenset(orders),
        position_quantities=quantities, position_cost_basis=costs,
    )


def intent() -> OrderIntent:
    return OrderIntent(
        condition_id="condition-1", token_id="new-token", side="BUY",
        price=D("0.20"), shares=D("5"), estimated_fee=D("0"),
        post_only=True, ttl_seconds=180, quote_age_seconds=1,
    )


class FakeExecutor:
    def __init__(self):
        self.calls = []
        self.killed = False

    async def submit(self, intent, state):
        self.calls.append((intent, state))
        return SimpleNamespace(accepted=True, reason="live")

    async def kill_switch(self):
        self.killed = True
        return {"status": "cancel_requested", "requires_reconciliation": True}


class FakeAPI:
    def __init__(self, snapshot, settings=None):
        self.snapshot = snapshot
        self.settings = settings or live_settings()
        self.initialized = False
        self.fetches = 0
        self.trade_rows: tuple[RemoteTrade, ...] = ()
        self.trade_error: Exception | None = None
        self.trade_fetches = []
        self.market_fetches = 0
        self.market_context = SimpleNamespace(
            condition_id="condition-1", condition_matches=True, token_matches=True, token_id="new-token",
            tick_size=D("0.01"), min_order_size=D("5"), fee_rate=D("0"),
            accepting_orders=True, rules_verified=True, disputed=False,
            book_timestamp=datetime.now(timezone.utc), book_hash="book-hash",
        )

    async def get_verified_market_context(self, condition_id, token_id):
        self.market_fetches += 1
        return self.market_context

    async def initialize_secure_client(self):
        self.initialized = True
        return self

    async def fetch_remote_snapshot(self):
        self.fetches += 1
        if isinstance(self.snapshot, Exception):
            raise self.snapshot
        return self.snapshot

    async def fetch_account_trades(self, *, max_items, page_limit):
        self.trade_fetches.append((max_items, page_limit))
        assert max_items > 0 and page_limit > 0
        if self.trade_error:
            raise self.trade_error
        return self.trade_rows[:max_items]

    async def place_limit_order(self, **kwargs):
        return SimpleNamespace(ok=True, order_id="placed", status="live")

    async def cancel_all(self):
        return {"canceled": ["old"], "not_canceled": {}}

    async def create_limit_order(self, **kwargs):
        self.created_order = kwargs
        return {"signed": True}

    async def post_order(self, signed):
        self.posted_order = signed
        return SimpleNamespace(ok=True, order_id="sdk-placed", status="live")

    def list_open_orders(self):
        async def pages():
            yield SimpleNamespace(items=(SimpleNamespace(id="open-fake"),))
        return pages()

    async def cancel_orders(self, *, order_ids):
        self.canceled_order_ids = order_ids
        return {"canceled": order_ids, "not_canceled": {}}


def test_create_binds_sdk_adapter_to_initialized_client_and_routes_sdk_calls():
    settings = live_settings()
    api = FakeAPI(remote_snapshot(), settings=settings)
    service = asyncio.run(LiveOrderService.create(
        api=api, settings=settings, risk_engine=risk_engine(), ledger=EventLedger(":memory:"),
    ))

    assert api.initialized
    assert isinstance(service._executor._client, SDKExecutionAdapter)
    assert service._executor._client._client is api
    result = asyncio.run(service._executor._client.place_limit_order(
        token_id="token-fake", price=D("0.2"), size=D("5"), side="BUY",
        post_only=True, expiration=10**10,
    ))
    assert result.order_id == "sdk-placed"
    assert api.created_order == {
        "token_id": "token-fake", "price": D("0.2"), "size": D("5"),
        "side": "BUY", "post_only": True, "expiration": 10**10,
    }
    assert api.posted_order == {"signed": True}
    cancel_result = asyncio.run(service._executor._client.cancel_all())
    assert api.canceled_order_ids == ("open-fake",)
    assert cancel_result == {"canceled": ("open-fake",), "not_canceled": {}}


def test_create_requires_live_limits_and_matching_risk_engine_before_secure_client():
    settings = live_settings()
    api = FakeAPI(remote_snapshot(), settings=settings)
    ledger = EventLedger(":memory:")
    api.trade_rows = (RemoteTrade(
        trade_id="create-trade", condition_id="condition-1", token_id="new-token",
        taker_order_id="managed-1", side="BUY", trader_side="TAKER",
        price=D("0.2"), size=D("2"), status="CONFIRMED",
        matched_at=datetime.now(timezone.utc), updated_at=None,
        fee_rate_bps=D("0"), transaction_hash=None, maker_orders=(),
    ),)
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "client-1", "order_id": "managed-1", "token_id": "new-token",
        "side": "BUY", "requested_size": "5", "status": "live",
    }))
    service = asyncio.run(LiveOrderService.create(
        api=api, settings=settings, risk_engine=risk_engine(), ledger=ledger,
    ))
    from src.v3.streaming import StreamEventProcessor
    assert StreamEventProcessor(ledger).orders["managed-1"].confirmed_size == D("2")
    assert api.trade_fetches == [(10_000, 100)]

    settings = live_settings()
    api = FakeAPI(remote_snapshot(), settings=settings)
    ledger = EventLedger(":memory:")
    api.trade_error = RuntimeError("history unavailable")
    service = asyncio.run(LiveOrderService.create(
        api=api, settings=settings, risk_engine=risk_engine(), ledger=ledger,
    ))
    assert api.trade_fetches == [(10_000, 100)]
    result = asyncio.run(service.submit(
        intent(), local_snapshot(), LiveRiskContext(daily_pnl=D("0"), peak_equity=D("10")),
    ))
    assert not result.accepted and "lifecycle reconciliation" in result.reason

    missing = V3Settings(
        live_enabled=True, paper_trading=False,
        live_confirmation="I_UNDERSTAND_REAL_MONEY",
        private_key="0x" + "1" * 64, wallet_address="0x" + "2" * 40,
    )
    blocked_api = FakeAPI(remote_snapshot(), settings=missing)
    with pytest.raises(RuntimeError, match="V3_MAX_DAILY_LOSS"):
        asyncio.run(LiveOrderService.create(
            api=blocked_api, settings=missing, risk_engine=risk_engine(),
            ledger=EventLedger(":memory:"),
        ))
    assert not blocked_api.initialized


def test_create_rejects_config_and_risk_engine_mismatch_before_client_init():
    settings = live_settings()
    api = FakeAPI(remote_snapshot(), settings=settings)
    with pytest.raises(ValueError, match="risk engine limits do not match"):
        asyncio.run(LiveOrderService.create(
            api=api, settings=settings, risk_engine=risk_engine(max_capital=D("100")),
            ledger=EventLedger(":memory:"),
        ))
    assert not api.initialized


def test_manual_constructor_cannot_submit_or_cancel_without_factory_gate(tmp_path):
    api = FakeAPI(remote_snapshot())
    executor = FakeExecutor()
    service = LiveOrderService(api, executor, risk_engine(), EventLedger(tmp_path / "manual-constructor.db"), Reconciler())

    result = asyncio.run(service.submit(
        intent(), local_snapshot(),
        LiveRiskContext(daily_pnl=D("0"), peak_equity=D("10")),
    ))
    assert not result.accepted
    assert "gated factory" in result.reason
    assert api.market_fetches == 0 and api.fetches == 0
    assert executor.calls == []

    cancel = asyncio.run(service.kill_switch())
    assert cancel["status"] == "service_not_authorized"
    assert executor.killed is False


def test_service_rejects_executor_with_different_kill_latch_ledger(tmp_path):
    risk = risk_engine()
    executor = V3OrderExecutor(
        SimpleNamespace(), risk, EventLedger(tmp_path / "executor.db"), settings=live_settings(),
    )
    with pytest.raises(ValueError, match="share the service ledger"):
        LiveOrderService(
            FakeAPI(remote_snapshot()), executor, risk,
            EventLedger(tmp_path / "service.db"), Reconciler(),
        )


def test_submit_preflights_remote_state_and_uses_remote_risk_values(tmp_path):
    remote = remote_snapshot(
        cash="10",
        positions=(RemotePosition("condition-1", "token-1", D("5"), D("2.5"), D("2.6")),),
        orders=(RemoteOrder("order-1", "condition-1", "token-2", D("0.8")),),
    )
    api = FakeAPI(remote)
    executor = FakeExecutor()
    ledger = EventLedger(tmp_path / "preflight.db")
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "client-1", "order_id": "order-1",
        "token_id": "token-2", "side": "BUY", "requested_size": "4", "status": "live",
    }))
    service = _authorized_service_for_test(
        api=api, executor=executor, risk_engine=risk_engine(), ledger=ledger,
        reconciler=Reconciler(),
    )
    result = asyncio.run(service.submit(
        intent(), local_snapshot(
            cash="10", tokens=("token-1",), orders=("order-1",),
            quantities={"token-1": D("5")}, costs={"token-1": D("2.6")},
        ),
        LiveRiskContext(daily_pnl=D("0"), peak_equity=D("20")),
    ))
    assert result.accepted
    assert api.market_fetches == 1
    assert api.fetches == 1
    assert len(executor.calls) == 1
    state = executor.calls[0][1]
    assert state.cash == D("9.2")
    assert state.equity == D("12.5")
    assert state.total_exposure == D("3.4")
    assert state.event_exposure == {"condition-1": D("3.4")}
    assert state.open_positions == 1 and state.open_orders == 1
    assert state.reconciled


def test_submit_blocks_unknown_or_mismatched_remote_state_without_executor_call(tmp_path):
    remote = remote_snapshot(
        cash="10", positions=(RemotePosition("foreign", "unknown", D("2"), D("1")),)
    )
    api = FakeAPI(remote)
    executor = FakeExecutor()
    ledger = EventLedger(tmp_path / "blocked.db")
    service = _authorized_service_for_test(api, executor, risk_engine(), ledger, Reconciler())
    result = asyncio.run(service.submit(
        intent(), local_snapshot(cash="10"),
        LiveRiskContext(daily_pnl=D("0"), peak_equity=D("10")),
    ))
    assert not result.accepted
    assert "reconciliation" in result.reason
    assert executor.calls == []
    assert tuple(ledger.events())[-1].event_type == "account.preflight.blocked"


def test_submit_reconciles_durable_open_order_even_if_caller_omits_it(tmp_path):
    ledger = EventLedger(tmp_path / "durable-open-order.db")
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "client-1", "order_id": "durable-open",
        "token_id": "token-1", "side": "BUY", "requested_size": "5", "status": "live",
    }))
    api = FakeAPI(remote_snapshot())
    executor = FakeExecutor()
    service = _authorized_service_for_test(api, executor, risk_engine(), ledger, Reconciler())

    result = asyncio.run(service.submit(
        intent(), local_snapshot(orders=()),
        LiveRiskContext(daily_pnl=D("0"), peak_equity=D("10")),
    ))

    assert not result.accepted
    assert "reconciliation" in result.reason
    assert executor.calls == []
    blocked = tuple(ledger.events())[-1]
    assert blocked.event_type == "account.preflight.blocked"
    assert blocked.payload["missing_order_count"] == 1


def test_submit_blocks_unverified_or_stale_market_context_before_account_read(tmp_path):
    for reason, update in (
        ("market and token identity", {"token_matches": False}),
        ("market rules are unverified", {"rules_verified": False}),
        ("market is not accepting orders", {"accepting_orders": False}),
        ("order-book quote is stale", {"book_timestamp": datetime(2020, 1, 1, tzinfo=timezone.utc)}),
    ):
        api = FakeAPI(remote_snapshot())
        api.market_context = SimpleNamespace(**{**vars(api.market_context), **update})
        executor = FakeExecutor()
        service = _authorized_service_for_test(api, executor, risk_engine(), EventLedger(tmp_path / f"{reason}.db"), Reconciler())
        result = asyncio.run(service.submit(
            intent(), local_snapshot(),
            LiveRiskContext(daily_pnl=D("0"), peak_equity=D("10")),
        ))
        assert not result.accepted
        assert reason in result.reason
        assert api.fetches == 0
        assert executor.calls == []


def test_submit_blocks_fee_enabled_market_until_fee_curve_is_modeled(tmp_path):
    api = FakeAPI(remote_snapshot())
    api.market_context = SimpleNamespace(**{**vars(api.market_context), "fee_rate": D("0.25")})
    executor = FakeExecutor()
    service = _authorized_service_for_test(api, executor, risk_engine(), EventLedger(tmp_path / "fee.db"), Reconciler())
    result = asyncio.run(service.submit(
        intent(), local_snapshot(),
        LiveRiskContext(daily_pnl=D("0"), peak_equity=D("10")),
    ))
    assert not result.accepted
    assert "taker-only post-only" in result.reason
    assert api.fetches == 0
    assert executor.calls == []


def test_submit_allows_verified_taker_only_post_only_maker_schedule(tmp_path):
    api = FakeAPI(remote_snapshot())
    api.market_context = SimpleNamespace(**{
        **vars(api.market_context),
        "fee_rate": D("0.05"),
        "fee_exponent": D("1"),
        "taker_only": True,
        "fees_enabled": True,
    })
    executor = FakeExecutor()
    service = _authorized_service_for_test(
        api, executor, risk_engine(), EventLedger(tmp_path / "taker-only-maker.db"), Reconciler(),
    )

    result = asyncio.run(service.submit(
        intent(), local_snapshot(),
        LiveRiskContext(daily_pnl=D("0"), peak_equity=D("10")),
    ))

    assert result.accepted
    assert len(executor.calls) == 1
    assert api.fetches == 1


def test_submit_blocks_snapshot_errors_and_unresolved_submissions(tmp_path):
    api = FakeAPI(RuntimeError("private error text must not be persisted"))
    executor = FakeExecutor()
    ledger = EventLedger(tmp_path / "snapshot-failure.db")
    service = _authorized_service_for_test(api, executor, risk_engine(), ledger, Reconciler())
    result = asyncio.run(service.submit(
        intent(), local_snapshot(), LiveRiskContext(daily_pnl=D("0"), peak_equity=D("10")),
    ))
    assert not result.accepted
    assert executor.calls == []
    failed = tuple(ledger.events())[-1]
    assert failed.event_type == "account.preflight.failed"
    assert "private error text" not in str(failed.payload)

    ledger.append(LedgerEvent.create(
        "order.submission.started", {"client_order_id": "pending-1"}, event_id="pending-start"
    ))
    result = asyncio.run(service.submit(
        intent(), local_snapshot(), LiveRiskContext(daily_pnl=D("0"), peak_equity=D("10")),
    ))
    assert not result.accepted
    assert "lifecycle reconciliation required" in result.reason
    assert executor.calls == []


def test_kill_switch_requires_a_fresh_empty_open_order_snapshot(tmp_path):
    api = FakeAPI(remote_snapshot(orders=()))
    executor = FakeExecutor()
    ledger = EventLedger(tmp_path / "kill.db")
    service = _authorized_service_for_test(api, executor, risk_engine(), ledger, Reconciler())
    result = asyncio.run(service.kill_switch())
    assert result["status"] == "cancel_snapshot_empty"
    assert executor.killed
    assert api.fetches == 1


def test_kill_switch_does_not_claim_success_if_orders_remain(tmp_path):
    api = FakeAPI(remote_snapshot(orders=(RemoteOrder("still-open", "c", "t", D("1")),)))
    executor = FakeExecutor()
    service = _authorized_service_for_test(api, executor, risk_engine(), EventLedger(tmp_path / "kill.db"), Reconciler())
    result = asyncio.run(service.kill_switch())
    assert result["status"] == "cancel_unverified"
    assert result["open_orders_remaining"] == 1


def test_kill_switch_honors_explicit_not_canceled_response(tmp_path):
    class CancelExecutor(FakeExecutor):
        async def kill_switch(self):
            self.killed = True
            return {
                "status": "cancel_requested",
                "response": {"canceled": [], "not_canceled": {"order-1": "pending"}},
            }

    api = FakeAPI(remote_snapshot(orders=()))
    executor = CancelExecutor()
    service = _authorized_service_for_test(api, executor, risk_engine(), EventLedger(tmp_path / "cancel.db"), Reconciler())
    result = asyncio.run(service.kill_switch())
    assert result["status"] == "cancel_unverified"
    assert result["not_canceled_ids"] == ("order-1",)


def test_trade_history_import_replays_confirmed_trade_and_persists_latch(tmp_path):
    ledger_path = tmp_path / "history.db"
    ledger = EventLedger(ledger_path)
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "client-1", "order_id": "managed-1", "token_id": "new-token",
        "side": "BUY", "requested_size": "5", "status": "live",
    }))
    api = FakeAPI(remote_snapshot())
    api.trade_rows = (RemoteTrade(
        trade_id="trade-1", condition_id="condition-1", token_id="new-token",
        taker_order_id="managed-1", side="BUY", trader_side="TAKER",
        price=D("0.2"), size=D("2"), status="CONFIRMED",
        matched_at=datetime.now(timezone.utc), updated_at=None,
        fee_rate_bps=D("0"), transaction_hash=None, maker_orders=(),
    ),)
    service = _authorized_service_for_test(api, FakeExecutor(), risk_engine(), ledger, Reconciler())
    result = asyncio.run(service.recover_trade_history(max_items=10, page_limit=2))
    assert result == {"imported_count": 1, "lifecycle_clear": True}
    duplicate = asyncio.run(service.recover_trade_history(max_items=10, page_limit=2))
    assert duplicate == {"imported_count": 0, "lifecycle_clear": True}
    assert "safe_to_trade" not in duplicate
    from src.v3.streaming import StreamEventProcessor
    processor = StreamEventProcessor(ledger)
    assert processor.orders["managed-1"].confirmed_size == D("2")


def test_trade_history_timeout_latches_reconciliation(tmp_path):
    class HangingTradeAPI(FakeAPI):
        async def fetch_account_trades(self, *, max_items, page_limit):
            self.trade_fetches.append((max_items, page_limit))
            await asyncio.sleep(1)
            return ()

    ledger_path = tmp_path / "history-timeout.db"
    ledger = EventLedger(ledger_path)
    api = HangingTradeAPI(remote_snapshot())
    service = _authorized_service_for_test(api, FakeExecutor(), risk_engine(), ledger, Reconciler())
    result = asyncio.run(service.recover_trade_history(
        max_items=10, page_limit=2, timeout_seconds=0.01,
    ))
    assert result == {"imported_count": 0, "lifecycle_clear": False}
    assert api.trade_fetches == [(10, 2)]
    assert any(e.event_type == "stream.reconciliation_required" for e in ledger.events())
    restarted = EventLedger(ledger_path)
    from src.v3.streaming import StreamEventProcessor
    assert StreamEventProcessor(restarted).reconciliation_required


def test_trade_history_cancellation_latches_before_propagating(tmp_path):
    class HangingTradeAPI(FakeAPI):
        async def fetch_account_trades(self, *, max_items, page_limit):
            self.trade_fetches.append((max_items, page_limit))
            self.fetch_started.set()
            await asyncio.Event().wait()

    async def run_cancelled_fetch(service, api):
        task = asyncio.create_task(service.recover_trade_history(max_items=10, page_limit=2))
        await api.fetch_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    ledger_path = tmp_path / "history-cancelled.db"
    ledger = EventLedger(ledger_path)
    api = HangingTradeAPI(remote_snapshot())
    api.fetch_started = asyncio.Event()
    service = _authorized_service_for_test(api, FakeExecutor(), risk_engine(), ledger, Reconciler())
    asyncio.run(run_cancelled_fetch(service, api))

    events = ledger.events()
    latch = next(e for e in events if e.event_type == "stream.reconciliation_required")
    assert latch.payload["reason"] == "trade history read cancelled: CancelledError"
    from src.v3.streaming import StreamEventProcessor
    assert StreamEventProcessor(EventLedger(ledger_path)).reconciliation_required


@pytest.mark.parametrize("timeout_seconds", [0, -1, float("nan"), float("inf")])
def test_trade_history_timeout_must_be_finite_and_positive(timeout_seconds):
    service = _authorized_service_for_test(FakeAPI(remote_snapshot()), FakeExecutor(), risk_engine(),
                               EventLedger(":memory:"), Reconciler())
    with pytest.raises(ValueError, match="timeout_seconds"):
        asyncio.run(service.recover_trade_history(
            max_items=10, page_limit=2, timeout_seconds=timeout_seconds,
        ))


def test_trade_history_failures_unknown_trades_latch_across_restart_and_block_submit(tmp_path):
    ledger_path = tmp_path / "history-failure.db"
    ledger = EventLedger(ledger_path)
    api = FakeAPI(remote_snapshot())
    api.trade_error = RuntimeError("private read failure")
    service = _authorized_service_for_test(api, FakeExecutor(), risk_engine(), ledger, Reconciler())
    failed = asyncio.run(service.recover_trade_history(max_items=10, page_limit=2))
    assert failed["lifecycle_clear"] is False
    assert any(e.event_type == "stream.reconciliation_required" for e in ledger.events())

    restarted_ledger = EventLedger(ledger_path)
    api.trade_error = None
    api.trade_rows = (RemoteTrade(
        trade_id="unknown", condition_id="condition-1", token_id="new-token",
        taker_order_id="foreign-order", side="BUY", trader_side="TAKER",
        price=D("0.2"), size=D("1"), status="CONFIRMED",
        matched_at=datetime.now(timezone.utc), updated_at=None,
        fee_rate_bps=D("0"), transaction_hash=None, maker_orders=(),
    ),)
    restarted_executor = FakeExecutor()
    restarted = _authorized_service_for_test(api, restarted_executor, risk_engine(), restarted_ledger, Reconciler())
    recovered = asyncio.run(restarted.recover_trade_history(max_items=10, page_limit=2))
    assert recovered == {"imported_count": 0, "lifecycle_clear": False}
    result = asyncio.run(restarted.submit(
        intent(), local_snapshot(), LiveRiskContext(daily_pnl=D("0"), peak_equity=D("10")),
    ))
    assert not result.accepted and restarted_executor.calls == []


def test_cancel_pagination_overflow_latches_executor_and_blocks_followup_submit(monkeypatch, tmp_path):
    import src.v3.sdk_execution_adapter as adapter_module

    monkeypatch.setattr(adapter_module, "CANCEL_ALL_PAGE_LIMIT", 1)
    settings = live_settings()
    api = FakeAPI(remote_snapshot(orders=()), settings=settings)

    def endless_pages():
        async def iterate():
            yield SimpleNamespace(items=())
            yield SimpleNamespace(items=())
        return iterate()

    api.list_open_orders = endless_pages
    service = asyncio.run(LiveOrderService.create(
        api=api, settings=settings, risk_engine=risk_engine(),
        ledger=EventLedger(tmp_path / "bounded-cancel.db"),
    ))
    killed = asyncio.run(service.kill_switch())
    assert killed == {"status": "cancel_unknown", "requires_reconciliation": True}
    assert not hasattr(api, "canceled_order_ids")

    blocked = asyncio.run(service._executor.submit(
        intent(), AccountRiskState(equity=D("0"), cash=D("0"), total_exposure=D("0")),
    ))
    assert not blocked.accepted and blocked.reason == "kill switch latched"
