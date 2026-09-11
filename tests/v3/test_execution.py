import asyncio
from decimal import Decimal
from types import SimpleNamespace

from src.v3.execution import V3OrderExecutor
from src.v3.ledger import EventLedger
from src.v3.risk import AccountRiskState, OrderIntent, RiskEngine, RiskLimits


def D(value: str) -> Decimal:
    return Decimal(value)


def risk_engine() -> RiskEngine:
    return RiskEngine(RiskLimits(
        max_capital=D("50"), reserve_fraction=D("0.25"),
        max_order_notional=D("2"), max_event_exposure=D("5"),
        max_open_orders=4, max_positions=5, daily_loss_limit=D("3"),
        max_drawdown_fraction=D("0.10"), max_quote_age_seconds=120,
        max_order_ttl_seconds=300,
    ))


def account_state(**overrides) -> AccountRiskState:
    values = dict(
        equity=D("50"), cash=D("50"), total_exposure=D("0"), event_exposure={},
        open_orders=0, open_positions=0, daily_pnl=D("0"), peak_equity=D("50"),
        reconciled=True, unknown_remote_positions=0, unknown_remote_orders=0,
    )
    values.update(overrides)
    return AccountRiskState(**values)


def order_intent(**overrides) -> OrderIntent:
    values = dict(
        condition_id="condition", token_id="token", side="BUY", price=D("0.20"),
        shares=D("5"), estimated_fee=D("0"), post_only=True, ttl_seconds=180,
        quote_age_seconds=1,
    )
    values.update(overrides)
    return OrderIntent(**values)


def test_executor_submits_post_only_gtd_but_does_not_book_resting_order_as_fill(tmp_path):
    calls = []

    class FakeClient:
        async def place_limit_order(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(
                ok=True, order_id="order-1", status="live",
                making_amount=D("1"), taking_amount=D("5"),
                trade_ids=(), transactions_hashes=(),
            )

    ledger = EventLedger(tmp_path / "events.db")
    executor = V3OrderExecutor(
        FakeClient(), risk_engine(), ledger, clock=lambda: 1_000,
        live_execution_authorized=True,
    )
    result = asyncio.run(executor.submit(order_intent(), account_state()))

    assert result.accepted
    assert result.order.confirmed_size == D("0")
    assert calls == [{
        "token_id": "token", "price": D("0.20"), "size": D("5"),
        "side": "BUY", "post_only": True, "expiration": 1_240,
    }]
    events = list(ledger.events())
    assert [event.event_type for event in events] == ["order.accepted"]
    assert events[0].payload["status"] == "live"


def test_executor_never_calls_api_when_risk_rejects(tmp_path):
    class FailClient:
        async def place_limit_order(self, **kwargs):
            raise AssertionError("API must not be called")

    disabled = V3OrderExecutor(
        FailClient(), risk_engine(), EventLedger(tmp_path / "disabled.db"), clock=lambda: 1_000
    )
    disabled_result = asyncio.run(disabled.submit(order_intent(), account_state()))
    assert not disabled_result.accepted
    assert "authorization disabled" in disabled_result.reason

    executor = V3OrderExecutor(
        FailClient(), risk_engine(), EventLedger(tmp_path / "events.db"),
        clock=lambda: 1_000, live_execution_authorized=True,
    )
    result = asyncio.run(executor.submit(order_intent(post_only=False), account_state()))
    assert not result.accepted
    assert "post-only" in result.reason


def test_ttl_minimum_rejected_before_api(tmp_path):
    class Client:
        async def place_limit_order(self, **kwargs): raise AssertionError('no API')
    executor=V3OrderExecutor(Client(),risk_engine(),EventLedger(tmp_path/'ttl.db'),live_execution_authorized=True)
    result=asyncio.run(executor.submit(order_intent(ttl_seconds=60),account_state()))
    assert not result.accepted and '121' in result.reason


def test_bounded_retry_and_kill_switch(tmp_path):
    calls=[]
    class Client:
        async def place_limit_order(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(ok=False,code=425,message='retry') if len(calls)<3 else SimpleNamespace(ok=True,order_id='o',status='live')
        async def cancel_all(self): return {'canceled':['o'],'not_canceled':{}}
    async def sleep(_): pass
    executor=V3OrderExecutor(Client(),risk_engine(),EventLedger(tmp_path/'retry.db'),live_execution_authorized=True,sleep=sleep)
    result=asyncio.run(executor.submit(order_intent(ttl_seconds=121),account_state()))
    assert result.accepted and len(calls)==3
    assert asyncio.run(executor.kill_switch())['status']=='cancel_requested'
    assert not asyncio.run(executor.submit(order_intent(),account_state())).accepted


def test_post_only_mode_retry_exhaustion_and_ambiguous_transport(tmp_path):
    import pytest
    count=[]
    class Client:
        async def place_limit_order(self, **kwargs):
            count.append(1)
            return SimpleNamespace(ok=False,code=503,message='post_only_mode')
    async def sleep(_): pass
    executor=V3OrderExecutor(Client(),risk_engine(),EventLedger(tmp_path/'503.db'),live_execution_authorized=True,sleep=sleep)
    assert not asyncio.run(executor.submit(order_intent(),account_state())).accepted
    assert len(count)==3
    class Ambiguous:
        async def place_limit_order(self, **kwargs): raise TimeoutError('unknown acceptance')
    executor=V3OrderExecutor(Ambiguous(),risk_engine(),EventLedger(tmp_path/'ambiguous.db'),live_execution_authorized=True,sleep=sleep)
    with pytest.raises(TimeoutError): asyncio.run(executor.submit(order_intent(),account_state()))
