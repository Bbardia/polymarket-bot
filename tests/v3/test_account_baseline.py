"""User-approved account-baseline semantics (2026-10-03).

Pre-bot positions are external, resolved positions carry only their payout
value as exposure, unexplained cash inflows may be allowed explicitly, and
trade history is replayed only from the recorded baseline.
"""
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from src.v3.api import UnifiedPolymarketAPI
from src.v3.config import V3Settings
from src.v3.live_service import LiveOrderService
from src.v3.reconciliation import (
    LocalSnapshot, Reconciler, RemoteOrder, RemotePosition, RemoteSnapshot,
)

D = Decimal


def _local(cash="100", qty=None, cost=None):
    qty = qty or {}
    return LocalSnapshot(cash=D(cash), position_tokens=frozenset(qty), order_ids=frozenset(),
                         position_quantities=qty, position_cost_basis=cost or {})


def _pos(cond, token, size="10", current="0", initial="2", redeemable=False):
    return RemotePosition(cond, token, D(size), D(current), D(initial), redeemable=redeemable)


def test_external_positions_do_not_block_but_new_unknown_ones_do():
    reconciler = Reconciler(external_condition_ids={"old"})
    remote = RemoteSnapshot(D("100"), (_pos("old", "t-old"),), ())
    assert reconciler.compare(_local(), remote).safe_to_trade
    remote = RemoteSnapshot(D("100"), (_pos("old", "t-old"), _pos("new", "t-new")), ())
    report = reconciler.compare(_local(), remote)
    assert not report.safe_to_trade and len(report.unknown_positions) == 1


def test_cash_inflow_allowed_only_when_opted_in_and_outflow_always_blocks():
    remote_up = RemoteSnapshot(D("100.25"), (), ())
    remote_down = RemoteSnapshot(D("99.50"), (), ())
    assert not Reconciler().compare(_local(), remote_up).safe_to_trade
    lenient = Reconciler(allow_cash_inflows=True)
    assert lenient.compare(_local(), remote_up).safe_to_trade
    assert not lenient.compare(_local(), remote_down).safe_to_trade


def test_cost_basis_tolerance_is_exact_by_default():
    remote = RemoteSnapshot(D("98.10"), (_pos("c", "t", size="10", current="2", initial="1.905"),), ())
    local = _local(cash="98.10", qty={"t": D("10")}, cost={"t": D("1.90")})
    assert not Reconciler().compare(local, remote).safe_to_trade
    assert Reconciler(cost_tolerance=D("0.01")).compare(local, remote).safe_to_trade
    assert not Reconciler(cost_tolerance=D("0.001")).compare(local, remote).safe_to_trade


def _risk_state(reconciler, remote):
    service = LiveOrderService.__new__(LiveOrderService)
    service._reconciler = reconciler
    return service._risk_state(remote, daily_pnl=D("0"), peak_equity=D("110"))


def test_risk_state_excludes_external_and_resolved_positions_from_caps():
    remote = RemoteSnapshot(
        D("100"),
        (
            _pos("old", "t-old", current="0", initial="50"),
            _pos("lost", "t-lost", current="0", initial="2", redeemable=True),
            _pos("won", "t-won", current="10", initial="2", redeemable=True),
            _pos("open", "t-open", current="1", initial="2"),
        ),
        (RemoteOrder("o1", "pending", "t-p", D("1.5")),),
    )
    state = _risk_state(Reconciler(external_condition_ids={"old"}), remote)
    assert state.equity == D("111")
    assert state.open_positions == 1
    assert state.total_exposure == D("10") + D("2") + D("1.5")
    assert "old" not in state.event_exposure and state.event_exposure["lost"] == D("0")


def test_risk_state_without_external_counts_everything():
    remote = RemoteSnapshot(D("100"), (_pos("old", "t-old", current="0", initial="50"),), ())
    state = _risk_state(Reconciler(), remote)
    assert state.open_positions == 1 and state.total_exposure == D("50")


class _Paginator:
    def __init__(self, rows):
        self.rows = rows

    def __aiter__(self):
        async def gen():
            yield SimpleNamespace(items=self.rows)
        return gen()


def _trade(trade_id, epoch):
    return SimpleNamespace(
        id=trade_id, condition_id="c", token_id="t", taker_order_id="tk", side="BUY",
        trader_side="TAKER", status="CONFIRMED", price="0.2", size="5", fee_rate_bps="0",
        matched_at=epoch, updated_at=None, maker_orders=[], transaction_hash=None,
    )


def test_trade_history_after_baseline_filters_and_passes_cursor():
    calls = []

    class Client:
        def list_account_trades(self, **kwargs):
            calls.append(kwargs)
            return _Paginator([_trade("old", 1_000), _trade("new", 2_000)])

    api = UnifiedPolymarketAPI(settings=V3Settings())
    api._secure_client = Client()
    all_rows = asyncio.run(api.fetch_account_trades(max_items=10, page_limit=2))
    recent = asyncio.run(api.fetch_account_trades(max_items=10, page_limit=2, after=1_500))
    assert [t.trade_id for t in all_rows] == ["old", "new"]
    assert [t.trade_id for t in recent] == ["new"]
    assert calls == [{}, {"after": "1500"}]
    assert recent[0].matched_at == datetime.fromtimestamp(2_000, tz=timezone.utc)
