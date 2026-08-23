from decimal import Decimal

from src.v3.ledger import EventLedger, LedgerEvent
from src.v3.reconciliation import (
    LocalSnapshot,
    Reconciler,
    RemoteOrder,
    RemotePosition,
    RemoteSnapshot,
)


def D(value: str) -> Decimal:
    return Decimal(value)


def test_event_ledger_is_append_only_and_idempotent(tmp_path):
    ledger = EventLedger(tmp_path / "events.db")
    event = LedgerEvent.create("order.accepted", {"order_id": "o-1"}, event_id="event-1")
    assert ledger.append(event)
    assert not ledger.append(event)
    assert [e.event_id for e in ledger.events()] == ["event-1"]


def test_unknown_remote_state_blocks_trading_but_never_creates_actions():
    local = LocalSnapshot(cash=D("10"), position_tokens=frozenset(), order_ids=frozenset())
    remote = RemoteSnapshot(
        cash=D("10"),
        positions=(RemotePosition("dota-condition", "dota-token", D("5"), D("2")),),
        open_orders=(RemoteOrder("manual-order", "dota-condition", "dota-token"),),
    )
    report = Reconciler().compare(local, remote)
    assert not report.safe_to_trade
    assert report.unknown_positions == remote.positions
    assert report.unknown_orders == remote.open_orders
    assert report.actions == ()


def test_explicit_external_positions_are_observe_only_and_not_touched():
    local = LocalSnapshot(cash=D("10"), position_tokens=frozenset(), order_ids=frozenset())
    remote = RemoteSnapshot(
        cash=D("10"),
        positions=(RemotePosition("dota-condition", "dota-token", D("5"), D("2")),),
        open_orders=(),
    )
    report = Reconciler(external_condition_ids={"dota-condition"}).compare(local, remote)
    assert report.safe_to_trade
    assert report.external_positions == remote.positions
    assert report.unknown_positions == ()
    assert report.actions == ()


def test_cash_mismatch_is_reported_with_decimal_precision():
    local = LocalSnapshot(cash=D("10.00"), position_tokens=frozenset(), order_ids=frozenset())
    remote = RemoteSnapshot(cash=D("9.97"), positions=(), open_orders=())
    report = Reconciler(cash_tolerance=D("0.01")).compare(local, remote)
    assert not report.safe_to_trade
    assert report.cash_delta == D("-0.03")
