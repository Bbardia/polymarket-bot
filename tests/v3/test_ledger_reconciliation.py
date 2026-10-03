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


def test_event_ledger_serializes_decimal_payloads_losslessly(tmp_path):
    ledger = EventLedger(tmp_path / "events.db")
    event = LedgerEvent.create("fill.confirmed", {"price": D("0.20"), "size": D("2")})
    assert ledger.append(event)
    restored = tuple(ledger.events())[0]
    assert restored.payload == {"price": "0.20", "size": "2"}


def test_in_memory_event_ledger_keeps_connection_state_between_operations():
    ledger = EventLedger(":memory:")
    event = LedgerEvent.create("test", {"value": "ok"}, event_id="memory-event")
    assert ledger.append(event)
    assert tuple(ledger.events())[0].event_id == "memory-event"


def test_unknown_remote_state_blocks_trading_but_never_creates_actions():
    local = LocalSnapshot(cash=D("10"), position_tokens=frozenset(), order_ids=frozenset())
    remote = RemoteSnapshot(
        cash=D("10"),
        positions=(RemotePosition("dota-condition", "dota-token", D("5"), D("2"), D("2")),),
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
        positions=(RemotePosition("dota-condition", "dota-token", D("5"), D("2"), D("2")),),
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


def test_missing_remote_local_position_and_order_block_trading():
    local = LocalSnapshot(
        cash=D("10"), position_tokens=frozenset({"token-1"}), order_ids=frozenset({"order-1"})
    )
    remote = RemoteSnapshot(cash=D("10"), positions=(), open_orders=())
    report = Reconciler().compare(local, remote)
    assert not report.safe_to_trade
    assert report.missing_positions == ("token-1",)
    assert report.missing_orders == ("order-1",)


def test_exactly_reconciled_remote_positions_and_orders_are_safe():
    local = LocalSnapshot(
        cash=D("10"), position_tokens=frozenset({"token-1"}), order_ids=frozenset({"order-1"}),
        position_quantities={"token-1": D("5")}, position_cost_basis={"token-1": D("2")},
    )
    remote = RemoteSnapshot(
        cash=D("10"),
        positions=(RemotePosition("condition-1", "token-1", D("5"), D("2"), D("2")),),
        open_orders=(RemoteOrder("order-1", "condition-1", "token-1", D("0.80")),),
    )
    report = Reconciler().compare(local, remote)
    assert report.safe_to_trade
    assert report.missing_positions == ()
    assert report.missing_orders == ()


def test_remote_open_order_without_remaining_notional_blocks_trading():
    remote = RemoteSnapshot(
        cash=D("10"), positions=(),
        open_orders=(RemoteOrder("order-1", "condition-1", "token-1"),),
    )
    report = Reconciler().compare(
        LocalSnapshot(cash=D("10"), position_tokens=frozenset(), order_ids=frozenset({"order-1"})),
        remote,
    )
    assert not report.safe_to_trade
    assert report.incomplete_orders == ("order-1",)


def test_partial_fill_quantity_drift_blocks_trading():
    local = LocalSnapshot(
        cash=D("10"), position_tokens=frozenset({"token-1"}), order_ids=frozenset(),
        position_quantities={"token-1": D("2")}, position_cost_basis={"token-1": D("0.8")},
    )
    remote = RemoteSnapshot(
        cash=D("10"),
        positions=(RemotePosition("c1", "token-1", D("3"), D("1.2"), D("1.2")),),
        open_orders=(),
    )
    report = Reconciler().compare(local, remote)
    assert not report.safe_to_trade
    assert report.position_mismatches == ("token-1",)


def test_cost_basis_drift_and_missing_local_position_detail_block_trading():
    remote = RemoteSnapshot(
        cash=D("10"),
        positions=(RemotePosition("c1", "token-1", D("2"), D("1"), D("0.9")),),
        open_orders=(),
    )
    local = LocalSnapshot(
        cash=D("10"), position_tokens=frozenset({"token-1"}), order_ids=frozenset(),
        position_quantities={"token-1": D("2")}, position_cost_basis={"token-1": D("0.8")},
    )
    report = Reconciler().compare(local, remote)
    assert not report.safe_to_trade
    assert report.position_mismatches == ("token-1",)
    legacy = Reconciler().compare(
        LocalSnapshot(D("10"), frozenset({"token-1"}), frozenset()), remote
    )
    assert not legacy.safe_to_trade
    assert legacy.position_mismatches == ("token-1",)


def test_duplicate_remote_position_and_order_identities_block_trading():
    remote = RemoteSnapshot(
        cash=D("10"),
        positions=(
            RemotePosition("c1", "token-1", D("2"), D("1"), D("1")),
            RemotePosition("c1", "token-1", D("3"), D("1.5"), D("1.5")),
        ),
        open_orders=(
            RemoteOrder("order-1", "c1", "token-1", D("0.5")),
            RemoteOrder("order-1", "c1", "token-1", D("0.5")),
        ),
    )
    report = Reconciler().compare(
        LocalSnapshot(D("10"), frozenset({"token-1"}), frozenset({"order-1"})), remote
    )
    assert not report.safe_to_trade


def test_malformed_runtime_snapshot_types_fail_closed_without_exception():
    report = Reconciler().compare(
        LocalSnapshot(cash="10", position_tokens=frozenset(), order_ids=frozenset()),
        RemoteSnapshot(cash=D("10"), positions=(), open_orders=()),
    )
    assert not report.safe_to_trade
    assert report.invalid_snapshot
