from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.v3.ledger import EventLedger, LedgerEvent
from src.v3.reconciliation import (
    RemotePosition, RemoteSnapshot, RemoteTrade, RemoteTradeMaker,
)
from src.v3.streaming import StreamEventProcessor, remote_snapshot_sha256

D = Decimal


def order_event():
    from types import SimpleNamespace
    return SimpleNamespace(
        topic="user", type="order",
        payload=SimpleNamespace(
            id="managed-order", owner="wallet", market="condition", asset_id="token",
            side="BUY", original_size=D("5"), size_matched=D("0"), price=D("0.31"),
            type="PLACEMENT", timestamp=None, created_at=None, expiration=None,
            order_type="GTD", status="LIVE",
        ),
    )


def trade_row(*, parent_fee, trade_id="remote-trade"):
    maker = RemoteTradeMaker(
        "managed-order", "token", "BUY", D("0.31"), D("5"), None,
    )
    return RemoteTrade(
        trade_id=trade_id, condition_id="condition", token_id="token",
        taker_order_id="external-order", side="BUY", trader_side="MAKER",
        price=D("0.69"), size=D("5"), status="CONFIRMED",
        matched_at=datetime(2020, 1, 1, tzinfo=timezone.utc), updated_at=None,
        fee_rate_bps=parent_fee, transaction_hash=None, maker_orders=(maker,),
    )


def test_managed_maker_can_use_explicit_zero_trade_fee_when_maker_fee_missing(tmp_path):
    processor = StreamEventProcessor(
        EventLedger(tmp_path / "ledger.sqlite"), managed_order_ids={"managed-order"},
    )
    processor.process(order_event())

    result = processor.import_remote_trade(trade_row(parent_fee=D("0")))

    assert result.accepted and not result.requires_reconciliation
    order = processor.orders["managed-order"]
    assert order.confirmed_size == D("5")
    assert order.confirmed_notional == D("1.55")
    assert order.confirmed_fees == D("0")
    restored = StreamEventProcessor(
        EventLedger(tmp_path / "ledger.sqlite"), managed_order_ids={"managed-order"},
    )
    assert not restored.reconciliation_required
    assert restored.orders["managed-order"].confirmed_size == D("5")


def test_managed_maker_still_blocks_if_both_fee_fields_missing(tmp_path):
    processor = StreamEventProcessor(
        EventLedger(tmp_path / "ledger.sqlite"), managed_order_ids={"managed-order"},
    )
    processor.process(order_event())

    result = processor.import_remote_trade(trade_row(parent_fee=None))

    assert not result.accepted and result.requires_reconciliation
    assert processor.orders["managed-order"].confirmed_size == D("0")


def _recovery_fixture(tmp_path):
    ledger = EventLedger(tmp_path / "recovery.sqlite")
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "client-1", "order_id": "managed-order",
        "status": "live", "condition_id": "condition", "token_id": "token",
        "side": "BUY", "requested_size": "5", "post_only": True,
    }))
    processor = StreamEventProcessor(ledger)
    processor.require_reconciliation(
        "remote maker trade fee rate is unknown",
        source_trade_id="remote-trade", source_order_id="managed-order",
    )
    processor.require_reconciliation(
        "remote maker trade fee rate is unknown",
        source_trade_id="remote-trade", source_order_id="managed-order",
    )
    remote = RemoteSnapshot(
        cash=D("98.45"),
        positions=(RemotePosition(
            condition_id="condition", token_id="token", size=D("5"),
            current_value=D("0.0775"), initial_value=D("1.55"),
        ),),
        open_orders=(),
    )
    return ledger, processor, remote


def test_zero_fee_recovery_resolves_only_evidenced_latches_and_replays_trade(tmp_path):
    ledger, processor, remote = _recovery_fixture(tmp_path)

    result = processor.resolve_zero_fee_maker_reconciliation(
        trade=trade_row(parent_fee=D("0")),
        account_trades_since_baseline=(trade_row(parent_fee=D("0")),),
        remote=remote,
        baseline_cash=D("100"), external_condition_ids=frozenset(),
        cash_flow_net=D("0"), account_snapshot_sha256=remote_snapshot_sha256(remote),
    )

    assert result.accepted and "2 fee-only latch events" in result.reason
    restored = StreamEventProcessor(ledger)
    assert not restored.reconciliation_required
    imported = restored.import_remote_trade(trade_row(parent_fee=D("0")))
    assert imported.accepted and not imported.requires_reconciliation
    assert restored.orders["managed-order"].confirmed_size == D("5")
    assert restored.orders["managed-order"].confirmed_fees == D("0")


def test_zero_fee_recovery_refuses_account_mismatch_without_clearing_latch(tmp_path):
    ledger, processor, remote = _recovery_fixture(tmp_path)
    bad_remote = RemoteSnapshot(
        cash=D("98.40"), positions=remote.positions, open_orders=(),
    )

    with pytest.raises(ValueError, match="cash do not match"):
        processor.resolve_zero_fee_maker_reconciliation(
            trade=trade_row(parent_fee=D("0")),
            account_trades_since_baseline=(trade_row(parent_fee=D("0")),),
            remote=bad_remote,
            baseline_cash=D("100"), external_condition_ids=frozenset(),
            cash_flow_net=D("0"), account_snapshot_sha256=remote_snapshot_sha256(bad_remote),
        )

    assert sum(event.event_type == "stream.reconciliation_resolved" for event in ledger.events()) == 0
    assert StreamEventProcessor(ledger).reconciliation_required


def test_zero_fee_recovery_refuses_latch_for_a_different_trade_even_if_later(tmp_path):
    ledger, processor, remote = _recovery_fixture(tmp_path)
    processor.require_reconciliation(
        "remote maker trade fee rate is unknown",
        source_trade_id="second-trade", source_order_id="second-order",
    )
    trade = trade_row(parent_fee=D("0"))

    with pytest.raises(ValueError, match="unscoped or ineligible"):
        processor.resolve_zero_fee_maker_reconciliation(
            trade=trade,
            account_trades_since_baseline=(trade,),
            remote=remote,
            baseline_cash=D("100"), external_condition_ids=frozenset(),
            cash_flow_net=D("0"), account_snapshot_sha256=remote_snapshot_sha256(remote),
        )

    assert sum(event.event_type == "stream.reconciliation_resolved" for event in ledger.events()) == 0
    assert StreamEventProcessor(ledger).reconciliation_required


def test_zero_fee_recovery_refuses_legacy_unscoped_latch(tmp_path):
    ledger, _, remote = _recovery_fixture(tmp_path)
    ledger.append(LedgerEvent.create(
        "stream.reconciliation_required",
        {"reason": "remote maker trade fee rate is unknown"},
        occurred_at="2019-12-31T23:59:00+00:00",
    ))
    processor = StreamEventProcessor(ledger)
    trade = trade_row(parent_fee=D("0"))

    with pytest.raises(ValueError, match="unscoped or ineligible"):
        processor.resolve_zero_fee_maker_reconciliation(
            trade=trade,
            account_trades_since_baseline=(trade,),
            remote=remote,
            baseline_cash=D("100"), external_condition_ids=frozenset(),
            cash_flow_net=D("0"), account_snapshot_sha256=remote_snapshot_sha256(remote),
        )

    assert sum(event.event_type == "stream.reconciliation_resolved" for event in ledger.events()) == 0
    assert StreamEventProcessor(ledger).reconciliation_required


def test_zero_fee_recovery_refuses_multiple_managed_trades_as_unscoped_latches(tmp_path):
    ledger, processor, remote = _recovery_fixture(tmp_path)
    trade = trade_row(parent_fee=D("0"))
    complete_history = (trade, replace(trade, trade_id="second-managed-trade"))

    with pytest.raises(ValueError, match="history must contain exactly this managed trade"):
        processor.resolve_zero_fee_maker_reconciliation(
            trade=trade,
            account_trades_since_baseline=complete_history,
            remote=remote,
            baseline_cash=D("100"), external_condition_ids=frozenset(),
            cash_flow_net=D("0"), account_snapshot_sha256=remote_snapshot_sha256(remote),
        )

    assert sum(event.event_type == "stream.reconciliation_resolved" for event in ledger.events()) == 0
    assert StreamEventProcessor(ledger).reconciliation_required

