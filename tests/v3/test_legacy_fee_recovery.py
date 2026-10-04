from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.v3.api import AccountCashFlow, CompleteAccountCashFlowHistory
from src.v3.ledger import EventLedger, LedgerEvent
from src.v3.reconciliation import (
    CompleteAccountTradeHistory, LocalSnapshot, Reconciler,
    RemotePosition, RemoteSnapshot, RemoteTrade, RemoteTradeMaker,
)
from src.v3.streaming import StreamEventProcessor

D = Decimal
BASELINE_EPOCH = 1577836800
BASELINE_CASH = D("100")
MATCHED_AT = datetime(2020, 1, 1, tzinfo=timezone.utc)


def _trade_row(trade_id="remote-trade", maker_fee_rate_bps=None):
    maker = RemoteTradeMaker("managed-order", "token", "BUY", D("0.31"), D("5"), maker_fee_rate_bps)
    return RemoteTrade(
        trade_id=trade_id, condition_id="condition", token_id="counterparty-token",
        taker_order_id="external-order", side="BUY", trader_side="MAKER",
        price=D("0.69"), size=D("5"), status="CONFIRMED",
        matched_at=MATCHED_AT, updated_at=None, fee_rate_bps=D("0"),
        transaction_hash=None, maker_orders=(maker,),
    )


def _fixture(tmp_path, *, earlier_latch=False, extra_blocker=False, maker_fee_rate_bps=None):
    ledger = EventLedger(tmp_path / "legacy.sqlite")
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "client-1", "order_id": "managed-order",
        "status": "live", "condition_id": "condition", "token_id": "token",
        "side": "BUY", "requested_size": "5", "post_only": True,
    }, occurred_at="2019-12-31T23:59:00+00:00"))
    first_time = "2019-12-31T23:59:59+00:00" if earlier_latch else "2020-01-01T00:01:00+00:00"
    ledger.append(LedgerEvent.create("stream.reconciliation_required", {
        "reason": "remote maker trade fee rate is unknown",
    }, occurred_at=first_time))
    ledger.append(LedgerEvent.create("stream.reconciliation_required", {
        "reason": "remote maker trade fee rate is unknown",
    }, occurred_at="2020-01-01T00:02:00+00:00"))
    if extra_blocker:
        ledger.append(LedgerEvent.create("stream.reconciliation_required", {
            "reason": "unrelated stream gap",
        }, occurred_at="2020-01-01T00:03:00+00:00"))
    processor = StreamEventProcessor(ledger)
    trade = _trade_row(maker_fee_rate_bps=maker_fee_rate_bps)
    assert processor.import_remote_trade(trade).accepted
    history = CompleteAccountTradeHistory(
        after=BASELINE_EPOCH, trades=(trade,),
        fetched_at=datetime.now(timezone.utc),
        max_items=100, page_limit=10,
    )
    remote = RemoteSnapshot(
        cash=D("98.45"),
        positions=(RemotePosition(
            condition_id="condition", token_id="token", size=D("5"),
            current_value=D("0.045"), initial_value=D("1.55"),
        ),),
        open_orders=(),
    )
    local = LocalSnapshot(
        cash=D("98.45"), position_tokens=frozenset({"token"}), order_ids=frozenset(),
        position_quantities={"token": D("5")}, position_cost_basis={"token": D("1.55")},
    )
    reconciler = Reconciler(external_condition_ids=frozenset())
    return ledger, processor, trade, history, remote, local, reconciler


def _resolve(processor, trade, history, remote, local, reconciler, *, cash_flow_history=None, snapshot_fetched_at=None):
    cash_flow_history = cash_flow_history or CompleteAccountCashFlowHistory(
        after=BASELINE_EPOCH, flows=(), fetched_at=datetime.now(timezone.utc),
        max_items=10_000, page_size=500,
    )
    return processor.resolve_legacy_fee_latches_after_reconciliation(
        history=history, remote=remote, local=local, reconciler=reconciler,
        baseline_epoch=BASELINE_EPOCH, baseline_cash=BASELINE_CASH,
        external_condition_ids=frozenset(), cash_flow_history=cash_flow_history,
        account_snapshot_fetched_at=snapshot_fetched_at or datetime.now(timezone.utc),
    )


def test_legacy_fee_recovery_resolves_all_latches_only_after_complete_parity(tmp_path):
    ledger, processor, trade, history, remote, local, reconciler = _fixture(tmp_path)

    result = _resolve(processor, trade, history, remote, local, reconciler)

    assert result.accepted and "2 legacy fee latches" in result.reason
    resolution = [event for event in ledger.events() if event.event_type == "stream.reconciliation_resolved"]
    assert len(resolution) == 1
    assert len(resolution[0].payload["resolved_event_ids"]) == 2
    restored = StreamEventProcessor(ledger)
    assert not restored.reconciliation_required
    assert restored.orders["managed-order"].confirmed_size == D("5")
    assert restored.orders["managed-order"].confirmed_fees == D("0")


def test_legacy_fee_recovery_also_accepts_explicit_zero_maker_fee(tmp_path):
    ledger, processor, trade, history, remote, local, reconciler = _fixture(
        tmp_path, maker_fee_rate_bps=D("0"),
    )

    _resolve(processor, trade, history, remote, local, reconciler)

    resolution = next(
        event for event in ledger.events() if event.event_type == "stream.reconciliation_resolved"
    )
    assert resolution.payload["evidence"]["maker_fee_rate_bps"] == "0"
    assert not StreamEventProcessor(ledger).reconciliation_required


def test_legacy_fee_recovery_refuses_extra_account_trade(tmp_path):
    ledger, processor, trade, history, remote, local, reconciler = _fixture(tmp_path)
    history = replace(history, trades=(trade, replace(trade, trade_id="second-trade")))

    with pytest.raises(ValueError, match="exactly one complete post-baseline"):
        _resolve(processor, trade, history, remote, local, reconciler)

    assert not any(event.event_type == "stream.reconciliation_resolved" for event in ledger.events())
    assert StreamEventProcessor(ledger).reconciliation_required


def test_legacy_fee_recovery_refuses_latch_older_than_trade(tmp_path):
    ledger, processor, trade, history, remote, local, reconciler = _fixture(tmp_path, earlier_latch=True)

    with pytest.raises(ValueError, match="predates the sole managed trade"):
        _resolve(processor, trade, history, remote, local, reconciler)

    assert not any(event.event_type == "stream.reconciliation_resolved" for event in ledger.events())
    assert StreamEventProcessor(ledger).reconciliation_required


def test_legacy_fee_recovery_refuses_unrelated_blocker_or_cash_flow(tmp_path):
    ledger, processor, trade, history, remote, local, reconciler = _fixture(tmp_path, extra_blocker=True)

    with pytest.raises(ValueError, match="non-legacy, scoped, or unrelated"):
        _resolve(processor, trade, history, remote, local, reconciler)

    assert not any(event.event_type == "stream.reconciliation_resolved" for event in ledger.events())
    assert StreamEventProcessor(ledger).reconciliation_required


def test_legacy_fee_recovery_refuses_cash_flows(tmp_path):
    ledger, processor, trade, history, remote, local, reconciler = _fixture(tmp_path)
    now = datetime.now(timezone.utc)
    cash_flow_history = CompleteAccountCashFlowHistory(
        after=BASELINE_EPOCH,
        flows=(AccountCashFlow("withdrawal-1", "WITHDRAWAL", now, "0xwithdrawal", D("0.1")),),
        fetched_at=now, max_items=10_000, page_size=500,
    )

    with pytest.raises(ValueError, match="no cash flows"):
        _resolve(processor, trade, history, remote, local, reconciler, cash_flow_history=cash_flow_history)

    assert not any(event.event_type == "stream.reconciliation_resolved" for event in ledger.events())
    assert StreamEventProcessor(ledger).reconciliation_required


def test_legacy_fee_recovery_refuses_stale_account_snapshot(tmp_path):
    from datetime import timedelta

    ledger, processor, trade, history, remote, local, reconciler = _fixture(tmp_path)

    with pytest.raises(ValueError, match="fresh account and trade-history reads"):
        _resolve(
            processor, trade, history, remote, local, reconciler,
            snapshot_fetched_at=datetime.now(timezone.utc) - timedelta(seconds=121),
        )

    assert not any(event.event_type == "stream.reconciliation_resolved" for event in ledger.events())
    assert StreamEventProcessor(ledger).reconciliation_required


def test_legacy_fee_recovery_refuses_remote_cash_mismatch(tmp_path):
    ledger, processor, trade, history, remote, local, reconciler = _fixture(tmp_path)
    bad_remote = replace(remote, cash=D("98.44"))

    with pytest.raises(ValueError, match="remote cash, position"):
        _resolve(processor, trade, history, bad_remote, local, reconciler)

    assert not any(event.event_type == "stream.reconciliation_resolved" for event in ledger.events())
    assert StreamEventProcessor(ledger).reconciliation_required


def test_legacy_fee_resolution_replay_rejects_fabricated_trade_id(tmp_path):
    source, processor, trade, history, remote, local, reconciler = _fixture(tmp_path / "source")
    _resolve(processor, trade, history, remote, local, reconciler)
    valid_resolution = next(
        event for event in source.events() if event.event_type == "stream.reconciliation_resolved"
    )
    tampered = dict(valid_resolution.payload)
    evidence = dict(tampered["evidence"])
    evidence["trade_id"] = "invented-trade"
    evidence["history_trade_ids"] = ["invented-trade"]
    tampered["evidence"] = evidence
    tampered["latch_scope_trade_id"] = "invented-trade"

    forged = EventLedger(tmp_path / "forged.sqlite")
    for event in source.events():
        if event.event_id != valid_resolution.event_id:
            forged.append(event)
    forged.append(LedgerEvent.create(
        "stream.reconciliation_resolved", tampered,
        occurred_at=valid_resolution.occurred_at,
    ))

    assert StreamEventProcessor(forged).reconciliation_required


def test_legacy_fee_resolution_replay_rejects_incomplete_latch_set(tmp_path):
    ledger, processor, trade, history, remote, local, reconciler = _fixture(tmp_path)
    _resolve(processor, trade, history, remote, local, reconciler)
    source_events = tuple(ledger.events())
    resolution = next(e for e in source_events if e.event_type == "stream.reconciliation_resolved")

    tampered = EventLedger(tmp_path / "tampered.sqlite")
    for event in source_events:
        if event.event_id != resolution.event_id:
            tampered.append(event)
    payload = dict(resolution.payload)
    payload["resolved_event_ids"] = payload["resolved_event_ids"][:1]
    tampered.append(LedgerEvent.create("stream.reconciliation_resolved", payload))

    assert StreamEventProcessor(tampered).reconciliation_required
