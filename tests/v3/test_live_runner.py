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
from src.v3.live_accounting import confirmed_fills
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
from src.v3.reconciliation import (
    CompleteAccountTradeHistory, LocalSnapshot, Reconciler, RemoteAccountOrder, RemoteOrder,
    RemotePosition, RemoteSnapshot, RemoteTrade, RemoteTradeMaker,
)
from src.v3.streaming import StreamEventProcessor

D = Decimal
NOW = datetime(2026, 10, 3, 14, 0, tzinfo=timezone.utc)
# Cancellations old enough to have passed the early-exit finality fence.
PAST_CANCEL = NOW - timedelta(hours=1)


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


@pytest.mark.parametrize(("sell_size", "quantity", "basis"), [
    ("4", D("6"), D("1.20")),
    ("10", D("0"), D("0")),
])
def test_local_snapshot_accounts_confirmed_sell_at_average_cost(sell_size, quantity, basis):
    processor = SimpleNamespace(orders={}, active_order_ids=frozenset())
    buy = OrderAggregate.new(client_order_id="b", token_id="tok", side="BUY", requested_size=D("10"))
    buy.confirmed_size, buy.confirmed_notional = D("10"), D("2")
    sell = OrderAggregate.new(client_order_id="s", token_id="tok", side="SELL", requested_size=D(sell_size))
    sell.confirmed_size, sell.confirmed_notional, sell.confirmed_fees = D(sell_size), D("1.50"), D("0.10")
    processor.orders.update(buy=buy, sell=sell)

    snap = local_snapshot(processor, D("100"))

    assert snap.cash == D("99.4")
    assert snap.position_quantities == ({"tok": quantity} if quantity else {})
    assert snap.position_cost_basis == ({"tok": basis} if quantity else {})


def _append_fill_order(ledger, order_id, side, size):
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": f"client-{order_id}", "order_id": order_id, "status": "live",
        "condition_id": "cond", "token_id": "tok", "side": side,
        "price": "0.50", "requested_size": str(size), "post_only": True,
        "decision_id": f"decision-{order_id}", "exit_stage": "first_tranche" if side == "SELL" else None,
    }))


def _apply_maker_fill(processor, *, order_id, side, trade_id, size, price, at):
    trade = {
        "id": trade_id, "taker_order_id": f"external-{trade_id}", "market": "cond",
        "asset_id": "tok", "side": "SELL" if side == "BUY" else "BUY",
        "size": str(size), "price": str(price), "status": "CONFIRMED",
        "fee_rate_bps": "0", "timestamp": at.isoformat(),
        "maker_orders": [{
            "order_id": order_id, "asset_id": "tok", "side": side,
            "matched_amount": str(size), "price": str(price), "fee_rate_bps": "0",
        }],
    }
    result = processor.process(SimpleNamespace(topic="user", type="trade", payload=trade))
    assert result.accepted and not result.requires_reconciliation


def test_local_snapshot_deduplicates_identical_trade_rows_with_missing_market(tmp_path):
    ledger = EventLedger(tmp_path / "duplicate.sqlite")
    _append_fill_order(ledger, "buy", "BUY", 10)
    processor = StreamEventProcessor(ledger)
    _apply_maker_fill(processor, order_id="buy", side="BUY", trade_id="dup",
                      size=2, price="0.50", at=NOW)
    original = next(e for e in ledger.events() if e.event_type == "user.trade")
    duplicate = dict(original.payload)
    duplicate["market"] = None
    ledger.append(LedgerEvent.create("user.trade", duplicate, occurred_at=original.occurred_at))

    snapshot = local_snapshot(StreamEventProcessor(ledger), D("100"))

    assert snapshot.position_quantities == {"tok": D("2")}
    assert snapshot.cash == D("99")


def test_duplicate_market_enrichment_preserves_first_fill_chronology(tmp_path):
    ledger = EventLedger(tmp_path / "chronology-duplicate.sqlite")
    _append_fill_order(ledger, "buy-a", "BUY", 10)
    _append_fill_order(ledger, "buy-b", "BUY", 10)
    processor = StreamEventProcessor(ledger)
    _apply_maker_fill(processor, order_id="buy-a", side="BUY", trade_id="a",
                      size=2, price="0.50", at=NOW)
    original = next(row for row in ledger.events()
                    if row.event_type == "user.trade" and row.payload["id"] == "a")
    # Legacy rows can lack market; a later account-history import enriches them.
    _apply_maker_fill(processor, order_id="buy-b", side="BUY", trade_id="b",
                      size=2, price="0.50", at=NOW)
    b = next(row for row in ledger.events()
             if row.event_type == "user.trade" and row.payload["id"] == "b")
    legacy = EventLedger(tmp_path / "legacy-chronology.sqlite")
    _append_fill_order(legacy, "buy-a", "BUY", 10)
    _append_fill_order(legacy, "buy-b", "BUY", 10)
    legacy.append(LedgerEvent.create("user.trade", {**original.payload, "market": None}))
    legacy.append(LedgerEvent.create("user.trade", dict(b.payload)))
    legacy.append(LedgerEvent.create("user.trade", dict(original.payload)))

    fills = confirmed_fills(StreamEventProcessor(legacy))
    assert [fill.trade_id for fill in fills] == ["a", "b"]


def test_local_snapshot_rejects_conflicting_duplicate_trade_rows(tmp_path):
    ledger = EventLedger(tmp_path / "conflicting-duplicate.sqlite")
    _append_fill_order(ledger, "buy", "BUY", 10)
    processor = StreamEventProcessor(ledger)
    _apply_maker_fill(processor, order_id="buy", side="BUY", trade_id="dup",
                      size=2, price="0.50", at=NOW)
    original = next(e for e in ledger.events() if e.event_type == "user.trade")
    duplicate = dict(original.payload)
    duplicate["price"] = "0.51"
    ledger.append(LedgerEvent.create("user.trade", duplicate, occurred_at=original.occurred_at))

    with pytest.raises(ValueError, match="confirmed fill chronology"):
        local_snapshot(StreamEventProcessor(ledger), D("100"))
    assert StreamEventProcessor(ledger).reconciliation_required


def _corrected_duplicate_latch_fixture(path, *, duplicate=True, conflict=False, missing_time=False):
    ledger = EventLedger(path)
    ledger.path.parent.mkdir(parents=True, exist_ok=True)
    (ledger.path.parent / "live_state.json").write_text(json.dumps({"baseline_cash": "100", "external_condition_ids": []}), encoding="utf-8")
    _append_fill_order(ledger, "buy", "BUY", 10)
    processor = StreamEventProcessor(ledger)
    _apply_maker_fill(processor, order_id="buy", side="BUY", trade_id="dup",
                      size=10, price="0.50", at=NOW)
    if duplicate:
        original = next(e for e in ledger.events() if e.event_type == "user.trade")
        row = dict(original.payload)
        row["market"] = None
        if conflict:
            row["price"] = "0.51"
        if missing_time:
            row["timestamp"] = None
        ledger.append(LedgerEvent.create("user.trade", row, occurred_at=original.occurred_at))
    processor = StreamEventProcessor(ledger)
    processor.require_reconciliation("confirmed fill chronology or ledger association is invalid")
    return ledger, processor


def _corrected_duplicate_latch_inputs(ledger, processor, *, remote_cash="95"):
    local = local_snapshot(processor, D("100"))
    remote = RemoteSnapshot(D(remote_cash), (RemotePosition("cond", "tok", D("10"), D("8"), D("5")),), ())
    reconciler = Reconciler(cost_tolerance=D("0.01"), cash_tolerance=D("0"))
    return local, remote, reconciler


def test_corrected_duplicate_replay_resolution_persists_and_replays(tmp_path):
    ledger, processor = _corrected_duplicate_latch_fixture(tmp_path / "resolve-duplicate.sqlite")
    local, remote, reconciler = _corrected_duplicate_latch_inputs(ledger, processor)
    result = processor._resolve_corrected_duplicate_trade_latch(
        remote=remote, reconciler=reconciler, trade_id="dup",
        account_snapshot_fetched_at=datetime.now(timezone.utc),
    )
    replayed = StreamEventProcessor(ledger)
    assert result.accepted
    assert not replayed.reconciliation_required


def test_corrected_duplicate_resolution_does_not_accept_caller_local_snapshot(tmp_path):
    ledger, processor = _corrected_duplicate_latch_fixture(tmp_path / "no-caller-local.sqlite")
    local, remote, reconciler = _corrected_duplicate_latch_inputs(ledger, processor)
    before = len(tuple(ledger.events()))
    with pytest.raises(TypeError, match="local"):
        kwargs = {"remote": remote, "local": local, "reconciler": reconciler,
                  "trade_id": "dup", "account_snapshot_fetched_at": datetime.now(timezone.utc)}
        processor._resolve_corrected_duplicate_trade_latch(**kwargs)
    assert len(tuple(ledger.events())) == before


def test_corrected_duplicate_resolution_does_not_accept_caller_cash_baseline(tmp_path):
    ledger, processor = _corrected_duplicate_latch_fixture(tmp_path / "no-caller-baseline.sqlite")
    _local, remote, reconciler = _corrected_duplicate_latch_inputs(ledger, processor)
    before = len(tuple(ledger.events()))
    with pytest.raises(TypeError, match="baseline_cash"):
        kwargs = {"remote": remote, "reconciler": reconciler, "trade_id": "dup",
                  "account_snapshot_fetched_at": datetime.now(timezone.utc), "baseline_cash": D("1")}
        processor._resolve_corrected_duplicate_trade_latch(**kwargs)
    assert len(tuple(ledger.events())) == before


def test_corrected_duplicate_replay_rechecks_persisted_baseline(tmp_path):
    ledger, processor = _corrected_duplicate_latch_fixture(tmp_path / "baseline-replay.sqlite")
    _local, remote, reconciler = _corrected_duplicate_latch_inputs(ledger, processor)
    processor._resolve_corrected_duplicate_trade_latch(
        remote=remote, reconciler=reconciler, trade_id="dup",
        account_snapshot_fetched_at=datetime.now(timezone.utc),
    )
    (ledger.path.parent / "live_state.json").write_text(json.dumps({"baseline_cash": "101"}), encoding="utf-8")
    assert StreamEventProcessor(ledger).reconciliation_required


def test_corrected_duplicate_replay_recomputes_local_snapshot_evidence(tmp_path, monkeypatch):
    ledger, processor = _corrected_duplicate_latch_fixture(tmp_path / "local-digest.sqlite")
    _local, remote, reconciler = _corrected_duplicate_latch_inputs(ledger, processor)
    append = ledger.append

    def tampered(event):
        if event.event_type == "stream.reconciliation_resolved":
            payload = dict(event.payload)
            evidence = dict(payload["evidence"])
            evidence["local_snapshot_sha256"] = "0" * 64
            payload["evidence"] = evidence
            event = LedgerEvent.create(event.event_type, payload, event_id=event.event_id, occurred_at=event.occurred_at)
        return append(event)

    monkeypatch.setattr(ledger, "append", tampered)
    processor._resolve_corrected_duplicate_trade_latch(
        remote=remote, reconciler=reconciler, trade_id="dup",
        account_snapshot_fetched_at=datetime.now(timezone.utc),
    )
    assert StreamEventProcessor(ledger).reconciliation_required


def test_corrected_duplicate_replay_rejects_permissive_policy_evidence(tmp_path, monkeypatch):
    ledger, processor = _corrected_duplicate_latch_fixture(tmp_path / "policy-digest.sqlite")
    _local, remote, reconciler = _corrected_duplicate_latch_inputs(ledger, processor)
    append = ledger.append

    def tampered(event):
        if event.event_type == "stream.reconciliation_resolved":
            payload = dict(event.payload)
            evidence = dict(payload["evidence"])
            evidence.update({
                "external_condition_ids": ["cond"], "cash_tolerance": "1000",
                "cost_tolerance": "1000", "allow_cash_inflows": True,
            })
            payload["evidence"] = evidence
            event = LedgerEvent.create(event.event_type, payload, event_id=event.event_id, occurred_at=event.occurred_at)
        return append(event)

    monkeypatch.setattr(ledger, "append", tampered)
    processor._resolve_corrected_duplicate_trade_latch(
        remote=remote, reconciler=reconciler, trade_id="dup",
        account_snapshot_fetched_at=datetime.now(timezone.utc),
    )
    assert StreamEventProcessor(ledger).reconciliation_required


def test_corrected_duplicate_resolution_refuses_unsafe_account_or_open_orders(tmp_path):
    ledger, processor = _corrected_duplicate_latch_fixture(tmp_path / "unsafe-duplicate.sqlite")
    local, remote, reconciler = _corrected_duplicate_latch_inputs(ledger, processor, remote_cash="94")
    before = len(tuple(ledger.events()))
    with pytest.raises(ValueError, match="unsafe"):
        processor._resolve_corrected_duplicate_trade_latch(
            remote=remote, reconciler=reconciler, trade_id="dup",
            account_snapshot_fetched_at=datetime.now(timezone.utc),
        )
    open_remote = RemoteSnapshot(D("95"), remote.positions, (RemoteOrder("open", "cond", "tok", D("1")),))
    with pytest.raises(ValueError, match="open orders"):
        processor._resolve_corrected_duplicate_trade_latch(
            remote=open_remote, reconciler=reconciler, trade_id="dup",
            account_snapshot_fetched_at=datetime.now(timezone.utc),
        )
    assert len(tuple(ledger.events())) == before


def test_corrected_duplicate_resolution_refuses_conflicting_or_missing_time_rows(tmp_path):
    for label, kwargs in (("conflict", {"conflict": True}), ("missing-time", {"missing_time": True})):
        ledger, processor = _corrected_duplicate_latch_fixture(tmp_path / f"{label}.sqlite", **kwargs)
        with pytest.raises(ValueError):
            local = local_snapshot(processor, D("100"))
            remote = RemoteSnapshot(D("95"), (RemotePosition("cond", "tok", D("10"), D("8"), D("5")),), ())
            processor._resolve_corrected_duplicate_trade_latch(
                remote=remote, reconciler=Reconciler(cost_tolerance=D("0.01"), cash_tolerance=D("0")),
                trade_id="dup", account_snapshot_fetched_at=datetime.now(timezone.utc),
            )
        assert StreamEventProcessor(ledger).reconciliation_required


def test_corrected_duplicate_resolution_never_clears_other_manual_latches(tmp_path):
    ledger, processor = _corrected_duplicate_latch_fixture(tmp_path / "other-latch.sqlite")
    processor.require_reconciliation("incomplete early-exit SELL requires manual reconciliation")
    local, remote, reconciler = _corrected_duplicate_latch_inputs(ledger, processor)
    before = len(tuple(ledger.events()))
    with pytest.raises(ValueError, match="single corrected duplicate-replay"):
        processor._resolve_corrected_duplicate_trade_latch(
            remote=remote, reconciler=reconciler, trade_id="dup",
            account_snapshot_fetched_at=datetime.now(timezone.utc),
        )
    assert len(tuple(ledger.events())) == before
    assert StreamEventProcessor(ledger).reconciliation_required


def test_corrected_duplicate_resolution_rejects_stale_and_future_account_snapshots(tmp_path):
    for label, fetched in (("stale", datetime.now(timezone.utc) - timedelta(seconds=121)),
                           ("future", datetime.now(timezone.utc) + timedelta(seconds=1))):
        ledger, processor = _corrected_duplicate_latch_fixture(tmp_path / f"{label}.sqlite")
        local, remote, reconciler = _corrected_duplicate_latch_inputs(ledger, processor)
        before = len(tuple(ledger.events()))
        with pytest.raises(ValueError, match="fresh"):
            processor._resolve_corrected_duplicate_trade_latch(
                remote=remote, reconciler=reconciler, trade_id="dup",
                account_snapshot_fetched_at=fetched,
            )
        assert len(tuple(ledger.events())) == before


def test_corrected_duplicate_resolution_refuses_managed_taker_association(tmp_path):
    ledger = EventLedger(tmp_path / "managed-taker.sqlite")
    (ledger.path.parent / "live_state.json").write_text(json.dumps({"baseline_cash": "100", "external_condition_ids": []}), encoding="utf-8")
    _append_fill_order(ledger, "buy", "BUY", 10)
    processor = StreamEventProcessor(ledger)
    trade = {
        "id": "taker-dup", "taker_order_id": "buy", "market": "cond", "asset_id": "tok",
        "side": "BUY", "size": "10", "price": "0.50", "status": "CONFIRMED",
        "fee_rate_bps": "0", "timestamp": NOW.isoformat(),
        "maker_orders": [{"order_id": "external", "asset_id": "tok", "side": "SELL",
                           "matched_amount": "10", "price": "0.50", "fee_rate_bps": "0"}],
    }
    assert processor.process(SimpleNamespace(topic="user", type="trade", payload=trade)).accepted
    duplicate = dict(trade)
    duplicate["market"] = None
    ledger.append(LedgerEvent.create("user.trade", duplicate))
    processor = StreamEventProcessor(ledger)
    processor.require_reconciliation("confirmed fill chronology or ledger association is invalid")
    local = local_snapshot(processor, D("100"))
    remote = RemoteSnapshot(D("95"), (RemotePosition("cond", "tok", D("10"), D("8"), D("5")),), ())
    with pytest.raises(ValueError, match="managed order"):
        processor._resolve_corrected_duplicate_trade_latch(
            remote=remote, reconciler=Reconciler(cost_tolerance=D("0.01"), cash_tolerance=D("0")),
            trade_id="taker-dup", account_snapshot_fetched_at=datetime.now(timezone.utc),
        )


def test_corrected_duplicate_resolution_refuses_extra_unmanaged_maker(tmp_path):
    ledger = EventLedger(tmp_path / "extra-maker.sqlite")
    (ledger.path.parent / "live_state.json").write_text(json.dumps({"baseline_cash": "100", "external_condition_ids": []}), encoding="utf-8")
    _append_fill_order(ledger, "buy", "BUY", 10)
    processor = StreamEventProcessor(ledger)
    trade = {
        "id": "maker-dup", "taker_order_id": "external", "market": "cond", "asset_id": "tok",
        "side": "SELL", "size": "10", "price": "0.50", "status": "CONFIRMED",
        "fee_rate_bps": "0", "timestamp": NOW.isoformat(),
        "maker_orders": [
            {"order_id": "buy", "asset_id": "tok", "side": "BUY", "matched_amount": "10", "price": "0.50", "fee_rate_bps": "0"},
            {"order_id": "other", "asset_id": "other-token", "side": "SELL", "matched_amount": "1", "price": "0.30", "fee_rate_bps": "0"},
        ],
    }
    assert processor.process(SimpleNamespace(topic="user", type="trade", payload=trade)).accepted
    duplicate = dict(trade)
    duplicate["market"] = None
    ledger.append(LedgerEvent.create("user.trade", duplicate))
    processor = StreamEventProcessor(ledger)
    processor.require_reconciliation("confirmed fill chronology or ledger association is invalid")
    local = local_snapshot(processor, D("100"))
    remote = RemoteSnapshot(D("95"), (RemotePosition("cond", "tok", D("10"), D("8"), D("5")),), ())
    with pytest.raises(ValueError, match="exactly one explicit maker"):
        processor._resolve_corrected_duplicate_trade_latch(
            remote=remote, reconciler=Reconciler(cost_tolerance=D("0.01"), cash_tolerance=D("0")),
            trade_id="maker-dup", account_snapshot_fetched_at=datetime.now(timezone.utc),
        )


def test_corrected_duplicate_resolution_refuses_nonduplicate_latch(tmp_path):
    ledger, processor = _corrected_duplicate_latch_fixture(tmp_path / "not-duplicate.sqlite", duplicate=False)
    local, remote, reconciler = _corrected_duplicate_latch_inputs(ledger, processor)
    with pytest.raises(ValueError, match="duplicate"):
        processor._resolve_corrected_duplicate_trade_latch(
            remote=remote, reconciler=reconciler, trade_id="dup",
            account_snapshot_fetched_at=datetime.now(timezone.utc),
        )


def test_corrected_duplicate_resolution_rejects_tampered_snapshot_digest(tmp_path, monkeypatch):
    ledger, processor = _corrected_duplicate_latch_fixture(tmp_path / "tampered-duplicate.sqlite")
    local, remote, reconciler = _corrected_duplicate_latch_inputs(ledger, processor)
    append = ledger.append

    def tampered(event):
        if event.event_type == "stream.reconciliation_resolved":
            payload = dict(event.payload)
            evidence = dict(payload["evidence"])
            evidence["account_snapshot_sha256"] = "0" * 64
            payload["evidence"] = evidence
            event = LedgerEvent.create(event.event_type, payload, event_id=event.event_id, occurred_at=event.occurred_at)
        return append(event)

    monkeypatch.setattr(ledger, "append", tampered)
    processor._resolve_corrected_duplicate_trade_latch(
        remote=remote, reconciler=reconciler, trade_id="dup",
        account_snapshot_fetched_at=datetime.now(timezone.utc),
    )
    assert StreamEventProcessor(ledger).reconciliation_required


def test_local_snapshot_replays_interleaved_fills_chronologically(tmp_path):
    ledger = EventLedger(tmp_path / "chronology.sqlite")
    _append_fill_order(ledger, "buy-1", "BUY", 10)
    _append_fill_order(ledger, "sell-1", "SELL", 4)
    _append_fill_order(ledger, "sell-2", "SELL", 1)
    _append_fill_order(ledger, "buy-2", "BUY", 5)
    processor = StreamEventProcessor(ledger)
    _apply_maker_fill(processor, order_id="buy-1", side="BUY", trade_id="b1",
                      size=10, price="0.50", at=NOW)
    # The later BUY arrives before delayed confirmations for the two earlier
    # SELL fills; replay must use venue match time rather than ledger order.
    _apply_maker_fill(processor, order_id="buy-2", side="BUY", trade_id="b2",
                      size=5, price="0.20", at=NOW + timedelta(seconds=3))
    _apply_maker_fill(processor, order_id="sell-1", side="SELL", trade_id="s1",
                      size=4, price="0.80", at=NOW + timedelta(seconds=1))
    _apply_maker_fill(processor, order_id="sell-2", side="SELL", trade_id="s2",
                      size=1, price="0.80", at=NOW + timedelta(seconds=2))

    snapshot = local_snapshot(processor, D("100"))

    assert snapshot.position_quantities == {"tok": D("10")}
    # First sale removes 2.5 cost basis; later buy adds 1.0, so later
    # purchases cannot retroactively change the cost allocated to that sale.
    assert snapshot.position_cost_basis == {"tok": D("3.5")}
    assert snapshot.cash == D("98")
    restored = StreamEventProcessor(ledger)
    assert local_snapshot(restored, D("100")) == snapshot


def test_local_snapshot_latches_sell_that_precedes_available_inventory(tmp_path):
    ledger = EventLedger(tmp_path / "chronological-oversell.sqlite")
    _append_fill_order(ledger, "buy-late", "BUY", 5)
    _append_fill_order(ledger, "sell-early", "SELL", 5)
    processor = StreamEventProcessor(ledger)
    _apply_maker_fill(processor, order_id="sell-early", side="SELL", trade_id="sell-first",
                      size=5, price="0.80", at=NOW)
    _apply_maker_fill(processor, order_id="buy-late", side="BUY", trade_id="buy-later",
                      size=5, price="0.50", at=NOW + timedelta(seconds=1))

    with pytest.raises(ValueError, match="SELL exceeds"):
        local_snapshot(processor, D("100"))
    assert processor.reconciliation_required
    assert any("chronological confirmed SELL" in reason for reason in processor.reconciliation_reasons)
    assert StreamEventProcessor(ledger).reconciliation_required


def test_local_snapshot_rejects_confirmed_sell_oversell():
    processor = SimpleNamespace(orders={})
    buy = OrderAggregate.new(client_order_id="b", token_id="tok", side="BUY", requested_size=D("2"))
    buy.confirmed_size, buy.confirmed_notional = D("2"), D("1")
    sell = OrderAggregate.new(client_order_id="s", token_id="tok", side="SELL", requested_size=D("3"))
    sell.confirmed_size, sell.confirmed_notional = D("3"), D("2")
    processor.orders.update(buy=buy, sell=sell)

    with pytest.raises(ValueError, match="SELL exceeds"):
        local_snapshot(processor, D("100"))


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
        self.account_order_details = {}
        self.account_trade_history: tuple[RemoteTrade, ...] = ()
        self.snapshot_fetches = 0
        self.fail_snapshot_at: int | None = None
        self.snapshot_overrides: dict[int, RemoteSnapshot] = {}

    async def fetch_remote_snapshot(self):
        self.snapshot_fetches += 1
        if self.snapshot_fetches == self.fail_snapshot_at:
            raise OSError("simulated post-cycle snapshot failure")
        return self.snapshot_overrides.get(self.snapshot_fetches, self.remote)

    async def fetch_account_order(self, order_id):
        return self.account_order_details[order_id]

    async def fetch_complete_account_trade_history(self, *, after, max_items, page_limit):
        return CompleteAccountTradeHistory(
            after=after, trades=tuple(self.account_trade_history), fetched_at=NOW,
            max_items=max_items, page_limit=page_limit,
        )

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


def test_runner_resolution_fetches_account_snapshot_from_api(tmp_path, monkeypatch):
    api = _API(cash="95", positions=(RemotePosition("cond", "tok", D("10"), D("8"), D("5")),))
    state = {
        "baseline_at": NOW.isoformat(), "baseline_epoch": int(NOW.timestamp()),
        "baseline_cash": "100", "baseline_equity": "100", "external_condition_ids": [],
        "peak_equity": "100", "event_orders": {},
    }
    runner, _store = _runner(tmp_path / "runner", monkeypatch, api=api, state=state)
    source, _processor = _corrected_duplicate_latch_fixture(tmp_path / "source" / "ledger.sqlite")
    for event in source.events():
        runner.ledger.append(event)

    result = asyncio.run(runner.resolve_corrected_duplicate_trade_latch("dup"))

    assert result.accepted
    assert api.snapshot_fetches == 1
    assert not StreamEventProcessor(runner.ledger).reconciliation_required


def _seed_managed_position(ledger, *, token="tok-exit", shares="10", price="0.50"):
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "buy-client", "order_id": "buy-order", "status": "live",
        "condition_id": "cond", "token_id": token, "side": "BUY",
        "price": price, "requested_size": shares, "post_only": True,
    }))
    ledger.append(LedgerEvent.create("user.trade", {
        "id": "buy-trade", "taker_order_id": "external-buy", "market": "cond",
        "asset_id": token, "side": "SELL", "size": shares, "price": price,
        "status": "CONFIRMED", "fee_rate_bps": "0", "timestamp": NOW.isoformat(),
        "maker_orders": [{
            "order_id": "buy-order", "asset_id": token, "side": "BUY",
            "matched_amount": shares, "price": price, "fee_rate_bps": "0",
        }],
    }))


def _seed_managed_sell(ledger, *, quantity, requested, stage="first_tranche", decision="exit-decision", order_id="prior-sell"):
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": f"sell-client-{order_id}", "decision_id": decision, "exit_stage": stage,
        "target_return": "0.50" if stage == "runner" else "0.28", "order_id": order_id, "status": "live",
        "condition_id": "cond", "token_id": "tok-exit", "side": "SELL",
        "price": "0.80", "requested_size": str(requested), "post_only": True,
    }))
    ledger.append(LedgerEvent.create("user.trade", {
        "id": f"sell-trade-{order_id}", "taker_order_id": f"external-sell-{order_id}", "market": "cond",
        "asset_id": "tok-exit", "side": "BUY", "size": str(quantity), "price": "0.80",
        "status": "CONFIRMED", "fee_rate_bps": "0", "timestamp": NOW.isoformat(),
        "maker_orders": [{
            "order_id": order_id, "asset_id": "tok-exit", "side": "SELL",
            "matched_amount": str(quantity), "price": "0.80", "fee_rate_bps": "0",
        }],
    }))


def _account_sell_detail(order_id, matched, *, price="0.80", requested, status="CANCELED"):
    return RemoteAccountOrder(order_id, "cond", "tok-exit", "SELL", D(price),
                              D(requested), D(matched), status)


def test_full_confirmed_first_tranche_advances_to_runner_target(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("7.5"), D("6"), D("3.75")),))
    api.remote = RemoteSnapshot(D("103"), api.remote.positions, ())
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger, shares="30")
    _seed_managed_sell(runner.ledger, quantity="22.5", requested="22.5")
    processor = StreamEventProcessor(runner.ledger)
    local = local_snapshot(processor, D("100"))
    service = _ExitService(runner.ledger)
    runner.service = service

    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local, remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))

    assert result[0]["outcome"] == "accepted"
    assert service.submitted[0][0].exit_stage == "runner"
    assert service.submitted[0][0].target_return == D("0.50")
    assert service.submitted[0][0].shares == D("7.5")


def test_canceled_partial_first_tranche_then_filled_retry_advances_to_runner(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("5"), D("4"), D("2.5")),))
    api.remote = RemoteSnapshot(D("102"), api.remote.positions, ())
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger, shares="20")
    _seed_managed_sell(runner.ledger, quantity="3", requested="15", order_id="first-tranche")
    StreamEventProcessor(runner.ledger).process(SimpleNamespace(topic="user", type="order", payload={
        "id": "first-tranche", "type": "CANCELLATION", "status": "canceled",
        "reason": "gtd_expired", "timestamp": PAST_CANCEL.isoformat(),
    }))
    _seed_managed_sell(runner.ledger, quantity="12", requested="12", decision="retry",
                       order_id="tranche-retry")
    processor = StreamEventProcessor(runner.ledger)
    service = _ExitService(runner.ledger)
    runner.service = service
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local_snapshot(processor, D("100")), remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))
    # 3 + 12 confirmed completes the 15-share first tranche: the rest is the runner.
    assert result[0]["outcome"] == "accepted", result
    assert not processor.reconciliation_required
    assert not StreamEventProcessor(runner.ledger).reconciliation_required
    intent = service.submitted[0][0]
    assert intent.exit_stage == "runner" and intent.shares == D("5")
    assert intent.target_return == D("0.50")


def test_canceled_zero_fill_closes_cleanly_for_a_fresh_exit_decision(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),))
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    runner.ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "zero-sell-client", "decision_id": "zero-sell-decision",
        "exit_stage": "first_tranche", "target_return": "0.28",
        "order_id": "zero-sell", "status": "live", "condition_id": "cond",
        "token_id": "tok-exit", "side": "SELL", "price": "0.8",
        "requested_size": "8", "post_only": True,
    }))
    StreamEventProcessor(runner.ledger).process(SimpleNamespace(topic="user", type="order", payload={
        "id": "zero-sell", "type": "CANCELLATION", "status": "canceled",
        "reason": "user_canceled", "timestamp": PAST_CANCEL.isoformat(),
    }))
    processor = StreamEventProcessor(runner.ledger)
    service = _ExitService(runner.ledger)
    runner.service = service
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local_snapshot(processor, D("100")), remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))
    # An unfilled terminal SELL has no inventory effect: no latch, fresh plan.
    assert result[0]["outcome"] == "accepted", result
    assert not processor.reconciliation_required
    assert not StreamEventProcessor(runner.ledger).reconciliation_required
    intent = service.submitted[0][0]
    # 75% of 10 leaves a 2.5-share runner below the 5-share minimum: sell all.
    assert intent.exit_stage == "full" and intent.shares == D("10")


@pytest.mark.parametrize("trade_status", ["MATCHED", "MINED", "RETRYING", "FAILED"])
def test_pending_sell_rows_wait_and_failed_rows_are_ignored(tmp_path, monkeypatch, trade_status):
    from src.v3.orders import TradeRecord, TradeStatus

    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),))
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    _sell_accept(runner.ledger, order_id="late-sell")
    StreamEventProcessor(runner.ledger).process(SimpleNamespace(topic="user", type="order", payload={
        "id": "late-sell", "type": "CANCELLATION", "status": "canceled",
        "reason": "gtd_expired", "timestamp": PAST_CANCEL.isoformat(),
    }))
    processor = StreamEventProcessor(runner.ledger)
    # Legacy/stream state: an unconfirmed row known for the canceled SELL.
    processor.orders["late-sell"].trades["late-trade"] = TradeRecord(
        "late-trade", D("10"), D("0.80"), D("0"), TradeStatus(trade_status),
    )
    service = _ExitService(runner.ledger)
    runner.service = service
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local_snapshot(processor, D("100")), remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))
    assert not processor.reconciliation_required
    if trade_status == "FAILED":
        # A failed match never executed: the SELL closed unfilled.
        assert result[0]["outcome"] == "accepted", result
        assert service.submitted[0][0].shares == D("10")
    else:
        assert result == [{"token_id": "tok-exit", "outcome": "blocked",
                           "reason": "SELL fill awaiting confirmation"}]
        assert service.submitted == []


def test_sell_still_open_on_venue_is_never_treated_as_closed(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),))
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    runner.ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "sell-client", "decision_id": "sell-decision",
        "exit_stage": "full", "target_return": "0.28", "order_id": "open-sell",
        "status": "live", "condition_id": "cond", "token_id": "tok-exit", "side": "SELL",
        "price": "0.8", "requested_size": "10", "post_only": True,
    }))
    StreamEventProcessor(runner.ledger).process(SimpleNamespace(topic="user", type="order", payload={
        "id": "open-sell", "type": "CANCELLATION", "status": "canceled",
        "reason": "user_canceled", "timestamp": PAST_CANCEL.isoformat(),
    }))
    api.remote = RemoteSnapshot(api.remote.cash, api.remote.positions,
                                (RemoteOrder("open-sell", "cond", "tok-exit", D("8")),))
    processor = StreamEventProcessor(runner.ledger)
    service = _ExitService(runner.ledger)
    runner.service = service
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local_snapshot(processor, D("100")), remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))
    assert result[0]["outcome"] == "blocked"
    assert result[0]["reason"] == "managed SELL is still open on the venue"
    assert service.submitted == []


def test_expired_zero_fill_sell_does_not_latch_cycle_and_is_replanned(tmp_path, monkeypatch):
    from dataclasses import replace

    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),))
    runner, store = _runner(tmp_path, monkeypatch, api=api)
    runner.state.update({
        "baseline_cash": "100", "baseline_equity": "100", "baseline_epoch": int(NOW.timestamp()),
        "baseline_at": NOW.isoformat(), "peak_equity": "100", "external_condition_ids": [],
    })
    store.save_state(runner.state)
    _seed_managed_position(runner.ledger)
    expired_at = int((NOW - timedelta(hours=1)).timestamp())
    runner.ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "sell-client", "decision_id": "sell-decision",
        "exit_stage": "full", "target_return": "0.28", "order_id": "expired-sell",
        "status": "live", "condition_id": "cond", "token_id": "tok-exit", "side": "SELL",
        "price": "0.95", "requested_size": "10", "post_only": True, "expiration": expired_at,
    }))
    runner.runner_settings = replace(runner.runner_settings, live_early_exit_enabled=True)
    service = _ExitService(runner.ledger, api)
    runner.service = service

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert status["orders_marked_expired"] == ["expired-sell"]
    assert "lifecycle_reconciliation_required" not in status
    assert status["reconciliation"]["safe_to_trade"] is True
    assert status["early_exits"][0]["outcome"] == "accepted", status["early_exits"]
    assert len(service.submitted) == 1 and service.submitted[0][0].exit_stage == "full"
    assert not StreamEventProcessor(runner.ledger).reconciliation_required


def test_partial_cancel_does_not_latch_cycle_and_exits_remainder(tmp_path, monkeypatch):
    from dataclasses import replace

    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("7"), D("5.6"), D("3.5")),))
    api.remote = RemoteSnapshot(D("97.4"), api.remote.positions, ())
    runner, store = _runner(tmp_path, monkeypatch, api=api)
    runner.state.update({
        "baseline_cash": "100", "baseline_equity": "100", "baseline_epoch": int(NOW.timestamp()),
        "baseline_at": NOW.isoformat(), "peak_equity": "100", "external_condition_ids": [],
    })
    store.save_state(runner.state)
    _seed_managed_position(runner.ledger)
    _seed_managed_sell(runner.ledger, quantity="3", requested="8")
    StreamEventProcessor(runner.ledger).process(SimpleNamespace(topic="user", type="order", payload={
        "id": "prior-sell", "type": "CANCELLATION", "status": "canceled",
        "reason": "gtd_expired", "timestamp": PAST_CANCEL.isoformat(),
    }))
    runner.runner_settings = replace(runner.runner_settings, live_early_exit_enabled=True)
    service = _ExitService(runner.ledger, api)
    runner.service = service

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert "lifecycle_reconciliation_required" not in status
    assert status["healthy"] is True
    assert status["reconciliation"]["safe_to_trade"] is True
    assert status["early_exits"][0]["outcome"] == "accepted", status["early_exits"]
    # 8 - 3 = 5 first-tranche shares would strand a 2-share runner: sell all 7.
    intent = service.submitted[0][0]
    assert intent.exit_stage == "full" and intent.shares == D("7")
    assert status["outcomes_this_cycle"].get("blocked", 0) == 1  # accepted exit blocks entries


def test_canceled_partial_first_tranche_offers_unfilled_remainder(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("17"), D("13.6"), D("8.5")),))
    api.remote = RemoteSnapshot(D("92.4"), api.remote.positions, ())
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger, shares="20")
    _seed_managed_sell(ledger=runner.ledger, quantity="3", requested="15")
    StreamEventProcessor(runner.ledger).process(SimpleNamespace(topic="user", type="order", payload={
        "id": "prior-sell", "type": "CANCELLATION", "status": "canceled",
        "reason": "gtd_expired", "timestamp": PAST_CANCEL.isoformat(),
    }))
    processor = StreamEventProcessor(runner.ledger)
    service = _ExitService(runner.ledger)
    runner.service = service
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local_snapshot(processor, D("100")), remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))
    assert result[0]["outcome"] == "accepted", result
    assert not processor.reconciliation_required
    assert not StreamEventProcessor(runner.ledger).reconciliation_required
    intent = service.submitted[0][0]
    # 15 requested - 3 confirmed: offer the unfilled 12; 5 shares remain for the runner.
    assert intent.exit_stage == "first_tranche" and intent.shares == D("12")


def test_active_partial_first_tranche_remains_blocked(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("7"), D("5.6"), D("3.5")),))
    api.remote = RemoteSnapshot(D("97.4"), api.remote.positions,
                                (RemoteOrder("prior-sell", "cond", "tok-exit", D("6.4")),))
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    _seed_managed_sell(runner.ledger, quantity="3", requested="8")
    processor = StreamEventProcessor(runner.ledger)
    local = local_snapshot(processor, D("100"))
    service = _ExitService(runner.ledger)
    runner.service = service

    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local, remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))

    assert result[0]["outcome"] == "blocked"
    assert result[0]["reason"] == "active managed order"
    assert service.submitted == []


def test_confirmed_sell_fills_above_requested_size_latch(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("7"), D("5.6"), D("3.5")),))
    api.remote = RemoteSnapshot(D("97.4"), api.remote.positions, ())
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    _seed_managed_sell(runner.ledger, quantity="3", requested="8")
    processor = StreamEventProcessor(runner.ledger)
    processor.process(SimpleNamespace(topic="user", type="order", payload={
        "id": "prior-sell", "type": "CANCELLATION", "status": "canceled",
        "reason": "gtd_expired", "timestamp": PAST_CANCEL.isoformat(),
    }))
    processor = StreamEventProcessor(runner.ledger)
    local = local_snapshot(processor, D("100"))
    processor.orders["prior-sell"].confirmed_size = D("9")  # corrupted aggregate
    service = _ExitService(runner.ledger)
    runner.service = service

    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local, remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))

    assert result[0]["outcome"] == "manual_review"
    assert processor.reconciliation_required
    assert StreamEventProcessor(runner.ledger).reconciliation_required
    assert service.submitted == []


def test_partial_filled_state_is_not_recoverable_even_with_canceled_detail(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("7"), D("5.6"), D("3.5")),))
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    _seed_managed_sell(runner.ledger, quantity="3", requested="8")
    processor = StreamEventProcessor(runner.ledger)
    from src.v3.orders import OrderState
    processor.orders["prior-sell"].state = OrderState.FILLED
    api.account_order_details["prior-sell"] = _account_sell_detail("prior-sell", "3", requested="8")
    service = _ExitService(runner.ledger)
    runner.service = service
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local_snapshot(processor, D("100")), remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))
    assert result[0]["outcome"] == "manual_review"
    assert processor.reconciliation_required
    assert StreamEventProcessor(runner.ledger).reconciliation_required
    assert service.submitted == []


def test_canceled_partial_runner_replans_remaining_runner(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("5.5"), D("4.4"), D("2.75")),))
    api.remote = RemoteSnapshot(D("107.6"), api.remote.positions, ())
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger, shares="40")
    _seed_managed_sell(runner.ledger, quantity="30", requested="30")
    _seed_managed_sell(runner.ledger, quantity="4.5", requested="10", stage="runner",
                       decision="runner-old", order_id="runner-sell")
    processor = StreamEventProcessor(runner.ledger)
    processor.process(SimpleNamespace(topic="user", type="order", payload={
        "id": "runner-sell", "type": "CANCELLATION", "status": "canceled",
        "reason": "gtd_expired", "timestamp": PAST_CANCEL.isoformat(),
    }))
    api.account_order_details["prior-sell"] = _account_sell_detail(
        "prior-sell", "30", requested="30",
    )
    api.account_order_details["runner-sell"] = _account_sell_detail(
        "runner-sell", "4.5", requested="10",
    )
    processor = StreamEventProcessor(runner.ledger)
    runner.service = _ExitService(runner.ledger)
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local_snapshot(processor, D("100")), remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))
    assert result[0]["outcome"] == "accepted", result
    assert not processor.reconciliation_required
    assert not StreamEventProcessor(runner.ledger).reconciliation_required
    intent = runner.service.submitted[0][0]
    assert intent.exit_stage == "runner" and intent.shares == D("5.5")


class _EarlyExitAPI(_API):
    def __init__(self, positions=(), orders=()):
        super().__init__(cash="95", positions=positions, orders=orders)
        # A quiet book (last changed 30 minutes ago) that is read fresh.
        self.book_time = datetime.now(timezone.utc) - timedelta(minutes=30)
        self.raw_book = SimpleNamespace(
            condition_id="cond", token_id="tok-exit", timestamp=self.book_time, hash="book-exit",
            bids=(SimpleNamespace(price=D("0.80"), size=D("20")),),
        )

    async def get_verified_market_context(self, condition_id, token_id):
        return SimpleNamespace(
            condition_id=condition_id, token_id=token_id, condition_matches=True,
            token_matches=True, book_timestamp=self.book_time, book_hash="book-exit",
            rules_verified=True, accepting_orders=True, disputed=False,
            tick_size=D("0.01"), min_order_size=D("5"), fee_rate=D("0.10"),
            fee_exponent=D("1"), fees_enabled=True, taker_only=True,
            book=self.raw_book, fetched_at=datetime.now(timezone.utc),
        )

    async def get_order_book(self, token_id):
        raise AssertionError("early exits must reuse the verified context's book snapshot")


class _ExitService(_Service):
    def __init__(self, ledger, api=None):
        super().__init__()
        self.ledger = ledger
        self.api = api

    async def submit(self, intent, local, context):
        self.submitted.append((intent, local, context))
        order_id = f"sell-order-{len(self.submitted)}"
        self.ledger.append(LedgerEvent.create("order.accepted", {
            "client_order_id": intent.decision_id, "decision_id": intent.decision_id,
            "exit_stage": intent.exit_stage, "target_return": str(intent.target_return),
            "order_id": order_id, "status": "live", "condition_id": intent.condition_id,
            "token_id": intent.token_id, "side": "SELL", "price": str(intent.price),
            "requested_size": str(intent.shares), "post_only": True,
        }))
        if self.api is not None:
            self.api.remote = RemoteSnapshot(
                self.api.remote.cash, self.api.remote.positions,
                (*self.api.remote.open_orders,
                 RemoteOrder(order_id, intent.condition_id, intent.token_id,
                             intent.price * intent.shares)),
            )
        order = OrderAggregate.new(client_order_id=intent.decision_id, token_id=intent.token_id,
                                   side="SELL", requested_size=intent.shares)
        order.accept(order_id=order_id, status="live")
        return ExecutionResult(True, "live", order)


def test_live_early_exit_submits_verified_bot_position_via_service_and_blocks_duplicate(tmp_path, monkeypatch):
    positions = (RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),)
    api = _EarlyExitAPI(positions=positions)
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    service = _ExitService(runner.ledger)
    runner.service = service
    processor = StreamEventProcessor(runner.ledger)
    local = local_snapshot(processor, D("100"))
    remote = api.remote
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local, remote=remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))

    assert result[0]["outcome"] == "accepted"
    intent, _, _ = service.submitted[0]
    assert intent.side == "SELL" and intent.post_only
    # 7.5 would strand a 2.5-share runner below the 5-share minimum: sell all 10.
    assert intent.exit_stage == "full" and intent.shares == D("10")
    assert intent.decision_id and intent.target_return == D("0.28")
    assert intent.estimated_fee == D("0")  # post-only maker, not bid-depth taker fee

    restarted = StreamEventProcessor(runner.ledger)
    replayed = asyncio.run(runner._run_early_exits(
        processor=restarted, local=local, remote=remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))
    assert replayed[0]["outcome"] == "blocked"
    assert len(service.submitted) == 1


def test_live_early_exit_never_selects_external_holdings(tmp_path, monkeypatch):
    positions = (RemotePosition("cond-old", "tok-exit", D("10"), D("8"), D("5")),)
    api = _EarlyExitAPI(positions=positions)
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    runner.reconciler = Reconciler(external_condition_ids={"cond"})
    _seed_managed_position(runner.ledger)
    processor = StreamEventProcessor(runner.ledger)
    local = local_snapshot(processor, D("100"))
    service = _ExitService(runner.ledger)
    runner.service = service

    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local, remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))

    assert result == []
    assert service.submitted == []


def test_live_early_exit_refuses_unverified_positive_fee_maker_schedule(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),))
    original_context = api.get_verified_market_context

    async def unverified(condition_id, token_id):
        context = await original_context(condition_id, token_id)
        context.taker_only = False
        return context

    api.get_verified_market_context = unverified
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    service = _ExitService(runner.ledger)
    runner.service = service
    processor = StreamEventProcessor(runner.ledger)
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local_snapshot(processor, D("100")),
        remote=api.remote, risk_context=SimpleNamespace(), now=NOW,
    ))
    assert result[0]["outcome"] == "blocked"
    assert not service.submitted


def test_live_cycle_runs_exit_when_entry_loss_guard_blocks_and_reconciles_after_submit(tmp_path, monkeypatch):
    positions = (RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),)
    api = _EarlyExitAPI(positions=positions)
    state = {
        "baseline_at": NOW.isoformat(), "baseline_epoch": int(NOW.timestamp()),
        "baseline_cash": "100", "baseline_equity": "100",
        "external_condition_ids": ["cond-old"], "peak_equity": "200",
        "day": NOW.date().isoformat(), "day_start_equity": "200", "event_orders": {},
    }
    evaluation = _evaluation(condition="cond")
    evaluation.token_id = "tok-exit"
    runner, _ = _runner(tmp_path, monkeypatch, api=api, evaluations=(evaluation,), state=state)
    _seed_managed_position(runner.ledger)
    runner.runner_settings = LiveRunnerSettings(
        shadow=runner.shadow, live_early_exit_enabled=True,
    )
    runner.state["event_orders"] = {evaluation.event_key: [{
        "token_id": "tok-exit", "order_id": "buy-order",
    }]}
    service = _ExitService(runner.ledger, api)
    runner.service = service

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert status["entry_block_reason"] in {"daily loss limit reached", "maximum drawdown reached"}
    assert status["early_exits"][0]["outcome"] == "accepted"
    assert status["reconciliation"]["safe_to_trade"] is True
    assert len(service.submitted) == 1 and service.submitted[0][0].side == "SELL"
    assert api.snapshot_fetches == 2


def test_accepted_exit_blocks_new_buy_candidates_same_cycle(tmp_path, monkeypatch):
    positions = (RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),)
    api = _EarlyExitAPI(positions=positions)
    state = {
        "baseline_at": NOW.isoformat(), "baseline_epoch": int(NOW.timestamp()),
        "baseline_cash": "100", "baseline_equity": "100",
        "external_condition_ids": ["cond-old"], "peak_equity": "104",
        "day": NOW.date().isoformat(), "day_start_equity": "104", "event_orders": {},
    }
    candidate = _evaluation(condition="cond-new")
    candidate.token_id = "tok-new"
    runner, store = _runner(tmp_path, monkeypatch, api=api, evaluations=(candidate,), state=state)
    _seed_managed_position(runner.ledger)
    runner.runner_settings = LiveRunnerSettings(
        shadow=runner.shadow, live_early_exit_enabled=True,
    )
    service = _ExitService(runner.ledger, api)
    runner.service = service

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert status["entry_block_reason"] is None
    assert status["early_exits"][0]["outcome"] == "accepted"
    assert len(service.submitted) == 1 and service.submitted[0][0].side == "SELL"
    assert status["outcomes_this_cycle"]["blocked"] == 1
    record = json.loads(store.intents_path.read_text().splitlines()[0])
    assert record["reason"] == "accepted early exit blocks entries for this cycle"


def test_live_early_exit_setting_defaults_off_and_reads_explicit_opt_in(monkeypatch, tmp_path):
    monkeypatch.delenv("V3_LIVE_EARLY_EXIT_ENABLED", raising=False)
    assert not LiveRunnerSettings.from_env(tmp_path).live_early_exit_enabled
    monkeypatch.setenv("V3_LIVE_EARLY_EXIT_ENABLED", "true")
    assert LiveRunnerSettings.from_env(tmp_path).live_early_exit_enabled


def test_cycle_submits_through_service_and_records_event_order(tmp_path, monkeypatch):
    service = _Service()
    runner, store = _runner(tmp_path, monkeypatch, service=service)
    status = asyncio.run(runner.run_cycle(now=NOW))
    assert status["outcomes_this_cycle"] == {"accepted": 1}
    timings = status["cycle_timing_seconds"]
    assert set(timings) == {"account_read", "early_exits", "weather_evaluation", "total"}
    assert all(isinstance(value, float) and value >= 0 for value in timings.values())
    assert timings["total"] >= timings["account_read"]
    intent, local, context = service.submitted[0]
    assert intent.post_only and intent.side == "BUY" and intent.all_in_notional == D("1.90")
    assert context.daily_pnl == D("0") and context.peak_equity == D("237.13")
    state = json.loads(store.state_path.read_text())
    assert state["event_orders"]["toronto:2026-10-04"][0]["order_id"] == "ord-1"


def test_live_cycle_processes_all_selected_candidates_beyond_cycle_limit(tmp_path, monkeypatch):
    class MultiService(_Service):
        async def submit(self, intent, local, context):
            self.submitted.append((intent, local, context))
            order = OrderAggregate.new(
                client_order_id=f"client-{len(self.submitted)}",
                token_id=intent.token_id,
                side="BUY",
                requested_size=intent.shares,
            )
            order.accept(order_id=f"order-{len(self.submitted)}", status="live")
            return ExecutionResult(True, "live", order)

    candidates = tuple(
        _evaluation(event_key=f"city-{i}:2026-10-04", condition=f"cond-{i}")
        for i in range(5)
    )
    service = MultiService()
    runner, _ = _runner(
        tmp_path, monkeypatch, service=service, evaluations=candidates,
    )
    runner.shadow = LiveShadowSettings(
        data_dir=runner.store.data_dir, max_new_orders_per_cycle=1,
    )

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert len(service.submitted) == len(candidates)
    assert status["outcomes_this_cycle"] == {"accepted": len(candidates)}


def test_live_cycle_does_not_apply_per_event_daily_submission_cap(tmp_path, monkeypatch):
    event_key = "toronto:2026-10-04"
    state = {
        "baseline_at": NOW.isoformat(), "baseline_epoch": int(NOW.timestamp()),
        "baseline_cash": "237.13", "baseline_equity": "237.13",
        "external_condition_ids": ["cond-old"], "peak_equity": "237.13",
        "event_orders": {event_key: [
            {"at": NOW.isoformat(), "order_id": f"expired-{i}", "token_id": f"old-{i}"}
            for i in range(4)
        ]},
    }
    service = _Service()
    runner, _ = _runner(
        tmp_path, monkeypatch, service=service,
        evaluations=(_evaluation(event_key=event_key),), state=state,
    )

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert len(service.submitted) == 1
    assert status["outcomes_this_cycle"] == {"accepted": 1}


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


def test_missing_order_is_reconciled_only_after_remote_cancel_and_zero_fill_proof(tmp_path, monkeypatch):
    state = {
        "baseline_at": NOW.isoformat(), "baseline_epoch": int(NOW.timestamp()),
        "baseline_cash": "237.13", "baseline_equity": "237.13",
        "external_condition_ids": ["cond-old"], "peak_equity": "237.13",
        "event_orders": {},
    }
    api = _API()
    api.account_order_details["gone"] = RemoteAccountOrder(
        order_id="gone", condition_id="cond", token_id="tok-gone", side="BUY",
        price=D("0.19"), original_size=D("10"), size_matched=D("0"), status="CANCELED",
    )
    service = _Service()
    runner, _ = _runner(
        tmp_path, monkeypatch, api=api, service=service,
        evaluations=(_evaluation(condition="cond-old"),), state=state,
    )
    _accept(runner.ledger, "gone", token="tok-gone", size="10",
            expiration=int((NOW + timedelta(hours=1)).timestamp()))

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert status["orders_marked_terminal_canceled"] == ["gone"]
    assert status["reconciliation"]["safe_to_trade"] is True
    assert status["reconciliation"]["missing_orders"] == 0
    assert status["bot_active_orders"] == 0
    assert StreamEventProcessor(runner.ledger).orders["gone"].state.value == "CANCELED"
    assert service.submitted == []


@pytest.mark.parametrize("proof", ["still_live", "partial_fill", "trade_history"])
def test_missing_order_stays_blocked_without_complete_cancel_proof(tmp_path, monkeypatch, proof):
    state = {
        "baseline_at": NOW.isoformat(), "baseline_epoch": int(NOW.timestamp()),
        "baseline_cash": "237.13", "baseline_equity": "237.13",
        "external_condition_ids": ["cond-old"], "peak_equity": "237.13",
        "event_orders": {},
    }
    api = _API()
    matched = D("1") if proof == "partial_fill" else D("0")
    status_text = "LIVE" if proof == "still_live" else "CANCELED"
    api.account_order_details["gone"] = RemoteAccountOrder(
        order_id="gone", condition_id="cond", token_id="tok-gone", side="BUY",
        price=D("0.19"), original_size=D("10"), size_matched=matched, status=status_text,
    )
    if proof == "trade_history":
        api.account_trade_history = (RemoteTrade(
            trade_id="trade-gone", condition_id="cond", token_id="tok-gone",
            taker_order_id="other", side="SELL", trader_side="MAKER", price=D("0.19"),
            size=D("1"), status="MATCHED", matched_at=NOW, updated_at=None,
            fee_rate_bps=D("0"), transaction_hash=None,
            maker_orders=(RemoteTradeMaker(
                order_id="gone", token_id="tok-gone", side="BUY", price=D("0.19"),
                matched_amount=D("1"), fee_rate_bps=D("0"),
            ),),
        ),)
    runner, _ = _runner(
        tmp_path, monkeypatch, api=api, evaluations=(_evaluation(condition="cond-old"),), state=state,
    )
    _accept(runner.ledger, "gone", token="tok-gone", size="10",
            expiration=int((NOW + timedelta(hours=1)).timestamp()))

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert status["orders_marked_terminal_canceled"] == []
    assert status["entry_block_reason"] == "account reconciliation blocked entries"
    assert status["reconciliation"]["missing_orders"] == 1
    assert StreamEventProcessor(runner.ledger).active_order_ids == frozenset({"gone"})


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
        self.conditions_by_token = {}
        self.external_values_after_post: list[Decimal] = []

    async def initialize_secure_client(self):
        return self

    async def initialize_account_client(self):
        return None

    def _authenticated_client(self):
        return SimpleNamespace(close=_async_none)

    async def fetch_remote_snapshot(self):
        return self.remote

    async def fetch_account_trades(self, *, max_items, page_limit, after=None):
        self.trade_after.append(after)
        return tuple(t for t in self.trades if after is None or t.matched_at.timestamp() >= after)

    async def get_verified_market_context(self, condition_id, token_id):
        self.conditions_by_token[token_id] = condition_id
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
        args = signed["signed"]
        order_id = f"sdk-ord-{len(self.created)}"
        self.remote = RemoteSnapshot(
            self.remote.cash,
            self.remote.positions,
            self.remote.open_orders + (RemoteOrder(
                order_id,
                self.conditions_by_token[args["token_id"]],
                args["token_id"],
                args["price"] * args["size"],
            ),),
        )
        if self.external_values_after_post:
            current_value = self.external_values_after_post.pop(0)
            self.remote = RemoteSnapshot(
                self.remote.cash,
                tuple(RemotePosition(
                    position.condition_id, position.token_id, position.size,
                    current_value, position.initial_value, position.redeemable,
                ) for position in self.remote.positions),
                self.remote.open_orders,
            )
        return SimpleNamespace(ok=True, order_id=order_id, status="live")


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


def _runner_with(
    tmp_path, monkeypatch, store, ledger, reconciler, service, api, settings, runner_settings,
    *, evaluations=None,
):
    from src.v3 import live_runner, v7_weather_intent
    monkeypatch.setattr(live_shadow, "station_metadata_reason", lambda path, city: None)
    monkeypatch.setattr(live_shadow, "propose_v7_weather_order", lambda *a, **k: (
        v7_weather_intent.V7WeatherOrderProposal(True, "ok", price=D("0.19"), shares=D("10"),
                                                 expected_edge=D("0.11"), quote_age_seconds=5)))

    async def universe(**kwargs):
        selected = tuple(evaluations or (_evaluation(),))
        return SimpleNamespace(evaluations=selected, markets_evaluated=len(selected),
                               forecast_status="available", errors=())

    monkeypatch.setattr(live_runner, "evaluate_weather_universe", universe)
    runner = LiveTradingRunner(
        service=service, ledger=ledger, reconciler=reconciler, runner_settings=runner_settings,
        api=api, settings=settings, shadow=runner_settings.shadow, store=store,
        weather_client=None, forecast=None, observation_provider=None,
    )
    return runner, store


def test_real_live_service_processes_every_candidate_but_honors_open_order_cap(
    tmp_path, monkeypatch,
):
    from src.v3.live_runner import _start
    from src.v3 import live_runner

    candidates = tuple(
        _evaluation(event_key=f"city-{i}:2026-10-04", condition=f"condition-{i}")
        for i in range(5)
    )
    settings = V3Settings(
        live_enabled=True, paper_trading=False, live_confirmation="I_UNDERSTAND_REAL_MONEY",
        private_key="0x" + "1" * 64, wallet_address="0x" + "2" * 40,
        max_capital=D("100"), max_order_notional=D("2"), reserve_fraction=D("0.25"),
        max_daily_loss=D("10"), max_drawdown_amount=D("10"),
    )
    api = _SdkAPI(settings, RemoteSnapshot(D("237.13"), (), ()))
    monkeypatch.setattr(live_runner, "UnifiedPolymarketAPI", lambda settings: api)

    runner_settings = LiveRunnerSettings(shadow=LiveShadowSettings(
        data_dir=tmp_path / "live", max_open_orders=2, max_new_orders_per_cycle=1,
    ))
    store, _, ledger, reconciler, service = asyncio.run(_start(settings, runner_settings))
    runner, _ = _runner_with(
        tmp_path, monkeypatch, store, ledger, reconciler, service, api, settings,
        runner_settings, evaluations=candidates,
    )

    status = asyncio.run(runner.run_cycle())

    assert status["outcomes_this_cycle"] == {"accepted": 2, "rejected": 3}, status
    assert len(api.created) == 2
    assert len(api.remote.open_orders) == 2
    assert status["bot_active_orders"] == 2
    assert status["reconciliation"]["safe_to_trade"] is True
    assert status["limits"]["max_open_orders"] == 2


def test_real_service_refreshes_daily_loss_before_each_candidate_submission(tmp_path, monkeypatch):
    from src.v3.live_runner import _start
    from src.v3 import live_runner

    candidates = tuple(
        _evaluation(event_key=f"city-{i}:2026-10-04", condition=f"condition-{i}")
        for i in range(5)
    )
    settings = V3Settings(
        live_enabled=True, paper_trading=False, live_confirmation="I_UNDERSTAND_REAL_MONEY",
        private_key="0x" + "1" * 64, wallet_address="0x" + "2" * 40,
        max_capital=D("100"), max_order_notional=D("2"), reserve_fraction=D("0.25"),
        max_daily_loss=D("10"), max_drawdown_amount=D("100"),
    )
    baseline_position = RemotePosition(
        "external-condition", "external-token", D("50"), D("10"), D("10"), True,
    )
    api = _SdkAPI(settings, RemoteSnapshot(D("90"), (baseline_position,), ()))
    api.external_values_after_post = [D("0")]
    monkeypatch.setattr(live_runner, "UnifiedPolymarketAPI", lambda settings: api)

    runner_settings = LiveRunnerSettings(shadow=LiveShadowSettings(
        data_dir=tmp_path / "live", max_open_orders=5, max_new_orders_per_cycle=1,
    ))
    store, _, ledger, reconciler, service = asyncio.run(_start(settings, runner_settings))
    runner, _ = _runner_with(
        tmp_path, monkeypatch, store, ledger, reconciler, service, api, settings,
        runner_settings, evaluations=candidates,
    )

    status = asyncio.run(runner.run_cycle())

    assert status["outcomes_this_cycle"] == {"accepted": 1, "rejected": 4}, status
    assert len(api.created) == 1
    assert status["daily_pnl"] == D("-10")
    assert status["entry_block_reason"] == "daily loss limit reached after submission"
    assert status["reconciliation"]["safe_to_trade"] is True


def test_post_cycle_snapshot_failure_marks_status_unhealthy_and_blocked(tmp_path, monkeypatch):
    api = _API()
    api.fail_snapshot_at = 2
    runner, _ = _runner(tmp_path, monkeypatch, api=api, service=_Service())

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert status["outcomes_this_cycle"] == {"accepted": 1}
    assert status["healthy"] is False
    assert status["post_cycle_reconciliation_error"] == "OSError"
    assert status["reconciliation"]["safe_to_trade"] is False
    assert status["entry_block_reason"] == "post-cycle account verification failed"


@pytest.mark.parametrize("corruption", ["peak", "epoch", "day_start_missing"])
def test_malformed_persisted_risk_baseline_blocks_before_account_reads(
    tmp_path, monkeypatch, corruption,
):
    api = _API()
    service = _Service()
    runner, _ = _runner(tmp_path, monkeypatch, api=api, service=service)
    if corruption == "peak":
        runner.state["peak_equity"] = "NaN"
    elif corruption == "epoch":
        runner.state["baseline_epoch"] = True
    else:
        runner.state["day"] = NOW.date().isoformat()

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert status["healthy"] is False
    assert status["reconciliation"]["safe_to_trade"] is False
    assert status["entry_block_reason"] == "persisted risk baseline invalid"
    assert api.snapshot_fetches == 0
    assert service.submitted == []


def test_real_service_carries_intracycle_peak_for_drawdown_fraction(tmp_path, monkeypatch):
    from src.v3.live_runner import _start
    from src.v3 import live_runner

    candidates = tuple(
        _evaluation(event_key=f"city-{i}:2026-10-04", condition=f"condition-{i}")
        for i in range(5)
    )
    settings = V3Settings(
        live_enabled=True, paper_trading=False, live_confirmation="I_UNDERSTAND_REAL_MONEY",
        private_key="0x" + "1" * 64, wallet_address="0x" + "2" * 40,
        max_capital=D("100"), max_order_notional=D("2"), reserve_fraction=D("0.25"),
        max_daily_loss=D("10"), max_drawdown_amount=D("100"),
        max_drawdown_fraction=D("0.04"),
    )
    baseline_position = RemotePosition(
        "external-condition", "external-token", D("50"), D("10"), D("10"), True,
    )
    api = _SdkAPI(settings, RemoteSnapshot(D("90"), (baseline_position,), ()))
    api.external_values_after_post = [D("20"), D("15")]
    monkeypatch.setattr(live_runner, "UnifiedPolymarketAPI", lambda settings: api)

    runner_settings = LiveRunnerSettings(shadow=LiveShadowSettings(
        data_dir=tmp_path / "live", max_open_orders=5, max_new_orders_per_cycle=1,
    ))
    store, _, ledger, reconciler, service = asyncio.run(_start(settings, runner_settings))
    runner, _ = _runner_with(
        tmp_path, monkeypatch, store, ledger, reconciler, service, api, settings,
        runner_settings, evaluations=candidates,
    )

    status = asyncio.run(runner.run_cycle())

    assert status["outcomes_this_cycle"] == {"accepted": 2, "rejected": 3}, status
    assert len(api.created) == 2
    assert status["peak_equity"] == D("110")
    assert status["equity"] == D("105")
    assert status["drawdown"] == D("5")
    assert status["entry_block_reason"] == "maximum drawdown reached after submission"


@pytest.mark.parametrize("bad_cash", ["NaN", "Infinity", "-1", "0"])
def test_invalid_post_cycle_snapshot_never_persists_invalid_peak(tmp_path, monkeypatch, bad_cash):
    api = _API()
    api.snapshot_overrides[2] = RemoteSnapshot(D(bad_cash), (), ())
    runner, store = _runner(tmp_path, monkeypatch, api=api, service=_Service())
    old_peak = runner.state["peak_equity"]

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert status["healthy"] is False
    assert status["post_cycle_reconciliation_error"] == "ValueError"
    assert status["reconciliation"]["safe_to_trade"] is False
    assert status["entry_block_reason"] == "post-cycle account verification failed"
    assert json.loads(store.state_path.read_text())["peak_equity"] == old_peak


# --- live exit halt regressions -------------------------------------------------

def _sell_accept(ledger, order_id="exit-sell", requested="10", stage="full"):
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": f"client-{order_id}", "decision_id": f"decision-{order_id}",
        "exit_stage": stage, "target_return": "0.28", "order_id": order_id, "status": "live",
        "condition_id": "cond", "token_id": "tok-exit", "side": "SELL", "price": "0.80",
        "requested_size": requested, "post_only": True,
    }))


def _sell_trade(*, status, maker_fee, trade_fee, order_id="exit-sell", size="10",
                taker_side="BUY", taker_token="tok-exit", trade_size=None, trade_price="0.80"):
    return RemoteTrade(
        trade_id=f"trade-{order_id}", condition_id="cond", token_id=taker_token,
        taker_order_id="external-taker", side=taker_side, trader_side="MAKER",
        price=D(trade_price), size=D(trade_size or size), status=status,
        matched_at=NOW, updated_at=None, fee_rate_bps=trade_fee, transaction_hash=None,
        maker_orders=(RemoteTradeMaker(
            order_id=order_id, token_id="tok-exit", side="SELL", price=D("0.80"),
            matched_amount=D(size), fee_rate_bps=maker_fee,
        ),),
    )


@pytest.mark.parametrize("status", ["MATCHED", "MINED"])
def test_unconfirmed_sell_maker_fill_is_deferred_without_latch(tmp_path, status):
    ledger = EventLedger(tmp_path / "ledger.sqlite")
    _seed_managed_position(ledger)
    _sell_accept(ledger)
    processor = StreamEventProcessor(ledger)
    events_before = len(tuple(ledger.events()))

    result = processor.import_remote_trade(_sell_trade(status=status, maker_fee=D("0"), trade_fee=D("0")))

    assert not result.accepted and not result.requires_reconciliation
    assert len(tuple(ledger.events())) == events_before  # nothing recorded
    # The streamed lifecycle row is likewise ignored for the SELL leg.
    streamed = processor.process(SimpleNamespace(topic="user", type="trade", payload={
        "id": "streamed", "taker_order_id": "external", "market": "cond", "asset_id": "tok-exit",
        "side": "BUY", "size": "10", "price": "0.80", "status": status, "fee_rate_bps": None,
        "timestamp": NOW.isoformat(),
        "maker_orders": [{"order_id": "exit-sell", "asset_id": "tok-exit", "side": "SELL",
                          "matched_amount": "10", "price": "0.80"}],
    }))
    assert streamed.accepted and not streamed.requires_reconciliation
    replay = StreamEventProcessor(ledger)
    assert not replay.reconciliation_required
    assert replay.orders["exit-sell"].trades == {}
    # The later confirmation is then accounted normally.
    confirmed = replay.import_remote_trade(_sell_trade(status="CONFIRMED", maker_fee=D("0"), trade_fee=D("0")))
    assert confirmed.accepted and not confirmed.requires_reconciliation
    final = StreamEventProcessor(ledger)
    assert not final.reconciliation_required
    assert final.orders["exit-sell"].confirmed_size == D("10")
    assert local_snapshot(final, D("100")).position_quantities == {}


@pytest.mark.parametrize("side", ["BUY", "SELL"])
def test_maker_leg_without_maker_fee_is_never_charged_the_taker_fee(tmp_path, side):
    ledger = EventLedger(tmp_path / "ledger.sqlite")
    _seed_managed_position(ledger)
    if side == "SELL":
        _sell_accept(ledger, order_id="maker")
    else:
        ledger.append(LedgerEvent.create("order.accepted", {
            "client_order_id": "client-maker", "order_id": "maker", "status": "live",
            "condition_id": "cond", "token_id": "tok-exit", "side": "BUY", "price": "0.80",
            "requested_size": "10", "post_only": True,
        }))
    processor = StreamEventProcessor(ledger)
    result = processor.process(SimpleNamespace(topic="user", type="trade", payload={
        "id": "fee-trade", "taker_order_id": "external", "market": "cond", "asset_id": "tok-exit",
        "side": "BUY" if side == "SELL" else "SELL", "size": "10", "price": "0.80",
        "status": "CONFIRMED", "fee_rate_bps": "200", "timestamp": NOW.isoformat(),
        "maker_orders": [{"order_id": "maker", "asset_id": "tok-exit", "side": side,
                          "matched_amount": "10", "price": "0.80"}],
    }))
    assert result.accepted and not result.requires_reconciliation
    replay = StreamEventProcessor(ledger)
    assert replay.orders["maker"].confirmed_size == D("10")
    assert replay.orders["maker"].confirmed_fees == D("0")


def test_non_post_only_maker_leg_without_maker_fee_stays_unknown(tmp_path):
    ledger = EventLedger(tmp_path / "ledger.sqlite")
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "client-maker", "order_id": "maker", "status": "live",
        "condition_id": "cond", "token_id": "tok-exit", "side": "BUY", "price": "0.80",
        "requested_size": "10", "post_only": False,
    }))
    processor = StreamEventProcessor(ledger)
    result = processor.process(SimpleNamespace(topic="user", type="trade", payload={
        "id": "fee-trade", "taker_order_id": "external", "market": "cond", "asset_id": "tok-exit",
        "side": "SELL", "size": "10", "price": "0.80", "status": "CONFIRMED",
        "fee_rate_bps": "200", "timestamp": NOW.isoformat(),
        "maker_orders": [{"order_id": "maker", "asset_id": "tok-exit", "side": "BUY",
                          "matched_amount": "10", "price": "0.80"}],
    }))
    assert result.requires_reconciliation
    assert processor.orders["maker"].confirmed_size == D("0")


def test_confirmed_sell_maker_without_maker_fee_uses_explicit_zero_trade_fee(tmp_path):
    ledger = EventLedger(tmp_path / "ledger.sqlite")
    _seed_managed_position(ledger)
    _sell_accept(ledger)
    processor = StreamEventProcessor(ledger)

    result = processor.import_remote_trade(_sell_trade(status="CONFIRMED", maker_fee=None, trade_fee=D("0")))

    assert result.accepted and not result.requires_reconciliation
    replay = StreamEventProcessor(ledger)
    assert not replay.reconciliation_required
    assert replay.orders["exit-sell"].confirmed_size == D("10")
    assert replay.orders["exit-sell"].confirmed_fees == D("0")
    assert local_snapshot(replay, D("100")).cash == D("103")


def test_confirmed_sell_maker_without_any_fee_latches_recoverable_reason(tmp_path):
    ledger = EventLedger(tmp_path / "ledger.sqlite")
    _seed_managed_position(ledger)
    _sell_accept(ledger)
    processor = StreamEventProcessor(ledger)

    result = processor.import_remote_trade(_sell_trade(status="CONFIRMED", maker_fee=None, trade_fee=None))

    assert result.requires_reconciliation
    assert processor.reconciliation_reasons == ["remote maker trade fee rate is unknown"]


def test_sell_maker_in_complementary_multi_maker_match_is_accepted(tmp_path):
    # The trade's top-level fields describe the taker (here a NO seller in a
    # merge match filling 25 shares across makers); only our maker row applies.
    ledger = EventLedger(tmp_path / "ledger.sqlite")
    _seed_managed_position(ledger)
    _sell_accept(ledger, requested="8")
    processor = StreamEventProcessor(ledger)

    result = processor.import_remote_trade(_sell_trade(
        status="CONFIRMED", maker_fee=D("0"), trade_fee=D("0"), size="8",
        taker_side="SELL", taker_token="tok-no", trade_size="25", trade_price="0.20",
    ))

    assert result.accepted and not result.requires_reconciliation
    replay = StreamEventProcessor(ledger)
    assert replay.orders["exit-sell"].confirmed_size == D("8")
    assert local_snapshot(replay, D("100")).position_quantities == {"tok-exit": D("2")}


def test_sell_maker_without_post_only_acceptance_still_latches(tmp_path):
    ledger = EventLedger(tmp_path / "ledger.sqlite")
    _seed_managed_position(ledger)
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "client-exit-sell", "order_id": "exit-sell", "status": "live",
        "condition_id": "cond", "token_id": "tok-exit", "side": "SELL", "price": "0.80",
        "requested_size": "10", "post_only": False,
    }))
    processor = StreamEventProcessor(ledger)

    result = processor.import_remote_trade(_sell_trade(status="CONFIRMED", maker_fee=D("0"), trade_fee=D("0")))

    assert result.requires_reconciliation and processor.reconciliation_required


def test_redeemable_position_is_skipped_before_any_network_call(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("10"), D("10"), D("5"), True),))

    async def no_network(*args, **kwargs):
        raise AssertionError("closed markets must be skipped before any network call")

    api.get_verified_market_context = no_network
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    processor = StreamEventProcessor(runner.ledger)
    runner.service = _ExitService(runner.ledger)
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local_snapshot(processor, D("100")), remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))
    assert result == [{"token_id": "tok-exit", "outcome": "skipped", "reason": "market closed"}]
    assert runner.service.submitted == []


def test_closed_unresolved_market_is_skipped_not_blocked(tmp_path, monkeypatch):
    from src.v3.api import MarketClosedError

    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),))

    async def closed(condition_id, token_id):
        raise MarketClosedError("market closed")

    api.get_verified_market_context = closed
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    processor = StreamEventProcessor(runner.ledger)
    runner.service = _ExitService(runner.ledger)
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local_snapshot(processor, D("100")), remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))
    assert result == [{"token_id": "tok-exit", "outcome": "skipped", "reason": "market closed"}]


def test_quiet_book_is_fresh_and_never_refetched(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),))
    # The last book change was long ago (NOW), but the book was read just now.
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    service = _ExitService(runner.ledger)
    runner.service = service
    processor = StreamEventProcessor(runner.ledger)
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local_snapshot(processor, D("100")), remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))
    assert result[0]["outcome"] == "accepted", result
    assert service.submitted[0][0].quote_age_seconds <= 5


def test_context_without_its_book_snapshot_fails_closed(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),))
    original = api.get_verified_market_context

    async def bookless(condition_id, token_id):
        context = await original(condition_id, token_id)
        context.book = None
        return context

    api.get_verified_market_context = bookless
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    service = _ExitService(runner.ledger)
    runner.service = service
    processor = StreamEventProcessor(runner.ledger)
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local_snapshot(processor, D("100")), remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))
    assert result[0]["outcome"] == "blocked" and "book snapshot" in result[0]["reason"]
    assert service.submitted == []


def test_sub_step_dust_is_not_an_exit_candidate_or_event_blocker(tmp_path, monkeypatch):
    dust = D("0.003528")
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", dust, D("0"), D("0.001")),))
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    local = LocalSnapshot(
        cash=D("100"), position_tokens=frozenset({"tok-exit", "tok-held"}), order_ids=frozenset(),
        position_quantities={"tok-exit": dust, "tok-held": D("5")},
        position_cost_basis={"tok-exit": D("0.001"), "tok-held": D("2.5")},
    )
    runner.service = _ExitService(runner.ledger)
    processor = StreamEventProcessor(runner.ledger)
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local, remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))
    assert result == [] and runner.service.submitted == []
    remote = RemoteSnapshot(D("100"), (), ())
    runner.state["event_orders"] = {
        "dust:2026-10-04": [{"token_id": "tok-exit", "order_id": "old"}],
        "held:2026-10-04": [{"token_id": "tok-held", "order_id": "old-2"}],
    }
    dust_event = SimpleNamespace(condition_id="cond-a", event_key="dust:2026-10-04")
    held_event = SimpleNamespace(condition_id="cond-b", event_key="held:2026-10-04")
    assert runner._event_block_reason(dust_event, remote, local) is None
    assert runner._event_block_reason(held_event, remote, local) == "bot already holds a position in this event"


def test_auto_redeem_failure_blocks_entries_but_not_exits(tmp_path, monkeypatch):
    from dataclasses import replace
    from src.v3 import live_auto_redeem

    positions = (RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),)
    api = _EarlyExitAPI(positions=positions)
    state = {
        "baseline_at": NOW.isoformat(), "baseline_epoch": int(NOW.timestamp()),
        "baseline_cash": "100", "baseline_equity": "100",
        "external_condition_ids": ["cond-old"], "peak_equity": "104",
        "day": NOW.date().isoformat(), "day_start_equity": "104", "event_orders": {},
    }
    runner, _ = _runner(tmp_path, monkeypatch, api=api, state=state)
    _seed_managed_position(runner.ledger)
    runner.runner_settings = replace(
        runner.runner_settings, live_early_exit_enabled=True, auto_redeem_enabled=True,
    )

    async def broken(**kwargs):
        raise RuntimeError("condition did not resolve to exactly one market")

    monkeypatch.setattr(live_auto_redeem, "auto_redeem_one_managed_winner", broken)
    service = _ExitService(runner.ledger, api)
    runner.service = service

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert status["auto_redemption_error"] == "RuntimeError"
    assert status["entry_block_reason"] == "auto-redemption submission requires reconciliation"
    assert status["early_exit_block_reason"] is None
    assert status["early_exits"][0]["outcome"] == "accepted"
    assert len(service.submitted) == 1 and service.submitted[0][0].side == "SELL"


def test_auto_redeem_skips_are_reported_in_status(tmp_path, monkeypatch):
    from dataclasses import replace
    from src.v3 import live_auto_redeem

    runner, _ = _runner(tmp_path, monkeypatch)
    runner.runner_settings = replace(runner.runner_settings, auto_redeem_enabled=True)

    async def skipping(**kwargs):
        kwargs["skipped"].append({"condition_id": "c", "token_id": "t",
                                  "reason": "skipped: neg-risk redemption unsupported"})
        return kwargs["remote"], (), None

    monkeypatch.setattr(live_auto_redeem, "auto_redeem_one_managed_winner", skipping)
    status = asyncio.run(runner.run_cycle(now=NOW))
    assert status["auto_redemption_skipped"] == [{
        "condition_id": "c", "token_id": "t", "reason": "skipped: neg-risk redemption unsupported",
    }]
    assert "auto_redemption_error" not in status


# --- review follow-ups: finality fence, caps, auto-redeem exit blocking -------

def _canceled_sell(runner, *, filled, canceled_at, requested="8", expiration=None, stage="first_tranche"):
    payload = {
        "client_order_id": "fence-client", "decision_id": "fence-decision", "exit_stage": stage,
        "target_return": "0.28", "order_id": "fence-sell", "status": "live",
        "condition_id": "cond", "token_id": "tok-exit", "side": "SELL", "price": "0.80",
        "requested_size": requested, "post_only": True,
    }
    if expiration is not None:
        payload["expiration"] = int(expiration.timestamp())
    runner.ledger.append(LedgerEvent.create("order.accepted", payload))
    if filled != "0":
        runner.ledger.append(LedgerEvent.create("user.trade", {
            "id": "fence-trade", "taker_order_id": "external", "market": "cond",
            "asset_id": "tok-exit", "side": "BUY", "size": filled, "price": "0.80",
            "status": "CONFIRMED", "fee_rate_bps": "0", "timestamp": (NOW + timedelta(seconds=1)).isoformat(),
            "maker_orders": [{"order_id": "fence-sell", "asset_id": "tok-exit", "side": "SELL",
                              "matched_amount": filled, "price": "0.80", "fee_rate_bps": "0"}],
        }))
    cancel = {"id": "fence-sell", "type": "CANCELLATION", "status": "canceled", "reason": "user_canceled"}
    if canceled_at is not None:
        cancel["timestamp"] = canceled_at.isoformat()
    StreamEventProcessor(runner.ledger).process(SimpleNamespace(topic="user", type="order", payload=cancel))


def _exit_once(runner, api, processor=None):
    processor = processor or StreamEventProcessor(runner.ledger)
    runner.service = _ExitService(runner.ledger)
    result = asyncio.run(runner._run_early_exits(
        processor=processor, local=local_snapshot(processor, D("100")), remote=api.remote,
        risk_context=SimpleNamespace(), now=NOW,
    ))
    return result, processor


@pytest.mark.parametrize("filled,held", [("0", "10"), ("3", "7")])
def test_recently_canceled_sell_waits_out_the_finality_fence(tmp_path, monkeypatch, filled, held):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D(held), D("5"), D("3")),))
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    _canceled_sell(runner, filled=filled, canceled_at=NOW - timedelta(seconds=599))

    result, processor = _exit_once(runner, api)

    assert result == [{"token_id": "tok-exit", "outcome": "blocked",
                       "reason": "terminal SELL inside finality fence; late fills may still import"}]
    assert not processor.reconciliation_required and runner.service.submitted == []


def test_expiry_bounds_the_fence_when_cancellation_is_later(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),))
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    # Recorded canceled just now, but the GTD order could not match after it expired an hour ago.
    _canceled_sell(runner, filled="0", canceled_at=NOW, expiration=NOW - timedelta(hours=1))

    result, _ = _exit_once(runner, api)

    assert result[0]["outcome"] == "accepted", result


def test_terminal_sell_without_any_time_bound_is_reported_not_replanned(tmp_path, monkeypatch):
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),))
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)
    _canceled_sell(runner, filled="0", canceled_at=None)

    result, processor = _exit_once(runner, api)

    assert result == [{"token_id": "tok-exit", "outcome": "blocked",
                       "reason": "terminal SELL has no cancellation or expiry time; finality unprovable"}]
    assert not processor.reconciliation_required


def test_failed_partial_first_tranche_continues_with_its_remainder(tmp_path, monkeypatch):
    from src.v3.orders import OrderState

    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("17"), D("13.6"), D("8.5")),))
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger, shares="20")
    _canceled_sell(runner, filled="3", requested="15", canceled_at=PAST_CANCEL)
    processor = StreamEventProcessor(runner.ledger)
    processor.orders["fence-sell"].state = OrderState.FAILED

    result, _ = _exit_once(runner, api, processor)

    assert result[0]["outcome"] == "accepted", result
    intent = runner.service.submitted[0][0]
    assert intent.exit_stage == "first_tranche" and intent.shares == D("12")


def test_replacement_size_is_capped_at_shares_still_held_on_venue(tmp_path, monkeypatch):
    # Venue holds 8 (an unconfirmed SELL match took 2); local confirmed shows 10.
    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("8"), D("6.4"), D("4")),))
    runner, _ = _runner(tmp_path, monkeypatch, api=api)
    _seed_managed_position(runner.ledger)

    result, _ = _exit_once(runner, api)

    assert result[0]["outcome"] == "accepted", result
    assert runner.service.submitted[0][0].shares == D("8")


def _redeem_cycle(tmp_path, monkeypatch, behaviour):
    from dataclasses import replace
    from src.v3 import live_auto_redeem

    api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),))
    state = {
        "baseline_at": NOW.isoformat(), "baseline_epoch": int(NOW.timestamp()),
        "baseline_cash": "100", "baseline_equity": "100",
        "external_condition_ids": ["cond-old"], "peak_equity": "104",
        "day": NOW.date().isoformat(), "day_start_equity": "104", "event_orders": {},
    }
    runner, _ = _runner(tmp_path, monkeypatch, api=api, state=state)
    _seed_managed_position(runner.ledger)
    runner.runner_settings = replace(
        runner.runner_settings, live_early_exit_enabled=True, auto_redeem_enabled=True,
    )

    async def redeem(**kwargs):
        return await behaviour(runner.ledger, kwargs)

    monkeypatch.setattr(live_auto_redeem, "auto_redeem_one_managed_winner", redeem)
    service = _ExitService(runner.ledger, api)
    runner.service = service
    return asyncio.run(runner.run_cycle(now=NOW)), service


def test_pending_auto_redemption_blocks_exits(tmp_path, monkeypatch):
    async def pending(ledger, kwargs):
        return kwargs["remote"], (), "auto-redemption submission needs manual reconciliation"

    status, service = _redeem_cycle(tmp_path, monkeypatch, pending)
    assert status["early_exit_block_reason"] == "auto-redemption submission needs manual reconciliation"
    assert service.submitted == []


def test_auto_redemption_failure_after_submission_blocks_exits(tmp_path, monkeypatch):
    async def failed_after_submit(ledger, kwargs):
        ledger.append(LedgerEvent.create("auto_redemption.submission_started", {
            "condition_id": "cond-r", "token_id": "tok-r", "quantity": "5", "at": NOW.isoformat(),
        }))
        raise RuntimeError("redemption transaction hash is missing or changed")

    status, service = _redeem_cycle(tmp_path, monkeypatch, failed_after_submit)
    assert status["auto_redemption_error"] == "RuntimeError"
    assert status["early_exit_block_reason"] == "auto-redemption submission requires reconciliation"
    assert service.submitted == []


def test_exit_and_cycle_share_one_inconsistent_sell_latch(tmp_path, monkeypatch):
    from src.v3.live_runner import _latch_inconsistent_sells

    expected = "inconsistent SELL fill state requires manual reconciliation: prior-sell"
    reasons = []
    for path, via_exit in ((tmp_path / "exit", True), (tmp_path / "cycle", False)):
        api = _EarlyExitAPI(positions=(RemotePosition("cond", "tok-exit", D("7"), D("5.6"), D("3.5")),))
        runner, _ = _runner(path, monkeypatch, api=api)
        _seed_managed_position(runner.ledger)
        _seed_managed_sell(runner.ledger, quantity="3", requested="8")
        StreamEventProcessor(runner.ledger).process(SimpleNamespace(topic="user", type="order", payload={
            "id": "prior-sell", "type": "CANCELLATION", "status": "canceled",
            "reason": "gtd_expired", "timestamp": PAST_CANCEL.isoformat(),
        }))
        processor = StreamEventProcessor(runner.ledger)
        local = local_snapshot(processor, D("100"))
        processor.orders["prior-sell"].confirmed_size = D("9")
        if via_exit:
            runner.service = _ExitService(runner.ledger)
            result = asyncio.run(runner._run_early_exits(
                processor=processor, local=local, remote=api.remote,
                risk_context=SimpleNamespace(), now=NOW,
            ))
            assert result[0]["outcome"] == "manual_review"
        else:
            _latch_inconsistent_sells(processor)
        reasons.append(list(processor.reconciliation_reasons))
    assert reasons == [[expected], [expected]]
