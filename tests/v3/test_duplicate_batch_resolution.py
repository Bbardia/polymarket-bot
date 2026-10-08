"""Offline-only evidence tests for batch duplicate chronology resolution."""
import json
import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from src.v3.ledger import EventLedger, LedgerEvent
from src.v3.api import CompleteAccountCashFlowHistory
from src.v3.config import V3Settings
from src.v3.live_runner import LiveRunnerSettings, local_snapshot, resolve_live_duplicate_batch
from src.v3.live_shadow import LiveShadowSettings
from src.v3.reconciliation import (CompleteAccountTradeHistory, Reconciler, RemotePosition,
                                   RemoteSnapshot, RemoteTrade, RemoteTradeMaker)
from src.v3.streaming import StreamEventProcessor


def fixture(tmp_path, *, bad_maker=False, managed_taker=False, conflict=False):
    ledger = EventLedger(tmp_path / "ledger.sqlite")
    (tmp_path / "live_state.json").write_text(json.dumps({"baseline_cash": "100", "baseline_epoch": 0,
                                                           "external_condition_ids": []}))
    for order in ("a", "b"):
        ledger.append(LedgerEvent.create("order.accepted", {
            "client_order_id": "client-" + order, "order_id": order, "status": "live",
            "condition_id": "cond", "token_id": "tok", "side": "BUY", "price": "0.5",
            "requested_size": "2", "post_only": True,
        }))
    processor = StreamEventProcessor(ledger)
    original_rows = []
    enriched_rows = []
    for trade_id, order in (("t1", "a"), ("t2", "b")):
        payload = {"id": trade_id, "taker_order_id": "a" if managed_taker and order == "b" else "external",
                   "market": "cond", "asset_id": "external-token", "side": "BUY",
                   "size": "2", "price": "0.5", "status": "CONFIRMED", "fee_rate_bps": "0",
                   "timestamp": "2026-01-01T00:00:00+00:00",
                   "maker_orders": [{"order_id": order, "asset_id": "tok", "side": "BUY",
                                     "matched_amount": "2", "price": "0.5", "fee_rate_bps": "0"},
                                    {"order_id": "unmanaged", "asset_id": "other", "side": "SELL",
                                     "matched_amount": "2", "price": "0.5", "fee_rate_bps": "0"}]}
        if bad_maker and order == "b":
            payload["maker_orders"][0]["asset_id"] = "bad"
        # For the valid case, append the exact confirmed stream row and let replay
        # account the managed maker; top-level taker identity is deliberately different.
        original_rows.append({**payload, "market": None})
        enriched_rows.append(payload)
    for payload in original_rows + enriched_rows:
        ledger.append(LedgerEvent.create("user.trade", payload))
    if conflict:
        ledger.append(LedgerEvent.create("user.trade", {**payload, "price": "0.6"}))
    processor = StreamEventProcessor(ledger)
    processor.require_reconciliation("confirmed fill chronology or ledger association is invalid")
    remote = RemoteSnapshot(D("98"), (RemotePosition("cond", "tok", D("4"), D("2"), D("2")),), ())
    policy = Reconciler(cash_tolerance=D("0"), cost_tolerance=D("0.01"))
    return ledger, processor, remote, policy


def resolve(processor, remote, policy):
    now = datetime.now(timezone.utc)
    confirmed = {row.payload["id"] for row in processor.ledger.events()
                 if row.event_type == "user.trade" and row.payload.get("status") == "CONFIRMED"}
    proof = {"after": 0, "trade_ids": sorted(confirmed), "trade_max_items": 100,
             "trade_page_limit": 10, "trade_fetched_at": now.isoformat(),
             "flow_after": 0, "flow_count": 0, "flow_max_items": 100,
             "flow_page_size": 50, "flow_fetched_at": now.isoformat()}
    return processor.resolve_corrected_duplicate_batch_latch(
        remote=remote, reconciler=policy, account_snapshot_fetched_at=now,
        history_proof=proof)


def test_batch_resolution_replays_all_groups_and_preserves_first_index(tmp_path):
    ledger, processor, remote, policy = fixture(tmp_path)
    before = local_snapshot(processor, D("100"))
    assert resolve(processor, remote, policy).accepted
    proof = tuple(ledger.events())[-1].payload["evidence"]
    assert proof["version"] == 1
    assert [group["trade_id"] for group in proof["groups"]] == ["t1", "t2"]
    assert [group["first_ledger_index"] for group in proof["groups"]] == [2, 3]
    replayed = StreamEventProcessor(ledger)
    assert not replayed.reconciliation_required
    assert local_snapshot(replayed, D("100")) == before


@pytest.mark.parametrize("change", [
    lambda e: e["groups"].pop(),
    lambda e: e["groups"][0].update(first_ledger_index=3),
    lambda e: e["groups"][1].update(managed_order_id="a"),
    lambda e: e.update(version=2),
    lambda e: e.update(local_snapshot_sha256="0" * 64),
    lambda e: e.update(cash_tolerance="100"),
    lambda e: e["history_proof"].update(flow_count=1),
    lambda e: e["history_proof"].update(trade_ids=["t1"]),
    lambda e: e["history_proof"].update(after=1),
])
def test_tampered_batch_evidence_remains_latched(tmp_path, change):
    ledger, processor, remote, policy = fixture(tmp_path)
    resolve(processor, remote, policy)
    events = tuple(ledger.events())
    proof = events[-1]
    replacement = dict(proof.payload)
    evidence = json.loads(json.dumps(replacement["evidence"]))
    change(evidence)
    replacement["evidence"] = evidence
    copied = EventLedger(tmp_path / "tampered.sqlite")
    for row in events[:-1]:
        copied.append(row)
    copied.append(LedgerEvent.create(proof.event_type, replacement, occurred_at=proof.occurred_at))
    assert StreamEventProcessor(copied).reconciliation_required


@pytest.mark.parametrize("variant", ["bad_maker", "managed_taker", "conflict"])
def test_ambiguous_batch_never_appends_resolution(tmp_path, variant):
    ledger, processor, remote, policy = fixture(tmp_path, **{variant: True})
    before = len(tuple(ledger.events()))
    with pytest.raises(ValueError):
        resolve(processor, remote, policy)
    assert not any(row.event_type == "stream.reconciliation_resolved" for row in ledger.events())
    assert len(tuple(ledger.events())) >= before


def test_unsafe_policy_or_snapshot_does_not_append(tmp_path):
    ledger, processor, remote, policy = fixture(tmp_path)
    before = len(tuple(ledger.events()))
    with pytest.raises(ValueError):
        resolve(processor, remote, Reconciler(cash_tolerance=D("1"), cost_tolerance=D("0.01")))
    with pytest.raises(ValueError):
        resolve(processor, RemoteSnapshot(D("97"), remote.positions, ()), policy)
    assert len(tuple(ledger.events())) == before


def test_other_latch_blocks_batch(tmp_path):
    ledger, processor, remote, policy = fixture(tmp_path)
    processor.require_reconciliation("unrelated manual blocker")
    with pytest.raises(ValueError, match="exactly one chronology latch"):
        resolve(processor, remote, policy)
    assert not any(row.event_type == "stream.reconciliation_resolved" for row in ledger.events())


def test_missing_match_time_refuses_batch(tmp_path):
    ledger, processor, remote, policy = fixture(tmp_path)
    copied = EventLedger(tmp_path / "missing-time.sqlite")
    for row in ledger.events():
        if row.event_type == "user.trade" and row.payload["id"] == "t2":
            copied.append(LedgerEvent.create(row.event_type, {**row.payload, "timestamp": None}))
        else:
            copied.append(row)
    with pytest.raises(ValueError):
        resolve(StreamEventProcessor(copied), remote, policy)
    assert StreamEventProcessor(copied).reconciliation_required


def test_unrelated_event_between_reimport_and_latch_refuses_batch(tmp_path):
    ledger, _, remote, policy = fixture(tmp_path)
    copied = EventLedger(tmp_path / "intervening.sqlite")
    for row in ledger.events():
        if row.event_type == "stream.reconciliation_required":
            copied.append(LedgerEvent.create("account.preflight.blocked", {"reason": "unrelated"}))
        copied.append(row)
    with pytest.raises(ValueError, match="immediately before latch"):
        resolve(StreamEventProcessor(copied), remote, policy)


def test_ledger_conditional_resolution_append_rejects_concurrent_tip(tmp_path):
    ledger = EventLedger(tmp_path / "atomic.sqlite")
    ledger.append(LedgerEvent.create("first", {}))
    observed = tuple(ledger.events())
    ledger.append(LedgerEvent.create("concurrent", {}))
    with pytest.raises(ValueError, match="ledger changed"):
        ledger.append_if_unchanged(LedgerEvent.create("resolution", {}),
                                   event_count=len(observed), last_event_id=observed[-1].event_id)
    assert [row.event_type for row in ledger.events()] == ["first", "concurrent"]


@pytest.mark.parametrize("failure", [None, "extra_trade", "cash_flow"])
def test_trusted_runner_fetches_full_history_before_batch_write(tmp_path, monkeypatch, failure):
    ledger, _, remote, _ = fixture(tmp_path)
    at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    trades = tuple(RemoteTrade(
        trade_id=trade_id, condition_id="cond", token_id="external-token",
        taker_order_id="external", side="BUY", trader_side="MAKER", price=D("0.5"),
        size=D("2"), status="CONFIRMED", matched_at=at, updated_at=None,
        fee_rate_bps=D("0"), transaction_hash=None,
        maker_orders=(RemoteTradeMaker(order, "tok", "BUY", D("0.5"), D("2"), D("0")),),
    ) for trade_id, order in (("t1", "a"), ("t2", "b")))
    if failure == "extra_trade":
        trades += (replace(trades[0], trade_id="unmanaged"),)

    class ReadOnlyAPI:
        secure_called = False

        def __init__(self, *, settings):
            pass

        async def initialize_account_client(self):
            return self

        async def initialize_secure_client(self):
            self.secure_called = True
            raise AssertionError("resolver must never create an order client")

        def _authenticated_client(self):
            return self

        async def close(self):
            pass

        async def fetch_complete_account_trade_history(self, *, after, max_items, page_limit):
            return CompleteAccountTradeHistory(after, trades, datetime.now(timezone.utc), max_items, page_limit)

        async def fetch_complete_account_cash_flow_history(self, *, after, max_items):
            return CompleteAccountCashFlowHistory(after, (), datetime.now(timezone.utc), max_items, 500)

        async def fetch_remote_snapshot(self):
            return remote

    # Use a valid flow model when testing nonempty activity.
    from src.v3.api import AccountCashFlow
    if failure == "cash_flow":
        flow = AccountCashFlow("0x" + "1" * 64 + ":DEPOSIT", "DEPOSIT", at,
                               "0x" + "1" * 64, D("1"))
        async def fetch_flows(self, *, after, max_items):
            return CompleteAccountCashFlowHistory(after, (flow,), datetime.now(timezone.utc), max_items, 500)
        ReadOnlyAPI.fetch_complete_account_cash_flow_history = fetch_flows
    monkeypatch.setattr("src.v3.live_runner.UnifiedPolymarketAPI", ReadOnlyAPI)
    settings = V3Settings(live_enabled=True, paper_trading=False,
                          live_confirmation="I_UNDERSTAND_REAL_MONEY",
                          private_key="0x" + "1" * 64, wallet_address="0x" + "2" * 40,
                          max_capital=D("100"), max_order_notional=D("2"),
                          max_daily_loss=D("10"), max_drawdown_amount=D("10"))
    runner = LiveRunnerSettings(shadow=LiveShadowSettings(data_dir=tmp_path))
    before = len(tuple(ledger.events()))
    if failure:
        with pytest.raises(ValueError):
            asyncio.run(resolve_live_duplicate_batch(settings, runner, assert_worker_stopped=lambda: None))
        assert len(tuple(ledger.events())) == before
    else:
        result = asyncio.run(resolve_live_duplicate_batch(settings, runner, assert_worker_stopped=lambda: None))
        assert result["lifecycle_clear"]
        assert not StreamEventProcessor(ledger).reconciliation_required

