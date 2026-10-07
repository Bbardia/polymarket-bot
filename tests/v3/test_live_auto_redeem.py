import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, patch

import pytest

from src.v3.live_auto_redeem import (
    auto_redeem_one_managed_winner, managed_winning_candidates,
    pending_auto_redemption_tokens,
)
from src.v3.api import UnifiedPolymarketAPI
from src.v3.config import V3Settings
from src.v3.live_runner import LiveRunnerSettings
from src.v3.live_shadow import LiveShadowSettings
from src.v3.ledger import EventLedger, LedgerEvent
from src.v3.reconciliation import Reconciler, RemotePosition, RemoteSnapshot
from src.v3.streaming import StreamEventProcessor

D = Decimal
NOW = datetime(2026, 10, 7, 7, 0, tzinfo=timezone.utc)
TX = "0x" + "a" * 64


def _ledger(token="managed", condition="weather-1", quantity="5"):
    ledger = EventLedger(":memory:")
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "c1", "order_id": "o1", "status": "live",
        "condition_id": condition, "token_id": token, "side": "BUY",
        "price": "0.5", "requested_size": quantity, "post_only": True,
    }))
    ledger.append(LedgerEvent.create("user.trade", {
        "id": "t1", "taker_order_id": "o1", "market": condition,
        "asset_id": token, "side": "BUY", "size": quantity,
        "price": "0.5", "status": "CONFIRMED", "owner": "wallet",
        "fee_rate_bps": "0",
    }))
    return ledger


def _remote(value="5", quantity: str | None="5", cash="97.5", redeemable=True):
    positions = () if quantity is None else (
        RemotePosition("weather-1", "managed", D(quantity), D(value), D("2.5"), redeemable),
    )
    return RemoteSnapshot(D(cash), positions, ())


class FakeAccount:
    def __init__(self):
        self.calls = []

    async def redeem_positions(self, **kwargs):
        self.calls = getattr(self, "calls", []) + [kwargs]
        return NS(transaction_hash=TX, transaction_id="relay-1", wait=self.wait)

    async def wait(self):
        return NS(transaction_hash=TX, transaction_id="relay-1")


class FakeAPI:
    def __init__(self, after):
        self.settings = NS(
            wallet_address="0x" + "1" * 40,
            builder_api_key="builder-key", builder_secret="builder-secret",
            builder_passphrase="builder-passphrase",
        )
        self.client = FakeAccount()
        self.after = after

    def _authenticated_client(self):
        return self.client

    async def fetch_market_is_neg_risk(self, condition_id):
        return False

    async def fetch_resolved_winner(self, condition_id):
        return "managed"

    async def fetch_remote_snapshot(self):
        return self.after

    async def fetch_redemption_activity(self, **kwargs):
        return (NS(
            type="REDEEM", wallet=self.settings.wallet_address,
            timestamp=NOW, transaction_hash=TX, condition_id="weather-1",
            amount=D("5"),
        ),)


def _processor(ledger):
    return StreamEventProcessor(ledger)


def test_only_exact_managed_redeemable_positive_mark_winner_is_candidate():
    ledger = _ledger()
    remote = _remote()
    candidates = managed_winning_candidates(
        ledger, _processor(ledger), D("100"), remote, Reconciler(), frozenset(),
    )
    assert [(c.condition_id, c.token_id, c.quantity) for c in candidates] == [
        ("weather-1", "managed", D("5")),
    ]


def test_zero_value_loser_is_not_auto_burned():
    ledger = _ledger()
    assert managed_winning_candidates(
        ledger, _processor(ledger), D("100"), _remote(value="0"), Reconciler(), frozenset(),
    ) == ()


def test_zero_value_loser_sharing_condition_blocks_condition_wide_redeem():
    ledger = _ledger()
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "c2", "order_id": "o2", "status": "live",
        "condition_id": "weather-1", "token_id": "managed-loser", "side": "BUY",
        "price": "0.5", "requested_size": "5", "post_only": True,
    }))
    ledger.append(LedgerEvent.create("user.trade", {
        "id": "t2", "taker_order_id": "o2", "market": "weather-1",
        "asset_id": "managed-loser", "side": "BUY", "size": "5",
        "price": "0.5", "status": "CONFIRMED", "owner": "wallet",
        "fee_rate_bps": "0",
    }))
    remote = RemoteSnapshot(D("95"), (
        RemotePosition("weather-1", "managed", D("5"), D("5"), D("2.5"), True),
        RemotePosition("weather-1", "managed-loser", D("5"), D("0"), D("2.5"), True),
    ), ())
    assert managed_winning_candidates(
        ledger, _processor(ledger), D("100"), remote, Reconciler(), frozenset(),
    ) == ()


def test_external_condition_is_never_selected():
    ledger = _ledger()
    assert managed_winning_candidates(
        ledger, _processor(ledger), D("100"), _remote(), Reconciler(), frozenset({"weather-1"}),
    ) == ()


def test_unredeemable_or_mismatched_position_is_not_selected():
    ledger = _ledger()
    for position in (
        _remote(redeemable=False),
        _remote(quantity="4", value="4"),
    ):
        assert managed_winning_candidates(
            ledger, _processor(ledger), D("100"), position, Reconciler(), frozenset(),
        ) == ()


def test_any_account_parity_failure_prevents_candidates():
    ledger = _ledger()
    remote = RemoteSnapshot(D("97.49"), (
        RemotePosition("weather-1", "managed", D("5"), D("5"), D("2.5"), True),
        RemotePosition("external", "unknown", D("1"), D("0"), D("1"), True),
    ), ())
    assert managed_winning_candidates(
        ledger, _processor(ledger), D("100"), remote, Reconciler(), frozenset(),
    ) == ()


@pytest.mark.parametrize("neg_risk,winner", [(True, "managed"), (False, "other")])
def test_unsupported_market_or_nonwinning_token_is_not_submitted(neg_risk, winner):
    ledger = _ledger()
    remote = _remote()
    api = FakeAPI(_remote(quantity=None, cash="102.5"))
    api.fetch_market_is_neg_risk = AsyncMock(return_value=neg_risk)
    api.fetch_resolved_winner = AsyncMock(return_value=winner)
    with patch("src.v3.live_redemption_chain.verify_ctf_redemption_transaction", new=AsyncMock()):
        fresh, records, pending = asyncio.run(auto_redeem_one_managed_winner(
            ledger=ledger, processor=_processor(ledger), baseline_cash=D("100"), remote=remote,
            api=api, reconciler=Reconciler(), external_condition_ids=frozenset(),
            baseline_epoch=0, now=NOW,
        ))
    assert api.client.calls == []
    assert records == () and pending is None


def test_successful_submission_requires_receipt_and_account_recognition(monkeypatch):
    ledger = _ledger()
    remote = _remote()
    after = _remote(quantity=None, cash="102.5")
    api = FakeAPI(after)
    verifier = AsyncMock()
    monkeypatch.setattr("src.v3.live_redemption_chain.verify_ctf_redemption_transaction", verifier)
    async def fake_recognize(ledger, processor, baseline_cash, remote, api, **kwargs):
        ledger.append(LedgerEvent.create("position.redeemed", {
            "condition_id": "weather-1", "token_id": "managed", "quantity": "5",
            "payout": "5", "transaction_hash": TX,
            "redeemed_at": NOW.isoformat(), "activity_type": "REDEEM",
            "winning_token_id": "managed", "account_snapshot_sha256": "a" * 64,
        }))
        return ("ledger-row-1",)
    monkeypatch.setattr("src.v3.live_redemption.recognize_remote_redemptions", fake_recognize)
    fresh, records, pending = asyncio.run(auto_redeem_one_managed_winner(
        ledger=ledger, processor=_processor(ledger), baseline_cash=D("100"), remote=remote,
        api=api, reconciler=Reconciler(), external_condition_ids=frozenset(),
        baseline_epoch=0, now=NOW,
    ))
    assert api.client.calls == [{
        "condition_id": "weather-1",
        "metadata": "Auto-redeem verified bot-managed winning position",
    }]
    assert verifier.await_count == 1
    assert fresh == after and records == ("ledger-row-1",) and pending is None


def test_ambiguous_submission_is_durable_and_never_retried(monkeypatch):
    ledger = _ledger()
    api = FakeAPI(_remote(quantity=None, cash="102.5"))
    async def timeout():
        raise TimeoutError("unknown relay state")
    api.client.wait = timeout
    monkeypatch.setattr("src.v3.live_redemption_chain.verify_ctf_redemption_transaction", AsyncMock())
    with pytest.raises(TimeoutError):
        asyncio.run(auto_redeem_one_managed_winner(
            ledger=ledger, processor=_processor(ledger), baseline_cash=D("100"), remote=_remote(),
            api=api, reconciler=Reconciler(), external_condition_ids=frozenset(),
            baseline_epoch=0, now=NOW,
        ))
    fresh, records, pending = asyncio.run(auto_redeem_one_managed_winner(
        ledger=ledger, processor=_processor(ledger), baseline_cash=D("100"), remote=_remote(),
        api=api, reconciler=Reconciler(), external_condition_ids=frozenset(),
        baseline_epoch=0, now=NOW,
    ))
    assert len(api.client.calls) == 1
    assert pending is not None and records == ()
    assert any(e.event_type == "auto_redemption.submission_accepted" for e in ledger.events())


def test_missing_relay_credentials_fail_before_persisting_submission_intent():
    ledger = _ledger()
    api = FakeAPI(_remote(quantity=None, cash="102.5"))
    api.settings.builder_secret = ""
    with pytest.raises(RuntimeError, match="Builder relay credentials"):
        asyncio.run(auto_redeem_one_managed_winner(
            ledger=ledger, processor=_processor(ledger), baseline_cash=D("100"), remote=_remote(),
            api=api, reconciler=Reconciler(), external_condition_ids=frozenset(),
            baseline_epoch=0, now=NOW,
        ))
    assert not any(e.event_type.startswith("auto_redemption.") for e in ledger.events())
    assert api.client.calls == []


def test_prior_redemption_does_not_resolve_later_same_token_intent():
    ledger = _ledger()
    identity = {"condition_id": "weather-1", "token_id": "managed", "quantity": "5"}
    ledger.append(LedgerEvent.create("auto_redemption.submission_started", identity))
    ledger.append(LedgerEvent.create("position.redeemed", {
        **identity, "payout": "5", "transaction_hash": TX,
        "redeemed_at": NOW.isoformat(), "activity_type": "REDEEM",
        "winning_token_id": "managed", "account_snapshot_sha256": "a" * 64,
    }))
    assert pending_auto_redemption_tokens(ledger) == frozenset()
    ledger.append(LedgerEvent.create("auto_redemption.submission_started", identity))
    assert pending_auto_redemption_tokens(ledger) == frozenset({"managed"})


def test_auto_redemption_is_disabled_by_default():
    settings = LiveRunnerSettings(shadow=LiveShadowSettings(data_dir=Path(".")))
    assert settings.auto_redeem_enabled is False


def test_auto_redemption_environment_switch_is_explicit(monkeypatch):
    monkeypatch.setattr(
        "src.v3.live_runner.LiveShadowSettings.from_env",
        lambda root: LiveShadowSettings(data_dir=root),
    )
    monkeypatch.delenv("V3_LIVE_AUTO_REDEEM", raising=False)
    assert LiveRunnerSettings.from_env(Path(".")).auto_redeem_enabled is False
    monkeypatch.setenv("V3_LIVE_AUTO_REDEEM", "true")
    assert LiveRunnerSettings.from_env(Path(".")).auto_redeem_enabled is True


def test_market_neg_risk_metadata_is_explicit_and_unique():
    class MarketFeed:
        def __init__(self, rows):
            self.rows = rows

        async def list_markets(self, **kwargs):
            yield NS(items=self.rows)

    async def check(value):
        api = UnifiedPolymarketAPI(settings=V3Settings())
        cast(Any, api).public_client = MarketFeed([NS(condition_id="weather-1", state=NS(neg_risk=value))])
        return await api.fetch_market_is_neg_risk("weather-1")

    assert asyncio.run(check(False)) is False
    assert asyncio.run(check(True)) is True
    assert asyncio.run(check(None)) is None


def test_market_neg_risk_metadata_refuses_missing_or_ambiguous_market():
    class MarketFeed:
        def __init__(self, rows):
            self.rows = rows

        async def list_markets(self, **kwargs):
            yield NS(items=self.rows)

    async def check(rows):
        api = UnifiedPolymarketAPI(settings=V3Settings())
        cast(Any, api).public_client = MarketFeed(rows)
        return await api.fetch_market_is_neg_risk("weather-1")

    with pytest.raises(RuntimeError, match="exactly one market"):
        asyncio.run(check([]))
    rows = [NS(condition_id="weather-1", state=NS(neg_risk=False))] * 2
    with pytest.raises(RuntimeError, match="multiple market"):
        asyncio.run(check(rows))
