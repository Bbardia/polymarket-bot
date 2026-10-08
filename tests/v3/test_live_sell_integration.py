"""Offline integration of the real early-exit runner, gated service, risk and SDK adapter."""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from src.v3.config import V3Settings
from src.v3.ledger import EventLedger, LedgerEvent
from src.v3.live_runner import LiveRunnerSettings, LiveStore, LiveTradingRunner, local_snapshot
from src.v3.live_service import LiveOrderService, LiveRiskContext
from src.v3.live_shadow import LiveShadowSettings, risk_limits_for
from src.v3.reconciliation import (
    Reconciler, RemoteOrder, RemotePosition, RemoteSnapshot, RemoteTrade, RemoteTradeMaker,
)
from src.v3.risk import RiskEngine
from src.v3.streaming import StreamEventProcessor


CONDITION = "managed-condition"
TOKEN = "managed-token"
BUY = "managed-buy"
BASELINE = D("100")


class OfflineAuthenticatedAPI:
    """Authenticated read and SDK signing/posting seam; never opens a socket."""

    def __init__(self, settings):
        self.settings = settings
        self.initialized = False
        self.created = []
        self.posted = []
        self.trades: tuple[RemoteTrade, ...] = ()
        self.book_time = datetime.now(timezone.utc)
        self.remote = RemoteSnapshot(
            D("85"), (RemotePosition(CONDITION, TOKEN, D("30"), D("24"), D("15")),), (),
        )
        self.context_override = {}

    async def initialize_secure_client(self):
        self.initialized = True
        return self

    async def fetch_account_trades(self, *, max_items, page_limit, after):
        assert self.initialized and max_items > 0 and page_limit > 0
        return self.trades

    async def fetch_remote_snapshot(self):
        assert self.initialized
        return self.remote

    async def get_verified_market_context(self, condition_id, token_id):
        assert (condition_id, token_id) == (CONDITION, TOKEN)
        return SimpleNamespace(**{
            "condition_id": CONDITION, "token_id": TOKEN,
            "condition_matches": True, "token_matches": True,
            "rules_verified": True, "accepting_orders": True, "disputed": False,
            "tick_size": D("0.01"), "min_order_size": D("5"),
            "fee_rate": D("0.1"), "fee_exponent": D("1"),
            "fees_enabled": True, "taker_only": True,
            "book_timestamp": self.book_time, "book_hash": "offline-book",
            "book": self._book(), "fetched_at": datetime.now(timezone.utc),
            **self.context_override,
        })

    def _book(self):
        return SimpleNamespace(
            condition_id=CONDITION, token_id=TOKEN, timestamp=self.book_time,
            hash="offline-book", bids=(SimpleNamespace(price=D("0.80"), size=D("50")),),
        )

    async def create_limit_order(self, **kwargs):
        assert self.initialized
        self.created.append(kwargs)
        return {"signed_offline_order": len(self.created)}

    async def post_order(self, signed):
        self.posted.append(signed)
        return SimpleNamespace(ok=True, order_id=f"offline-sell-{len(self.posted)}", status="live")


async def _system(tmp_path, *, external=(), max_order_notional="25"):
    store = LiveStore(tmp_path / "isolated-live")
    now = datetime.now(timezone.utc)
    store.save_state({
        "baseline_at": now.isoformat(), "baseline_epoch": int(now.timestamp()) - 3600,
        "baseline_cash": str(BASELINE), "baseline_equity": str(BASELINE),
        "external_condition_ids": list(external), "peak_equity": str(BASELINE), "event_orders": {},
    })
    ledger = EventLedger(store.ledger_path)
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "buy-client", "order_id": BUY, "status": "live",
        "condition_id": CONDITION, "token_id": TOKEN, "side": "BUY",
        "price": "0.50", "requested_size": "30", "post_only": True,
    }))
    ledger.append(LedgerEvent.create("user.trade", {
        "id": "buy-fill", "taker_order_id": "external-buy", "market": CONDITION,
        "asset_id": TOKEN, "side": "SELL", "size": "30", "price": "0.50",
        "status": "CONFIRMED", "fee_rate_bps": "0", "timestamp": now.isoformat(),
        "maker_orders": [{"order_id": BUY, "asset_id": TOKEN, "side": "BUY",
                          "matched_amount": "30", "price": "0.50", "fee_rate_bps": "0"}],
    }))
    settings = V3Settings(
        live_enabled=True, paper_trading=False,
        live_confirmation="I_UNDERSTAND_REAL_MONEY",
        private_key="0x" + "1" * 64, wallet_address="0x" + "2" * 40,
        max_capital=D("100"), max_order_notional=D(max_order_notional), reserve_fraction=D("0.25"),
        max_daily_loss=D("10"), max_drawdown_amount=D("20"),
    )
    shadow = LiveShadowSettings(data_dir=store.data_dir)
    risk = RiskEngine(risk_limits_for(settings, shadow))
    api = OfflineAuthenticatedAPI(settings)
    policy = Reconciler(external_condition_ids=set(external), cash_tolerance=D("0"),
                        cost_tolerance=D("0.01"), allow_cash_inflows=False)
    service = await LiveOrderService.create(
        api=api, settings=settings, risk_engine=risk, ledger=ledger, reconciler=policy,
        baseline_cash=BASELINE, trade_history_after=store.load_state()["baseline_epoch"],
    )
    runner = LiveTradingRunner(
        service=service, ledger=ledger, reconciler=policy,
        runner_settings=LiveRunnerSettings(shadow=shadow, live_early_exit_enabled=True),
        api=api, settings=settings, shadow=shadow, store=store,
        weather_client=None, forecast=None, observation_provider=None,
    )
    return runner, api


async def _exit(runner, api):
    processor = StreamEventProcessor(runner.ledger)
    api.book_time = datetime.now(timezone.utc)
    return await runner._run_early_exits(
        processor=processor, local=local_snapshot(processor, BASELINE), remote=api.remote,
        risk_context=LiveRiskContext(daily_pnl=D("0"), peak_equity=BASELINE),
        now=datetime.now(timezone.utc),
    )


def test_real_service_sell_acceptance_fill_replay_and_runner_transition(tmp_path):
    async def scenario():
        runner, api = await _system(tmp_path)
        assert api.initialized and runner.service._factory_authorized
        before = local_snapshot(StreamEventProcessor(runner.ledger), BASELINE)
        assert before.position_quantities == {TOKEN: D("30")}
        first = await _exit(runner, api)
        assert first[0]["outcome"] == "accepted", first
        accepted = [e.payload for e in runner.ledger.events()
                    if e.event_type == "order.accepted" and e.payload.get("side") == "SELL"]
        assert len(accepted) == 1
        order = accepted[0]
        assert order["order_id"] == "offline-sell-1"
        assert order["exit_stage"] == "first_tranche" and order["target_return"] == "0.28"
        assert order["decision_id"] == first[0]["decision_id"]
        assert order["requested_size"] == "22.50"
        assert len(api.created) == 1
        created = api.created[0]
        assert {key: created[key] for key in ("token_id", "price", "size", "side", "post_only")} == {
            "token_id": TOKEN, "price": D(order["price"]), "size": D("22.50"),
            "side": "SELL", "post_only": True,
        }
        assert isinstance(created["expiration"], int) and created["expiration"] == order["expiration"]
        assert api.posted == [{"signed_offline_order": 1}]
        replay = StreamEventProcessor(EventLedger(runner.ledger.path))
        assert replay.orders[order["order_id"]].confirmed_size == D("0")
        assert local_snapshot(replay, BASELINE).position_quantities == {TOKEN: D("30")}
        assert local_snapshot(replay, BASELINE).cash == D("85")

        # Even a remotely resting SELL must not be treated as a confirmed fill or
        # released inventory; an active managed order bars a second submission.
        api.remote = RemoteSnapshot(D("85"), api.remote.positions,
                                    (RemoteOrder(order["order_id"], CONDITION, TOKEN,
                                                 D(order["price"]) * D("22.50")),))
        blocked = await _exit(runner, api)
        assert blocked[0]["outcome"] == "blocked" and "active" in blocked[0]["reason"]
        assert len(api.created) == 1

        price = D(order["price"])
        api.trades = (RemoteTrade(
            trade_id="confirmed-sell", condition_id=CONDITION, token_id=TOKEN,
            taker_order_id="external-taker", side="BUY", trader_side="MAKER",
            price=price, size=D("22.50"), status="CONFIRMED",
            matched_at=datetime.now(timezone.utc), updated_at=None, fee_rate_bps=D("0"),
            transaction_hash=None, maker_orders=(RemoteTradeMaker(
                order_id=order["order_id"], token_id=TOKEN, side="SELL",
                price=price, matched_amount=D("22.50"), fee_rate_bps=D("0"),
            ),),
        ),)
        api.remote = RemoteSnapshot(
            D("85") + price * D("22.50"),
            (RemotePosition(CONDITION, TOKEN, D("7.50"), D("6"), D("3.75")),), (),
        )
        recovered = await runner.service.recover_trade_history(max_items=100, page_limit=100)
        assert recovered == {"imported_count": 1, "lifecycle_clear": True}
        after = StreamEventProcessor(EventLedger(runner.ledger.path))
        assert after.orders[order["order_id"]].confirmed_size == D("22.50")
        local = local_snapshot(after, BASELINE)
        assert local.position_quantities == {TOKEN: D("7.50")}
        assert local.position_cost_basis == {TOKEN: D("3.7500")}
        assert local.cash == api.remote.cash
        assert runner.reconciler.compare(local, api.remote).safe_to_trade
        assert (await runner.service.recover_trade_history(max_items=100, page_limit=100))["imported_count"] == 0
        assert local_snapshot(StreamEventProcessor(runner.ledger), BASELINE) == local
        runner_exit = await _exit(runner, api)
        assert runner_exit[0]["outcome"] == "accepted", runner_exit
        next_order = [e.payload for e in runner.ledger.events()
                      if e.event_type == "order.accepted" and e.payload.get("side") == "SELL"][-1]
        assert next_order["exit_stage"] == "runner"
        assert next_order["requested_size"] == "7.50"
        assert local_snapshot(StreamEventProcessor(runner.ledger), BASELINE).position_quantities == {TOKEN: D("7.50")}
    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["external", "unverified", "cash_mismatch", "risk_limit"])
def test_real_service_early_exit_fails_closed_without_sdk_post(tmp_path, failure):
    async def scenario():
        runner, api = await _system(
            tmp_path, external=(CONDITION,) if failure == "external" else (),
            max_order_notional="10" if failure == "risk_limit" else "25",
        )
        if failure == "unverified":
            api.context_override["rules_verified"] = False
        if failure == "cash_mismatch":
            api.remote = RemoteSnapshot(D("84"), api.remote.positions, ())
        result = await _exit(runner, api)
        assert not api.created and not api.posted
        assert not any(e.event_type == "order.accepted" and e.payload.get("side") == "SELL"
                       for e in runner.ledger.events())
        if failure == "external":
            assert result == []
        else:
            assert result[0]["outcome"] in {"blocked", "rejected"}, result
        if failure == "risk_limit":
            assert any(e.event_type == "order.risk_rejected" and "max order notional" in e.payload["reason"]
                       for e in runner.ledger.events())
        assert local_snapshot(StreamEventProcessor(runner.ledger), BASELINE).position_quantities == {TOKEN: D("30")}
    asyncio.run(scenario())
