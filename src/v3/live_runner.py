"""Gated real-money runner for the V7 weather strategy.

Entries are post-only GTD BUY orders built by the same path as ``live-shadow``
(V7 evaluation and selection, resolver-station gate, fresh verified context,
unchanged V7 proposal bridge) and submitted only through the factory-built
``LiveOrderService``, which re-checks market rules, account reconciliation and
the hard ``RiskEngine`` limits before any order. Positions are held to
resolution. Optional auto-redemption can claim only bot-managed finalized
winning positions after exact full-account reconciliation; external inventory
and zero-value losses are never submitted.

Account baseline: on first start the runner records the account's existing
positions as external and the start time as the trade-history baseline. Any
later trade, position or open order the bot cannot attribute to itself blocks
new entries.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

from .api import UnifiedPolymarketAPI
from .config import V3Settings, _env_bool
from .ledger import EventLedger
from .live_service import LiveOrderService, LiveRiskContext
from .live_shadow import (
    LiveShadowRunner,
    LiveShadowSettings,
    ShadowStore,
    _json_default,
    risk_limits_for,
    select_v7_candidates,
)
from .paper import build_weather_forecast
from .paper_weather import NOAAStationObservations, OffsetWeatherPublicClient, evaluate_weather_universe
from .reconciliation import (
    CompleteAccountTradeHistory, LocalSnapshot, Reconciler, RemoteAccountOrder, RemoteSnapshot,
)
from .risk import OrderIntent, RiskEngine
from .streaming import StreamEventProcessor

ZERO = Decimal("0")
ONE = Decimal("1")
TRADE_HISTORY_MAX_ITEMS = 10_000
TRADE_HISTORY_PAGE_LIMIT = 100


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class LiveRunnerSettings:
    shadow: LiveShadowSettings
    cost_tolerance: Decimal = Decimal("0.01")
    # Wait this long after GTD expiry before recording an unseen order as
    # expired, so late fill confirmations land on the still-open aggregate.
    expiry_grace_seconds: int = 600
    auto_redeem_enabled: bool = False
    live_early_exit_enabled: bool = False

    def __post_init__(self) -> None:
        if type(self.auto_redeem_enabled) is not bool:
            raise ValueError("auto-redeem setting must be boolean")
        if type(self.live_early_exit_enabled) is not bool:
            raise ValueError("live early-exit setting must be boolean")
        if (not self.cost_tolerance.is_finite() or self.cost_tolerance < ZERO
                or self.cost_tolerance > Decimal("0.01")):
            raise ValueError("cost tolerance must be finite, nonnegative and at most 0.01")
        if self.expiry_grace_seconds < 60:
            raise ValueError("expiry grace must be at least 60 seconds")

    @classmethod
    def from_env(cls, root: Path) -> "LiveRunnerSettings":
        shadow = LiveShadowSettings.from_env(root)
        if "V3_LIVE_DATA_DIR" not in os.environ:
            shadow = replace(shadow, data_dir=(root / "data/live-v7").resolve())
        return cls(
            shadow=shadow,
            cost_tolerance=Decimal(os.getenv("V3_LIVE_COST_TOLERANCE", "0.01")),
            expiry_grace_seconds=int(os.getenv("V3_LIVE_EXPIRY_GRACE_SECONDS", "600")),
            auto_redeem_enabled=_env_bool("V3_LIVE_AUTO_REDEEM", False),
            live_early_exit_enabled=_env_bool("V3_LIVE_EARLY_EXIT_ENABLED", False),
        )


class LiveStore(ShadowStore):
    def __init__(self, data_dir: Path) -> None:
        super().__init__(data_dir)
        self.state_path = data_dir / "live_state.json"
        self.intents_path = data_dir / "live_orders.jsonl"
        self.errors_path = data_dir / "errors.jsonl"
        self.ledger_path = data_dir / "ledger.sqlite"


def local_snapshot(processor: StreamEventProcessor, baseline_cash: Decimal) -> LocalSnapshot:
    """Bot-attributable holdings and expected cash from confirmed fills only.

    BUY accounting preserves the maker-fee treatment: the venue's reported
    taker rate is excluded. Confirmed SELL fees reduce net proceeds. Sell cost
    basis is allocated at the token's average confirmed BUY cost; oversells and
    invalid fill economics fail closed. This is read-model accounting only.
    """
    quantities: dict[str, Decimal] = {}
    costs: dict[str, Decimal] = {}
    cash = baseline_cash
    if isinstance(processor, StreamEventProcessor):
        from .live_accounting import confirmed_fills
        try:
            fills = confirmed_fills(processor)
        except (ArithmeticError, KeyError, TypeError, ValueError):
            processor.require_reconciliation("confirmed fill chronology or ledger association is invalid")
            raise ValueError("confirmed fill chronology or ledger association is invalid")
        for fill in fills:
            size, notional, fees = fill.size, fill.notional, fill.fee
            token = fill.token_id
            if not all(value.is_finite() for value in (size, notional, fees)):
                raise ValueError("confirmed fill economics are invalid")
            if size <= ZERO or notional <= ZERO or fees < ZERO:
                raise ValueError("confirmed fill economics are invalid")
            if fill.side == "BUY":
                quantities[token] = quantities.get(token, ZERO) + size
                costs[token] = costs.get(token, ZERO) + notional
                cash -= notional
            elif fill.side == "SELL":
                held = quantities.get(token, ZERO)
                if size > held or held <= ZERO or fees > notional:
                    processor.require_reconciliation(
                        "chronological confirmed SELL exceeds managed inventory or has invalid proceeds"
                    )
                    raise ValueError("confirmed SELL exceeds managed inventory or has invalid proceeds")
                basis = costs[token]
                allocated = basis * size / held
                remaining = held - size
                cash += notional - fees
                if remaining == ZERO:
                    quantities.pop(token)
                    costs.pop(token)
                else:
                    quantities[token] = remaining
                    costs[token] = basis - allocated
            else:
                raise ValueError("confirmed order side is invalid")
    else:
        # Lightweight accounting fixtures may provide only aggregate snapshots;
        # production always uses StreamEventProcessor and chronological fills.
        sells = []
        for order in processor.orders.values():
            size, notional, fees = (
                order.confirmed_size, order.confirmed_notional, order.confirmed_fees,
            )
            if any(not isinstance(value, Decimal) or not value.is_finite() for value in (size, notional, fees)):
                raise ValueError("confirmed order economics are invalid")
            if size < ZERO or notional < ZERO or fees < ZERO or (size > ZERO and notional <= ZERO):
                raise ValueError("confirmed order economics are invalid")
            if size == ZERO:
                if notional != ZERO or fees != ZERO:
                    raise ValueError("unfilled order has confirmed economics")
                continue
            if not isinstance(order.token_id, str) or not order.token_id:
                raise ValueError("confirmed order token is invalid")
            if order.side == "BUY":
                quantities[order.token_id] = quantities.get(order.token_id, ZERO) + size
                costs[order.token_id] = costs.get(order.token_id, ZERO) + notional
                cash -= notional
            elif order.side == "SELL":
                sells.append(order)
            else:
                raise ValueError("confirmed order side is invalid")
        for order in sells:
            token = order.token_id
            size, notional, fees = order.confirmed_size, order.confirmed_notional, order.confirmed_fees
            held = quantities.get(token, ZERO)
            if size > held or held <= ZERO or fees > notional:
                raise ValueError("confirmed SELL exceeds managed inventory or has invalid proceeds")
            basis = costs[token]
            allocated = basis * size / held
            remaining = held - size
            cash += notional - fees
            if remaining == ZERO:
                quantities.pop(token)
                costs.pop(token)
            else:
                quantities[token] = remaining
                costs[token] = basis - allocated
    # Redemption audit rows are replayed separately from confirmed fill/order state.
    # Malformed, duplicate, or unattributed rows fail closed rather than clearing inventory.
    if isinstance(processor, StreamEventProcessor):
        from .live_redemption import redemption_adjustments
        redeemed, payout = redemption_adjustments(processor.ledger, processor)
        for token, quantity in redeemed.items():
            if quantities.get(token) != quantity:
                raise ValueError("redeemed token does not match confirmed inventory")
            del quantities[token]
            del costs[token]
        cash += payout
    # The account collateral balance is denominated in six-decimal pUSD. SDK
    # float-to-Decimal fill sizes can leave sub-micro-unit arithmetic residue.
    cash = cash.quantize(Decimal("0.000001"))
    return LocalSnapshot(
        cash=cash,
        position_tokens=frozenset(quantities),
        order_ids=processor.active_order_ids,
        position_quantities=quantities,
        position_cost_basis=costs,
    )


def _sell_is_inconsistent(order: Any) -> bool:
    """A SELL whose confirmed fills contradict its requested size or state."""
    from .orders import OrderState
    return (
        order.confirmed_size > order.requested_size
        or (order.state is OrderState.FILLED and order.confirmed_size != order.requested_size)
    )


def _sellable_position_tokens(local: LocalSnapshot) -> frozenset[str]:
    """Held tokens excluding sub-step dust that no venue order could ever sell.

    Accounting (``local_snapshot``/reconciliation) still carries dust exactly.
    """
    from .live_early_exit import is_dust
    quantities = local.position_quantities
    if quantities is None:
        return local.position_tokens
    return frozenset(
        token for token in local.position_tokens if not is_dust(quantities.get(token))
    )


def accepted_expirations(ledger: EventLedger) -> dict[str, int]:
    expirations: dict[str, int] = {}
    for event in ledger.events():
        if event.event_type != "order.accepted":
            continue
        order_id = event.payload.get("order_id")
        expiration = event.payload.get("expiration")
        if isinstance(order_id, str) and order_id and isinstance(expiration, int):
            expirations[order_id] = expiration
    return expirations


def record_expired_orders(
    processor: StreamEventProcessor,
    ledger: EventLedger,
    remote: RemoteSnapshot,
    now: datetime,
    *,
    grace_seconds: int,
) -> list[str]:
    """Mark managed GTD orders terminal once past expiry and absent remotely."""
    remote_ids = {order.order_id for order in remote.open_orders}
    expirations = accepted_expirations(ledger)
    expired: list[str] = []
    for order_id in sorted(processor.active_order_ids - remote_ids):
        expiration = expirations.get(order_id)
        if expiration is None or now.timestamp() < expiration + grace_seconds:
            continue
        result = processor.process(SimpleNamespace(topic="user", type="order", payload={
            "id": order_id,
            "type": "CANCELLATION",
            "status": "canceled",
            "reason": "gtd_expired_absent_from_open_orders",
            "expiration": expiration,
        }))
        if result.accepted and not result.requires_reconciliation:
            expired.append(order_id)
    return expired


async def record_verified_cancellations(
    processor: StreamEventProcessor,
    ledger: EventLedger,
    api: UnifiedPolymarketAPI,
    remote: RemoteSnapshot,
    history: CompleteAccountTradeHistory,
) -> list[str]:
    """Close only missing local orders proven canceled and wholly unfilled remotely."""
    remote_ids = {order.order_id for order in remote.open_orders}
    missing = processor.active_order_ids - remote_ids
    if not missing or not isinstance(history, CompleteAccountTradeHistory):
        return []
    accepted_by_id: dict[str, list[dict[str, Any]]] = {}
    for event in ledger.events():
        if event.event_type != "order.accepted":
            continue
        order_id = event.payload.get("order_id")
        if isinstance(order_id, str) and order_id:
            accepted_by_id.setdefault(order_id, []).append(dict(event.payload))

    canceled: list[str] = []
    for order_id in sorted(missing):
        local_order = processor.orders.get(order_id)
        accepted_rows = accepted_by_id.get(order_id, [])
        if local_order is None or local_order.confirmed_size != ZERO or len(accepted_rows) != 1:
            continue
        try:
            detail = await api.fetch_account_order(order_id)
        except Exception:
            continue
        if not isinstance(detail, RemoteAccountOrder):
            continue
        accepted = accepted_rows[0]
        try:
            accepted_price = Decimal(str(accepted.get("price")))
            accepted_size = Decimal(str(accepted.get("requested_size")))
        except (ArithmeticError, TypeError, ValueError):
            continue
        if (
            detail.order_id != order_id
            or detail.status not in {"CANCELED", "CANCELLED"}
            or detail.size_matched != ZERO
            or detail.original_size != accepted_size
            or detail.price != accepted_price
            or detail.condition_id != accepted.get("condition_id")
            or detail.token_id != accepted.get("token_id")
            or detail.side != accepted.get("side")
            or local_order.token_id != detail.token_id
            or local_order.side != detail.side
            or local_order.requested_size != detail.original_size
        ):
            continue
        referenced_trades = [
            trade for trade in history.trades
            if trade.taker_order_id == order_id
            or any(maker.order_id == order_id for maker in trade.maker_orders)
        ]
        if referenced_trades:
            continue
        result = processor.process(SimpleNamespace(topic="user", type="order", payload={
            "id": order_id,
            "type": "CANCELLATION",
            "status": "CANCELED",
            "reason": "remote_order_detail_canceled_zero_match",
            "condition_id": detail.condition_id,
            "token_id": detail.token_id,
            "side": detail.side,
            "price": str(detail.price),
            "original_size": str(detail.original_size),
            "size_matched": str(detail.size_matched),
        }))
        if result.accepted and not result.requires_reconciliation:
            canceled.append(order_id)
    return canceled


def baseline_from(remote: RemoteSnapshot, now: datetime, configured_external: frozenset[str]) -> dict[str, Any]:
    if remote.open_orders:
        raise RuntimeError(
            "account has open orders at baseline; cancel them or let them expire before the first live start"
        )
    equity = remote.cash + sum((p.current_value for p in remote.positions), ZERO)
    return {
        "baseline_at": now.isoformat(),
        "baseline_epoch": int(now.timestamp()),
        "baseline_cash": str(remote.cash),
        "baseline_equity": str(equity),
        "external_condition_ids": sorted(
            {p.condition_id for p in remote.positions} | set(configured_external)
        ),
        "peak_equity": str(equity),
        "event_orders": {},
    }


class LiveTradingRunner(LiveShadowRunner):
    def __init__(
        self,
        *,
        service: LiveOrderService,
        ledger: EventLedger,
        reconciler: Reconciler,
        runner_settings: LiveRunnerSettings,
        **kwargs: Any,
    ) -> None:
        super().__init__(account_reads=True, **kwargs)
        self.service = service
        self.ledger = ledger
        self.reconciler = reconciler
        self.runner_settings = runner_settings

    async def resolve_corrected_duplicate_trade_latch(self, trade_id: str):
        """Fetch authenticated account state, then attempt the narrow replay-latch resolution."""
        if not isinstance(trade_id, str) or not trade_id:
            raise ValueError("confirmed trade ID is required")
        if not isinstance(self.store, LiveStore):
            raise ValueError("live session store is required")
        ledger_path = self.ledger.path.resolve()
        state_path = self.store.state_path.resolve()
        if (ledger_path != self.store.ledger_path.resolve()
                or state_path != (ledger_path.parent / "live_state.json").resolve()):
            raise ValueError("live ledger and persisted session state paths do not match")
        from .streaming import _corrected_duplicate_resolution_reconciler
        persisted_state = self.store.load_state()
        resolver = _corrected_duplicate_resolution_reconciler(persisted_state)
        remote = await self.api.fetch_remote_snapshot()
        fetched_at = _utc_now()
        processor = StreamEventProcessor(self.ledger)
        return processor._resolve_corrected_duplicate_trade_latch(
            remote=remote, reconciler=resolver, trade_id=trade_id,
            account_snapshot_fetched_at=fetched_at,
        )

    async def _run_early_exits(self, *, processor, local, remote, risk_context, now):
        from uuid import uuid4
        from .api import MarketClosedError
        from .live_early_exit import ExitPosition, is_dust, plan_live_early_exit, verified_book_from_api
        from .orders import OrderState, TradeStatus

        results = []
        accepted_buys = {}
        for event in self.ledger.events():
            if event.event_type == "order.accepted" and event.payload.get("side") == "BUY":
                accepted_buys.setdefault(event.payload.get("token_id"), set()).add(event.payload.get("condition_id"))
        open_remote_ids = {order.order_id for order in remote.open_orders}
        terminal = {OrderState.CANCELED, OrderState.FILLED, OrderState.FAILED}
        for token_id, shares in (local.position_quantities or {}).items():
            if is_dust(shares):
                # Sub-step leftovers can never be sold; they are not exit candidates.
                continue
            conditions = accepted_buys.get(token_id, set())
            matching = [p for p in remote.positions if p.token_id == token_id]
            if len(conditions) != 1 or len(matching) != 1 or matching[0].condition_id not in conditions:
                continue
            condition_id = next(iter(conditions))
            if condition_id in self.reconciler.external_condition_ids:
                continue
            if matching[0].redeemable:
                # Resolved market: nothing can trade; redemption handles it.
                results.append({"token_id": token_id, "outcome": "skipped", "reason": "market closed"})
                continue
            if any(o.token_id == token_id and o.order_id in processor.active_order_ids
                   for o in processor.orders.values()):
                results.append({"token_id": token_id, "outcome": "blocked", "reason": "active managed order"})
                continue
            all_sells = [o for o in processor.orders.values() if o.token_id == token_id and o.side == "SELL"]
            if any(o.order_id in open_remote_ids for o in all_sells):
                results.append({"token_id": token_id, "outcome": "blocked",
                                "reason": "managed SELL is still open on the venue"})
                continue
            if any(t.status is not TradeStatus.CONFIRMED for o in all_sells for t in o.trades.values()):
                # A matched/mined fill may still confirm; wait for finality.
                results.append({"token_id": token_id, "outcome": "blocked",
                                "reason": "SELL fill awaiting confirmation"})
                continue
            inconsistent = [o for o in all_sells if _sell_is_inconsistent(o)]
            if inconsistent:
                if not processor.reconciliation_required:
                    processor.require_reconciliation(
                        "inconsistent early-exit SELL requires manual reconciliation: "
                        + ",".join(o.order_id for o in inconsistent)
                    )
                results.append({"token_id": token_id, "outcome": "manual_review",
                                "reason": "SELL confirmed fills contradict requested size or state"})
                continue
            # A terminal SELL with no trade rows at all, absent from the venue's
            # open orders, closed cleanly unfilled (e.g. an expired post-only
            # quote): it has no inventory effect and a fresh decision is made.
            sells = [o for o in all_sells
                     if not (o.state in terminal and o.confirmed_size == ZERO and not o.trades)]
            accepted_sells = {}
            for event in self.ledger.events():
                if event.event_type == "order.accepted" and event.payload.get("side") == "SELL" \
                        and event.payload.get("token_id") == token_id:
                    accepted_sells.setdefault(event.payload.get("order_id"), []).append(event.payload)
            sell_metadata = {}
            for order in sells:
                rows = accepted_sells.get(order.order_id, [])
                if (
                    len(rows) != 1 or not isinstance(rows[0].get("decision_id"), str)
                    or not rows[0]["decision_id"]
                    or rows[0].get("exit_stage") not in {"first_tranche", "runner", "full"}
                ):
                    results.append({"token_id": token_id, "outcome": "blocked",
                                    "reason": "SELL lacks unique durable early-exit metadata"})
                    break
                sell_metadata[order.order_id] = rows[0]
            if len(sell_metadata) != len(sells):
                continue
            decision_ids = [row["decision_id"] for row in sell_metadata.values()]
            if len(decision_ids) != len(set(decision_ids)):
                results.append({"token_id": token_id, "outcome": "blocked",
                                "reason": "duplicate durable early-exit decision IDs"})
                continue
            # Every remaining SELL is terminal, final and fill-consistent: filled
            # exactly, or canceled/expired with verified partial fills.
            first_tranche_orders = [o for o in sells
                                    if sell_metadata[o.order_id].get("exit_stage") == "first_tranche"]
            runner_orders = [o for o in sells if sell_metadata[o.order_id].get("exit_stage") == "runner"]
            full_orders = [o for o in sells if sell_metadata[o.order_id].get("exit_stage") == "full"]
            if full_orders and runner_orders:
                results.append({"token_id": token_id, "outcome": "blocked",
                                "reason": "runner and full-exit SELLs both exist"})
                continue
            # Once a whole-position SELL has filled anything, hybrid staging no
            # longer applies: the remainder is planned as a full exit.
            hybrid_enabled = not full_orders
            done = False
            tranche_remaining = None
            if first_tranche_orders and hybrid_enabled:
                target = first_tranche_orders[0].requested_size
                confirmed = sum((o.confirmed_size for o in first_tranche_orders), ZERO)
                if (not isinstance(target, Decimal) or not target.is_finite() or target <= ZERO
                        or confirmed > target
                        or any(o.state is not OrderState.CANCELED
                               for o in first_tranche_orders if o.confirmed_size < o.requested_size)):
                    results.append({"token_id": token_id, "outcome": "blocked",
                                    "reason": "first-tranche target/state inconsistent"})
                    continue
                done = confirmed == target
                if not done:
                    if runner_orders:
                        results.append({"token_id": token_id, "outcome": "blocked",
                                        "reason": "runner SELL exists before first tranche completion"})
                        continue
                    # Verified partial first tranche: offer only its unfilled rest.
                    tranche_remaining = target - confirmed
            elif runner_orders and hybrid_enabled:
                results.append({"token_id": token_id, "outcome": "blocked",
                                "reason": "runner SELL exists before first tranche completion"})
                continue
            buys = [o for o in processor.orders.values() if o.token_id == token_id and o.side == "BUY"]
            if not buys:
                continue
            cost = (local.position_cost_basis or {}).get(token_id)
            if not isinstance(cost, Decimal) or cost <= ZERO or shares <= ZERO:
                continue
            try:
                try:
                    context = await self.api.get_verified_market_context(condition_id, token_id)
                except MarketClosedError:
                    results.append({"token_id": token_id, "outcome": "skipped", "reason": "market closed"})
                    continue
                # Verify and plan from the exact book snapshot the context was
                # built from; a second fetch could race a book change.
                raw = getattr(context, "book", None)
                fetched_at = getattr(context, "fetched_at", None)
                if raw is None or not isinstance(fetched_at, datetime) or fetched_at.tzinfo is None:
                    raise ValueError("verified market context lacks its book snapshot or read time")
                checked_at = _utc_now()
                book = verified_book_from_api(
                    context, raw, condition_id=condition_id, token_id=token_id,
                    now=checked_at, max_quote_age_seconds=self.risk.limits.max_quote_age_seconds,
                )
                # The stage transition above is based exclusively on a fully
                # confirmed first-tranche order; never infer it from any SELL fill.
                plan = plan_live_early_exit(
                    ExitPosition("YES", shares, cost, hybrid_exit_done=done, hybrid_enabled=hybrid_enabled),
                    book, first_tranche_quantity=tranche_remaining,
                )
                if plan.intent is None:
                    continue
                sell = plan.intent
                # The planner already stress-tests bid-depth taker fees. A verified
                # taker-only schedule charges a resting post-only maker zero fee;
                # never pass the taker estimate as the submitted maker fee.
                if book.fee_rate > ZERO and getattr(context, "taker_only", None) is not True:
                    raise ValueError("positive-fee SELL lacks verified taker-only maker fee")
                intent = OrderIntent(
                    condition_id=condition_id, token_id=token_id, side="SELL", price=sell.price,
                    shares=sell.size, estimated_fee=ZERO,
                    post_only=True, ttl_seconds=min(3600, self.risk.limits.max_order_ttl_seconds),
                    quote_age_seconds=max(0, int((checked_at - fetched_at).total_seconds())),
                    tick_size=book.tick_size, min_order_size=book.min_order_size,
                    market_accepting_orders=True, rules_verified=True,
                    decision_id=str(uuid4()), exit_stage=sell.stage, target_return=sell.target_return,
                )
                outcome = await self.service.submit(intent, local, risk_context)
                results.append({"token_id": token_id, "outcome": "accepted" if outcome.accepted else "rejected",
                                "decision_id": intent.decision_id, "stage": sell.stage, "reason": outcome.reason})
            except Exception as exc:
                results.append({"token_id": token_id, "outcome": "blocked",
                                "reason": f"{type(exc).__name__}: {str(exc)[:160]}"})
        return results

    def _event_block_reason(
        self, evaluation: Any, remote: RemoteSnapshot, local: LocalSnapshot,
    ) -> str | None:
        if evaluation.condition_id in self.reconciler.external_condition_ids:
            return "condition is held outside the bot (external)"
        orders = self.state.setdefault("event_orders", {}).get(evaluation.event_key, [])
        open_ids = {order.order_id for order in remote.open_orders}
        if any(order.get("order_id") in open_ids for order in orders):
            return "bot order for this event is still resting"
        if any(order.get("token_id") in _sellable_position_tokens(local) for order in orders):
            return "bot already holds a position in this event"
        return None

    async def run_cycle(self, *, now: datetime | None = None) -> dict[str, Any]:
        now = now or _utc_now()
        today = now.date().isoformat()
        status: dict[str, Any] = {
            "mode": "LIVE",
            "healthy": True,
            "cycle_started_at": now.isoformat(),
            "baseline_at": self.state.get("baseline_at"),
            "data_dir": self.store.data_dir,
        }
        entry_block: str | None = None
        try:
            baseline_cash = Decimal(str(self.state["baseline_cash"]))
            baseline_epoch = self.state["baseline_epoch"]
            stored_peak = Decimal(str(self.state["peak_equity"]))
            stored_day_start = self.state.get("day_start_equity")
            if (
                type(baseline_epoch) is not int or baseline_epoch < 0
                or not baseline_cash.is_finite() or baseline_cash < ZERO
                or not stored_peak.is_finite() or stored_peak <= ZERO
                or ("day" in self.state) != (stored_day_start is not None)
            ):
                raise ValueError("persisted risk baseline is inconsistent")
            if stored_day_start is not None:
                day_start_equity = Decimal(str(stored_day_start))
                if not day_start_equity.is_finite() or day_start_equity <= ZERO:
                    raise ValueError("persisted day-start equity is invalid")
        except (ArithmeticError, KeyError, TypeError, ValueError):
            status.update({
                "healthy": False,
                "cycle": self.state.get("cycles", 0),
                "entry_block_reason": "persisted risk baseline invalid",
                "weather_markets_evaluated": 0,
                "weather_forecast_status": "not_started",
                "weather_errors": 0,
                "v7_candidates": 0,
                "outcomes_this_cycle": {},
                "limits": asdict(self.risk.limits),
                "reconciliation": {
                    "safe_to_trade": False,
                    "invalid_snapshot": True,
                    "cash_delta": ZERO,
                    "unknown_positions": 0,
                    "unknown_orders": 0,
                    "missing_positions": 0,
                    "missing_orders": 0,
                    "position_mismatches": 0,
                    "external_positions": 0,
                },
                "cycle_finished_at": _utc_now().isoformat(),
            })
            self.store.write_status(status)
            return status

        recovery = await self.service.recover_trade_history(
            max_items=TRADE_HISTORY_MAX_ITEMS, page_limit=TRADE_HISTORY_PAGE_LIMIT,
        )
        status["trade_history"] = recovery
        remote = await self.api.fetch_remote_snapshot()
        processor = StreamEventProcessor(self.ledger)
        status["orders_marked_expired"] = record_expired_orders(
            processor, self.ledger, remote, now,
            grace_seconds=self.runner_settings.expiry_grace_seconds,
        )
        status["orders_marked_terminal_canceled"] = []
        if processor.active_order_ids - {order.order_id for order in remote.open_orders}:
            try:
                history = await self.api.fetch_complete_account_trade_history(
                    after=int(self.state["baseline_epoch"]),
                    max_items=TRADE_HISTORY_MAX_ITEMS,
                    page_limit=TRADE_HISTORY_PAGE_LIMIT,
                )
                status["orders_marked_terminal_canceled"] = await record_verified_cancellations(
                    processor, self.ledger, self.api, remote, history,
                )
            except Exception as exc:
                status["order_detail_reconciliation_error"] = type(exc).__name__
        # An expired or canceled post-only SELL with zero or partial confirmed
        # fills is a normal outcome handled by the early-exit planner. Only an
        # internally inconsistent SELL aggregate requires manual reconciliation.
        inconsistent_sells = [
            order for order in processor.orders.values()
            if order.side == "SELL" and _sell_is_inconsistent(order)
        ]
        if inconsistent_sells and not processor.reconciliation_required:
            processor.require_reconciliation(
                "inconsistent SELL fill state requires manual reconciliation: "
                + ",".join(order.order_id or order.client_order_id for order in inconsistent_sells)
            )
        if processor.reconciliation_required:
            entry_block = "lifecycle reconciliation latched: " + "; ".join(processor.reconciliation_reasons)
            status["healthy"] = False
            status["lifecycle_reconciliation_required"] = True
            status["lifecycle_reconciliation_reasons"] = list(processor.reconciliation_reasons)

        equity = remote.cash + sum((p.current_value for p in remote.positions), ZERO)
        if self.state.get("day") != today:
            self.state["day"] = today
            self.state["day_start_equity"] = str(equity)
        peak = max(Decimal(self.state["peak_equity"]), equity)
        self.state["peak_equity"] = str(peak)
        daily_pnl = equity - Decimal(self.state["day_start_equity"])

        # Only public, bounded evidence plus exact full-account parity may clear
        # locally held tokens absent from the remote snapshot. A failed lookup
        # blocks entries even if a later comparison appears otherwise safe.
        from .live_redemption import recognize_remote_redemptions
        status["redemptions_recorded"] = []
        try:
            status["redemptions_recorded"] = list(await recognize_remote_redemptions(
                self.ledger, processor, baseline_cash, remote, self.api,
                baseline_epoch=baseline_epoch, now=now, reconciler=self.reconciler,
            ))
        except Exception as exc:
            status["redemption_reconciliation_error"] = type(exc).__name__
            status["healthy"] = False
            entry_block = entry_block or "redemption evidence or account parity could not be verified"

        status["auto_redeem_enabled"] = self.runner_settings.auto_redeem_enabled
        status["live_early_exit_enabled"] = self.runner_settings.live_early_exit_enabled
        status["auto_redemptions_recorded"] = []
        # Auto-redemption failures block entries but never risk-reducing exits:
        # exits re-check full account reconciliation against fresh state.
        pre_redeem_block = entry_block
        if self.runner_settings.auto_redeem_enabled and entry_block is None:
            redeem_skips: list[dict[str, str]] = []
            status["auto_redemption_skipped"] = redeem_skips
            try:
                from .live_auto_redeem import auto_redeem_one_managed_winner
                remote, auto_records, pending_reason = await auto_redeem_one_managed_winner(
                    ledger=self.ledger, processor=processor,
                    baseline_cash=baseline_cash, remote=remote, api=self.api,
                    reconciler=self.reconciler,
                    external_condition_ids=self.reconciler.external_condition_ids,
                    baseline_epoch=baseline_epoch, now=now, skipped=redeem_skips,
                )
                status["auto_redemptions_recorded"] = list(auto_records)
                status["redemptions_recorded"].extend(auto_records)
                if pending_reason:
                    status["healthy"] = False
                    status["auto_redemption_pending_reconciliation"] = True
                    entry_block = pending_reason
                if auto_records:
                    processor = StreamEventProcessor(self.ledger)
                    equity = remote.cash + sum((p.current_value for p in remote.positions), ZERO)
                    peak = max(Decimal(self.state["peak_equity"]), equity)
                    daily_pnl = equity - Decimal(self.state["day_start_equity"])
                    self.state["peak_equity"] = str(peak)
            except Exception as exc:
                status["healthy"] = False
                status["auto_redemption_error"] = type(exc).__name__
                entry_block = entry_block or "auto-redemption submission requires reconciliation"

        local = local_snapshot(processor, baseline_cash)
        report = self.reconciler.compare(local, remote)
        exit_block = pre_redeem_block
        if not report.safe_to_trade:
            exit_block = exit_block or "account reconciliation blocks exits"
        if entry_block is None and not report.safe_to_trade:
            entry_block = "account reconciliation blocked entries"
        limits = self.risk.limits
        if entry_block is None and daily_pnl <= -limits.daily_loss_limit:
            entry_block = "daily loss limit reached"
        if entry_block is None and peak - equity >= limits.max_drawdown_amount:
            entry_block = "maximum drawdown reached"
        if entry_block is None and limits.max_drawdown_fraction is not None and peak > ZERO and (
            (peak - equity) / peak >= limits.max_drawdown_fraction
        ):
            entry_block = "maximum drawdown reached"
        risk_context = LiveRiskContext(
            daily_pnl=daily_pnl,
            peak_equity=peak,
            day_start_equity=Decimal(self.state["day_start_equity"]),
        )

        status.update({
            "cash": remote.cash,
            "expected_bot_cash": local.cash,
            "equity": equity,
            "daily_pnl": daily_pnl,
            "peak_equity": peak,
            "drawdown": peak - equity,
            "open_orders": len(remote.open_orders),
            "bot_active_orders": len(processor.active_order_ids),
            "bot_position_tokens": len(local.position_tokens),
            "bot_cost_basis": sum((local.position_cost_basis or {}).values(), ZERO),
            "reconciliation": {
                "safe_to_trade": report.safe_to_trade and not processor.reconciliation_required,
                "lifecycle_reconciliation_required": processor.reconciliation_required,
                "lifecycle_reconciliation_reasons": list(processor.reconciliation_reasons),
                "cash_delta": report.cash_delta,
                "unknown_positions": len(report.unknown_positions),
                "unknown_orders": len(report.unknown_orders),
                "missing_positions": len(report.missing_positions),
                "missing_orders": len(report.missing_orders),
                "position_mismatches": len(report.position_mismatches),
                "external_positions": len(report.external_positions),
            },
        })

        # Early exits are an independent, opt-in risk-reducing path. Every candidate
        # is derived from confirmed bot BUY inventory and submitted via the gated service.
        status["early_exits"] = []
        status["early_exit_block_reason"] = exit_block
        exit_accepted = False
        if self.runner_settings.live_early_exit_enabled and exit_block is None:
            status["early_exits"] = await self._run_early_exits(
                processor=processor, local=local, remote=remote, risk_context=risk_context, now=now,
            )
            exit_accepted = any(row.get("outcome") == "accepted" for row in status["early_exits"])
            post_exit_processor = StreamEventProcessor(self.ledger)
            if post_exit_processor.reconciliation_required:
                entry_block = entry_block or "early-exit submission requires lifecycle reconciliation"
                status["healthy"] = False
                status["early_exit_block_reason"] = entry_block
                status["lifecycle_reconciliation_required"] = True
                status["lifecycle_reconciliation_reasons"] = list(post_exit_processor.reconciliation_reasons)
                status["reconciliation"]["safe_to_trade"] = False
                status["reconciliation"]["lifecycle_reconciliation_required"] = True
                status["reconciliation"]["lifecycle_reconciliation_reasons"] = list(
                    post_exit_processor.reconciliation_reasons
                )

        policy = self.policy
        if policy.kelly_sizing_enabled:
            bankroll = max(ZERO, min(
                remote.cash, self.settings.max_capital * (ONE - self.settings.reserve_fraction),
            ))
            policy = replace(
                policy, sizing_bankroll=bankroll,
                max_order_notional=min(self.settings.max_order_notional, policy.max_order_notional),
            )
        result = await evaluate_weather_universe(
            client=self.weather_client, forecast=self.forecast, policy=policy,
            observation_provider=self.observation_provider, now=now,
        )
        candidates = select_v7_candidates(result.evaluations)
        outcomes: dict[str, int] = {}
        for evaluation in candidates:
            base = {"at": now.isoformat(), "event_key": evaluation.event_key,
                    "condition_id": evaluation.condition_id, "question": evaluation.question,
                    "submitted": False}
            if exit_accepted:
                record = {**base, "outcome": "blocked", "stage": "account",
                          "reason": "accepted early exit blocks entries for this cycle"}
            elif entry_block is not None:
                record = {**base, "outcome": "blocked", "stage": "account", "reason": entry_block}
            elif (reason := self._event_block_reason(evaluation, remote, local)) is not None:
                record = {**base, "outcome": "skipped", "stage": "event", "reason": reason}
            else:
                record, intent = await self._build_intent(evaluation, now)
                if intent is not None:
                    execution = await self.service.submit(intent, local, risk_context)
                    self.state["peak_equity"] = str(risk_context.peak_equity)
                    order = execution.order
                    record.update(
                        stage="submit",
                        outcome="accepted" if execution.accepted else "rejected",
                        reason=execution.reason,
                        submitted=execution.accepted,
                        order_id=None if order is None else order.order_id,
                        client_order_id=None if order is None else order.client_order_id,
                    )
                    if execution.accepted:
                        self.state.setdefault("event_orders", {}).setdefault(
                            evaluation.event_key, [],
                        ).append({
                            "at": now.isoformat(),
                            "order_id": order.order_id if order else None,
                            "condition_id": evaluation.condition_id,
                            "token_id": evaluation.token_id,
                            "price": str(intent.price),
                            "shares": str(intent.shares),
                        })
            self.store.append(self.store.intents_path, record)
            outcomes[record["outcome"]] = outcomes.get(record["outcome"], 0) + 1
            self.store.save_state(self.state)

        if outcomes.get("accepted", 0) or exit_accepted:
            try:
                final_remote = await self.api.fetch_remote_snapshot()
                final_processor = StreamEventProcessor(self.ledger)
                final_local = local_snapshot(
                    final_processor, Decimal(self.state["baseline_cash"]),
                )
                final_report = self.reconciler.compare(final_local, final_remote)
                if final_report.invalid_snapshot:
                    raise ValueError("post-cycle account snapshot failed validation")
                final_equity = final_remote.cash + sum(
                    (position.current_value for position in final_remote.positions), ZERO,
                )
                prior_peak = Decimal(self.state["peak_equity"])
                day_start_equity = Decimal(self.state["day_start_equity"])
                if (
                    not final_equity.is_finite() or final_equity <= ZERO
                    or not prior_peak.is_finite() or prior_peak <= ZERO
                    or not day_start_equity.is_finite() or day_start_equity <= ZERO
                ):
                    raise ValueError("post-cycle account or risk baseline is invalid")
                final_peak = max(prior_peak, final_equity)
                final_daily_pnl = final_equity - day_start_equity
                if not final_daily_pnl.is_finite():
                    raise ValueError("post-cycle daily P&L is invalid")
                if entry_block is None and not final_report.safe_to_trade:
                    entry_block = "account reconciliation blocked entries after submission"
                if entry_block is None and final_daily_pnl <= -limits.daily_loss_limit:
                    entry_block = "daily loss limit reached after submission"
                if entry_block is None and final_peak - final_equity >= limits.max_drawdown_amount:
                    entry_block = "maximum drawdown reached after submission"
                if entry_block is None and limits.max_drawdown_fraction is not None and final_peak > ZERO and (
                    (final_peak - final_equity) / final_peak >= limits.max_drawdown_fraction
                ):
                    entry_block = "maximum drawdown reached after submission"
                self.state["peak_equity"] = str(final_peak)
                status.update({
                    "cash": final_remote.cash,
                    "expected_bot_cash": final_local.cash,
                    "equity": final_equity,
                    "daily_pnl": final_daily_pnl,
                    "peak_equity": final_peak,
                    "drawdown": final_peak - final_equity,
                    "open_orders": len(final_remote.open_orders),
                    "bot_active_orders": len(final_processor.active_order_ids),
                    "bot_position_tokens": len(final_local.position_tokens),
                    "bot_cost_basis": sum((final_local.position_cost_basis or {}).values(), ZERO),
                    "reconciliation": {
                        "safe_to_trade": final_report.safe_to_trade,
                        "cash_delta": final_report.cash_delta,
                        "unknown_positions": len(final_report.unknown_positions),
                        "unknown_orders": len(final_report.unknown_orders),
                        "missing_positions": len(final_report.missing_positions),
                        "missing_orders": len(final_report.missing_orders),
                        "position_mismatches": len(final_report.position_mismatches),
                        "external_positions": len(final_report.external_positions),
                    },
                })
            except Exception as exc:
                status["healthy"] = False
                status["post_cycle_reconciliation_error"] = type(exc).__name__
                status["reconciliation"]["safe_to_trade"] = False
                if entry_block is None:
                    entry_block = "post-cycle account verification failed"

        self.state["cycles"] = int(self.state.get("cycles", 0)) + 1
        self.store.save_state(self.state)
        status.update({
            "cycle": self.state["cycles"],
            "entry_block_reason": entry_block,
            "weather_markets_evaluated": result.markets_evaluated,
            "weather_forecast_status": result.forecast_status,
            "weather_errors": len(result.errors),
            "v7_candidates": len(candidates),
            "outcomes_this_cycle": outcomes,
            "limits": asdict(self.risk.limits),
            "cycle_finished_at": _utc_now().isoformat(),
        })
        self.store.write_status(status)
        return status


async def _start(settings: V3Settings, runner_settings: LiveRunnerSettings):
    shadow = runner_settings.shadow
    store = LiveStore(shadow.data_dir)
    store.seed(shadow.seed_dir)
    api = UnifiedPolymarketAPI(settings=settings)
    state = store.load_state()
    risk = RiskEngine(risk_limits_for(settings, shadow))
    ledger = EventLedger(store.ledger_path)
    if "baseline_epoch" not in state:
        await api.initialize_account_client()
        try:
            remote = await api.fetch_remote_snapshot()
        finally:
            await api._authenticated_client().close()
        state.update(baseline_from(remote, _utc_now(), settings.external_condition_ids))
        store.save_state(state)
    reconciler = Reconciler(
        external_condition_ids=frozenset(state["external_condition_ids"]),
        cost_tolerance=runner_settings.cost_tolerance,
        cash_tolerance=ZERO,
        allow_cash_inflows=False,
    )
    try:
        baseline_cash = Decimal(str(state["baseline_cash"]))
    except (ArithmeticError, KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("persisted account baseline cash is missing or invalid") from exc
    if not baseline_cash.is_finite() or baseline_cash < ZERO:
        raise RuntimeError("persisted account baseline cash is missing or invalid")
    # The gated factory binds the trusted runner-owned baseline for SELL replay.
    service = await LiveOrderService.create(
        api=api, settings=settings, risk_engine=risk, ledger=ledger,
        reconciler=reconciler, trade_history_after=int(state["baseline_epoch"]),
        baseline_cash=baseline_cash,
    )
    return store, api, ledger, reconciler, service


async def resolve_live_duplicate_batch(
    settings: V3Settings, runner_settings: LiveRunnerSettings, *,
    assert_worker_stopped: Callable[[], None],
) -> dict[str, Any]:
    """Resolve only an audited import-batch latch, without constructing an order client."""
    errors = settings.live_client_errors()
    if errors:
        raise RuntimeError("Live resolution refused: " + "; ".join(errors))
    assert_worker_stopped()
    store = LiveStore(runner_settings.shadow.data_dir)
    if not store.state_path.is_file() or not store.ledger_path.is_file():
        raise ValueError("persisted live ledger and session baseline are required")
    state_digest = hashlib.sha256(store.state_path.read_bytes()).digest()
    state = store.load_state()
    epoch = state.get("baseline_epoch")
    if type(epoch) is not int or epoch < 0:
        raise ValueError("persisted trade-history baseline is invalid")
    ledger = EventLedger(store.ledger_path)
    processor = StreamEventProcessor(ledger)
    from .live_accounting import confirmed_fills
    from .streaming import _corrected_duplicate_resolution_reconciler, _venue_match_time
    fills = confirmed_fills(processor)
    local_ids = {fill.trade_id for fill in fills}
    if len(local_ids) != len(fills) or not local_ids:
        raise ValueError("managed confirmed fills are not unique")
    policy = _corrected_duplicate_resolution_reconciler(state)
    if policy.cost_tolerance != runner_settings.cost_tolerance:
        raise ValueError("runner reconciliation policy differs from persisted strict policy")
    api = UnifiedPolymarketAPI(settings=settings)
    await api.initialize_account_client()  # read-only; never initialize_secure_client
    try:
        history = await api.fetch_complete_account_trade_history(
            after=epoch, max_items=TRADE_HISTORY_MAX_ITEMS, page_limit=TRADE_HISTORY_PAGE_LIMIT,
        )
        flows = await api.fetch_complete_account_cash_flow_history(after=epoch, max_items=10_000)
        remote = await api.fetch_remote_snapshot()
        fetched_at = _utc_now()
    finally:
        await api._authenticated_client().close()
    if (history.after != epoch or flows.after != epoch or flows.flows
            or {row.trade_id for row in history.trades} != local_ids
            or remote.open_orders):
        raise ValueError("complete account trade/flow history or open orders do not match managed baseline")
    events = tuple(ledger.events())
    for trade in history.trades:
        original = next((row for row in events if row.event_type == "user.trade"
                         and row.payload.get("id") == trade.trade_id
                         and row.payload.get("status") == "CONFIRMED"), None)
        if original is None or trade.status != "CONFIRMED":
            raise ValueError("authenticated trade history does not match confirmed ledger")
        if trade.matched_at != _venue_match_time(original.payload):
            raise ValueError("authenticated venue match time differs from ledger")
        fill = next(fill for fill in fills if fill.trade_id == trade.trade_id)
        makers = [maker for maker in trade.maker_orders if maker.order_id == fill.order_id]
        if (trade.condition_id not in {row.payload.get("condition_id") for row in ledger.events()
                                        if row.event_type == "order.accepted" and row.payload.get("order_id") == fill.order_id}
                or len(makers) != 1 or makers[0].token_id != fill.token_id
                or makers[0].side != fill.side or makers[0].matched_amount != fill.size
                or makers[0].price != fill.price):
            raise ValueError("authenticated managed maker fill differs from ledger")
    baseline_cash = Decimal(str(state["baseline_cash"]))
    before = local_snapshot(processor, baseline_cash)
    report = policy.compare(before, remote)
    if not report.safe_to_trade or report.cash_delta != ZERO:
        raise ValueError("fresh authenticated account snapshot does not reconcile")
    assert_worker_stopped()
    if hashlib.sha256(store.state_path.read_bytes()).digest() != state_digest:
        raise ValueError("persisted live state changed during audited resolution")
    result = processor.resolve_corrected_duplicate_batch_latch(
        remote=remote, reconciler=policy, account_snapshot_fetched_at=fetched_at,
        history_proof={
            "after": history.after, "trade_ids": sorted(row.trade_id for row in history.trades),
            "trade_max_items": history.max_items, "trade_page_limit": history.page_limit,
            "trade_fetched_at": history.fetched_at.isoformat(),
            "flow_after": flows.after, "flow_count": len(flows.flows),
            "flow_max_items": flows.max_items, "flow_page_size": flows.page_size,
            "flow_fetched_at": flows.fetched_at.isoformat(),
        },
    )
    replayed = StreamEventProcessor(ledger)
    if (not result.accepted or replayed.reconciliation_required
            or local_snapshot(replayed, baseline_cash) != before):
        raise RuntimeError("batch resolution did not replay without accounting drift; worker must remain stopped")
    return {"resolved": True, "managed_fills": len(fills), "cash_delta": str(report.cash_delta),
            "cash_flows": len(flows.flows), "lifecycle_clear": True}


async def run_live(settings: V3Settings, runner_settings: LiveRunnerSettings, *, cycles: int = 0) -> None:
    if cycles < 0:
        raise ValueError("live cycles cannot be negative")
    errors = settings.live_client_errors()
    if errors:
        raise RuntimeError("Live runner refused: " + "; ".join(errors))
    store, api, ledger, reconciler, service = await _start(settings, runner_settings)
    shadow = runner_settings.shadow
    runner = LiveTradingRunner(
        service=service, ledger=ledger, reconciler=reconciler, runner_settings=runner_settings,
        api=api, settings=settings, shadow=shadow, store=store,
        weather_client=OffsetWeatherPublicClient(api.public_client),
        forecast=build_weather_forecast(
            store.data_dir,
            max_requests_per_day=shadow.open_meteo_max_requests_per_day,
            cache_seconds=shadow.open_meteo_cache_seconds,
        ),
        observation_provider=NOAAStationObservations(),
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    completed = 0
    while not stop.is_set():
        try:
            status = await runner.run_cycle()
            print(json.dumps({key: status.get(key) for key in (
                "cycle", "equity", "daily_pnl", "entry_block_reason", "v7_candidates",
                "outcomes_this_cycle",
            )}, default=_json_default, sort_keys=True), flush=True)
        except Exception as exc:
            # A failed cycle stops before any further submission; retry next interval.
            failure = {"mode": "LIVE", "healthy": False, "at": _utc_now().isoformat(),
                       "last_error": f"{type(exc).__name__}: {exc}"}
            store.append(store.errors_path, failure)
            previous = store.load_state()
            store.write_status({**failure, "cycle": previous.get("cycles"), "data_dir": store.data_dir})
            print(json.dumps(failure), flush=True)
        completed += 1
        if cycles and completed >= cycles:
            break
        try:
            await asyncio.wait_for(stop.wait(), timeout=shadow.scan_interval_seconds)
        except TimeoutError:
            pass


async def kill_live(settings: V3Settings, runner_settings: LiveRunnerSettings) -> dict[str, Any]:
    """Latch the kill switch for this ledger and cancel every open account order."""
    errors = settings.live_client_errors()
    if errors:
        raise RuntimeError("Live kill refused: " + "; ".join(errors))
    _, _, _, _, service = await _start(settings, runner_settings)
    return await service.kill_switch()
