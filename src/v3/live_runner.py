"""Gated real-money runner for the V7 weather strategy.

Entries are post-only GTD BUY orders built by the same path as ``live-shadow``
(V7 evaluation and selection, resolver-station gate, fresh verified context,
unchanged V7 proposal bridge) and submitted only through the factory-built
``LiveOrderService``, which re-checks market rules, account reconciliation and
the hard ``RiskEngine`` limits before every order. Positions are held to
resolution; this runner never sells, merges or redeems.

Account baseline: on first start the runner records the account's existing
positions as external and the start time as the trade-history baseline. Any
later trade, position or open order the bot cannot attribute to itself blocks
new entries.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from .api import UnifiedPolymarketAPI
from .config import V3Settings
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
from .risk import RiskEngine
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

    def __post_init__(self) -> None:
        if not self.cost_tolerance.is_finite() or self.cost_tolerance < ZERO:
            raise ValueError("cost tolerance must be finite and nonnegative")
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

    Bot orders are post-only makers on fee-free or taker-only schedules (the V7
    bridge refuses anything else), so they pay no fee. Venue trade rows still
    carry the market's taker rate, which the ledger books as a fee; it is
    excluded here. A fee actually charged would appear as an unexplained cash
    decrease, which reconciliation blocks.
    """
    quantities: dict[str, Decimal] = {}
    costs: dict[str, Decimal] = {}
    cash = baseline_cash
    for order in processor.orders.values():
        if order.confirmed_size <= ZERO:
            continue
        if order.side != "BUY":
            raise ValueError("live runner only manages BUY orders")
        quantities[order.token_id] = quantities.get(order.token_id, ZERO) + order.confirmed_size
        costs[order.token_id] = costs.get(order.token_id, ZERO) + order.confirmed_notional
        cash -= order.confirmed_notional
    return LocalSnapshot(
        cash=cash,
        position_tokens=frozenset(quantities),
        order_ids=processor.active_order_ids,
        position_quantities=quantities,
        position_cost_basis=costs,
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

    def _event_block_reason(
        self, evaluation: Any, remote: RemoteSnapshot, local: LocalSnapshot,
    ) -> str | None:
        if evaluation.condition_id in self.reconciler.external_condition_ids:
            return "condition is held outside the bot (external)"
        orders = self.state.setdefault("event_orders", {}).get(evaluation.event_key, [])
        open_ids = {order.order_id for order in remote.open_orders}
        if any(order.get("order_id") in open_ids for order in orders):
            return "bot order for this event is still resting"
        if any(order.get("token_id") in local.position_tokens for order in orders):
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
        if processor.reconciliation_required:
            entry_block = "lifecycle reconciliation latched: " + "; ".join(processor.reconciliation_reasons)

        equity = remote.cash + sum((p.current_value for p in remote.positions), ZERO)
        if self.state.get("day") != today:
            self.state["day"] = today
            self.state["day_start_equity"] = str(equity)
        peak = max(Decimal(self.state["peak_equity"]), equity)
        self.state["peak_equity"] = str(peak)
        daily_pnl = equity - Decimal(self.state["day_start_equity"])

        local = local_snapshot(processor, Decimal(self.state["baseline_cash"]))
        report = self.reconciler.compare(local, remote)
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
                "safe_to_trade": report.safe_to_trade,
                "cash_delta": report.cash_delta,
                "unknown_positions": len(report.unknown_positions),
                "unknown_orders": len(report.unknown_orders),
                "missing_positions": len(report.missing_positions),
                "missing_orders": len(report.missing_orders),
                "position_mismatches": len(report.position_mismatches),
                "external_positions": len(report.external_positions),
            },
        })

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
            if entry_block is not None:
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

        if outcomes.get("accepted", 0):
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
        allow_cash_inflows=True,
    )
    # The gated factory builds the signing client and replays post-baseline fills.
    service = await LiveOrderService.create(
        api=api, settings=settings, risk_engine=risk, ledger=ledger,
        reconciler=reconciler, trade_history_after=int(state["baseline_epoch"]),
    )
    return store, api, ledger, reconciler, service


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
