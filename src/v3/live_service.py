"""Fail-closed live order orchestration; it is not wired to the V7 paper strategy."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from math import ceil, isfinite
from typing import Any

from .config import V3Settings
from .execution import ExecutionResult, V3OrderExecutor
from .ledger import EventLedger, LedgerEvent
from .live_early_exit import book_quote_age_seconds
from .reconciliation import LocalSnapshot, Reconciler, RemoteSnapshot
from .risk import AccountRiskState, OrderIntent, RiskEngine
from .streaming import StreamEventProcessor
from .sdk_execution_adapter import SDKExecutionAdapter

ZERO = Decimal("0")
TRADE_HISTORY_MAX_ITEMS = 10_000
TRADE_HISTORY_PAGE_LIMIT = 100
TRADE_HISTORY_TIMEOUT_SECONDS = 30.0


@dataclass
class LiveRiskContext:
    """Shared within a cycle and refreshed from each authenticated account snapshot."""

    daily_pnl: Decimal
    peak_equity: Decimal
    day_start_equity: Decimal | None = None

    def __post_init__(self) -> None:
        if not self.daily_pnl.is_finite() or not self.peak_equity.is_finite() or self.peak_equity <= ZERO:
            raise ValueError("risk context P&L and peak equity must be finite; peak must be positive")
        if self.day_start_equity is not None and (
            not self.day_start_equity.is_finite() or self.day_start_equity <= ZERO
        ):
            raise ValueError("day-start equity must be finite and positive")

    def refresh_from_remote(self, remote: RemoteSnapshot) -> None:
        equity = remote.cash + sum((position.current_value for position in remote.positions), ZERO)
        if not equity.is_finite() or equity <= ZERO:
            raise ValueError("remote equity is invalid for risk evaluation")
        self.peak_equity = max(self.peak_equity, equity)
        if self.day_start_equity is not None:
            self.daily_pnl = equity - self.day_start_equity


class LiveOrderService:
    """Require gated credentials, matching risk limits, and fresh account reconciliation."""

    def __init__(
        self,
        api: Any,
        executor: Any,
        risk_engine: RiskEngine,
        ledger: EventLedger,
        reconciler: Reconciler,
    ) -> None:
        if isinstance(executor, V3OrderExecutor) and (
            executor._ledger is not ledger or executor._risk is not risk_engine
        ):
            raise ValueError("executor must share the service ledger and risk engine")
        self._api = api
        self._executor = executor
        self._risk = risk_engine
        self._ledger = ledger
        self._reconciler = reconciler
        self._submit_lock = asyncio.Lock()
        # Only the asynchronous factory can unlock real service operations.
        self._factory_authorized = False
        self._baseline_cash: Decimal | None = None
        # Account baseline (Unix seconds); trades before it predate the bot.
        self._trade_history_after: int | None = None

    @classmethod
    async def create(
        cls,
        *,
        api: Any,
        settings: V3Settings,
        risk_engine: RiskEngine,
        ledger: EventLedger,
        reconciler: Reconciler | None = None,
        trade_history_after: int | None = None,
        baseline_cash: Decimal | None = None,
    ) -> "LiveOrderService":
        if baseline_cash is not None and (
            not isinstance(baseline_cash, Decimal) or not baseline_cash.is_finite() or baseline_cash < ZERO
        ):
            raise ValueError("service baseline cash must be a finite nonnegative Decimal")
        errors = settings.live_client_errors()
        if errors:
            raise RuntimeError("Live order service refused: " + "; ".join(errors))
        if getattr(api, "settings", None) != settings:
            raise ValueError("API settings do not match live service settings")

        limits = risk_engine.limits
        expected = (
            ("max_capital", limits.max_capital, settings.max_capital),
            ("max_order_notional", limits.max_order_notional, settings.max_order_notional),
            ("reserve_fraction", limits.reserve_fraction, settings.reserve_fraction),
            ("daily_loss_limit", limits.daily_loss_limit, settings.max_daily_loss),
            ("max_drawdown_amount", limits.max_drawdown_amount, settings.max_drawdown_amount),
            ("max_drawdown_fraction", limits.max_drawdown_fraction, settings.max_drawdown_fraction),
        )
        mismatches = [name for name, actual, configured in expected if actual != configured]
        if mismatches:
            raise ValueError("risk engine limits do not match live settings: " + ", ".join(mismatches))

        resolver = reconciler or Reconciler(
            cash_tolerance=ZERO, cost_tolerance=Decimal("0.01"), allow_cash_inflows=False,
        )
        if (type(resolver) is not Reconciler or resolver.cash_tolerance != ZERO
                or not isinstance(resolver.cost_tolerance, Decimal)
                or not resolver.cost_tolerance.is_finite() or resolver.cost_tolerance < ZERO
                or resolver.cost_tolerance > Decimal("0.01")
                or resolver.allow_cash_inflows is not False):
            raise ValueError("live service requires strict cash reconciliation and cost tolerance at most 0.01")
        expected_external_ids = frozenset()
        persisted_state: dict[str, Any] | None = None
        if not ledger._in_memory:
            try:
                persisted = json.loads((ledger.path.parent / "live_state.json").read_text(encoding="utf-8"))
                if not isinstance(persisted, dict):
                    raise ValueError("persisted live state is invalid")
                persisted_state = persisted
                raw_external_ids = persisted.get("external_condition_ids")
                if (not isinstance(raw_external_ids, list)
                        or any(not isinstance(item, str) or not item for item in raw_external_ids)
                        or len(set(raw_external_ids)) != len(raw_external_ids)):
                    raise ValueError("persisted external condition IDs are invalid")
                expected_external_ids = frozenset(raw_external_ids)
            except (OSError, TypeError, ValueError) as exc:
                raise ValueError("live service requires a valid persisted account baseline") from exc
        if resolver.external_condition_ids != expected_external_ids:
            raise ValueError("reconciler external positions must match the persisted account baseline")
        if persisted_state is not None:
            try:
                stored_baseline = Decimal(str(persisted_state["baseline_cash"]))
                stored_epoch = persisted_state["baseline_epoch"]
            except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
                raise ValueError("persisted cash/time baseline is invalid") from exc
            if (not stored_baseline.is_finite() or stored_baseline < ZERO
                    or type(stored_epoch) is not int or stored_epoch < 0
                    or type(trade_history_after) is not int or trade_history_after != stored_epoch
                    or baseline_cash != stored_baseline):
                raise ValueError("service cash/history baselines must match persisted live state")

        client = await api.initialize_secure_client()
        executor = V3OrderExecutor(SDKExecutionAdapter(client), risk_engine, ledger, settings=settings)
        service = cls(api, executor, risk_engine, ledger, resolver)
        service._trade_history_after = trade_history_after
        service._baseline_cash = baseline_cash
        await service.recover_trade_history(
            max_items=TRADE_HISTORY_MAX_ITEMS,
            page_limit=TRADE_HISTORY_PAGE_LIMIT,
        )
        service._executor._live_service_authorized = True
        service._factory_authorized = True
        return service

    def _unresolved_submissions(self) -> tuple[str, ...]:
        started: set[str] = set()
        terminal: set[str] = set()
        unknown: set[str] = set()
        for event in self._ledger.events():
            client_order_id = str(event.payload.get("client_order_id", ""))
            if not client_order_id:
                continue
            if event.event_type == "order.submission.started":
                started.add(client_order_id)
            elif event.event_type in {"order.accepted", "order.rejected", "order.reconciled"}:
                terminal.add(client_order_id)
                if event.event_type == "order.reconciled":
                    unknown.discard(client_order_id)
            elif event.event_type in {"order.submission_unknown", "order.acceptance_unknown"}:
                unknown.add(client_order_id)
        return tuple(sorted((started - terminal) | unknown))

    def _risk_state(
        self,
        remote: RemoteSnapshot,
        *,
        daily_pnl: Decimal,
        peak_equity: Decimal,
    ) -> AccountRiskState:
        if not daily_pnl.is_finite() or not peak_equity.is_finite() or peak_equity <= ZERO:
            raise ValueError("persisted daily P&L or peak equity is invalid")

        position_value = ZERO
        position_exposure = ZERO
        open_positions = 0
        event_exposure: dict[str, Decimal] = {}
        external = getattr(self._reconciler, "external_condition_ids", frozenset()) if self else frozenset()
        for position in remote.positions:
            initial_value = position.initial_value
            if (
                not position.condition_id or not position.token_id
                or not position.size.is_finite() or position.size <= ZERO
                or not position.current_value.is_finite() or position.current_value < ZERO
                or initial_value is None or not initial_value.is_finite() or initial_value < ZERO
            ):
                raise ValueError("remote position is incomplete or invalid")
            position_value += position.current_value
            if position.condition_id in external:
                # Pre-bot/manual holdings count toward equity, not bot exposure.
                continue
            if position.redeemable:
                # Resolved: remaining risk is only the unredeemed payout value.
                exposure_basis = position.current_value
            else:
                exposure_basis = max(initial_value, position.current_value)
                open_positions += 1
            position_exposure += exposure_basis
            event_exposure[position.condition_id] = (
                event_exposure.get(position.condition_id, ZERO) + exposure_basis
            )

        pending_order_notional = ZERO
        for order in remote.open_orders:
            amount = order.remaining_notional
            if (
                not order.order_id or not order.condition_id or not order.token_id
                or amount is None or not amount.is_finite() or amount < ZERO
            ):
                raise ValueError("remote open order is incomplete or invalid")
            pending_order_notional += amount
            event_exposure[order.condition_id] = (
                event_exposure.get(order.condition_id, ZERO) + amount
            )

        equity = remote.cash + position_value
        if not remote.cash.is_finite() or remote.cash < ZERO or not equity.is_finite():
            raise ValueError("remote cash or equity is invalid")
        return AccountRiskState(
            equity=equity,
            cash=remote.cash - pending_order_notional,
            total_exposure=position_exposure + pending_order_notional,
            event_exposure=event_exposure,
            open_orders=len(remote.open_orders),
            open_positions=open_positions,
            daily_pnl=daily_pnl,
            peak_equity=peak_equity,
            reconciled=True,
            unknown_remote_positions=0,
            unknown_remote_orders=0,
        )

    async def _validated_intent(self, intent: OrderIntent) -> tuple[OrderIntent | None, str | None]:
        try:
            context = await self._api.get_verified_market_context(
                intent.condition_id, intent.token_id,
            )
        except Exception as exc:
            self._ledger.append(LedgerEvent.create("account.market_preflight.failed", {
                "failure_type": type(exc).__name__,
            }))
            return None, "market preflight failed; no order submitted"

        fee_rate = getattr(context, "fee_rate", None)
        if not isinstance(fee_rate, Decimal) or not fee_rate.is_finite() or fee_rate < ZERO:
            reason = "market fee schedule is missing or invalid"
        elif fee_rate > ZERO and not (
            intent.post_only is True
            and getattr(context, "taker_only", None) is True
            and intent.estimated_fee == ZERO
        ):
            reason = "fee-bearing market requires a verified taker-only post-only order with zero estimated maker fee"
        elif (
            context.condition_id != intent.condition_id
            or context.token_id != intent.token_id
            or not context.condition_matches
            or not context.token_matches
        ):
            reason = "market and token identity do not match intent"
        elif context.disputed:
            reason = "market is disputed"
        elif not context.rules_verified:
            reason = "market rules are unverified"
        elif not context.accepting_orders:
            reason = "market is not accepting orders"
        else:
            try:
                timestamp = context.book_timestamp
                if timestamp is None or timestamp.tzinfo is None:
                    raise ValueError("book timestamp missing or timezone-naive")
                # Exits use the shared read-time freshness rule; BUY entries keep
                # the stricter last-change age (no read time is passed).
                fetched_at = None
                if intent.side == "SELL":
                    fetched_at = getattr(context, "fetched_at", None)
                    if not isinstance(fetched_at, datetime) or fetched_at.tzinfo is None:
                        raise ValueError("book read time missing or timezone-naive")
                try:
                    age: float | None = book_quote_age_seconds(
                        book_timestamp=timestamp, fetched_at=fetched_at,
                        now=datetime.now(timezone.utc),
                        max_quote_age_seconds=self._risk.limits.max_quote_age_seconds,
                    )
                except ValueError:
                    age = None
                if age is None:
                    reason = "order-book quote is stale"
                elif not context.book_hash:
                    reason = "order-book hash is missing"
                else:
                    return replace(
                        intent,
                        tick_size=context.tick_size,
                        min_order_size=context.min_order_size,
                        market_accepting_orders=context.accepting_orders,
                        rules_verified=context.rules_verified,
                        disputed=context.disputed,
                        quote_age_seconds=max(intent.quote_age_seconds, ceil(age)),
                    ), None
            except (AttributeError, TypeError, ValueError):
                reason = "market context is incomplete"

        self._ledger.append(LedgerEvent.create("account.market_preflight.blocked", {
            "reason": reason,
            "condition_id": intent.condition_id,
            "token_id": intent.token_id,
        }))
        return None, reason

    async def submit(
        self,
        intent: OrderIntent,
        local_snapshot: LocalSnapshot,
        context: LiveRiskContext,
    ) -> ExecutionResult:
        if not self._factory_authorized:
            return ExecutionResult(False, "live service must be created by the gated factory; no order submitted")
        async with self._submit_lock:
            return await self._submit_locked(intent, local_snapshot, context)

    async def _submit_locked(
        self,
        intent: OrderIntent,
        local_snapshot: LocalSnapshot,
        context: LiveRiskContext,
    ) -> ExecutionResult:
        processor = StreamEventProcessor(self._ledger)
        if processor.reconciliation_required:
            self._ledger.append(LedgerEvent.create("account.preflight.blocked", {
                "reason": "lifecycle reconciliation required",
            }))
            return ExecutionResult(False, "lifecycle reconciliation required; no order submitted")
        unresolved = self._unresolved_submissions()
        if unresolved:
            self._ledger.append(LedgerEvent.create("account.preflight.blocked", {
                "reason": "unresolved prior submission",
                "unresolved_submission_count": len(unresolved),
            }))
            return ExecutionResult(False, "unresolved prior submission requires manual reconciliation")

        validated_intent, market_block = await self._validated_intent(intent)
        if market_block or validated_intent is None:
            return ExecutionResult(False, market_block or "market context unavailable")
        intent = validated_intent

        try:
            remote = await self._api.fetch_remote_snapshot()
        except Exception as exc:
            self._ledger.append(LedgerEvent.create("account.preflight.failed", {
                "failure_type": type(exc).__name__,
            }))
            return ExecutionResult(False, "account preflight failed; no order submitted")

        if intent.side == "SELL":
            if self._baseline_cash is None:
                self._ledger.append(LedgerEvent.create("account.sell_preflight.blocked", {
                    "reason": "trusted account baseline cash unavailable",
                }))
                return ExecutionResult(False, "SELL requires service-bound baseline cash")
            try:
                from .live_runner import local_snapshot as build_local_snapshot
                processor = StreamEventProcessor(self._ledger)
                trusted_local = build_local_snapshot(processor, self._baseline_cash)
            except Exception as exc:
                self._ledger.append(LedgerEvent.create("account.sell_preflight.failed", {
                    "failure_type": type(exc).__name__,
                }))
                return ExecutionResult(False, "SELL inventory replay failed; no order submitted")
            quantities = trusted_local.position_quantities
            costs = trusted_local.position_cost_basis
            quantity = quantities.get(intent.token_id) if quantities is not None else None
            cost = costs.get(intent.token_id) if costs is not None else None
            matches = [p for p in remote.positions if p.token_id == intent.token_id]
            managed_conditions = {
                event.payload.get("condition_id") for event in self._ledger.events()
                if event.event_type == "order.accepted"
                and event.payload.get("side") == "BUY"
                and event.payload.get("token_id") == intent.token_id
            }
            active_ids = processor.active_order_ids
            active_order_for_token = any(
                order.token_id == intent.token_id and order.order_id in active_ids
                for order in processor.orders.values()
            )
            if (
                not isinstance(quantity, Decimal) or not quantity.is_finite() or quantity <= ZERO
                or not isinstance(cost, Decimal) or not cost.is_finite() or cost <= ZERO
                or not isinstance(intent.shares, Decimal) or not intent.shares.is_finite()
                or intent.shares <= ZERO or intent.shares > quantity
                or len(matches) != 1 or matches[0].condition_id != intent.condition_id
                or matches[0].size != quantity
                or managed_conditions != {intent.condition_id}
                or intent.condition_id in self._reconciler.external_condition_ids
                or active_order_for_token
            ):
                self._ledger.append(LedgerEvent.create("account.sell_preflight.blocked", {
                    "reason": "SELL is not uniquely attributable to reconciled free bot inventory",
                    "condition_id": intent.condition_id,
                    "token_id": intent.token_id,
                }))
                return ExecutionResult(False, "SELL inventory, ownership, or open-order scope is not exact")
            local_snapshot = trusted_local

        local_snapshot = replace(
            local_snapshot, order_ids=processor.active_order_ids,
        )
        report = self._reconciler.compare(local_snapshot, remote)
        if not report.safe_to_trade:
            self._ledger.append(LedgerEvent.create("account.preflight.blocked", {
                "cash_delta": str(report.cash_delta),
                "unknown_position_count": len(report.unknown_positions),
                "unknown_order_count": len(report.unknown_orders),
                "missing_position_count": len(report.missing_positions),
                "missing_order_count": len(report.missing_orders),
                "incomplete_order_count": len(report.incomplete_orders),
            }))
            return ExecutionResult(False, "account reconciliation blocked order submission")

        try:
            context.refresh_from_remote(remote)
            state = self._risk_state(
                remote, daily_pnl=context.daily_pnl, peak_equity=context.peak_equity,
            )
            if intent.side == "SELL":
                state = replace(
                    state,
                    position_quantities=dict(local_snapshot.position_quantities or {}),
                )
        except (ArithmeticError, AttributeError, TypeError, ValueError) as exc:
            self._ledger.append(LedgerEvent.create("account.preflight.failed", {
                "failure_type": type(exc).__name__,
            }))
            return ExecutionResult(False, "account risk snapshot invalid; no order submitted")
        return await self._executor.submit(intent, state)

    async def recover_trade_history(
        self, *, max_items: int, page_limit: int,
        timeout_seconds: float = TRADE_HISTORY_TIMEOUT_SECONDS,
    ) -> dict[str, Any]:
        """Replay a bounded account trade read; this never clears a reconciliation latch.

        Only trades at or after the factory's account baseline are replayed;
        any later trade not attributable to a managed order still latches.
        """
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or not isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be a finite positive number")
        processor = StreamEventProcessor(self._ledger)
        try:
            fetch = (
                self._api.fetch_account_trades(max_items=max_items, page_limit=page_limit)
                if self._trade_history_after is None
                else self._api.fetch_account_trades(
                    max_items=max_items, page_limit=page_limit,
                    after=self._trade_history_after,
                )
            )
            trades = await asyncio.wait_for(fetch, timeout=timeout_seconds)
        except asyncio.CancelledError:
            processor.require_reconciliation(
                "trade history read cancelled: CancelledError"
            )
            raise
        except Exception as exc:
            processor.require_reconciliation(
                f"trade history read failed: {type(exc).__name__}"
            )
            return {"imported_count": 0, "lifecycle_clear": False}
        imported = 0
        for trade in trades:
            result = processor.import_remote_trade(trade)
            if result.accepted and not result.duplicate and not result.requires_reconciliation:
                imported += 1
        # This reports only processor-latch state, not whole-account trade readiness.
        processor = StreamEventProcessor(self._ledger)
        return {"imported_count": imported, "lifecycle_clear": not processor.reconciliation_required}

    async def kill_switch(self) -> dict[str, Any]:
        if not self._factory_authorized:
            return {"status": "service_not_authorized", "requires_reconciliation": True}
        result = await self._executor.kill_switch()
        if result.get("status") != "cancel_requested":
            return result
        response = result.get("response")
        if isinstance(response, dict):
            not_canceled = response.get("not_canceled", {})
        else:
            not_canceled = getattr(response, "not_canceled", {})
        not_canceled_ids = tuple(sorted(str(order_id) for order_id in (not_canceled or {})))
        try:
            remote = await self._api.fetch_remote_snapshot()
        except Exception as exc:
            self._ledger.append(LedgerEvent.create("order.cancel_verification_failed", {
                "failure_type": type(exc).__name__,
            }))
            return {"status": "cancel_unverified", "requires_reconciliation": True}
        if not_canceled_ids:
            self._ledger.append(LedgerEvent.create("order.cancel_verification_failed", {
                "not_canceled_count": len(not_canceled_ids),
            }))
            return {
                "status": "cancel_unverified",
                "not_canceled_ids": not_canceled_ids,
                "open_orders_remaining": len(remote.open_orders),
                "requires_reconciliation": True,
            }
        if remote.open_orders:
            self._ledger.append(LedgerEvent.create("order.cancel_verification_failed", {
                "open_orders_remaining": len(remote.open_orders),
            }))
            return {
                "status": "cancel_unverified",
                "open_orders_remaining": len(remote.open_orders),
                "requires_reconciliation": True,
            }
        self._ledger.append(LedgerEvent.create("order.cancel_snapshot_empty", {
            "open_orders_remaining": 0,
        }))
        return {"status": "cancel_snapshot_empty", "requires_reconciliation": True}
