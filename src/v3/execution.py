"""Risk-gated V2 order submission primitive.

Nothing constructs this executor with live authorization by default. Accepted
CLOB orders are recorded as orders only; inventory remains zero until separate
confirmed trade events are applied to the order aggregate.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from .config import V3Settings
from .ledger import EventLedger, LedgerEvent
from .orders import OrderAggregate
from .risk import AccountRiskState, OrderIntent, RiskEngine


@dataclass(frozen=True)
class ExecutionResult:
    accepted: bool
    reason: str
    order: OrderAggregate | None = None


class V3OrderExecutor:
    def __init__(
        self,
        secure_client: Any,
        risk_engine: RiskEngine,
        ledger: EventLedger,
        *,
        settings: V3Settings | None = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable = asyncio.sleep,
        api_timeout_seconds: float = 10.0,
    ) -> None:
        if api_timeout_seconds <= 0:
            raise ValueError("API timeout must be positive")
        self._sleep = sleep
        self._killed = any(
            event.event_type == "order.kill_switch_latched"
            for event in ledger.events()
        )
        self._lock = asyncio.Lock()
        self._client = secure_client
        self._risk = risk_engine
        self._ledger = ledger
        self._clock = clock
        self._settings = settings or V3Settings()
        self._live_capable = not self._settings.live_client_errors()
        self._live_service_authorized = False
        self._api_timeout_seconds = api_timeout_seconds

    async def submit(self, intent: OrderIntent, state: AccountRiskState) -> ExecutionResult:
        if not self._live_service_authorized:
            return ExecutionResult(False, "executor may only submit through the gated live service")
        async with self._lock:
            return await self._submit_locked(intent, state)

    async def _submit_locked(self, intent: OrderIntent, state: AccountRiskState) -> ExecutionResult:
        if not self._live_service_authorized:
            return ExecutionResult(False, "executor may only submit through the gated live service")
        if self._killed:
            return ExecutionResult(False, "kill switch latched")
        if not self._live_capable:
            return ExecutionResult(False, "executor live authorization disabled")
        if intent.ttl_seconds < 121:
            return ExecutionResult(False, "effective GTD TTL must be at least 121 seconds")
        decision = self._risk.evaluate(intent, state)
        if not decision.allowed:
            self._ledger.append(LedgerEvent.create("order.risk_rejected", {
                "condition_id": intent.condition_id,
                "token_id": intent.token_id,
                "reason": decision.reason,
            }))
            return ExecutionResult(False, decision.reason)
        # Same comparison as RiskEngine (age == max allows), evaluated before any
        # durable submission record so a stale quote never strands a submission.
        if intent.quote_age_seconds > self._risk.limits.max_quote_age_seconds:
            return ExecutionResult(False, "quote is stale")

        client_order_id = str(uuid.uuid4())
        order = OrderAggregate.new(
            client_order_id=client_order_id,
            token_id=intent.token_id,
            side=intent.side,
            requested_size=intent.shares,
        )
        started_at = self._clock()
        expiration = int(started_at) + 60 + intent.ttl_seconds
        intent_payload = {
            "client_order_id": client_order_id,
            "condition_id": intent.condition_id,
            "token_id": intent.token_id,
            "side": intent.side,
            "price": str(intent.price),
            "requested_size": str(intent.shares),
            "estimated_fee": str(intent.estimated_fee),
            "post_only": True,
            "effective_ttl_seconds": intent.ttl_seconds,
        }
        if intent.decision_id is not None:
            intent_payload.update({
                "decision_id": intent.decision_id,
                "exit_stage": intent.exit_stage,
                "target_return": None if intent.target_return is None else str(intent.target_return),
            })
        self._ledger.append(LedgerEvent.create("order.submission.started", intent_payload))

        response = None
        for attempt in range(3):
            now = self._clock()
            if attempt:
                # Every prior attempt returned a retryable venue rejection, so no
                # order rests at the venue; a local abort is a terminal outcome.
                abort = None
                if self._killed:
                    abort = "kill switch latched"
                elif not self._risk.evaluate(intent, state).allowed:
                    abort = "risk rejected retry"
                elif intent.quote_age_seconds + max(0, now - started_at) > self._risk.limits.max_quote_age_seconds:
                    abort = "quote became stale during retry"
                if abort is not None:
                    self._local_abort(client_order_id, abort, attempts=attempt)
                    return ExecutionResult(False, abort, order)
            expiration = int(now) + 60 + intent.ttl_seconds
            self._ledger.append(LedgerEvent.create("order.submission.attempted", {
                **intent_payload, "attempt": attempt + 1, "expiration": expiration,
            }))
            try:
                response = await asyncio.wait_for(
                    self._client.place_limit_order(
                        token_id=intent.token_id, price=intent.price, size=intent.shares,
                        side=intent.side, post_only=True, expiration=expiration,
                    ),
                    timeout=self._api_timeout_seconds,
                )
            except asyncio.CancelledError:
                self._ledger.append(LedgerEvent.create("order.submission_unknown", {
                    **intent_payload, "attempt": attempt + 1,
                    "failure_type": "CancelledError",
                }))
                raise
            except Exception as exc:
                self._ledger.append(LedgerEvent.create("order.submission_unknown", {
                    **intent_payload, "attempt": attempt + 1,
                    "failure_type": type(exc).__name__,
                }))
                return ExecutionResult(
                    False, "submission outcome unknown; reconcile remote account before retry", order
                )
            code = str(getattr(response, "code", ""))
            message = str(getattr(response, "message", ""))
            retryable = code == "425" or (code == "503" and "post_only_mode" in message)
            if bool(getattr(response, "ok", False)) or not retryable or attempt == 2:
                break
            self._ledger.append(LedgerEvent.create("order.retry", {
                "client_order_id": client_order_id, "code": code, "attempt": attempt + 1,
            }))
            await self._sleep(2 ** attempt)

        if response is None:
            reason = "submission aborted before a venue response"
            self._local_abort(client_order_id, reason, attempts=0)
            return ExecutionResult(False, reason, order)
        if not bool(getattr(response, "ok", False)):
            code = str(getattr(response, "code", "unknown"))
            message = str(getattr(response, "message", "order rejected"))
            self._ledger.append(LedgerEvent.create(
                "order.rejected",
                {"client_order_id": client_order_id, "code": code, "message": message},
            ))
            return ExecutionResult(False, f"{code}: {message}", order)

        try:
            order.accept(order_id=str(response.order_id), status=str(response.status))
        except (AttributeError, ValueError) as exc:
            self._ledger.append(LedgerEvent.create("order.acceptance_unknown", {
                **intent_payload, "failure_type": type(exc).__name__,
            }))
            return ExecutionResult(
                False, "order accepted with unsupported response; reconcile remote account", order
            )
        self._ledger.append(LedgerEvent.create(
            "order.accepted",
            {
                "client_order_id": client_order_id,
                "order_id": str(response.order_id),
                "status": str(response.status),
                "condition_id": intent.condition_id,
                "token_id": intent.token_id,
                "side": intent.side,
                "price": str(intent.price),
                "requested_size": str(intent.shares),
                "post_only": True,
                "expiration": expiration,
                **({"decision_id": intent.decision_id, "exit_stage": intent.exit_stage,
                    "target_return": None if intent.target_return is None else str(intent.target_return)}
                   if intent.decision_id is not None else {}),
            },
        ))
        return ExecutionResult(True, str(response.status), order)

    def _local_abort(self, client_order_id: str, reason: str, *, attempts: int) -> None:
        """Terminally close a started submission that provably reached no resting venue order."""
        self._ledger.append(LedgerEvent.create("order.rejected", {
            "client_order_id": client_order_id, "code": "local_abort", "message": reason,
            "venue_attempts": attempts,
        }))

    async def kill_switch(self) -> dict:
        """Latch entry stop before requesting cancellation; response is not proof of zero orders."""
        if not self._live_service_authorized:
            return {"status": "not_authorized"}
        async with self._lock:
            self._killed = True
            self._ledger.append(LedgerEvent.create("order.kill_switch_latched", {
                "reason": "operator kill switch requested",
            }))
            if not self._live_capable:
                return {"status": "not_authorized"}
            try:
                response = await asyncio.wait_for(
                    self._client.cancel_all(), timeout=self._api_timeout_seconds
                )
            except asyncio.CancelledError:
                self._ledger.append(LedgerEvent.create("order.cancel_all_unknown", {
                    "failure_type": "CancelledError",
                }))
                raise
            except Exception as exc:
                self._ledger.append(LedgerEvent.create("order.cancel_all_unknown", {
                    "failure_type": type(exc).__name__,
                }))
                return {"status": "cancel_unknown", "requires_reconciliation": True}
            self._ledger.append(LedgerEvent.create(
                "order.cancel_all_requested", {"response": str(response)}
            ))
            return {"status": "cancel_requested", "response": response, "requires_reconciliation": True}
