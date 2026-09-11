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
        clock: Callable[[], float] = time.time,
        live_execution_authorized: bool = False,
        sleep: Callable = asyncio.sleep,
    ) -> None:
        self._sleep = sleep
        self._killed = False
        self._client = secure_client
        self._risk = risk_engine
        self._ledger = ledger
        self._clock = clock
        self._live_execution_authorized = live_execution_authorized

    async def submit(self, intent: OrderIntent, state: AccountRiskState) -> ExecutionResult:
        if self._killed:
            return ExecutionResult(False, "kill switch latched")
        if not self._live_execution_authorized:
            return ExecutionResult(False, "executor live authorization disabled")
        if intent.ttl_seconds < 121:
            return ExecutionResult(False, "effective GTD TTL must be at least 121 seconds")
        decision = self._risk.evaluate(intent, state)
        if not decision.allowed:
            return ExecutionResult(False, decision.reason)

        client_order_id = str(uuid.uuid4())
        order = OrderAggregate.new(
            client_order_id=client_order_id,
            token_id=intent.token_id,
            side=intent.side,
            requested_size=intent.shares,
        )
        # Polymarket applies a 60-second GTD security threshold. Add it to the
        # requested effective lifetime rather than silently shortening the TTL.
        started_at = self._clock()
        expiration = int(started_at) + 60 + intent.ttl_seconds
        # Retry only explicit rejection codes. Transport ambiguity is never resubmitted.
        for attempt in range(3):
            if self._killed:
                return ExecutionResult(False, "kill switch latched")
            if not self._risk.evaluate(intent, state).allowed:
                return ExecutionResult(False, "risk rejected retry")
            now = self._clock()
            if intent.quote_age_seconds + max(0, now-started_at) > self._risk.limits.max_quote_age_seconds:
                return ExecutionResult(False, "quote became stale during retry")
            expiration = int(now) + 60 + intent.ttl_seconds
            response = await self._client.place_limit_order(
                token_id=intent.token_id, price=intent.price, size=intent.shares,
                side=intent.side, post_only=True, expiration=expiration,
            )
            code = str(getattr(response, "code", ""))
            message = str(getattr(response, "message", ""))
            retryable = code == "425" or (code == "503" and "post_only_mode" in message)
            if bool(getattr(response, "ok", False)) or not retryable or attempt == 2:
                break
            self._ledger.append(LedgerEvent.create("order.retry", {"client_order_id":client_order_id,"code":code,"attempt":attempt+1}))
            await self._sleep(2 ** attempt)
        if not bool(getattr(response, "ok", False)):
            code = str(getattr(response, "code", "unknown"))
            message = str(getattr(response, "message", "order rejected"))
            self._ledger.append(LedgerEvent.create(
                "order.rejected",
                {"client_order_id": client_order_id, "code": code, "message": message},
            ))
            return ExecutionResult(False, f"{code}: {message}", order)

        order.accept(order_id=str(response.order_id), status=str(response.status))
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
            },
        ))
        return ExecutionResult(True, str(response.status), order)


    async def kill_switch(self) -> dict:
        """Latch entry stop before requesting cancellation; response is not proof of zero orders."""
        self._killed = True
        if not self._live_execution_authorized:
            return {'status':'not_authorized'}
        response = await self._client.cancel_all()
        self._ledger.append(LedgerEvent.create('order.cancel_all_requested', {'response':str(response)}))
        return {'status':'cancel_requested','response':response,'requires_reconciliation':True}
