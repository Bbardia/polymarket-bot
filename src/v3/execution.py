"""Risk-gated V2 order submission primitive.

Nothing constructs this executor with live authorization by default. Accepted
CLOB orders are recorded as orders only; inventory remains zero until separate
confirmed trade events are applied to the order aggregate.
"""

from __future__ import annotations

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
    ) -> None:
        self._client = secure_client
        self._risk = risk_engine
        self._ledger = ledger
        self._clock = clock
        self._live_execution_authorized = live_execution_authorized

    async def submit(self, intent: OrderIntent, state: AccountRiskState) -> ExecutionResult:
        if not self._live_execution_authorized:
            return ExecutionResult(False, "executor live authorization disabled")
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
        expiration = int(self._clock()) + 60 + intent.ttl_seconds
        response = await self._client.place_limit_order(
            token_id=intent.token_id,
            price=intent.price,
            size=intent.shares,
            side=intent.side,
            post_only=True,
            expiration=expiration,
        )
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
