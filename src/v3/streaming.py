"""Read-only stream normalization, replay, and reconnect supervision."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol, cast

from .ledger import EventLedger, LedgerEvent
from .math import taker_fee
from .orders import OrderAggregate, OrderState, TradeStatus


class StreamState(str, Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    SUBSCRIBED = "SUBSCRIBED"
    BACKOFF = "BACKOFF"
    STOPPED = "STOPPED"


@dataclass(frozen=True)
class NormalizedStreamEvent:
    event_id: str
    event_type: str
    occurred_at: str | None
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class ProcessResult:
    accepted: bool
    duplicate: bool = False
    requires_reconciliation: bool = False
    reason: str = ""
    event_id: str = ""


@dataclass(frozen=True)
class ReconnectPolicy:
    base_delay_seconds: float = 2.0
    max_delay_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.base_delay_seconds <= 0:
            raise ValueError("base reconnect delay must be positive")
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("maximum reconnect delay must not be below the base delay")

    def delay(self, attempt: int) -> float:
        if attempt < 1:
            raise ValueError("reconnect attempt must be positive")
        delay = self.base_delay_seconds
        for _ in range(1, attempt):
            if delay >= self.max_delay_seconds / 2.0:
                return self.max_delay_seconds
            delay *= 2.0
        return min(self.max_delay_seconds, delay)


def _json_safe(value: Any) -> Any:
    """Convert SDK/test payloads to deterministic, lossless JSON values."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return _json_safe(value.value)
    if hasattr(value, "model_dump"):
        return _json_safe(value.model_dump(mode="json", by_alias=True, exclude_none=True))
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "__dict__"):
        return {
            key: _json_safe(item)
            for key, item in vars(value).items()
            if not key.startswith("_")
        }
    raise TypeError(f"unsupported stream payload value: {type(value)!r}")


def _stable_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def normalize_stream_event(event: Any) -> NormalizedStreamEvent:
    topic = str(getattr(event, "topic", "unknown"))
    event_kind = str(getattr(event, "type", "unknown"))
    payload_value = _json_safe(getattr(event, "payload", event))
    if not isinstance(payload_value, Mapping):
        raise TypeError("stream payload must normalize to an object")
    payload = dict(payload_value)
    occurred_at = (
        payload.get("timestamp")
        or payload.get("last_update")
        or payload.get("updated_at")
    )

    # A transport replay can assign fresh timestamps to the same exchange state.
    # Status and all economic fields remain in the fingerprint, so a MATCHED to
    # CONFIRMED transition is still a distinct event.
    identity_payload = dict(payload)
    if topic == "user":
        for key in (
            "timestamp",
            "match_time",
            "matchtime",
            "matched_at",
            "last_update",
            "updated_at",
        ):
            identity_payload.pop(key, None)
    fingerprint = _stable_json({
        "topic": topic,
        "type": event_kind,
        "payload": identity_payload,
    })
    digest = hashlib.sha256(fingerprint.encode()).hexdigest()
    return NormalizedStreamEvent(
        event_id=f"stream:{digest}",
        event_type=f"{topic}.{event_kind}",
        occurred_at=str(occurred_at) if occurred_at is not None else None,
        payload=payload,
    )


def _value(payload: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
    return default


class StreamEventProcessor:
    """Persist events while adopting only explicitly managed order IDs."""

    def __init__(
        self,
        ledger: EventLedger,
        *,
        managed_order_ids: set[str] | frozenset[str] = frozenset(),
    ) -> None:
        self.ledger = ledger
        self.managed_order_ids = frozenset(managed_order_ids)
        self.orders: dict[str, OrderAggregate] = {}
        self.reconciliation_required = False
        self.reconciliation_reasons: list[str] = []
        self._applied_event_ids: set[str] = set()
        self._replay_ledger()

    def _replay_ledger(self) -> None:
        for event in self.ledger.events():
            normalized = NormalizedStreamEvent(
                event_id=event.event_id,
                event_type=event.event_type,
                occurred_at=event.occurred_at or None,
                payload=event.payload,
            )
            result = self._apply(normalized)
            self._record_result(result)
            self._applied_event_ids.add(event.event_id)

    def _record_result(self, result: ProcessResult) -> ProcessResult:
        if result.requires_reconciliation:
            self.reconciliation_required = True
            if result.reason and result.reason not in self.reconciliation_reasons:
                self.reconciliation_reasons.append(result.reason)
        return result

    def require_reconciliation(self, reason: str) -> None:
        """Set a sticky fail-closed blocker after a stream gap or ambiguity."""
        self._record_result(ProcessResult(
            accepted=False,
            requires_reconciliation=True,
            reason=reason,
        ))

    def process(self, event: Any) -> ProcessResult:
        normalized = normalize_stream_event(event)
        ledger_event = LedgerEvent(
            event_id=normalized.event_id,
            event_type=normalized.event_type,
            occurred_at=normalized.occurred_at or "",
            payload=normalized.payload,
        )
        inserted = self.ledger.append(ledger_event)
        if not inserted and normalized.event_id in self._applied_event_ids:
            return ProcessResult(
                accepted=False,
                duplicate=True,
                reason="duplicate stream event",
                event_id=normalized.event_id,
            )

        result = self._apply(normalized)
        self._applied_event_ids.add(normalized.event_id)
        return self._record_result(result)

    def _apply(self, event: NormalizedStreamEvent) -> ProcessResult:
        if event.event_type == "user.order":
            return self._apply_order(event)
        if event.event_type == "user.trade":
            return self._apply_trade(event)
        return ProcessResult(
            True,
            reason="recorded read-only market event",
            event_id=event.event_id,
        )

    def _apply_order(self, event: NormalizedStreamEvent) -> ProcessResult:
        payload = event.payload
        order_id = str(_value(payload, "id", default=""))
        subtype = str(_value(payload, "order_event_type", "type", default="")).upper()
        raw_status = _value(payload, "status")
        status = str(raw_status or "LIVE").lower()
        if not order_id:
            return ProcessResult(
                True,
                requires_reconciliation=True,
                reason="order event has no exchange order id",
                event_id=event.event_id,
            )

        if order_id not in self.managed_order_ids:
            return ProcessResult(
                True,
                requires_reconciliation=True,
                reason="order event references unmanaged order",
                event_id=event.event_id,
            )

        if subtype == "CANCELLATION" or status == "canceled":
            order = self.orders.get(order_id)
            if order is None:
                return ProcessResult(
                    True,
                    requires_reconciliation=True,
                    reason="cancellation references unknown order",
                    event_id=event.event_id,
                )
            try:
                order.cancel()
            except ValueError:
                return ProcessResult(
                    True,
                    requires_reconciliation=True,
                    reason="invalid cancellation transition requires reconciliation",
                    event_id=event.event_id,
                )
            return ProcessResult(
                True,
                reason="order cancellation recorded",
                event_id=event.event_id,
            )

        if subtype not in {"PLACEMENT", "UPDATE"}:
            return ProcessResult(
                True,
                requires_reconciliation=True,
                reason="unsupported order event requires reconciliation",
                event_id=event.event_id,
            )

        existing_order = self.orders.get(order_id)
        if existing_order is None:
            try:
                order = OrderAggregate.new(
                    client_order_id=f"stream:{order_id}",
                    token_id=str(_value(payload, "asset_id", "token_id")),
                    side=str(_value(payload, "side")),
                    requested_size=Decimal(str(_value(payload, "original_size"))),
                )
                order.accept(order_id=order_id, status=status)
                self.orders[order_id] = order
            except (ArithmeticError, TypeError, ValueError):
                return ProcessResult(
                    True,
                    requires_reconciliation=True,
                    reason="malformed order event requires reconciliation",
                    event_id=event.event_id,
                )
        else:
            try:
                token_id = str(_value(payload, "asset_id", "token_id"))
                side = str(_value(payload, "side"))
                requested_size = Decimal(str(_value(payload, "original_size")))
                if (
                    token_id != existing_order.token_id
                    or side != existing_order.side
                    or requested_size != existing_order.requested_size
                ):
                    raise ValueError("order economics changed")
                if raw_status is not None and existing_order.state in {
                    OrderState.CREATED,
                    OrderState.LIVE,
                    OrderState.DELAYED,
                    OrderState.MATCHED,
                }:
                    existing_order.accept(order_id=order_id, status=status)
            except (ArithmeticError, TypeError, ValueError):
                return ProcessResult(
                    True,
                    requires_reconciliation=True,
                    reason="changed order event requires reconciliation",
                    event_id=event.event_id,
                )
        return ProcessResult(True, reason="order event applied", event_id=event.event_id)

    def _apply_trade(self, event: NormalizedStreamEvent) -> ProcessResult:
        payload = event.payload
        try:
            status = TradeStatus(str(_value(payload, "status")))
            targets: list[tuple[OrderAggregate, Mapping[str, Any], str]] = []

            taker_order_id = str(_value(payload, "taker_order_id", default=""))
            taker_order = self.orders.get(taker_order_id)
            if taker_order is not None:
                targets.append((taker_order, payload, "size"))

            maker_orders = _value(payload, "maker_orders", default=[]) or []
            if not isinstance(maker_orders, (list, tuple)):
                raise TypeError("maker_orders must be a sequence")
            for maker_payload in maker_orders:
                if not isinstance(maker_payload, Mapping):
                    raise TypeError("maker order payload must be an object")
                maker_order_id = str(
                    _value(maker_payload, "order_id", "id", default="")
                )
                maker_order = self.orders.get(maker_order_id)
                if maker_order is not None and maker_order is not taker_order:
                    targets.append((maker_order, maker_payload, "matched_amount"))

            if not targets:
                return ProcessResult(
                    True,
                    requires_reconciliation=True,
                    reason="trade references unknown order",
                    event_id=event.event_id,
                )

            parsed_targets: list[tuple[OrderAggregate, Decimal, Decimal, Decimal]] = []
            for order, target_payload, size_key in targets:
                size = Decimal(str(_value(target_payload, size_key)))
                price = Decimal(str(_value(target_payload, "price")))
                fee_rate_value = _value(
                    target_payload,
                    "fee_rate_bps",
                    default=_value(payload, "fee_rate_bps"),
                )
                if status is TradeStatus.CONFIRMED and fee_rate_value is None:
                    raise ValueError("confirmed trade fee rate is unknown")
                fee_rate_bps = Decimal(str(fee_rate_value or "0"))
                token_id = str(
                    _value(target_payload, "asset_id", "token_id", default="")
                )
                if token_id and token_id != order.token_id:
                    raise ValueError("trade token does not match local order")
                fee = taker_fee(
                    shares=size,
                    price=price,
                    fee_rate=fee_rate_bps / Decimal("10000"),
                )
                parsed_targets.append((order, size, price, fee))

            trade_id = str(_value(payload, "id"))
            for order, size, price, fee in parsed_targets:
                order.record_trade(
                    trade_id,
                    size=size,
                    price=price,
                    fee=fee,
                    status=status,
                )
        except (ArithmeticError, TypeError, ValueError):
            return ProcessResult(
                True,
                requires_reconciliation=True,
                reason="malformed trade event requires reconciliation",
                event_id=event.event_id,
            )
        return ProcessResult(True, reason="trade event applied", event_id=event.event_id)


class SubscriptionHandle(Protocol):
    def __aiter__(self) -> AsyncIterator[Any]: ...

    async def close(self) -> None: ...


SubscribeFactory = Callable[
    [Any], SubscriptionHandle | Awaitable[SubscriptionHandle]
]
Sleep = Callable[[float], Awaitable[None]]


class ReconnectingStream:
    """Supervise an SDK subscription without placing account actions."""

    def __init__(
        self,
        subscribe: SubscribeFactory,
        spec: Any,
        processor: StreamEventProcessor,
        *,
        policy: ReconnectPolicy | None = None,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self._subscribe = subscribe
        self._spec = spec
        self._processor = processor
        self._policy = policy or ReconnectPolicy()
        self._sleep = sleep
        self.state = StreamState.DISCONNECTED
        self.last_error: str | None = None
        self.reconnect_attempts = 0

    async def run(self, *, max_cycles: int | None = None) -> None:
        cycles = 0
        while max_cycles is None or cycles < max_cycles:
            cycles += 1
            handle: SubscriptionHandle | None = None
            self.state = StreamState.CONNECTING
            try:
                candidate = self._subscribe(self._spec)
                if inspect.isawaitable(candidate):
                    handle = await candidate
                else:
                    handle = cast(SubscriptionHandle, candidate)
                self.state = StreamState.SUBSCRIBED
                async for event in handle:
                    self._processor.process(event)
                    self.reconnect_attempts = 0
                self._processor.require_reconciliation(
                    "stream gap: subscription ended"
                )
                self.last_error = None
            except asyncio.CancelledError:
                self.state = StreamState.STOPPED
                raise
            except Exception as exc:  # reconnect is deliberately fail-closed
                self.last_error = f"{type(exc).__name__}: {exc}"
                self.state = StreamState.DISCONNECTED
                self._processor.require_reconciliation(
                    f"stream gap: {type(exc).__name__}: {exc}"
                )
            finally:
                if handle is not None:
                    try:
                        await handle.close()
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        self.last_error = f"close {type(exc).__name__}: {exc}"
                        self.state = StreamState.DISCONNECTED
                        self._processor.require_reconciliation(
                            f"stream gap during close: {type(exc).__name__}: {exc}"
                        )

            if max_cycles is not None and cycles >= max_cycles:
                break
            self.reconnect_attempts += 1
            self.state = StreamState.BACKOFF
            await self._sleep(self._policy.delay(self.reconnect_attempts))
        self.state = StreamState.STOPPED
