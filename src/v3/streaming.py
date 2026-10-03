"""Read-only stream normalization, replay, and reconnect supervision."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from types import SimpleNamespace
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol, cast

from .ledger import EventLedger, LedgerEvent
from .math import taker_fee
from .orders import OrderAggregate, OrderReconciliationRequired, OrderState, TradeStatus
from .reconciliation import RemoteTrade


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
        self._reconcile_pending_submissions()

    @property
    def active_order_ids(self) -> frozenset[str]:
        """Exchange IDs for durable orders that have not reached a terminal state."""
        terminal = {OrderState.CANCELED, OrderState.FILLED, OrderState.FAILED}
        return frozenset(
            order_id for order_id, order in self.orders.items()
            if order.state not in terminal
        )

    def _reconcile_pending_submissions(self) -> None:
        pending: dict[str, str] = {}
        latched = any(
            event.event_type == "order.submission_reconciliation_latched"
            for event in self.ledger.events()
        )
        for event in self.ledger.events():
            if event.event_type not in {
                "order.submission.started", "order.submission.attempted",
                "order.accepted", "order.rejected",
            }:
                continue
            client_id = event.payload.get("client_order_id")
            valid_id = isinstance(client_id, str) and bool(client_id)
            if event.event_type in {"order.submission.started", "order.submission.attempted"}:
                identity = f"client:{client_id}" if valid_id else f"event:{event.event_id}"
                pending[identity] = client_id if valid_id else event.event_id  # type: ignore[assignment]
            elif valid_id and not latched and self._valid_submission_terminal(event):
                pending.pop(f"client:{client_id}", None)
        if not pending:
            return
        identities = sorted(pending)
        client_ids = sorted(value for key, value in pending.items() if key.startswith("client:"))
        event_ids = sorted(value for key, value in pending.items() if key.startswith("event:"))
        labels = client_ids + [f"malformed event {event_id}" for event_id in event_ids]
        reason = "unresolved order submission requires reconciliation: " + ", ".join(labels)
        latch_id = "submission-reconciliation:" + hashlib.sha256(
            ",".join(identities).encode()
        ).hexdigest()
        self.ledger.append(LedgerEvent.create(
            "order.submission_reconciliation_latched",
            {"client_order_ids": client_ids, "event_ids": event_ids, "reason": reason}, event_id=latch_id,
        ))
        self._record_result(ProcessResult(
            False, requires_reconciliation=True, reason=reason, event_id=latch_id,
        ))

    @staticmethod
    def _valid_submission_terminal(event: LedgerEvent) -> bool:
        payload = event.payload
        client_id = payload.get("client_order_id")
        if not isinstance(client_id, str) or not client_id:
            return False
        if event.event_type == "order.rejected":
            return all(
                isinstance(payload.get(key), str) and bool(payload[key])
                for key in ("code", "message")
            )
        if event.event_type != "order.accepted":
            return False
        order_id = payload.get("order_id")
        token_id = payload.get("token_id")
        side = payload.get("side")
        status = payload.get("status")
        requested_size = payload.get("requested_size")
        if not all(isinstance(value, str) and value for value in (order_id, token_id, side, status)):
            return False
        if not isinstance(requested_size, (str, int, float, Decimal)) or isinstance(requested_size, bool):
            return False
        try:
            parsed_size = Decimal(str(requested_size))
            if not parsed_size.is_finite() or parsed_size <= 0:
                return False
            aggregate = OrderAggregate.new(
                client_order_id=client_id, token_id=cast(str, token_id), side=cast(str, side),
                requested_size=parsed_size,
            )
            aggregate.accept(order_id=cast(str, order_id), status=cast(str, status))
        except (ArithmeticError, TypeError, ValueError):
            return False
        return True

    def _replay_ledger(self) -> None:
        for event in self.ledger.events():
            if event.event_type == "order.accepted":
                result = self._adopt_accepted_order(event)
            elif event.event_type == "order.submission_reconciliation_latched":
                result = ProcessResult(
                    False, requires_reconciliation=True,
                    reason=str(event.payload.get("reason", "persisted submission reconciliation latch")),
                    event_id=event.event_id,
                )
            elif event.event_type == "stream.reconciliation_required":
                result = ProcessResult(
                    False, requires_reconciliation=True,
                    reason=str(event.payload.get("reason", "persisted stream gap")),
                    event_id=event.event_id,
                )
            else:
                normalized = NormalizedStreamEvent(
                    event_id=event.event_id,
                    event_type=event.event_type,
                    occurred_at=event.occurred_at or None,
                    payload=event.payload,
                )
                result = self._apply(normalized)
            self._record_result(result)
            self._applied_event_ids.add(event.event_id)

    def _adopt_accepted_order(self, event: LedgerEvent) -> ProcessResult:
        if not self._valid_submission_terminal(event):
            return ProcessResult(
                False, requires_reconciliation=True,
                reason="persisted accepted order is malformed; reconciliation required",
                event_id=event.event_id,
            )
        payload = event.payload
        order_id = str(payload.get("order_id", ""))
        try:
            if not order_id:
                raise ValueError("accepted order has no exchange ID")
            order = OrderAggregate.new(
                client_order_id=str(payload["client_order_id"]),
                token_id=str(payload["token_id"]),
                side=str(payload["side"]),
                requested_size=Decimal(str(payload["requested_size"])),
            )
            order.accept(order_id=order_id, status=str(payload["status"]).lower())
        except (ArithmeticError, KeyError, TypeError, ValueError):
            return ProcessResult(
                False, requires_reconciliation=True,
                reason="persisted accepted order is malformed; reconciliation required",
                event_id=event.event_id,
            )
        existing = self.orders.get(order_id)
        if existing is not None:
            if (
                existing.client_order_id != order.client_order_id
                or existing.token_id != order.token_id
                or existing.side != order.side
                or existing.requested_size != order.requested_size
            ):
                return ProcessResult(
                    False, requires_reconciliation=True,
                    reason="persisted accepted order conflicts with existing order",
                    event_id=event.event_id,
                )
        else:
            self.orders[order_id] = order
        self.managed_order_ids = self.managed_order_ids | {order_id}
        return ProcessResult(True, reason="persisted accepted order restored", event_id=event.event_id)

    def _record_result(self, result: ProcessResult) -> ProcessResult:
        if result.requires_reconciliation:
            self.reconciliation_required = True
            if result.reason and result.reason not in self.reconciliation_reasons:
                self.reconciliation_reasons.append(result.reason)
        return result

    def require_reconciliation(self, reason: str) -> None:
        """Persist a sticky fail-closed blocker after a stream gap or ambiguity."""
        event = LedgerEvent.create("stream.reconciliation_required", {"reason": reason})
        self.ledger.append(event)
        self._applied_event_ids.add(event.event_id)
        self._record_result(ProcessResult(
            accepted=False,
            requires_reconciliation=True,
            reason=reason,
            event_id=event.event_id,
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

    def import_remote_trade(self, trade: RemoteTrade) -> ProcessResult:
        """Replay one validated history row through the normal user-trade path.

        This imports only associations attributable to managed orders; it is not
        a complete remote account reconciliation.
        """
        if not isinstance(trade, RemoteTrade):
            self.require_reconciliation("malformed remote trade row requires reconciliation")
            return ProcessResult(False, requires_reconciliation=True,
                                 reason="malformed remote trade row requires reconciliation")
        maker_rows = [{
            "order_id": maker.order_id,
            "asset_id": maker.token_id,
            "side": maker.side,
            "matched_amount": maker.matched_amount,
            "price": maker.price,
            "fee_rate_bps": maker.fee_rate_bps,
        } for maker in trade.maker_orders]
        if trade.trader_side == "TAKER":
            if trade.taker_order_id not in self.managed_order_ids:
                reason = "remote trade references unknown taker order"
                self.require_reconciliation(reason)
                return ProcessResult(False, requires_reconciliation=True, reason=reason)
            managed_order = self.orders.get(trade.taker_order_id)
            if managed_order is None or managed_order.side != trade.side:
                reason = "remote trade side conflicts with managed taker order"
                self.require_reconciliation(reason)
                return ProcessResult(False, requires_reconciliation=True, reason=reason)
        elif not any(row["order_id"] in self.managed_order_ids for row in maker_rows):
            reason = "remote trade has no managed maker association"
            self.require_reconciliation(reason)
            return ProcessResult(False, requires_reconciliation=True, reason=reason)
        elif any(
            row["order_id"] in self.managed_order_ids
            and (self.orders.get(row["order_id"]) is None
                 or self.orders[row["order_id"]].side != row["side"])
            for row in maker_rows
        ):
            reason = "remote maker side conflicts with managed maker order"
            self.require_reconciliation(reason)
            return ProcessResult(False, requires_reconciliation=True, reason=reason)
        if trade.trader_side == "TAKER" and trade.fee_rate_bps is None:
            reason = "remote trade fee rate is unknown"
            self.require_reconciliation(reason)
            return ProcessResult(False, requires_reconciliation=True, reason=reason)
        if trade.trader_side == "MAKER" and any(
            maker.order_id in self.managed_order_ids and maker.fee_rate_bps is None
            for maker in trade.maker_orders
        ):
            reason = "remote maker trade fee rate is unknown"
            self.require_reconciliation(reason)
            return ProcessResult(False, requires_reconciliation=True, reason=reason)
        payload = {
            "id": trade.trade_id,
            "taker_order_id": trade.taker_order_id,
            "asset_id": trade.token_id,
            "side": trade.side,
            "size": trade.size,
            "price": trade.price,
            "status": trade.status,
            "fee_rate_bps": trade.fee_rate_bps,
            "timestamp": trade.matched_at,
            "last_update": trade.updated_at,
            "maker_orders": maker_rows,
        }
        return self.process(SimpleNamespace(topic="user", type="trade", payload=payload))

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
            target_order_ids: set[str] = set()

            taker_order_id = str(_value(payload, "taker_order_id", default=""))
            taker_order = self.orders.get(taker_order_id)
            if taker_order is not None:
                target_order_ids.add(taker_order_id)
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
                if maker_order is not None and maker_order_id in target_order_ids:
                    raise OrderReconciliationRequired(
                        "trade event repeats a managed order target"
                    )
                if maker_order is not None:
                    target_order_ids.add(maker_order_id)
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
                trade_id = str(_value(payload, "id"))
                order.validate_trade(
                    trade_id, size=size, price=price, fee=fee, status=status,
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
        except OrderReconciliationRequired as exc:
            return ProcessResult(
                True,
                requires_reconciliation=True,
                reason=str(exc),
                event_id=event.event_id,
            )
        except ValueError:
            return ProcessResult(
                True,
                requires_reconciliation=True,
                reason="malformed trade event requires reconciliation",
                event_id=event.event_id,
            )
        except (ArithmeticError, TypeError):
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
