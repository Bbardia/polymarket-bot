"""Read-only stream normalization, replay, and reconnect supervision."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from types import SimpleNamespace
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol, cast

from .api import CompleteAccountCashFlowHistory
from .ledger import EventLedger, LedgerEvent
from .math import taker_fee
from .orders import OrderAggregate, OrderReconciliationRequired, OrderState, TradeStatus
from .reconciliation import (
    CompleteAccountTradeHistory, LocalSnapshot, Reconciler,
    RemotePosition, RemoteTrade, RemoteSnapshot,
)

ZERO = Decimal("0")


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


def _as_aware_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        try:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            result = datetime.fromtimestamp(value, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    else:
        return None
    if result.tzinfo is None or result.utcoffset() is None:
        return None
    return result.astimezone(timezone.utc)


def _venue_match_time(payload: Mapping[str, Any]) -> datetime | None:
    """Return only an explicit venue match timestamp, never local receipt time."""
    for key in ("timestamp", "match_time", "matchtime", "matched_at"):
        parsed = _as_aware_datetime(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def _remote_snapshot_payload(remote: RemoteSnapshot) -> dict[str, Any]:
    positions = sorted(remote.positions, key=lambda item: (item.condition_id, item.token_id))
    orders = sorted(remote.open_orders, key=lambda item: item.order_id)
    return {
        "cash": str(remote.cash),
        "positions": [
            {
                "condition_id": item.condition_id,
                "token_id": item.token_id,
                "size": str(item.size),
                "current_value": str(item.current_value),
                "initial_value": None if item.initial_value is None else str(item.initial_value),
                "redeemable": item.redeemable,
            }
            for item in positions
        ],
        "open_orders": [
            {
                "order_id": item.order_id,
                "condition_id": item.condition_id,
                "token_id": item.token_id,
                "remaining_notional": None if item.remaining_notional is None else str(item.remaining_notional),
            }
            for item in orders
        ],
    }


def remote_snapshot_sha256(remote: RemoteSnapshot) -> str:
    """Fingerprint the exact authenticated account snapshot used for recovery."""
    return hashlib.sha256(_stable_json(_remote_snapshot_payload(remote)).encode("utf-8")).hexdigest()



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
        self._resolved_reconciliation_event_ids = self._validated_fee_resolutions()
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

    @staticmethod
    def _legacy_snapshot_matches(evidence: Mapping[str, Any], digest: str) -> bool:
        raw = evidence.get("account_snapshot")
        external_raw = evidence.get("external_condition_ids")
        if (
            not isinstance(raw, Mapping)
            or set(raw) != {"cash", "positions", "open_orders"}
            or not isinstance(raw.get("positions"), list)
            or raw.get("open_orders") != []
            or not isinstance(external_raw, list)
            or any(not isinstance(value, str) or not value for value in external_raw)
            or external_raw != sorted(set(external_raw))
        ):
            return False
        try:
            cash = Decimal(str(raw["cash"]))
            positions: list[RemotePosition] = []
            for row in raw["positions"]:
                if not isinstance(row, Mapping) or set(row) != {
                    "condition_id", "token_id", "size", "current_value", "initial_value", "redeemable",
                }:
                    return False
                if (
                    not isinstance(row["condition_id"], str) or not row["condition_id"]
                    or not isinstance(row["token_id"], str) or not row["token_id"]
                    or type(row["redeemable"]) is not bool
                ):
                    return False
                size = Decimal(str(row["size"]))
                current_value = Decimal(str(row["current_value"]))
                initial_value = None if row["initial_value"] is None else Decimal(str(row["initial_value"]))
                if not all(value.is_finite() and value >= ZERO for value in (size, current_value)):
                    return False
                if initial_value is not None and (not initial_value.is_finite() or initial_value < ZERO):
                    return False
                positions.append(RemotePosition(
                    condition_id=row["condition_id"], token_id=row["token_id"], size=size,
                    current_value=current_value, initial_value=initial_value, redeemable=row["redeemable"],
                ))
        except (ArithmeticError, KeyError, TypeError, ValueError):
            return False
        if not cash.is_finite() or cash < ZERO:
            return False
        remote = RemoteSnapshot(cash=cash, positions=tuple(positions), open_orders=())
        if remote_snapshot_sha256(remote) != digest:
            return False
        external_ids = frozenset(external_raw)
        managed_positions = [row for row in positions if row.condition_id not in external_ids]
        try:
            remote_cash = Decimal(str(evidence["remote_cash"]))
            remote_size = Decimal(str(evidence["remote_position_size"]))
            remote_cost = Decimal(str(evidence["remote_position_cost"]))
            expected_size = Decimal(str(evidence["matched_size"]))
            expected_cost = expected_size * Decimal(str(evidence["maker_price"]))
        except (ArithmeticError, KeyError, TypeError, ValueError):
            return False
        return (
            len(managed_positions) == 1
            and all(row.condition_id in external_ids for row in positions if row not in managed_positions)
            and type(evidence.get("remote_open_orders")) is int and evidence["remote_open_orders"] == 0
            and type(evidence.get("external_position_count")) is int
            and evidence["external_position_count"] == len(positions) - 1
            and managed_positions[0].condition_id == evidence.get("condition_id")
            and managed_positions[0].token_id == evidence.get("token_id")
            and managed_positions[0].size == remote_size == expected_size
            and managed_positions[0].initial_value == remote_cost == expected_cost
            and remote.cash == remote_cash
        )

    @staticmethod
    def _validated_legacy_fee_resolution(
        event: LedgerEvent,
        events: tuple[LedgerEvent, ...],
        by_id: Mapping[str, LedgerEvent],
    ) -> frozenset[str]:
        payload = event.payload
        evidence = payload.get("evidence")
        ids = payload.get("resolved_event_ids")
        if (
            payload.get("resolution") != "verified_legacy_fee_history_reconciliation"
            or payload.get("reason") != "remote maker trade fee rate is unknown"
            or not isinstance(evidence, Mapping)
            or not isinstance(ids, list) or not ids
            or any(not isinstance(target_id, str) or not target_id for target_id in ids)
            or len(ids) != len(set(ids))
            or evidence.get("complete_account_trade_history") is not True
            or evidence.get("history_trade_ids") != [evidence.get("trade_id")]
            or evidence.get("complete_account_cash_flow_history") is not True
            or type(evidence.get("cash_flow_count")) is not int or evidence.get("cash_flow_count") != 0
            or evidence.get("cash_flow_net") != "0"
            or evidence.get("trade_status") != "CONFIRMED"
            or evidence.get("trader_side") != "MAKER"
            or evidence.get("trade_fee_rate_bps") != "0"
            or evidence.get("maker_fee_rate_bps") not in (None, "0")
            or evidence.get("maker_side") != "BUY"
            or evidence.get("order_side") != "BUY"
            or evidence.get("post_only") is not True
            or type(evidence.get("remote_open_orders")) is not int
            or evidence.get("remote_open_orders") != 0
            or evidence.get("full_account_reconciliation_safe") is not True
            or payload.get("latch_scope_trade_id") != evidence.get("trade_id")
            or payload.get("latch_scope_order_id") != evidence.get("managed_order_id")
        ):
            return frozenset()
        digest = evidence.get("account_snapshot_sha256")
        if (
            not isinstance(digest, str) or len(digest) != 64
            or any(ch not in "0123456789abcdef" for ch in digest)
            or not StreamEventProcessor._legacy_snapshot_matches(evidence, digest)
        ):
            return frozenset()
        trade_id = evidence.get("trade_id")
        order_id = evidence.get("managed_order_id")
        matched_raw = evidence.get("trade_matched_at")
        fetched_raw = evidence.get("history_fetched_at")
        flow_raw = evidence.get("cash_flow_history_fetched_at")
        snapshot_raw = evidence.get("account_snapshot_fetched_at")
        resolution_raw = event.occurred_at
        try:
            matched_at = datetime.fromisoformat(str(matched_raw))
            fetched_at = datetime.fromisoformat(str(fetched_raw))
            flow_at = datetime.fromisoformat(str(flow_raw))
            snapshot_at = datetime.fromisoformat(str(snapshot_raw))
            resolved_at = datetime.fromisoformat(resolution_raw)
            size = Decimal(str(evidence["matched_size"]))
            price = Decimal(str(evidence["maker_price"]))
            baseline_cash = Decimal(str(evidence["baseline_cash"]))
            remote_cash = Decimal(str(evidence["remote_cash"]))
            remote_size = Decimal(str(evidence["remote_position_size"]))
            remote_cost = Decimal(str(evidence["remote_position_cost"]))
            cash_delta = Decimal(str(evidence["cash_delta"]))
            history_after = evidence["history_after"]
            baseline_epoch = evidence["baseline_epoch"]
            cash_flow_after = evidence["cash_flow_history_after"]
            max_items = evidence["history_max_items"]
            page_limit = evidence["history_page_limit"]
            cash_max_items = evidence["cash_flow_max_items"]
            cash_page_size = evidence["cash_flow_page_size"]
        except (ArithmeticError, KeyError, TypeError, ValueError):
            return frozenset()
        if (
            not all(isinstance(x, str) and x for x in (trade_id, order_id, evidence.get("token_id"), evidence.get("condition_id")))
            or any(dt.tzinfo is None or dt.utcoffset() is None for dt in (matched_at, fetched_at, flow_at, snapshot_at, resolved_at))
            or fetched_at < matched_at or flow_at < fetched_at or snapshot_at < flow_at or resolved_at < snapshot_at
            or type(history_after) is not int or type(baseline_epoch) is not int or history_after != baseline_epoch or baseline_epoch < 0
            or type(cash_flow_after) is not int or cash_flow_after != baseline_epoch
            or type(max_items) is not int or max_items <= 0
            or type(page_limit) is not int or page_limit <= 0
            or type(cash_max_items) is not int or cash_max_items <= 0
            or type(cash_page_size) is not int or cash_page_size <= 0
            or not all(value.is_finite() for value in (size, price, baseline_cash, remote_cash, remote_size, remote_cost, cash_delta))
            or cash_delta != ZERO or size <= ZERO or price <= ZERO
            or remote_cash != baseline_cash - size * price
            or remote_size != size or remote_cost != size * price
        ):
            return frozenset()
        accepted = [
            row for row in events
            if row.event_type == "order.accepted"
            and row.payload.get("order_id") == order_id
            and row.payload.get("condition_id") == evidence.get("condition_id")
            and row.payload.get("token_id") == evidence.get("token_id")
            and row.payload.get("side") == "BUY"
            and row.payload.get("post_only") is True
        ]
        persisted_trades = [
            row for row in events
            if row.event_type == "user.trade" and row.payload.get("id") == trade_id
        ]
        if len(persisted_trades) != 1:
            return frozenset()
        persisted = persisted_trades[0]
        persisted_makers = persisted.payload.get("maker_orders")
        matching_makers = [
            row for row in persisted_makers
            if isinstance(row, Mapping) and row.get("order_id") == order_id
        ] if isinstance(persisted_makers, (list, tuple)) else []
        try:
            persisted_match_at = _as_aware_datetime(persisted.payload.get("timestamp"))
            persisted_size = Decimal(str(matching_makers[0]["matched_amount"])) if len(matching_makers) == 1 else ZERO
            persisted_price = Decimal(str(matching_makers[0]["price"])) if len(matching_makers) == 1 else ZERO
        except (ArithmeticError, KeyError, TypeError, ValueError):
            return frozenset()
        persisted_maker = matching_makers[0] if len(matching_makers) == 1 else {}
        if (
            persisted.payload.get("status") != "CONFIRMED"
            or persisted.payload.get("fee_rate_bps") != "0"
            or persisted_match_at != matched_at
            or persisted.payload.get("asset_id") is None
            or persisted_maker.get("asset_id") != evidence.get("token_id")
            or persisted_maker.get("side") != "BUY"
            or persisted_maker.get("fee_rate_bps") not in (None, "0")
            or evidence.get("maker_fee_rate_bps") == "0" and persisted_maker.get("fee_rate_bps") != "0"
            or persisted_size != size or persisted_price != price
        ):
            return frozenset()
        accepted_index = {row.event_id: index for index, row in enumerate(events)}
        if len(accepted) != 1 or accepted_index.get(accepted[0].event_id, len(events)) >= accepted_index.get(persisted.event_id, -1):
            return frozenset()
        try:
            prior_blockers = [
                row for row in events
                if row.event_type == "stream.reconciliation_required"
                and datetime.fromisoformat(row.occurred_at) <= resolved_at
            ]
            eligible = [
                row for row in prior_blockers
                if row.payload.get("reason") == "remote maker trade fee rate is unknown"
                and "source_trade_id" not in row.payload
                and "source_order_id" not in row.payload
            ]
            targets_follow_trade = all(
                row.event_id in by_id
                and datetime.fromisoformat(row.occurred_at) >= matched_at
                and datetime.fromisoformat(row.occurred_at) < resolved_at
                for row in eligible
            )
        except (TypeError, ValueError):
            return frozenset()
        ledger_index = {row.event_id: index for index, row in enumerate(events)}
        trade_precedes_resolution = ledger_index.get(persisted.event_id, len(events)) < ledger_index.get(event.event_id, -1)
        latches_precede_resolution = all(ledger_index.get(row.event_id, len(events)) < ledger_index.get(event.event_id, -1) for row in eligible)
        if (
            len(accepted) != 1
            or accepted_index.get(accepted[0].event_id, len(events)) >= accepted_index.get(persisted.event_id, -1)
            or not trade_precedes_resolution or not latches_precede_resolution
            or len(prior_blockers) != len(eligible)
            or set(ids) != {row.event_id for row in eligible}
            or not targets_follow_trade
        ):
            return frozenset()
        return frozenset(ids)

    def _validated_fee_resolutions(self) -> frozenset[str]:
        """Accept only complete, scoped, evidence-bearing zero-maker-fee acks."""
        events = tuple(self.ledger.events())
        by_id = {event.event_id: event for event in events}
        resolved: set[str] = set()
        for event in events:
            if event.event_type != "stream.reconciliation_resolved":
                continue
            payload = event.payload
            evidence = payload.get("evidence")
            ids = payload.get("resolved_event_ids")
            if payload.get("resolution") == "verified_legacy_fee_history_reconciliation":
                resolved.update(self._validated_legacy_fee_resolution(event, events, by_id))
                continue
            if (
                payload.get("resolution") != "verified_zero_fee_managed_maker"
                or payload.get("reason") != "remote maker trade fee rate is unknown"
                or not isinstance(evidence, Mapping)
                or not isinstance(ids, list) or not ids
                or evidence.get("trade_status") != "CONFIRMED"
                or evidence.get("trader_side") != "MAKER"
                or evidence.get("trade_fee_rate_bps") != "0"
                or evidence.get("maker_fee_rate_bps") is not None
                or evidence.get("post_only") is not True
                or evidence.get("remote_open_orders") != 0
                or evidence.get("cash_flow_net") != "0"
            ):
                continue
            digest = evidence.get("account_snapshot_sha256")
            if (
                not isinstance(digest, str) or len(digest) != 64
                or any(ch not in "0123456789abcdef" for ch in digest)
            ):
                continue
            trade_id = evidence.get("trade_id")
            order_id = evidence.get("managed_order_id")
            history_ids = evidence.get("managed_trade_history_ids")
            matched_at_raw = evidence.get("trade_matched_at")
            try:
                matched_at = datetime.fromisoformat(str(matched_at_raw))
            except ValueError:
                continue
            if (
                evidence.get("complete_managed_trade_history") is not True
                or history_ids != [trade_id]
                or payload.get("latch_scope_trade_id") != trade_id
                or payload.get("latch_scope_order_id") != order_id
                or matched_at.tzinfo is None
            ):
                continue
            try:
                size = Decimal(str(evidence["matched_size"]))
                price = Decimal(str(evidence["maker_price"]))
                baseline_cash = Decimal(str(evidence["baseline_cash"]))
                remote_cash = Decimal(str(evidence["remote_cash"]))
                remote_size = Decimal(str(evidence["remote_position_size"]))
                remote_cost = Decimal(str(evidence["remote_position_cost"]))
                expected_cost = size * price
            except (ArithmeticError, KeyError, TypeError, ValueError):
                continue
            order_id = evidence.get("managed_order_id")
            token_id = evidence.get("token_id")
            condition_id = evidence.get("condition_id")
            trade_id = evidence.get("trade_id")
            if (
                not all(isinstance(x, str) and x for x in (order_id, token_id, condition_id, trade_id))
                or not size.is_finite() or size <= ZERO
                or not price.is_finite() or price <= ZERO
                or not all(x.is_finite() for x in (baseline_cash, remote_cash, remote_size, remote_cost))
                or remote_cash != baseline_cash - expected_cost
                or remote_size != size or remote_cost != expected_cost
            ):
                continue
            accepted = [
                prior for prior in events
                if prior.event_type == "order.accepted"
                and prior.payload.get("order_id") == order_id
                and prior.payload.get("token_id") == token_id
                and prior.payload.get("condition_id") == condition_id
                and prior.payload.get("post_only") is True
            ]
            targets = [by_id.get(target_id) for target_id in ids]
            try:
                target_times_valid = all(
                    target is not None
                    and datetime.fromisoformat(target.occurred_at) >= matched_at
                    for target in targets
                )
            except (TypeError, ValueError):
                target_times_valid = False
            if (
                len(accepted) != 1
                or len(ids) != len(set(ids))
                or not target_times_valid
                or any(
                    target is None
                    or target.event_type != "stream.reconciliation_required"
                    or target.payload.get("reason") != "remote maker trade fee rate is unknown"
                    or target.payload.get("source_trade_id") != trade_id
                    or target.payload.get("source_order_id") != order_id
                    for target in targets
                )
            ):
                continue
            resolved.update(ids)
        return frozenset(resolved)

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
                if event.event_id in self._resolved_reconciliation_event_ids:
                    result = ProcessResult(
                        True, reason="reconciliation latch explicitly resolved with account evidence",
                        event_id=event.event_id,
                    )
                else:
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

    def require_reconciliation(
        self,
        reason: str,
        *,
        source_trade_id: str | None = None,
        source_order_id: str | None = None,
    ) -> None:
        """Persist a sticky fail-closed blocker, optionally bound to one trade/order."""
        payload: dict[str, Any] = {"reason": reason}
        if source_trade_id is not None or source_order_id is not None:
            if not source_trade_id or not source_order_id:
                raise ValueError("trade-scoped latches require both source IDs")
            payload.update({"source_trade_id": source_trade_id, "source_order_id": source_order_id})
        event = LedgerEvent.create("stream.reconciliation_required", payload)
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

    def resolve_zero_fee_maker_reconciliation(
        self,
        *,
        trade: RemoteTrade,
        account_trades_since_baseline: tuple[RemoteTrade, ...],
        remote: RemoteSnapshot,
        baseline_cash: Decimal,
        external_condition_ids: frozenset[str],
        cash_flow_net: Decimal,
        account_snapshot_sha256: str,
    ) -> ProcessResult:
        """Append an audited resolution for fee-only latches after parity is proven.

        This narrow recovery exists for venue history that explicitly reports a
        zero trade fee but omits the maker-specific fee field. It will not clear
        stream gaps, unrelated blockers, unexplained flows, open orders, or any
        account-position/cash mismatch. Existing latch rows remain immutable.
        Call only after a fresh authenticated read-only snapshot and trade-history
        lookup; a new processor must be constructed after this append.
        """
        if not isinstance(trade, RemoteTrade) or not isinstance(remote, RemoteSnapshot):
            raise ValueError("verified account trade and snapshot are required")
        if not isinstance(account_trades_since_baseline, tuple) or any(
            not isinstance(row, RemoteTrade) for row in account_trades_since_baseline
        ):
            raise ValueError("complete bounded account-trade history since baseline is required")
        managed_history = [
            row for row in account_trades_since_baseline
            if row.taker_order_id in self.managed_order_ids
            or any(maker.order_id in self.managed_order_ids for maker in row.maker_orders)
        ]
        if len(managed_history) != 1 or managed_history[0] != trade:
            raise ValueError("fee latches cannot be scoped: history must contain exactly this managed trade")
        if (
            trade.status != "CONFIRMED" or trade.trader_side != "MAKER"
            or trade.fee_rate_bps != Decimal("0")
            or not isinstance(cash_flow_net, Decimal) or cash_flow_net != ZERO
        ):
            raise ValueError("recovery requires a confirmed maker trade with explicit zero fee and no net cash flows")
        if account_snapshot_sha256.lower() != remote_snapshot_sha256(remote):
            raise ValueError("account snapshot digest does not match the supplied remote snapshot")

        managed_makers = [maker for maker in trade.maker_orders if maker.order_id in self.managed_order_ids]
        if len(managed_makers) != 1:
            raise ValueError("trade must identify exactly one managed maker order")
        maker = managed_makers[0]
        order = self.orders.get(maker.order_id)
        if (
            order is None or order.side != "BUY" or maker.side != "BUY"
            or maker.token_id != order.token_id or maker.token_id != trade.token_id
            or maker.fee_rate_bps is not None
            or maker.price <= ZERO or maker.matched_amount <= ZERO
            or maker.matched_amount > order.requested_size
        ):
            raise ValueError("maker trade does not match a managed BUY order")
        accepted = [
            event for event in self.ledger.events()
            if event.event_type == "order.accepted"
            and event.payload.get("order_id") == maker.order_id
        ]
        if len(accepted) != 1 or accepted[0].payload.get("post_only") is not True:
            raise ValueError("managed order lacks unique persisted post-only acceptance evidence")
        bot_positions = [
            position for position in remote.positions
            if position.condition_id not in external_condition_ids
        ]
        expected_cost = maker.matched_amount * maker.price
        if (
            remote.open_orders
            or len(bot_positions) != 1
            or bot_positions[0].token_id != maker.token_id
            or bot_positions[0].condition_id != trade.condition_id
            or bot_positions[0].size != maker.matched_amount
            or bot_positions[0].initial_value != expected_cost
            or remote.cash != baseline_cash - expected_cost
        ):
            raise ValueError("remote account positions, open orders, or cash do not match the maker fill")
        if any(position.condition_id not in external_condition_ids for position in remote.positions if position is not bot_positions[0]):
            raise ValueError("account contains an unclassified remote position")

        events = tuple(self.ledger.events())
        resolutions = self._resolved_reconciliation_event_ids
        all_fee_latches = [
            event for event in events
            if event.event_type == "stream.reconciliation_required"
            and event.event_id not in resolutions
            and event.payload.get("reason") == "remote maker trade fee rate is unknown"
        ]
        fee_latches = [
            event for event in all_fee_latches
            if event.payload.get("source_trade_id") == trade.trade_id
            and event.payload.get("source_order_id") == maker.order_id
        ]
        unresolved_other = [
            event for event in events
            if event.event_type == "stream.reconciliation_required"
            and event.event_id not in resolutions
            and event.payload.get("reason") != "remote maker trade fee rate is unknown"
        ]
        try:
            latches_follow_trade = all(
                datetime.fromisoformat(event.occurred_at) >= trade.matched_at
                for event in fee_latches
            )
        except (TypeError, ValueError):
            latches_follow_trade = False
        if (
            not fee_latches
            or len(fee_latches) != len(all_fee_latches)
            or not latches_follow_trade
            or unresolved_other
            or any(reason != "remote maker trade fee rate is unknown" for reason in self.reconciliation_reasons)
        ):
            raise ValueError("ledger contains unscoped or ineligible reconciliation blockers")

        evidence = {
            "trade_id": trade.trade_id,
            "trade_status": trade.status,
            "trader_side": trade.trader_side,
            "trade_fee_rate_bps": str(trade.fee_rate_bps),
            "managed_order_id": maker.order_id,
            "post_only": True,
            "condition_id": trade.condition_id,
            "token_id": maker.token_id,
            "matched_size": str(maker.matched_amount),
            "maker_price": str(maker.price),
            "maker_fee_rate_bps": None,
            "baseline_cash": str(baseline_cash),
            "remote_cash": str(remote.cash),
            "remote_position_size": str(bot_positions[0].size),
            "remote_position_cost": str(bot_positions[0].initial_value),
            "remote_open_orders": 0,
            "cash_flow_net": str(cash_flow_net),
            "account_snapshot_sha256": account_snapshot_sha256.lower(),
            "complete_managed_trade_history": True,
            "managed_trade_history_ids": [row.trade_id for row in managed_history],
            "trade_matched_at": trade.matched_at.isoformat(),
        }
        resolved_ids = sorted(event.event_id for event in fee_latches)
        self.ledger.append(LedgerEvent.create(
            "stream.reconciliation_resolved",
            {
                "resolution": "verified_zero_fee_managed_maker",
                "reason": "remote maker trade fee rate is unknown",
                "latch_scope_trade_id": trade.trade_id,
                "latch_scope_order_id": maker.order_id,
                "resolved_event_ids": resolved_ids,
                "evidence": evidence,
            },
        ))
        return ProcessResult(
            accepted=True,
            reason=f"appended audited resolution for {len(resolved_ids)} fee-only latch events",
        )

    def resolve_legacy_fee_latches_after_reconciliation(
        self,
        *,
        history: CompleteAccountTradeHistory,
        remote: RemoteSnapshot,
        local: LocalSnapshot,
        reconciler: Reconciler,
        baseline_epoch: int,
        baseline_cash: Decimal,
        external_condition_ids: frozenset[str],
        cash_flow_history: CompleteAccountCashFlowHistory,
        account_snapshot_fetched_at: datetime,
    ) -> ProcessResult:
        """Append an exact-ID migration for old unscoped fee-only latches.

        This is deliberately narrower than normal recovery: a complete bounded
        history must contain exactly one managed trade, every legacy latch must
        postdate it, cash flows must be absent, and all current remote account
        state must match the already-replayed ledger before any latch is resolved.
        """
        if not isinstance(history, CompleteAccountTradeHistory) or history.after != baseline_epoch:
            raise ValueError("complete bounded account history at the session baseline is required")
        if not isinstance(remote, RemoteSnapshot) or not isinstance(local, LocalSnapshot):
            raise ValueError("fresh remote and replayed local snapshots are required")
        if type(baseline_epoch) is not int or baseline_epoch < 0 or not isinstance(baseline_cash, Decimal) or not baseline_cash.is_finite():
            raise ValueError("a valid session baseline is required")
        if not isinstance(external_condition_ids, frozenset) or any(not isinstance(x, str) or not x for x in external_condition_ids):
            raise ValueError("the external position allowlist must be an immutable validated set")
        if (
            not isinstance(cash_flow_history, CompleteAccountCashFlowHistory)
            or cash_flow_history.after != baseline_epoch
            or cash_flow_history.flows
            or cash_flow_history.net_amount != ZERO
        ):
            raise ValueError("complete post-baseline account history must show no cash flows")
        now = datetime.now(timezone.utc)
        flow_fetched_at = cash_flow_history.fetched_at.astimezone(timezone.utc)
        if (
            not isinstance(account_snapshot_fetched_at, datetime)
            or account_snapshot_fetched_at.tzinfo is None
            or account_snapshot_fetched_at.utcoffset() is None
            or (now - flow_fetched_at).total_seconds() < 0
            or (now - flow_fetched_at).total_seconds() > 300
            or flow_fetched_at < history.fetched_at.astimezone(timezone.utc)
            or account_snapshot_fetched_at.astimezone(timezone.utc) < flow_fetched_at
            or (now - account_snapshot_fetched_at.astimezone(timezone.utc)).total_seconds() < 0
            or (now - account_snapshot_fetched_at.astimezone(timezone.utc)).total_seconds() > 120
            or (now - history.fetched_at.astimezone(timezone.utc)).total_seconds() < 0
            or (now - history.fetched_at.astimezone(timezone.utc)).total_seconds() > 300
        ):
            raise ValueError("fresh account and trade-history reads are required for legacy recovery")
        if len(history.trades) != 1:
            raise ValueError("legacy latches require exactly one complete post-baseline account trade")
        trade = history.trades[0]
        if (
            trade.status != "CONFIRMED" or trade.trader_side != "MAKER"
            or trade.fee_rate_bps != ZERO
        ):
            raise ValueError("the sole post-baseline trade must be a confirmed explicit-zero-fee maker fill")
        managed_rows = [
            row for row in history.trades
            if row.taker_order_id in self.managed_order_ids
            or any(maker.order_id in self.managed_order_ids for maker in row.maker_orders)
        ]
        makers = [maker for maker in trade.maker_orders if maker.order_id in self.managed_order_ids]
        if len(managed_rows) != 1 or managed_rows[0] != trade or len(makers) != 1:
            raise ValueError("the sole account trade does not map to exactly one managed order")
        maker = makers[0]
        order = self.orders.get(maker.order_id)
        record = None if order is None else order.trades.get(trade.trade_id)
        if (
            order is None or record is None or not record.accounted
            or record.status is not TradeStatus.CONFIRMED
            or record.size != maker.matched_amount or record.price != maker.price
            or record.fee != ZERO or order.side != "BUY" or maker.side != "BUY"
            or maker.fee_rate_bps not in (None, ZERO)
            or maker.token_id != order.token_id
        ):
            raise ValueError("the confirmed trade is not fully accounted against its managed maker order")
        trade_events = [
            event for event in self.ledger.events()
            if event.event_type == "user.trade"
            and event.payload.get("id") == trade.trade_id
            and event.payload.get("status") == "CONFIRMED"
        ]
        if len(trade_events) != 1:
            raise ValueError("the confirmed remote trade must already exist exactly once in the append-only ledger")
        accepted = [
            event for event in self.ledger.events()
            if event.event_type == "order.accepted"
            and event.payload.get("order_id") == maker.order_id
            and event.payload.get("condition_id") == trade.condition_id
            and event.payload.get("token_id") == maker.token_id
            and event.payload.get("side") == "BUY"
            and event.payload.get("post_only") is True
        ]
        if len(accepted) != 1:
            raise ValueError("managed order lacks unique persisted post-only acceptance evidence")
        bot_positions = [p for p in remote.positions if p.condition_id not in external_condition_ids]
        expected_cost = maker.matched_amount * maker.price
        if (
            remote.open_orders or len(bot_positions) != 1
            or bot_positions[0].condition_id != trade.condition_id
            or bot_positions[0].token_id != maker.token_id
            or bot_positions[0].size != maker.matched_amount
            or bot_positions[0].initial_value != expected_cost
            or remote.cash != baseline_cash - expected_cost
        ):
            raise ValueError("remote cash, position, or open-order state does not match the sole managed trade")
        report = reconciler.compare(local, remote)
        if not report.safe_to_trade or report.cash_delta != ZERO:
            raise ValueError("replayed ledger does not reconcile exactly to the remote account")

        events = tuple(self.ledger.events())
        if self._resolved_reconciliation_event_ids:
            raise ValueError("legacy migration cannot overlap an existing latch-resolution event")
        blockers = [event for event in events if event.event_type == "stream.reconciliation_required"]
        if (
            not blockers
            or any(event.payload.get("reason") != "remote maker trade fee rate is unknown" for event in blockers)
            or any("source_trade_id" in event.payload or "source_order_id" in event.payload for event in blockers)
            or any(reason != "remote maker trade fee rate is unknown" for reason in self.reconciliation_reasons)
        ):
            raise ValueError("ledger contains a non-legacy, scoped, or unrelated reconciliation blocker")
        try:
            latch_times = [datetime.fromisoformat(event.occurred_at) for event in blockers]
        except (TypeError, ValueError) as exc:
            raise ValueError("legacy latch timestamps are missing or inconsistent") from exc
        if any(timestamp < trade.matched_at for timestamp in latch_times):
            raise ValueError("a legacy fee latch predates the sole managed trade")

        digest = remote_snapshot_sha256(remote)
        evidence = {
            "complete_account_trade_history": True,
            "history_after": history.after,
            "history_fetched_at": history.fetched_at.isoformat(),
            "account_snapshot_fetched_at": account_snapshot_fetched_at.astimezone(timezone.utc).isoformat(),
            "history_max_items": history.max_items,
            "history_page_limit": history.page_limit,
            "history_trade_ids": [row.trade_id for row in history.trades],
            "trade_id": trade.trade_id,
            "trade_status": trade.status,
            "trader_side": trade.trader_side,
            "trade_fee_rate_bps": str(trade.fee_rate_bps),
            "trade_matched_at": trade.matched_at.isoformat(),
            "managed_order_id": maker.order_id,
            "condition_id": trade.condition_id,
            "token_id": maker.token_id,
            "maker_side": maker.side,
            "order_side": order.side,
            "matched_size": str(maker.matched_amount),
            "maker_price": str(maker.price),
            "maker_fee_rate_bps": None if maker.fee_rate_bps is None else str(maker.fee_rate_bps),
            "post_only": True,
            "baseline_epoch": baseline_epoch,
            "baseline_cash": str(baseline_cash),
            "remote_cash": str(remote.cash),
            "remote_position_size": str(bot_positions[0].size),
            "remote_position_cost": str(bot_positions[0].initial_value),
            "remote_open_orders": len(remote.open_orders),
            "external_position_count": sum(p.condition_id in external_condition_ids for p in remote.positions),
            "external_condition_ids": sorted(external_condition_ids),
            "account_snapshot": _remote_snapshot_payload(remote),
            "complete_account_cash_flow_history": True,
            "cash_flow_history_after": cash_flow_history.after,
            "cash_flow_history_fetched_at": cash_flow_history.fetched_at.isoformat(),
            "cash_flow_max_items": cash_flow_history.max_items,
            "cash_flow_page_size": cash_flow_history.page_size,
            "cash_flow_count": len(cash_flow_history.flows),
            "cash_flow_net": str(cash_flow_history.net_amount),
            "account_snapshot_sha256": digest,
            "full_account_reconciliation_safe": True,
            "cash_delta": str(report.cash_delta),
        }
        target_ids = sorted(event.event_id for event in blockers)
        self.ledger.append(LedgerEvent.create(
            "stream.reconciliation_resolved",
            {
                "resolution": "verified_legacy_fee_history_reconciliation",
                "reason": "remote maker trade fee rate is unknown",
                "latch_scope_trade_id": trade.trade_id,
                "latch_scope_order_id": maker.order_id,
                "resolved_event_ids": target_ids,
                "evidence": evidence,
            },
        ))
        return ProcessResult(
            True,
            reason=f"appended full-history account reconciliation for {len(target_ids)} legacy fee latches",
        )

    def import_remote_trade(self, trade: RemoteTrade) -> ProcessResult:
        """Replay one validated history row through the normal user-trade path.

        This imports only associations attributable to managed orders; it is not
        a complete remote account reconciliation.
        """
        if not isinstance(trade, RemoteTrade):
            self.require_reconciliation("malformed remote trade row requires reconciliation")
            return ProcessResult(False, requires_reconciliation=True,
                                 reason="malformed remote trade row requires reconciliation")
        # Some account-trade responses omit the maker-specific fee field while
        # explicitly reporting a zero trade fee rate. Preserve that authoritative
        # zero for the managed maker; never infer zero from a missing value.
        maker_rows = [{
            "order_id": maker.order_id,
            "asset_id": maker.token_id,
            "side": maker.side,
            "matched_amount": maker.matched_amount,
            "price": maker.price,
            "fee_rate_bps": (
                maker.fee_rate_bps
                if maker.fee_rate_bps is not None
                else trade.fee_rate_bps if trade.fee_rate_bps == Decimal("0") else None
            ),
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
        # SELL-maker replay is deliberately limited to one uniquely persisted,
        # post-only managed order with complete, internally consistent fill data.
        sell_makers = [maker for maker in trade.maker_orders
                       if maker.order_id in self.managed_order_ids
                       and self.orders.get(maker.order_id) is not None
                       and self.orders[maker.order_id].side == "SELL"]
        if sell_makers:
            maker = sell_makers[0]
            order = self.orders[maker.order_id]
            accepted = [event for event in self.ledger.events()
                        if event.event_type == "order.accepted"
                        and event.payload.get("order_id") == maker.order_id]
            valid = (
                len(sell_makers) == 1 and len(accepted) == 1
                and accepted[0].payload.get("post_only") is True
                and accepted[0].payload.get("side") == "SELL"
                and accepted[0].payload.get("token_id") == order.token_id == maker.token_id == trade.token_id
                and accepted[0].payload.get("condition_id") == trade.condition_id
                and maker.side == "SELL" and trade.side == "BUY"
                and trade.trader_side == "MAKER"
                and trade.status == "CONFIRMED"
                and trade.fee_rate_bps is not None and maker.fee_rate_bps is not None
                and maker.matched_amount.is_finite() and maker.matched_amount > ZERO
                and maker.matched_amount <= order.requested_size
                and maker.price.is_finite() and maker.price > ZERO
                and maker.fee_rate_bps.is_finite() and maker.fee_rate_bps >= ZERO
                and trade.size == maker.matched_amount and trade.price == maker.price
            )
            if not valid:
                reason = "managed SELL maker fill lacks unique post-only evidence or consistent confirmed fee/quantity"
                self.require_reconciliation(reason, source_trade_id=trade.trade_id,
                                            source_order_id=maker.order_id)
                return ProcessResult(False, requires_reconciliation=True, reason=reason)
        if trade.trader_side == "TAKER" and trade.fee_rate_bps is None:
            reason = "remote trade fee rate is unknown"
            self.require_reconciliation(reason)
            return ProcessResult(False, requires_reconciliation=True, reason=reason)
        if trade.trader_side == "MAKER" and trade.fee_rate_bps != Decimal("0"):
            missing_fee_makers = [
                maker for maker in trade.maker_orders
                if maker.order_id in self.managed_order_ids and maker.fee_rate_bps is None
            ]
            if missing_fee_makers:
                reason = "remote maker trade fee rate is unknown"
                for maker in missing_fee_makers:
                    self.require_reconciliation(
                        reason,
                        source_trade_id=trade.trade_id,
                        source_order_id=maker.order_id,
                    )
                return ProcessResult(False, requires_reconciliation=True, reason=reason)
        payload = {
            "id": trade.trade_id,
            "taker_order_id": trade.taker_order_id,
            "market": trade.condition_id,
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
            canceled_at = _as_aware_datetime(event.occurred_at)
            if (
                canceled_at is None
                and _value(payload, "reason") == "gtd_expired_absent_from_open_orders"
            ):
                canceled_at = _as_aware_datetime(_value(payload, "expiration"))
            try:
                order.cancel(canceled_at=canceled_at)
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
            matched_at = _venue_match_time(payload)
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
            if matched_at is None:
                raise OrderReconciliationRequired("managed trade lacks a valid venue match timestamp")

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
                if order.side == "SELL":
                    accepted = [event for event in self.ledger.events()
                                if event.event_type == "order.accepted"
                                and event.payload.get("order_id") == order.order_id]
                    if (
                        size_key != "matched_amount" or status is not TradeStatus.CONFIRMED
                        or str(_value(target_payload, "side", default="")) != "SELL"
                        or str(_value(payload, "side", default="")) != "BUY"
                        or fee_rate_value is None or len(accepted) != 1
                        or accepted[0].payload.get("post_only") is not True
                        or accepted[0].payload.get("side") != "SELL"
                        or accepted[0].payload.get("token_id") != order.token_id
                        or accepted[0].payload.get("condition_id") != str(_value(payload, "market", "condition_id", default=""))
                    ):
                        raise OrderReconciliationRequired(
                            "managed SELL fill lacks confirmed post-only maker evidence"
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
                    matched_at=matched_at,
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
                    matched_at=matched_at,
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
