"""Read-only stream normalization, replay, and reconnect supervision."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from types import SimpleNamespace
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
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
CORRECTED_DUPLICATE_COST_TOLERANCE = Decimal("0.01")


def _corrected_duplicate_resolution_reconciler(state: Mapping[str, Any]) -> Reconciler:
    external_ids = state.get("external_condition_ids")
    if (not isinstance(external_ids, list)
            or any(not isinstance(item, str) or not item for item in external_ids)
            or len(set(external_ids)) != len(external_ids)):
        raise ValueError("persisted external-condition baseline is invalid")
    return Reconciler(
        external_condition_ids=external_ids,
        cash_tolerance=ZERO,
        cost_tolerance=CORRECTED_DUPLICATE_COST_TOLERANCE,
        allow_cash_inflows=False,
    )


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


def _local_snapshot_payload(local: LocalSnapshot) -> dict[str, Any]:
    return {
        "cash": str(local.cash),
        "position_tokens": sorted(local.position_tokens),
        "order_ids": sorted(local.order_ids),
        "position_quantities": None if local.position_quantities is None else {
            token: str(value) for token, value in sorted(local.position_quantities.items())
        },
        "position_cost_basis": None if local.position_cost_basis is None else {
            token: str(value) for token, value in sorted(local.position_cost_basis.items())
        },
    }


def _local_snapshot_sha256(local: LocalSnapshot) -> str:
    return hashlib.sha256(_stable_json(_local_snapshot_payload(local)).encode("utf-8")).hexdigest()


def remote_snapshot_sha256(remote: RemoteSnapshot) -> str:
    """Fingerprint the exact authenticated account snapshot used for recovery."""
    return hashlib.sha256(_stable_json(_remote_snapshot_payload(remote)).encode("utf-8")).hexdigest()



def _duplicate_chronology_groups(
    prior: tuple[LedgerEvent, ...], latch_index: int,
) -> list[dict[str, Any]]:
    """Derive every duplicate confirmed trade group before the exact latch."""
    groups: dict[str, list[tuple[int, LedgerEvent]]] = {}
    for index, row in enumerate(prior):
        if row.event_type == "user.trade" and row.payload.get("status") == "CONFIRMED":
            trade_id = row.payload.get("id")
            if not isinstance(trade_id, str) or not trade_id:
                raise ValueError("confirmed trade lacks stable ID")
            groups.setdefault(trade_id, []).append((index, row))
    result = []
    for trade_id, rows in groups.items():
        if len(rows) < 2:
            continue
        if len(rows) != 2:
            raise ValueError("duplicate import must contain exactly one original and one later copy")
        if rows[-1][0] >= latch_index:
            raise ValueError("duplicate follows chronology latch")
        canonical = dict(rows[0][1].payload)
        market = canonical.pop("market", None)
        for _, row in rows:
            payload = dict(row.payload)
            row_market = payload.pop("market", None)
            if (payload != canonical or _venue_match_time(row.payload) is None
                    or (market is not None and row_market is not None and market != row_market)):
                raise ValueError("duplicate rows conflict or lack venue match time")
            market = market or row_market
        makers = rows[0][1].payload.get("maker_orders")
        if not isinstance(market, str) or not market or not isinstance(makers, (list, tuple)) or not makers:
            raise ValueError("duplicate market or maker association is missing")
        managed = []
        for maker in makers:
            if not isinstance(maker, Mapping):
                raise ValueError("malformed maker association")
            order_id = maker.get("order_id", maker.get("id"))
            if not isinstance(order_id, str) or not order_id:
                raise ValueError("maker has no order ID")
            accepted = [(i, row) for i, row in enumerate(prior) if row.event_type == "order.accepted"
                        and row.payload.get("order_id") == order_id]
            if accepted:
                if (len(accepted) != 1 or accepted[0][0] >= rows[0][0]
                        or accepted[0][1].payload.get("condition_id") != market
                        or accepted[0][1].payload.get("token_id") != maker.get("asset_id")
                        or accepted[0][1].payload.get("side") != maker.get("side")
                        or maker.get("side") not in ("BUY", "SELL")):
                    raise ValueError("managed maker identity or chronology conflicts")
                managed.append(order_id)
        taker = rows[0][1].payload.get("taker_order_id")
        if (len(managed) != 1 or len(set(managed)) != 1
                or any(row.event_type == "order.accepted" and row.payload.get("order_id") == taker
                       for row in prior)):
            raise ValueError("duplicate has ambiguous managed order maker or managed taker")
        result.append({"trade_id": trade_id, "duplicate_event_ids": [row.event_id for _, row in rows],
                       "first_ledger_index": rows[0][0], "condition_id": market,
                       "managed_order_id": managed[0]})
    if not result:
        raise ValueError("no duplicate confirmed trade groups precede latch")
    # One unscoped latch may be attributed to this import only when the later
    # copies form the entire immediate event suffix before that latch. Older,
    # unrelated duplicate rows cannot be used as evidence for this batch.
    reimport_ids = [group["duplicate_event_ids"][-1] for group in result]
    suffix = prior[latch_index - len(result):latch_index]
    if (len(suffix) != len(result) or [row.event_id for row in suffix] != reimport_ids
            or any(row.event_type != "user.trade" for row in suffix)):
        raise ValueError("duplicate reimport batch is not immediately before latch")
    return result


def _valid_batch_history_proof(
    proof: Any, groups: list[dict[str, Any]], persisted_state: Mapping[str, Any],
    snapshot_at: datetime, resolved_at: datetime,
) -> bool:
    """Bind the trusted caller's exhausted histories to the exact batch and baseline."""
    if not isinstance(proof, Mapping) or set(proof) != {
        "after", "trade_ids", "trade_max_items", "trade_page_limit", "trade_fetched_at",
        "flow_after", "flow_count", "flow_max_items", "flow_page_size", "flow_fetched_at",
    }:
        return False
    if (type(persisted_state.get("baseline_epoch")) is not int
            or type(proof["after"]) is not int or proof["after"] != persisted_state["baseline_epoch"]
            or type(proof["flow_after"]) is not int or proof["flow_after"] != proof["after"]
            or type(proof["flow_count"]) is not int or proof["flow_count"] != 0
            or not isinstance(proof["trade_ids"], list)
            or proof["trade_ids"] != sorted(group["trade_id"] for group in groups)):
        return False
    for field in ("trade_max_items", "trade_page_limit", "flow_max_items", "flow_page_size"):
        if type(proof[field]) is not int or proof[field] <= 0:
            return False
    if len(proof["trade_ids"]) >= proof["trade_max_items"]:
        return False
    try:
        trade_at = datetime.fromisoformat(proof["trade_fetched_at"])
        flow_at = datetime.fromisoformat(proof["flow_fetched_at"])
        if any(at.tzinfo is None or at.utcoffset() is None for at in (trade_at, flow_at)):
            return False
        return all(timedelta(0) <= resolved_at - at <= timedelta(seconds=120)
                   and at <= snapshot_at for at in (trade_at, flow_at))
    except (TypeError, ValueError):
        return False


def _validated_duplicate_batch_replay_resolution(
    event: LedgerEvent, events: tuple[LedgerEvent, ...], resolved: set[str], ledger: EventLedger,
) -> frozenset[str]:
    evidence = event.payload.get("evidence")
    ids = event.payload.get("resolved_event_ids")
    if not isinstance(evidence, Mapping) or type(evidence.get("version")) is not int or evidence["version"] != 1 or not isinstance(ids, list) or len(ids) != 1:
        return frozenset()
    index = next((i for i, row in enumerate(events) if row.event_id == event.event_id), -1)
    if index < 0:
        return frozenset()
    prior = events[:index]
    blockers = [row for row in prior if row.event_type == "stream.reconciliation_required" and row.event_id not in resolved]
    if (len(blockers) != 1 or ids != [blockers[0].event_id]
            or blockers[0].payload.get("reason") != "confirmed fill chronology or ledger association is invalid"):
        return frozenset()
    latch_index = next(i for i, row in enumerate(prior) if row.event_id == ids[0])
    try:
        if evidence.get("groups") != _duplicate_chronology_groups(prior, latch_index):
            return frozenset()
        state = json.loads((ledger.path.parent / "live_state.json").read_text(encoding="utf-8"))
        if not isinstance(state, Mapping) or not _valid_batch_history_proof(
            evidence.get("history_proof"), evidence["groups"], state,
            datetime.fromisoformat(evidence["account_snapshot_fetched_at"]),
            datetime.fromisoformat(event.occurred_at),
        ):
            return frozenset()
        # Reuse the existing snapshot, persisted-policy and pre-resolution local
        # replay checks, but bind them to each group rather than one chosen ID.
        for group in evidence["groups"]:
            single = LedgerEvent(event.event_id, event.event_type, event.occurred_at, {
                "resolved_event_ids": ids, "evidence": {**evidence,
                    "trade_id": group["trade_id"], "duplicate_event_ids": group["duplicate_event_ids"],
                    "duplicate_rows": len(group["duplicate_event_ids"])} })
            if _validated_duplicate_replay_resolution(single, events, resolved, ledger, batch_group=group) != frozenset(ids):
                return frozenset()
        from .live_accounting import confirmed_fills
        replay_ledger = EventLedger(":memory:")
        for row in prior:
            replay_ledger.append(row)
        replay_ledger.path = ledger.path
        fills = confirmed_fills(StreamEventProcessor(replay_ledger))
        if any(len([fill for fill in fills if fill.trade_id == group["trade_id"]]) != 1
               or next(fill.ledger_index for fill in fills if fill.trade_id == group["trade_id"]) != group["first_ledger_index"]
               or next(fill.order_id for fill in fills if fill.trade_id == group["trade_id"]) != group["managed_order_id"]
               for group in evidence["groups"]):
            return frozenset()
    except (ArithmeticError, KeyError, OSError, TypeError, ValueError):
        return frozenset()
    return frozenset(ids)


def _validated_duplicate_replay_resolution(
    event: LedgerEvent, events: tuple[LedgerEvent, ...], resolved: set[str], ledger: EventLedger,
    *, batch_group: Mapping[str, Any] | None = None,
) -> frozenset[str]:
    """Validate a durable resolution for identical duplicate confirmed trades."""
    payload = event.payload
    evidence = payload.get("evidence")
    ids = payload.get("resolved_event_ids")
    if (not isinstance(evidence, Mapping) or not isinstance(ids, list) or len(ids) != 1
            or any(not isinstance(item, str) or not item for item in ids)):
        return frozenset()
    idx = next((i for i, row in enumerate(events) if row.event_id == event.event_id), -1)
    if idx < 0:
        return frozenset()
    prior = events[:idx]
    target = next((row for row in prior if row.event_id == ids[0]), None)
    if target is None or target.event_type != "stream.reconciliation_required" or target.payload.get("reason") != "confirmed fill chronology or ledger association is invalid":
        return frozenset()
    target_index = next(i for i, row in enumerate(prior) if row.event_id == target.event_id)
    try:
        # A one-trade proof is never sufficient for an unscoped multi-trade import latch.
        eligible_groups = _duplicate_chronology_groups(prior, target_index)
    except (TypeError, ValueError):
        return frozenset()
    if batch_group is None and len(eligible_groups) != 1:
        return frozenset()
    unresolved = [row for row in prior if row.event_type == "stream.reconciliation_required" and row.event_id not in resolved]
    if unresolved != [target]:
        return frozenset()
    trade_id = evidence.get("trade_id")
    if not isinstance(trade_id, str) or not trade_id:
        return frozenset()
    trades = [row for row in prior if row.event_type == "user.trade"
              and row.payload.get("id") == trade_id and row.payload.get("status") == "CONFIRMED"]
    duplicate_ids = evidence.get("duplicate_event_ids")
    if (type(evidence.get("duplicate_rows")) is not int or evidence["duplicate_rows"] != len(trades)
            or len(trades) < 2 or not isinstance(duplicate_ids, list)
            or duplicate_ids != [row.event_id for row in trades]):
        return frozenset()
    if max(i for i, row in enumerate(prior) if row.event_id in set(duplicate_ids)) >= target_index:
        return frozenset()
    canonical = dict(trades[0].payload)
    canonical_market = canonical.pop("market", None)
    if _venue_match_time(trades[0].payload) is None:
        return frozenset()
    for row in trades[1:]:
        duplicate = dict(row.payload)
        duplicate_market = duplicate.pop("market", None)
        if (duplicate != canonical or _venue_match_time(row.payload) is None
                or (canonical_market is not None and duplicate_market is not None and canonical_market != duplicate_market)):
            return frozenset()
        canonical_market = canonical_market or duplicate_market
    if not isinstance(canonical_market, str) or not canonical_market:
        return frozenset()
    makers = trades[0].payload.get("maker_orders")
    if not isinstance(makers, (list, tuple)):
        return frozenset()
    if batch_group is not None:
        if (batch_group.get("condition_id") != canonical_market
                or batch_group.get("first_ledger_index") != next(i for i, row in enumerate(prior) if row.event_id == trades[0].event_id)
                or batch_group.get("managed_order_id") not in [maker.get("order_id", maker.get("id")) for maker in makers if isinstance(maker, Mapping)]):
            return frozenset()
    else:
        if len(makers) != 1 or not isinstance(makers[0], Mapping):
            return frozenset()
        maker = makers[0]
        order_id = maker.get("order_id", maker.get("id"))
        if not isinstance(order_id, str):
            return frozenset()
        accepted = [row for row in prior if row.event_type == "order.accepted" and row.payload.get("order_id") == order_id]
        trade_index = next(i for i, row in enumerate(prior) if row.event_id == trades[0].event_id)
        accepted_index = next((i for i, row in enumerate(prior) if row.event_id in {item.event_id for item in accepted}), -1)
        taker_id = trades[0].payload.get("taker_order_id")
        if (len(accepted) != 1 or accepted_index < 0 or accepted_index >= trade_index
                or accepted[0].payload.get("condition_id") != canonical_market
                or accepted[0].payload.get("token_id") != maker.get("asset_id")
                or accepted[0].payload.get("side") != maker.get("side")
                or trades[0].payload.get("asset_id") != maker.get("asset_id")
                or trades[0].payload.get("side") == maker.get("side")
                or any(row.event_type == "order.accepted" and row.payload.get("order_id") == taker_id for row in prior)):
            return frozenset()
    snapshot = evidence.get("account_snapshot")
    digest = evidence.get("account_snapshot_sha256")
    fetched_raw = evidence.get("account_snapshot_fetched_at")
    if not isinstance(fetched_raw, str) or not isinstance(event.occurred_at, str):
        return frozenset()
    try:
        if not isinstance(snapshot, Mapping) or set(snapshot) != {"cash", "positions", "open_orders"}:
            return frozenset()
        if snapshot.get("open_orders") != [] or not isinstance(snapshot.get("positions"), list):
            return frozenset()
        cash = Decimal(str(snapshot["cash"]))
        if not cash.is_finite() or cash < ZERO:
            return frozenset()
        positions = []
        for raw in snapshot["positions"]:
            if not isinstance(raw, Mapping) or set(raw) != {"condition_id", "token_id", "size", "current_value", "initial_value", "redeemable"}:
                return frozenset()
            size = Decimal(str(raw["size"]))
            current = Decimal(str(raw["current_value"]))
            initial = None if raw["initial_value"] is None else Decimal(str(raw["initial_value"]))
            if (not isinstance(raw["condition_id"], str) or not raw["condition_id"]
                    or not isinstance(raw["token_id"], str) or not raw["token_id"]
                    or not size.is_finite() or size < ZERO or not current.is_finite() or current < ZERO
                    or (initial is not None and (not initial.is_finite() or initial < ZERO))
                    or type(raw["redeemable"]) is not bool):
                return frozenset()
            positions.append(RemotePosition(raw["condition_id"], raw["token_id"], size, current, initial, raw["redeemable"]))
        if not isinstance(digest, str) or remote_snapshot_sha256(RemoteSnapshot(cash, tuple(positions), ())) != digest:
            return frozenset()
        snapshot_at = datetime.fromisoformat(fetched_raw)
        resolved_at = datetime.fromisoformat(event.occurred_at)
        # This is durable historical resolution evidence: snapshot freshness is
        # measured at resolution time. Every live cycle independently fetches
        # and reconciles a fresh account snapshot before it may submit orders.
        if (snapshot_at.tzinfo is None or snapshot_at.utcoffset() is None or resolved_at.tzinfo is None
                or resolved_at.utcoffset() is None or not timedelta(0) <= resolved_at - snapshot_at <= timedelta(seconds=120)):
            return frozenset()
        cash_delta = Decimal(str(evidence.get("cash_delta")))
        if not cash_delta.is_finite() or cash_delta != ZERO or evidence.get("account_reconciliation_safe") is not True:
            return frozenset()
        baseline_cash = Decimal(str(evidence.get("baseline_cash")))
        persisted_state = json.loads((ledger.path.parent / "live_state.json").read_text(encoding="utf-8"))
        if not isinstance(persisted_state, Mapping):
            return frozenset()
        expected_reconciler = _corrected_duplicate_resolution_reconciler(persisted_state)
        if (Decimal(str(persisted_state.get("baseline_cash"))) != baseline_cash
                or not baseline_cash.is_finite() or baseline_cash < ZERO):
            return frozenset()
        cash_tolerance = Decimal(str(evidence.get("cash_tolerance")))
        cost_tolerance = Decimal(str(evidence.get("cost_tolerance")))
        external_ids = evidence.get("external_condition_ids")
        allow_cash_inflows = evidence.get("allow_cash_inflows")
        local_digest = evidence.get("local_snapshot_sha256")
        if (cash_tolerance != expected_reconciler.cash_tolerance
                or cost_tolerance != expected_reconciler.cost_tolerance
                or external_ids != sorted(expected_reconciler.external_condition_ids)
                or allow_cash_inflows is not expected_reconciler.allow_cash_inflows
                or not isinstance(local_digest, str) or len(local_digest) != 64):
            return frozenset()
        replay_ledger = EventLedger(":memory:")
        for prior_event in prior:
            replay_ledger.append(prior_event)
        replay_ledger.path = ledger.path
        replay_processor = StreamEventProcessor(replay_ledger)
        from .live_runner import local_snapshot
        replay_local = local_snapshot(replay_processor, baseline_cash)
        if _local_snapshot_sha256(replay_local) != local_digest:
            return frozenset()
        replay_report = expected_reconciler.compare(replay_local, RemoteSnapshot(cash, tuple(positions), ()))
        if not replay_report.safe_to_trade or replay_report.cash_delta != ZERO:
            return frozenset()
    except (ArithmeticError, KeyError, OSError, TypeError, ValueError):
        return frozenset()
    return frozenset({target.event_id})


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
            elif valid_id and (not latched or self._is_local_abort(event)) and self._valid_submission_terminal(event):
                # A local abort provably sent nothing that can rest at the venue,
                # so it closes its own submission even after an earlier latch.
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
    def _is_local_abort(event: LedgerEvent) -> bool:
        return event.event_type == "order.rejected" and event.payload.get("code") == "local_abort"

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
        if not persisted_trades:
            return frozenset()
        persisted = persisted_trades[0]
        canonical = dict(persisted.payload)
        canonical_market = canonical.pop("market", None)
        for duplicate in persisted_trades[1:]:
            duplicate_payload = dict(duplicate.payload)
            duplicate_market = duplicate_payload.pop("market", None)
            if (
                duplicate_payload != canonical
                or (canonical_market is not None and duplicate_market is not None
                    and canonical_market != duplicate_market)
            ):
                return frozenset()
            if canonical_market is None and duplicate_market is not None:
                canonical_market = duplicate_market
        if canonical_market not in (None, evidence.get("condition_id")):
            return frozenset()
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
            if payload.get("resolution") == "corrected_duplicate_trade_replay":
                resolved.update(_validated_duplicate_replay_resolution(event, events, resolved, self.ledger))
                continue
            if payload.get("resolution") == "corrected_duplicate_batch_replay_v1":
                resolved.update(_validated_duplicate_batch_replay_resolution(event, events, resolved, self.ledger))
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

    def require_reconciliation_once(self, reason: str) -> None:
        """Persist an unscoped latch unless the same reason is already unresolved.

        Resolution validators require exactly one unresolved latch, so a
        condition re-detected every cycle must not append a fresh latch each time.
        """
        if reason not in self.reconciliation_reasons:
            self.require_reconciliation(reason)

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

    def resolve_corrected_duplicate_batch_latch(
        self, *, remote: RemoteSnapshot, reconciler: Reconciler,
        account_snapshot_fetched_at: datetime, history_proof: Mapping[str, Any],
    ) -> ProcessResult:
        """Append a v1 all-groups proof; caller must supply an authenticated account read."""
        if not isinstance(remote, RemoteSnapshot) or remote.open_orders:
            raise ValueError("fresh full account snapshot without open orders is required")
        try:
            state = json.loads((self.ledger.path.parent / "live_state.json").read_text(encoding="utf-8"))
            baseline = Decimal(str(state["baseline_cash"]))
            expected = _corrected_duplicate_resolution_reconciler(state)
        except (OSError, KeyError, TypeError, ValueError, ArithmeticError) as exc:
            raise ValueError("persisted session baseline is unavailable") from exc
        if (not baseline.is_finite() or baseline < ZERO or not isinstance(reconciler, Reconciler)
                or reconciler.external_condition_ids != expected.external_condition_ids
                or reconciler.cash_tolerance != expected.cash_tolerance
                or reconciler.cost_tolerance != expected.cost_tolerance
                or reconciler.allow_cash_inflows != expected.allow_cash_inflows):
            raise ValueError("resolver policy differs from persisted strict policy")
        if (not isinstance(account_snapshot_fetched_at, datetime)
                or account_snapshot_fetched_at.tzinfo is None
                or account_snapshot_fetched_at.utcoffset() is None):
            raise ValueError("timestamped account snapshot is required")
        fetched_at = account_snapshot_fetched_at.astimezone(timezone.utc)
        if not timedelta(0) <= datetime.now(timezone.utc) - fetched_at <= timedelta(seconds=120):
            raise ValueError("account snapshot is stale")
        events = tuple(self.ledger.events())
        blockers = [row for row in events if row.event_type == "stream.reconciliation_required"
                    and row.event_id not in self._resolved_reconciliation_event_ids]
        if (len(blockers) != 1 or blockers[0].payload.get("reason") !=
                "confirmed fill chronology or ledger association is invalid"):
            raise ValueError("exactly one chronology latch is required")
        latch_index = next(i for i, row in enumerate(events) if row.event_id == blockers[0].event_id)
        groups = _duplicate_chronology_groups(events, latch_index)
        if not _valid_batch_history_proof(history_proof, groups, state, fetched_at, datetime.now(timezone.utc)):
            raise ValueError("complete trade/cash-flow history proof is missing or stale")
        from .live_accounting import confirmed_fills
        fills = confirmed_fills(self)
        if any(len(matches := [fill for fill in fills if fill.trade_id == group["trade_id"]]) != 1
               or matches[0].ledger_index != group["first_ledger_index"]
               or matches[0].order_id != group["managed_order_id"] for group in groups):
            raise ValueError("duplicate groups do not have exact first-row managed fills")
        from .live_runner import local_snapshot
        local = local_snapshot(self, baseline)
        report = expected.compare(local, remote)
        if not report.safe_to_trade or report.cash_delta != ZERO:
            raise ValueError("account reconciliation is unsafe or cash differs")
        candidate = LedgerEvent.create("stream.reconciliation_resolved", {
            "resolution": "corrected_duplicate_batch_replay_v1",
            "resolved_event_ids": [blockers[0].event_id],
            "evidence": {"version": 1, "groups": groups, "history_proof": dict(history_proof),
                         "account_reconciliation_safe": True,
                         "cash_delta": str(report.cash_delta),
                         "account_snapshot": _remote_snapshot_payload(remote),
                         "account_snapshot_sha256": remote_snapshot_sha256(remote),
                         "account_snapshot_fetched_at": fetched_at.isoformat(),
                         "baseline_cash": str(baseline),
                         "local_snapshot_sha256": _local_snapshot_sha256(local),
                         "cash_tolerance": str(expected.cash_tolerance),
                         "cost_tolerance": str(expected.cost_tolerance),
                         "allow_cash_inflows": expected.allow_cash_inflows,
                         "external_condition_ids": sorted(expected.external_condition_ids)},
        })
        if _validated_duplicate_batch_replay_resolution(candidate, events + (candidate,),
                set(self._resolved_reconciliation_event_ids), self.ledger) != frozenset({blockers[0].event_id}):
            raise ValueError("candidate batch proof failed durable replay validation")
        self.ledger.append_if_unchanged(candidate, event_count=len(events), last_event_id=events[-1].event_id)
        return ProcessResult(True, reason="appended audited duplicate-batch chronology resolution")

    def _resolve_corrected_duplicate_trade_latch(
        self, *, remote: RemoteSnapshot, reconciler: Reconciler,
        trade_id: str, account_snapshot_fetched_at: datetime,
    ) -> ProcessResult:
        """Resolve a chronology latch using fresh account evidence and a ledger-rebuilt local snapshot."""
        if not isinstance(remote, RemoteSnapshot):
            raise ValueError("fresh account snapshot is required")
        try:
            state = json.loads((self.ledger.path.parent / "live_state.json").read_text(encoding="utf-8"))
            if not isinstance(state, Mapping):
                raise ValueError("persisted live state must be an object")
            baseline_cash = Decimal(str(state["baseline_cash"]))
            expected_reconciler = _corrected_duplicate_resolution_reconciler(state)
        except (OSError, KeyError, TypeError, ValueError, ArithmeticError) as exc:
            raise ValueError("persisted session reconciliation baseline is unavailable or invalid") from exc
        if not baseline_cash.is_finite() or baseline_cash < ZERO:
            raise ValueError("persisted session cash baseline must be finite and non-negative")
        if (not isinstance(reconciler, Reconciler)
                or reconciler.external_condition_ids != expected_reconciler.external_condition_ids
                or reconciler.cash_tolerance != expected_reconciler.cash_tolerance
                or reconciler.cost_tolerance != expected_reconciler.cost_tolerance
                or reconciler.allow_cash_inflows != expected_reconciler.allow_cash_inflows):
            raise ValueError("resolver reconciliation policy must match the persisted fail-closed policy")
        reconciler = expected_reconciler
        if (not isinstance(account_snapshot_fetched_at, datetime) or account_snapshot_fetched_at.tzinfo is None
                or account_snapshot_fetched_at.utcoffset() is None):
            raise ValueError("timestamped fresh account snapshot is required")
        fetched_at = account_snapshot_fetched_at.astimezone(timezone.utc)
        age = (datetime.now(timezone.utc) - fetched_at).total_seconds()
        if age < 0 or age > 120:
            raise ValueError("account snapshot is not fresh")
        if remote.open_orders:
            raise ValueError("account snapshot has open orders")
        from .live_runner import local_snapshot
        local = local_snapshot(self, baseline_cash)
        report = reconciler.compare(local, remote)
        if not report.safe_to_trade or report.cash_delta != ZERO:
            raise ValueError("account reconciliation is unsafe or cash does not match exactly")
        events = tuple(self.ledger.events())
        latches = [e for e in events if e.event_type == "stream.reconciliation_required" and e.event_id not in self._resolved_reconciliation_event_ids]
        if len(latches) != 1 or latches[0].payload.get("reason") != "confirmed fill chronology or ledger association is invalid":
            raise ValueError("only the single corrected duplicate-replay chronology latch is eligible")
        trade_rows = [e for e in events if e.event_type == "user.trade" and e.payload.get("id") == trade_id and e.payload.get("status") == "CONFIRMED"]
        if len(trade_rows) < 2:
            raise ValueError("confirmed duplicate trade rows are required")
        latch_index = next(i for i, e in enumerate(events) if e.event_id == latches[0].event_id)
        groups = _duplicate_chronology_groups(events, latch_index)
        if len(groups) != 1 or groups[0]["trade_id"] != trade_id:
            raise ValueError("single-trade resolution cannot clear a batch chronology latch")
        if max(i for i, e in enumerate(events) if e.event_id in {row.event_id for row in trade_rows}) >= latch_index:
            raise ValueError("duplicate trade rows must precede the chronology latch")
        canonical = dict(trade_rows[0].payload)
        market = canonical.pop("market", None)
        for row in trade_rows[1:]:
            duplicate = dict(row.payload)
            duplicate_market = duplicate.pop("market", None)
            if canonical != duplicate or (market is not None and duplicate_market is not None and market != duplicate_market):
                raise ValueError("duplicate trade rows conflict")
            market = market or duplicate_market
        makers = trade_rows[0].payload.get("maker_orders")
        if not isinstance(makers, (list, tuple)) or len(makers) != 1 or not isinstance(makers[0], Mapping):
            raise ValueError("duplicate trade must have exactly one explicit maker association")
        maker = makers[0]
        order_id = maker.get("order_id", maker.get("id"))
        if not isinstance(order_id, str) or order_id not in self.orders:
            raise ValueError("duplicate trade maker must bind to one managed order")
        taker_id = trade_rows[0].payload.get("taker_order_id")
        if isinstance(taker_id, str) and taker_id in self.orders:
            raise ValueError("taker-side managed trades are not eligible for this resolution")
        accepted = [e for e in events if e.event_type == "order.accepted" and e.payload.get("order_id") == order_id]
        accepted_index = next((i for i, event in enumerate(events) if accepted and event.event_id == accepted[0].event_id), -1)
        first_trade_index = min(i for i, event in enumerate(events) if event.event_id in {row.event_id for row in trade_rows})
        if (len(accepted) != 1 or accepted_index < 0 or accepted_index >= first_trade_index
                or accepted[0].payload.get("condition_id") != market
                or accepted[0].payload.get("token_id") != maker.get("asset_id")
                or accepted[0].payload.get("side") != maker.get("side")
                or trade_rows[0].payload.get("asset_id") != maker.get("asset_id")
                or trade_rows[0].payload.get("side") == maker.get("side")):
            raise ValueError("duplicate trade identity does not match its unique managed maker order")
        if any(_venue_match_time(row.payload) is None for row in trade_rows):
            raise ValueError("duplicate trade rows require explicit venue match time")
        from .live_accounting import confirmed_fills
        fills = confirmed_fills(self)
        if len([fill for fill in fills if fill.trade_id == trade_id]) != 1:
            raise ValueError("corrected trade replay is not uniquely accounted")
        snapshot = _remote_snapshot_payload(remote)
        digest = remote_snapshot_sha256(remote)
        self.ledger.append(LedgerEvent.create("stream.reconciliation_resolved", {
            "resolution": "corrected_duplicate_trade_replay", "resolved_event_ids": [latches[0].event_id],
            "evidence": {"trade_id": trade_id, "duplicate_rows": len(trade_rows),
                         "duplicate_event_ids": [row.event_id for row in trade_rows],
                         "account_reconciliation_safe": True, "cash_delta": str(report.cash_delta),
                         "account_snapshot": snapshot,
                         "account_snapshot_sha256": digest,
                         "account_snapshot_fetched_at": fetched_at.isoformat(),
                         "baseline_cash": str(baseline_cash),
                         "local_snapshot_sha256": _local_snapshot_sha256(local),
                         "cash_tolerance": str(reconciler.cash_tolerance),
                         "cost_tolerance": str(reconciler.cost_tolerance),
                         "allow_cash_inflows": reconciler.allow_cash_inflows,
                         "external_condition_ids": sorted(reconciler.external_condition_ids)},
        }))
        return ProcessResult(True, reason="appended audited duplicate-replay chronology resolution")

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

    def _accepted_post_only(self, order_id: str | None) -> bool:
        accepted = [event for event in self.ledger.events()
                    if event.event_type == "order.accepted"
                    and event.payload.get("order_id") == order_id]
        return len(accepted) == 1 and accepted[0].payload.get("post_only") is True

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
        # A not-yet-confirmed SELL row is skipped without a latch and without
        # recording; the CONFIRMED row is imported later. The maker fee may come
        # from an explicit zero trade fee; an unknown fee on a confirmed row
        # takes the common latch below. The trade's top-level side/token/size/
        # price describe the taker and may legitimately differ (complementary
        # or multi-maker matches).
        sell_makers = [maker for maker in trade.maker_orders
                       if maker.order_id in self.managed_order_ids
                       and self.orders.get(maker.order_id) is not None
                       and self.orders[maker.order_id].side == "SELL"]
        if sell_makers and trade.status != "CONFIRMED":
            return ProcessResult(False, reason="unconfirmed managed SELL fill deferred until CONFIRMED")
        if sell_makers:
            maker = sell_makers[0]
            order = self.orders[maker.order_id]
            maker_fee = next(row["fee_rate_bps"] for row in maker_rows if row["order_id"] == maker.order_id)
            accepted = [event for event in self.ledger.events()
                        if event.event_type == "order.accepted"
                        and event.payload.get("order_id") == maker.order_id]
            valid = (
                len(sell_makers) == 1 and len(accepted) == 1
                and accepted[0].payload.get("post_only") is True
                and accepted[0].payload.get("side") == "SELL"
                and accepted[0].payload.get("token_id") == order.token_id == maker.token_id
                and accepted[0].payload.get("condition_id") == trade.condition_id
                and maker.side == "SELL"
                and trade.trader_side == "MAKER"
                and maker.matched_amount.is_finite() and maker.matched_amount > ZERO
                and maker.matched_amount <= order.requested_size
                and maker.price.is_finite() and ZERO < maker.price < Decimal("1")
                and (maker_fee is None or (maker_fee.is_finite() and maker_fee >= ZERO))
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
                if order.side == "SELL" and status is not TradeStatus.CONFIRMED:
                    # Managed SELL fills are only ever recorded once CONFIRMED;
                    # earlier lifecycle rows are skipped without a latch.
                    continue
                size = Decimal(str(_value(target_payload, size_key)))
                price = Decimal(str(_value(target_payload, "price")))
                if size_key == "matched_amount":
                    # The trade's top-level fee rate is the taker's. Never charge
                    # it to our maker leg: a post-only maker with no reported
                    # maker rate pays no fee; any other maker leg stays unknown.
                    fee_rate_value = (
                        target_payload["fee_rate_bps"] if "fee_rate_bps" in target_payload
                        else "0" if self._accepted_post_only(order.order_id) else None
                    )
                else:
                    fee_rate_value = _value(payload, "fee_rate_bps")
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
                        size_key != "matched_amount"
                        or str(_value(target_payload, "side", default="")) != "SELL"
                        or len(accepted) != 1
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
