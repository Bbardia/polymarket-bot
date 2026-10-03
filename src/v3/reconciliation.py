"""Read-only local-vs-remote reconciliation models and comparison."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import re
from typing import Iterable

ZERO = Decimal("0")
TRADE_STATUSES = frozenset({"MATCHED", "MATCHED_NOT_BROADCASTED", "MINED", "CONFIRMED", "RETRYING", "FAILED"})
TRADE_TRADER_SIDES = frozenset({"TAKER", "MAKER"})
_TRANSACTION_HASH_PATTERN = re.compile(r"0x[0-9a-fA-F]{64}")


@dataclass(frozen=True)
class RemoteTradeMaker:
    order_id: str
    token_id: str
    side: str
    price: Decimal
    matched_amount: Decimal
    fee_rate_bps: Decimal | None = None

    def __post_init__(self) -> None:
        if not all(isinstance(value, str) and value.strip() for value in (self.order_id, self.token_id)):
            raise ValueError("maker order identity is required")
        if self.side not in ("BUY", "SELL"):
            raise ValueError("maker side is invalid")
        _validate_trade_economics(self.price, self.matched_amount, self.fee_rate_bps)


@dataclass(frozen=True)
class RemoteTrade:
    trade_id: str
    condition_id: str
    token_id: str
    taker_order_id: str
    side: str
    trader_side: str
    price: Decimal
    size: Decimal
    status: str
    matched_at: datetime
    updated_at: datetime | None
    fee_rate_bps: Decimal | None
    transaction_hash: str | None
    maker_orders: tuple[RemoteTradeMaker, ...]

    def __post_init__(self) -> None:
        if not all(isinstance(value, str) and value.strip() for value in
                   (self.trade_id, self.condition_id, self.token_id, self.taker_order_id)):
            raise ValueError("trade identity is required")
        if not isinstance(self.status, str) or self.status not in TRADE_STATUSES:
            raise ValueError("status must be a supported SDK trade status")
        if (
            not isinstance(self.side, str)
            or self.side not in ("BUY", "SELL")
            or not isinstance(self.trader_side, str)
            or self.trader_side not in TRADE_TRADER_SIDES
        ):
            raise ValueError("trade side is invalid")
        _validate_trade_economics(self.price, self.size, self.fee_rate_bps)
        if not isinstance(self.matched_at, datetime):
            raise ValueError("matched_at must be a datetime")
        if self.matched_at.tzinfo is None or self.matched_at.utcoffset() is None:
            raise ValueError("matched_at must be timezone-aware")
        if self.updated_at is not None:
            if not isinstance(self.updated_at, datetime):
                raise ValueError("updated_at must be a datetime or None")
            if self.updated_at.tzinfo is None or self.updated_at.utcoffset() is None:
                raise ValueError("updated_at must be timezone-aware")
        if self.transaction_hash is not None and (
            not isinstance(self.transaction_hash, str)
            or _TRANSACTION_HASH_PATTERN.fullmatch(self.transaction_hash) is None
        ):
            raise ValueError("transaction_hash must be 0x-prefixed 32-byte hex or None")
        if not isinstance(self.maker_orders, tuple) or not all(isinstance(x, RemoteTradeMaker) for x in self.maker_orders):
            raise ValueError("maker_orders must be an immutable tuple")


def _validate_trade_economics(price: Decimal, size: Decimal, fee: Decimal | None) -> None:
    if not isinstance(price, Decimal) or not price.is_finite() or price <= ZERO:
        raise ValueError("trade price must be finite and positive")
    if not isinstance(size, Decimal) or not size.is_finite() or size <= ZERO:
        raise ValueError("trade size must be finite and positive")
    if fee is not None and (not isinstance(fee, Decimal) or not fee.is_finite() or fee < ZERO):
        raise ValueError("trade fee must be finite and nonnegative")


@dataclass(frozen=True)
class RemotePosition:
    condition_id: str
    token_id: str
    size: Decimal
    current_value: Decimal
    initial_value: Decimal | None = None


@dataclass(frozen=True)
class RemoteOrder:
    order_id: str
    condition_id: str
    token_id: str
    remaining_notional: Decimal | None = None


@dataclass(frozen=True)
class LocalSnapshot:
    cash: Decimal
    position_tokens: frozenset[str]
    order_ids: frozenset[str]
    # Quantities and acquisition cost (not mark value), keyed by token ID.
    # None means the ledger cannot establish position-level parity; fail closed.
    position_quantities: dict[str, Decimal] | None = None
    position_cost_basis: dict[str, Decimal] | None = None


@dataclass(frozen=True)
class RemoteSnapshot:
    cash: Decimal
    positions: tuple[RemotePosition, ...]
    open_orders: tuple[RemoteOrder, ...]


@dataclass(frozen=True)
class ReconciliationReport:
    safe_to_trade: bool
    cash_delta: Decimal
    unknown_positions: tuple[RemotePosition, ...]
    external_positions: tuple[RemotePosition, ...]
    unknown_orders: tuple[RemoteOrder, ...]
    missing_positions: tuple[str, ...] = ()
    missing_orders: tuple[str, ...] = ()
    incomplete_orders: tuple[str, ...] = ()
    position_mismatches: tuple[str, ...] = ()
    invalid_snapshot: bool = False
    actions: tuple[object, ...] = ()


class Reconciler:
    """Compare state only. It never cancels, sells, merges, or redeems."""

    def __init__(
        self,
        *,
        external_condition_ids: Iterable[str] = (),
        cash_tolerance: Decimal = Decimal("0.01"),
    ) -> None:
        self.external_condition_ids = frozenset(external_condition_ids)
        self.cash_tolerance = cash_tolerance

    def compare(self, local: LocalSnapshot, remote: RemoteSnapshot) -> ReconciliationReport:
        valid_local = (
            isinstance(local, LocalSnapshot)
            and isinstance(local.cash, Decimal)
            and isinstance(local.position_tokens, frozenset)
            and isinstance(local.order_ids, frozenset)
            and (
                local.position_quantities is None
                or isinstance(local.position_quantities, dict)
                and all(isinstance(value, Decimal) for value in local.position_quantities.values())
            )
            and (
                local.position_cost_basis is None
                or isinstance(local.position_cost_basis, dict)
                and all(isinstance(value, Decimal) for value in local.position_cost_basis.values())
            )
        )
        valid_remote = (
            isinstance(remote, RemoteSnapshot)
            and isinstance(remote.cash, Decimal)
            and isinstance(remote.positions, tuple)
            and all(
                isinstance(position, RemotePosition)
                and isinstance(position.size, Decimal)
                and isinstance(position.current_value, Decimal)
                and (position.initial_value is None or isinstance(position.initial_value, Decimal))
                for position in remote.positions
            )
            and isinstance(remote.open_orders, tuple)
            and all(
                isinstance(order, RemoteOrder)
                and (order.remaining_notional is None or isinstance(order.remaining_notional, Decimal))
                for order in remote.open_orders
            )
        )
        if not valid_local or not valid_remote:
            return ReconciliationReport(
                safe_to_trade=False,
                cash_delta=ZERO,
                unknown_positions=(),
                external_positions=(),
                unknown_orders=(),
                invalid_snapshot=True,
                actions=(),
            )
        external = tuple(
            position
            for position in remote.positions
            if position.condition_id in self.external_condition_ids
        )
        unknown_positions = tuple(
            position
            for position in remote.positions
            if position.token_id not in local.position_tokens
            and position.condition_id not in self.external_condition_ids
        )
        unknown_orders = tuple(
            order for order in remote.open_orders if order.order_id not in local.order_ids
        )
        cash_delta = remote.cash - local.cash
        remote_tokens = frozenset(position.token_id for position in remote.positions)
        remote_order_ids = frozenset(order.order_id for order in remote.open_orders)
        duplicate_positions = len(remote_tokens) != len(remote.positions)
        duplicate_orders = len(remote_order_ids) != len(remote.open_orders)
        missing_positions = tuple(sorted(local.position_tokens - remote_tokens))
        local_quantities = local.position_quantities
        local_costs = local.position_cost_basis
        managed_remote_tokens = frozenset(
            position.token_id for position in remote.positions
            if position.condition_id not in self.external_condition_ids
        )
        remote_by_token = {
            position.token_id: position for position in remote.positions
            if position.condition_id not in self.external_condition_ids
        }
        position_mismatches = tuple(sorted(
            token for token in local.position_tokens | managed_remote_tokens
            if local_quantities is None or local_costs is None
            or token not in local_quantities or token not in local_costs
            or token not in remote_by_token
            or not local_quantities[token].is_finite()
            or not local_costs[token].is_finite()
            or local_quantities[token] != remote_by_token[token].size
            or local_costs[token] != remote_by_token[token].initial_value
        ))
        missing_orders = tuple(sorted(local.order_ids - remote_order_ids))
        incomplete_orders = tuple(sorted(
            order.order_id for order in remote.open_orders
            if order.remaining_notional is None
            or not order.remaining_notional.is_finite()
            or order.remaining_notional < ZERO
        ))
        invalid_snapshot = (
            not remote.cash.is_finite()
            or remote.cash < ZERO
            or any(
                not position.condition_id
                or not position.token_id
                or not position.size.is_finite()
                or position.size <= ZERO
                or not position.current_value.is_finite()
                or position.current_value < ZERO
                or position.initial_value is None
                or not position.initial_value.is_finite()
                or position.initial_value < ZERO
                for position in remote.positions
            )
        )
        safe = (
            not invalid_snapshot
            and not duplicate_positions
            and not duplicate_orders
            and abs(cash_delta) <= self.cash_tolerance
            and not unknown_positions
            and not unknown_orders
            and not missing_positions
            and not missing_orders
            and not incomplete_orders
            and not position_mismatches
        )
        return ReconciliationReport(
            safe_to_trade=safe,
            cash_delta=cash_delta,
            unknown_positions=unknown_positions,
            external_positions=external,
            unknown_orders=unknown_orders,
            missing_positions=missing_positions,
            missing_orders=missing_orders,
            incomplete_orders=incomplete_orders,
            position_mismatches=position_mismatches,
            actions=(),
        )
