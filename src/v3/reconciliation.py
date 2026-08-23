"""Read-only local-vs-remote reconciliation models and comparison."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

ZERO = Decimal("0")


@dataclass(frozen=True)
class RemotePosition:
    condition_id: str
    token_id: str
    size: Decimal
    current_value: Decimal


@dataclass(frozen=True)
class RemoteOrder:
    order_id: str
    condition_id: str
    token_id: str


@dataclass(frozen=True)
class LocalSnapshot:
    cash: Decimal
    position_tokens: frozenset[str]
    order_ids: frozenset[str]


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
        safe = (
            abs(cash_delta) <= self.cash_tolerance
            and not unknown_positions
            and not unknown_orders
        )
        return ReconciliationReport(
            safe_to_trade=safe,
            cash_delta=cash_delta,
            unknown_positions=unknown_positions,
            external_positions=external,
            unknown_orders=unknown_orders,
            actions=(),
        )
