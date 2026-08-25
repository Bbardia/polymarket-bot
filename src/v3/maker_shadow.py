"""Honest passive-quote diagnostics that never claim simulated fills."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from .math import BookLevel

ZERO = Decimal("0")
ONE = Decimal("1")


@dataclass(frozen=True)
class MakerShadowQuote:
    side: str
    price: Decimal
    size: Decimal
    queue_ahead: Decimal
    expected_probability: Decimal
    best_bid: Decimal
    best_ask: Decimal
    edge: Decimal
    fill_status: str = field(default="unobserved", init=False)
    execution_status: str = field(default="not_submitted", init=False)
    cash_delta: Decimal = field(default=ZERO, init=False)
    inventory_delta: Decimal = field(default=ZERO, init=False)

    @property
    def expected_edge(self) -> Decimal:
        return self.edge

    @property
    def paper_tradeable(self) -> bool:
        return self.edge > ZERO


def propose_buy_quote(
    *,
    bids: tuple[BookLevel, ...],
    asks: tuple[BookLevel, ...],
    tick_size: Decimal,
    size: Decimal,
    expected_probability: Decimal,
) -> MakerShadowQuote:
    """Join or improve the bid without crossing the best ask."""

    if not bids or not asks:
        raise ValueError("maker shadow requires a two-sided book")
    if tick_size <= ZERO:
        raise ValueError("tick size must be positive")
    if size <= ZERO:
        raise ValueError("quote size must be positive")
    if not (ZERO <= expected_probability <= ONE):
        raise ValueError("expected probability must be in [0, 1]")

    best_bid = max(level.price for level in bids)
    best_ask = min(level.price for level in asks)
    if not (ZERO < best_bid < best_ask < ONE):
        raise ValueError("maker shadow requires a valid uncrossed book")

    improved = best_bid + tick_size
    price = improved if improved < best_ask else best_bid
    queue_ahead = sum(
        (level.size for level in bids if level.price == price),
        ZERO,
    )
    return MakerShadowQuote(
        side="BUY",
        price=price,
        size=size,
        queue_ahead=queue_ahead,
        expected_probability=expected_probability,
        best_bid=best_bid,
        best_ask=best_ask,
        edge=expected_probability - price,
    )
