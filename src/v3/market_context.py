"""Fail-closed extraction of live market constraints from unified SDK models."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class MarketContext:
    condition_id: str
    condition_matches: bool
    tick_size: Decimal
    min_order_size: Decimal
    fee_rate: Decimal | None
    accepting_orders: bool
    rules_verified: bool
    disputed: bool
    negative_risk: bool
    resolution_source: str | None

    @classmethod
    def from_sdk(cls, market: Any, book: Any) -> "MarketContext":
        market_condition = str(market.condition_id or "")
        book_condition = str(book.condition_id or "")
        condition_matches = bool(market_condition) and market_condition == book_condition

        tick_size = Decimal(str(
            book.tick_size
            if getattr(book, "tick_size", None) is not None
            else market.trading.minimum_tick_size
        ))
        min_order_size = Decimal(str(
            book.min_order_size
            if getattr(book, "min_order_size", None) is not None
            else market.trading.minimum_order_size
        ))

        fees_enabled = bool(getattr(market.trading, "fees_enabled", False))
        fee_schedule = getattr(market.trading, "fee_schedule", None)
        if not fees_enabled:
            fee_rate: Decimal | None = Decimal("0")
        elif fee_schedule is None:
            fee_rate = None
        else:
            fee_rate = Decimal(str(fee_schedule.rate))

        resolution_source = getattr(market.resolution, "source", None)
        status = getattr(market.resolution, "uma_resolution_status", None)
        disputed = "DISPUTED" in str(status).upper() if status is not None else False
        state = market.state
        state_accepting = (
            bool(getattr(state, "accepting_orders", False))
            and bool(getattr(state, "active", False))
            and not bool(getattr(state, "closed", False))
            and not bool(getattr(state, "archived", False))
        )
        rules_verified = bool(
            condition_matches
            and getattr(market, "question", None)
            and resolution_source
            and not disputed
        )
        accepting_orders = bool(
            condition_matches
            and state_accepting
            and fee_rate is not None
            and not disputed
        )
        return cls(
            condition_id=market_condition,
            condition_matches=condition_matches,
            tick_size=tick_size,
            min_order_size=min_order_size,
            fee_rate=fee_rate,
            accepting_orders=accepting_orders,
            rules_verified=rules_verified,
            disputed=disputed,
            negative_risk=bool(getattr(state, "neg_risk", False) or getattr(book, "neg_risk", False)),
            resolution_source=str(resolution_source) if resolution_source else None,
        )
