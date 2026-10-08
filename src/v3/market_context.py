"""Fail-closed extraction of live market constraints from unified SDK models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class MarketContext:
    condition_id: str
    condition_matches: bool
    token_matches: bool
    tick_size: Decimal
    min_order_size: Decimal
    fee_rate: Decimal | None
    accepting_orders: bool
    rules_verified: bool
    disputed: bool
    negative_risk: bool
    resolution_source: str | None
    token_id: str | None = None
    book_timestamp: datetime | None = None
    book_hash: str | None = None
    fees_enabled: bool | None = None
    fee_exponent: Decimal | None = None
    taker_only: bool | None = None
    # Local read time of the order book. The book's own timestamp is its last
    # change; freshness of a quiet book is bounded by when it was read.
    fetched_at: datetime | None = None
    # The exact book snapshot this context was built from, so callers never
    # refetch (and race) a different book than the one verified here.
    book: Any = field(default=None, compare=False, repr=False)

    @classmethod
    def from_sdk(cls, market: Any, book: Any) -> "MarketContext":
        market_condition = str(market.condition_id or "")
        book_condition = str(book.condition_id or "")
        condition_matches = bool(market_condition) and market_condition == book_condition
        outcomes = getattr(market, "outcomes", None)
        outcome_token_ids = {
            str(getattr(getattr(outcomes, outcome, None), "token_id", "") or "")
            for outcome in ("yes", "no")
        }
        book_token_id = str(getattr(book, "token_id", "") or "")
        token_matches = bool(book_token_id) and book_token_id in outcome_token_ids

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

        if (
            not tick_size.is_finite() or tick_size <= 0
            or not min_order_size.is_finite() or min_order_size <= 0
        ):
            raise ValueError("market tick size and minimum order size must be finite and positive")

        raw_fees_enabled = getattr(market.trading, "fees_enabled", None)
        fees_enabled = raw_fees_enabled if type(raw_fees_enabled) is bool else None
        fee_schedule = getattr(market.trading, "fee_schedule", None)
        fee_exponent: Decimal | None = None
        taker_only: bool | None = None
        if fees_enabled is False:
            fee_rate: Decimal | None = Decimal("0")
        elif fees_enabled is not True or fee_schedule is None:
            fee_rate = None
        else:
            fee_rate = Decimal(str(fee_schedule.rate))
            try:
                fee_exponent = Decimal(str(fee_schedule.exponent))
            except (AttributeError, ArithmeticError, TypeError, ValueError):
                fee_exponent = None
            raw_taker_only = getattr(fee_schedule, "taker_only", None)
            taker_only = raw_taker_only if type(raw_taker_only) is bool else None
            if not fee_rate.is_finite() or fee_rate < 0:
                raise ValueError("market fee rate must be finite and nonnegative")
            if fee_exponent is not None and (
                not fee_exponent.is_finite() or fee_exponent < 0
            ):
                raise ValueError("market fee exponent must be finite and nonnegative")

        fee_curve_supported = (
            fee_rate == Decimal("0")
            or fee_exponent == Decimal("1")
        )

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
            and token_matches
            and getattr(market, "question", None)
            and resolution_source
            and not disputed
        )
        accepting_orders = bool(
            condition_matches
            and token_matches
            and state_accepting
            and fee_rate is not None
            and fee_curve_supported
            and not disputed
        )
        return cls(
            condition_id=market_condition,
            condition_matches=condition_matches,
            token_matches=token_matches,
            tick_size=tick_size,
            min_order_size=min_order_size,
            fee_rate=fee_rate,
            accepting_orders=accepting_orders,
            rules_verified=rules_verified,
            disputed=disputed,
            negative_risk=bool(getattr(state, "neg_risk", False) or getattr(book, "neg_risk", False)),
            resolution_source=str(resolution_source) if resolution_source else None,
            token_id=str(getattr(book, "token_id", "")) or None,
            book_timestamp=getattr(book, "timestamp", None),
            book_hash=str(getattr(book, "hash", "")) or None,
            fees_enabled=fees_enabled,
            fee_exponent=fee_exponent,
            taker_only=taker_only,
        )
