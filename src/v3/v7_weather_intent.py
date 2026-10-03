"""Non-submitting proposal bridge from V7 weather evaluations to passive intent data.

This module deliberately returns a proposal record, not ``OrderIntent`` and has
no API/client dependency. It is not a live authorization or fill model.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from math import ceil
from typing import Any

from .market_context import MarketContext
from .math import is_tick_aligned

ZERO = Decimal("0")
ONE = Decimal("1")


@dataclass(frozen=True)
class V7WeatherOrderProposal:
    proposed: bool
    reason: str
    condition_id: str | None = None
    token_id: str | None = None
    outcome: str | None = None
    side: str | None = None
    price: Decimal | None = None
    shares: Decimal | None = None
    estimated_fee: Decimal | None = None
    market_fee_rate: Decimal | None = None
    market_fee_exponent: Decimal | None = None
    taker_only: bool | None = None
    expected_edge: Decimal | None = None
    quote_age_seconds: int | None = None
    ttl_seconds: int | None = None
    book_hash: str | None = None
    submittable: bool = False


def _blocked(reason: str) -> V7WeatherOrderProposal:
    return V7WeatherOrderProposal(False, reason)


def _bounded_ratio(value: Decimal) -> tuple[int, int]:
    """Return an exact rational for a bounded Decimal without context rounding."""
    parts = value.as_tuple()
    if (
        not isinstance(parts.exponent, int)
        or len(parts.digits) > 128
        or not -100 <= parts.exponent <= 12
    ):
        raise ValueError("decimal precision exceeds the supported bound")
    return value.as_integer_ratio()


def _decimal_times_integer(value: Decimal, multiplier: int) -> Decimal:
    sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        raise ValueError("decimal exponent is not finite")
    coefficient = int("".join(str(digit) for digit in digits)) * multiplier
    result_digits = tuple(int(digit) for digit in str(coefficient))
    return Decimal((sign, result_digits, exponent))


def _bounded_shares(candidate: Decimal, cap: Decimal, price: Decimal, step: Decimal) -> Decimal:
    candidate_num, candidate_den = _bounded_ratio(candidate)
    cap_num, cap_den = _bounded_ratio(cap)
    price_num, price_den = _bounded_ratio(price)
    step_num, step_den = _bounded_ratio(step)
    candidate_units = (candidate_num * step_den) // (candidate_den * step_num)
    cap_units = (cap_num * price_den * step_den) // (
        cap_den * price_num * step_num
    )
    return _decimal_times_integer(step, min(candidate_units, cap_units))


def propose_v7_weather_order(
    evaluation: Any,
    context: MarketContext,
    *,
    best_bid: Decimal,
    best_ask: Decimal,
    book_timestamp: datetime,
    book_hash: str,
    decision_timestamp: datetime,
    now: datetime,
    size_step: Decimal,
    order_cap: Decimal,
    ttl_seconds: int,
    max_quote_age_seconds: int,
    max_order_ttl_seconds: int,
    min_price: Decimal = Decimal("0.02"),
    max_price: Decimal = Decimal("0.98"),
) -> V7WeatherOrderProposal:
    """Build a fail-closed, audit-only post-only BUY proposal from a V7 signal.

    Supported fee schedules are currently fee-free only. Size never exceeds the
    V7 candidate's already-computed shares; it may be reduced to the supplied
    live notional cap and venue size step. Fresh top-of-book inputs must be from
    the same snapshot as ``context``. No order or SDK method is called.
    """
    if getattr(evaluation, "strategy", None) != "weather_directional":
        return _blocked("unsupported strategy")
    if not bool(getattr(evaluation, "paper_tradeable", False)):
        return _blocked("V7 paper candidate is not tradeable")
    decision = getattr(evaluation, "decision", None)
    if decision is None or not bool(getattr(decision, "tradeable", False)):
        return _blocked("V7 weather decision is not tradeable")

    condition_id = str(getattr(evaluation, "condition_id", "") or "")
    token_id = str(getattr(evaluation, "token_id", "") or "")
    outcome = getattr(evaluation, "side", None)
    if not condition_id or not token_id or outcome not in {"YES", "NO"}:
        return _blocked("candidate market identity or outcome is invalid")
    if (
        context.condition_id != condition_id
        or context.token_id != token_id
        or not context.condition_matches
        or not context.token_matches
        or not context.rules_verified
        or not context.accepting_orders
        or context.disputed
        or not context.negative_risk
        or not context.resolution_source
    ):
        return _blocked("current market identity/rules/status do not match the V7 candidate")

    shadow = getattr(evaluation, "maker_shadow", None)
    if shadow is None:
        return _blocked("V7 candidate has no matching maker-shadow book context")
    if getattr(shadow, "best_bid", None) != best_bid or getattr(shadow, "best_ask", None) != best_ask:
        return _blocked("current top of book differs from the V7 candidate snapshot")

    evaluation_fee_rate = getattr(evaluation, "fee_rate", None)
    evaluation_fee_exponent = getattr(evaluation, "fee_exponent", None)
    evaluation_taker_only = getattr(evaluation, "taker_only", None)
    evaluation_fee = getattr(evaluation, "fee", None)
    if (
        context.fee_rate is None
        or not isinstance(context.fee_rate, Decimal)
        or not context.fee_rate.is_finite()
        or not isinstance(evaluation_fee_rate, Decimal)
        or not evaluation_fee_rate.is_finite()
        or evaluation_fee_rate != context.fee_rate
        or not isinstance(evaluation_fee, Decimal)
        or not evaluation_fee.is_finite()
        or evaluation_fee < ZERO
        or evaluation_fee_exponent != context.fee_exponent
        or evaluation_taker_only != context.taker_only
    ):
        return _blocked("missing or mismatched fee schedule provenance")
    if context.fee_rate == ZERO:
        if evaluation_fee != ZERO:
            return _blocked("fee-free schedule conflicts with the V7 candidate fee")
    elif (
        context.fee_rate < ZERO
        or context.fee_exponent != ONE
        or context.taker_only is not True
        or evaluation_taker_only is not True
    ):
        return _blocked("fee-bearing passive entry requires a verified exponent-1 taker-only schedule")

    timestamps = (book_timestamp, decision_timestamp, now, context.book_timestamp)
    if any(not isinstance(value, datetime) or value.utcoffset() is None for value in timestamps):
        return _blocked("book, decision, and current timestamps must be timezone-aware")
    if (
        getattr(evaluation, "decision_timestamp", None) != decision_timestamp
        or getattr(evaluation, "book_timestamp", None) != book_timestamp
        or getattr(evaluation, "book_hash", None) != book_hash
    ):
        return _blocked("V7 decision time and source-book provenance do not match the supplied snapshot")
    if context.book_timestamp != book_timestamp or context.book_hash != book_hash or not book_hash:
        return _blocked("top of book does not match the verified market-context snapshot")
    now_utc = now.astimezone(timezone.utc)
    ages = [
        (now_utc - value.astimezone(timezone.utc)).total_seconds()
        for value in (book_timestamp, decision_timestamp)
    ]
    if any(age < 0 for age in ages):
        return _blocked("book or V7 decision timestamp is in the future")
    quote_age = ceil(max(ages))
    if type(max_quote_age_seconds) is not int or max_quote_age_seconds < 0 or quote_age > max_quote_age_seconds:
        return _blocked("book or V7 decision is stale")

    decimal_inputs = (
        best_bid, best_ask, context.tick_size, context.min_order_size,
        size_step, order_cap, min_price, max_price,
        getattr(evaluation, "shares", None),
        getattr(decision, "calibrated_probability", None),
        getattr(decision, "minimum_edge", None),
    )
    if any(not isinstance(value, Decimal) or not value.is_finite() for value in decimal_inputs):
        return _blocked("price, size, policy, or market-rule input is missing or non-finite")
    if not (ZERO < best_bid < best_ask < ONE):
        return _blocked("book is crossed, locked, or outside binary price bounds")
    if (
        context.tick_size <= ZERO or context.min_order_size <= ZERO
        or size_step <= ZERO or order_cap <= ZERO
        or not (ZERO < min_price <= max_price < ONE)
    ):
        return _blocked("tick, minimum, size step, cap, or price range is invalid")
    if not is_tick_aligned(best_bid, context.tick_size) or not is_tick_aligned(best_ask, context.tick_size):
        return _blocked("top of book is not aligned to the verified tick size")

    ttl_values_valid = (
        type(ttl_seconds) is int
        and type(max_order_ttl_seconds) is int
        and ttl_seconds >= 121
        and ttl_seconds <= max_order_ttl_seconds
    )
    if not ttl_values_valid:
        return _blocked("TTL is outside the supported GTD range")

    price = min(best_bid + context.tick_size, best_ask - context.tick_size)
    if price <= ZERO or not is_tick_aligned(price, context.tick_size):
        return _blocked("cannot form a positive tick-aligned post-only price")
    if not (min_price <= price <= max_price):
        return _blocked("passive price is outside the V7 weather price range")

    probability = decision.calibrated_probability
    minimum_edge = decision.minimum_edge
    if not (ZERO <= probability <= ONE) or minimum_edge < ZERO:
        return _blocked("V7 calibrated probability or edge threshold is invalid")
    edge = probability - price
    if edge < minimum_edge:
        return _blocked("passive-price edge is below the V7 minimum edge")

    candidate_shares = evaluation.shares
    if candidate_shares <= ZERO:
        return _blocked("V7 candidate size must be positive")
    try:
        shares = _bounded_shares(candidate_shares, order_cap, price, size_step)
    except (ArithmeticError, ValueError):
        return _blocked("size arithmetic exceeded the supported exact-decimal bounds")
    if shares < context.min_order_size:
        return _blocked("live order cap and size step do not meet the venue minimum")

    return V7WeatherOrderProposal(
        proposed=True,
        reason="offline passive proposal only; fill probability is unvalidated",
        condition_id=condition_id,
        token_id=token_id,
        outcome=outcome,
        side="BUY",
        price=price,
        shares=shares,
        estimated_fee=ZERO,
        market_fee_rate=context.fee_rate,
        market_fee_exponent=context.fee_exponent,
        taker_only=context.taker_only,
        expected_edge=edge,
        quote_age_seconds=quote_age,
        ttl_seconds=ttl_seconds,
        book_hash=book_hash,
    )
