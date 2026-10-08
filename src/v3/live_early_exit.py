"""Pure, fail-closed planner for post-only live early-exit SELL intents.

This module never submits orders and makes no fill/state mutation claims. Callers
must provide verified, fresh metadata and reconcile outstanding orders/fills.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from typing import Any, Iterable

from .math import BookLevel, execution_bid_vwap, execution_fee

ZERO = Decimal("0")
ONE = Decimal("1")
# Polymarket CLOB share sizes have two decimals; anything below one step can
# never be sold and is treated as dust for exit planning and event blocking.
SHARE_SIZE_STEP = Decimal("0.01")


@dataclass(frozen=True)
class VerifiedBook:
    bids: tuple[BookLevel, ...]
    best_bid: Decimal
    tick_size: Decimal
    min_order_size: Decimal
    size_step: Decimal
    fee_rate: Decimal
    fee_schedule: str = "polymarket_quadratic"
    fresh: bool = False
    rules_verified: bool = False
    fee_verified: bool = False


@dataclass(frozen=True)
class ExitPosition:
    side: str
    shares: Decimal
    all_in_cost: Decimal
    hybrid_exit_done: bool = False
    hybrid_enabled: bool = False


@dataclass(frozen=True)
class SellIntent:
    side: str
    price: Decimal
    size: Decimal
    post_only: bool
    stage: str
    target_return: Decimal
    estimated_bid_vwap: Decimal
    estimated_fee: Decimal
    estimated_net_proceeds: Decimal
    estimated_profit: Decimal
    estimated_return: Decimal


@dataclass(frozen=True)
class PlanResult:
    intent: SellIntent | None
    reason: str | None


def verified_book_from_api(
    context: Any,
    raw_book: Any,
    *,
    condition_id: str,
    token_id: str,
    now: datetime,
    max_quote_age_seconds: int,
) -> VerifiedBook:
    """Build planner input only from matching, fresh API context and depth.

    The order-book response passed here must be the exact snapshot represented
    by the context hash/timestamp; unknown fee curves or market rules fail closed.
    Polymarket order docs specify two share-size decimals for all listed ticks.

    The CLOB book ``timestamp`` is the time of the last book change, not of the
    read: a quiet book is still current when fetched. When the context records
    its own read time (``fetched_at``), quote age is measured from that read;
    the book's last change must never postdate ``now``. Without a read time,
    the stricter last-change age applies.
    """
    timestamp = getattr(raw_book, "timestamp", None)
    context_timestamp = getattr(context, "book_timestamp", None)
    fetched_at = getattr(context, "fetched_at", None)
    if (
        not isinstance(condition_id, str) or not condition_id
        or not isinstance(token_id, str) or not token_id
        or not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None
        or not isinstance(timestamp, datetime) or timestamp.tzinfo is None or timestamp.utcoffset() is None
        or not isinstance(context_timestamp, datetime) or context_timestamp.tzinfo is None
        or context_timestamp.utcoffset() is None
        or fetched_at is not None and (
            not isinstance(fetched_at, datetime) or fetched_at.tzinfo is None
            or fetched_at.utcoffset() is None
        )
        or type(max_quote_age_seconds) is not int or max_quote_age_seconds <= 0
    ):
        raise ValueError("verified book identity or timestamp inputs are invalid")
    now_utc, timestamp_utc = now.astimezone(timezone.utc), timestamp.astimezone(timezone.utc)
    observed_utc = timestamp_utc if fetched_at is None else fetched_at.astimezone(timezone.utc)
    age = (now_utc - observed_utc).total_seconds()
    if age < 0 or age > max_quote_age_seconds or timestamp_utc > now_utc:
        raise ValueError("order book is future-dated or stale")
    context_hash = getattr(context, "book_hash", None)
    raw_hash = getattr(raw_book, "hash", None)
    if (
        getattr(context, "condition_id", None) != condition_id
        or getattr(context, "token_id", None) != token_id
        or getattr(context, "condition_matches", False) is not True
        or getattr(context, "token_matches", False) is not True
        or getattr(context, "rules_verified", False) is not True
        or getattr(context, "accepting_orders", False) is not True
        or getattr(raw_book, "condition_id", None) != condition_id
        or getattr(raw_book, "token_id", None) != token_id
        or not isinstance(context_hash, str) or not context_hash
        or raw_hash != context_hash
        or timestamp_utc != context_timestamp.astimezone(timezone.utc)
    ):
        raise ValueError("market, token, rules, or exact book snapshot do not match")
    fee_rate = getattr(context, "fee_rate", None)
    exponent = getattr(context, "fee_exponent", None)
    fees_enabled = getattr(context, "fees_enabled", None)
    if not isinstance(fee_rate, Decimal) or not fee_rate.is_finite() or fee_rate < ZERO:
        raise ValueError("verified fee rate is missing or invalid")
    fee_verified = fees_enabled is False or (fees_enabled is True and exponent == ONE)
    if not fee_verified:
        raise ValueError("fee schedule is unsupported or unverified")
    raw_levels = getattr(raw_book, "bids", None)
    if not isinstance(raw_levels, (tuple, list)) or not raw_levels:
        raise ValueError("verified order book has no bid depth")
    try:
        levels = tuple(BookLevel(Decimal(str(level.price)), Decimal(str(level.size))) for level in raw_levels)
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise ValueError("order-book bid level is malformed") from exc
    if any(
        not level.price.is_finite() or not level.size.is_finite()
        or not ZERO < level.price < ONE or level.size <= ZERO
        for level in levels
    ):
        raise ValueError("order-book bid level economics are invalid")
    # The CLOB returns bids ascending (best bid last); the planner walks depth
    # best-first. Canonicalize, but never merge or guess at duplicate prices.
    if len({level.price for level in levels}) != len(levels):
        raise ValueError("bid levels contain duplicate prices")
    levels = tuple(sorted(levels, key=lambda item: item.price, reverse=True))
    tick_size = getattr(context, "tick_size", None)
    min_order_size = getattr(context, "min_order_size", None)
    if not isinstance(tick_size, Decimal) or not isinstance(min_order_size, Decimal):
        raise ValueError("verified venue tick/minimum size is missing")
    raw_tick_size = getattr(raw_book, "tick_size", None)
    raw_min_order_size = getattr(raw_book, "min_order_size", None)
    if (
        raw_tick_size is not None and Decimal(str(raw_tick_size)) != tick_size
        or raw_min_order_size is not None and Decimal(str(raw_min_order_size)) != min_order_size
    ):
        raise ValueError("book venue rules differ from verified market context")
    return VerifiedBook(
        # Polymarket CLOB order docs: Size decimals = 2; round share quantity down.
        # https://docs.polymarket.com/trading/place-orders
        bids=levels, best_bid=levels[0].price, tick_size=tick_size,
        min_order_size=min_order_size, size_step=SHARE_SIZE_STEP,
        fee_rate=fee_rate, fresh=True, rules_verified=True, fee_verified=True,
    )


def _ceil_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def _floor_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def is_dust(quantity: Any) -> bool:
    """True for a finite holding smaller than one venue share-size step."""
    return isinstance(quantity, Decimal) and quantity.is_finite() and quantity < SHARE_SIZE_STEP


def _d(value: Decimal) -> bool:
    return value.is_finite()


def plan_live_early_exit(
    position: ExitPosition,
    book: VerifiedBook,
    *,
    target_return: Decimal = Decimal("0.28"),
    min_profit: Decimal = Decimal("0.10"),
    hybrid_fraction: Decimal = Decimal("0.75"),
    runner_target_return: Decimal = Decimal("0.50"),
    first_tranche_quantity: Decimal | None = None,
) -> PlanResult:
    """Return a non-crossing, post-only SELL intent or fail-closed reason.

    Economics use executable bid VWAP and levelwise quadratic fee exactly as
    paper does. The posted limit itself is conservatively tick-rounded and is
    not counted as executed revenue.
    """
    try:
        values = (position.shares, position.all_in_cost, target_return, min_profit,
                  hybrid_fraction, runner_target_return, book.tick_size,
                  book.min_order_size, book.size_step, book.fee_rate, book.best_bid)
        if not all(_d(v) for v in values):
            return PlanResult(None, "non-finite input")
        if position.side.upper() not in {"YES", "NO"}:
            return PlanResult(None, "directional position required")
        if not (book.fresh and book.rules_verified and book.fee_verified):
            return PlanResult(None, "book, rules, or fee metadata is unverified/stale")
        if book.fee_schedule != "polymarket_quadratic":
            return PlanResult(None, "unsupported fee schedule")
        if book.tick_size <= ZERO or book.size_step <= ZERO or book.min_order_size <= ZERO:
            return PlanResult(None, "invalid venue tick or size rules")
        if not (ZERO <= book.fee_rate <= ONE) or target_return < ZERO or min_profit < ZERO:
            return PlanResult(None, "invalid fee or target policy")
        if position.shares <= ZERO or position.all_in_cost <= ZERO:
            return PlanResult(None, "invalid shares or position cost")
        if book.best_bid <= ZERO or book.best_bid >= ONE:
            return PlanResult(None, "invalid best bid")
        levels = tuple(book.bids)
        if not levels or any(not isinstance(level, BookLevel) for level in levels):
            return PlanResult(None, "verified bid depth is malformed")
        if max(level.price for level in levels) != book.best_bid:
            return PlanResult(None, "best bid does not match verified bid depth")
        if position.hybrid_enabled and not (ZERO < hybrid_fraction < ONE):
            return PlanResult(None, "invalid hybrid fraction")
        if first_tranche_quantity is not None and (
            not isinstance(first_tranche_quantity, Decimal) or not first_tranche_quantity.is_finite()
            or first_tranche_quantity <= ZERO or first_tranche_quantity > position.shares
        ):
            return PlanResult(None, "invalid first-tranche quantity override")
        if runner_target_return < target_return:
            return PlanResult(None, "hybrid runner target below first target")
        partial = position.hybrid_enabled and not position.hybrid_exit_done
        stage = "first_tranche" if partial else ("runner" if position.hybrid_enabled else "full")
        qty = (first_tranche_quantity if first_tranche_quantity is not None and partial
               else position.shares * hybrid_fraction if partial else position.shares)
        target = target_return if not position.hybrid_enabled or partial else runner_target_return
        qty = _floor_step(qty, book.size_step)
        if partial:
            # A tranche below the venue minimum, or one leaving an unsellable
            # sub-minimum runner, would strand inventory. Sell the whole
            # (step-rounded) position instead, at first-tranche economics.
            whole = _floor_step(position.shares, book.size_step)
            if whole >= book.min_order_size and (
                qty < book.min_order_size or position.shares - qty < book.min_order_size
            ):
                stage, qty = "full", whole
        # Allocate basis at the actual venue-rounded share fraction. The CLOB
        # requires share size rounding down, never up, to avoid overselling.
        cost = position.all_in_cost * qty / position.shares
        if qty <= ZERO or qty > position.shares or cost <= ZERO:
            return PlanResult(None, "invalid hybrid tranche/position state")
        if qty < book.min_order_size:
            return PlanResult(None, "sell tranche below venue minimum size")
        # Inferred per-share cost must be a valid binary-contract price.
        if cost / qty <= ZERO or cost / qty >= ONE:
            return PlanResult(None, "inconsistent shares and allocated cost")
        levels = tuple(book.bids)
        quote = execution_bid_vwap(levels, qty)
        fees = execution_fee(levels, qty, book.fee_rate, descending=True)
        net = quote.notional - fees
        profit = net - cost
        ret = profit / cost
        if profit < min_profit or ret < target:
            return PlanResult(None, "executable bid-depth economics below target")
        # Find the smallest valid sell limit whose modeled proceeds after fee
        # meet both paper thresholds; then impose an explicit no-cross bound.
        required = max(cost * (ONE + target), cost + min_profit)
        lo, hi = ZERO, ONE
        for _ in range(96):
            mid = (lo + hi) / 2
            proceeds = qty * mid * (ONE - book.fee_rate * (ONE - mid))
            if proceeds >= required:
                hi = mid
            else:
                lo = mid
        threshold = _ceil_step(hi, book.tick_size)
        price = max(threshold, book.best_bid + book.tick_size)
        price = _ceil_step(price, book.tick_size)
        if price >= ONE:
            return PlanResult(None, "profitable non-crossing limit outside valid price range")
        return PlanResult(SellIntent(
            side="SELL", price=price, size=qty, post_only=True, stage=stage,
            target_return=target, estimated_bid_vwap=quote.vwap,
            estimated_fee=fees, estimated_net_proceeds=net,
            estimated_profit=profit, estimated_return=ret,
        ), None)
    except (ValueError, ArithmeticError, TypeError) as exc:
        return PlanResult(None, f"invalid or insufficient book/input: {exc}")
