"""Bounded paper-only sizing on a captured ask book; no order client.

Search downward-safe microshare quantities, re-evaluating fee-adjusted Kelly
and the uncertainty edge at every quantity. A rejected minimum is diagnostic
only: never raise a Kelly budget to satisfy a venue minimum.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import ROUND_FLOOR, Decimal
from typing import Sequence

from .math import BookLevel, execution_fee, execution_vwap
from .strategies.weather import WeatherDecision, WeatherMarketInput, evaluate_weather_market

ZERO = Decimal('0')
QUANTUM = Decimal('0.000001')  # Paper precision, not a claim about venue lot size.


@dataclass(frozen=True)
class WeatherSizing:
    accepted: bool
    reason: str
    shares: Decimal
    ask: Decimal
    fee: Decimal
    all_in_cost: Decimal
    budget: Decimal
    decision: WeatherDecision


def size_weather_entry(
    *, levels: Sequence[BookLevel], market: WeatherMarketInput,
    minimum_shares: Decimal, bankroll: Decimal | None, order_cap: Decimal,
) -> WeatherSizing:
    for name, value in (('bankroll', bankroll), ('minimum shares', minimum_shares), ('order cap', order_cap)):
        if value is None or not value.is_finite() or value <= ZERO:
            raise ValueError(f'{name} must be finite and positive')
    assert bankroll is not None
    if not market.fee_rate.is_finite() or market.fee_rate < ZERO:
        raise ValueError('fee rate must be finite and nonnegative')
    if not levels:
        raise ValueError('missing ask depth')

    def quote(shares: Decimal) -> WeatherSizing:
        execution = execution_vwap(levels, shares)
        fee = execution_fee(levels, shares, market.fee_rate)
        decision = evaluate_weather_market(replace(
            market, best_ask=execution.vwap, executable_fee_per_share=fee / shares,
        ))
        cost = execution.notional + fee
        budget = min(order_cap, bankroll, bankroll * decision.kelly_fraction)
        accepted = decision.tradeable and cost <= budget
        reason = 'kelly paper candidate' if accepted else (
            decision.reason if not decision.tradeable else 'kelly budget below venue minimum'
        )
        return WeatherSizing(accepted, reason, shares, execution.vwap, fee, cost, budget, decision)

    minimum = quote(minimum_shares)  # Raises on insufficient minimum depth.
    if not minimum.accepted:
        return minimum
    # All-in cost rises with quantity; Kelly at the executable VWAP cannot
    # increase with worse asks. The minimum must pass before any search.
    max_shares = min(sum((level.size for level in levels), ZERO), order_cap / min(level.price for level in levels))
    high = int(((max_shares - minimum_shares) / QUANTUM).to_integral_value(rounding=ROUND_FLOOR))
    low = 0
    best = minimum
    while low < high:
        middle = (low + high + 1) // 2
        candidate = quote(minimum_shares + QUANTUM * middle)
        if candidate.accepted:
            low, best = middle, candidate
        else:
            high = middle - 1
    return best
