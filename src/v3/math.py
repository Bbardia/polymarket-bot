"""Decimal-only execution, fee, edge, and sizing mathematics."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Sequence

ZERO = Decimal("0")
ONE = Decimal("1")


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    size: Decimal

    def __post_init__(self) -> None:
        if not (ZERO < self.price < ONE):
            raise ValueError("book price must be between 0 and 1")
        if self.size <= ZERO:
            raise ValueError("book size must be positive")


@dataclass(frozen=True)
class ExecutionQuote:
    shares: Decimal
    notional: Decimal
    vwap: Decimal


@dataclass(frozen=True)
class CompleteSetOpportunity:
    shares: Decimal
    yes: ExecutionQuote
    no: ExecutionQuote
    gross_cost: Decimal
    fees: Decimal
    payout: Decimal
    net_profit: Decimal
    return_on_cost: Decimal


def taker_fee(*, shares: Decimal, price: Decimal, fee_rate: Decimal) -> Decimal:
    """Current Polymarket fee curve: C × feeRate × p × (1-p)."""
    if shares < ZERO:
        raise ValueError("shares cannot be negative")
    if not (ZERO <= price <= ONE):
        raise ValueError("price must be in [0, 1]")
    if fee_rate < ZERO:
        raise ValueError("fee rate cannot be negative")
    return shares * fee_rate * price * (ONE - price)


def _walk_levels(levels: Sequence[BookLevel] | Iterable[BookLevel], shares: Decimal) -> tuple[tuple[BookLevel, Decimal], ...]:
    if shares <= ZERO:
        raise ValueError("requested shares must be positive")
    remaining = shares
    filled: list[tuple[BookLevel, Decimal]] = []
    for level in sorted(tuple(levels), key=lambda item: item.price):
        take = min(level.size, remaining)
        filled.append((level, take))
        remaining -= take
        if remaining == ZERO:
            break
    if remaining > ZERO:
        raise ValueError(f"insufficient liquidity: missing {remaining} shares")
    return tuple(filled)


def execution_fee(
    levels: Sequence[BookLevel] | Iterable[BookLevel],
    shares: Decimal,
    fee_rate: Decimal,
) -> Decimal:
    """Sum nonlinear fees at each executable depth level."""
    return sum(
        (taker_fee(shares=take, price=level.price, fee_rate=fee_rate) for level, take in _walk_levels(levels, shares)),
        ZERO,
    )


def execution_vwap(levels: Sequence[BookLevel] | Iterable[BookLevel], shares: Decimal) -> ExecutionQuote:
    """Walk asks from cheapest upward and require the requested size in full."""
    if shares <= ZERO:
        raise ValueError("requested shares must be positive")

    filled = _walk_levels(levels, shares)
    notional = sum((take * level.price for level, take in filled), ZERO)
    return ExecutionQuote(shares=shares, notional=notional, vwap=notional / shares)


def complete_set_opportunity(
    *,
    yes_asks: Sequence[BookLevel] | Iterable[BookLevel],
    no_asks: Sequence[BookLevel] | Iterable[BookLevel],
    shares: Decimal,
    fee_rate: Decimal,
) -> CompleteSetOpportunity:
    """Price an equal-share YES+NO acquisition using executable books and fees."""
    yes = execution_vwap(yes_asks, shares)
    no = execution_vwap(no_asks, shares)
    fees = execution_fee(yes_asks, shares, fee_rate)
    fees += execution_fee(no_asks, shares, fee_rate)
    gross_cost = yes.notional + no.notional
    payout = shares
    net_profit = payout - gross_cost - fees
    all_in_cost = gross_cost + fees
    return CompleteSetOpportunity(
        shares=shares,
        yes=yes,
        no=no,
        gross_cost=gross_cost,
        fees=fees,
        payout=payout,
        net_profit=net_profit,
        return_on_cost=net_profit / all_in_cost if all_in_cost > ZERO else ZERO,
    )


def correlated_effective_sample_size(
    n_members: int,
    *,
    intraclass_correlation: Decimal,
) -> Decimal:
    """Cluster/design-effect effective N for correlated ensemble members."""
    if n_members < 1:
        raise ValueError("n_members must be at least 1")
    if not (ZERO <= intraclass_correlation < ONE):
        raise ValueError("intraclass correlation must be in [0, 1)")
    n = Decimal(n_members)
    design_effect = ONE + (n - ONE) * intraclass_correlation
    return n / design_effect


def shrink_probability(
    *,
    model_probability: Decimal,
    anchor_probability: Decimal,
    effective_sample_size: Decimal,
    prior_strength: Decimal,
) -> Decimal:
    """Shrink a model toward a point-in-time base rate, not a midpoint fill."""
    for value in (model_probability, anchor_probability):
        if not (ZERO <= value <= ONE):
            raise ValueError("probabilities must be in [0, 1]")
    if effective_sample_size < ZERO or prior_strength < ZERO:
        raise ValueError("sample and prior strength cannot be negative")
    if prior_strength == ZERO:
        return model_probability
    denominator = effective_sample_size + prior_strength
    if denominator == ZERO:
        return model_probability
    return (
        effective_sample_size * model_probability
        + prior_strength * anchor_probability
    ) / denominator


def probability_standard_error(probability: Decimal, effective_sample_size: Decimal) -> Decimal:
    if not (ZERO <= probability <= ONE):
        raise ValueError("probability must be in [0, 1]")
    if effective_sample_size <= ZERO:
        raise ValueError("effective sample size must be positive")
    return (probability * (ONE - probability) / effective_sample_size).sqrt()


def dynamic_minimum_edge(
    *,
    probability: Decimal,
    effective_sample_size: Decimal,
    spread: Decimal,
    lead_days: int,
    price: Decimal,
    base_edge: Decimal = Decimal("0.05"),
    uncertainty_z: Decimal = ONE,
) -> Decimal:
    """Maximum of base, uncertainty, half-spread, lead, and tail cushions."""
    if spread < ZERO:
        raise ValueError("spread cannot be negative")
    uncertainty = uncertainty_z * probability_standard_error(
        probability, effective_sample_size
    )
    lead_penalty = base_edge + Decimal(max(0, lead_days - 1)) * Decimal("0.01")
    tail_penalty = base_edge
    if price < Decimal("0.05") or price > Decimal("0.95"):
        tail_penalty += Decimal("0.03")
    return max(base_edge, uncertainty, spread / Decimal("2"), lead_penalty, tail_penalty)


def fee_adjusted_binary_kelly(
    *,
    probability: Decimal,
    price: Decimal,
    fee_per_share: Decimal = ZERO,
) -> Decimal:
    """Full Kelly fraction using the all-in cost of one binary share."""
    if not (ZERO <= probability <= ONE):
        raise ValueError("probability must be in [0, 1]")
    all_in_cost = price + fee_per_share
    if all_in_cost <= ZERO or all_in_cost >= ONE:
        return ZERO
    edge = probability - all_in_cost
    if edge <= ZERO:
        return ZERO
    return edge / (ONE - all_in_cost)


def is_tick_aligned(price: Decimal, tick_size: Decimal) -> bool:
    if tick_size <= ZERO:
        raise ValueError("tick size must be positive")
    return price.remainder_near(tick_size) == ZERO
