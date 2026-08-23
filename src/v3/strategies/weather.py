"""Fee- and uncertainty-aware paper evaluator for weather markets."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from ..math import (
    correlated_effective_sample_size,
    dynamic_minimum_edge,
    fee_adjusted_binary_kelly,
    shrink_probability,
    taker_fee,
)

ZERO = Decimal("0")
ONE = Decimal("1")


@dataclass(frozen=True)
class WeatherMarketInput:
    raw_probability: Decimal
    anchor_probability: Decimal
    n_members: int
    intraclass_correlation: Decimal
    best_bid: Decimal
    best_ask: Decimal
    fee_rate: Decimal
    lead_days: int
    resolution_source_verified: bool
    prior_strength: Decimal = Decimal("10")
    fractional_kelly: Decimal = Decimal("0.10")


@dataclass(frozen=True)
class WeatherDecision:
    tradeable: bool
    reason: str
    effective_sample_size: Decimal
    calibrated_probability: Decimal
    fee_per_share: Decimal
    net_edge: Decimal
    minimum_edge: Decimal
    kelly_fraction: Decimal
    paper_only: bool = True


def evaluate_weather_market(market: WeatherMarketInput) -> WeatherDecision:
    if not market.resolution_source_verified:
        return WeatherDecision(
            False,
            "resolution source is not verified",
            ZERO,
            market.raw_probability,
            ZERO,
            ZERO,
            ZERO,
            ZERO,
        )
    if not (ZERO < market.best_bid <= market.best_ask < ONE):
        raise ValueError("invalid executable book")

    n_eff = correlated_effective_sample_size(
        market.n_members,
        intraclass_correlation=market.intraclass_correlation,
    )
    probability = shrink_probability(
        model_probability=market.raw_probability,
        anchor_probability=market.anchor_probability,
        effective_sample_size=n_eff,
        prior_strength=market.prior_strength,
    )
    fee_per_share = taker_fee(
        shares=ONE,
        price=market.best_ask,
        fee_rate=market.fee_rate,
    )
    spread = market.best_ask - market.best_bid
    minimum_edge = dynamic_minimum_edge(
        probability=probability,
        effective_sample_size=n_eff,
        spread=spread,
        lead_days=market.lead_days,
        price=market.best_ask,
    )
    net_edge = probability - market.best_ask - fee_per_share
    if net_edge < minimum_edge:
        return WeatherDecision(
            False,
            f"net executable edge {net_edge} is below uncertainty-adjusted minimum {minimum_edge}",
            n_eff,
            probability,
            fee_per_share,
            net_edge,
            minimum_edge,
            ZERO,
        )

    full_kelly = fee_adjusted_binary_kelly(
        probability=probability,
        price=market.best_ask,
        fee_per_share=fee_per_share,
    )
    kelly = min(market.fractional_kelly, full_kelly * market.fractional_kelly)
    return WeatherDecision(
        True,
        "paper candidate",
        n_eff,
        probability,
        fee_per_share,
        net_edge,
        minimum_edge,
        kelly,
    )
