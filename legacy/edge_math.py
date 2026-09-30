"""Math helpers for prediction-market edge, uncertainty, and sizing.

The functions in this module are deliberately pure/offline: they do not fetch
market data, start sockets, or place orders. Strategies can use them both in
live code and in tests/backtests.
"""
from __future__ import annotations

import math
from typing import Optional


def clamp(value: float, low: float, high: float) -> float:
    """Clamp ``value`` into [low, high]."""
    return max(low, min(high, value))


def shrink_probability(
    model_prob: float,
    anchor_prob: float,
    n_eff: float,
    prior_strength: float = 25.0,
) -> float:
    """Shrink a noisy model probability toward an anchor/base probability.

    Empirical-Bayes style weighted average:
        p = (n_eff * model_prob + prior_strength * anchor_prob)
            / (n_eff + prior_strength)

    ``anchor_prob`` should usually be the executable market price for entry
    decisions, or a calibrated climatology/base rate for offline evaluation.
    """
    model_prob = clamp(float(model_prob), 0.001, 0.999)
    anchor_prob = clamp(float(anchor_prob), 0.001, 0.999)
    n_eff = max(0.0, float(n_eff))
    prior_strength = max(0.0, float(prior_strength))
    denom = n_eff + prior_strength
    if denom <= 0:
        return model_prob
    return clamp((n_eff * model_prob + prior_strength * anchor_prob) / denom, 0.001, 0.999)


def probability_standard_error(prob: float, n_eff: float) -> float:
    """Binomial-style standard error for a probability estimate."""
    prob = clamp(float(prob), 0.001, 0.999)
    n_eff = max(1.0, float(n_eff))
    return math.sqrt(prob * (1.0 - prob) / n_eff)


def conservative_probability(prob: float, n_eff: float, z: float = 1.0) -> float:
    """Lower-confidence probability used for conservative Kelly sizing."""
    prob = clamp(float(prob), 0.001, 0.999)
    return clamp(prob - float(z) * probability_standard_error(prob, n_eff), 0.001, 0.999)


def effective_ensemble_size(n_members: int, ensemble_std: float, max_std: float = 5.0) -> float:
    """Estimate an effective sample size from member count and ensemble agreement.

    A tight ensemble keeps most of the nominal member count; a very wide ensemble
    shrinks confidence sharply. This is a heuristic until enough resolved local
    history exists for formal calibration.
    """
    n_members = max(1, int(n_members or 1))
    spread_penalty = clamp(float(ensemble_std) / max_std, 0.0, 1.0)
    agreement = 1.0 - spread_penalty
    return max(1.0, n_members * agreement)


def dynamic_min_edge(
    *,
    base_edge: float = 0.05,
    max_edge: float = 0.20,
    ensemble_std: float = 0.0,
    lead_days: int = 1,
    market_price: Optional[float] = None,
    n_eff: Optional[float] = None,
    spread: Optional[float] = None,
    z: float = 1.0,
) -> float:
    """Minimum executable edge required before trading.

    Combines four safeguards:
    - base strategy edge floor
    - weather ensemble spread and lead-time uncertainty
    - probability-estimate uncertainty (``z * sigma``) when ``n_eff`` known
    - market microstructure: at least half the bid/ask spread
    - tail penalty near very low/high prices where odds are fragile
    """
    raw = float(base_edge) + max(0.0, float(ensemble_std)) * 0.025
    raw += max(0, int(lead_days or 1) - 1) * 0.01

    candidates = [raw]
    if market_price is not None and n_eff is not None:
        sigma = probability_standard_error(float(market_price), float(n_eff))
        candidates.append(float(z) * sigma)

    if spread is not None:
        candidates.append(max(0.0, float(spread)) * 0.5)

    if market_price is not None:
        p = clamp(float(market_price), 0.001, 0.999)
        # Small extra cushion in the tails; not huge, but blocks fragile 1-2c edges.
        tail_distance = max(0.0, abs(p - 0.5) - 0.35) / 0.15
        candidates.append(float(base_edge) + 0.03 * clamp(tail_distance, 0.0, 1.0))

    return clamp(max(candidates), float(base_edge), float(max_edge))


def kelly_fraction_binary(prob: float, price: float) -> float:
    """Full-Kelly fraction for a binary YES share bought at ``price``.

    For a share costing ``c`` and paying 1 if the event occurs:
        f* = (p - c) / (1 - c)
    """
    prob = clamp(float(prob), 0.0, 1.0)
    price = float(price)
    if price <= 0.0 or price >= 1.0:
        return 0.0
    return max(0.0, (prob - price) / (1.0 - price))


def executable_buy_price(
    *,
    best_bid: Optional[float],
    best_ask: Optional[float],
    fallback: Optional[float] = None,
    tick_size: float = 0.01,
) -> Optional[float]:
    """Return the executable BUY price for edge checks.

    Prefer best ask. If no ask exists but there is a bid, use bid + one tick as
    a maker/no-ask fallback. Only use ``fallback`` when the book has no usable
    side at all.
    """
    if best_ask is not None and best_ask > 0:
        return float(best_ask)
    if best_bid is not None and best_bid > 0:
        return clamp(round(float(best_bid) + tick_size, 2), tick_size, 0.99)
    if fallback is not None and fallback > 0:
        return float(fallback)
    return None
