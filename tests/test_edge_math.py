import math

import pytest

from src.edge_math import (
    conservative_probability,
    dynamic_min_edge,
    executable_buy_price,
    kelly_fraction_binary,
    probability_standard_error,
    shrink_probability,
)


def test_shrink_probability_moves_noisy_forecast_toward_anchor():
    assert shrink_probability(0.80, 0.50, n_eff=5, prior_strength=15) == pytest.approx(0.575)
    assert shrink_probability(0.80, 0.50, n_eff=60, prior_strength=15) == pytest.approx(0.74)


def test_probability_standard_error_uses_effective_sample_size():
    assert probability_standard_error(0.50, n_eff=100) == pytest.approx(0.05)
    assert probability_standard_error(0.50, n_eff=25) > probability_standard_error(0.50, n_eff=100)


def test_conservative_probability_subtracts_uncertainty_floor():
    p = conservative_probability(0.70, n_eff=25, z=1.0)
    assert p == pytest.approx(0.70 - math.sqrt(0.70 * 0.30 / 25))


def test_dynamic_min_edge_includes_spread_uncertainty_and_tail_penalty():
    min_edge = dynamic_min_edge(
        base_edge=0.05,
        ensemble_std=2.0,
        lead_days=2,
        market_price=0.08,
        n_eff=25,
        spread=0.04,
    )
    assert min_edge > 0.05
    assert min_edge >= 0.02  # half-spread guard
    assert min_edge <= 0.20


def test_kelly_fraction_binary_matches_prediction_market_formula():
    # Binary YES bought at c=.20 with p=.30 -> (p-c)/(1-c)=0.125
    assert kelly_fraction_binary(0.30, 0.20) == pytest.approx(0.125)
    assert kelly_fraction_binary(0.20, 0.30) == 0.0


def test_executable_buy_price_prefers_best_ask_over_midpoint():
    assert executable_buy_price(best_bid=0.10, best_ask=0.16, fallback=0.13) == 0.16
    assert executable_buy_price(best_bid=0.10, best_ask=None, fallback=0.13) == pytest.approx(0.11)
