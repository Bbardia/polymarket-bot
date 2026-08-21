import pytest

from src.kelly import KellySizer


def test_kelly_sizer_uses_uncertainty_adjusted_probability_when_n_eff_given():
    sizer = KellySizer(bankroll=100, kelly_fraction=1.0, min_edge=0.0, reserve_pct=0.0)

    raw = sizer.size_position(forecast_prob=0.70, market_price=0.50)
    conservative = sizer.size_position(
        forecast_prob=0.70,
        market_price=0.50,
        probability_n_eff=10,
        uncertainty_z=1.0,
    )

    assert conservative.fraction < raw.fraction
    assert "p_conservative" in conservative.reason


def test_kelly_sizer_skips_when_conservative_edge_disappears():
    sizer = KellySizer(bankroll=100, kelly_fraction=1.0, min_edge=0.05, reserve_pct=0.0)

    result = sizer.size_position(
        forecast_prob=0.58,
        market_price=0.50,
        probability_n_eff=4,
        uncertainty_z=1.0,
    )

    assert result.bet_size_dollars == 0.0
    assert result.confidence == "SKIP"
