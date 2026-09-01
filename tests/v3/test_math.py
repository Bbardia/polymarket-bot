from decimal import Decimal

import pytest

from src.v3.math import (
    BookLevel,
    complete_set_opportunity,
    correlated_effective_sample_size,
    execution_bid_vwap,
    execution_vwap,
    fee_adjusted_binary_kelly,
    is_tick_aligned,
    taker_fee,
)


def D(value: str) -> Decimal:
    return Decimal(value)


def test_taker_fee_uses_current_category_curve():
    assert taker_fee(shares=D("100"), price=D("0.50"), fee_rate=D("0.05")) == D("1.2500")
    assert taker_fee(shares=D("46.875"), price=D("0.64"), fee_rate=D("0.05")) == D("0.54000000")


def test_execution_vwap_walks_depth_and_refuses_partial_liquidity():
    asks = [BookLevel(D("0.20"), D("3")), BookLevel(D("0.22"), D("4"))]
    quote = execution_vwap(asks, D("5"))
    assert quote.shares == D("5")
    assert quote.notional == D("1.04")
    assert quote.vwap == D("0.208")

    with pytest.raises(ValueError, match="insufficient liquidity"):
        execution_vwap(asks, D("8"))


def test_execution_bid_vwap_walks_highest_bids_first_and_refuses_partial_liquidity():
    bids = [BookLevel(D("0.40"), D("3")), BookLevel(D("0.50"), D("4"))]
    quote = execution_bid_vwap(bids, D("5"))
    assert quote.shares == D("5")
    assert quote.notional == D("2.40")
    assert quote.vwap == D("0.48")

    with pytest.raises(ValueError, match="insufficient liquidity"):
        execution_bid_vwap(bids, D("8"))


def test_complete_set_edge_is_based_on_executable_books_and_fees():
    opportunity = complete_set_opportunity(
        yes_asks=[BookLevel(D("0.32"), D("100"))],
        no_asks=[BookLevel(D("0.64"), D("100"))],
        shares=D("46.875"),
        fee_rate=D("0.05"),
    )
    assert opportunity.gross_cost == D("45.00000")
    assert opportunity.fees == D("1.05000000")
    assert opportunity.net_profit == D("0.82500000")
    assert opportunity.return_on_cost == pytest.approx(D("0.0179153094"), rel=Decimal("1e-8"))


def test_complete_set_fee_is_summed_per_depth_level_not_applied_to_vwap():
    opportunity = complete_set_opportunity(
        yes_asks=[BookLevel(D("0.20"), D("5")), BookLevel(D("0.80"), D("5"))],
        no_asks=[BookLevel(D("0.40"), D("10"))],
        shares=D("10"),
        fee_rate=D("0.05"),
    )
    expected_yes_fees = taker_fee(shares=D("5"), price=D("0.20"), fee_rate=D("0.05"))
    expected_yes_fees += taker_fee(shares=D("5"), price=D("0.80"), fee_rate=D("0.05"))
    expected_no_fees = taker_fee(shares=D("10"), price=D("0.40"), fee_rate=D("0.05"))
    assert opportunity.fees == expected_yes_fees + expected_no_fees


    n_eff = correlated_effective_sample_size(143, intraclass_correlation=D("0.20"))
    assert D("4") < n_eff < D("6")
    assert correlated_effective_sample_size(143, intraclass_correlation=D("0")) == D("143")


def test_fee_adjusted_kelly_is_conservative_and_zero_without_net_edge():
    no_fee = fee_adjusted_binary_kelly(probability=D("0.60"), price=D("0.50"), fee_per_share=D("0"))
    with_fee = fee_adjusted_binary_kelly(probability=D("0.60"), price=D("0.50"), fee_per_share=D("0.0125"))
    no_edge = fee_adjusted_binary_kelly(probability=D("0.50"), price=D("0.50"), fee_per_share=D("0.0125"))
    assert D("0") < with_fee < no_fee
    assert no_edge == D("0")


def test_tick_alignment_supports_non_cent_ticks():
    assert is_tick_aligned(D("0.4125"), D("0.0025"))
    assert not is_tick_aligned(D("0.413"), D("0.0025"))
