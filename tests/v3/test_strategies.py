from decimal import Decimal
from types import SimpleNamespace

from src.v3.math import BookLevel
from src.v3.strategies.complete_set import evaluate_complete_set
from src.v3.strategies.weather import WeatherMarketInput, evaluate_weather_market


def D(value: str) -> Decimal:
    return Decimal(value)


def test_weather_evaluator_penalizes_correlated_ensemble_members():
    common = dict(
        raw_probability=D("0.60"),
        anchor_probability=D("0.20"),
        n_members=143,
        best_bid=D("0.19"),
        best_ask=D("0.20"),
        fee_rate=D("0.05"),
        lead_days=1,
        resolution_source_verified=True,
    )
    correlated = evaluate_weather_market(
        WeatherMarketInput(**common, intraclass_correlation=D("0.20"))
    )
    diverse = evaluate_weather_market(
        WeatherMarketInput(**common, intraclass_correlation=D("0.02"))
    )
    assert not correlated.tradeable
    assert "uncertainty" in correlated.reason
    assert diverse.tradeable
    assert D("0") < diverse.kelly_fraction <= D("0.10")


def test_weather_evaluator_requires_verified_resolution_source():
    decision = evaluate_weather_market(
        WeatherMarketInput(
            raw_probability=D("0.80"),
            anchor_probability=D("0.50"),
            n_members=100,
            intraclass_correlation=D("0.01"),
            best_bid=D("0.10"),
            best_ask=D("0.11"),
            fee_rate=D("0.05"),
            lead_days=1,
            resolution_source_verified=False,
        )
    )
    assert not decision.tradeable
    assert "resolution source" in decision.reason


def test_complete_set_research_requires_same_standard_market_and_net_edge():
    yes_book = SimpleNamespace(
        condition_id="condition",
        asks=(SimpleNamespace(price=D("0.32"), size=D("100")),),
        min_order_size=D("5"),
        neg_risk=False,
    )
    no_book = SimpleNamespace(
        condition_id="condition",
        asks=(SimpleNamespace(price=D("0.64"), size=D("100")),),
        min_order_size=D("5"),
        neg_risk=False,
    )
    decision = evaluate_complete_set(
        yes_book=yes_book,
        no_book=no_book,
        shares=D("46.875"),
        fee_rate=D("0.05"),
        min_net_return=D("0.01"),
    )
    assert decision.tradeable
    assert decision.opportunity.net_profit == D("0.82500000")
    assert decision.paper_only

    no_book.condition_id = "other"
    mismatch = evaluate_complete_set(
        yes_book=yes_book,
        no_book=no_book,
        shares=D("5"),
        fee_rate=D("0.05"),
        min_net_return=D("0"),
    )
    assert not mismatch.tradeable
