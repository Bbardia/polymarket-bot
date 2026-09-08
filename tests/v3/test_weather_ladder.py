from decimal import Decimal

from src.v3.math import BookLevel
from src.v3.weather_ladder import LadderBucket, evaluate_ladder


def bucket(key: str, degree: int, probability: str, price: str) -> LadderBucket:
    return LadderBucket(
        key=key,
        market_id=f"m-{key}",
        condition_id=f"c-{key}",
        token_id=f"t-{key}",
        question=f"bucket {degree}",
        lower_display=Decimal(degree),
        upper_display=Decimal(degree),
        model_probability=Decimal(probability),
        yes_asks=(BookLevel(Decimal(price), Decimal("10")),),
        minimum_size=Decimal("1"),
        fee_rate=Decimal("0"),
    )


def test_ladder_ev_and_outside_loss_are_explicit() -> None:
    result = evaluate_ladder(
        event_key="city:2026-09-10",
        unit="C",
        buckets=(
            bucket("a", 29, "0.20", "0.10"),
            bucket("b", 30, "0.45", "0.20"),
            bucket("c", 31, "0.20", "0.10"),
        ),
        min_cluster_probability=Decimal("0.70"),
        min_expected_profit=Decimal("0.10"),
    )
    assert result.tradeable is True
    assert result.total_cost == Decimal("0.40")
    assert result.cluster_probability == Decimal("0.85")
    assert result.expected_profit == Decimal("0.45")
    assert result.profit_if_selected_wins == Decimal("0.60")
    assert result.loss_if_outside_cluster == Decimal("-0.40")


def test_ladder_rejects_when_fees_break_selected_outcome_profit() -> None:
    result = evaluate_ladder(
        event_key="city:2026-09-10",
        unit="C",
        buckets=(
            LadderBucket(
                **{**bucket("a", 29, "0.30", "0.32").__dict__, "fee_rate": Decimal("0.10")}
            ),
            LadderBucket(
                **{**bucket("b", 30, "0.30", "0.32").__dict__, "fee_rate": Decimal("0.10")}
            ),
            LadderBucket(
                **{**bucket("c", 31, "0.30", "0.32").__dict__, "fee_rate": Decimal("0.10")}
            ),
        ),
        min_cluster_probability=Decimal("0.50"),
        min_expected_profit=Decimal("0"),
    )
    assert result.tradeable is False
    assert result.expected_profit < Decimal("0")
    assert result.profit_if_selected_wins < Decimal("0")


def test_ladder_rejects_gaps() -> None:
    result = evaluate_ladder(
        event_key="city:2026-09-10",
        unit="C",
        buckets=(
            bucket("a", 29, "0.30", "0.10"),
            bucket("c", 31, "0.30", "0.10"),
            bucket("d", 32, "0.30", "0.10"),
        ),
    )
    assert result.tradeable is False
    assert result.executable is False
    assert "adjacent" in result.reason
