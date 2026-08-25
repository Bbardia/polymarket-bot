from decimal import Decimal

import pytest

from src.v3.math import BookLevel
from src.v3.weather_surface import SurfaceBucket, analyze_event_surface


def D(value: str) -> Decimal:
    return Decimal(value)


def bucket(
    key: str,
    lower: str | None,
    upper: str | None,
    probability: str,
    ask: str,
    *,
    size: str = "20",
    minimum_size: str = "5",
    fee_rate: str = "0",
) -> SurfaceBucket:
    return SurfaceBucket(
        key=key,
        lower_display=None if lower is None else D(lower),
        upper_display=None if upper is None else D(upper),
        model_probability=D(probability),
        yes_asks=(BookLevel(D(ask), D(size)),),
        minimum_size=D(minimum_size),
        fee_rate=D(fee_rate),
    )


def test_complete_weather_surface_prices_one_exhaustive_basket():
    result = analyze_event_surface(
        event_key="weather:test:2026-08-25",
        unit="C",
        buckets=(
            bucket("below", None, "29", "0.10", "0.10"),
            bucket("30", "30", "30", "0.20", "0.20"),
            bucket("31-32", "31", "32", "0.30", "0.30"),
            bucket("above", "33", None, "0.40", "0.25"),
        ),
    )

    assert result.complete_partition
    assert result.executable
    assert result.common_shares == D("5")
    assert result.model_probability_sum == D("1.00")
    assert result.model_probability_residual == D("0.00")
    assert result.gross_cost == D("4.25")
    assert result.fees == D("0")
    assert result.payout == D("5")
    assert result.net_profit == D("0.75")
    assert result.tradeable


def test_weather_surface_rejects_gaps_and_overlaps():
    gap = analyze_event_surface(
        event_key="weather:gap",
        unit="F",
        buckets=(
            bucket("below", None, "79", "0.5", "0.4"),
            bucket("above", "81", None, "0.5", "0.4"),
        ),
    )
    assert not gap.complete_partition
    assert "gap" in gap.reason
    assert gap.partition_violations == ("gap:below:above",)
    assert gap.monotonic_violations == ()
    assert not gap.tradeable

    overlap = analyze_event_surface(
        event_key="weather:overlap",
        unit="F",
        buckets=(
            bucket("below", None, "79", "0.3", "0.2"),
            bucket("80", "80", "80", "0.2", "0.2"),
            bucket("80-81", "80", "81", "0.2", "0.2"),
            bucket("above", "82", None, "0.3", "0.2"),
        ),
    )
    assert not overlap.complete_partition
    assert "overlap" in overlap.reason
    assert overlap.partition_violations == ("overlap:80:80-81",)
    assert overlap.monotonic_violations == ()
    assert not overlap.tradeable


def test_weather_surface_refuses_partial_depth_at_common_size():
    result = analyze_event_surface(
        event_key="weather:thin",
        unit="C",
        buckets=(
            bucket("below", None, "29", "0.5", "0.4", size="4"),
            bucket("above", "30", None, "0.5", "0.4", minimum_size="5"),
        ),
    )

    assert result.complete_partition
    assert not result.executable
    assert "liquidity" in result.reason
    assert not result.tradeable


def test_weather_surface_sums_per_level_nonlinear_fees():
    result = analyze_event_surface(
        event_key="weather:fees",
        unit="C",
        buckets=(
            bucket("below", None, "29", "0.5", "0.40", fee_rate="0.05"),
            bucket("above", "30", None, "0.5", "0.40", fee_rate="0.05"),
        ),
    )

    expected_fee = D("5") * D("0.05") * D("0.40") * D("0.60") * D("2")
    assert result.fees == expected_fee
    assert result.net_profit == D("5") - D("4") - expected_fee


def test_surface_bucket_rejects_invalid_probability_and_bounds():
    with pytest.raises(ValueError, match="probability"):
        bucket("bad", "1", "1", "1.1", "0.5")
    with pytest.raises(ValueError, match="bounds"):
        bucket("bad", "2", "1", "0.5", "0.5")


def test_single_bucket_surface_is_reported_as_incomplete_not_profitable():
    result = analyze_event_surface(
        event_key="weather:singleton",
        unit="C",
        buckets=(bucket("only", "30", "30", "0.4", "0.1"),),
    )
    assert not result.complete_partition
    assert not result.executable
    assert not result.tradeable
    assert result.net_profit == D("0")
    assert result.partition_violations == (
        "missing_lower_tail",
        "missing_upper_tail",
    )
    assert result.monotonic_violations == ()


def test_weather_surface_reports_only_comparable_tail_price_monotonicity():
    result = analyze_event_surface(
        event_key="weather:tail-order",
        unit="C",
        buckets=(
            bucket("lower-29", None, "29", "0.2", "0.60"),
            bucket("lower-30", None, "30", "0.3", "0.50"),
            bucket("middle", "31", "31", "0.2", "0.99"),
            bucket("upper-32", "32", None, "0.2", "0.40"),
            bucket("upper-33", "33", None, "0.1", "0.50"),
        ),
    )

    assert result.monotonic_violations == (
        "lower_tail_price_inversion:lower-29:lower-30",
        "upper_tail_price_inversion:upper-32:upper-33",
    )
    assert all("middle" not in violation for violation in result.monotonic_violations)
