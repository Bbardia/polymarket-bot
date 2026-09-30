from datetime import datetime, timezone

import pytest

from src.v3.paper_weather import (
    _deterministic_forecast,
    _ensemble_probability,
    forecast_sigma_floor_c,
    parse_high_temperature_contract,
)

NOW = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)
END = datetime(2026, 8, 25, 12, tzinfo=timezone.utc)


def _contract(outcome: str):
    contract = parse_high_temperature_contract(
        f"Will the highest temperature in Miami be {outcome} on August 25?",
        end_date=END,
    )
    assert contract is not None
    return contract


def _f_to_c(value_f: float) -> float:
    return (value_f - 32) * 5 / 9


def test_sigma_floor_grows_with_lead_and_is_wider_for_deterministic():
    for lead in range(4):
        assert forecast_sigma_floor_c(lead, deterministic=True) > forecast_sigma_floor_c(
            lead, deterministic=False,
        )
        assert forecast_sigma_floor_c(lead + 1, deterministic=True) > forecast_sigma_floor_c(
            lead, deterministic=True,
        )
    assert forecast_sigma_floor_c(-3, deterministic=True) == forecast_sigma_floor_c(
        0, deterministic=True,
    )


def test_deterministic_forecast_is_not_overconfident_on_exact_bucket():
    # A single value centred on an exact 1F bucket used to get a 0.5C sigma,
    # putting ~43% on one 0.56C-wide bucket. Realistic day-1 errors cap it
    # well below that.
    result = _deterministic_forecast(
        _contract("80°F"), _f_to_c(80), now=NOW, source="nws",
    )
    assert result.lead_days == 1
    assert 0.08 < float(result.raw_probability) < 0.20
    assert float(result.ensemble_std_c) == pytest.approx(
        forecast_sigma_floor_c(1, deterministic=True),
    )


def test_deterministic_forecast_keeps_meaningful_neighbour_probability():
    # Two degrees F away used to be priced near zero (tail of a 0.5C normal),
    # manufacturing "edge" on NO for neighbouring buckets.
    result = _deterministic_forecast(
        _contract("82°F"), _f_to_c(80), now=NOW, source="nws",
    )
    assert float(result.raw_probability) > 0.05


def test_tight_ensemble_is_floored_but_wide_ensemble_keeps_its_spread():
    contract = _contract("80°F")
    centre = _f_to_c(80)
    tight = _ensemble_probability(contract, ((centre - 0.05, centre + 0.05),), 1)
    assert float(tight.ensemble_std_c) == pytest.approx(
        forecast_sigma_floor_c(1, deterministic=False),
    )
    wide_members = tuple(centre + offset for offset in (-4.0, -2.0, 0.0, 2.0, 4.0))
    wide = _ensemble_probability(contract, (wide_members,), 1)
    assert float(wide.ensemble_std_c) > forecast_sigma_floor_c(1, deterministic=False)
