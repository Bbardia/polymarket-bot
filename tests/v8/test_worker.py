from __future__ import annotations

import time
from pathlib import Path

from src.v8.worker import CPIModel, ClubEloModel, FedModel, PublicHTTP, V8Settings, fee_for, soccer_probabilities


def test_fee_is_zero_for_non_taker_schedule() -> None:
    assert fee_for(5, 0.4, {"rate": 0.05, "exponent": 1, "takerOnly": False}) == 0


def test_fee_is_applied_at_consumed_price() -> None:
    assert round(fee_for(5, 0.4, {"rate": 0.05, "exponent": 1, "takerOnly": True}), 8) == 0.06


def test_poisson_sports_probabilities_sum_to_one() -> None:
    probabilities = soccer_probabilities(1.4, 1.1)
    assert set(probabilities) == {"home", "draw", "away"}
    assert abs(sum(probabilities.values()) - 1.0) < 1e-9
    assert all(0 < value < 1 for value in probabilities.values())


def test_cpi_model_uses_observed_threshold_without_network() -> None:
    model = CPIModel(PublicHTTP())
    model.yoy = {(2026, 1): 4.2, (2026, 2): 4.4, (2026, 3): 4.1}
    model.cached_at = time.time()
    result = model.probability({"question": "Will inflation reach more than 4% in 2026?", "groupItemTitle": "Above 4%"})
    assert result is not None
    assert result[0] == 1.0


def test_cpi_model_does_not_apply_us_data_to_foreign_market() -> None:
    model = CPIModel(PublicHTTP())
    model.yoy = {(2026, 1): 4.2}
    model.cached_at = time.time()
    assert model.probability(
        {"question": "Will Canada's 2026 inflation be at least 4%?", "groupItemTitle": "Above 4%"},
        "Canada Annual Inflation 2026",
    ) is None


def test_fed_cut_by_month_uses_only_meetings_through_cutoff() -> None:
    model = FedModel(PublicHTTP())
    model.cached_at = time.time()
    model.meetings = [
        {"event": "SEP", "month": 9, "none": 0.9, "cut25": 0.1, "cut50": 0.0},
        {"event": "OCT", "month": 10, "none": 0.5, "cut25": 0.5, "cut50": 0.0},
    ]
    model.source = "test"
    result = model.probability({"question": "Fed rate cut by September 2026 meeting?", "groupItemTitle": ""})
    assert result is not None
    assert abs(result[0] - 0.1) < 1e-9


def test_sports_spread_is_not_match_result() -> None:
    model = ClubEloModel(PublicHTTP())
    assert model.outcome_probability(
        "Seattle Sounders FC vs. Real Salt Lake", "Spread: Real Salt Lake (-1.5)"
    ) is None


def test_settings_are_public_paper_only(tmp_path: Path) -> None:
    settings = V8Settings(root=tmp_path, data_dir=tmp_path / "data")
    assert settings.safety()["paper_trading"] is True
    assert settings.safety()["account_reads"] is False
    assert settings.safety()["live_trading"] is False
    assert settings.safety()["authenticated_client"] is False
