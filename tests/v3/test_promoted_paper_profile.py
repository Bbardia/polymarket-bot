"""The shipped main profile promotes hybrid paper, never live execution."""
from decimal import Decimal
from pathlib import Path

from run_v3 import load_paper_environment
from src.v3.paper import PaperSettings


def test_main_template_is_bounded_hybrid_paper(monkeypatch, tmp_path):
    import os
    monkeypatch.setattr(os, "environ", os.environ.copy())
    for key in tuple(os.environ):
        if key.startswith("V3_") or key.startswith("POLY_") or key == "PAPER_TRADING":
            monkeypatch.delenv(key, raising=False)
    template = Path(__file__).resolve().parents[2] / ".env.template"
    load_paper_environment(template)
    settings = PaperSettings.from_env(tmp_path)
    assert settings.early_exit_enabled
    assert settings.hybrid_exit_enabled
    assert settings.early_exit_target_return == Decimal("0.28")
    assert settings.hybrid_exit_fraction == Decimal("0.75")
    assert settings.hybrid_runner_target_return == Decimal("0.50")
    assert settings.max_open_positions == 5
    assert settings.weather_policy.max_open_positions == 5
    assert settings.max_realized_loss == Decimal("5")
    assert settings.max_drawdown_fraction == Decimal("0.10")
    assert settings.weather_policy.minimum_provider_count == 2
    assert not settings.complete_set_enabled
    assert os.environ["PAPER_TRADING"] == "true"
    assert os.environ["ENABLE_V3_LIVE_TRADING"] == "false"
    assert os.environ["ENABLE_V3_ACCOUNT_READS"] == "false"
    assert "POLY_PRIVATE_KEY" not in os.environ
