import pytest

from src.config import Config


def test_live_trading_refuses_template_placeholders(monkeypatch):
    monkeypatch.setattr(Config, "ENABLE_LIVE_TRADING", True)
    monkeypatch.setattr(Config, "PAPER_TRADING", False)
    monkeypatch.setattr(Config, "PRIVATE_KEY", "replace_me_with_local_private_key")
    monkeypatch.setattr(Config, "FUNDER_ADDRESS", "replace_me_with_local_funder_address")

    errors = Config.live_trading_errors()

    assert "POLY_PRIVATE_KEY is missing or still a placeholder" in errors
    assert "POLY_FUNDER_ADDRESS is missing or still a placeholder" in errors


def test_live_trading_requires_dual_opt_in(monkeypatch):
    monkeypatch.setattr(Config, "ENABLE_LIVE_TRADING", False)
    monkeypatch.setattr(Config, "PAPER_TRADING", True)
    monkeypatch.setattr(Config, "PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setattr(Config, "FUNDER_ADDRESS", "0x" + "2" * 40)

    with pytest.raises(RuntimeError, match="Live trading refused"):
        Config.assert_live_trading_allowed()


def test_live_trading_rejects_malformed_credentials(monkeypatch):
    monkeypatch.setattr(Config, "ENABLE_LIVE_TRADING", True)
    monkeypatch.setattr(Config, "PAPER_TRADING", False)
    monkeypatch.setattr(Config, "PRIVATE_KEY", "not-a-private-key")
    monkeypatch.setattr(Config, "FUNDER_ADDRESS", "not-an-address")

    errors = Config.live_trading_errors()

    assert "POLY_PRIVATE_KEY must be 0x followed by 64 hex characters" in errors
    assert "POLY_FUNDER_ADDRESS must be 0x followed by 40 hex characters" in errors


def test_live_trading_allowed_when_all_guards_configured(monkeypatch):
    monkeypatch.setattr(Config, "ENABLE_LIVE_TRADING", True)
    monkeypatch.setattr(Config, "PAPER_TRADING", False)
    monkeypatch.setattr(Config, "PRIVATE_KEY", "0x" + "1" * 64)
    monkeypatch.setattr(Config, "FUNDER_ADDRESS", "0x" + "2" * 40)

    assert Config.live_trading_errors() == []


def test_dry_run_housekeeping_returns_before_private_side_effects(monkeypatch):
    import run_full_loop

    class DummyPosition:
        market_type = "FORECAST"
        status = "OPEN"

    class DummyPortfolio:
        positions = {"p1": DummyPosition()}

    def fail_if_called():
        raise AssertionError("live-trading guard should not be needed in dry-run")

    monkeypatch.setattr(Config, "assert_live_trading_allowed", fail_if_called)

    assert run_full_loop.manage_weather_positions(DummyPortfolio(), dry_run=True, pm=None) == []
    assert run_full_loop._cleanup_phantom_positions(DummyPortfolio(), dry_run=True, pm=None) is None
    assert run_full_loop._auto_redeem_resolved(DummyPortfolio(), dry_run=True, pm=None) is None
    assert run_full_loop._sync_untracked_positions(DummyPortfolio(), dry_run=True) is None
