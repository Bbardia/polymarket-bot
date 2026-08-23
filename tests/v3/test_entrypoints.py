import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PYTHON = ROOT / "venv" / "bin" / "python"


def test_legacy_live_entrypoint_is_permanently_disabled():
    result = subprocess.run(
        [str(PYTHON), "run_full_loop.py", "--live"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode != 0
    assert "Legacy live trading is permanently disabled" in result.stderr


def test_v3_validation_is_network_free_and_does_not_initialize_secure_client():
    env = os.environ.copy()
    env.update({
        "ENABLE_V3_LIVE_TRADING": "false",
        "PAPER_TRADING": "true",
        "V3_LIVE_CONFIRMATION": "",
    })
    result = subprocess.run(
        [str(PYTHON), "run_v3.py", "validate-config"],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "SDK: polymarket-client 0.6.0" in result.stdout
    assert "Mode: PAPER / LIVE CLIENT BLOCKED" in result.stdout
    assert "Secure client initialized: false" in result.stdout


def test_v3_cli_exposes_no_start_command():
    result = subprocess.run(
        [str(PYTHON), "run_v3.py", "--help"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "start" not in result.stdout.lower()
    assert "validate-config" in result.stdout


def test_all_legacy_side_effect_boundaries_are_hard_disabled():
    import run_full_loop
    from src.polymarket_client import PolymarketClient

    class EmptyPortfolio:
        positions = {}

    calls = [
        lambda: run_full_loop._execute_trade(None, "token", 0.1, 1.0, object()),
        lambda: run_full_loop.manage_weather_positions(EmptyPortfolio(), dry_run=False),
        lambda: run_full_loop._cleanup_phantom_positions(EmptyPortfolio(), dry_run=False),
        lambda: run_full_loop._auto_redeem_resolved(EmptyPortfolio(), dry_run=False),
        lambda: run_full_loop._sync_untracked_positions(EmptyPortfolio(), dry_run=False),
    ]
    for call in calls:
        with pytest.raises(RuntimeError, match="Legacy live trading is permanently disabled"):
            call()

    client = PolymarketClient()
    assert not client.init_trading_client()
    assert client.place_limit_order("token", 0.1, 5)["error"] == "Legacy live trading disabled"
    assert client.place_market_order("token", 1)["error"] == "Legacy live trading disabled"
