import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.v3 import live_shadow
from src.v3.config import V3Settings
from src.v3.live_shadow import (
    LiveShadowRunner,
    LiveShadowSettings,
    ShadowStore,
    risk_limits_for,
    select_v7_candidates,
)
from src.v3.reconciliation import RemoteSnapshot
from src.v3.v7_weather_intent import V7WeatherOrderProposal

D = Decimal
NOW = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)


def _settings(**overrides):
    values = dict(max_capital=D("100"), max_order_notional=D("2"), reserve_fraction=D("0.25"),
                  max_daily_loss=D("10"), max_drawdown_amount=D("10"))
    values.update(overrides)
    return V3Settings(**values)


def _evaluation(event_key, edge, *, tradeable=True, strategy="weather_directional"):
    return SimpleNamespace(
        strategy=strategy, event_key=event_key, paper_tradeable=tradeable,
        decision=SimpleNamespace(net_edge=D(edge), calibrated_probability=D("0.30"),
                                 minimum_edge=D("0.05")),
        condition_id=f"cond-{event_key}-{edge}", token_id=f"tok-{event_key}-{edge}",
        question="Will it be hot?", side="YES", city="toronto", shares=D("10"),
        ask=D("0.20"), bid=D("0.18"),
        maker_shadow=SimpleNamespace(best_bid=D("0.18"), best_ask=D("0.20")),
        book_timestamp=NOW, book_hash="h1", decision_timestamp=NOW,
    )


def test_selection_keeps_best_edge_per_event_and_ranks():
    picked = select_v7_candidates((
        _evaluation("a", "0.05"), _evaluation("a", "0.09"), _evaluation("b", "0.07"),
        _evaluation("c", "0.20", tradeable=False),
        _evaluation("d", "0.30", strategy="weather_ladder"),
    ))
    assert [(item.event_key, item.decision.net_edge) for item in picked] == [
        ("a", D("0.09")), ("b", D("0.07")),
    ]


def test_risk_limits_match_live_settings_and_require_loss_stops(tmp_path):
    limits = risk_limits_for(_settings(), LiveShadowSettings(data_dir=tmp_path))
    assert (limits.max_capital, limits.max_order_notional, limits.reserve_fraction,
            limits.daily_loss_limit, limits.max_drawdown_amount) == (
        D("100"), D("2"), D("0.25"), D("10"), D("10"))
    with pytest.raises(ValueError):
        risk_limits_for(_settings(max_daily_loss=None), LiveShadowSettings(data_dir=tmp_path))


def test_settings_reject_short_ttl(tmp_path):
    with pytest.raises(ValueError):
        LiveShadowSettings(data_dir=tmp_path, order_ttl_seconds=60)


def test_store_seed_never_overwrites(tmp_path):
    seed = tmp_path / "seed"
    seed.mkdir()
    (seed / "station-metadata.json").write_text("new")
    store = ShadowStore(tmp_path / "live")
    (store.data_dir / "station-metadata.json").write_text("existing")
    assert store.seed(seed) == []
    assert (store.data_dir / "station-metadata.json").read_text() == "existing"


class _FakeAPI:
    def __init__(self, cash="150"):
        self.cash = D(cash)
        self.submitted = []

    async def fetch_remote_snapshot(self):
        return RemoteSnapshot(cash=self.cash, positions=(), open_orders=())

    async def fetch_account_trades(self, *, max_items, page_limit):
        return ()

    async def get_verified_market_context(self, condition_id, token_id):
        return SimpleNamespace(
            book_hash="h1", tick_size=D("0.01"), min_order_size=D("5"),
            accepting_orders=True, rules_verified=True, disputed=False,
        )


def _runner(tmp_path, monkeypatch, *, account_reads=True, proposal=None):
    evaluation = _evaluation("toronto:2026-10-04", "0.10")
    monkeypatch.setattr(live_shadow, "station_metadata_reason", lambda path, city: None)

    async def fake_universe(**kwargs):
        return SimpleNamespace(evaluations=(evaluation,), markets_evaluated=1,
                               forecast_status="available", errors=())

    monkeypatch.setattr(live_shadow, "evaluate_weather_universe", fake_universe)
    monkeypatch.setattr(live_shadow, "propose_v7_weather_order", lambda *a, **k: proposal or (
        V7WeatherOrderProposal(True, "ok", price=D("0.19"), shares=D("10"),
                               expected_edge=D("0.11"), quote_age_seconds=5)))
    store = ShadowStore(tmp_path / "live")
    return LiveShadowRunner(
        api=_FakeAPI(), settings=_settings(), shadow=LiveShadowSettings(data_dir=store.data_dir),
        store=store, weather_client=None, forecast=None, observation_provider=None,
        account_reads=account_reads,
    ), store


def test_cycle_logs_would_submit_without_any_order_call(tmp_path, monkeypatch):
    runner, store = _runner(tmp_path, monkeypatch)
    status = asyncio.run(runner.run_cycle(now=NOW))
    assert status["orders_submitted"] == 0
    assert status["secure_order_client_initialized"] is False
    assert status["shadow_outcomes_this_cycle"] == {"would_submit": 1}
    record = json.loads(store.intents_path.read_text().splitlines()[0])
    assert record["submitted"] is False and record["notional"] == "1.90"
    state = json.loads(store.state_path.read_text())
    assert state["baseline_cash"] == "150" and state["peak_equity"] == "150"


def test_risk_engine_blocks_order_over_cap(tmp_path, monkeypatch):
    big = V7WeatherOrderProposal(True, "ok", price=D("0.40"), shares=D("6"),
                                 expected_edge=D("0.1"), quote_age_seconds=5)
    runner, store = _runner(tmp_path, monkeypatch, proposal=big)
    status = asyncio.run(runner.run_cycle(now=NOW))
    assert status["shadow_outcomes_this_cycle"] == {"blocked": 1}
    record = json.loads(store.intents_path.read_text().splitlines()[0])
    assert record["reason"] == "order exceeds max order notional"


def test_public_only_cycle_stops_at_proposal(tmp_path, monkeypatch):
    runner, _ = _runner(tmp_path, monkeypatch, account_reads=False)
    status = asyncio.run(runner.run_cycle(now=NOW))
    assert status["shadow_outcomes_this_cycle"] == {"proposal_only": 1}
    assert "equity" not in status


def test_daily_loss_from_day_start_equity_blocks(tmp_path, monkeypatch):
    runner, store = _runner(tmp_path, monkeypatch)
    asyncio.run(runner.run_cycle(now=NOW))
    runner.api.cash = D("139")
    status = asyncio.run(runner.run_cycle(now=NOW))
    assert status["daily_pnl"] == D("-11")
    record = json.loads(store.intents_path.read_text().splitlines()[-1])
    # Unexplained cash drift trips strict reconciliation first; risk also refuses.
    assert record["outcome"] == "would_block"
    assert record["risk_allowed"] is False
    assert record["risk_reason"] == "daily loss limit reached"
