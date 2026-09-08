"""Offline V7 budget, executable edge, and opt-in safety regressions."""
from dataclasses import replace
from decimal import Decimal as D

import pytest

from src.v3.math import BookLevel, execution_fee, execution_vwap
from src.v3.strategies.weather import WeatherMarketInput
from src.v3.weather_sizing import size_weather_entry
from src.v3.paper import PaperSettings, paper_status


def signal():
    return WeatherMarketInput(
        raw_probability=D('.8'), anchor_probability=D('.8'), n_members=100,
        intraclass_correlation=D('0'), best_bid=D('.09'), best_ask=D('.1'),
        fee_rate=D('.05'), lead_days=1, resolution_source_verified=True,
        prior_strength=D('0'), fractional_kelly=D('.05'), uncertainty_z=D('0'),
    )


def size(levels=None, **kwargs):
    return size_weather_entry(
        levels=levels or (BookLevel(D('.1'), D('100')),),
        market=kwargs.pop('market', signal()), minimum_shares=D('5'),
        bankroll=kwargs.pop('bankroll', D('37.50')),
        order_cap=kwargs.pop('order_cap', D('5')), **kwargs,
    )


def test_actual_all_in_kelly_not_fixed_minimum():
    result = size()
    assert result.accepted
    assert result.shares > D('5')
    assert result.all_in_cost <= result.budget <= D('5')
    assert result.budget == D('37.50') * result.decision.kelly_fraction
    assert result.fee == execution_fee((BookLevel(D('.1'), D('100')),), result.shares, D('.05'))


def test_minimum_is_skipped_never_rounded_up_over_budget():
    result = size(bankroll=D('1'))
    assert not result.accepted
    assert result.reason == 'kelly budget below venue minimum'
    assert result.shares == D('5')  # diagnostic quote, not an authorized size


def test_final_depth_fees_and_kelly_recomputed():
    levels = (BookLevel(D('.1'), D('5')), BookLevel(D('.4'), D('100')))
    result = size(levels)
    assert result.accepted
    assert result.shares > D('5')
    assert result.ask == execution_vwap(levels, result.shares).vwap
    assert result.fee == execution_fee(levels, result.shares, D('.05'))
    assert result.all_in_cost <= result.budget
    assert result.decision.net_edge == D('.8') - result.ask - result.fee / result.shares
    assert result.decision.net_edge >= result.decision.minimum_edge


def test_shallow_depth_caps_size_and_order_cap_includes_fees():
    assert size((BookLevel(D('.1'), D('6')),)).shares == D('6')
    result = size(order_cap=D('.60'))
    assert result.accepted and result.all_in_cost <= D('.60')
    assert result.shares < D('6')


def test_invalid_missing_bankroll_and_nonpositive_edge_fail_closed():
    for bankroll in (None, D('0'), D('NaN'), D('-1')):
        with pytest.raises(ValueError):
            size(bankroll=bankroll)
    result = size(market=replace(signal(), raw_probability=D('.1')))
    assert not result.accepted


def test_opt_in_roundtrip_and_invalid_flag(monkeypatch, tmp_path):
    monkeypatch.setenv('V3_PAPER_WEATHER_KELLY_SIZING_ENABLED', 'true')
    settings = PaperSettings.from_env(tmp_path)
    assert settings.weather_policy.kelly_sizing_enabled
    assert settings.weather_policy.sizing_bankroll is None
    status = paper_status(settings)
    assert status['weather_kelly_sizing_enabled'] is True
    assert status['state'] == 'not_started'
    monkeypatch.setenv('V3_PAPER_WEATHER_KELLY_SIZING_ENABLED', 'tru')
    with pytest.raises(ValueError):
        PaperSettings.from_env(tmp_path)


@pytest.mark.parametrize('flag,value', [('PAPER_TRADING', 'false'), ('ENABLE_V3_LIVE_TRADING', 'true'), ('ENABLE_V3_ACCOUNT_READS', 'true')])
def test_v7_refuses_unsafe_flags_at_settings_construction(monkeypatch, tmp_path, flag, value):
    monkeypatch.setenv('V3_PAPER_WEATHER_KELLY_SIZING_ENABLED', 'true')
    monkeypatch.setenv(flag, value)
    with pytest.raises(ValueError, match='paper-only'):
        PaperSettings.from_env(tmp_path)


def test_worker_sizing_audit_cash_recheck_and_restart(tmp_path):
    import asyncio
    from datetime import datetime, timezone
    from types import SimpleNamespace
    from test_paper import event_weather_market, event_worker
    from src.v3.paper import PaperStore, PaperWorker

    first = event_weather_market('kelly-one', '31°C', '.1')
    second = event_weather_market('kelly-two', '31°C', '.1')
    second.question = second.question.replace('Singapore', 'Tokyo')
    second.resolution.source = 'https://www.weather.gov/wrh/timeseries?site=rjtt'
    for item in (first, second):
        item.trading.fees_enabled = True
        item.trading.fee_schedule = SimpleNamespace(rate=D('.05'))
        item.trading.base_fee = 1000  # Legacy metadata must not become rate=.1.
    worker, store = event_worker(tmp_path, [first, second], {(D('31'), D('31')): D('.8')})
    worker.settings = replace(worker.settings, weather_policy=replace(
        worker.settings.weather_policy, kelly_sizing_enabled=True,
        base_edge=D('.03'), prior_strength=D('0'), uncertainty_z=D('0'),
    ))
    result = asyncio.run(worker.run_cycle(now=datetime(2026, 8, 24, 0, tzinfo=timezone.utc)))
    assert result.paper_trades == 1
    candidates = store.read_records(store.candidates_path)
    assert len(candidates) == 2
    traded = next(row for row in candidates if row['paper_executed'])
    skipped = next(row for row in candidates if not row['paper_executed'])
    assert D(traded['shares']) > D('5')
    assert traded['sizing_mode'] == 'fractional_kelly'
    assert traded['fee_rate'] == '.05' or D(traded['fee_rate']) == D('.05')
    assert D(traded['all_in_cost']) <= D(traded['sizing_budget'])
    assert D(skipped['execution_sizing_budget']) < D(skipped['sizing_budget'])
    assert skipped['paper_reason'] == 'kelly budget reduced since scan; skip without upsizing'
    assert store.read_status()['weather_kelly_sizing_enabled'] is True
    persisted = store.read_records(store.trades_path)
    assert persisted[0]['sizing_mode'] == 'fractional_kelly'
    restarted = PaperWorker(client=worker.client, settings=worker.settings,
        store=PaperStore(tmp_path), forecast=worker.forecast)
    assert restarted.state.cash == worker.state.cash
    assert restarted.state.open_positions == worker.state.open_positions


def test_v6_default_stays_venue_minimum_and_v7_missing_bankroll_blocks():
    import asyncio
    from datetime import datetime, timezone
    from test_paper_weather import weather_market, book, FakePublicClient, FakeForecast
    from src.v3.paper_weather import WeatherPaperPolicy, evaluate_weather_universe
    item = weather_market()
    client = FakePublicClient([item], [book('yes-weather-1', bid='.09', ask='.1'), book('no-weather-1', bid='.89', ask='.9')])
    policy = WeatherPaperPolicy(prior_strength=D('0'), uncertainty_z=D('0'))
    def evaluate(policy):
        return asyncio.run(evaluate_weather_universe(client=client, forecast=FakeForecast(),
            policy=policy, now=datetime(2026, 8, 24, tzinfo=timezone.utc)))
    baseline = evaluate(policy)
    assert any(row.paper_tradeable and row.shares == D('5') for row in baseline.evaluations)
    enabled = evaluate(replace(policy, kelly_sizing_enabled=True))
    assert not any(row.paper_tradeable for row in enabled.evaluations)
    assert any('bankroll' in row.paper_reason for row in enabled.evaluations)

