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
    result = model.probability(
        {"question": "Will inflation reach more than 4% in 2026?", "groupItemTitle": "Above 4%"},
        "US inflation 2026",
        "Resolves per the U.S. Bureau of Labor Statistics CPI-U release.",
    )
    assert result is not None
    assert result[0] == 1.0
    assert result[2]["probability_whitelist_reason"] == "observed_threshold_already_exceeded"


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


# ----------------------------------------------------------- remediation P0 --

import json
from datetime import datetime, timedelta, timezone

import pytest

from src.v3.sanity import SanityError
from src.v8.worker import V8Worker


def _bls_payload(rows):
    return {"Results": {"series": [{"data": rows}]}}


def test_bls_payload_is_newest_first_and_latest_print_is_used() -> None:
    # Shape per BLS API v2 (newest at index 0); values chosen so a naive
    # ``known[-1]`` on insertion order would read January instead of July.
    rows = [
        {"year": "2026", "period": "M07", "value": "310.0"},
        {"year": "2026", "period": "M01", "value": "300.0"},
        {"year": "2025", "period": "M07", "value": "300.0"},
        {"year": "2025", "period": "M01", "value": "293.0"},
    ]
    yoy = CPIModel.yoy_from_payload(_bls_payload(rows))
    assert list(yoy) == [(2026, 1), (2026, 7)]
    assert round(yoy[(2026, 7)], 4) == round(10.0 / 3.0, 4)
    model = CPIModel(PublicHTTP())
    model.yoy = {(2026, 7): 3.36, (2026, 1): 2.39}  # deliberately unsorted insertion
    model.cached_at = time.time()
    result = model.probability(
        {"question": "Will inflation reach more than 4.5% in 2026?", "groupItemTitle": ""},
        "US inflation 2026", "Bureau of Labor Statistics CPI-U",
    )
    assert result is not None
    assert result[2]["latest_known_period"] == "2026-07"
    assert result[2]["known_yoy"][-1] == 3.36


def test_bls_payload_with_wrong_declared_ordering_fails_closed() -> None:
    rows = [
        {"year": "2026", "period": "M01", "value": "300.0"},
        {"year": "2026", "period": "M07", "value": "310.0"},
    ]
    with pytest.raises(SanityError):
        CPIModel.yoy_from_payload(_bls_payload(rows))


def test_cpi_geography_fails_closed_for_argentina_and_unnamed_geography() -> None:
    model = CPIModel(PublicHTTP())
    model.yoy = {(2026, 1): 4.2}
    model.cached_at = time.time()
    # Real question text from the V8 snapshot (198 rows were priced with US CPI).
    assert model.probability(
        {"question": "Will Argentina’s annual inflation in 2026 be at least 45%?", "groupItemTitle": ""},
        "Argentina annual inflation 2026", "",
    ) is None
    assert model.probability(
        {"question": "Will inflation reach more than 4.5% in 2026?", "groupItemTitle": ""}, "", "",
    ) is None


def test_fed_predicates_route_count_buckets_separately_from_any_cut() -> None:
    model = FedModel(PublicHTTP())
    model.cached_at = time.time()
    model.meetings = [
        {"event": "SEP", "month": 9, "none": 0.5, "cut25": 0.5, "cut50": 0.0},
        {"event": "DEC", "month": 12, "none": 0.5, "cut25": 0.5, "cut50": 0.0},
    ]
    model.distribution = {0: 0.25, 1: 0.5, 2: 0.25}
    model.source = "test"
    exact_one = model.probability({"question": "Will 1 Fed rate cut happen in 2026?", "groupItemTitle": "1"})
    by_december = model.probability({"question": "Fed rate cut by December 2026 meeting?", "groupItemTitle": ""})
    at_least_two = model.probability({"question": "Will there be 2+ Fed rate cuts in 2026?", "groupItemTitle": "2+"})
    assert exact_one is not None and abs(exact_one[0] - 0.5) < 1e-9
    assert by_december is not None and abs(by_december[0] - 0.75) < 1e-9
    assert at_least_two is not None and abs(at_least_two[0] - 0.25) < 1e-9
    assert exact_one[2]["predicate"] == "exact" and by_december[2]["predicate"] == "cut_by"
    assert FedModel.classify_predicate("How many Fed rate cuts in 2026?", "Several") is None


def test_fed_distribution_that_is_not_a_partition_is_rejected() -> None:
    model = FedModel(PublicHTTP())
    model.cached_at = time.time()
    model.meetings = [{"event": "DEC", "month": 12, "none": 0.5, "cut25": 0.5, "cut50": 0.0}]
    model.distribution = {0: 0.30, 1: 0.40, 2: 0.30, 3: 0.16}  # sums to 1.16
    with pytest.raises(SanityError):
        model.probability({"question": "Will 1 Fed rate cut happen in 2026?", "groupItemTitle": "1"})


def test_entries_disabled_by_default_blocks_paper_entry_but_records_telemetry(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("V8_ENTRIES_ENABLED", raising=False)
    settings = V8Settings.from_env(tmp_path)
    assert settings.entries_enabled is False
    worker = V8Worker(V8Settings(root=tmp_path, data_dir=tmp_path / "data"))
    worker.fed.cached_at = time.time()
    worker.fed.meetings = [{"event": "DEC", "month": 12, "none": 0.5, "cut25": 0.5, "cut50": 0.0}]
    worker.fed.distribution = {0: 0.5, 1: 0.5}
    worker.fed.source = "test"
    now_ms = str(int(datetime.now(timezone.utc).timestamp() * 1000))
    worker.book = lambda token_id: {  # type: ignore[method-assign]
        "asks": [{"price": "0.10", "size": "100"}],
        "bids": [{"price": "0.08", "size": "100"}],
        "timestamp": now_ms, "hash": "h",
    }
    market = {
        "id": "m1", "question": "Will 1 Fed rate cut happen in 2026?", "groupItemTitle": "1",
        "clobTokenIds": json.dumps(["yes", "no"]), "orderMinSize": 5, "feeSchedule": None,
    }
    record = worker.evaluate_market("macro", {"id": "e1", "title": "Fed decisions 2026"}, market, set(), {})
    assert record["status"] == "rejected"
    assert record["reason"] == "entries_disabled_by_profile"
    assert worker.state.positions == [] and worker.state.trades == 0
    assert record["quotes"]  # telemetry still recorded


def test_stale_thin_or_wide_books_are_hard_rejected(tmp_path: Path) -> None:
    worker = V8Worker(V8Settings(root=tmp_path, data_dir=tmp_path / "data", max_price_age_seconds=300))
    now = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
    fresh = str(int((now - timedelta(seconds=30)).timestamp() * 1000))
    stale = str(int((now - timedelta(seconds=3590)).timestamp() * 1000))
    good = {"asks": [{"price": "0.45", "size": "10"}], "bids": [{"price": "0.40", "size": "10"}], "timestamp": fresh}
    assert worker.book_block_reason(good, now=now) is None
    assert "stale" in worker.book_block_reason({**good, "timestamp": stale}, now=now)
    assert "untimestamped" in worker.book_block_reason({**good, "timestamp": None}, now=now)
    thin = {**good, "asks": [{"price": "0.45", "size": "4"}]}
    assert "venue minimum" in worker.book_block_reason(thin, now=now)
    wide = {**good, "bids": [{"price": "0.30", "size": "10"}]}
    assert "spread" in worker.book_block_reason(wide, now=now)


def test_strict_boolean_env_for_entries(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("V8_ENTRIES_ENABLED", "maybe")
    with pytest.raises(ValueError):
        V8Settings.from_env(tmp_path)


def _test_position():
    from src.v8.worker import Position
    return Position('p', 'macro', 'e', 'm', 'test', 'YES', 't', 5, .5, 0, 2.5, .6, .1, '2026-09-10T00:00:00Z')


def test_disputed_or_proposed_v8_never_settles(tmp_path):
    worker = V8Worker(V8Settings(root=tmp_path, data_dir=tmp_path))
    worker.state.positions = [_test_position()]
    cash = worker.state.cash
    for status in ('proposed', 'disputed', None):
        worker.http.get_json = lambda *a, **k: {'closed': True, 'umaResolutionStatus': status, 'outcomePrices': '["1", "0"]'}
        assert worker.settle_positions() == 0
        assert worker.state.cash == cash


def test_v8_mark_missing_book_zero_and_migrates_peak(tmp_path):
    worker = V8Worker(V8Settings(root=tmp_path, data_dir=tmp_path, initial_capital=50))
    worker.state.cash = 47.5
    worker.state.positions = [_test_position()]
    worker.market_detail = lambda market_id: {'feesEnabled': False, 'feeSchedule': None}
    worker.book = lambda token_id: {}
    mark = worker.mark_positions()
    assert mark['mark_equity'] == 47.5
    assert mark['gross_exposure'] == 2.5
    assert mark['unmarkable_legs'] == 1
    assert mark['peak_mark_equity'] == 50
    worker.state.peak_mark_equity = 60
    assert worker.risk_block_reason(0) == 'mark_drawdown_breaker'


def test_v8_partial_depth_is_telemetry_but_whole_leg_marks_zero(tmp_path):
    worker = V8Worker(V8Settings(root=tmp_path, data_dir=tmp_path, initial_capital=10))
    worker.state.cash = 7.5
    worker.state.positions = [_test_position()]
    worker.book = lambda token_id: {'timestamp': time.time() * 1000, 'bids': [{'price': '.5', 'size': '2'}]}
    worker.market_detail = lambda market_id: {
        'feesEnabled': True,
        'feeSchedule': {'rate': 0.05, 'exponent': 1, 'takerOnly': True},
    }
    mark = worker.mark_positions()
    assert mark['mark_legs'][0]['quoted_partial_value'] == pytest.approx(0.975)
    assert mark['mark_legs'][0]['unmarkable_shares'] == 3
    assert mark['mark_legs'][0]['value'] == 0
    assert mark['mark_equity'] == 7.5
    worker.state.peak_mark_equity = 7.5
    assert worker.risk_block_reason(1) == 'gross_exposure_cap'


def test_v8_mark_uses_market_fee_schedule_and_fails_closed_when_missing(tmp_path):
    worker = V8Worker(V8Settings(root=tmp_path, data_dir=tmp_path, initial_capital=10))
    worker.state.cash = 7.5
    worker.state.positions = [_test_position()]
    worker.book = lambda token_id: {
        'timestamp': time.time() * 1000,
        'bids': [{'price': '.5', 'size': '5'}],
    }
    worker.market_detail = lambda market_id: {
        'feesEnabled': True,
        'feeSchedule': {'rate': 0.05, 'exponent': 1, 'takerOnly': True},
    }
    mark = worker.mark_positions()
    assert mark['mark_legs'][0]['value'] == pytest.approx(2.4375)
    assert mark['mark_legs'][0]['fee_source'] == 'gamma_market_fee_schedule'

    worker.market_detail = lambda market_id: {'feesEnabled': True, 'feeSchedule': None}
    mark = worker.mark_positions()
    assert mark['mark_legs'][0]['value'] == 0
    assert mark['mark_legs'][0]['mark_error'] == 'enabled_fee_schedule_missing'
