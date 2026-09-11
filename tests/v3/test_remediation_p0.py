"""Regression tests for the 2026-09 remediation P0 items 1-5."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.v3.marking import mark_leg, mark_portfolio, migrated_peak
from src.v3.math import BookLevel
from src.v3.paper import PaperSettings, PaperState, PaperStore, PaperWorker
from src.v3.resolver import (
    StationMetadata,
    StationRecord,
    parse_resolver_identity,
    verify_station_for_city,
)
from src.v3.sanity import (
    SanityError,
    assert_newest_at_index,
    assert_partition_sums_to_one,
    book_hard_reject,
    check_price_age,
    check_probability,
    geography_gate,
    mid_as_probability,
)
from test_paper import FakePublicClient, book, market, settings  # same test directory

D = Decimal


# ---------------------------------------------------------------- item 1 -----

def test_entries_are_disabled_by_default_in_settings_and_env(tmp_path, monkeypatch):
    for key in ("V3_PAPER_ENTRIES_ENABLED",):
        monkeypatch.delenv(key, raising=False)
    assert PaperSettings(data_dir=tmp_path).entries_enabled is False
    assert PaperSettings.from_env(tmp_path).entries_enabled is False
    monkeypatch.setenv("V3_PAPER_ENTRIES_ENABLED", "maybe")
    with pytest.raises(ValueError):
        PaperSettings.from_env(tmp_path)


def test_entries_disabled_keeps_settlement_and_marking_running(tmp_path):
    resolved_market = market()
    client = FakePublicClient(
        [resolved_market],
        [book("yes-token", ask="0.45"), book("no-token", ask="0.45")],
    )
    store = PaperStore(tmp_path)
    worker = PaperWorker(client=client, settings=settings(tmp_path), store=store)
    asyncio.run(worker.run_cycle())
    assert len(store.load_state().open_positions) == 1

    frozen = PaperWorker(
        client=client, settings=settings(tmp_path, entries_enabled=False), store=PaperStore(tmp_path)
    )
    resolved_market.state.closed = True
    resolved_market.state.accepting_orders = False
    resolved_market.outcomes.yes.price = D("1")
    resolved_market.outcomes.no.price = D("0")
    result = asyncio.run(frozen.run_cycle())
    assert result.settlements == 1
    assert result.paper_trades == 0
    status = json.loads((tmp_path / "status.json").read_text())
    assert status["paper_entry_block_reason"] == "paper entries disabled by profile"
    assert status["mark_equity"] is not None


# ---------------------------------------------------------------- item 2 -----

def test_mark_leg_walks_bid_depth_and_counts_unmarkable_remainder():
    bids = (BookLevel(D("0.40"), D("3")), BookLevel(D("0.30"), D("1")))
    leg = mark_leg(
        key="p", token_id="t", shares=D("5"), all_in_cost=D("2.5"), bids=bids, fee_rate=D("0.05"),
    )
    assert leg.marked_shares == D("4")
    assert leg.unmarkable_shares == D("1")
    assert leg.gross_notional == D("1.50")
    # fee = 0.05 * (3*0.4*0.6 + 1*0.3*0.7)
    assert leg.exit_fee == D("0.05") * (D("3") * D("0.4") * D("0.6") + D("1") * D("0.3") * D("0.7"))
    assert leg.value == leg.gross_notional - leg.exit_fee
    assert leg.partial and leg.book_available


def test_mark_leg_without_book_is_zero_and_flagged_with_conservative_fee():
    leg = mark_leg(key="p", token_id="t", shares=D("5"), all_in_cost=D("2"), bids=None, fee_rate=None)
    assert leg.value == D("0")
    assert not leg.book_available
    assert leg.fee_rate_assumed and leg.fee_rate == D("0.05")
    portfolio = mark_portfolio(cash=D("10"), legs=[leg])
    assert portfolio.mark_equity == D("10")
    assert portfolio.unmarkable_legs == 1
    assert portfolio.gross_exposure == D("2")


def test_peak_migration_never_defaults_to_initial_cash_alone():
    peak, migrated = migrated_peak(
        stored_peak=None, initial_cash=D("37.5"), peak_entry_equity=D("40.97"), current_mark_equity=D("34.35"),
    )
    assert migrated and peak == D("40.97")
    peak, migrated = migrated_peak(
        stored_peak=D("41"), initial_cash=D("37.5"), peak_entry_equity=D("40.97"), current_mark_equity=D("42"),
    )
    assert not migrated and peak == D("42")


def test_legacy_state_migrates_peak_and_marks_open_legs_to_bids(tmp_path):
    store = PaperStore(tmp_path)
    legacy = PaperState.new(D("37.5")).to_json()
    for key in ("peak_mark_equity", "peak_mark_equity_migrated", "last_mark_equity",
                "realized_settlement_pnl", "realized_exit_pnl", "settlement_failures"):
        legacy.pop(key)
    legacy["cash"] = "30"
    legacy["peak_entry_equity"] = "40"
    legacy["open_positions"] = {
        "condition-1": {
            "strategy": "weather_directional", "market_id": "market-1", "condition_id": "condition-1",
            "token_id": "yes-token", "shares": "5", "all_in_cost": "2.5", "side": "YES",
            "model_probability": "0.6",
        },
    }
    store.save_state(PaperState.from_json(legacy))
    client = FakePublicClient([market()], [book("yes-token", ask="0.45", bid="0.40")])
    worker = PaperWorker(client=client, settings=settings(tmp_path, complete_set_enabled=False), store=store)
    mark = asyncio.run(worker._mark_positions("2026-09-10T00:00:00+00:00"))
    # 5 shares at 0.40 = 2.00 notional, fee 0.05*5*0.4*0.6 = 0.06 -> 1.94
    assert mark.position_value == D("1.94")
    assert mark.mark_equity == D("31.94")
    state = worker.state
    assert state.peak_mark_equity_migrated is True
    assert state.peak_mark_equity == D("40")
    assert worker._mark_drawdown() == (D("40") - D("31.94")) / D("40")


def test_mark_drawdown_breaker_and_gross_exposure_cap_block_entries(tmp_path):
    client = FakePublicClient([market()], [book("yes-token", ask="0.45"), book("no-token", ask="0.45")])
    worker = PaperWorker(
        client=client,
        settings=settings(tmp_path, max_mark_drawdown_fraction=D("0.05"), max_gross_exposure_fraction=D("0.30")),
        store=PaperStore(tmp_path),
    )
    worker.state.peak_mark_equity = D("40")
    worker.state.cash = D("37.5")
    asyncio.run(worker._mark_positions("t"))  # no positions -> equity 37.5, drawdown 6.25%
    assert worker._entry_block_reason() == "paper mark-to-market drawdown breaker reached"

    worker.state.peak_mark_equity = D("37.5")
    assert worker._entry_block_reason() is None
    # 30% of 37.5 = 11.25 cap; a 12 pUSD prospective fill is refused.
    assert worker._entry_block_reason(prospective_cost=D("12")) == "paper gross exposure cap reached"
    assert worker._entry_block_reason(prospective_cost=D("11")) is None


# ---------------------------------------------------------------- item 3 -----

def test_settlement_retries_are_bounded_and_record_provenance(tmp_path):
    resolved_market = market()
    client = FakePublicClient([resolved_market], [book("yes-token", ask="0.45"), book("no-token", ask="0.45")])
    store = PaperStore(tmp_path)
    worker = PaperWorker(
        client=client,
        settings=settings(tmp_path, settlement_max_attempts=3, settlement_retry_delay_seconds=0.0),
        store=store,
    )
    asyncio.run(worker.run_cycle())
    resolved_market.state.closed = True
    resolved_market.outcomes.yes.price = D("1")
    resolved_market.outcomes.no.price = D("0")

    calls = {"n": 0}
    original = client.get_market

    async def flaky(*, id):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("TransportError")
        return await original(id=id)

    client.get_market = flaky
    result = asyncio.run(worker.run_cycle())
    assert result.settlements == 1
    row = [r for r in store.read_records(store.settlements_path) if r.get("status") != "settlement_error"][0]
    assert row["settlement_attempts"] == 3
    assert row["settlement_source"] == "gamma-public-market-outcome-prices"
    assert worker.state.realized_settlement_pnl == D("0.50")
    assert worker.state.realized_exit_pnl == D("0")


def test_repeated_settlement_failures_flag_stuck_position_without_write_off(tmp_path):
    client = FakePublicClient([market()], [book("yes-token", ask="0.45"), book("no-token", ask="0.45")])
    store = PaperStore(tmp_path)
    worker = PaperWorker(
        client=client,
        settings=settings(tmp_path, settlement_max_attempts=1, settlement_stuck_after_failures=2),
        store=store,
    )
    asyncio.run(worker.run_cycle())
    client.get_market_error = ConnectionError("TransportError")
    asyncio.run(worker.run_cycle())
    asyncio.run(worker.run_cycle())
    assert worker.state.settlement_failures["condition-1"] == 2
    status = json.loads((tmp_path / "status.json").read_text())
    assert status["stuck_positions"][0]["condition_id"] == "condition-1"
    assert status["stuck_positions"][0]["action"] == "flagged; no automatic write-off"
    assert "condition-1" in worker.state.open_positions
    assert status["realized_pnl_publishable"] is False
    assert status["ledger_pnl_reconciles_state"] is True


# ---------------------------------------------------------------- item 4 -----

BLS_NEWEST_FIRST = [  # shape per BLS API v2 docs: newest print at index 0
    {"year": "2026", "period": "M07", "value": "331.0"},
    {"year": "2026", "period": "M06", "value": "330.1"},
    {"year": "2026", "period": "M01", "value": "325.2"},
]


def _bls_key(item):
    return (int(item["year"]), int(item["period"][1:]))


def test_bls_newest_first_ordering_assert_rejects_last_index():
    assert assert_newest_at_index(BLS_NEWEST_FIRST, newest_index=0, key=_bls_key)["period"] == "M07"
    with pytest.raises(SanityError):
        assert_newest_at_index(BLS_NEWEST_FIRST, newest_index=-1, key=_bls_key)


def test_clamp_and_flag_rejects_extremes_without_whitelist():
    assert check_probability("0.999").flagged
    assert check_probability("0.001").flagged
    assert not check_probability("0.999", whitelist_reason="resolver_certain_observation_bound").flagged
    assert check_probability("1.2").flagged and check_probability("1.2").value == D("1")
    assert not check_probability("0.5").flagged


def test_partition_vectors_must_sum_to_one():
    with pytest.raises(SanityError):  # the Fed cut-count vector that summed to 1.16
        assert_partition_sums_to_one([0.30, 0.40, 0.30, 0.16], name="fed cuts")
    with pytest.raises(SanityError):  # ladder pmf sum 0.435 from a censored tape
        assert_partition_sums_to_one([0.1, 0.2, 0.135], name="ladder pmf")
    assert assert_partition_sums_to_one(["0.25", "0.25", "0.5"]) == D("1")


def test_price_age_over_decision_interval_fails():
    now = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
    with pytest.raises(SanityError):  # C1 price age of 3,590 s against a 300 s interval
        check_price_age(decision_at=now, price_at=now - timedelta(seconds=3590), max_age_seconds=300)
    assert check_price_age(decision_at=now, price_at=now - timedelta(seconds=120), max_age_seconds=300) == 120
    with pytest.raises(SanityError):
        check_price_age(decision_at=now, price_at=None, max_age_seconds=300)


def test_wide_mids_are_not_probabilities_and_thin_books_are_rejected():
    assert mid_as_probability(bid="0.40", ask="0.50") is None  # 22% relative spread
    assert mid_as_probability(bid="0.45", ask="0.50") == D("0.475")
    assert book_hard_reject(best_bid="0.40", best_ask="0.45", best_ask_size="4") is not None
    assert book_hard_reject(best_bid="0.30", best_ask="0.45", best_ask_size="10") is not None
    assert book_hard_reject(best_bid="0.40", best_ask="0.45", best_ask_size="10") is None


def test_geography_gate_refuses_us_series_on_argentina_market():
    title = "Will Argentina’s annual inflation in 2026 be at least 45%?"
    assert geography_gate(title, required_markers=("united states", "u.s.", "bls")) is not None
    assert geography_gate("U.S. CPI above 3% in 2026?", required_markers=("u.s.",)) is None


# ---------------------------------------------------------------- item 5 -----

def test_resolver_identity_parses_metar_and_refuses_other_authorities():
    ok = parse_resolver_identity("https://www.weather.gov/wrh/timeseries?site=KLGA")
    assert ok.supported and ok.station == "KLGA"
    wu = parse_resolver_identity("https://www.wunderground.com/history/daily/tw/taoyuan-city/RCTP/date/2026-9-1")
    assert not wu.supported and wu.station == "RCTP" and wu.authority == "wunderground"
    hk = parse_resolver_identity("https://www.hko.gov.hk/en/cis/dailyExtract.htm")
    assert not hk.supported and hk.authority == "non-metar"
    assert not parse_resolver_identity(None).supported


def test_station_verification_fails_closed_on_map_disagreement_and_distance():
    metadata = StationMetadata({
        "KMDW": StationRecord("KMDW", 41.786, -87.752, 188.0, "Chicago Midway", "test"),
        "KORD": StationRecord("KORD", 41.979, -87.904, 202.0, "Chicago O'Hare", "test"),
    })
    parsed = parse_resolver_identity("https://www.weather.gov/wrh/timeseries?site=KORD")
    mismatch = verify_station_for_city(
        city="chicago", expected_station="KMDW", city_coordinates=(41.7868, -87.7522),
        parsed=parsed, metadata=metadata,
    )
    assert not mismatch.verified and "differs from mapped" in mismatch.reason
    match = verify_station_for_city(
        city="chicago", expected_station="KORD", city_coordinates=(41.7868, -87.7522),
        parsed=parsed, metadata=metadata,
    )
    assert match.verified and match.distance_km is not None and match.distance_km < 60
    far = verify_station_for_city(
        city="chicago", expected_station="KORD", city_coordinates=(40.0, -80.0),
        parsed=parsed, metadata=metadata,
    )
    assert not far.verified


def test_complete_set_marks_both_tokens_and_preserves_total_cost():
    from src.v3.marking import position_legs
    legs = position_legs('basket', {'strategy': 'complete_set', 'shares': '5',
        'all_in_cost': '4.5', 'yes_token_id': 'yes', 'no_token_id': 'no'})
    assert {leg[1] for leg in legs} == {'yes', 'no'}
    assert sum(leg[3] for leg in legs) == D('4.5')


def test_complete_set_prospective_exposure_is_rejected(tmp_path):
    client = FakePublicClient([market()], [book('yes-token', ask='.45'), book('no-token', ask='.45')])
    worker = PaperWorker(client=client, settings=settings(tmp_path, max_gross_exposure_fraction=D('.01')),
                         store=PaperStore(tmp_path))
    result = asyncio.run(worker.run_cycle())
    assert result.paper_trades == 0
    assert not worker.state.open_positions


def test_stale_or_missing_timestamp_blocks_entry(tmp_path):
    books = [book('yes-token', ask='.45'), book('no-token', ask='.45')]
    books[0].timestamp = datetime.now(timezone.utc) - timedelta(seconds=3590)
    worker = PaperWorker(client=FakePublicClient([market()], books), settings=settings(tmp_path), store=PaperStore(tmp_path))
    assert asyncio.run(worker.run_cycle()).paper_trades == 0
    books[0].timestamp = None
    assert asyncio.run(worker.run_cycle()).paper_trades == 0


def test_missing_station_metadata_is_never_verified():
    result = verify_station_for_city(city='chicago', expected_station='KORD',
        city_coordinates=(41.98, -87.9), parsed=parse_resolver_identity('https://www.weather.gov/wrh/timeseries?site=KORD'), metadata=None)
    assert not result.verified


@pytest.mark.parametrize('value', ['NaN', 'Infinity', '-Infinity'])
def test_partition_nonfinite_fails_closed(value):
    with pytest.raises(SanityError):
        assert_partition_sums_to_one([value])


def test_shadow_repeated_trades_are_preserved_and_ambiguous_exits_refused(tmp_path):
    from src.v3.shadow_settlement import load_cohort
    row = {'paper_executed': True, 'condition_id': 'c', 'shares': '5', 'all_in_cost': '2', 'side': 'YES'}
    (tmp_path/'paper_trades.jsonl').write_text(json.dumps(row)+'\n'+json.dumps(row)+'\n')
    assert len(load_cohort(tmp_path)) == 2
    (tmp_path/'paper_exits.jsonl').write_text(json.dumps({'condition_id':'c','shares':'5','realized_pnl':'1'})+'\n')
    with pytest.raises(ValueError, match='ambiguous'):
        load_cohort(tmp_path)


def test_shadow_incomplete_cohort_does_not_publish_selected_statistics():
    from src.v3.shadow_settlement import TradeOutcome, reconcile
    cohort = {str(i): TradeOutcome(str(i), 'weather_directional', 'YES', D(5), D(2), '2026-09-09', 'x', D('.6')) for i in range(2)}
    report = reconcile(cohort, getter=lambda url: {'tokens':[{'outcome':'YES', 'winner':url.endswith('/0')} ]})
    assert not report['realized_pnl_publishable']
    assert report['cohort']['hit_rate'] is None
    assert report['cohort']['brier'] is None
    assert report['statistics_status'] == 'insufficient_data'


@pytest.mark.parametrize('source', [
    'https://evil-weather.gov/wrh/timeseries?site=KORD',
    'https://www.weather.gov/wrh/timeseries?site=KORD5',
    'https://www.weather.gov/wrh/timeseries?site=KORD&site=KMDW',
])
def test_resolver_rejects_spoofed_or_ambiguous_source(source):
    assert not parse_resolver_identity(source).supported


def test_resolver_conflicting_description_fails_closed():
    assert not parse_resolver_identity('https://www.weather.gov/wrh/timeseries?site=KORD',
        'Resolves at https://www.weather.gov/wrh/timeseries?site=KMDW').supported
