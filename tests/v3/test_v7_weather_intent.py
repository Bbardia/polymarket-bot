from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from src.v3.market_context import MarketContext
from src.v3.strategies.weather import WeatherDecision
from src.v3.v7_weather_intent import propose_v7_weather_order

NOW = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)


def make_evaluation(*, paper_tradeable=True, decision_tradeable=True, side="YES", fee_rate=D("0"), fee_exponent=None, taker_only=None, fee=D("0")):
    decision = WeatherDecision(
        tradeable=decision_tradeable,
        reason="paper candidate" if decision_tradeable else "blocked",
        effective_sample_size=D("8"),
        calibrated_probability=D("0.80"),
        fee_per_share=D("0"),
        net_edge=D("0.20"),
        minimum_edge=D("0.05"),
        kelly_fraction=D("0.05"),
    )
    return SimpleNamespace(
        strategy="weather_directional",
        event_key="NYC|2026-10-04",
        condition_id="condition-1",
        token_id="token-yes" if side == "YES" else "token-no",
        side=side,
        bid=D("0.30"),
        ask=D("0.35"),
        shares=D("10"),
        fee=fee,
        fee_rate=fee_rate,
        fee_exponent=fee_exponent,
        taker_only=taker_only,
        paper_tradeable=paper_tradeable,
        paper_reason="weather paper candidate",
        maker_shadow=SimpleNamespace(best_bid=D("0.30"), best_ask=D("0.35")),
        decision_timestamp=NOW - timedelta(seconds=3),
        book_timestamp=NOW - timedelta(seconds=2),
        book_hash="book-1",
        decision=decision,
    )


def make_context(*, fee_rate=D("0"), fee_exponent=None, taker_only=None, token_id="token-yes", timestamp=None, book_hash="book-1"):
    return MarketContext(
        condition_id="condition-1",
        condition_matches=True,
        token_matches=True,
        tick_size=D("0.01"),
        min_order_size=D("5"),
        fee_rate=fee_rate,
        accepting_orders=True,
        rules_verified=True,
        disputed=False,
        negative_risk=True,
        resolution_source="verified-source",
        token_id=token_id,
        book_timestamp=timestamp or NOW - timedelta(seconds=2),
        book_hash=book_hash,
        fees_enabled=fee_rate != D("0"),
        fee_exponent=fee_exponent,
        taker_only=taker_only,
    )


def propose(evaluation=None, context=None, **overrides):
    timestamp = overrides.pop("book_timestamp", NOW - timedelta(seconds=2))
    book_hash = overrides.pop("book_hash", "book-1")
    kwargs = {
        "best_bid": D("0.30"),
        "best_ask": D("0.35"),
        "book_timestamp": timestamp,
        "book_hash": book_hash,
        "decision_timestamp": NOW - timedelta(seconds=3),
        "now": NOW,
        "size_step": D("0.01"),
        "order_cap": D("2"),
        "ttl_seconds": 180,
        "max_quote_age_seconds": 120,
        "max_order_ttl_seconds": 300,
    }
    kwargs.update(overrides)
    signal = evaluation or make_evaluation()
    if evaluation is None:
        signal.maker_shadow = SimpleNamespace(
            best_bid=kwargs["best_bid"], best_ask=kwargs["best_ask"],
        )
        signal.decision_timestamp = kwargs["decision_timestamp"]
        signal.book_timestamp = kwargs["book_timestamp"]
        signal.book_hash = kwargs["book_hash"]
    return propose_v7_weather_order(
        signal,
        context or make_context(timestamp=timestamp, book_hash=book_hash),
        **kwargs,
    )


def test_proposes_passive_order_from_v7_weather_signal_without_submission():
    proposal = propose()

    assert proposal.proposed
    assert proposal.submittable is False
    assert proposal.side == "BUY"
    assert proposal.price == D("0.31")  # one tick above bid; below the ask
    assert proposal.shares == D("6.45")  # capped at $2 and rounded down to size step
    assert proposal.estimated_fee == D("0")
    assert proposal.quote_age_seconds == 3
    assert proposal.expected_edge == D("0.49")


def test_rejects_candidate_blocked_by_either_v7_gate():
    assert not propose(make_evaluation(paper_tradeable=False)).proposed
    assert not propose(make_evaluation(decision_tradeable=False)).proposed


@pytest.mark.parametrize(
    ("fee_rate", "fee_exponent", "taker_only"),
    [(None, None, None), (D("0.05"), D("1"), False), (D("0.05"), D("2"), True)],
)
def test_rejects_missing_unsupported_or_non_maker_only_fee_schedule(fee_rate, fee_exponent, taker_only):
    evaluation = make_evaluation(
        fee_rate=fee_rate or D("0"), fee_exponent=fee_exponent,
        taker_only=taker_only, fee=D("0.01") if fee_rate else D("0"),
    )
    context = make_context(
        fee_rate=fee_rate, fee_exponent=fee_exponent, taker_only=taker_only,
    )
    assert not propose(evaluation, context).proposed


def test_allows_verified_taker_only_fee_schedule_for_post_only_proposal():
    evaluation = make_evaluation(
        fee_rate=D("0.05"), fee_exponent=D("1"), taker_only=True, fee=D("0.01"),
    )
    context = make_context(
        fee_rate=D("0.05"), fee_exponent=D("1"), taker_only=True,
    )

    proposal = propose(evaluation, context)

    assert proposal.proposed
    assert proposal.side == "BUY"
    assert proposal.estimated_fee == D("0")  # a verified post-only maker pays no taker fee


def test_rejects_wrong_token_or_condition_identity():
    assert not propose(context=make_context(token_id="other-token")).proposed
    context = make_context()
    context = MarketContext(**{**context.__dict__, "condition_id": "other-condition"})
    assert not propose(context=context).proposed


def test_rejects_stale_decision_or_book_and_book_identity_mismatch():
    assert not propose(decision_timestamp=NOW - timedelta(seconds=121)).proposed
    assert not propose(book_timestamp=NOW - timedelta(seconds=121)).proposed
    assert not propose(context=make_context(), book_hash="different-book").proposed


def test_rejects_supplied_fresh_time_not_bound_to_stale_evaluation():
    evaluation = make_evaluation()
    context = make_context()
    proposal = propose(
        evaluation=evaluation,
        context=context,
        decision_timestamp=NOW,
    )
    assert not proposal.proposed
    assert "provenance" in proposal.reason


def test_rejects_crossed_or_invalid_book_and_tick_or_price_mismatch():
    assert not propose(best_bid=D("0.60"), best_ask=D("0.60")).proposed
    assert not propose(best_bid=D("0.555"), best_ask=D("0.60")).proposed
    assert not propose(min_price=D("0.57")).proposed


def test_cap_size_uses_exact_decimal_arithmetic_near_a_size_step_boundary():
    cap = D("0.99999999999999999999999999999999")
    context = make_context()
    context = MarketContext(**{**context.__dict__, "min_order_size": D("0.1")})

    proposal = propose(
        context=context,
        best_bid=D("0.49"),
        best_ask=D("0.60"),
        size_step=D("0.1"),
        order_cap=cap,
    )

    assert proposal.proposed
    assert proposal.price == D("0.50")
    assert proposal.shares == D("1.9")
    assert proposal.price * proposal.shares <= cap


def test_rejects_below_minimum_after_order_cap_and_size_step_rounding():
    assert not propose(order_cap=D("0.01")).proposed
    assert not propose(size_step=D("0")).proposed


def test_rejects_invalid_ttl_and_non_weather_or_sell_signal():
    assert not propose(ttl_seconds=120).proposed
    assert not propose(ttl_seconds=301).proposed
    evaluation = make_evaluation()
    evaluation.strategy = "other"
    assert not propose(evaluation).proposed
    no_evaluation = make_evaluation(side="NO")
    no_context = make_context(token_id="token-no")
    no_proposal = propose(no_evaluation, no_context)
    assert no_proposal.proposed
    assert no_proposal.side == "BUY"
