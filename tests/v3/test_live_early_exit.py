from datetime import datetime, timezone
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from src.v3.live_early_exit import (
    ExitPosition, VerifiedBook, plan_live_early_exit, verified_book_from_api,
)
from src.v3.math import BookLevel


def book(*, bids=None, min_size="1", step="0.1", fee="0.1", **flags):
    bids = tuple(bids if bids is not None else (BookLevel(D("0.8"), D("20")),))
    return VerifiedBook(bids=bids, best_bid=flags.pop("best_bid", max(x.price for x in bids)),
        tick_size=D("0.01"), min_order_size=D(min_size), size_step=D(step),
        fee_rate=D(fee), fresh=flags.pop("fresh", True),
        rules_verified=flags.pop("rules_verified", True),
        fee_verified=flags.pop("fee_verified", True), **flags)


def position(**kwargs):
    defaults = dict(side="YES", shares=D("10"), all_in_cost=D("5"))
    defaults.update(kwargs)
    return ExitPosition(**defaults)


def test_exact_paper_economics_and_post_only_tick():
    result = plan_live_early_exit(position(), book())
    intent = result.intent
    assert intent is not None
    assert intent.estimated_bid_vwap == D("0.8")
    assert intent.estimated_fee == D("0.16")
    assert intent.estimated_net_proceeds == D("7.84")
    assert intent.estimated_profit == D("2.84")
    assert intent.estimated_return == D("0.568")
    assert intent.post_only and intent.side == "SELL"
    assert intent.price % D("0.01") == 0
    assert intent.price >= D("0.81")


def test_depth_walk_and_levelwise_nonlinear_fee():
    b = book(bids=(BookLevel(D("0.8"), D("4")), BookLevel(D("0.6"), D("6"))))
    result = plan_live_early_exit(position(), b)
    intent = result.intent
    assert intent is not None
    assert intent.estimated_bid_vwap == D("0.68")
    assert intent.estimated_fee == D("0.208")
    assert intent.estimated_net_proceeds == D("6.592")


def test_shallow_book_fails_closed():
    result = plan_live_early_exit(position(), book(bids=(BookLevel(D("0.8"), D("9")),)))
    assert result.intent is None
    assert "insufficient" in result.reason


def test_hybrid_first_tranche_minimum_and_runner_stage_target():
    b = book()
    first = plan_live_early_exit(position(hybrid_enabled=True), b)
    assert first.intent is not None
    assert first.intent.size == D("7.5")
    assert first.intent.stage == "first_tranche"
    assert first.intent.target_return == D("0.28")
    runner = plan_live_early_exit(position(hybrid_enabled=True, hybrid_exit_done=True), b)
    assert runner.intent is not None
    assert runner.intent.size == D("10")
    assert runner.intent.stage == "runner"
    assert runner.intent.target_return == D("0.50")


def test_hybrid_first_tranche_quantity_override_is_bounded_and_rounded_down():
    position_data = position(hybrid_enabled=True)
    partial = plan_live_early_exit(position_data, book(step="0.01"), first_tranche_quantity=D("6.25"))
    assert partial.intent is not None
    assert partial.intent.stage == "first_tranche"
    assert partial.intent.size == D("6.25")
    malformed = plan_live_early_exit(
        position_data, book(), first_tranche_quantity=position_data.shares + D("0.01"),
    )
    assert malformed.intent is None
    assert isinstance(malformed.reason, str) and "override" in malformed.reason


def test_too_small_tranche_and_unverified_metadata_rejected():
    small = plan_live_early_exit(position(shares=D("1"), all_in_cost=D("0.5"), hybrid_enabled=True), book(min_size="1"))
    assert small.intent is None
    assert "minimum" in small.reason
    unverified = plan_live_early_exit(position(), book(fresh=False))
    assert unverified.intent is None
    assert "unverified" in unverified.reason


def test_unsupported_fee_and_invalid_hybrid_state_rejected():
    unsupported = plan_live_early_exit(position(), book(fee_schedule="unknown"))
    assert unsupported.intent is None
    assert "unsupported" in unsupported.reason
    invalid = plan_live_early_exit(position(hybrid_enabled=True), book(), hybrid_fraction=D("1"))
    assert invalid.intent is None
    assert "hybrid fraction" in invalid.reason


def test_non_directional_and_inconsistent_position_rejected():
    assert plan_live_early_exit(position(side="BOTH"), book()).intent is None
    assert plan_live_early_exit(position(all_in_cost=D("11")), book()).intent is None


def test_best_bid_must_match_depth_and_rounded_hybrid_basis():
    mismatched = book(best_bid=D("0.79"))
    assert plan_live_early_exit(position(), mismatched).intent is None

    rounded = plan_live_early_exit(
        position(shares=D("10"), all_in_cost=D("5"), hybrid_enabled=True),
        book(step="0.6"),
    )
    assert rounded.intent is not None
    assert rounded.intent.size == D("7.2")
    # Cost basis follows actual rounded quantity: 5 * 7.2 / 10 = 3.6.
    assert rounded.intent.estimated_profit == rounded.intent.estimated_net_proceeds - D("3.6")


def _verified_api_pair(now=None, **changes):
    at = now or datetime(2026, 10, 7, 12, tzinfo=timezone.utc)
    context = dict(
        condition_id="cond", token_id="tok", condition_matches=True, token_matches=True,
        rules_verified=True, accepting_orders=True, tick_size=D("0.01"),
        min_order_size=D("5"), fee_rate=D("0.1"), fee_exponent=D("1"),
        fees_enabled=True, book_timestamp=at, book_hash="hash",
    )
    context.update(changes)
    raw = SimpleNamespace(
        condition_id="cond", token_id="tok", timestamp=at, hash="hash",
        bids=(SimpleNamespace(price=D("0.8"), size=D("10")),),
    )
    return SimpleNamespace(**context), raw, at


def test_api_verified_book_binds_identity_hash_time_fee_rules_and_two_decimals():
    context, raw, now = _verified_api_pair()
    verified = verified_book_from_api(
        context, raw, condition_id="cond", token_id="tok", now=now,
        max_quote_age_seconds=30,
    )
    assert verified.size_step == D("0.01")
    assert verified.fresh and verified.rules_verified and verified.fee_verified
    assert verified.best_bid == D("0.8")


@pytest.mark.parametrize("changes", [
    {"token_matches": False}, {"condition_matches": False},
    {"rules_verified": False}, {"accepting_orders": False},
    {"fee_exponent": D("2")}, {"fee_rate": None},
])
def test_api_verified_book_rejects_unverified_context(changes):
    context, raw, now = _verified_api_pair(**changes)
    with pytest.raises(ValueError):
        verified_book_from_api(
            context, raw, condition_id="cond", token_id="tok", now=now,
            max_quote_age_seconds=30,
        )


@pytest.mark.parametrize("price,size", [(D("0.8"), D("Infinity")), (D("1.01"), D("1")), (D("0.8"), D("0"))])
def test_api_verified_book_rejects_nonfinite_or_nonpositive_depth(price, size):
    context, raw, now = _verified_api_pair()
    malformed = SimpleNamespace(**{
        **vars(raw), "bids": (SimpleNamespace(price=price, size=size),),
    })
    with pytest.raises(ValueError, match="bid level"):
        verified_book_from_api(
            context, malformed, condition_id="cond", token_id="tok", now=now,
            max_quote_age_seconds=30,
        )


def test_api_verified_book_rejects_stale_mismatched_and_unsorted_depth():
    context, raw, now = _verified_api_pair()
    with pytest.raises(ValueError, match="stale"):
        verified_book_from_api(
            context, raw, condition_id="cond", token_id="tok",
            now=now.replace(minute=1), max_quote_age_seconds=30,
        )
    with pytest.raises(ValueError, match="snapshot"):
        verified_book_from_api(
            context, SimpleNamespace(**{**vars(raw), "hash": "other"}),
            condition_id="cond", token_id="tok", now=now, max_quote_age_seconds=30,
        )
    unsorted = SimpleNamespace(**{
        **vars(raw), "bids": (SimpleNamespace(price=D("0.6"), size=D("5")),
                               SimpleNamespace(price=D("0.8"), size=D("5"))),
    })
    with pytest.raises(ValueError, match="descending"):
        verified_book_from_api(
            context, unsorted, condition_id="cond", token_id="tok", now=now,
            max_quote_age_seconds=30,
        )
    changed_rules = SimpleNamespace(**{**vars(raw), "tick_size": D("0.05")})
    with pytest.raises(ValueError, match="rules differ"):
        verified_book_from_api(
            context, changed_rules, condition_id="cond", token_id="tok", now=now,
            max_quote_age_seconds=30,
        )

