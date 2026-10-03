from decimal import Decimal
from types import SimpleNamespace
import pytest

from src.v3.market_context import MarketContext


def D(value: str) -> Decimal:
    return Decimal(value)


def test_market_context_prefers_live_book_constraints_and_current_fee_schedule():
    market = SimpleNamespace(
        condition_id="condition",
        outcomes=SimpleNamespace(
            yes=SimpleNamespace(token_id="token-1"), no=SimpleNamespace(token_id="token-2"),
        ),
        question="Will it happen?",
        state=SimpleNamespace(
            accepting_orders=True, active=True, closed=False, archived=False,
            neg_risk=False,
        ),
        trading=SimpleNamespace(
            minimum_order_size=D("5"), minimum_tick_size=D("0.01"),
            fees_enabled=True,
            fee_schedule=SimpleNamespace(
                rate=D("0.05"), exponent=1, taker_only=True, rebate_rate=D("0.25"),
            ),
        ),
        resolution=SimpleNamespace(source="Official source", uma_resolution_status=None),
    )
    book = SimpleNamespace(
        condition_id="condition", token_id="token-1", tick_size=D("0.0025"), min_order_size=D("10"),
        neg_risk=False,
    )
    context = MarketContext.from_sdk(market, book)
    assert context.tick_size == D("0.0025")
    assert context.min_order_size == D("10")
    assert context.fee_rate == D("0.05")
    assert context.fee_exponent == D("1")
    assert context.taker_only is True
    assert context.fees_enabled is True
    assert context.accepting_orders
    assert context.rules_verified
    assert not context.disputed


def test_non_one_fee_exponent_is_retained_but_market_is_not_orderable():
    market = SimpleNamespace(
        condition_id="condition",
        outcomes=SimpleNamespace(
            yes=SimpleNamespace(token_id="token-1"), no=SimpleNamespace(token_id="token-2"),
        ),
        question="Will it happen?",
        state=SimpleNamespace(
            accepting_orders=True, active=True, closed=False, archived=False, neg_risk=True,
        ),
        trading=SimpleNamespace(
            fees_enabled=True,
            fee_schedule=SimpleNamespace(
                rate=D("0.05"), exponent=2, taker_only=True, rebate_rate=D("0.2"),
            ),
        ),
        resolution=SimpleNamespace(source="Official source", uma_resolution_status=None),
    )
    book = SimpleNamespace(
        condition_id="condition", token_id="token-1", tick_size=D("0.01"),
        min_order_size=D("5"), neg_risk=True,
    )

    context = MarketContext.from_sdk(market, book)

    assert context.fee_rate == D("0.05")
    assert context.fee_exponent == D("2")
    assert context.taker_only is True
    assert not context.accepting_orders


def test_missing_fee_enabled_flag_does_not_assume_fee_free():
    market = SimpleNamespace(
        condition_id="condition",
        outcomes=SimpleNamespace(
            yes=SimpleNamespace(token_id="token-1"), no=SimpleNamespace(token_id="token-2"),
        ),
        question="Will it happen?",
        state=SimpleNamespace(
            accepting_orders=True, active=True, closed=False, archived=False, neg_risk=True,
        ),
        trading=SimpleNamespace(
            fee_schedule=SimpleNamespace(
                rate=D("0.05"), exponent=1, taker_only=True, rebate_rate=D("0.2"),
            ),
        ),
        resolution=SimpleNamespace(source="Official source", uma_resolution_status=None),
    )
    book = SimpleNamespace(
        condition_id="condition", token_id="token-1", tick_size=D("0.01"),
        min_order_size=D("5"), neg_risk=True,
    )

    context = MarketContext.from_sdk(market, book)

    assert context.fees_enabled is None
    assert context.fee_rate is None
    assert not context.accepting_orders


def test_market_context_fails_closed_on_mismatch_or_missing_rules():
    market = SimpleNamespace(
        condition_id="condition-a", question=None,
        state=SimpleNamespace(
            accepting_orders=True, active=True, closed=False, archived=False,
            neg_risk=False,
        ),
        trading=SimpleNamespace(fees_enabled=True, fee_schedule=None),
        resolution=SimpleNamespace(source=None, uma_resolution_status="DISPUTED"),
    )
    book = SimpleNamespace(
        condition_id="condition-b", tick_size=D("0.01"), min_order_size=D("5"),
        neg_risk=False,
    )
    context = MarketContext.from_sdk(market, book)
    assert not context.condition_matches
    assert not context.accepting_orders
    assert not context.rules_verified
    assert context.disputed
    assert context.fee_rate is None


def test_market_context_rejects_nonfinite_or_nonpositive_market_rules():
    market = SimpleNamespace(
        condition_id="c", question="q",
        state=SimpleNamespace(accepting_orders=True, active=True, closed=False, archived=False),
        trading=SimpleNamespace(
            minimum_order_size=D("5"), minimum_tick_size=D("0.01"),
            fees_enabled=False, fee_schedule=None,
        ),
        resolution=SimpleNamespace(source="source", uma_resolution_status=None),
    )
    book = SimpleNamespace(condition_id="c", tick_size=D("NaN"), min_order_size=D("5"))
    with pytest.raises(ValueError, match="tick size"):
        MarketContext.from_sdk(market, book)
