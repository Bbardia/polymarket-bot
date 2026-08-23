from decimal import Decimal
from types import SimpleNamespace

from src.v3.market_context import MarketContext


def D(value: str) -> Decimal:
    return Decimal(value)


def test_market_context_prefers_live_book_constraints_and_current_fee_schedule():
    market = SimpleNamespace(
        condition_id="condition",
        question="Will it happen?",
        state=SimpleNamespace(
            accepting_orders=True, active=True, closed=False, archived=False,
            neg_risk=False,
        ),
        trading=SimpleNamespace(
            minimum_order_size=D("5"), minimum_tick_size=D("0.01"),
            fees_enabled=True,
            fee_schedule=SimpleNamespace(rate=D("0.05"), taker_only=True),
        ),
        resolution=SimpleNamespace(source="Official source", uma_resolution_status=None),
    )
    book = SimpleNamespace(
        condition_id="condition", tick_size=D("0.0025"), min_order_size=D("10"),
        neg_risk=False,
    )
    context = MarketContext.from_sdk(market, book)
    assert context.tick_size == D("0.0025")
    assert context.min_order_size == D("10")
    assert context.fee_rate == D("0.05")
    assert context.accepting_orders
    assert context.rules_verified
    assert not context.disputed


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
