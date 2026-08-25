from decimal import Decimal
from typing import Any, cast

import pytest

from src.v3.maker_shadow import MakerShadowQuote, propose_buy_quote
from src.v3.math import BookLevel


def D(value: str) -> Decimal:
    return Decimal(value)


def test_maker_shadow_improves_wide_spread_without_crossing():
    quote = propose_buy_quote(
        bids=(BookLevel(D("0.40"), D("12")),),
        asks=(BookLevel(D("0.43"), D("10")),),
        tick_size=D("0.01"),
        size=D("5"),
        expected_probability=D("0.55"),
    )

    assert quote.price == D("0.41")
    assert quote.queue_ahead == D("0")
    assert quote.edge == D("0.14")
    assert quote.expected_edge == D("0.14")
    assert quote.expected_probability == D("0.55")
    assert quote.best_bid == D("0.40")
    assert quote.best_ask == D("0.43")
    assert quote.fill_status == "unobserved"
    assert quote.execution_status == "not_submitted"
    assert quote.cash_delta == D("0")
    assert quote.inventory_delta == D("0")
    assert quote.paper_tradeable


def test_maker_shadow_joins_best_bid_when_spread_is_one_tick():
    quote = propose_buy_quote(
        bids=(
            BookLevel(D("0.40"), D("12")),
            BookLevel(D("0.40"), D("3")),
        ),
        asks=(BookLevel(D("0.41"), D("10")),),
        tick_size=D("0.01"),
        size=D("5"),
        expected_probability=D("0.50"),
    )

    assert quote.price == D("0.40")
    assert quote.queue_ahead == D("15")
    assert quote.edge == D("0.10")


def test_maker_shadow_records_negative_edge_but_never_claims_a_fill():
    quote = propose_buy_quote(
        bids=(BookLevel(D("0.70"), D("8")),),
        asks=(BookLevel(D("0.72"), D("8")),),
        tick_size=D("0.01"),
        size=D("5"),
        expected_probability=D("0.69"),
    )

    assert quote.price == D("0.71")
    assert quote.edge == D("-0.02")
    assert not quote.paper_tradeable
    assert quote.fill_status == "unobserved"


def test_maker_shadow_rejects_invalid_books_and_sizes():
    with pytest.raises(ValueError, match="book"):
        propose_buy_quote(
            bids=(),
            asks=(BookLevel(D("0.5"), D("5")),),
            tick_size=D("0.01"),
            size=D("5"),
            expected_probability=D("0.6"),
        )
    with pytest.raises(ValueError, match="size"):
        propose_buy_quote(
            bids=(BookLevel(D("0.4"), D("5")),),
            asks=(BookLevel(D("0.5"), D("5")),),
            tick_size=D("0.01"),
            size=D("0"),
            expected_probability=D("0.6"),
        )


def test_maker_shadow_safety_fields_cannot_be_overridden_by_callers():
    required = {
        "side": "BUY",
        "price": D("0.41"),
        "size": D("5"),
        "queue_ahead": D("0"),
        "expected_probability": D("0.55"),
        "best_bid": D("0.40"),
        "best_ask": D("0.43"),
        "edge": D("0.14"),
    }
    constructor = cast(Any, MakerShadowQuote)
    with pytest.raises(TypeError):
        constructor(**required, execution_status="submitted")
    with pytest.raises(TypeError):
        constructor(**required, cash_delta=D("1"))
