from decimal import Decimal

import pytest

from src.v3.orders import OrderAggregate, OrderState, TradeStatus


def D(value: str) -> Decimal:
    return Decimal(value)


def test_live_and_delayed_acceptance_do_not_create_filled_inventory():
    order = OrderAggregate.new(client_order_id="client-1", token_id="token", side="BUY", requested_size=D("10"))
    order.accept(order_id="exchange-1", status="live")
    assert order.state is OrderState.LIVE
    assert order.confirmed_size == D("0")

    delayed = OrderAggregate.new(client_order_id="client-2", token_id="token", side="BUY", requested_size=D("10"))
    delayed.accept(order_id="exchange-2", status="delayed")
    assert delayed.state is OrderState.DELAYED
    assert delayed.confirmed_size == D("0")


def test_trade_is_booked_only_after_confirmation_and_is_idempotent():
    order = OrderAggregate.new(client_order_id="client-1", token_id="token", side="BUY", requested_size=D("10"))
    order.accept(order_id="exchange-1", status="matched")
    order.record_trade("trade-1", size=D("4"), price=D("0.40"), fee=D("0.02"), status=TradeStatus.MATCHED)
    assert order.confirmed_size == D("0")

    order.record_trade("trade-1", size=D("4"), price=D("0.40"), fee=D("0.02"), status=TradeStatus.CONFIRMED)
    assert order.state is OrderState.PARTIALLY_FILLED
    assert order.confirmed_size == D("4")
    assert order.confirmed_notional == D("1.60")
    assert order.confirmed_fees == D("0.02")

    order.record_trade("trade-1", size=D("4"), price=D("0.40"), fee=D("0.02"), status=TradeStatus.CONFIRMED)
    assert order.confirmed_size == D("4")
    assert order.confirmed_notional == D("1.60")


def test_full_confirmed_fill_reaches_filled_state():
    order = OrderAggregate.new(client_order_id="client-1", token_id="token", side="BUY", requested_size=D("5"))
    order.accept(order_id="exchange-1", status="matched")
    order.record_trade("trade-1", size=D("5"), price=D("0.40"), fee=D("0"), status=TradeStatus.CONFIRMED)
    assert order.state is OrderState.FILLED
    assert order.average_fill_price == D("0.40")


def test_fill_cannot_exceed_requested_size():
    order = OrderAggregate.new(client_order_id="client-1", token_id="token", side="BUY", requested_size=D("5"))
    order.accept(order_id="exchange-1", status="matched")
    with pytest.raises(ValueError, match="exceeds requested size"):
        order.record_trade("trade-1", size=D("6"), price=D("0.40"), fee=D("0"), status=TradeStatus.CONFIRMED)
