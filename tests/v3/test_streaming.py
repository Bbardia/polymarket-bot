import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from polymarket.models.clob.user_events import UserOrderEvent

from src.v3.ledger import EventLedger
from src.v3.orders import OrderState
from src.v3.streaming import (
    ReconnectPolicy,
    ReconnectingStream,
    StreamEventProcessor,
    normalize_stream_event,
)


def D(value: str) -> Decimal:
    return Decimal(value)


def order_event(
    *, event_type="PLACEMENT", status: str | None = "LIVE", matched="0", timestamp=None,
    original_size="5",
):
    return SimpleNamespace(
        topic="user", type="order",
        payload=SimpleNamespace(
            id="order-1", owner="wallet", market="condition", asset_id="token",
            side="BUY", original_size=D(original_size), size_matched=D(matched),
            price=D("0.20"), type=event_type, timestamp=timestamp,
            created_at=None, expiration=None, order_type="GTD", status=status,
        ),
    )


def trade_event(*, status="CONFIRMED", fee_rate_bps: str | None = "50"):
    return SimpleNamespace(
        topic="user", type="trade",
        payload=SimpleNamespace(
            id="trade-1", taker_order_id="order-1", market="condition",
            asset_id="token", side="BUY", size=D("2"), price=D("0.20"),
            status=status, owner="wallet",
            fee_rate_bps=None if fee_rate_bps is None else D(fee_rate_bps),
            timestamp=None, match_time=None, last_update=None,
        ),
    )


def maker_trade_event(*, status="CONFIRMED"):
    return SimpleNamespace(
        topic="user", type="trade",
        payload=SimpleNamespace(
            id="trade-maker-1", taker_order_id="external-order", market="condition",
            asset_id="token", side="SELL", size=D("2"), price=D("0.20"),
            status=status, owner="external-wallet", fee_rate_bps=D("50"),
            timestamp=None, match_time=None, last_update=None,
            maker_orders=[SimpleNamespace(
                order_id="order-1", owner="wallet", asset_id="token", side="BUY",
                matched_amount=D("2"), price=D("0.20"), fee_rate_bps=D("50"),
            )],
        ),
    )


def test_normalized_stream_event_has_stable_id_for_duplicate_payloads():
    first = normalize_stream_event(order_event(timestamp=datetime(2026, 8, 23, tzinfo=timezone.utc)))
    second = normalize_stream_event(order_event(timestamp=datetime(2026, 8, 24, tzinfo=timezone.utc)))
    assert first.event_id == second.event_id
    assert first.event_type == "user.order"
    assert first.payload["original_size"] == "5"


def test_distinct_timestamped_market_events_do_not_collide():
    first = normalize_stream_event(SimpleNamespace(
        topic="market", type="last_trade_price",
        payload=SimpleNamespace(
            market="condition", asset_id="token", price=D("0.20"),
            size=D("1"), side="BUY", timestamp="2026-08-23T10:00:00Z",
        ),
    ))
    second = normalize_stream_event(SimpleNamespace(
        topic="market", type="last_trade_price",
        payload=SimpleNamespace(
            market="condition", asset_id="token", price=D("0.20"),
            size=D("1"), side="BUY", timestamp="2026-08-23T10:00:01Z",
        ),
    ))
    assert first.event_id != second.event_id


def test_processor_accepts_installed_sdk_user_order_shape(tmp_path):
    sdk_event = UserOrderEvent.model_validate({
        "type": "order",
        "payload": {
            "id": "order-1", "owner": "wallet", "market": "condition",
            "asset_id": "token", "side": "BUY", "original_size": "5",
            "size_matched": "0", "price": "0.20", "type": "PLACEMENT",
            "status": "LIVE",
        },
    })
    processor = StreamEventProcessor(
        EventLedger(tmp_path / "events.db"), managed_order_ids={"order-1"}
    )
    result = processor.process(sdk_event)
    assert result.accepted
    assert not result.requires_reconciliation
    assert processor.orders["order-1"].token_id == "token"


def test_processor_books_only_confirmed_trade_and_deduplicates_it(tmp_path):
    processor = StreamEventProcessor(
        EventLedger(tmp_path / "events.db"), managed_order_ids={"order-1"}
    )
    processor.process(order_event())
    first = processor.process(trade_event(status="MATCHED"))
    assert first.accepted
    assert processor.orders["order-1"].confirmed_size == D("0")

    confirmed = processor.process(trade_event(status="CONFIRMED"))
    assert confirmed.accepted
    assert processor.orders["order-1"].confirmed_size == D("2")
    assert processor.orders["order-1"].confirmed_fees == D("0.0016")

    duplicate = processor.process(trade_event(status="CONFIRMED"))
    assert duplicate.duplicate
    assert processor.orders["order-1"].confirmed_size == D("2")


def test_processor_accepts_fee_details_that_arrive_at_confirmation(tmp_path):
    processor = StreamEventProcessor(
        EventLedger(tmp_path / "events.db"), managed_order_ids={"order-1"}
    )
    processor.process(order_event())
    processor.process(trade_event(status="MATCHED", fee_rate_bps=None))
    confirmed = processor.process(trade_event(status="CONFIRMED", fee_rate_bps="50"))
    assert confirmed.accepted
    assert not confirmed.requires_reconciliation
    assert processor.orders["order-1"].confirmed_fees == D("0.0016")


def test_confirmed_trade_without_fee_requires_reconciliation(tmp_path):
    processor = StreamEventProcessor(
        EventLedger(tmp_path / "events.db"), managed_order_ids={"order-1"}
    )
    processor.process(order_event())
    result = processor.process(trade_event(status="CONFIRMED", fee_rate_bps=None))
    assert result.requires_reconciliation
    assert processor.orders["order-1"].confirmed_size == D("0")


def test_processor_accounts_for_known_maker_order_in_trade_event(tmp_path):
    processor = StreamEventProcessor(
        EventLedger(tmp_path / "events.db"), managed_order_ids={"order-1"}
    )
    processor.process(order_event())
    result = processor.process(maker_trade_event())
    assert result.accepted
    assert not result.requires_reconciliation
    assert processor.orders["order-1"].confirmed_size == D("2")
    assert processor.orders["order-1"].confirmed_fees == D("0.0016")


def test_unknown_trade_is_recorded_but_requires_reconciliation(tmp_path):
    processor = StreamEventProcessor(EventLedger(tmp_path / "events.db"))
    result = processor.process(trade_event())
    assert result.accepted
    assert result.requires_reconciliation
    assert processor.reconciliation_required
    assert processor.orders == {}


def test_unmanaged_manual_order_is_observed_but_not_adopted(tmp_path):
    processor = StreamEventProcessor(EventLedger(tmp_path / "events.db"))
    result = processor.process(order_event())
    assert result.accepted
    assert result.requires_reconciliation
    assert processor.orders == {}


def test_processor_replays_persisted_order_and_confirmed_fill(tmp_path):
    ledger = EventLedger(tmp_path / "events.db")
    first = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    first.process(order_event())
    first.process(trade_event(status="MATCHED"))
    first.process(trade_event(status="CONFIRMED"))

    restored = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    order = restored.orders["order-1"]
    assert order.confirmed_size == D("2")
    assert order.confirmed_fees == D("0.0016")
    assert order.state is OrderState.PARTIALLY_FILLED


def test_processor_applies_cancellation_without_account_action(tmp_path):
    processor = StreamEventProcessor(
        EventLedger(tmp_path / "events.db"), managed_order_ids={"order-1"}
    )
    processor.process(order_event())
    result = processor.process(order_event(event_type="CANCELLATION", status="CANCELED"))
    assert result.accepted
    assert processor.orders["order-1"].state is OrderState.CANCELED


def test_processor_rejects_changed_order_economics(tmp_path):
    processor = StreamEventProcessor(
        EventLedger(tmp_path / "events.db"), managed_order_ids={"order-1"}
    )
    processor.process(order_event())
    result = processor.process(order_event(event_type="UPDATE", original_size="6"))
    assert result.requires_reconciliation
    assert processor.orders["order-1"].requested_size == D("5")


def test_order_update_without_status_does_not_demote_delayed_order(tmp_path):
    processor = StreamEventProcessor(
        EventLedger(tmp_path / "events.db"), managed_order_ids={"order-1"}
    )
    processor.process(order_event(status="DELAYED"))
    processor.process(order_event(event_type="UPDATE", status=None))
    assert processor.orders["order-1"].state is OrderState.DELAYED


def test_reconnecting_stream_retries_with_bounded_backoff_and_closes_handles(tmp_path):
    class Handle:
        def __init__(self, events=(), error=None):
            self.events = list(events)
            self.error = error
            self.closed = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.error is not None:
                error, self.error = self.error, None
                raise error
            if self.events:
                return self.events.pop(0)
            raise StopAsyncIteration

        async def close(self):
            self.closed = True

    handles = [Handle(error=ConnectionError("dropped")), Handle(events=[trade_event()])]
    sleeps = []

    async def subscribe(_spec):
        return handles.pop(0)

    async def sleep(delay):
        sleeps.append(delay)

    processor = StreamEventProcessor(EventLedger(tmp_path / "events.db"))
    runner = ReconnectingStream(
        subscribe, "user-spec", processor,
        policy=ReconnectPolicy(base_delay_seconds=2, max_delay_seconds=10),
        sleep=sleep,
    )
    asyncio.run(runner.run(max_cycles=2))
    assert runner.state.value == "STOPPED"
    assert sleeps == [2.0]
    assert len(handles) == 0
    assert runner.last_error is None
    assert processor.reconciliation_required
    assert any("stream gap" in reason for reason in processor.reconciliation_reasons)


def test_reconnect_policy_caps_exponential_delay():
    policy = ReconnectPolicy(base_delay_seconds=2, max_delay_seconds=10)
    assert [policy.delay(attempt) for attempt in range(1, 6)] == [2.0, 4.0, 8.0, 10.0, 10.0]
    assert policy.delay(10_000) == 10.0
