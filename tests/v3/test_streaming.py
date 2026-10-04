import asyncio
import pytest
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from polymarket.models.clob.user_events import UserOrderEvent

from src.v3.ledger import EventLedger, LedgerEvent
from src.v3.orders import OrderState
from src.v3.reconciliation import RemoteTrade, RemoteTradeMaker
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
    expiration=None, reason=None, original_size="5", order_id="order-1",
):
    return SimpleNamespace(
        topic="user", type="order",
        payload=SimpleNamespace(
            id=order_id, owner="wallet", market="condition", asset_id="token",
            side="BUY", original_size=D(original_size), size_matched=D(matched),
            price=D("0.20"), type=event_type, timestamp=timestamp,
            created_at=None, expiration=expiration, order_type="GTD", status=status,
            reason=reason,
        ),
    )


def trade_event(*, status="CONFIRMED", fee_rate_bps: str | None = "50", trade_id="trade-1", timestamp=None):
    return SimpleNamespace(
        topic="user", type="trade",
        payload=SimpleNamespace(
            id=trade_id, taker_order_id="order-1", market="condition",
            asset_id="token", side="BUY", size=D("2"), price=D("0.20"),
            status=status, owner="wallet",
            fee_rate_bps=None if fee_rate_bps is None else D(fee_rate_bps),
            timestamp=timestamp, match_time=None, last_update=None,
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


def multi_target_trade_event():
    event = trade_event(trade_id="trade-multi")
    event.payload.maker_orders = [SimpleNamespace(
        order_id="order-2", owner="wallet", asset_id="token", side="BUY",
        matched_amount=D("2"), price=D("0.20"), fee_rate_bps=D("50"),
    )]
    return event


def duplicate_maker_trade_event():
    event = trade_event(trade_id="trade-duplicate", status="CONFIRMED")
    event.payload.taker_order_id = "unmanaged-taker"
    event.payload.maker_orders = [
        SimpleNamespace(
            order_id="order-2", owner="wallet", asset_id="token", side="BUY",
            matched_amount=D("2"), price=D("0.20"), fee_rate_bps=D("50"),
        ),
        SimpleNamespace(
            order_id="order-2", owner="wallet", asset_id="token", side="BUY",
            matched_amount=D("3"), price=D("0.20"), fee_rate_bps=D("50"),
        ),
    ]
    return event


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


def test_stream_gap_reconciliation_latch_survives_restart(tmp_path):
    ledger = EventLedger(tmp_path / "events.db")
    processor = StreamEventProcessor(ledger)
    processor.require_reconciliation("simulated stream gap")
    restored = StreamEventProcessor(ledger)
    assert restored.reconciliation_required
    assert "simulated stream gap" in restored.reconciliation_reasons


def test_unresolved_submission_intent_latches_once_across_restart(tmp_path):
    ledger = EventLedger(tmp_path / "events.db")
    for kind, client_id in [
        ("order.submission.started", "client-1"),
        ("order.submission.attempted", "client-1"),
        ("order.submission.attempted", "client-1"),
        ("order.submission_unknown", "client-1"),
    ]:
        ledger.append(LedgerEvent.create(kind, {"client_order_id": client_id}))
    first = StreamEventProcessor(ledger)
    assert first.reconciliation_required
    assert any("client-1" in reason for reason in first.reconciliation_reasons)
    latch_count = sum(e.event_type == "order.submission_reconciliation_latched" for e in ledger.events())
    second = StreamEventProcessor(ledger)
    assert second.reconciliation_required
    assert sum(e.event_type == "order.submission_reconciliation_latched" for e in ledger.events()) == latch_count


def test_definitive_submission_terminals_clear_pending_but_unknown_does_not(tmp_path):
    for terminal in ("order.accepted", "order.rejected"):
        ledger = EventLedger(tmp_path / f"{terminal.replace('.', '-')}.db")
        ledger.append(LedgerEvent.create("order.submission.started", {"client_order_id": "c"}))
        ledger.append(LedgerEvent.create("order.submission_unknown", {"client_order_id": "c"}))
        payload = {"client_order_id": "c"}
        if terminal == "order.accepted":
            payload.update({"order_id": "o", "status": "live", "token_id": "t", "side": "BUY", "requested_size": "1"})
        else:
            payload.update({"code": "400", "message": "rejected"})
        ledger.append(LedgerEvent.create(terminal, payload))
        assert not StreamEventProcessor(ledger).reconciliation_required
    for terminal in ("order.submission_unknown", "order.acceptance_unknown"):
        ledger = EventLedger(tmp_path / f"{terminal.replace('.', '-')}.db")
        ledger.append(LedgerEvent.create("order.submission.started", {"client_order_id": "c"}))
        ledger.append(LedgerEvent.create(terminal, {"client_order_id": "c"}))
        assert StreamEventProcessor(ledger).reconciliation_required


def test_submission_latch_remains_sticky_after_later_terminal_and_restart(tmp_path):
    ledger = EventLedger(tmp_path / "sticky.db")
    ledger.append(LedgerEvent.create("order.submission.started", {"client_order_id": "c"}))
    assert StreamEventProcessor(ledger).reconciliation_required
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "c", "order_id": "o", "status": "live",
        "token_id": "t", "side": "BUY", "requested_size": "1",
    }))
    restored = StreamEventProcessor(ledger)
    assert restored.reconciliation_required
    assert any("unresolved order submission" in reason for reason in restored.reconciliation_reasons)


def test_malformed_submission_ids_fail_closed_stably_across_restart(tmp_path):
    for index, payload in enumerate(({}, {"client_order_id": ""}, {"client_order_id": 7})):
        ledger = EventLedger(tmp_path / f"malformed-{index}.db")
        ledger.append(LedgerEvent.create("order.submission.started", payload))
        first = StreamEventProcessor(ledger)
        assert first.reconciliation_required
        latches = [e for e in ledger.events() if e.event_type == "order.submission_reconciliation_latched"]
        assert latches and latches[0].payload.get("event_ids")
        second = StreamEventProcessor(ledger)
        assert second.reconciliation_required
        assert len([e for e in ledger.events() if e.event_type == "order.submission_reconciliation_latched"]) == len(latches)


def test_malformed_attempted_submission_ids_latch_once_across_restart(tmp_path):
    for index, payload in enumerate(({}, {"client_order_id": ""}, {"client_order_id": 7})):
        ledger = EventLedger(tmp_path / f"attempted-malformed-{index}.db")
        ledger.append(LedgerEvent.create("order.submission.attempted", payload))
        first = StreamEventProcessor(ledger)
        assert first.reconciliation_required
        latches = [e for e in ledger.events() if e.event_type == "order.submission_reconciliation_latched"]
        assert latches and latches[0].payload.get("event_ids")
        second = StreamEventProcessor(ledger)
        assert second.reconciliation_required
        assert len([e for e in ledger.events() if e.event_type == "order.submission_reconciliation_latched"]) == len(latches)


def test_malformed_terminal_id_does_not_clear_pending_submission(tmp_path):
    ledger = EventLedger(tmp_path / "bad-terminal.db")
    ledger.append(LedgerEvent.create("order.submission.started", {"client_order_id": "c"}))
    ledger.append(LedgerEvent.create("order.rejected", {"client_order_id": 9}))
    processor = StreamEventProcessor(ledger)
    assert processor.reconciliation_required
    assert any("c" in reason for reason in processor.reconciliation_reasons)


def test_malformed_submission_terminals_remain_latched_after_restart(tmp_path):
    for name, terminal, payload in (
        ("accepted-missing", "order.accepted", {"client_order_id": "c"}),
        ("accepted-status", "order.accepted", {
            "client_order_id": "c", "order_id": "o", "status": "unknown",
            "token_id": "t", "side": "BUY", "requested_size": "1",
        }),
        ("accepted-infinity", "order.accepted", {
            "client_order_id": "c", "order_id": "o", "status": "live",
            "token_id": "t", "side": "BUY", "requested_size": "Infinity",
        }),
        ("accepted-nan", "order.accepted", {
            "client_order_id": "c", "order_id": "o", "status": "live",
            "token_id": "t", "side": "BUY", "requested_size": "NaN",
        }),
        ("rejected-missing", "order.rejected", {"client_order_id": "c", "code": "400"}),
        ("rejected-invalid", "order.rejected", {
            "client_order_id": "c", "code": "", "message": "no",
        }),
    ):
        ledger = EventLedger(tmp_path / f"{name}.db")
        ledger.append(LedgerEvent.create("order.submission.started", {"client_order_id": "c"}))
        ledger.append(LedgerEvent.create(terminal, payload))
        first = StreamEventProcessor(ledger)
        assert first.reconciliation_required
        restored = StreamEventProcessor(ledger)
        assert restored.reconciliation_required


def test_valid_rejection_after_submission_latch_stays_latched_after_restart(tmp_path):
    ledger = EventLedger(tmp_path / "rejected-after-latch.db")
    ledger.append(LedgerEvent.create("order.submission.started", {"client_order_id": "c"}))
    first = StreamEventProcessor(ledger)
    assert first.reconciliation_required
    second = StreamEventProcessor(ledger)
    assert second.reconciliation_required
    ledger.append(LedgerEvent.create("order.rejected", {
        "client_order_id": "c", "code": "400", "message": "rejected",
    }))
    third = StreamEventProcessor(ledger)
    assert third.reconciliation_required
    assert any("unresolved order submission" in reason for reason in third.reconciliation_reasons)


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


def test_executor_accepted_order_is_adopted_and_fill_recovered_after_restart(tmp_path):
    ledger = EventLedger(tmp_path / "events.db")
    ledger.append(LedgerEvent.create("order.submission.started", {
        "client_order_id": "client-1", "condition_id": "condition",
        "token_id": "token", "side": "BUY", "requested_size": "5",
    }))
    ledger.append(LedgerEvent.create("order.accepted", {
        "client_order_id": "client-1", "order_id": "order-1", "status": "live",
        "condition_id": "condition", "token_id": "token", "side": "BUY",
        "requested_size": "5",
    }))

    first = StreamEventProcessor(ledger)
    assert "order-1" in first.managed_order_ids
    first.process(trade_event(status="CONFIRMED"))

    restored = StreamEventProcessor(ledger)
    assert restored.orders["order-1"].confirmed_size == D("2")
    assert restored.orders["order-1"].state is OrderState.PARTIALLY_FILLED


def test_active_order_ids_excludes_canceled_order_across_restart(tmp_path):
    ledger = EventLedger(tmp_path / "active-orders.db")
    processor = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    processor.process(order_event())
    assert processor.active_order_ids == frozenset({"order-1"})
    with pytest.raises(AttributeError):
        processor.active_order_ids.add("other")

    processor.process(order_event(event_type="CANCELLATION", status="CANCELED"))
    assert processor.active_order_ids == frozenset()
    assert StreamEventProcessor(ledger).active_order_ids == frozenset()


def test_processor_applies_cancellation_without_account_action(tmp_path):
    processor = StreamEventProcessor(
        EventLedger(tmp_path / "events.db"), managed_order_ids={"order-1"}
    )
    processor.process(order_event())
    result = processor.process(order_event(event_type="CANCELLATION", status="CANCELED"))
    assert result.accepted
    assert processor.orders["order-1"].state is OrderState.CANCELED


def test_confirmed_fill_matched_before_gtd_expiry_is_imported_after_cancel_event(tmp_path):
    ledger = EventLedger(tmp_path / "events.db")
    processor = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    expires_at = datetime(2026, 10, 4, 0, 31, 14, tzinfo=timezone.utc)
    matched_at = datetime(2026, 10, 4, 0, 21, 53, tzinfo=timezone.utc)
    expiration = int(expires_at.timestamp())

    processor.process(order_event(expiration=expiration))
    canceled = processor.process(order_event(
        event_type="CANCELLATION", status="CANCELED", expiration=expiration,
        reason="gtd_expired_absent_from_open_orders",
    ))
    result = processor.process(trade_event(
        status="CONFIRMED", timestamp=matched_at,
    ))

    assert canceled.accepted
    assert result.accepted and not result.requires_reconciliation
    assert not processor.reconciliation_required
    order = processor.orders["order-1"]
    assert order.state is OrderState.CANCELED
    assert order.confirmed_size == D("2")


def test_matched_trade_confirmed_after_cancel_is_persisted_and_replayed_without_accounting(tmp_path):
    ledger = EventLedger(tmp_path / "events.db")
    processor = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    placement = processor.process(order_event())
    matched_event = trade_event(status="MATCHED", trade_id="trade-1")
    matched = processor.process(matched_event)
    cancellation = processor.process(
        order_event(event_type="CANCELLATION", status="CANCELED")
    )
    order = processor.orders["order-1"]
    prior_trade = order.trades["trade-1"]
    prior_trade_snapshot = (prior_trade.status, prior_trade.fee, prior_trade.accounted)
    before_economics = (
        order.confirmed_size, order.confirmed_notional, order.confirmed_fees,
    )

    late_event = trade_event(status="CONFIRMED", trade_id="trade-1")
    late = processor.process(late_event)
    assert placement.event_id != matched.event_id
    assert matched.event_id != cancellation.event_id
    assert matched.event_id != late.event_id
    assert cancellation.event_id != late.event_id
    assert late.accepted and late.requires_reconciliation
    assert "confirmed trade after cancellation" in late.reason
    assert any(event.event_id == late.event_id for event in ledger.events())
    assert processor.reconciliation_required
    assert order.state is OrderState.CANCELED
    assert (
        order.confirmed_size, order.confirmed_notional, order.confirmed_fees,
    ) == before_economics
    assert (prior_trade.status, prior_trade.fee, prior_trade.accounted) == prior_trade_snapshot
    assert prior_trade.status.name == "MATCHED"
    assert prior_trade.fee == D("0.0016")
    assert not prior_trade.accounted

    restored = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    replayed = restored.orders["order-1"]
    replayed_trade = replayed.trades["trade-1"]
    assert restored.reconciliation_required
    assert any("confirmed trade after cancellation" in reason for reason in restored.reconciliation_reasons)
    assert replayed.state is OrderState.CANCELED
    assert (
        replayed.confirmed_size, replayed.confirmed_notional, replayed.confirmed_fees,
    ) == before_economics
    assert (replayed_trade.status, replayed_trade.fee, replayed_trade.accounted) == prior_trade_snapshot


def test_late_confirmed_fill_after_cancel_latches_and_replays_without_accounting(tmp_path):
    ledger = EventLedger(tmp_path / "events.db")
    processor = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    processor.process(order_event())
    processor.process(trade_event(status="CONFIRMED", trade_id="trade-1"))
    processor.process(order_event(event_type="CANCELLATION", status="CANCELED"))
    order = processor.orders["order-1"]
    before = (order.confirmed_size, order.confirmed_notional, order.confirmed_fees, len(order.trades))

    late = processor.process(trade_event(status="CONFIRMED", trade_id="trade-2"))
    assert late.accepted and late.requires_reconciliation
    assert "confirmed trade after cancellation" in late.reason
    assert processor.reconciliation_required
    assert (order.state, order.confirmed_size, order.confirmed_notional, order.confirmed_fees, len(order.trades)) == (OrderState.CANCELED, *before)

    restored = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    replayed = restored.orders["order-1"]
    assert restored.reconciliation_required
    assert any("confirmed trade after cancellation" in reason for reason in restored.reconciliation_reasons)
    assert (replayed.state, replayed.confirmed_size, replayed.confirmed_notional, replayed.confirmed_fees, len(replayed.trades)) == (OrderState.CANCELED, *before)


def test_confirmed_multi_target_trade_is_atomic_when_maker_was_canceled(tmp_path):
    ledger = EventLedger(tmp_path / "events.db")
    processor = StreamEventProcessor(
        ledger, managed_order_ids={"order-1", "order-2"}
    )
    processor.process(order_event(order_id="order-1"))
    processor.process(order_event(order_id="order-2"))
    processor.process(order_event(order_id="order-2", event_type="CANCELLATION", status="CANCELED"))

    def state(order):
        return (
            order.state, order.confirmed_size, order.confirmed_notional,
            order.confirmed_fees, dict(order.trades),
        )

    before = {key: state(order) for key, order in processor.orders.items()}
    result = processor.process(multi_target_trade_event())
    assert result.accepted and result.requires_reconciliation
    assert processor.reconciliation_required
    assert {key: state(order) for key, order in processor.orders.items()} == before

    restored = StreamEventProcessor(
        EventLedger(tmp_path / "events.db"), managed_order_ids={"order-1", "order-2"}
    )
    assert restored.reconciliation_required
    assert {key: state(order) for key, order in restored.orders.items()} == before


def test_confirmed_multi_target_overfill_is_atomic_and_replays_latch(tmp_path):
    ledger = EventLedger(tmp_path / "events.db")
    processor = StreamEventProcessor(
        ledger, managed_order_ids={"order-1", "order-2"}
    )
    processor.process(order_event(order_id="order-1", original_size="5"))
    processor.process(order_event(order_id="order-2", original_size="1"))

    def state(order):
        return (
            order.state, order.confirmed_size, order.confirmed_notional,
            order.confirmed_fees,
            {key: (trade.size, trade.price, trade.fee, trade.status, trade.accounted)
             for key, trade in order.trades.items()},
        )

    before = {key: state(order) for key, order in processor.orders.items()}
    result = processor.process(multi_target_trade_event())
    assert result.accepted and result.requires_reconciliation
    assert processor.reconciliation_required
    assert {key: state(order) for key, order in processor.orders.items()} == before

    restored = StreamEventProcessor(
        ledger, managed_order_ids={"order-1", "order-2"}
    )
    assert restored.reconciliation_required
    assert {key: state(order) for key, order in restored.orders.items()} == before


def test_confirmed_duplicate_maker_targets_are_atomic_and_replay_latch(tmp_path):
    ledger = EventLedger(tmp_path / "events.db")
    processor = StreamEventProcessor(ledger, managed_order_ids={"order-2"})
    processor.process(order_event(order_id="order-2"))
    order = processor.orders["order-2"]
    before = (
        order.state, order.confirmed_size, order.confirmed_notional,
        order.confirmed_fees, dict(order.trades),
    )

    result = processor.process(duplicate_maker_trade_event())
    assert result.accepted and result.requires_reconciliation
    assert processor.reconciliation_required
    assert (order.state, order.confirmed_size, order.confirmed_notional,
            order.confirmed_fees, dict(order.trades)) == before
    assert any(event.event_id == result.event_id for event in ledger.events())

    restored = StreamEventProcessor(
        EventLedger(tmp_path / "events.db"), managed_order_ids={"order-2"}
    )
    replayed = restored.orders["order-2"]
    assert restored.reconciliation_required
    assert (replayed.state, replayed.confirmed_size, replayed.confirmed_notional,
            replayed.confirmed_fees, dict(replayed.trades)) == before


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


def remote_trade_row(status="CONFIRMED"):
    return RemoteTrade(
        trade_id="remote-1", condition_id="condition", token_id="token",
        taker_order_id="order-1", side="BUY", trader_side="TAKER",
        price=D("0.20"), size=D("2"), status=status,
        matched_at=datetime(2026, 1, 1, tzinfo=timezone.utc), updated_at=None,
        fee_rate_bps=D("50"), transaction_hash=None, maker_orders=(),
    )


def test_import_remote_trade_replays_durably_idempotently(tmp_path):
    ledger = EventLedger(tmp_path / "events.db")
    processor = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    processor.process(order_event())
    row = remote_trade_row()
    result = processor.import_remote_trade(row)
    assert result.accepted and not result.requires_reconciliation
    assert processor.orders["order-1"].trades["remote-1"].status.name == "CONFIRMED"
    assert processor.orders["order-1"].confirmed_fees == D("0.0016")
    assert processor.import_remote_trade(row).duplicate
    restored = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    assert restored.orders["order-1"].confirmed_size == D("2")
    assert restored.orders["order-1"].trades["remote-1"].status.name == "CONFIRMED"


def test_remote_import_preserves_matched_then_confirmed_transition(tmp_path):
    processor = StreamEventProcessor(EventLedger(tmp_path / "events.db"), managed_order_ids={"order-1"})
    processor.process(order_event())
    processor.import_remote_trade(remote_trade_row("MATCHED"))
    assert not processor.orders["order-1"].trades["remote-1"].accounted
    processor.import_remote_trade(remote_trade_row("CONFIRMED"))
    assert processor.orders["order-1"].confirmed_size == D("2")


def test_remote_import_rejects_role_mismatch_without_fill_mutation(tmp_path):
    from dataclasses import replace
    processor = StreamEventProcessor(EventLedger(tmp_path / "events.db"), managed_order_ids={"order-1"})
    processor.process(order_event())
    result = processor.import_remote_trade(replace(remote_trade_row(), trader_side="MAKER"))
    assert result.requires_reconciliation
    assert not processor.orders["order-1"].trades


def test_remote_import_side_mismatch_is_atomic_and_latches_reconciliation(tmp_path):
    from dataclasses import replace

    ledger = EventLedger(tmp_path / "events.db")
    processor = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    processor.process(order_event())
    order = processor.orders["order-1"]
    before = (order.state, order.confirmed_size, order.confirmed_notional,
              order.confirmed_fees, dict(order.trades))

    # Taker order side must agree with the remote trade's side.
    result = processor.import_remote_trade(replace(remote_trade_row(), side="SELL"))

    assert not result.accepted and result.requires_reconciliation
    assert processor.reconciliation_required
    assert (order.state, order.confirmed_size, order.confirmed_notional,
            order.confirmed_fees, dict(order.trades)) == before
    assert any("side" in reason for reason in processor.reconciliation_reasons)

    # Maker association side is authoritative for the managed maker order.
    maker = RemoteTradeMaker("order-1", "token", "SELL", D("0.20"), D("2"), D("50"))
    maker_trade = replace(remote_trade_row(), taker_order_id="external", trader_side="MAKER",
                          maker_orders=(maker,))
    result = processor.import_remote_trade(maker_trade)
    assert not result.accepted and result.requires_reconciliation
    assert (order.state, order.confirmed_size, order.confirmed_notional,
            order.confirmed_fees, dict(order.trades)) == before


def test_remote_maker_import_preserves_order_association_and_fee(tmp_path):
    from dataclasses import replace
    processor = StreamEventProcessor(EventLedger(tmp_path / "events.db"), managed_order_ids={"order-1"})
    processor.process(order_event())
    maker = RemoteTradeMaker("order-1", "token", "BUY", D("0.20"), D("2"), D("50"))
    row = replace(remote_trade_row(), taker_order_id="external", trader_side="MAKER",
                  maker_orders=(maker,))
    result = processor.import_remote_trade(row)
    assert result.accepted and not result.requires_reconciliation
    record = processor.orders["order-1"].trades["remote-1"]
    assert record.status.name == "CONFIRMED"
    assert record.fee == D("0.0016")
    assert processor.orders["order-1"].confirmed_size == D("2")


def test_remote_matched_taker_without_fee_latches_before_mutation_and_restart(tmp_path):
    from dataclasses import replace

    ledger = EventLedger(tmp_path / "unknown-taker-fee.db")
    processor = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    processor.process(order_event())
    order = processor.orders["order-1"]
    before = (order.state, order.confirmed_size, order.confirmed_notional,
              order.confirmed_fees, dict(order.trades))

    result = processor.import_remote_trade(
        replace(remote_trade_row("MATCHED"), fee_rate_bps=None)
    )

    assert not result.accepted and result.requires_reconciliation
    assert processor.reconciliation_required
    assert "fee" in result.reason
    assert (order.state, order.confirmed_size, order.confirmed_notional,
            order.confirmed_fees, dict(order.trades)) == before
    restored = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    assert restored.reconciliation_required
    assert (restored.orders["order-1"].state,
            restored.orders["order-1"].confirmed_size,
            restored.orders["order-1"].confirmed_notional,
            restored.orders["order-1"].confirmed_fees,
            dict(restored.orders["order-1"].trades)) == before


def test_remote_matched_maker_without_fee_latches_before_mutation_and_restart(tmp_path):
    from dataclasses import replace

    ledger = EventLedger(tmp_path / "unknown-maker-fee.db")
    processor = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    processor.process(order_event())
    order = processor.orders["order-1"]
    before = (order.state, order.confirmed_size, order.confirmed_notional,
              order.confirmed_fees, dict(order.trades))
    maker = RemoteTradeMaker("order-1", "token", "BUY", D("0.20"), D("2"), None)
    row = replace(remote_trade_row("MATCHED"), taker_order_id="external",
                  trader_side="MAKER", maker_orders=(maker,))

    result = processor.import_remote_trade(row)

    assert not result.accepted and result.requires_reconciliation
    assert processor.reconciliation_required
    assert "fee" in result.reason
    assert (order.state, order.confirmed_size, order.confirmed_notional,
            order.confirmed_fees, dict(order.trades)) == before
    restored = StreamEventProcessor(ledger, managed_order_ids={"order-1"})
    assert restored.reconciliation_required
    assert (restored.orders["order-1"].state,
            restored.orders["order-1"].confirmed_size,
            restored.orders["order-1"].confirmed_notional,
            restored.orders["order-1"].confirmed_fees,
            dict(restored.orders["order-1"].trades)) == before
