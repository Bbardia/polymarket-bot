import asyncio
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from polymarket.models.data.activity import DepositActivity, WithdrawalActivity

from src.v3.api import AccountCashFlow, CompleteAccountCashFlowHistory, UnifiedPolymarketAPI
from src.v3.config import V3Settings

WALLET = "0x" + "a" * 40

class Pages:
    def __init__(self, pages):
        self._pages = pages
    def __aiter__(self):
        async def iterate():
            for page in self._pages:
                yield SimpleNamespace(items=page)
        return iterate()

TX_A = "0x" + "A" * 64
TX_B = "0x" + "b" * 64


def row(kind="DEPOSIT", *, tx=TX_A, amount="2.50", wallet=WALLET, timestamp=None):
    model = DepositActivity if kind == "DEPOSIT" else WithdrawalActivity
    return model(wallet=wallet, transaction_hash=tx,
                 amount=Decimal(amount), type=kind,
                 timestamp=timestamp or datetime(2026, 1, 1, tzinfo=timezone.utc))

def api_for(pages, *, wallet=WALLET, secure_calls=None):
    calls = []
    class Public:
        def list_activity(self, **kwargs):
            calls.append(kwargs)
            return Pages(pages)
    async def factory(**kwargs):
        if secure_calls is not None:
            secure_calls.append(kwargs)
        return object()
    api = UnifiedPolymarketAPI(settings=V3Settings(wallet_address=wallet), secure_client_factory=factory)
    api.public_client = Public()
    return api, calls


@pytest.mark.parametrize("event_type", ["TRADE", "deposit", ""])
def test_account_cash_flow_rejects_unknown_event_type(event_type):
    with pytest.raises(ValueError, match="event_type"):
        AccountCashFlow("id", event_type, datetime.now(timezone.utc), TX_A.lower(), Decimal("1"))


@pytest.mark.parametrize("amount", [Decimal("0"), Decimal("-1"), Decimal("NaN"), 1, "1"])
def test_account_cash_flow_rejects_invalid_amount(amount):
    with pytest.raises(ValueError, match="amount"):
        AccountCashFlow("id", "DEPOSIT", datetime.now(timezone.utc), TX_A.lower(), amount)


def test_cash_flow_count_args_require_positive_int_not_bool_or_float():
    api, _ = api_for([[row()]])
    for kwargs in ({"max_items": True}, {"page_size": True}, {"max_items": 1.5}, {"page_size": 1.5}):
        with pytest.raises(ValueError):
            asyncio.run(api.fetch_account_cash_flows(**kwargs))
    assert len(asyncio.run(api.fetch_account_cash_flows(max_items=1, page_size=1))) == 1

def test_fetches_only_deposits_and_withdrawals_from_public_client_and_normalizes():
    secure = []
    later = datetime(2026, 1, 2, 3, tzinfo=timezone(timedelta(hours=3)))
    api, calls = api_for([[row("DEPOSIT"), row("WITHDRAWAL", tx=TX_B, timestamp=later)]], secure_calls=secure)
    events = asyncio.run(api.fetch_account_cash_flows(max_items=10, page_size=7))
    assert calls == [{"user": WALLET, "activity_types": ["DEPOSIT", "WITHDRAWAL"], "start": 1, "page_size": 7}]
    assert events[0].amount == Decimal("2.50") and events[0].signed_amount == Decimal("2.50")
    assert events[1].signed_amount == Decimal("-2.50")
    assert events[1].timestamp == datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert events[0].transaction_hash == TX_A.lower()
    assert secure == [] and not api.secure_client_initialized
    with pytest.raises((FrozenInstanceError, AttributeError)):
        events[0].amount = Decimal("9")

def test_complete_cash_flow_history_filters_post_baseline_after_full_read():
    baseline = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp())
    api, _ = api_for([[
        row("DEPOSIT", tx=TX_A, timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc)),
        row("WITHDRAWAL", tx=TX_B, amount="1", timestamp=datetime(2026, 1, 3, tzinfo=timezone.utc)),
    ]])

    history = asyncio.run(api.fetch_complete_account_cash_flow_history(
        after=baseline, max_items=10, page_size=7,
    ))

    assert isinstance(history, CompleteAccountCashFlowHistory)
    assert history.after == baseline
    assert len(history.flows) == 1
    assert history.flows[0].event_type == "WITHDRAWAL"
    assert history.net_amount == Decimal("-1")
    assert history.fetched_at.tzinfo is not None


def test_complete_cash_flow_history_returns_empty_post_baseline_window():
    baseline = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp())
    api, _ = api_for([[row(timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc))]])

    history = asyncio.run(api.fetch_complete_account_cash_flow_history(after=baseline))

    assert history.flows == () and history.net_amount == Decimal("0")


def test_pagination_deduplication_and_stable_order():
    a = row("DEPOSIT", tx=TX_A, timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc))
    b = row("WITHDRAWAL", tx=TX_B, timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc))
    api, _ = api_for([[b], [a, b]])
    events = asyncio.run(api.fetch_account_cash_flows(max_items=3))
    assert len(events) == 2
    assert [event.transaction_hash for event in events] == [TX_A.lower(), TX_B.lower()]

def test_raw_duplicate_rows_beyond_max_items_fail_closed():
    duplicate = row()
    api, _ = api_for([[duplicate], [duplicate, duplicate]])
    with pytest.raises(RuntimeError, match="limit"):
        asyncio.run(api.fetch_account_cash_flows(max_items=2, page_size=2))


def test_too_many_empty_pages_fail_closed():
    api, _ = api_for([[], [], []])
    with pytest.raises(RuntimeError, match="page"):
        asyncio.run(api.fetch_account_cash_flows(max_items=2, page_size=2))


def test_conflicting_duplicate_stable_id_is_rejected():
    api, _ = api_for([[row(tx=TX_A)], [row(tx=TX_A, amount="3")]])
    with pytest.raises(RuntimeError, match="conflicting duplicate"):
        asyncio.run(api.fetch_account_cash_flows())

@pytest.mark.parametrize("bad", [
    SimpleNamespace(type="TRADE", wallet=WALLET, transaction_hash=TX_A, amount=Decimal("2.5"), timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc)),
    SimpleNamespace(type="DEPOSIT", wallet="0x" + "b" * 40, transaction_hash=TX_A, amount=Decimal("2.5"), timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc)),
    SimpleNamespace(type="DEPOSIT", wallet=WALLET, transaction_hash="", amount=Decimal("2.5"), timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc)),
    SimpleNamespace(type="DEPOSIT", wallet=WALLET, transaction_hash="0xabc", amount=Decimal("2.5"), timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc)),
    SimpleNamespace(type="DEPOSIT", wallet=WALLET, transaction_hash="0x" + "g" * 64, amount=Decimal("2.5"), timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc)),
    SimpleNamespace(type="DEPOSIT", wallet=WALLET, transaction_hash="0x" + "a" * 63, amount=Decimal("2.5"), timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc)),
    SimpleNamespace(type="DEPOSIT", wallet=WALLET, transaction_hash=TX_A, amount=Decimal("NaN"), timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc)),
    SimpleNamespace(type="DEPOSIT", wallet=WALLET, transaction_hash=TX_A, amount=Decimal("2.5"), timestamp=datetime(2026, 1, 1)),
    SimpleNamespace(type="DEPOSIT"),
])
def test_invalid_activity_rows_fail_closed(bad):
    api, _ = api_for([[bad]])
    with pytest.raises(RuntimeError):
        asyncio.run(api.fetch_account_cash_flows())

def test_missing_wallet_and_limit_overflow_fail_closed():
    api, calls = api_for([[row()]], wallet="")
    with pytest.raises(RuntimeError, match="wallet"):
        asyncio.run(api.fetch_account_cash_flows())
    assert calls == []
    api, _ = api_for([[row(), row(tx=TX_B)]])
    with pytest.raises(RuntimeError, match="limit"):
        asyncio.run(api.fetch_account_cash_flows(max_items=1))

def test_bad_pagination_arguments_and_page_shape_fail_closed():
    api, _ = api_for([[row()]])
    for kwargs in ({"max_items": 0}, {"page_size": 0}, {"page_size": 501}):
        with pytest.raises(ValueError):
            asyncio.run(api.fetch_account_cash_flows(**kwargs))
    class BadPublic:
        def list_activity(self, **kwargs):
            async def pages():
                yield object()
            return pages()
    api.public_client = BadPublic()
    with pytest.raises(RuntimeError, match="page"):
        asyncio.run(api.fetch_account_cash_flows())


def test_explicit_window_uses_bounds_ascending_and_accepts_empty():
    api, calls = api_for([[]])
    assert asyncio.run(api.fetch_account_cash_flows_window(start=123, end=456)) == ()
    assert calls == [{"user": WALLET, "activity_types": ["DEPOSIT", "WITHDRAWAL"],
                      "start": 123, "end": 456, "sort_direction": "ASC", "page_size": 500}]


def test_window_splits_full_5000_offset_leaf_into_disjoint_second_ranges():
    calls = []
    dense = [row(tx="0x" + f"{i:064x}", timestamp=datetime.fromtimestamp(10, tz=timezone.utc)) for i in range(5000)]
    sparse = row(tx=TX_B, timestamp=datetime.fromtimestamp(11, tz=timezone.utc))
    class Public:
        def list_activity(self, **kwargs):
            calls.append(kwargs)
            if kwargs["start"] == 0 and kwargs["end"] == 11:
                return Pages([dense[i:i + 500] for i in range(0, 5000, 500)])
            return Pages([[sparse] if kwargs["start"] == 6 else []])
    api = UnifiedPolymarketAPI(settings=V3Settings(wallet_address=WALLET))
    api.public_client = Public()
    result = asyncio.run(api.fetch_account_cash_flows_window(start=0, end=11, max_items=6000))
    assert len(result) == 5001
    assert [(c["start"], c["end"]) for c in calls] == [(0, 11), (0, 5), (6, 11)]


@pytest.mark.parametrize("kwargs", [
    {"start": True, "end": 1}, {"start": 2, "end": 1},
    {"start": 0, "end": 1, "page_limit": 0},
])
def test_window_rejects_invalid_bounds_and_caps(kwargs):
    api, calls = api_for([[]])
    with pytest.raises(ValueError):
        asyncio.run(api.fetch_account_cash_flows_window(**kwargs))
    assert not calls


def test_window_rejects_out_of_range_and_nonascending_rows():
    outside = row(timestamp=datetime.fromtimestamp(20, tz=timezone.utc))
    api, _ = api_for([[outside]])
    with pytest.raises(RuntimeError, match="outside requested"):
        asyncio.run(api.fetch_account_cash_flows_window(start=10, end=15))
    a = row(tx=TX_A, timestamp=datetime.fromtimestamp(12, tz=timezone.utc))
    b = row(tx=TX_B, timestamp=datetime.fromtimestamp(11, tz=timezone.utc))
    api, _ = api_for([[a, b]])
    with pytest.raises(RuntimeError, match="ascending"):
        asyncio.run(api.fetch_account_cash_flows_window(start=10, end=15))
