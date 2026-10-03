import asyncio
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.v3.api import UnifiedPolymarketAPI
from src.v3.config import V3Settings
from src.v3.reconciliation import RemoteTrade


class Pages:
    def __init__(self, pages):
        self.pages = pages
    def __aiter__(self):
        async def iterate():
            for page in self.pages:
                yield SimpleNamespace(items=page)
        return iterate()


def trade(**overrides):
    data = dict(id="trade-1", condition_id="condition", token_id="token", taker_order_id="taker-order-1", side="BUY",
                trader_side="TAKER", price="0.5", size="2", status="MATCHED",
                fee_rate_bps="10", transaction_hash="0x" + "a" * 64,
                maker_orders=[SimpleNamespace(order_id="order-1", token_id="token-maker", side="SELL",
                                              price="0.4", matched_amount="1", fee_rate_bps="5")],
                matched_at=datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=2))), updated_at=None)
    data.update(overrides)
    return SimpleNamespace(**data)


def api_with_pages(pages):
    api = UnifiedPolymarketAPI(settings=V3Settings())
    client = SimpleNamespace(list_account_trades=lambda **kwargs: Pages(pages))
    api._secure_client = client
    return api


def test_fetch_trades_normalizes_timestamp_and_maker_and_deduplicates_identical_rows():
    row = trade()
    result = asyncio.run(api_with_pages([[row], [row]]).fetch_account_trades(max_items=3, page_limit=3))
    assert len(result) == 1
    assert result[0].trade_id == "trade-1"
    assert result[0].taker_order_id == "taker-order-1"
    assert result[0].matched_at == datetime(2025, 12, 31, 22, tzinfo=timezone.utc)
    assert result[0].maker_orders[0].order_id == "order-1"
    assert isinstance(result, tuple)


def test_fetch_trades_rejects_conflicting_duplicate_ids():
    with pytest.raises(RuntimeError, match="conflicting duplicate"):
        asyncio.run(api_with_pages([[trade()], [trade(price="0.6")]]).fetch_account_trades(max_items=3, page_limit=3))


@pytest.mark.parametrize("kwargs", [{"max_items": True, "page_limit": 1}, {"max_items": 1, "page_limit": False},
                                     {"max_items": 0, "page_limit": 1}, {"max_items": 1, "page_limit": 0}])
def test_fetch_trades_requires_exact_positive_integer_bounds(kwargs):
    with pytest.raises(ValueError):
        asyncio.run(api_with_pages([]).fetch_account_trades(**kwargs))


def test_fetch_trades_caps_raw_rows_and_pages_including_empty_pages():
    api = api_with_pages([[trade()], [], [], []])
    with pytest.raises(RuntimeError, match="page limit"):
        asyncio.run(api.fetch_account_trades(max_items=2, page_limit=2))
    api = api_with_pages([[trade(), trade(id="trade-2")]])
    with pytest.raises(RuntimeError, match="item limit"):
        asyncio.run(api.fetch_account_trades(max_items=1, page_limit=2))


@pytest.mark.parametrize("changes", [{"id": ""}, {"condition_id": None}, {"token_id": ""}, {"side": "BAD"},
                                     {"status": None}, {"price": "NaN"}, {"size": "0"}, {"fee_rate_bps": "-1"},
                                     {"matched_at": None}])
def test_fetch_trades_fails_closed_on_invalid_trade_fields(changes):
    with pytest.raises(RuntimeError, match="invalid account trade"):
        asyncio.run(api_with_pages([[trade(**changes)]]).fetch_account_trades(max_items=2, page_limit=2))


def test_fetch_trades_requires_initialized_authenticated_client():
    with pytest.raises(RuntimeError, match="not been initialized"):
        asyncio.run(UnifiedPolymarketAPI(settings=V3Settings()).fetch_account_trades(max_items=1, page_limit=1))


@pytest.mark.parametrize("field,value", [("status", "PENDING"), ("status", "matched"),
                                          ("trader_side", "BOTH"), ("trader_side", "taker")])
def test_fetch_trades_rejects_unknown_sdk_status_and_role(field, value):
    with pytest.raises(RuntimeError, match="invalid account trade"):
        asyncio.run(api_with_pages([[trade(**{field: value})]]).fetch_account_trades(max_items=2, page_limit=2))


@pytest.mark.parametrize("items", [None, "not rows", 7])
def test_fetch_trades_rejects_malformed_page_items(items):
    api = api_with_pages([])
    api._secure_client = SimpleNamespace(list_account_trades=lambda **kwargs: BrokenPage(items))
    with pytest.raises(RuntimeError, match="invalid account trade"):
        asyncio.run(api.fetch_account_trades(max_items=2, page_limit=2))


class BrokenPage:
    def __init__(self, items): self.items = items
    def __aiter__(self):
        async def iterate(): yield self
        return iterate()


@pytest.mark.parametrize("maker", [None, SimpleNamespace(order_id=None, token_id="t", side="SELL",
                                                           price="0.4", matched_amount="1"),
                                    SimpleNamespace(order_id="o", token_id="t", side="BAD",
                                                    price="0.4", matched_amount="1")])
def test_fetch_trades_rejects_missing_or_invalid_maker_fields(maker):
    with pytest.raises(RuntimeError, match="invalid account trade"):
        asyncio.run(api_with_pages([[trade(maker_orders=[maker])]]).fetch_account_trades(max_items=2, page_limit=2))


@pytest.mark.parametrize("value", [datetime(2026, 1, 1), "2026-01-01T00:00:00Z"])
def test_fetch_trades_rejects_naive_or_malformed_timestamp(value):
    with pytest.raises(RuntimeError, match="invalid account trade"):
        asyncio.run(api_with_pages([[trade(matched_at=value)]]).fetch_account_trades(max_items=2, page_limit=2))


def test_fetch_trades_result_order_is_deterministic():
    rows = [trade(id="z", matched_at=datetime(2026, 1, 2, tzinfo=timezone.utc)),
            trade(id="b", matched_at=datetime(2026, 1, 1, tzinfo=timezone.utc)),
            trade(id="a", matched_at=datetime(2026, 1, 1, tzinfo=timezone.utc))]
    result = asyncio.run(api_with_pages([rows]).fetch_account_trades(max_items=4, page_limit=2))
    assert [item.trade_id for item in result] == ["a", "b", "z"]


def test_fetch_trades_caps_nested_maker_rows_by_max_items():
    makers = [trade().maker_orders[0] for _ in range(3)]
    with pytest.raises(RuntimeError, match="maker order limit"):
        asyncio.run(api_with_pages([[trade(maker_orders=makers)]]).fetch_account_trades(max_items=2, page_limit=2))


def remote_trade(**overrides):
    values = dict(trade_id="trade-1", condition_id="condition", token_id="token", taker_order_id="taker-order-1", side="BUY",
                  trader_side="TAKER", price=Decimal("0.5"), size=Decimal("2"), status="MATCHED",
                  matched_at=datetime(2026, 1, 1, tzinfo=timezone.utc), updated_at=None,
                  fee_rate_bps=None, transaction_hash=None, maker_orders=())
    values.update(overrides)
    return RemoteTrade(**values)


@pytest.mark.parametrize("field,value", [
    ("status", "PENDING"), ("status", "matched"), ("status", []),
    ("trader_side", "BOTH"), ("trader_side", "taker"), ("trader_side", []),
    ("side", []),
    ("taker_order_id", ""), ("taker_order_id", None), ("taker_order_id", 42),
    ("transaction_hash", "0x1234"), ("transaction_hash", "a" * 64),
    ("transaction_hash", 42),
    ("matched_at", "2026-01-01T00:00:00Z"), ("matched_at", None),
    ("updated_at", "2026-01-01T00:00:00Z"),
    ("updated_at", datetime(2026, 1, 1)),
])
def test_remote_trade_direct_construction_rejects_invalid_sdk_fields(field, value):
    with pytest.raises(ValueError):
        remote_trade(**{field: value})
