import asyncio
from types import SimpleNamespace

import pytest
from polymarket import CancelOrdersResponse

from src.v3.sdk_execution_adapter import (
    CANCEL_ALL_ORDER_LIMIT,
    CANCEL_ALL_PAGE_LIMIT,
    SDKExecutionAdapter,
)


class Pages:
    def __init__(self, pages):
        self.pages = pages

    def __aiter__(self):
        async def iterate():
            for page in self.pages:
                yield page
        return iterate()


class FakeClient:
    def __init__(self, pages=()):
        self.pages = pages
        self.calls = []

    async def create_limit_order(self, **kwargs):
        self.calls.append(("create", kwargs))
        return "signed"

    async def post_order(self, signed):
        self.calls.append(("post", signed))
        return SimpleNamespace(ok=True, order_id="oid", status="live")

    def list_open_orders(self, **kwargs):
        self.calls.append(("list", kwargs))
        return Pages(self.pages)

    async def cancel_orders(self, *, order_ids):
        self.calls.append(("cancel", tuple(order_ids)))
        return CancelOrdersResponse(canceled=tuple(order_ids))


def test_place_maps_through_sdk_with_post_only_and_exact_valid_gtd():
    client = FakeClient()
    adapter = SDKExecutionAdapter(client, clock=lambda: 1000)

    result = asyncio.run(adapter.place_limit_order(
        token_id="tok", price=0.4, size=5, side="BUY", post_only=True,
        expiration=1180,
    ))

    assert result.ok and result.order_id == "oid"
    assert client.calls == [
        ("create", {"token_id": "tok", "price": 0.4, "size": 5,
                    "side": "BUY", "post_only": True, "expiration": 1180}),
        ("post", "signed"),
    ]


def test_place_rejects_non_post_only_without_sdk_calls():
    client = FakeClient()
    with pytest.raises(ValueError, match="post_only"):
        asyncio.run(SDKExecutionAdapter(client, clock=lambda: 1000).place_limit_order(
            token_id="tok", price=0.4, size=5, side="BUY", post_only=False,
            expiration=1180))
    assert client.calls == []


def test_cancel_all_walks_orders_and_cancels_exact_ids_preserving_partial_results():
    client = FakeClient([SimpleNamespace(items=[SimpleNamespace(id="one")]),
                         SimpleNamespace(items=[SimpleNamespace(id="two")])])
    expected = CancelOrdersResponse(canceled=("one",), not_canceled={"two": "busy"})

    async def cancel(*, order_ids):
        client.calls.append(("cancel", tuple(order_ids)))
        return expected
    client.cancel_orders = cancel

    result = asyncio.run(SDKExecutionAdapter(client).cancel_all())

    assert result is expected
    assert client.calls == [("list", {}), ("cancel", ("one", "two"))]


def test_cancel_all_rejects_duplicate_ids_across_pages_before_cancel():
    client = FakeClient([SimpleNamespace(items=[SimpleNamespace(id="same")]),
                         SimpleNamespace(items=[SimpleNamespace(id="same")])])
    with pytest.raises(ValueError, match="duplicate"):
        asyncio.run(SDKExecutionAdapter(client).cancel_all())
    assert client.calls == [("list", {})]


def test_cancel_all_empty_returns_sdk_response_without_cancel_call():
    client = FakeClient([])
    result = asyncio.run(SDKExecutionAdapter(client).cancel_all())
    assert result == CancelOrdersResponse(canceled=())
    assert client.calls == [("list", {})]


@pytest.mark.parametrize("page", [SimpleNamespace(items=[SimpleNamespace(id="")]),
                                   SimpleNamespace(items=[SimpleNamespace()])])
def test_cancel_all_fails_closed_on_invalid_order_identity(page):
    with pytest.raises((TypeError, ValueError)):
        asyncio.run(SDKExecutionAdapter(FakeClient([page])).cancel_all())


def test_cancel_all_rejects_malformed_page_shape():
    with pytest.raises((TypeError, ValueError)):
        asyncio.run(SDKExecutionAdapter(FakeClient([SimpleNamespace()])).cancel_all())


@pytest.mark.parametrize("expiration", [None, True, False, 1180.0, "1180", 1179])
def test_place_rejects_invalid_or_too_short_gtd_without_sdk_calls(expiration):
    client = FakeClient()
    with pytest.raises((TypeError, ValueError)):
        asyncio.run(SDKExecutionAdapter(client, clock=lambda: 1000).place_limit_order(
            token_id="tok", price=0.4, size=5, side="BUY", expiration=expiration))
    assert client.calls == []


def test_post_exception_propagates_without_retry():
    class Failed(FakeClient):
        async def post_order(self, signed):
            self.calls.append(("post", signed))
            raise RuntimeError("ambiguous")
    client = Failed()
    with pytest.raises(RuntimeError, match="ambiguous"):
        asyncio.run(SDKExecutionAdapter(client, clock=lambda: 1000).place_limit_order(
            token_id="tok", price=0.4, size=5, side="BUY", expiration=1180))
    assert [call[0] for call in client.calls] == ["create", "post"]


def test_cancel_exception_propagates_without_retry():
    class Failed(FakeClient):
        async def cancel_orders(self, *, order_ids):
            self.calls.append(("cancel", tuple(order_ids)))
            raise RuntimeError("ambiguous")
    client = Failed([SimpleNamespace(items=[SimpleNamespace(id="one")])])
    with pytest.raises(RuntimeError, match="ambiguous"):
        asyncio.run(SDKExecutionAdapter(client).cancel_all())
    assert [call[0] for call in client.calls] == ["list", "cancel"]


def test_cancel_all_fails_closed_when_pagination_exceeds_page_cap():
    client = FakeClient([SimpleNamespace(items=()) for _ in range(CANCEL_ALL_PAGE_LIMIT + 1)])
    with pytest.raises(RuntimeError, match="page limit"):
        asyncio.run(SDKExecutionAdapter(client).cancel_all())
    assert client.calls == [("list", {})]


def test_cancel_all_fails_closed_when_raw_order_count_exceeds_cap():
    orders = [SimpleNamespace(id=f"order-{i}") for i in range(CANCEL_ALL_ORDER_LIMIT + 1)]
    client = FakeClient([SimpleNamespace(items=orders)])
    with pytest.raises(RuntimeError, match="item limit"):
        asyncio.run(SDKExecutionAdapter(client).cancel_all())
    assert client.calls == [("list", {})]
