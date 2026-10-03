"""Narrow offline boundary adapter for polymarket SDK 0.6.0.

The caller owns client construction and all authorization/network policy.
"""
from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from polymarket import CancelOrdersResponse

CANCEL_ALL_PAGE_LIMIT = 100
CANCEL_ALL_ORDER_LIMIT = 10_000


class SDKExecutionAdapter:
    """Translate executor operations to the SDK's signed-order API."""

    def __init__(self, client: Any, *, clock: Callable[[], float] = time.time):
        self._client = client
        self._clock = clock

    async def place_limit_order(
        self, *, token_id: str, price: Any, size: Any, side: str,
        post_only: bool = True, expiration: int | None = None, **kwargs: Any,
    ) -> Any:
        create = getattr(self._client, "create_limit_order", None)
        post = getattr(self._client, "post_order", None)
        if not callable(create) or not callable(post):
            raise TypeError("SDK client lacks create_limit_order/post_order")
        if post_only is not True:
            raise ValueError("post_only must be exactly True")
        if type(expiration) is not int:
            raise TypeError("GTD expiration must be an int")
        if expiration < self._clock() + 180:
            raise ValueError("GTD expiration must be at least 180 seconds from now")
        signed = await create(
            token_id=token_id, price=price, size=size, side=side,
            post_only=True, expiration=expiration,
        )
        return await post(signed)

    async def cancel_all(self) -> CancelOrdersResponse:
        list_orders = getattr(self._client, "list_open_orders", None)
        cancel = getattr(self._client, "cancel_orders", None)
        if not callable(list_orders) or not callable(cancel):
            raise TypeError("SDK client lacks list_open_orders/cancel_orders")
        paginator = list_orders()
        if not hasattr(paginator, "__aiter__"):
            raise TypeError("SDK open-orders paginator is not asynchronously iterable")
        order_ids: list[str] = []
        seen_order_ids: set[str] = set()
        pages_seen = raw_order_rows = 0
        async for page in paginator:
            pages_seen += 1
            if pages_seen > CANCEL_ALL_PAGE_LIMIT:
                raise RuntimeError("open-orders cancellation page limit exceeded")
            items = getattr(page, "items", None)
            if items is None or isinstance(items, (str, bytes)):
                raise TypeError("SDK open-orders page has malformed items")
            try:
                iterator = iter(items)
            except TypeError as exc:
                raise TypeError("SDK open-orders page has malformed items") from exc
            for order in iterator:
                raw_order_rows += 1
                if raw_order_rows > CANCEL_ALL_ORDER_LIMIT:
                    raise RuntimeError("open-orders cancellation item limit exceeded")
                order_id = getattr(order, "id", None)
                if not isinstance(order_id, str) or not order_id.strip():
                    raise ValueError("open order has malformed or missing identity")
                if order_id in seen_order_ids:
                    raise ValueError("duplicate open order identity")
                seen_order_ids.add(order_id)
                order_ids.append(order_id)
        if not order_ids:
            return CancelOrdersResponse(canceled=())
        return await cancel(order_ids=tuple(order_ids))
