import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.v3.api import UnifiedPolymarketAPI
from src.v3.reconciliation import RemoteAccountOrder


class _Client:
    def __init__(self, record):
        self.record = record

    async def get_order(self, *, order_id):
        return self.record


class _API(UnifiedPolymarketAPI):
    def __init__(self, client):
        self.client = client

    def _authenticated_client(self):
        return self.client


def _record(**changes):
    values = {
        "id": "order-1", "condition_id": "condition-1", "token_id": "token-1",
        "side": "BUY", "price": Decimal("0.25"), "original_size": Decimal("5.39"),
        "size_matched": Decimal("0"), "status": "CANCELED",
    }
    values.update(changes)
    return SimpleNamespace(**values)


def test_fetch_account_order_normalizes_exact_read_only_record():
    result = asyncio.run(_API(_Client(_record())).fetch_account_order("order-1"))

    assert result == RemoteAccountOrder(
        order_id="order-1", condition_id="condition-1", token_id="token-1", side="BUY",
        price=Decimal("0.25"), original_size=Decimal("5.39"),
        size_matched=Decimal("0"), status="CANCELED",
    )


def test_fetch_account_order_rejects_a_different_exchange_order_id():
    with pytest.raises(RuntimeError, match="different order ID"):
        asyncio.run(_API(_Client(_record(id="other"))).fetch_account_order("order-1"))


def test_fetch_account_order_rejects_malformed_exchange_economics():
    with pytest.raises(RuntimeError, match="malformed"):
        asyncio.run(
            _API(_Client(_record(size_matched=Decimal("6")))).fetch_account_order("order-1")
        )
