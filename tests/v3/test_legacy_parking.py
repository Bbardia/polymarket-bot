import pytest
from src.polymarket_client import PolymarketClient


def test_legacy_v1_path_never_constructs_client_or_reads_account():
    client=PolymarketClient()
    assert client.init_trading_client() is False
    with pytest.raises(RuntimeError,match='parked'): client.get_order_book('token')
    with pytest.raises(RuntimeError,match='parked'): client.get_price('token')
    client._api_creds_set=True
    assert client.get_positions()==[]
