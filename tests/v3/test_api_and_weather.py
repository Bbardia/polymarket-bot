import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.v3.api import UnifiedPolymarketAPI
from src.v3.config import V3Settings
from src.v3.weather import (
    ForecastFailureCache,
    ForecastRequest,
    brier_score,
    deduplicate_forecast_requests,
)


def D(value: str) -> Decimal:
    return Decimal(value)


def test_public_api_uses_official_unified_client_without_credentials():
    api = UnifiedPolymarketAPI()
    assert api.sdk_version == "0.6.0"
    assert api.public_client.__class__.__name__ == "AsyncPublicClient"
    assert not api.secure_client_initialized


def test_secure_client_is_lazy_and_requires_explicit_three_part_live_gate():
    calls = []

    async def fake_factory(**kwargs):
        calls.append(kwargs)
        return object()

    settings = V3Settings(
        live_enabled=True,
        paper_trading=False,
        live_confirmation="I_UNDERSTAND_REAL_MONEY",
        private_key="0x" + "1" * 64,
        wallet_address="0x" + "2" * 40,
    )
    api = UnifiedPolymarketAPI(settings=settings, secure_client_factory=fake_factory)
    assert calls == []
    asyncio.run(api.initialize_secure_client())
    assert len(calls) == 1
    assert api.secure_client_initialized


def test_account_read_client_has_separate_gate_and_does_not_require_live_mode():
    calls = []

    async def fake_factory(**kwargs):
        calls.append(kwargs)
        return object()

    settings = V3Settings(
        account_reads_enabled=True,
        live_enabled=False,
        paper_trading=True,
        private_key="0x" + "1" * 64,
        wallet_address="0x" + "2" * 40,
    )
    api = UnifiedPolymarketAPI(settings=settings, secure_client_factory=fake_factory)
    asyncio.run(api.initialize_account_client())
    assert len(calls) == 1
    assert calls[0].keys() == {"private_key", "wallet"}
    assert api.secure_client_initialized
    assert not hasattr(api, "secure_client")


def test_secure_client_refuses_default_or_incomplete_configuration():
    api = UnifiedPolymarketAPI(settings=V3Settings())
    with pytest.raises(RuntimeError, match="Live client refused"):
        asyncio.run(api.initialize_secure_client())


def test_authenticated_snapshot_maps_pusd_positions_and_open_orders_without_actions():
    class AsyncPages:
        def __init__(self, items):
            self.items = items

        def __aiter__(self):
            async def iterate():
                yield SimpleNamespace(items=tuple(self.items))
            return iterate()

    class FakeSecureClient:
        async def get_balance_allowance(self, **kwargs):
            assert kwargs == {"asset_type": "COLLATERAL"}
            return SimpleNamespace(balance=174_628_775)

        def list_positions(self, **kwargs):
            return AsyncPages([
                SimpleNamespace(
                    condition_id="dota-condition", token_id="dota-token",
                    size=D("5"), current_value=D("2.5"),
                )
            ])

        def list_open_orders(self, **kwargs):
            return AsyncPages([
                SimpleNamespace(
                    id="order-1", condition_id="condition", token_id="token"
                )
            ])

    async def fake_factory(**kwargs):
        return FakeSecureClient()

    settings = V3Settings(
        account_reads_enabled=True,
        live_enabled=False, paper_trading=True,
        private_key="0x" + "1" * 64, wallet_address="0x" + "2" * 40,
    )
    api = UnifiedPolymarketAPI(settings=settings, secure_client_factory=fake_factory)
    asyncio.run(api.initialize_account_client())
    snapshot = asyncio.run(api.fetch_remote_snapshot())
    assert snapshot.cash == D("174.628775")
    assert snapshot.positions[0].condition_id == "dota-condition"
    assert snapshot.open_orders[0].order_id == "order-1"


def test_weather_requests_are_batched_by_unique_city_date():
    requests = [
        ForecastRequest("bern", "2026-08-24"),
        ForecastRequest("bern", "2026-08-24"),
        ForecastRequest("zurich", "2026-08-24"),
    ]
    assert deduplicate_forecast_requests(requests) == (
        ForecastRequest("bern", "2026-08-24"),
        ForecastRequest("zurich", "2026-08-24"),
    )


def test_forecast_failure_cache_uses_exponential_backoff():
    cache = ForecastFailureCache(base_delay_seconds=60, max_delay_seconds=600)
    key = ForecastRequest("bern", "2026-08-24")
    cache.record_failure(key, now=1000)
    assert not cache.can_request(key, now=1059)
    assert cache.can_request(key, now=1060)
    cache.record_failure(key, now=1060)
    assert not cache.can_request(key, now=1179)
    assert cache.can_request(key, now=1180)
    cache.record_success(key)
    assert cache.can_request(key, now=1061)


def test_brier_score_is_point_in_time_probability_quality_metric():
    assert brier_score([D("0.8"), D("0.2")], [1, 0]) == D("0.04")
