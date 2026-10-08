import asyncio
from datetime import datetime, timezone
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


def test_default_live_capital_matches_selected_limit(monkeypatch):
    monkeypatch.delenv("V3_MAX_CAPITAL", raising=False)
    assert V3Settings().max_capital == D("100")
    assert V3Settings.from_env().max_capital == D("100")


def test_public_market_context_resolves_market_and_matching_token_book():
    class AsyncPages:
        def __init__(self, items):
            self.items = items
        def __aiter__(self):
            async def pages():
                yield SimpleNamespace(items=self.items)
            return pages()

    market = SimpleNamespace(
        condition_id="condition-1", question="Question",
        outcomes=SimpleNamespace(
            yes=SimpleNamespace(token_id="token-1"), no=SimpleNamespace(token_id="token-2"),
        ),
        state=SimpleNamespace(accepting_orders=True, active=True, closed=False, archived=False, neg_risk=False),
        trading=SimpleNamespace(
            minimum_tick_size=D("0.01"), minimum_order_size=D("5"),
            fees_enabled=False, fee_schedule=None,
        ),
        resolution=SimpleNamespace(source="official", uma_resolution_status=None),
    )
    book = SimpleNamespace(
        condition_id="condition-1", token_id="token-1", timestamp=datetime.now(timezone.utc),
        tick_size=D("0.01"), min_order_size=D("5"), neg_risk=False, hash="book-hash",
    )
    class PublicClient:
        def list_markets(self, **kwargs):
            assert kwargs["condition_ids"] == ["condition-1"]
            return AsyncPages([market])
        async def get_order_book(self, *, token_id):
            assert token_id == "token-1"
            return book

    api = UnifiedPolymarketAPI(settings=V3Settings())
    api.public_client = PublicClient()
    context = asyncio.run(api.get_verified_market_context("condition-1", "token-1"))
    assert context.condition_matches and context.token_matches and context.rules_verified and context.accepting_orders
    assert context.tick_size == D("0.01") and context.min_order_size == D("5")


def test_public_market_context_rejects_token_not_listed_in_condition_outcomes():
    class AsyncPages:
        def __aiter__(self):
            async def pages():
                yield SimpleNamespace(items=[market])
            return pages()

    market = SimpleNamespace(
        condition_id="condition-1", question="Question",
        outcomes=SimpleNamespace(
            yes=SimpleNamespace(token_id="token-2"), no=SimpleNamespace(token_id="token-3"),
        ),
        state=SimpleNamespace(accepting_orders=True, active=True, closed=False, archived=False),
        trading=SimpleNamespace(
            minimum_tick_size=D("0.01"), minimum_order_size=D("5"),
            fees_enabled=False, fee_schedule=None,
        ),
        resolution=SimpleNamespace(source="official", uma_resolution_status=None),
    )

    class PublicClient:
        def list_markets(self, **kwargs):
            return AsyncPages()
        async def get_order_book(self, *, token_id):
            return SimpleNamespace(
                condition_id="condition-1", token_id=token_id,
                timestamp=datetime.now(timezone.utc), tick_size=D("0.01"),
                min_order_size=D("5"), neg_risk=False, hash="book-hash",
            )

    api = UnifiedPolymarketAPI(settings=V3Settings())
    api.public_client = PublicClient()
    with pytest.raises(RuntimeError, match="not one of the market's outcome tokens"):
        asyncio.run(api.get_verified_market_context("condition-1", "token-1"))


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
        max_daily_loss=D("10"),
        max_drawdown_amount=D("10"),
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


def test_live_client_requires_explicit_finite_loss_and_drawdown_limits():
    settings = V3Settings(
        live_enabled=True,
        paper_trading=False,
        live_confirmation="I_UNDERSTAND_REAL_MONEY",
        private_key="0x" + "1" * 64,
        wallet_address="0x" + "2" * 40,
    )
    errors = settings.live_client_errors()
    assert "V3_MAX_DAILY_LOSS must be explicitly set" in errors
    assert "V3_MAX_DRAWDOWN_AMOUNT must be explicitly set" in errors


def test_live_risk_limits_are_loaded_from_explicit_environment_values(monkeypatch):
    monkeypatch.setenv("V3_MAX_CAPITAL", "100")
    monkeypatch.setenv("V3_MAX_DAILY_LOSS", "10")
    monkeypatch.setenv("V3_MAX_DRAWDOWN_AMOUNT", "10")
    settings = V3Settings.from_env()
    assert settings.max_capital == D("100")
    assert settings.max_daily_loss == D("10")
    assert settings.max_drawdown_amount == D("10")


def test_malformed_risk_environment_value_has_clear_validation_error(monkeypatch):
    monkeypatch.setenv("V3_MAX_DAILY_LOSS", "not-a-number")
    with pytest.raises(ValueError, match="V3_MAX_DAILY_LOSS must be a decimal number"):
        V3Settings.from_env()


@pytest.mark.parametrize("loss", [D("0"), D("-1"), D("51")])
def test_live_client_rejects_nonpositive_or_over_cap_daily_loss(loss):
    settings = V3Settings(
        live_enabled=True,
        paper_trading=False,
        live_confirmation="I_UNDERSTAND_REAL_MONEY",
        private_key="0x" + "1" * 64,
        wallet_address="0x" + "2" * 40,
        max_capital=D("50"),
        max_daily_loss=loss,
        max_drawdown_amount=D("10"),
    )
    assert "V3_MAX_DAILY_LOSS must be > 0 and <= V3_MAX_CAPITAL" in settings.live_client_errors()


@pytest.mark.parametrize("amount", [D("0"), D("-1"), D("101"), D("NaN"), D("Infinity")])
def test_live_client_rejects_invalid_drawdown_amount(amount):
    settings = V3Settings(
        live_enabled=True,
        paper_trading=False,
        live_confirmation="I_UNDERSTAND_REAL_MONEY",
        private_key="0x" + "1" * 64,
        wallet_address="0x" + "2" * 40,
        max_daily_loss=D("10"),
        max_drawdown_amount=amount,
    )
    assert "V3_MAX_DRAWDOWN_AMOUNT must be finite, > 0, and <= V3_MAX_CAPITAL" in settings.live_client_errors()


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
                    size=D("5"), current_value=D("2.5"), initial_value=D("2.5"),
                )
            ])

        def list_open_orders(self, **kwargs):
            return AsyncPages([
                SimpleNamespace(
                    id="order-1", condition_id="condition", token_id="token",
                    price=D("0.20"), original_size=D("5"), size_matched=D("1"),
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
    assert snapshot.positions[0].initial_value == D("2.5")
    assert snapshot.open_orders[0].order_id == "order-1"
    assert snapshot.open_orders[0].remaining_notional == D("0.80")


def test_authenticated_snapshot_rejects_missing_position_initial_value():
    class AsyncPages:
        def __init__(self, items):
            self.items = items
        def __aiter__(self):
            async def pages():
                yield SimpleNamespace(items=self.items)
            return pages()

    class Client:
        async def get_balance_allowance(self, **kwargs):
            return SimpleNamespace(balance="10000000")
        def list_positions(self, **kwargs):
            return AsyncPages([SimpleNamespace(
                size=D("5"), condition_id="c", token_id="t",
                current_value=D("1"), initial_value=None,
            )])
        def list_open_orders(self, **kwargs):
            return AsyncPages([])

    api = UnifiedPolymarketAPI(settings=V3Settings())
    api._secure_client = Client()
    with pytest.raises(RuntimeError, match="initial value"):
        asyncio.run(api.fetch_remote_snapshot())


def test_authenticated_snapshot_rejects_nonfinite_position_size():
    class AsyncPages:
        def __init__(self, items):
            self.items = items
        def __aiter__(self):
            async def pages():
                yield SimpleNamespace(items=self.items)
            return pages()

    class Client:
        async def get_balance_allowance(self, **kwargs):
            return SimpleNamespace(balance="10000000")
        def list_positions(self, **kwargs):
            return AsyncPages([SimpleNamespace(
                size=D("NaN"), condition_id="c", token_id="t",
                current_value=D("1"), initial_value=D("1"),
            )])
        def list_open_orders(self, **kwargs):
            return AsyncPages([])

    api = UnifiedPolymarketAPI(settings=V3Settings())
    api._secure_client = Client()
    with pytest.raises(RuntimeError, match="position size"):
        asyncio.run(api.fetch_remote_snapshot())


def test_authenticated_snapshot_rejects_invalid_open_order_sizes():
    class AsyncPages:
        def __init__(self, items):
            self.items = items
        def __aiter__(self):
            async def pages():
                yield SimpleNamespace(items=self.items)
            return pages()

    class Client:
        async def get_balance_allowance(self, **kwargs):
            return SimpleNamespace(balance="10000000")
        def list_positions(self, **kwargs):
            return AsyncPages([])
        def list_open_orders(self, **kwargs):
            return AsyncPages([SimpleNamespace(
                id="o", condition_id="c", token_id="t", price=D("0.2"),
                original_size=D("5"), size_matched=D("6"),
            )])

    api = UnifiedPolymarketAPI(settings=V3Settings())
    api._secure_client = Client()
    with pytest.raises(RuntimeError, match="invalid price or size"):
        asyncio.run(api.fetch_remote_snapshot())


def test_authenticated_snapshot_rejects_nonfinite_collateral():
    class AsyncPages:
        def __aiter__(self):
            async def pages():
                if False:
                    yield None
            return pages()

    class Client:
        async def get_balance_allowance(self, **kwargs):
            return SimpleNamespace(balance="NaN")
        def list_positions(self, **kwargs):
            return AsyncPages()
        def list_open_orders(self, **kwargs):
            return AsyncPages()

    api = UnifiedPolymarketAPI(settings=V3Settings())
    api._secure_client = Client()
    with pytest.raises(RuntimeError, match="collateral balance"):
        asyncio.run(api.fetch_remote_snapshot())


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


def test_snapshot_bounds_empty_pages_and_rejects_non_integer_limits():
    api = UnifiedPolymarketAPI(settings=V3Settings())
    with pytest.raises(ValueError, match="max_items"):
        asyncio.run(api.fetch_remote_snapshot(max_items=True))

    class Client:
        async def get_balance_allowance(self, **kwargs):
            return SimpleNamespace(balance="10000000")

        def list_positions(self, **kwargs):
            async def pages():
                while True:
                    yield SimpleNamespace(items=())
            return pages()

    api._secure_client = Client()
    with pytest.raises(RuntimeError, match="position reconciliation page limit"):
        asyncio.run(api.fetch_remote_snapshot(page_limit=2))


def test_snapshot_raw_row_cap_counts_zero_size_positions():
    class Client:
        async def get_balance_allowance(self, **kwargs):
            return SimpleNamespace(balance="10000000")

        def list_positions(self, **kwargs):
            async def pages():
                yield SimpleNamespace(items=(
                    SimpleNamespace(size=D("0")), SimpleNamespace(size=D("0")),
                ))
            return pages()

        def list_open_orders(self, **kwargs):
            raise AssertionError("position row cap should be enforced first")

    api = UnifiedPolymarketAPI(settings=V3Settings())
    api._secure_client = Client()
    with pytest.raises(RuntimeError, match="position reconciliation item limit"):
        asyncio.run(api.fetch_remote_snapshot(max_items=1))


def test_snapshot_rejects_position_token_duplicated_from_zero_size_row():
    class Client:
        async def get_balance_allowance(self, **kwargs):
            return SimpleNamespace(balance="10000000")

        def list_positions(self, **kwargs):
            async def pages():
                yield SimpleNamespace(items=(
                    SimpleNamespace(size=D("0"), token_id="t"),
                    SimpleNamespace(
                        size=D("1"), token_id="t", condition_id="c",
                        current_value=D("0.5"), initial_value=D("0.5"),
                    ),
                ))
            return pages()

        def list_open_orders(self):
            raise AssertionError("duplicate position identity should block before open-order read")

    api = UnifiedPolymarketAPI(settings=V3Settings())
    api._secure_client = Client()
    with pytest.raises(RuntimeError, match="duplicate position identity"):
        asyncio.run(api.fetch_remote_snapshot())


def test_snapshot_rejects_duplicate_open_order_ids():
    order = SimpleNamespace(
        id="same", condition_id="c", token_id="t", price=D("0.20"),
        original_size=D("5"), size_matched=D("0"),
    )

    class Client:
        async def get_balance_allowance(self, **kwargs):
            return SimpleNamespace(balance="10000000")

        def list_positions(self, **kwargs):
            async def pages():
                yield SimpleNamespace(items=())
            return pages()

        def list_open_orders(self):
            async def pages():
                yield SimpleNamespace(items=(order, order))
            return pages()

    api = UnifiedPolymarketAPI(settings=V3Settings())
    api._secure_client = Client()
    with pytest.raises(RuntimeError, match="duplicate open-order identity"):
        asyncio.run(api.fetch_remote_snapshot())


def _context_market(closed=False):
    return SimpleNamespace(
        condition_id="condition-1", question="Question",
        outcomes=SimpleNamespace(
            yes=SimpleNamespace(token_id="token-1"), no=SimpleNamespace(token_id="token-2"),
        ),
        state=SimpleNamespace(accepting_orders=not closed, active=True, closed=closed,
                              archived=False, neg_risk=True),
        trading=SimpleNamespace(
            minimum_tick_size=D("0.01"), minimum_order_size=D("5"),
            fees_enabled=False, fee_schedule=None,
        ),
        resolution=SimpleNamespace(source="official", uma_resolution_status=None),
    )


class _Pages:
    def __init__(self, items):
        self.items = items

    def __aiter__(self):
        async def pages():
            yield SimpleNamespace(items=self.items)
        return pages()


def test_market_context_carries_its_exact_book_snapshot_and_read_time():
    quiet_since = datetime(2026, 1, 1, tzinfo=timezone.utc)
    book = SimpleNamespace(
        condition_id="condition-1", token_id="token-1", timestamp=quiet_since,
        tick_size=D("0.01"), min_order_size=D("5"), neg_risk=True, hash="book-hash",
    )
    fetches = []

    class PublicClient:
        def list_markets(self, **kwargs):
            return _Pages([_context_market()])

        async def get_order_book(self, *, token_id):
            fetches.append(token_id)
            return book

    api = UnifiedPolymarketAPI(settings=V3Settings())
    api.public_client = PublicClient()
    before = datetime.now(timezone.utc)
    context = asyncio.run(api.get_verified_market_context("condition-1", "token-1"))
    assert context.book is book and fetches == ["token-1"]
    assert context.book_timestamp == quiet_since
    assert before <= context.fetched_at <= datetime.now(timezone.utc)


def test_market_context_reports_closed_market_distinctly():
    from src.v3.api import MarketClosedError

    calls = []

    class PublicClient:
        def list_markets(self, **kwargs):
            calls.append(kwargs.get("closed"))
            return _Pages([_context_market(closed=True)] if kwargs.get("closed") else [])

        async def get_order_book(self, *, token_id):
            raise AssertionError("closed markets need no book read")

    api = UnifiedPolymarketAPI(settings=V3Settings())
    api.public_client = PublicClient()
    with pytest.raises(MarketClosedError, match="market closed"):
        asyncio.run(api.get_verified_market_context("condition-1", "token-1"))
    assert calls == [None, True]


def test_market_context_unknown_condition_still_fails_generically():
    from src.v3.api import MarketClosedError

    class PublicClient:
        def list_markets(self, **kwargs):
            return _Pages([])

    api = UnifiedPolymarketAPI(settings=V3Settings())
    api.public_client = PublicClient()
    with pytest.raises(RuntimeError, match="exactly one market") as raised:
        asyncio.run(api.get_verified_market_context("condition-1", "token-1"))
    assert not isinstance(raised.value, MarketClosedError)
