"""Transient read failures and local aborts must never become permanent latches."""

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest
from polymarket.errors import RateLimitError

from src.v3 import api as api_module
from src.v3 import live_accounting
from src.v3.api import POSITIONS_PAGE_SIZE, UnifiedPolymarketAPI
from src.v3.config import V3Settings
from src.v3.execution import V3OrderExecutor
from src.v3.ledger import EventLedger
from src.v3.live_runner import LiveRunnerSettings, local_snapshot
from src.v3.live_service import LiveOrderService
from src.v3.reconciliation import Reconciler, RemotePosition
from src.v3.streaming import StreamEventProcessor
from test_execution import account_state, live_settings, order_intent, risk_engine
from test_live_runner import (  # same test directory
    NOW, _EarlyExitAPI, _ExitService, _runner, _seed_managed_position, _Service,
)

D = Decimal


def _executor(client, ledger, **kwargs):
    executor = V3OrderExecutor(client, risk_engine(), ledger, settings=live_settings(), **kwargs)
    executor._live_service_authorized = True
    return executor


def _service_view(ledger):
    service = LiveOrderService.__new__(LiveOrderService)
    service._ledger = ledger
    return service


# --- Fix 1: stranded submissions -------------------------------------------------

def test_stale_quote_is_rejected_before_durable_submission_record(tmp_path):
    class Client:
        async def place_limit_order(self, **kwargs):
            raise AssertionError("no venue call")

    ledger = EventLedger(tmp_path / "stale.db")
    result = asyncio.run(_executor(Client(), ledger).submit(
        order_intent(quote_age_seconds=121), account_state(),
    ))
    assert not result.accepted
    assert not any(e.event_type == "order.submission.started" for e in ledger.events())
    assert _service_view(ledger)._unresolved_submissions() == ()


def test_quote_age_at_limit_is_accepted_by_risk_and_executor_with_advancing_clock(tmp_path):
    """RiskEngine allows age == max; the executor must not reject it on attempt one."""
    calls = []
    ticks = iter(range(1_000, 2_000))

    class Client:
        async def place_limit_order(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(ok=True, order_id="o", status="live")

    ledger = EventLedger(tmp_path / "boundary.db")
    executor = _executor(Client(), ledger, clock=lambda: next(ticks))
    result = asyncio.run(executor.submit(order_intent(quote_age_seconds=120), account_state()))
    assert result.accepted and len(calls) == 1


def test_retry_abort_after_started_writes_terminal_local_abort(tmp_path):
    calls = []
    now = [1_000.0]

    class Client:
        async def place_limit_order(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(ok=False, code=425, message="too early")

    async def sleep(seconds):
        now[0] += 60  # the quote ages past the limit while backing off

    ledger = EventLedger(tmp_path / "abort.db")
    executor = _executor(Client(), ledger, clock=lambda: now[0], sleep=sleep)
    result = asyncio.run(executor.submit(order_intent(quote_age_seconds=100), account_state()))

    assert not result.accepted and result.reason == "quote became stale during retry"
    assert len(calls) == 1
    events = list(ledger.events())
    assert [e.event_type for e in events] == [
        "order.submission.started", "order.submission.attempted", "order.retry", "order.rejected",
    ]
    abort = events[-1].payload
    assert abort["code"] == "local_abort" and abort["venue_attempts"] == 1
    assert abort["client_order_id"] == events[0].payload["client_order_id"]
    assert _service_view(ledger)._unresolved_submissions() == ()
    assert not StreamEventProcessor(ledger).reconciliation_required


def test_retry_abort_on_risk_change_writes_terminal_local_abort(tmp_path):
    class Client:
        async def place_limit_order(self, **kwargs):
            return SimpleNamespace(ok=False, code=503, message="post_only_mode")

    ledger = EventLedger(tmp_path / "risk-abort.db")
    engine = risk_engine()
    executor = V3OrderExecutor(Client(), engine, ledger, settings=live_settings())
    executor._live_service_authorized = True
    original = engine.evaluate
    seen = []

    def flaky_evaluate(intent, state):
        seen.append(1)
        decision = original(intent, state)
        if len(seen) > 1:
            return SimpleNamespace(allowed=False, reason="changed")
        return decision

    engine.evaluate = flaky_evaluate

    async def sleep(_):
        pass

    executor._sleep = sleep
    result = asyncio.run(executor.submit(order_intent(), account_state()))
    assert not result.accepted and result.reason == "risk rejected retry"
    rejected = [e for e in ledger.events() if e.event_type == "order.rejected"]
    assert [e.payload["code"] for e in rejected] == ["local_abort"]
    assert _service_view(ledger)._unresolved_submissions() == ()
    assert not StreamEventProcessor(ledger).reconciliation_required


# --- Fix 3: latches do not accumulate --------------------------------------------

def test_local_snapshot_appends_one_chronology_latch_across_cycles(tmp_path, monkeypatch):
    ledger = EventLedger(tmp_path / "chronology.db")

    def broken(processor):
        raise ValueError("bad chronology")

    monkeypatch.setattr(live_accounting, "confirmed_fills", broken)
    for _ in range(3):
        with pytest.raises(ValueError, match="chronology"):
            local_snapshot(StreamEventProcessor(ledger), D("100"))
    latches = [e for e in ledger.events() if e.event_type == "stream.reconciliation_required"]
    assert len(latches) == 1


def test_local_snapshot_appends_one_oversell_latch_across_cycles(tmp_path, monkeypatch):
    ledger = EventLedger(tmp_path / "oversell.db")
    fill = SimpleNamespace(size=D("5"), notional=D("4"), fee=D("0"), token_id="tok", side="SELL")
    monkeypatch.setattr(live_accounting, "confirmed_fills", lambda processor: [fill])
    for _ in range(3):
        with pytest.raises(ValueError, match="SELL exceeds"):
            local_snapshot(StreamEventProcessor(ledger), D("100"))
    latches = [e for e in ledger.events() if e.event_type == "stream.reconciliation_required"]
    assert len(latches) == 1


# --- Fix 2: trade-history read failure is a per-cycle block ----------------------

class _FailingHistoryService(_Service):
    async def recover_trade_history(self, *, max_items, page_limit):
        return {"imported_count": 0, "lifecycle_clear": False,
                "read_error": "trade history read failed: RateLimitError"}


def test_trade_history_read_failure_blocks_entries_and_exits_for_cycle_only(tmp_path, monkeypatch):
    positions = (RemotePosition("cond", "tok-exit", D("10"), D("8"), D("5")),)
    api = _EarlyExitAPI(positions=positions)
    state = {
        "baseline_at": NOW.isoformat(), "baseline_epoch": int(NOW.timestamp()),
        "baseline_cash": "100", "baseline_equity": "100",
        "external_condition_ids": ["cond-old"], "peak_equity": "100", "event_orders": {},
    }
    runner, _ = _runner(tmp_path, monkeypatch, api=api, state=state)
    _seed_managed_position(runner.ledger)
    runner.runner_settings = LiveRunnerSettings(shadow=runner.shadow, live_early_exit_enabled=True)
    service = _FailingHistoryService()
    runner.service = service

    status = asyncio.run(runner.run_cycle(now=NOW))

    reason = "trade history read failed: RateLimitError"
    assert status["healthy"] is False
    assert status["entry_block_reason"] == reason
    assert status["early_exit_block_reason"] == reason
    assert status["early_exits"] == []
    assert service.submitted == []
    assert not StreamEventProcessor(runner.ledger).reconciliation_required

    # The next cycle with a successful read runs exits again.
    exit_service = _ExitService(runner.ledger, api)
    runner.service = exit_service
    status = asyncio.run(runner.run_cycle(now=NOW))
    assert status["early_exits"][0]["outcome"] == "accepted"


# --- Fix 4: snapshot pagination and rate limits ----------------------------------

class _Pages:
    def __init__(self, items):
        self.items = items

    def __aiter__(self):
        async def pages():
            yield SimpleNamespace(items=tuple(self.items))
        return pages()


class _SnapshotClient:
    def __init__(self, failures=0, retry_after=None):
        self.failures = failures
        self.retry_after = retry_after
        self.position_kwargs = []

    async def get_balance_allowance(self, **kwargs):
        if self.failures:
            self.failures -= 1
            exc = RateLimitError("rate limited")
            if self.retry_after is not None:
                exc.retry_after = self.retry_after
            raise exc
        return SimpleNamespace(balance="10000000")

    def list_positions(self, **kwargs):
        self.position_kwargs.append(kwargs)
        return _Pages([])

    def list_open_orders(self, **kwargs):
        return _Pages([])


def _api(client):
    api = UnifiedPolymarketAPI(settings=V3Settings())
    api._secure_client = client
    return api


def test_positions_are_read_with_large_page_size():
    client = _SnapshotClient()
    asyncio.run(_api(client).fetch_remote_snapshot())
    assert client.position_kwargs == [{"size_threshold": 0, "page_size": POSITIONS_PAGE_SIZE}]
    assert POSITIONS_PAGE_SIZE == 500


def test_rate_limited_snapshot_retries_with_capped_delay(monkeypatch):
    delays = []

    async def sleep(seconds):
        delays.append(seconds)

    monkeypatch.setattr(api_module, "_sleep", sleep)
    client = _SnapshotClient(failures=2, retry_after=60)
    snapshot = asyncio.run(_api(client).fetch_remote_snapshot())
    assert snapshot.cash == D("10")
    assert delays == [10.0, 10.0]

    delays.clear()
    client = _SnapshotClient(failures=1)
    asyncio.run(_api(client).fetch_remote_snapshot())
    assert delays == [api_module.SNAPSHOT_RATE_LIMIT_BASE_DELAY_SECONDS]


def test_rate_limited_snapshot_gives_up_after_bounded_attempts(monkeypatch):
    async def sleep(seconds):
        pass

    monkeypatch.setattr(api_module, "_sleep", sleep)
    client = _SnapshotClient(failures=5)
    with pytest.raises(RateLimitError):
        asyncio.run(_api(client).fetch_remote_snapshot())
    assert client.failures == 5 - api_module.SNAPSHOT_RATE_LIMIT_ATTEMPTS


def test_non_rate_limit_snapshot_errors_are_not_retried(monkeypatch):
    calls = []

    class Client(_SnapshotClient):
        async def get_balance_allowance(self, **kwargs):
            calls.append(1)
            raise RuntimeError("boom")

    async def sleep(seconds):
        raise AssertionError("must not retry")

    monkeypatch.setattr(api_module, "_sleep", sleep)
    with pytest.raises(RuntimeError):
        asyncio.run(_api(Client()).fetch_remote_snapshot())
    assert calls == [1]


def test_rate_limited_cycle_snapshot_ends_cycle_as_soft_failure(tmp_path, monkeypatch):
    class LimitedAPI(_EarlyExitAPI):
        async def fetch_remote_snapshot(self):
            self.snapshot_fetches += 1
            raise RateLimitError("rate limited")

    api = LimitedAPI()
    service = _Service()
    runner, store = _runner(tmp_path, monkeypatch, api=api, service=service)

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert status["healthy"] is False
    assert status["remote_snapshot_error"] == "RateLimitError"
    assert status["entry_block_reason"] == "remote account snapshot unavailable"
    assert status["outcomes_this_cycle"] == {}
    assert service.submitted == []
    assert not StreamEventProcessor(runner.ledger).reconciliation_required
    assert not store.errors_path.exists()
