"""Transient read failures and local aborts must never become permanent latches."""

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest
from polymarket.errors import (
    RateLimitError, RequestRejectedError, TransportError, UnexpectedResponseError,
)
from polymarket.errors import TimeoutError as SDKTimeoutError

from src.v3 import live_accounting
from src.v3 import live_runner as live_runner_module
from src.v3.api import POSITIONS_PAGE_SIZE, UnifiedPolymarketAPI
from src.v3.config import V3Settings
from src.v3.execution import V3OrderExecutor
from src.v3.ledger import EventLedger, LedgerEvent
from src.v3.live_runner import LiveRunnerSettings, fetch_cycle_snapshot, local_snapshot
from src.v3.live_service import LiveOrderService
from src.v3.reconciliation import Reconciler, RemotePosition, RemoteSnapshot
from src.v3.streaming import StreamEventProcessor
from test_execution import account_state, live_settings, order_intent, risk_engine
from test_live_runner import (  # same test directory
    NOW, _API, _EarlyExitAPI, _ExitService, _accept, _runner, _seed_managed_position, _Service,
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




def test_trade_history_read_failure_skips_expiry_cancel_and_redemption_marking(tmp_path, monkeypatch):
    calls = []

    class HistoryAPI(_API):
        async def fetch_complete_account_trade_history(self, **kwargs):
            calls.append("history")
            raise AssertionError("cancel marking must be skipped")

    from src.v3 import live_redemption

    async def no_redemptions(*args, **kwargs):
        calls.append("redemption")
        return []

    monkeypatch.setattr(live_redemption, "recognize_remote_redemptions", no_redemptions)
    runner, _ = _runner(tmp_path, monkeypatch, api=HistoryAPI(), service=_FailingHistoryService())
    # Expired long ago and absent from the snapshot: normally marked canceled.
    _accept(runner.ledger, "filled-maybe", expiration=int(NOW.timestamp()) - 10_000)

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert status["orders_marked_expired"] == []
    assert status["orders_marked_terminal_canceled"] == []
    assert status["lifecycle_marking_skipped"] == "trade history read failed: RateLimitError"
    assert calls == []
    assert "filled-maybe" in StreamEventProcessor(runner.ledger).active_order_ids

    runner.service = _Service()
    status = asyncio.run(runner.run_cycle(now=NOW))
    assert status["orders_marked_expired"] == ["filled-maybe"]
    assert "redemption" in calls


class _RaisingHistoryAPI:
    def __init__(self, rows=()):
        self.rows = rows
        self.error = None

    async def fetch_account_trades(self, *, max_items, page_limit):
        if self.error:
            raise self.error
        return self.rows


def test_trade_history_block_clears_only_after_import_loop_completes(tmp_path, monkeypatch):
    api = _RaisingHistoryAPI(rows=(object(),))
    api.error = RateLimitError("rate limited")
    service = LiveOrderService(
        api, SimpleNamespace(), risk_engine(), EventLedger(tmp_path / "h.db"), Reconciler(),
    )
    asyncio.run(service.recover_trade_history(max_items=10, page_limit=1))
    assert service._trade_history_read_error == "trade history read failed: RateLimitError"

    api.error = None

    def explode(self, trade):
        raise RuntimeError("import failed")

    monkeypatch.setattr(StreamEventProcessor, "import_remote_trade", explode)
    with pytest.raises(RuntimeError):
        asyncio.run(service.recover_trade_history(max_items=10, page_limit=1))
    assert service._trade_history_read_error is not None

    monkeypatch.setattr(StreamEventProcessor, "import_remote_trade",
                        lambda self, trade: SimpleNamespace(accepted=True, duplicate=True,
                                                            requires_reconciliation=False))
    asyncio.run(service.recover_trade_history(max_items=10, page_limit=1))
    assert service._trade_history_read_error is None


# --- local_abort terminal vs an earlier submission latch --------------------------

def test_local_abort_closes_its_submission_even_after_prior_submission_latch(tmp_path):
    ledger = EventLedger(tmp_path / "latched.db")
    ledger.append(LedgerEvent.create("order.submission.started", {"client_order_id": "stranded"}))
    assert StreamEventProcessor(ledger).reconciliation_required
    latches = [e for e in ledger.events() if e.event_type == "order.submission_reconciliation_latched"]
    assert len(latches) == 1

    ledger.append(LedgerEvent.create("order.submission.started", {"client_order_id": "aborted"}))
    ledger.append(LedgerEvent.create("order.rejected", {
        "client_order_id": "aborted", "code": "local_abort", "message": "risk rejected retry",
        "venue_attempts": 1,
    }))
    replayed = StreamEventProcessor(ledger)
    latches = [e for e in ledger.events() if e.event_type == "order.submission_reconciliation_latched"]
    # The aborted submission does not widen the latch; only the stranded one remains.
    assert len(latches) == 1
    assert all("aborted" not in reason for reason in replayed.reconciliation_reasons)

    # A venue rejection after a latch keeps the prior fail-closed behaviour.
    ledger.append(LedgerEvent.create("order.submission.started", {"client_order_id": "venue"}))
    ledger.append(LedgerEvent.create("order.rejected", {
        "client_order_id": "venue", "code": "400", "message": "invalid",
    }))
    StreamEventProcessor(ledger)
    reasons = [e.payload["reason"] for e in ledger.events()
               if e.event_type == "order.submission_reconciliation_latched"]
    assert any("venue" in reason for reason in reasons)


# --- require_reconciliation_once ---------------------------------------------------

def test_require_reconciliation_once_dedups_without_changing_base_semantics(tmp_path):
    ledger = EventLedger(tmp_path / "once.db")
    processor = StreamEventProcessor(ledger)
    processor.require_reconciliation_once("reason A")
    processor.require_reconciliation_once("reason A")
    StreamEventProcessor(ledger).require_reconciliation_once("reason A")
    assert len([e for e in ledger.events() if e.event_type == "stream.reconciliation_required"]) == 1

    processor.require_reconciliation("reason B")
    processor.require_reconciliation("reason B")
    assert len([e for e in ledger.events() if e.event_type == "stream.reconciliation_required"]) == 3


# --- Fix 4: snapshot pagination, retry and soft failure ---------------------------

class _Pages:
    def __init__(self, items):
        self.items = items

    def __aiter__(self):
        async def pages():
            yield SimpleNamespace(items=tuple(self.items))
        return pages()


class _SnapshotClient:
    def __init__(self, error=None):
        self.error = error
        self.balance_calls = 0
        self.position_kwargs = []

    async def get_balance_allowance(self, **kwargs):
        self.balance_calls += 1
        if self.error is not None:
            raise self.error
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


def test_api_snapshot_is_single_attempt_so_submit_preflight_never_sleeps():
    client = _SnapshotClient(error=RateLimitError("rate limited"))
    with pytest.raises(RateLimitError):
        asyncio.run(_api(client).fetch_remote_snapshot())
    assert client.balance_calls == 1


class _ScriptedSnapshotAPI:
    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    async def fetch_remote_snapshot(self):
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


@pytest.fixture
def delays(monkeypatch):
    recorded = []

    async def sleep(seconds):
        recorded.append(seconds)

    monkeypatch.setattr(live_runner_module, "_sleep", sleep)
    return recorded


SNAP = RemoteSnapshot(D("10"), (), ())


def test_cycle_snapshot_retries_rate_limit_with_backoff(delays):
    api = _ScriptedSnapshotAPI(RateLimitError("429"), RateLimitError("429"), SNAP)
    assert asyncio.run(fetch_cycle_snapshot(api)) is SNAP
    assert delays == [2.0, 4.0] and api.calls == 3


def test_cycle_snapshot_honours_capped_retry_after_on_503(delays):
    api = _ScriptedSnapshotAPI(
        RequestRejectedError("busy", status=503, retry_after=60),
        RequestRejectedError("busy", status=503, retry_after=1.5),
        SNAP,
    )
    assert asyncio.run(fetch_cycle_snapshot(api)) is SNAP
    assert delays == [10.0, 1.5]


def test_cycle_snapshot_gives_up_after_bounded_attempts(delays):
    api = _ScriptedSnapshotAPI(*(RateLimitError("429") for _ in range(5)))
    with pytest.raises(RateLimitError):
        asyncio.run(fetch_cycle_snapshot(api))
    assert api.calls == 3 and len(delays) == 2


@pytest.mark.parametrize("error", [
    RequestRejectedError("bad gateway", status=502),
    RequestRejectedError("bad request", status=400),
    RuntimeError("content"),
])
def test_cycle_snapshot_does_not_retry_other_errors(delays, error):
    api = _ScriptedSnapshotAPI(error, SNAP)
    with pytest.raises(type(error)):
        asyncio.run(fetch_cycle_snapshot(api))
    assert api.calls == 1 and delays == []


@pytest.mark.parametrize("error", [
    RateLimitError("429"),
    RequestRejectedError("unavailable", status=503),
    RequestRejectedError("bad gateway", status=502),
    UnexpectedResponseError("html"),
    SDKTimeoutError("slow"),
    TransportError("reset"),
])
def test_transient_cycle_snapshot_failure_is_soft_and_advances_cycle(tmp_path, monkeypatch, delays, error):
    class FailingAPI(_API):
        async def fetch_remote_snapshot(self):
            self.snapshot_fetches += 1
            raise error

    service = _Service()
    runner, store = _runner(tmp_path, monkeypatch, api=FailingAPI(), service=service)
    runner.state["cycles"] = 7

    status = asyncio.run(runner.run_cycle(now=NOW))

    assert status["healthy"] is False
    assert status["remote_snapshot_error"] == type(error).__name__
    assert status["entry_block_reason"] == "remote account snapshot unavailable"
    assert status["outcomes_this_cycle"] == {}
    assert status["reconciliation"]["safe_to_trade"] is False
    assert status["reconciliation"]["invalid_snapshot"] is True
    assert status["cycle"] == 8 and store.load_state()["cycles"] == 8
    assert service.submitted == []
    assert not StreamEventProcessor(runner.ledger).reconciliation_required
    assert not store.errors_path.exists()


@pytest.mark.parametrize("error", [
    RequestRejectedError("bad request", status=400),
    RuntimeError("duplicate position identity in account snapshot"),
    TimeoutError("builtin"),
])
def test_non_transient_cycle_snapshot_failure_still_raises(tmp_path, monkeypatch, delays, error):
    class FailingAPI(_API):
        async def fetch_remote_snapshot(self):
            raise error

    runner, _ = _runner(tmp_path, monkeypatch, api=FailingAPI(), service=_Service())
    with pytest.raises(type(error)):
        asyncio.run(runner.run_cycle(now=NOW))
