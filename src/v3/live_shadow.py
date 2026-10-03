"""Read-only live shadow of the V7 weather strategy.

Runs the unchanged V7 weather evaluation against live public order books,
optionally reads the configured account (balance, positions, open orders and
trade history) through the separately gated account-read client, and builds
the exact post-only intent the gated live service would receive. Every intent
is passed through the real ``RiskEngine`` and logged; nothing is ever
submitted. The secure order client is never constructed here.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from .api import UnifiedPolymarketAPI
from .config import V3Settings
from .live_service import LiveOrderService
from .paper import build_weather_forecast, station_metadata_reason, weather_policy_from_env
from .paper_weather import (
    NOAAStationObservations,
    OffsetWeatherPublicClient,
    WeatherEvaluation,
    evaluate_weather_universe,
)
from .reconciliation import LocalSnapshot, Reconciler, RemoteSnapshot
from .risk import OrderIntent, RiskEngine, RiskLimits
from .v7_weather_intent import propose_v7_weather_order

ZERO = Decimal("0")
SEED_FILES = ("station-metadata.json", "weather_calibration.json")
TRADE_HISTORY_MAX_ITEMS = 10_000
TRADE_HISTORY_PAGE_LIMIT = 100


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


@dataclass(frozen=True)
class LiveShadowSettings:
    data_dir: Path
    seed_dir: Path | None = None
    scan_interval_seconds: float = 300.0
    order_ttl_seconds: int = 900
    max_order_ttl_seconds: int = 1_800
    max_quote_age_seconds: int = 300
    max_open_orders: int = 5
    max_positions: int = 15
    max_new_orders_per_cycle: int = 3
    open_meteo_max_requests_per_day: int = 96
    open_meteo_cache_seconds: float = 86_400.0

    def __post_init__(self) -> None:
        if self.scan_interval_seconds <= 0:
            raise ValueError("shadow scan interval must be positive")
        if not 121 <= self.order_ttl_seconds <= self.max_order_ttl_seconds:
            raise ValueError("order TTL must be in [121, max order TTL]")
        if self.max_quote_age_seconds < 0:
            raise ValueError("max quote age cannot be negative")
        if min(self.max_open_orders, self.max_positions, self.max_new_orders_per_cycle) < 1:
            raise ValueError("order, position and per-cycle caps must be positive")

    @classmethod
    def from_env(cls, root: Path) -> "LiveShadowSettings":
        raw_dir = Path(os.getenv("V3_LIVE_DATA_DIR", "data/live-shadow"))
        seed = os.getenv("V3_LIVE_SEED_DIR", "").strip()
        return cls(
            data_dir=(raw_dir if raw_dir.is_absolute() else root / raw_dir).resolve(),
            seed_dir=Path(seed).resolve() if seed else None,
            scan_interval_seconds=float(os.getenv("V3_LIVE_SCAN_INTERVAL_SECONDS", "300")),
            order_ttl_seconds=int(os.getenv("V3_LIVE_ORDER_TTL_SECONDS", "900")),
            max_order_ttl_seconds=int(os.getenv("V3_LIVE_MAX_ORDER_TTL_SECONDS", "1800")),
            max_quote_age_seconds=int(os.getenv("V3_LIVE_MAX_QUOTE_AGE_SECONDS", "300")),
            max_open_orders=int(os.getenv("V3_LIVE_MAX_OPEN_ORDERS", "5")),
            max_positions=int(os.getenv("V3_LIVE_MAX_POSITIONS", "15")),
            max_new_orders_per_cycle=int(os.getenv("V3_LIVE_MAX_NEW_ORDERS_PER_CYCLE", "3")),
            open_meteo_max_requests_per_day=int(
                os.getenv("V3_PAPER_OPEN_METEO_MAX_REQUESTS_PER_DAY", "96")
            ),
            open_meteo_cache_seconds=float(
                os.getenv("V3_PAPER_OPEN_METEO_CACHE_SECONDS", "86400")
            ),
        )


def risk_limits_for(settings: V3Settings, shadow: LiveShadowSettings) -> RiskLimits:
    """The limits the live service factory would require to match settings."""
    if settings.max_daily_loss is None or settings.max_drawdown_amount is None:
        raise ValueError("V3_MAX_DAILY_LOSS and V3_MAX_DRAWDOWN_AMOUNT must be set")
    return RiskLimits(
        max_capital=settings.max_capital,
        reserve_fraction=settings.reserve_fraction,
        max_order_notional=settings.max_order_notional,
        max_event_exposure=settings.max_order_notional,
        max_open_orders=shadow.max_open_orders,
        max_positions=shadow.max_positions,
        daily_loss_limit=settings.max_daily_loss,
        max_quote_age_seconds=shadow.max_quote_age_seconds,
        max_order_ttl_seconds=shadow.max_order_ttl_seconds,
        max_drawdown_amount=settings.max_drawdown_amount,
        max_drawdown_fraction=settings.max_drawdown_fraction,
    )


def select_v7_candidates(
    evaluations: tuple[WeatherEvaluation, ...],
) -> list[WeatherEvaluation]:
    """V7 paper selection: best net edge per weather event, ranked by edge."""
    selected: dict[str, WeatherEvaluation] = {}
    for evaluation in evaluations:
        if evaluation.strategy != "weather_directional" or not evaluation.paper_tradeable:
            continue
        previous = selected.get(evaluation.event_key)
        if previous is None or evaluation.decision.net_edge > previous.decision.net_edge:
            selected[evaluation.event_key] = evaluation
    return sorted(selected.values(), key=lambda item: item.decision.net_edge, reverse=True)


class ShadowStore:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        data_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = data_dir / "shadow_state.json"
        self.status_path = data_dir / "status.json"
        self.intents_path = data_dir / "shadow_intents.jsonl"
        self.account_path = data_dir / "account_snapshots.jsonl"

    def seed(self, seed_dir: Path | None) -> list[str]:
        """Copy V7 station metadata and calibration once; never overwrite."""
        copied: list[str] = []
        if seed_dir is None:
            return copied
        for name in SEED_FILES:
            source, target = seed_dir / name, self.data_dir / name
            if source.is_file() and not target.exists():
                shutil.copy2(source, target)
                copied.append(name)
        return copied

    def load_state(self) -> dict[str, Any]:
        if not self.state_path.is_file():
            return {}
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def _write_json(self, path: Path, payload: Any) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, default=_json_default, sort_keys=True, indent=2),
                       encoding="utf-8")
        tmp.replace(path)

    def save_state(self, state: dict[str, Any]) -> None:
        self._write_json(self.state_path, state)

    def write_status(self, status: dict[str, Any]) -> None:
        self._write_json(self.status_path, status)

    def append(self, path: Path, payload: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=_json_default, sort_keys=True) + "\n")


@dataclass(frozen=True)
class AccountView:
    remote: RemoteSnapshot
    equity: Decimal
    daily_pnl: Decimal
    peak_equity: Decimal
    reconciliation: dict[str, Any]
    trade_history_rows: int | None
    trade_history_error: str | None


class LiveShadowRunner:
    def __init__(
        self,
        *,
        api: UnifiedPolymarketAPI,
        settings: V3Settings,
        shadow: LiveShadowSettings,
        store: ShadowStore,
        weather_client: Any,
        forecast: Any,
        observation_provider: Any,
        account_reads: bool,
    ) -> None:
        self.api = api
        self.settings = settings
        self.shadow = shadow
        self.store = store
        self.weather_client = weather_client
        self.forecast = forecast
        self.observation_provider = observation_provider
        self.account_reads = account_reads
        self.risk = RiskEngine(risk_limits_for(settings, shadow))
        self.policy = weather_policy_from_env()
        self.station_metadata_path = store.data_dir / "station-metadata.json"
        self.state = store.load_state()

    async def _account_view(self, now: datetime) -> AccountView:
        remote = await self.api.fetch_remote_snapshot()
        equity = remote.cash + sum((p.current_value for p in remote.positions), ZERO)
        day = now.date().isoformat()
        if self.state.get("day") != day:
            self.state["day"] = day
            self.state["day_start_equity"] = str(equity)
        peak = max(Decimal(self.state.get("peak_equity", "0")), equity)
        self.state["peak_equity"] = str(peak)
        if "baseline_at" not in self.state:
            self.state.update({
                "baseline_at": now.isoformat(),
                "baseline_cash": str(remote.cash),
                "baseline_equity": str(equity),
                "baseline_position_tokens": sorted(p.token_id for p in remote.positions),
                "baseline_open_order_ids": sorted(o.order_id for o in remote.open_orders),
            })
        daily_pnl = equity - Decimal(self.state["day_start_equity"])

        # What the strict live service would see for a bot with no history yet.
        report = Reconciler(external_condition_ids=self.settings.external_condition_ids).compare(
            LocalSnapshot(
                cash=Decimal(self.state["baseline_cash"]),
                position_tokens=frozenset(), order_ids=frozenset(),
                position_quantities={}, position_cost_basis={},
            ),
            remote,
        )
        rows: int | None = None
        history_error: str | None = None
        try:
            rows = len(await self.api.fetch_account_trades(
                max_items=TRADE_HISTORY_MAX_ITEMS, page_limit=TRADE_HISTORY_PAGE_LIMIT,
            ))
        except Exception as exc:
            history_error = f"{type(exc).__name__}: {exc}"
        return AccountView(
            remote=remote,
            equity=equity,
            daily_pnl=daily_pnl,
            peak_equity=peak,
            reconciliation={
                "safe_to_trade": report.safe_to_trade,
                "cash_delta_vs_baseline": report.cash_delta,
                "unknown_positions": len(report.unknown_positions),
                "external_positions": len(report.external_positions),
                "unknown_orders": len(report.unknown_orders),
                "position_mismatches": len(report.position_mismatches),
            },
            trade_history_rows=rows,
            trade_history_error=history_error,
        )

    async def _build_intent(
        self, evaluation: WeatherEvaluation, now: datetime,
    ) -> tuple[dict[str, Any], OrderIntent | None]:
        """Station gate, fresh verified context and the unchanged V7 bridge."""
        record: dict[str, Any] = {
            "at": now.isoformat(),
            "event_key": evaluation.event_key,
            "condition_id": evaluation.condition_id,
            "token_id": evaluation.token_id,
            "question": evaluation.question,
            "side": evaluation.side,
            "model_probability": evaluation.decision.calibrated_probability,
            "net_edge_at_ask": evaluation.decision.net_edge,
            "minimum_edge": evaluation.decision.minimum_edge,
            "v7_shares": evaluation.shares,
            "ask": evaluation.ask,
            "bid": evaluation.bid,
            "submitted": False,
        }
        reason = station_metadata_reason(self.station_metadata_path, evaluation.city)
        if reason is not None:
            record.update(stage="station", outcome="blocked", reason=reason)
            return record, None
        shadow = evaluation.maker_shadow
        if shadow is None or evaluation.book_timestamp is None or evaluation.book_hash is None \
                or evaluation.decision_timestamp is None:
            record.update(stage="provenance", outcome="blocked",
                          reason="V7 evaluation lacks book or decision provenance")
            return record, None
        try:
            context = await self.api.get_verified_market_context(
                evaluation.condition_id, evaluation.token_id,
            )
        except Exception as exc:
            record.update(stage="market_context", outcome="blocked",
                          reason=f"{type(exc).__name__}: {exc}")
            return record, None
        proposal = propose_v7_weather_order(
            evaluation, context,
            best_bid=shadow.best_bid, best_ask=shadow.best_ask,
            book_timestamp=evaluation.book_timestamp, book_hash=evaluation.book_hash,
            decision_timestamp=evaluation.decision_timestamp, now=_utc_now(),
            size_step=Decimal("0.01"), order_cap=self.settings.max_order_notional,
            ttl_seconds=self.shadow.order_ttl_seconds,
            max_quote_age_seconds=self.shadow.max_quote_age_seconds,
            max_order_ttl_seconds=self.shadow.max_order_ttl_seconds,
            min_price=self.policy.min_price, max_price=self.policy.max_price,
        )
        record["book_unchanged_since_signal"] = context.book_hash == evaluation.book_hash
        if not proposal.proposed:
            record.update(stage="proposal", outcome="blocked", reason=proposal.reason)
            return record, None
        intent = OrderIntent(
            condition_id=evaluation.condition_id, token_id=evaluation.token_id,
            side="BUY", price=proposal.price, shares=proposal.shares,
            estimated_fee=ZERO, post_only=True, ttl_seconds=self.shadow.order_ttl_seconds,
            quote_age_seconds=proposal.quote_age_seconds or 0,
            tick_size=context.tick_size, min_order_size=context.min_order_size,
            market_accepting_orders=context.accepting_orders,
            rules_verified=context.rules_verified, disputed=context.disputed,
        )
        record.update(
            price=proposal.price, shares=proposal.shares,
            notional=intent.all_in_notional, expected_edge=proposal.expected_edge,
        )
        return record, intent

    async def _shadow_intent(
        self, evaluation: WeatherEvaluation, account: AccountView | None, now: datetime,
    ) -> dict[str, Any]:
        record, intent = await self._build_intent(evaluation, now)
        if intent is None:
            return record
        if account is None:
            record.update(stage="risk", outcome="proposal_only",
                          reason="no account snapshot; risk engine not evaluated")
            return record
        try:
            # Same account-to-risk mapping the live service applies before submit.
            state = LiveOrderService._risk_state(
                None, account.remote,  # type: ignore[arg-type]
                daily_pnl=account.daily_pnl, peak_equity=account.peak_equity,
            )
        except (ArithmeticError, AttributeError, TypeError, ValueError) as exc:
            record.update(stage="risk", outcome="blocked",
                          reason=f"account risk snapshot invalid: {exc}")
            return record
        decision = self.risk.evaluate(intent, state)
        if not account.reconciliation["safe_to_trade"]:
            record.update(stage="reconciliation", outcome="would_block",
                          reason="strict reconciliation would block this account",
                          risk_allowed=decision.allowed, risk_reason=decision.reason)
            return record
        record.update(
            stage="risk",
            outcome="would_submit" if decision.allowed else "blocked",
            reason=decision.reason,
            deployable_capital=decision.deployable_capital,
        )
        return record

    async def run_cycle(self, *, now: datetime | None = None) -> dict[str, Any]:
        now = now or _utc_now()
        account: AccountView | None = None
        account_error: str | None = None
        if self.account_reads:
            try:
                account = await self._account_view(now)
            except Exception as exc:
                account_error = f"{type(exc).__name__}: {exc}"
        if account is not None:
            self.store.append(self.store.account_path, {
                "at": now.isoformat(),
                "cash": account.remote.cash,
                "equity": account.equity,
                "positions": len(account.remote.positions),
                "open_orders": len(account.remote.open_orders),
                "daily_pnl": account.daily_pnl,
                "peak_equity": account.peak_equity,
                "reconciliation": account.reconciliation,
                "trade_history_rows": account.trade_history_rows,
                "trade_history_error": account.trade_history_error,
            })

        policy = self.policy
        if policy.kelly_sizing_enabled:
            bankroll = self.settings.max_capital * (Decimal("1") - self.settings.reserve_fraction)
            if account is not None:
                bankroll = max(ZERO, min(account.remote.cash, bankroll))
            policy = replace(
                policy, sizing_bankroll=bankroll,
                max_order_notional=min(self.settings.max_order_notional, policy.max_order_notional),
            )
        result = await evaluate_weather_universe(
            client=self.weather_client, forecast=self.forecast, policy=policy,
            observation_provider=self.observation_provider, now=now,
        )
        candidates = select_v7_candidates(result.evaluations)
        records = []
        for evaluation in candidates[: self.shadow.max_new_orders_per_cycle]:
            record = await self._shadow_intent(evaluation, account, now)
            self.store.append(self.store.intents_path, record)
            records.append(record)
        self.state["cycles"] = int(self.state.get("cycles", 0)) + 1
        self.store.save_state(self.state)
        outcomes: dict[str, int] = {}
        for record in records:
            outcomes[record["outcome"]] = outcomes.get(record["outcome"], 0) + 1
        status = {
            "mode": "LIVE_SHADOW",
            "orders_submitted": 0,
            "secure_order_client_initialized": False,
            "account_reads": self.account_reads,
            "account_error": account_error,
            "cycle": self.state["cycles"],
            "last_cycle_at": now.isoformat(),
            "weather_markets_evaluated": result.markets_evaluated,
            "weather_forecast_status": result.forecast_status,
            "weather_errors": len(result.errors),
            "v7_candidates": len(candidates),
            "shadow_outcomes_this_cycle": outcomes,
            "limits": {k: v for k, v in asdict(self.risk.limits).items()},
            "data_dir": self.store.data_dir,
        }
        if account is not None:
            status.update({
                "cash": account.remote.cash,
                "equity": account.equity,
                "daily_pnl": account.daily_pnl,
                "peak_equity": account.peak_equity,
                "positions": len(account.remote.positions),
                "open_orders": len(account.remote.open_orders),
                "reconciliation": account.reconciliation,
                "trade_history_rows": account.trade_history_rows,
                "trade_history_error": account.trade_history_error,
            })
        self.store.write_status(status)
        return status


async def run_live_shadow(
    settings: V3Settings, shadow: LiveShadowSettings, *, cycles: int = 0,
) -> None:
    if cycles < 0:
        raise ValueError("shadow cycles cannot be negative")
    store = ShadowStore(shadow.data_dir)
    store.seed(shadow.seed_dir)
    api = UnifiedPolymarketAPI(settings=settings)
    account_reads = not settings.account_client_errors()
    if account_reads:
        await api.initialize_account_client()
    runner = LiveShadowRunner(
        api=api, settings=settings, shadow=shadow, store=store,
        weather_client=OffsetWeatherPublicClient(api.public_client),
        forecast=build_weather_forecast(
            store.data_dir,
            max_requests_per_day=shadow.open_meteo_max_requests_per_day,
            cache_seconds=shadow.open_meteo_cache_seconds,
        ),
        observation_provider=NOAAStationObservations(),
        account_reads=account_reads,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop.set)
        except (NotImplementedError, RuntimeError):
            pass
    completed = 0
    while not stop.is_set():
        status = await runner.run_cycle()
        print(json.dumps({
            key: status.get(key) for key in (
                "cycle", "v7_candidates", "shadow_outcomes_this_cycle", "equity",
                "account_error", "weather_forecast_status",
            )
        }, default=_json_default, sort_keys=True), flush=True)
        completed += 1
        if cycles and completed >= cycles:
            break
        try:
            await asyncio.wait_for(stop.wait(), timeout=shadow.scan_interval_seconds)
        except TimeoutError:
            pass
