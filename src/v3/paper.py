"""Strictly public-data paper worker and durable baseline state."""

from __future__ import annotations

import asyncio
import json
import os
import signal
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

import polymarket
from polymarket import AsyncPublicClient

from .market_context import MarketContext
from .paper_weather import (
    ForecastProvider,
    OpenMeteoEnsemble,
    WeatherEvaluation,
    WeatherPaperPolicy,
    evaluate_weather_universe,
)
from .strategies.complete_set import CompleteSetDecision, evaluate_complete_set

ZERO = Decimal("0")
ONE = Decimal("1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def _decimal_env(name: str, default: str) -> Decimal:
    value = Decimal(os.getenv(name, default))
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


@dataclass(frozen=True)
class PaperSettings:
    data_dir: Path
    paper_trading: bool = True
    live_enabled: bool = False
    account_reads_enabled: bool = False
    scan_interval_seconds: float = 300.0
    market_limit: int = 25
    discovery_max_markets: int = 2_000
    min_liquidity: Decimal = Decimal("1000")
    min_net_return: Decimal = Decimal("0.005")
    max_capital: Decimal = Decimal("50")
    reserve_fraction: Decimal = Decimal("0.25")
    max_order_notional: Decimal = Decimal("5")
    max_open_positions: int = 10
    weather_policy: WeatherPaperPolicy = field(
        default_factory=lambda: WeatherPaperPolicy(enabled=False)
    )

    def __post_init__(self) -> None:
        if self.scan_interval_seconds <= 0:
            raise ValueError("paper scan interval must be positive")
        if self.market_limit < 1 or self.market_limit > 200:
            raise ValueError("paper market limit must be in [1, 200]")
        if not (self.market_limit <= self.discovery_max_markets <= 5_000):
            raise ValueError("paper discovery limit must be between market limit and 5000")
        if self.min_liquidity < ZERO:
            raise ValueError("paper minimum liquidity cannot be negative")
        if self.min_net_return < ZERO:
            raise ValueError("paper minimum return cannot be negative")
        if self.max_capital <= ZERO or self.max_order_notional <= ZERO:
            raise ValueError("paper capital and order cap must be positive")
        if not (ZERO <= self.reserve_fraction < ONE):
            raise ValueError("paper reserve fraction must be in [0, 1)")
        if self.max_open_positions < 1:
            raise ValueError("paper max open positions must be positive")

    @classmethod
    def from_env(cls, root: Path) -> "PaperSettings":
        raw_dir = Path(os.getenv("V3_PAPER_DATA_DIR", "data/v3-paper"))
        data_dir = raw_dir if raw_dir.is_absolute() else root / raw_dir
        return cls(
            data_dir=data_dir.resolve(),
            paper_trading=_env_bool("PAPER_TRADING", True),
            live_enabled=_env_bool("ENABLE_V3_LIVE_TRADING", False),
            account_reads_enabled=_env_bool("ENABLE_V3_ACCOUNT_READS", False),
            scan_interval_seconds=float(os.getenv("V3_PAPER_SCAN_INTERVAL_SECONDS", "300")),
            market_limit=int(os.getenv("V3_PAPER_MARKET_LIMIT", "25")),
            discovery_max_markets=int(os.getenv("V3_PAPER_DISCOVERY_MAX_MARKETS", "2000")),
            min_liquidity=_decimal_env("V3_PAPER_MIN_LIQUIDITY", "1000"),
            min_net_return=_decimal_env("V3_PAPER_MIN_NET_RETURN", "0.005"),
            max_capital=_decimal_env("V3_MAX_CAPITAL", "50"),
            reserve_fraction=_decimal_env("V3_RESERVE_FRACTION", "0.25"),
            max_order_notional=_decimal_env("V3_PAPER_MAX_ORDER_NOTIONAL", "5"),
            max_open_positions=int(os.getenv("V3_PAPER_MAX_OPEN_POSITIONS", "10")),
            weather_policy=WeatherPaperPolicy(
                enabled=_env_bool("V3_PAPER_WEATHER_ENABLED", True),
                horizon_days=int(os.getenv("V3_PAPER_WEATHER_HORIZON_DAYS", "3")),
                discovery_limit=int(os.getenv("V3_PAPER_WEATHER_DISCOVERY_LIMIT", "1500")),
                market_limit=int(os.getenv("V3_PAPER_WEATHER_MARKET_LIMIT", "100")),
                min_liquidity=_decimal_env("V3_PAPER_WEATHER_MIN_LIQUIDITY", "1000"),
                min_price=_decimal_env("V3_PAPER_WEATHER_MIN_PRICE", "0.03"),
                max_price=_decimal_env("V3_PAPER_WEATHER_MAX_PRICE", "0.20"),
                max_order_notional=_decimal_env("V3_PAPER_WEATHER_MAX_ORDER_NOTIONAL", "1"),
                max_open_positions=int(os.getenv("V3_PAPER_WEATHER_MAX_OPEN_POSITIONS", "5")),
                base_edge=_decimal_env("V3_PAPER_WEATHER_BASE_EDGE", "0.03"),
                intraclass_correlation=_decimal_env("V3_PAPER_WEATHER_ICC", "0.05"),
                prior_strength=_decimal_env("V3_PAPER_WEATHER_PRIOR_STRENGTH", "10"),
                fractional_kelly=_decimal_env("V3_PAPER_WEATHER_FRACTIONAL_KELLY", "0.05"),
                uncertainty_z=_decimal_env("V3_PAPER_WEATHER_UNCERTAINTY_Z", "1"),
            ),
        )

    @property
    def initial_cash(self) -> Decimal:
        return self.max_capital * (ONE - self.reserve_fraction)

    def safety_errors(self) -> tuple[str, ...]:
        errors: list[str] = []
        if not self.paper_trading:
            errors.append("PAPER_TRADING must be true")
        if self.live_enabled:
            errors.append("ENABLE_V3_LIVE_TRADING must be false")
        if self.account_reads_enabled:
            errors.append("ENABLE_V3_ACCOUNT_READS must be false")
        return tuple(errors)


@dataclass
class PaperState:
    started_at: str
    initial_cash: Decimal
    cash: Decimal
    cycles: int = 0
    total_candidates: int = 0
    total_paper_trades: int = 0
    realized_pnl: Decimal = ZERO
    open_positions: dict[str, dict[str, Any]] = field(default_factory=dict)
    traded_conditions: set[str] = field(default_factory=set)
    traded_strategy_keys: set[str] = field(default_factory=set)
    weather_resolved: int = 0
    weather_brier_sum: Decimal = ZERO

    @classmethod
    def new(cls, initial_cash: Decimal) -> "PaperState":
        return cls(started_at=_utc_now(), initial_cash=initial_cash, cash=initial_cash)

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> "PaperState":
        return cls(
            started_at=str(payload["started_at"]),
            initial_cash=Decimal(str(payload["initial_cash"])),
            cash=Decimal(str(payload["cash"])),
            cycles=int(payload.get("cycles", 0)),
            total_candidates=int(payload.get("total_candidates", 0)),
            total_paper_trades=int(payload.get("total_paper_trades", 0)),
            realized_pnl=Decimal(str(payload.get("realized_pnl", "0"))),
            open_positions={str(key): dict(value) for key, value in payload.get("open_positions", {}).items()},
            traded_conditions={str(value) for value in payload.get("traded_conditions", [])},
            traded_strategy_keys={
                str(value)
                for value in payload.get("traded_strategy_keys", payload.get("traded_conditions", []))
            },
            weather_resolved=int(payload.get("weather_resolved", 0)),
            weather_brier_sum=Decimal(str(payload.get("weather_brier_sum", "0"))),
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at,
            "initial_cash": str(self.initial_cash),
            "cash": str(self.cash),
            "cycles": self.cycles,
            "total_candidates": self.total_candidates,
            "total_paper_trades": self.total_paper_trades,
            "realized_pnl": str(self.realized_pnl),
            "open_positions": self.open_positions,
            "traded_conditions": sorted(self.traded_conditions),
            "traded_strategy_keys": sorted(self.traded_strategy_keys),
            "weather_resolved": self.weather_resolved,
            "weather_brier_sum": str(self.weather_brier_sum),
        }


class PaperStore:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)
        self.state_path = self.data_dir / "state.json"
        self.status_path = self.data_dir / "status.json"
        self.scans_path = self.data_dir / "scans.jsonl"
        self.weather_scans_path = self.data_dir / "weather_scans.jsonl"
        self.candidates_path = self.data_dir / "candidates.jsonl"
        self.trades_path = self.data_dir / "paper_trades.jsonl"
        self.settlements_path = self.data_dir / "settlements.jsonl"
        self.pid_path = self.data_dir / "worker.pid"
        self.data_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _json_default(value: Any) -> Any:
        if isinstance(value, Decimal):
            return str(value)
        raise TypeError(f"unsupported paper JSON value: {type(value)!r}")

    def _write_json(self, path: Path, payload: Mapping[str, Any]) -> None:
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, sort_keys=True, indent=2, default=self._json_default) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)

    def append_record(self, path: Path, payload: Mapping[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True, default=self._json_default) + "\n")

    def read_records(self, path: Path) -> tuple[dict[str, Any], ...]:
        if not path.is_file():
            return ()
        rows: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError(f"paper record must be an object: {path}")
                rows.append(value)
        return tuple(rows)

    def load_state(self, initial_cash: Decimal = ZERO) -> PaperState:
        if not self.state_path.is_file():
            return PaperState.new(initial_cash)
        payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("paper state must be an object")
        return PaperState.from_json(payload)

    def save_state(self, state: PaperState) -> None:
        self._write_json(self.state_path, state.to_json())

    def write_status(self, payload: Mapping[str, Any]) -> None:
        self._write_json(self.status_path, payload)

    def read_status(self) -> dict[str, Any]:
        if not self.status_path.is_file():
            return {}
        payload = json.loads(self.status_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("paper status must be an object")
        return payload

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except (OSError, ProcessLookupError):
            return False
        return True

    def current_pid(self) -> int | None:
        if not self.pid_path.is_file():
            return None
        try:
            return int(self.pid_path.read_text(encoding="utf-8").strip())
        except ValueError:
            return None

    def acquire(self) -> None:
        while True:
            try:
                descriptor = os.open(
                    self.pid_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
            except FileExistsError:
                existing = self.current_pid()
                if existing is not None and self._pid_alive(existing):
                    raise RuntimeError(f"paper worker already running with pid {existing}")
                self.pid_path.unlink(missing_ok=True)
                continue
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(f"{os.getpid()}\n")
            return

    def release(self) -> None:
        if self.current_pid() == os.getpid():
            self.pid_path.unlink(missing_ok=True)


@dataclass(frozen=True)
class CycleSummary:
    cycle: int
    markets_discovered: int
    markets_scanned: int
    weather_markets_scanned: int
    candidates: int
    weather_candidates: int
    paper_trades: int
    settlements: int
    errors: int
    cash: Decimal
    open_positions: int


class PaperPublicClient(Protocol):
    async def get_tag(self, *, slug: str) -> Any: ...

    def list_markets(self, **kwargs: Any) -> Any: ...

    async def get_order_books(self, *, token_ids: list[str]) -> tuple[Any, ...]: ...

    async def get_market(self, *, id: str) -> Any: ...


class PaperWorker:
    def __init__(
        self,
        *,
        client: PaperPublicClient,
        settings: PaperSettings,
        store: PaperStore,
        forecast: ForecastProvider | None = None,
    ) -> None:
        errors = settings.safety_errors()
        if errors:
            raise RuntimeError("Paper worker refused: " + "; ".join(errors))
        self.client = client
        self.settings = settings
        self.store = store
        self.state = store.load_state(settings.initial_cash)
        self.forecast = forecast
        if self.forecast is None and settings.weather_policy.enabled:
            self.forecast = OpenMeteoEnsemble()

    async def _discover_markets(self) -> tuple[Any, ...]:
        markets: list[Any] = []
        examined = 0
        paginator = self.client.list_markets(
            closed=False,
            liquidity_num_min=float(self.settings.min_liquidity),
            order="liquidityNum",
            ascending=False,
            page_size=100,
        )
        async for market in paginator.iter_items():
            examined += 1
            state = market.state
            yes_token = getattr(market.outcomes.yes, "token_id", None)
            no_token = getattr(market.outcomes.no, "token_id", None)
            liquidity = Decimal(str(getattr(market.metrics, "liquidity_num", 0) or 0))
            if (
                bool(getattr(state, "active", False))
                and not bool(getattr(state, "closed", False))
                and not bool(getattr(state, "neg_risk", False))
                and bool(getattr(state, "accepting_orders", False))
                and bool(getattr(market, "question", None))
                and bool(getattr(market.resolution, "source", None))
                and yes_token
                and no_token
                and liquidity >= self.settings.min_liquidity
            ):
                markets.append(market)
            if len(markets) >= self.settings.market_limit:
                break
            if examined >= self.settings.discovery_max_markets:
                break
        return tuple(markets)

    @staticmethod
    def _best_price(levels: Any, *, best: str) -> Decimal | None:
        prices = tuple(Decimal(str(level.price)) for level in levels)
        if not prices:
            return None
        return min(prices) if best == "ask" else max(prices)

    @staticmethod
    def _opportunity_payload(decision: CompleteSetDecision) -> dict[str, Any] | None:
        opportunity = decision.opportunity
        if opportunity is None:
            return None
        return {
            "shares": str(opportunity.shares),
            "yes_vwap": str(opportunity.yes.vwap),
            "yes_notional": str(opportunity.yes.notional),
            "no_vwap": str(opportunity.no.vwap),
            "no_notional": str(opportunity.no.notional),
            "gross_cost": str(opportunity.gross_cost),
            "fees": str(opportunity.fees),
            "payout": str(opportunity.payout),
            "net_profit": str(opportunity.net_profit),
            "return_on_cost": str(opportunity.return_on_cost),
        }

    async def _scan_market(self, market: Any, scanned_at: str) -> tuple[dict[str, Any], bool, bool]:
        condition_id = str(market.condition_id or "")
        market_id = str(market.id)
        yes_token = str(market.outcomes.yes.token_id)
        no_token = str(market.outcomes.no.token_id)
        liquidity = Decimal(str(getattr(market.metrics, "liquidity_num", 0) or 0))
        row: dict[str, Any] = {
            "scanned_at": scanned_at,
            "market_id": market_id,
            "condition_id": condition_id,
            "slug": getattr(market, "slug", None),
            "question": getattr(market, "question", None),
            "liquidity": str(liquidity),
            "yes_token_id": yes_token,
            "no_token_id": no_token,
            "strategy": "complete_set",
            "public_data_only": True,
        }

        books = await self.client.get_order_books(token_ids=[yes_token, no_token])
        by_token = {str(book.token_id): book for book in books}
        if yes_token not in by_token or no_token not in by_token:
            raise ValueError("public book response omitted an outcome token")
        yes_book = by_token[yes_token]
        no_book = by_token[no_token]
        context = MarketContext.from_sdk(market, yes_book)
        row.update({
            "yes_best_bid": self._best_price(yes_book.bids, best="bid"),
            "yes_best_ask": self._best_price(yes_book.asks, best="ask"),
            "no_best_bid": self._best_price(no_book.bids, best="bid"),
            "no_best_ask": self._best_price(no_book.asks, best="ask"),
            "tick_size": str(context.tick_size),
            "min_order_size": str(max(yes_book.min_order_size, no_book.min_order_size)),
            "fee_rate": None if context.fee_rate is None else str(context.fee_rate),
            "rules_verified": context.rules_verified,
            "accepting_orders": context.accepting_orders,
            "negative_risk": context.negative_risk,
        })

        if context.negative_risk:
            row.update({"tradeable": False, "reason": "negative-risk market excluded"})
            return row, False, False
        if not context.rules_verified:
            row.update({"tradeable": False, "reason": "resolution rules are not verified"})
            return row, False, False
        if not context.accepting_orders or context.fee_rate is None:
            row.update({"tradeable": False, "reason": "market context is not safely tradeable"})
            return row, False, False

        shares = max(Decimal(str(yes_book.min_order_size)), Decimal(str(no_book.min_order_size)))
        decision = evaluate_complete_set(
            yes_book=yes_book,
            no_book=no_book,
            shares=shares,
            fee_rate=context.fee_rate,
            min_net_return=self.settings.min_net_return,
        )
        row.update({
            "tradeable": decision.tradeable,
            "reason": decision.reason,
            "opportunity": self._opportunity_payload(decision),
        })
        if not decision.tradeable or decision.opportunity is None:
            return row, False, False

        opportunity = decision.opportunity
        candidate = dict(row)
        candidate["candidate_id"] = f"complete-set:{condition_id}:{scanned_at}"
        paper_reason = "paper candidate"
        paper_executed = False
        if condition_id in self.state.traded_conditions:
            paper_reason = "condition already paper traded"
        elif len(self.state.open_positions) >= self.settings.max_open_positions:
            paper_reason = "paper open-position cap reached"
        elif (
            opportunity.yes.notional > self.settings.max_order_notional
            or opportunity.no.notional > self.settings.max_order_notional
        ):
            paper_reason = "paper order cap exceeded"
        else:
            all_in_cost = opportunity.gross_cost + opportunity.fees
            if all_in_cost > self.state.cash:
                paper_reason = "insufficient paper cash"
            else:
                paper_executed = True
                self.state.cash -= all_in_cost
                self.state.traded_conditions.add(condition_id)
                self.state.traded_strategy_keys.add(condition_id)
                self.state.open_positions[condition_id] = {
                    "strategy": "complete_set",
                    "event_key": condition_id,
                    "opened_at": scanned_at,
                    "market_id": market_id,
                    "condition_id": condition_id,
                    "question": getattr(market, "question", None),
                    "shares": str(opportunity.shares),
                    "all_in_cost": str(all_in_cost),
                    "expected_payout": str(opportunity.payout),
                    "expected_profit": str(opportunity.net_profit),
                    "yes_token_id": yes_token,
                    "no_token_id": no_token,
                }
                self.state.total_paper_trades += 1
                # Persist the position before its audit record so a crash can
                # never cause the same condition to be paper-traded twice.
                self.store.save_state(self.state)
                trade = dict(candidate)
                trade.update({
                    "paper_executed": True,
                    "paper_reason": paper_reason,
                    "paper_cash_after": str(self.state.cash),
                })
                self.store.append_record(self.store.trades_path, trade)
        candidate.update({
            "paper_executed": paper_executed,
            "paper_reason": paper_reason,
            "paper_cash_after": str(self.state.cash),
        })
        self.store.append_record(self.store.candidates_path, candidate)
        row.update({"paper_executed": paper_executed, "paper_reason": paper_reason})
        return row, True, paper_executed

    @staticmethod
    def _weather_row(evaluation: WeatherEvaluation, scanned_at: str) -> dict[str, Any]:
        decision = evaluation.decision
        forecast = evaluation.forecast
        return {
            "scanned_at": scanned_at,
            "strategy": evaluation.strategy,
            "event_key": evaluation.event_key,
            "market_id": evaluation.market_id,
            "condition_id": evaluation.condition_id,
            "question": evaluation.question,
            "city": evaluation.city,
            "target_date": evaluation.target_date,
            "target_c": str(evaluation.target_c),
            "side": evaluation.side,
            "token_id": evaluation.token_id,
            "best_bid": str(evaluation.bid),
            "best_ask": str(evaluation.ask),
            "shares": str(evaluation.shares),
            "fee": str(evaluation.fee),
            "all_in_cost": str(evaluation.all_in_cost),
            "raw_probability": str(evaluation.raw_probability),
            "calibrated_probability": str(decision.calibrated_probability),
            "ensemble_mean_c": str(forecast.ensemble_mean_c),
            "ensemble_std_c": str(forecast.ensemble_std_c),
            "n_members": forecast.n_members,
            "model_count": forecast.model_count,
            "effective_sample_size": str(decision.effective_sample_size),
            "lead_days": forecast.lead_days,
            "net_edge": str(decision.net_edge),
            "minimum_edge": str(decision.minimum_edge),
            "kelly_fraction": str(decision.kelly_fraction),
            "tradeable": evaluation.paper_tradeable,
            "reason": evaluation.paper_reason,
            "negative_risk_directional_only": True,
            "public_data_only": True,
        }

    async def _scan_weather(
        self,
        *,
        now: datetime,
        scanned_at: str,
    ) -> tuple[int, int, int, int]:
        if not self.settings.weather_policy.enabled:
            return 0, 0, 0, 0
        if self.forecast is None:
            raise RuntimeError("weather forecast provider is not initialized")
        try:
            result = await evaluate_weather_universe(
                client=self.client,
                forecast=self.forecast,
                policy=self.settings.weather_policy,
                now=now,
            )
        except Exception as exc:
            self.store.append_record(self.store.weather_scans_path, {
                "scanned_at": scanned_at,
                "status": "weather_discovery_error",
                "error": f"{type(exc).__name__}: {exc}",
                "public_data_only": True,
            })
            return 0, 0, 0, 1

        for error in result.errors:
            self.store.append_record(self.store.weather_scans_path, {
                "scanned_at": scanned_at,
                "status": "weather_market_error",
                "error": error,
                "public_data_only": True,
            })

        selected: dict[str, WeatherEvaluation] = {}
        for evaluation in result.evaluations:
            if not evaluation.paper_tradeable:
                continue
            previous = selected.get(evaluation.event_key)
            if previous is None or evaluation.decision.net_edge > previous.decision.net_edge:
                selected[evaluation.event_key] = evaluation

        ranked_selected = sorted(
            selected.values(),
            key=lambda item: item.decision.net_edge,
            reverse=True,
        )
        selected_rank = {
            id(evaluation): rank
            for rank, evaluation in enumerate(ranked_selected)
        }
        ordered_evaluations = sorted(
            result.evaluations,
            key=lambda item: (
                0 if id(item) in selected_rank else 1,
                selected_rank.get(id(item), 0),
            ),
        )
        paper_trades = 0
        weather_open = sum(
            1
            for position in self.state.open_positions.values()
            if position.get("strategy") == "weather_directional"
        )
        for evaluation in ordered_evaluations:
            row = self._weather_row(evaluation, scanned_at)
            chosen = selected.get(evaluation.event_key) is evaluation
            if not chosen:
                if evaluation.paper_tradeable:
                    row.update({
                        "tradeable": False,
                        "reason": "correlated weather event candidate not selected",
                    })
                self.store.append_record(self.store.weather_scans_path, row)
                continue

            candidate = dict(row)
            candidate["candidate_id"] = (
                f"weather:{evaluation.condition_id}:{evaluation.side}:{scanned_at}"
            )
            paper_reason = "weather paper candidate"
            paper_executed = False
            if evaluation.event_key in self.state.traded_strategy_keys:
                paper_reason = "weather event already paper traded"
            elif weather_open >= self.settings.weather_policy.max_open_positions:
                paper_reason = "weather paper position cap reached"
            elif len(self.state.open_positions) >= self.settings.max_open_positions:
                paper_reason = "paper open-position cap reached"
            elif evaluation.all_in_cost > self.state.cash:
                paper_reason = "insufficient paper cash"
            else:
                paper_executed = True
                paper_trades += 1
                weather_open += 1
                self.state.cash -= evaluation.all_in_cost
                self.state.traded_conditions.add(evaluation.condition_id)
                self.state.traded_strategy_keys.add(evaluation.event_key)
                self.state.open_positions[evaluation.condition_id] = {
                    "strategy": evaluation.strategy,
                    "event_key": evaluation.event_key,
                    "opened_at": scanned_at,
                    "market_id": evaluation.market_id,
                    "condition_id": evaluation.condition_id,
                    "question": evaluation.question,
                    "side": evaluation.side,
                    "token_id": evaluation.token_id,
                    "shares": str(evaluation.shares),
                    "entry_price": str(evaluation.ask),
                    "all_in_cost": str(evaluation.all_in_cost),
                    "model_probability": str(evaluation.decision.calibrated_probability),
                    "raw_probability": str(evaluation.raw_probability),
                    "city": evaluation.city,
                    "target_date": evaluation.target_date,
                }
                self.state.total_paper_trades += 1
                self.store.save_state(self.state)
                trade = dict(candidate)
                trade.update({
                    "paper_executed": True,
                    "paper_reason": paper_reason,
                    "paper_cash_after": str(self.state.cash),
                })
                self.store.append_record(self.store.trades_path, trade)
            candidate.update({
                "paper_executed": paper_executed,
                "paper_reason": paper_reason,
                "paper_cash_after": str(self.state.cash),
            })
            self.store.append_record(self.store.candidates_path, candidate)
            row.update({
                "paper_executed": paper_executed,
                "paper_reason": paper_reason,
            })
            self.store.append_record(self.store.weather_scans_path, row)
        return (
            result.markets_evaluated,
            len(selected),
            paper_trades,
            len(result.errors),
        )

    async def _settle_positions(self, settled_at: str) -> tuple[int, int]:
        settled = 0
        errors = 0
        for condition_id, position in tuple(self.state.open_positions.items()):
            try:
                market = await self.client.get_market(id=str(position["market_id"]))
                if not bool(getattr(market.state, "closed", False)):
                    continue
                yes_price = getattr(market.outcomes.yes, "price", None)
                no_price = getattr(market.outcomes.no, "price", None)
                if yes_price is None or no_price is None:
                    continue
                resolution_status = getattr(market.resolution, "uma_resolution_status", None)
                if "DISPUTED" in str(resolution_status).upper():
                    continue
                outcome_prices = {Decimal(str(yes_price)), Decimal(str(no_price))}
                if outcome_prices != {ZERO, ONE}:
                    continue
                shares = Decimal(str(position["shares"]))
                all_in_cost = Decimal(str(position["all_in_cost"]))
                strategy = str(position.get("strategy", "complete_set"))
                brier: Decimal | None = None
                outcome: int | None = None
                if strategy == "weather_directional":
                    side = str(position["side"])
                    winning_price = Decimal(str(
                        yes_price if side == "YES" else no_price
                    ))
                    outcome = int(winning_price == ONE)
                    payout = shares if outcome else ZERO
                    probability = Decimal(str(position["model_probability"]))
                    brier = (probability - Decimal(outcome)) ** 2
                    self.state.weather_resolved += 1
                    self.state.weather_brier_sum += brier
                else:
                    payout = shares
                pnl = payout - all_in_cost
                self.state.cash += payout
                self.state.realized_pnl += pnl
                del self.state.open_positions[condition_id]
                # Persist removal before the settlement record so restart
                # cannot credit the same payout twice.
                self.store.save_state(self.state)
                self.store.append_record(self.store.settlements_path, {
                    "settled_at": settled_at,
                    "condition_id": condition_id,
                    "market_id": str(position["market_id"]),
                    "strategy": strategy,
                    "payout": str(payout),
                    "realized_pnl": str(pnl),
                    "directional_outcome": outcome,
                    "brier_score": None if brier is None else str(brier),
                    "paper_cash_after": str(self.state.cash),
                    "public_data_only": True,
                })
                settled += 1
            except Exception as exc:
                errors += 1
                self.store.append_record(self.store.settlements_path, {
                    "settled_at": settled_at,
                    "condition_id": condition_id,
                    "market_id": str(position.get("market_id", "")),
                    "status": "settlement_error",
                    "error": f"{type(exc).__name__}: {exc}",
                    "public_data_only": True,
                })
        return settled, errors

    async def run_cycle(self, *, now: datetime | None = None) -> CycleSummary:
        now = now or datetime.now(timezone.utc)
        scanned_at = now.isoformat()
        errors = 0
        candidates = 0
        paper_trades = 0
        settlements, settlement_errors = await self._settle_positions(scanned_at)
        errors += settlement_errors
        try:
            markets = await self._discover_markets()
        except Exception as exc:
            markets = ()
            errors += 1
            self.store.append_record(self.store.scans_path, {
                "scanned_at": scanned_at,
                "status": "discovery_error",
                "error": f"{type(exc).__name__}: {exc}",
                "public_data_only": True,
            })

        scanned = 0
        for market in markets:
            try:
                row, is_candidate, paper_executed = await self._scan_market(market, scanned_at)
                scanned += 1
                candidates += int(is_candidate)
                paper_trades += int(paper_executed)
                self.store.append_record(self.store.scans_path, row)
            except Exception as exc:
                errors += 1
                self.store.append_record(self.store.scans_path, {
                    "scanned_at": scanned_at,
                    "market_id": str(getattr(market, "id", "")),
                    "condition_id": str(getattr(market, "condition_id", "")),
                    "question": getattr(market, "question", None),
                    "status": "market_error",
                    "error": f"{type(exc).__name__}: {exc}",
                    "public_data_only": True,
                })

        weather_scanned, weather_candidates, weather_trades, weather_errors = (
            await self._scan_weather(now=now, scanned_at=scanned_at)
        )
        candidates += weather_candidates
        paper_trades += weather_trades
        errors += weather_errors

        self.state.cycles += 1
        self.state.total_candidates += candidates
        self.store.save_state(self.state)
        summary = CycleSummary(
            cycle=self.state.cycles,
            markets_discovered=len(markets),
            markets_scanned=scanned,
            weather_markets_scanned=weather_scanned,
            candidates=candidates,
            weather_candidates=weather_candidates,
            paper_trades=paper_trades,
            settlements=settlements,
            errors=errors,
            cash=self.state.cash,
            open_positions=len(self.state.open_positions),
        )
        self.store.write_status({
            "mode": "PAPER",
            "running": bool(self.store.current_pid() and self.store._pid_alive(self.store.current_pid() or 0)),
            "healthy": errors == 0,
            "public_data_only": True,
            "authenticated_client_initialized": False,
            "account_reads_enabled": False,
            "live_trading_enabled": False,
            "weather_directional_enabled": self.settings.weather_policy.enabled,
            "weather_order_cap": str(self.settings.weather_policy.max_order_notional),
            "weather_position_cap": self.settings.weather_policy.max_open_positions,
            "sdk_version": polymarket.__version__,
            "started_at": self.state.started_at,
            "last_scan_at": scanned_at,
            "cycle": summary.cycle,
            "markets_discovered": summary.markets_discovered,
            "markets_scanned": summary.markets_scanned,
            "weather_markets_scanned": summary.weather_markets_scanned,
            "candidates_this_cycle": summary.candidates,
            "weather_candidates_this_cycle": summary.weather_candidates,
            "paper_trades_this_cycle": summary.paper_trades,
            "settlements_this_cycle": summary.settlements,
            "errors_this_cycle": summary.errors,
            "paper_cash": str(summary.cash),
            "open_positions": summary.open_positions,
            "total_candidates": self.state.total_candidates,
            "total_paper_trades": self.state.total_paper_trades,
            "realized_pnl": str(self.state.realized_pnl),
            "weather_resolved": self.state.weather_resolved,
            "weather_brier_score": (
                None
                if self.state.weather_resolved == 0
                else str(self.state.weather_brier_sum / Decimal(self.state.weather_resolved))
            ),
            "data_dir": str(self.store.data_dir),
        })
        return summary


ClientFactory = Callable[[], PaperPublicClient]


async def run_paper(
    settings: PaperSettings,
    *,
    cycles: int = 0,
    client_factory: ClientFactory = AsyncPublicClient,
) -> None:
    errors = settings.safety_errors()
    if errors:
        raise RuntimeError("Paper worker refused: " + "; ".join(errors))
    if cycles < 0:
        raise ValueError("paper cycles cannot be negative")

    store = PaperStore(settings.data_dir)
    store.acquire()
    try:
        client = client_factory()
        worker = PaperWorker(client=client, settings=settings, store=store)
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, stop_event.set)
            except (NotImplementedError, RuntimeError):
                pass

        completed = 0
        while not stop_event.is_set():
            summary = await worker.run_cycle()
            print(json.dumps({
                "cycle": summary.cycle,
                "markets_scanned": summary.markets_scanned,
                "weather_markets_scanned": summary.weather_markets_scanned,
                "candidates": summary.candidates,
                "weather_candidates": summary.weather_candidates,
                "paper_trades": summary.paper_trades,
                "errors": summary.errors,
                "paper_cash": str(summary.cash),
                "open_positions": summary.open_positions,
            }, sort_keys=True), flush=True)
            completed += 1
            if cycles and completed >= cycles:
                break
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=settings.scan_interval_seconds)
            except TimeoutError:
                pass
    except Exception as exc:
        status = store.read_status()
        status.update({
            "running": False,
            "healthy": False,
            "stopped_at": _utc_now(),
            "last_error": f"{type(exc).__name__}: {exc}",
        })
        store.write_status(status)
        raise
    finally:
        store.release()
        status = store.read_status()
        status.update({"running": False, "stopped_at": _utc_now()})
        store.write_status(status)


def paper_status(settings: PaperSettings) -> dict[str, Any]:
    store = PaperStore(settings.data_dir)
    status = store.read_status()
    pid = store.current_pid()
    running = bool(pid is not None and store._pid_alive(pid))
    if not status:
        status = {
            "mode": "PAPER",
            "healthy": False,
            "public_data_only": True,
            "authenticated_client_initialized": False,
            "account_reads_enabled": False,
            "live_trading_enabled": False,
            "weather_directional_enabled": settings.weather_policy.enabled,
            "data_dir": str(store.data_dir),
            "state": "not_started",
        }
    status["running"] = running
    status["pid"] = pid if running else None
    return status
