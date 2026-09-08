"""Strictly public-data paper worker and durable baseline state."""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import signal
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

import polymarket
from polymarket import AsyncPublicClient

from .market_context import MarketContext
from .math import BookLevel, execution_bid_vwap, execution_fee
from .paper_weather import (
    ForecastProvider,
    MetNoLocationForecast,
    NOAAStationObservations,
    NWSGridForecast,
    JMAForecast,
    OpenMeteoEnsemble,
    ObservationProvider,
    OffsetWeatherPublicClient,
    ProbabilityCalibration,
    ResilientForecastEnsemble,
    WeatherEventEvaluation,
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


def _strict_env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name, str(default)).strip().lower()
    if value not in {"1", "true", "yes", "on", "0", "false", "no", "off"}:
        raise ValueError(f"{name} must be an explicit boolean")
    return value in {"1", "true", "yes", "on"}


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
    entries_enabled: bool = True
    max_realized_loss: Decimal = ZERO
    max_drawdown_fraction: Decimal = ZERO
    scan_interval_seconds: float = 300.0
    market_limit: int = 25
    discovery_max_markets: int = 2_000
    min_liquidity: Decimal = Decimal("1000")
    min_net_return: Decimal = Decimal("0.005")
    max_capital: Decimal = Decimal("50")
    reserve_fraction: Decimal = Decimal("0.25")
    max_order_notional: Decimal = Decimal("5")
    max_open_positions: int = 10
    complete_set_enabled: bool = True
    open_meteo_max_requests_per_day: int = 24
    open_meteo_cache_seconds: float = 21_600.0
    early_exit_enabled: bool = False
    early_exit_target_return: Decimal = Decimal("0.25")
    early_exit_min_profit: Decimal = Decimal("0.10")
    hybrid_exit_enabled: bool = False
    hybrid_exit_fraction: Decimal = Decimal("0.75")
    hybrid_runner_target_return: Decimal = Decimal("0.50")
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
        if self.max_realized_loss < ZERO:
            raise ValueError("paper maximum realized loss cannot be negative")
        if not (ZERO <= self.max_drawdown_fraction < ONE):
            raise ValueError("paper maximum drawdown fraction must be in [0, 1)")
        if self.max_open_positions < 1:
            raise ValueError("paper max open positions must be positive")
        if self.open_meteo_max_requests_per_day < 1:
            raise ValueError("Open-Meteo daily request cap must be positive")
        if self.open_meteo_cache_seconds <= 0:
            raise ValueError("Open-Meteo cache duration must be positive")
        if self.early_exit_target_return < ZERO:
            raise ValueError("early-exit target return cannot be negative")
        if self.early_exit_min_profit < ZERO:
            raise ValueError("early-exit minimum profit cannot be negative")
        if self.hybrid_exit_enabled and not self.early_exit_enabled:
            raise ValueError("hybrid exits require early exits to be enabled")
        if self.hybrid_exit_enabled and not (ZERO < self.hybrid_exit_fraction < ONE):
            raise ValueError("hybrid exit fraction must be between zero and one")
        if (
            self.hybrid_exit_enabled
            and self.hybrid_runner_target_return < self.early_exit_target_return
        ):
            raise ValueError("hybrid runner target must not be below the first exit target")

        if self.weather_policy.kelly_sizing_enabled and self.safety_errors():
            raise ValueError("V7 Kelly sizing is paper-only: " + "; ".join(self.safety_errors()))

    @classmethod
    def from_env(cls, root: Path) -> "PaperSettings":
        raw_dir = Path(os.getenv("V3_PAPER_DATA_DIR", "data/v3-paper"))
        data_dir = raw_dir if raw_dir.is_absolute() else root / raw_dir
        return cls(
            data_dir=data_dir.resolve(),
            paper_trading=_env_bool("PAPER_TRADING", True),
            live_enabled=_env_bool("ENABLE_V3_LIVE_TRADING", False),
            account_reads_enabled=_env_bool("ENABLE_V3_ACCOUNT_READS", False),
            entries_enabled=_env_bool("V3_PAPER_ENTRIES_ENABLED", True),
            max_realized_loss=_decimal_env("V3_PAPER_MAX_REALIZED_LOSS", "0"),
            max_drawdown_fraction=_decimal_env(
                "V3_PAPER_MAX_DRAWDOWN_FRACTION", "0"
            ),
            scan_interval_seconds=float(os.getenv("V3_PAPER_SCAN_INTERVAL_SECONDS", "300")),
            market_limit=int(os.getenv("V3_PAPER_MARKET_LIMIT", "25")),
            discovery_max_markets=int(os.getenv("V3_PAPER_DISCOVERY_MAX_MARKETS", "2000")),
            min_liquidity=_decimal_env("V3_PAPER_MIN_LIQUIDITY", "1000"),
            min_net_return=_decimal_env("V3_PAPER_MIN_NET_RETURN", "0.005"),
            max_capital=_decimal_env("V3_MAX_CAPITAL", "50"),
            reserve_fraction=_decimal_env("V3_RESERVE_FRACTION", "0.25"),
            max_order_notional=_decimal_env("V3_PAPER_MAX_ORDER_NOTIONAL", "5"),
            max_open_positions=int(os.getenv("V3_PAPER_MAX_OPEN_POSITIONS", "10")),
            complete_set_enabled=_env_bool("V3_PAPER_COMPLETE_SET_ENABLED", True),
            open_meteo_max_requests_per_day=int(
                os.getenv("V3_PAPER_OPEN_METEO_MAX_REQUESTS_PER_DAY", "24")
            ),
            open_meteo_cache_seconds=float(
                os.getenv("V3_PAPER_OPEN_METEO_CACHE_SECONDS", "21600")
            ),
            early_exit_enabled=_env_bool("V3_PAPER_EARLY_EXIT_ENABLED", False),
            early_exit_target_return=_decimal_env(
                "V3_PAPER_EARLY_EXIT_TARGET_RETURN", "0.25"
            ),
            early_exit_min_profit=_decimal_env(
                "V3_PAPER_EARLY_EXIT_MIN_PROFIT", "0.10"
            ),
            hybrid_exit_enabled=_env_bool("V3_PAPER_HYBRID_ENABLED", False),
            hybrid_exit_fraction=_decimal_env(
                "V3_PAPER_HYBRID_EXIT_FRACTION", "0.75"
            ),
            hybrid_runner_target_return=_decimal_env(
                "V3_PAPER_HYBRID_RUNNER_TARGET_RETURN", "0.50"
            ),
            weather_policy=WeatherPaperPolicy(
                enabled=_env_bool("V3_PAPER_WEATHER_ENABLED", True),
                horizon_days=int(os.getenv("V3_PAPER_WEATHER_HORIZON_DAYS", "3")),
                discovery_limit=int(os.getenv("V3_PAPER_WEATHER_DISCOVERY_LIMIT", "1500")),
                market_limit=int(os.getenv("V3_PAPER_WEATHER_MARKET_LIMIT", "100")),
                min_liquidity=_decimal_env("V3_PAPER_WEATHER_MIN_LIQUIDITY", "1000"),
                min_price=_decimal_env("V3_PAPER_WEATHER_MIN_PRICE", "0.02"),
                max_price=_decimal_env("V3_PAPER_WEATHER_MAX_PRICE", "0.98"),
                max_order_notional=_decimal_env("V3_PAPER_WEATHER_MAX_ORDER_NOTIONAL", "5"),
                max_open_positions=int(os.getenv("V3_PAPER_WEATHER_MAX_OPEN_POSITIONS", "15")),
                base_edge=_decimal_env("V3_PAPER_WEATHER_BASE_EDGE", "0.03"),
                intraclass_correlation=_decimal_env("V3_PAPER_WEATHER_ICC", "0.05"),
                prior_strength=_decimal_env("V3_PAPER_WEATHER_PRIOR_STRENGTH", "10"),
                fractional_kelly=_decimal_env("V3_PAPER_WEATHER_FRACTIONAL_KELLY", "0.05"),
                kelly_sizing_enabled=_strict_env_bool("V3_PAPER_WEATHER_KELLY_SIZING_ENABLED", False),
                uncertainty_z=_decimal_env("V3_PAPER_WEATHER_UNCERTAINTY_Z", "1"),
                observations_enabled=_env_bool(
                    "V3_PAPER_WEATHER_OBSERVATIONS_ENABLED",
                    True,
                ),
                require_healthy_forecast=_env_bool(
                    "V3_PAPER_WEATHER_REQUIRE_HEALTHY_FORECAST",
                    False,
                ),
                minimum_provider_count=int(
                    os.getenv("V3_PAPER_WEATHER_MIN_PROVIDER_COUNT", "2")
                ),
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
    total_paper_exits: int = 0
    realized_pnl: Decimal = ZERO
    peak_entry_equity: Decimal = ZERO
    open_positions: dict[str, dict[str, Any]] = field(default_factory=dict)
    traded_conditions: set[str] = field(default_factory=set)
    traded_strategy_keys: set[str] = field(default_factory=set)
    weather_resolved: int = 0
    weather_brier_sum: Decimal = ZERO
    pending_audits: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def new(cls, initial_cash: Decimal) -> "PaperState":
        return cls(
            started_at=_utc_now(),
            initial_cash=initial_cash,
            cash=initial_cash,
            peak_entry_equity=initial_cash,
        )

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> "PaperState":
        return cls(
            started_at=str(payload["started_at"]),
            initial_cash=Decimal(str(payload["initial_cash"])),
            cash=Decimal(str(payload["cash"])),
            cycles=int(payload.get("cycles", 0)),
            total_candidates=int(payload.get("total_candidates", 0)),
            total_paper_trades=int(payload.get("total_paper_trades", 0)),
            total_paper_exits=int(payload.get("total_paper_exits", 0)),
            realized_pnl=Decimal(str(payload.get("realized_pnl", "0"))),
            peak_entry_equity=Decimal(str(payload.get(
                "peak_entry_equity",
                payload.get("initial_cash", "0"),
            ))),
            open_positions={str(key): dict(value) for key, value in payload.get("open_positions", {}).items()},
            traded_conditions={str(value) for value in payload.get("traded_conditions", [])},
            traded_strategy_keys={
                str(value)
                for value in payload.get("traded_strategy_keys", payload.get("traded_conditions", []))
            },
            weather_resolved=int(payload.get("weather_resolved", 0)),
            weather_brier_sum=Decimal(str(payload.get("weather_brier_sum", "0"))),
            pending_audits={
                str(key): dict(value)
                for key, value in payload.get("pending_audits", {}).items()
            },
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "started_at": self.started_at,
            "initial_cash": str(self.initial_cash),
            "cash": str(self.cash),
            "cycles": self.cycles,
            "total_candidates": self.total_candidates,
            "total_paper_trades": self.total_paper_trades,
            "total_paper_exits": self.total_paper_exits,
            "realized_pnl": str(self.realized_pnl),
            "peak_entry_equity": str(self.peak_entry_equity),
            "open_positions": self.open_positions,
            "traded_conditions": sorted(self.traded_conditions),
            "traded_strategy_keys": sorted(self.traded_strategy_keys),
            "weather_resolved": self.weather_resolved,
            "weather_brier_sum": str(self.weather_brier_sum),
            "pending_audits": self.pending_audits,
        }


class PaperStore:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)
        self.state_path = self.data_dir / "state.json"
        self.status_path = self.data_dir / "status.json"
        self.scans_path = self.data_dir / "scans.jsonl"
        self.weather_scans_path = self.data_dir / "weather_scans.jsonl"
        self.weather_events_path = self.data_dir / "weather_events.jsonl"
        self.candidates_path = self.data_dir / "candidates.jsonl"
        self.trades_path = self.data_dir / "paper_trades.jsonl"
        self.settlements_path = self.data_dir / "settlements.jsonl"
        self.exits_path = self.data_dir / "paper_exits.jsonl"
        self.pid_path = self.data_dir / "worker.pid"
        self._unique_ids: dict[tuple[Path, str], set[str]] = {}
        self.data_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _json_default(value: Any) -> Any:
        if isinstance(value, Decimal):
            return str(value)
        raise TypeError(f"unsupported paper JSON value: {type(value)!r}")

    def _write_json(self, path: Path, payload: Mapping[str, Any]) -> None:
        temporary = path.with_suffix(path.suffix + ".tmp")
        serialized = (
            json.dumps(payload, sort_keys=True, indent=2, default=self._json_default)
            + "\n"
        )
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def append_record(self, path: Path, payload: Mapping[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True, default=self._json_default) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        for (indexed_path, id_field), ids in self._unique_ids.items():
            if indexed_path == path and id_field in payload:
                ids.add(str(payload[id_field]))

    def append_unique_record(
        self,
        path: Path,
        payload: Mapping[str, Any],
        *,
        id_field: str,
    ) -> bool:
        record_id = str(payload[id_field])
        index_key = (path, id_field)
        existing_ids = self._unique_ids.get(index_key)
        if existing_ids is None:
            existing_ids = {
                str(row[id_field])
                for row in self.read_records(path)
                if id_field in row
            }
            self._unique_ids[index_key] = existing_ids
        if record_id in existing_ids:
            return False
        self.append_record(path, payload)
        existing_ids.add(record_id)
        return True

    def read_records(self, path: Path) -> tuple[dict[str, Any], ...]:
        if not path.is_file():
            return ()
        rows: list[dict[str, Any]] = []
        raw = path.read_bytes()
        lines = raw.splitlines(keepends=True)
        for index, raw_line in enumerate(lines):
            if not raw_line.strip():
                continue
            try:
                line = raw_line.decode("utf-8")
                value = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                final_unterminated = (
                    index == len(lines) - 1
                    and not raw_line.endswith((b"\n", b"\r"))
                )
                if not final_unterminated:
                    raise ValueError(
                        f"corrupt paper JSONL record at {path}:{index + 1}"
                    ) from exc
                # A crash can tear only the final append. Preserve that tail
                # for diagnosis, truncate it durably, then let the state outbox
                # replay the missing audit idempotently.
                quarantine = path.with_suffix(path.suffix + ".torn")
                with quarantine.open("ab") as handle:
                    handle.write(raw_line)
                    handle.write(b"\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                temporary = path.with_suffix(path.suffix + ".repair.tmp")
                with temporary.open("wb") as handle:
                    handle.write(b"".join(lines[:index]))
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, path)
                directory_fd = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
                break
            if not isinstance(value, dict):
                raise ValueError(f"paper record must be an object: {path}")
            rows.append(value)
            if index == len(lines) - 1 and not raw_line.endswith((b"\n", b"\r")):
                # The JSON object reached disk but its record delimiter did
                # not. Repair the boundary durably before a future append can
                # concatenate another object onto this valid one.
                with path.open("ab") as handle:
                    handle.write(b"\n")
                    handle.flush()
                    os.fsync(handle.fileno())
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

    def _audit_path(self, stream: str) -> Path:
        paths = {
            "paper_trades": self.trades_path,
            "settlements": self.settlements_path,
            "paper_exits": self.exits_path,
        }
        try:
            return paths[stream]
        except KeyError as exc:
            raise ValueError(f"unknown paper audit stream: {stream}") from exc

    def flush_pending_audits(self, state: PaperState) -> None:
        if not state.pending_audits:
            return
        pending = {
            audit_id: dict(record)
            for audit_id, record in state.pending_audits.items()
        }
        for audit_id in sorted(pending):
            record = pending[audit_id]
            stream = str(record.get("stream", ""))
            raw_payload = record.get("payload")
            if not isinstance(raw_payload, dict):
                raise ValueError(f"pending audit {audit_id} has no object payload")
            payload = dict(raw_payload)
            if str(payload.get("audit_id", "")) != audit_id:
                raise ValueError(f"pending audit {audit_id} has a mismatched payload id")
            self.append_unique_record(
                self._audit_path(stream),
                payload,
                id_field="audit_id",
            )

        state.pending_audits = {}
        try:
            self.save_state(state)
        except Exception:
            state.pending_audits = pending
            raise

    def commit_with_audit(
        self,
        state: PaperState,
        *,
        audit_id: str,
        stream: str,
        payload: Mapping[str, Any],
    ) -> None:
        if not audit_id:
            raise ValueError("paper audit id is required")
        audit_payload = dict(payload)
        audit_payload["audit_id"] = audit_id
        pending_record = {"stream": stream, "payload": audit_payload}
        existing = state.pending_audits.get(audit_id)
        if existing is not None and existing != pending_record:
            raise ValueError(f"paper audit id collision: {audit_id}")
        state.pending_audits[audit_id] = pending_record
        # The business mutation and its pending audit are durable together.
        self.save_state(state)
        # JSONL append is fsync'd and idempotent before the outbox is cleared.
        self.flush_pending_audits(state)

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
    weather_markets_discovered: int
    weather_forecast_unavailable: int
    weather_markets_modeled: int
    weather_side_evaluable: int
    weather_forecast_status: str
    candidates: int
    weather_candidates: int
    weather_events_observed: int
    weather_complete_partitions: int
    weather_indicative_profitable_baskets: int
    weather_observations_available: int
    weather_observation_errors: int
    paper_trades: int
    paper_exits: int
    settlements: int
    errors: int
    cash: Decimal
    open_positions: int


@dataclass(frozen=True)
class _WeatherScanSummary:
    markets_evaluated: int = 0
    markets_discovered: int = 0
    forecast_unavailable: int = 0
    markets_modeled: int = 0
    side_evaluable: int = 0
    forecast_status: str = "not_requested"
    forecast_errors: int = 0
    provider_names: tuple[str, ...] = ()
    provider_failures: tuple[tuple[str, str], ...] = ()
    candidates: int = 0
    paper_trades: int = 0
    errors: int = 0
    events_observed: int = 0
    complete_partitions: int = 0
    indicative_profitable_baskets: int = 0
    observations_available: int = 0
    observation_errors: int = 0


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
        observation_provider: ObservationProvider | None = None,
        weather_client: Any | None = None,
    ) -> None:
        errors = settings.safety_errors()
        if errors:
            raise RuntimeError("Paper worker refused: " + "; ".join(errors))
        self.client = client
        self.weather_client = weather_client or client
        self.settings = settings
        self.store = store
        self.state = store.load_state(settings.initial_cash)
        # Finish any state-first audit writes left by a prior crash before work.
        self.store.flush_pending_audits(self.state)
        self.forecast = forecast
        if self.forecast is None and settings.weather_policy.enabled:
            calibrator = ProbabilityCalibration(store.data_dir / "weather_calibration.json")
            self.forecast = ResilientForecastEnsemble(
                (
                    OpenMeteoEnsemble(
                        quota_path=store.data_dir / "open_meteo_quota.json",
                        max_requests_per_day=settings.open_meteo_max_requests_per_day,
                        cache_seconds=settings.open_meteo_cache_seconds,
                    ),
                    MetNoLocationForecast(),
                    NWSGridForecast(),
                    JMAForecast(),
                ),
                calibrator=calibrator,
            )
        self.observation_provider = observation_provider
        if (
            self.observation_provider is None
            and settings.weather_policy.enabled
            and settings.weather_policy.observations_enabled
        ):
            self.observation_provider = NOAAStationObservations()

    def _open_entry_cost(self) -> Decimal:
        return sum(
            (
                Decimal(str(position.get("all_in_cost", "0")))
                for position in self.state.open_positions.values()
            ),
            ZERO,
        )

    def _entry_equity(self) -> Decimal:
        """Return cash plus original cost of open positions."""
        return self.state.cash + self._open_entry_cost()

    def _refresh_peak_entry_equity(self) -> None:
        self.state.peak_entry_equity = max(
            self.state.peak_entry_equity,
            self._entry_equity(),
        )

    def _entry_block_reason(self, *, forecast_status: str | None = None) -> str | None:
        if not self.settings.entries_enabled:
            return "paper entries disabled by profile"
        if (
            self.settings.max_realized_loss > ZERO
            and self.state.realized_pnl <= -self.settings.max_realized_loss
        ):
            return "paper realized-loss breaker reached"
        self._refresh_peak_entry_equity()
        if (
            self.settings.max_drawdown_fraction > ZERO
            and self.state.peak_entry_equity > ZERO
        ):
            drawdown = (
                self.state.peak_entry_equity - self._entry_equity()
            ) / self.state.peak_entry_equity
            if drawdown >= self.settings.max_drawdown_fraction:
                return "paper drawdown breaker reached"
        if (
            forecast_status is not None
            and self.settings.weather_policy.require_healthy_forecast
            and forecast_status != "available"
        ):
            return "weather forecast health gate blocked entries"
        return None

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
        entry_block_reason = self._entry_block_reason()
        if entry_block_reason is not None:
            paper_reason = entry_block_reason
            candidate.update({"tradeable": False, "reason": entry_block_reason})
        elif condition_id in self.state.traded_conditions:
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
                trade = dict(candidate)
                trade.update({
                    "paper_executed": True,
                    "paper_reason": paper_reason,
                    "paper_cash_after": str(self.state.cash),
                })
                self.store.commit_with_audit(
                    self.state,
                    audit_id=str(candidate["candidate_id"]),
                    stream="paper_trades",
                    payload=trade,
                )
        candidate.update({
            "paper_executed": paper_executed,
            "paper_reason": paper_reason,
            "paper_cash_after": str(self.state.cash),
        })
        self.store.append_record(self.store.candidates_path, candidate)
        row.update({
            "paper_executed": paper_executed,
            "paper_reason": paper_reason,
            "tradeable": row["tradeable"] if entry_block_reason is None else False,
            "reason": row["reason"] if entry_block_reason is None else entry_block_reason,
        })
        return row, True, paper_executed

    @staticmethod
    def _maker_shadow_payload(quote: Any) -> dict[str, Any] | None:
        if quote is None:
            return None
        return {
            "side": quote.side,
            "price": str(quote.price),
            "size": str(quote.size),
            "queue_ahead": str(quote.queue_ahead),
            "expected_probability": str(quote.expected_probability),
            "expected_edge": str(quote.expected_edge),
            "best_bid": str(quote.best_bid),
            "best_ask": str(quote.best_ask),
            "fill_status": quote.fill_status,
            "execution_status": quote.execution_status,
            "cash_delta": str(quote.cash_delta),
            "inventory_delta": str(quote.inventory_delta),
        }

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
            "target_c": None if evaluation.target_c is None else str(evaluation.target_c),
            "unit": evaluation.unit,
            "display_lower": (
                None if evaluation.display_lower is None else str(evaluation.display_lower)
            ),
            "display_upper": (
                None if evaluation.display_upper is None else str(evaluation.display_upper)
            ),
            "contract_kind": evaluation.contract_kind,
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
            "forecast_source": forecast.source,
            "provider_count": forecast.provider_count,
            "provider_names": list(forecast.provider_names),
            "provider_weights": {
                source: str(weight)
                for source, weight in forecast.provider_weights
            },
            "continent": forecast.continent,
            "provider_probabilities": {
                source: str(probability)
                for source, probability in forecast.provider_probabilities
            },
            "provider_failures": {
                source: message
                for source, message in forecast.provider_failures
            },
            "calibration_samples": forecast.calibration_samples,
            "effective_sample_size": str(decision.effective_sample_size),
            "lead_days": forecast.lead_days,
            "net_edge": str(decision.net_edge),
            "minimum_edge": str(decision.minimum_edge),
            "kelly_fraction": str(decision.kelly_fraction),
            "sizing_mode": evaluation.sizing_mode,
            "sizing_bankroll": None if evaluation.sizing_bankroll is None else str(evaluation.sizing_bankroll),
            "sizing_budget": None if evaluation.sizing_budget is None else str(evaluation.sizing_budget),
            "venue_minimum_shares": None if evaluation.venue_minimum_shares is None else str(evaluation.venue_minimum_shares),
            "fee_rate": None if evaluation.fee_rate is None else str(evaluation.fee_rate),
            "fee_source": "gamma_market_schedule_or_disabled",
            "same_day_contract": evaluation.same_day_contract,
            "same_day_observation_available": (
                evaluation.same_day_observation_available
            ),
            "current_high_display": (
                None
                if evaluation.current_high_display is None
                else str(evaluation.current_high_display)
            ),
            "observation_error": evaluation.observation_error,
            "same_day_observation_status": evaluation.same_day_observation_status,
            "maker_shadow": PaperWorker._maker_shadow_payload(evaluation.maker_shadow),
            "tradeable": evaluation.paper_tradeable,
            "reason": evaluation.paper_reason,
            "negative_risk_directional_only": True,
            "public_data_only": True,
        }

    @staticmethod
    def _weather_event_row(
        event: WeatherEventEvaluation,
        *,
        event_id: str,
        scanned_at: str,
    ) -> dict[str, Any]:
        surface = event.surface
        contracts = []
        maker_shadows = []
        for contract in event.contracts:
            maker = PaperWorker._maker_shadow_payload(contract.maker_shadow)
            contracts.append({
                "market_id": contract.market_id,
                "condition_id": contract.condition_id,
                "question": contract.question,
                "yes_token_id": contract.yes_token_id,
                "contract_kind": contract.contract_kind,
                "display_lower": (
                    None if contract.display_lower is None else str(contract.display_lower)
                ),
                "display_upper": (
                    None if contract.display_upper is None else str(contract.display_upper)
                ),
                "model_probability": (
                    None
                    if contract.model_probability is None
                    else str(contract.model_probability)
                ),
                "maker_shadow": maker,
            })
            if maker is not None:
                maker_shadows.append(maker)
        return {
            "event_id": event_id,
            "scanned_at": scanned_at,
            "event_key": event.event_key,
            "unit": event.unit,
            "contracts": contracts,
            "model_probability_sum": str(surface.model_probability_sum),
            "model_probability_residual": str(surface.model_probability_residual),
            "bucket_count": surface.bucket_count,
            "complete_partition": surface.complete_partition,
            "partition_reason": surface.reason,
            "partition_violations": list(surface.partition_violations),
            "monotonic_violations": list(surface.monotonic_violations),
            "common_shares": str(surface.common_shares),
            "basket_executable": surface.executable,
            "basket_gross_cost": str(surface.gross_cost),
            "basket_fees": str(surface.fees),
            "basket_payout": str(surface.payout),
            "basket_net_profit": str(surface.net_profit),
            "indicative_profitable_basket": bool(
                surface.tradeable and event.event_membership_verified
            ),
            "unverified_cross_market_hypothesis": bool(
                surface.tradeable and not event.event_membership_verified
            ),
            "maker_shadows": maker_shadows,
            "negative_risk_verified": event.negative_risk_verified,
            "resolution_station_verified": event.resolution_station_verified,
            "unit_verified": event.unit_verified,
            "parsed_event_membership_verified": event.parsed_event_membership_verified,
            "event_membership_verified": event.event_membership_verified,
            "public_data_only": event.public_data_only,
            "execution_status": event.execution_status,
            "cash_delta": "0",
            "inventory_delta": "0",
        }

    async def _scan_weather(
        self,
        *,
        now: datetime,
        scanned_at: str,
    ) -> _WeatherScanSummary:
        if not self.settings.weather_policy.enabled:
            return _WeatherScanSummary()
        if self.forecast is None:
            raise RuntimeError("weather forecast provider is not initialized")
        try:
            result = await evaluate_weather_universe(
                client=self.weather_client,
                forecast=self.forecast,
                policy=(replace(
                    self.settings.weather_policy,
                    sizing_bankroll=max(ZERO, min(self.state.cash, self.settings.initial_cash)),
                    max_order_notional=min(
                        self.settings.max_order_notional,
                        self.settings.weather_policy.max_order_notional,
                    ),
                ) if self.settings.weather_policy.kelly_sizing_enabled else self.settings.weather_policy),
                observation_provider=self.observation_provider,
                now=now,
            )
        except Exception as exc:
            self.store.append_record(self.store.weather_scans_path, {
                "scanned_at": scanned_at,
                "status": "weather_discovery_error",
                "error": f"{type(exc).__name__}: {exc}",
                "public_data_only": True,
            })
            return _WeatherScanSummary(errors=1)

        forecast_errors = set(result.forecast_errors)
        for error in result.errors:
            self.store.append_record(self.store.weather_scans_path, {
                "scanned_at": scanned_at,
                "status": (
                    "weather_forecast_error"
                    if error in forecast_errors
                    else "weather_market_error"
                ),
                "error": error,
                "public_data_only": True,
            })
        for source, message in result.provider_failures:
            self.store.append_record(self.store.weather_scans_path, {
                "scanned_at": scanned_at,
                "status": "weather_provider_degraded",
                "provider": source,
                "error": message,
                "provider_fallback_active": True,
                "public_data_only": True,
            })

        prospective_cycle = self.state.cycles + 1
        for event in result.events:
            event_id = (
                f"{self.state.started_at}:{prospective_cycle}:"
                f"{event.event_key}:{event.unit}"
            )
            self.store.append_unique_record(
                self.store.weather_events_path,
                self._weather_event_row(
                    event,
                    event_id=event_id,
                    scanned_at=scanned_at,
                ),
                id_field="event_id",
            )

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
        entry_block_reason = self._entry_block_reason()
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
            if self.settings.weather_policy.kelly_sizing_enabled:
                execution_bankroll = max(
                    ZERO,
                    min(self.state.cash, self.settings.initial_cash),
                )
                execution_sizing_budget = min(
                    self.settings.max_order_notional,
                    execution_bankroll,
                    execution_bankroll * evaluation.decision.kelly_fraction,
                )
                candidate.update({
                    "execution_sizing_bankroll": str(execution_bankroll),
                    "execution_sizing_budget": str(execution_sizing_budget),
                })
            if entry_block_reason is not None:
                paper_reason = entry_block_reason
                candidate.update({"tradeable": False, "reason": entry_block_reason})
            elif evaluation.event_key in self.state.traded_strategy_keys:
                paper_reason = "weather event already paper traded"
            elif weather_open >= self.settings.weather_policy.max_open_positions:
                paper_reason = "weather paper position cap reached"
            elif len(self.state.open_positions) >= self.settings.max_open_positions:
                paper_reason = "paper open-position cap reached"
            elif (
                self.settings.weather_policy.kelly_sizing_enabled
                and evaluation.all_in_cost > min(
                    self.settings.max_order_notional,
                    max(ZERO, min(self.state.cash, self.settings.initial_cash))
                    * evaluation.decision.kelly_fraction,
                )
            ):
                paper_reason = "kelly budget reduced since scan; skip without upsizing"
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
                    "provider_probabilities": {
                        source: str(probability)
                        for source, probability in evaluation.forecast.provider_probabilities
                    },
                    "lead_days": evaluation.forecast.lead_days,
                    "city": evaluation.city,
                    "target_date": evaluation.target_date,
                }
                self.state.total_paper_trades += 1
                trade = dict(candidate)
                trade.update({
                    "paper_executed": True,
                    "paper_reason": paper_reason,
                    "paper_cash_after": str(self.state.cash),
                })
                self.store.commit_with_audit(
                    self.state,
                    audit_id=str(candidate["candidate_id"]),
                    stream="paper_trades",
                    payload=trade,
                )
            candidate.update({
                "paper_executed": paper_executed,
                "paper_reason": paper_reason,
                "paper_cash_after": str(self.state.cash),
            })
            self.store.append_record(self.store.candidates_path, candidate)
            row.update({
                "paper_executed": paper_executed,
                "paper_reason": paper_reason,
                "tradeable": row["tradeable"] if entry_block_reason is None else False,
                "reason": row["reason"] if entry_block_reason is None else entry_block_reason,
            })
            self.store.append_record(self.store.weather_scans_path, row)
        return _WeatherScanSummary(
            markets_evaluated=result.markets_evaluated,
            markets_discovered=result.markets_discovered,
            forecast_unavailable=result.markets_forecast_unavailable,
            markets_modeled=result.markets_modeled,
            side_evaluable=result.markets_side_evaluable,
            forecast_status=result.forecast_status,
            forecast_errors=len(result.forecast_errors),
            provider_names=result.provider_names,
            provider_failures=result.provider_failures,
            candidates=len(selected),
            paper_trades=paper_trades,
            errors=len(result.errors),
            events_observed=len(result.events),
            complete_partitions=sum(
                event.surface.complete_partition for event in result.events
            ),
            indicative_profitable_baskets=sum(
                event.surface.tradeable and event.event_membership_verified
                for event in result.events
            ),
            observations_available=result.observations_available,
            observation_errors=result.observation_errors,
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
                    calibration_outcome = int(Decimal(str(yes_price)) == ONE)
                    payout = shares if outcome else ZERO
                    probability = Decimal(str(position["model_probability"]))
                    brier = (probability - Decimal(outcome)) ** 2
                    self.state.weather_resolved += 1
                    self.state.weather_brier_sum += brier
                    record_outcome = getattr(self.forecast, "record_outcome", None)
                    if callable(record_outcome):
                        provider_probabilities = tuple(
                            (
                                str(source),
                                Decimal(str(probability)),
                            )
                            for source, probability in dict(
                                position.get("provider_probabilities", {})
                            ).items()
                        )
                        if provider_probabilities:
                            record_outcome(
                                city=str(position.get("city", "")),
                                lead_days=int(position.get("lead_days", 0)),
                                outcome=calibration_outcome,
                                provider_probabilities=provider_probabilities,
                            )
                else:
                    payout = shares
                pnl = payout - all_in_cost
                self.state.cash += payout
                self.state.realized_pnl += pnl
                del self.state.open_positions[condition_id]
                settlement = {
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
                }
                self.store.commit_with_audit(
                    self.state,
                    audit_id=f"settlement:{condition_id}",
                    stream="settlements",
                    payload=settlement,
                )
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

    async def _exit_positions(self, exited_at: str) -> tuple[int, int]:
        """Paper-sell directional positions at executable bids.

        Hybrid campaigns take a configurable partial exit at the first target,
        then leave the residual position as a runner. The residual can be sold
        at the higher runner target or settle normally. This remains a paper
        simulation because partial quantities may be below venue minimums.
        """
        if not self.settings.early_exit_enabled:
            return 0, 0

        positions = tuple(
            (condition_id, position)
            for condition_id, position in self.state.open_positions.items()
            if position.get("strategy") == "weather_directional"
        )
        if not positions:
            return 0, 0

        token_ids = [str(position["token_id"]) for _, position in positions]
        try:
            books = await self.client.get_order_books(token_ids=token_ids)
            books_by_token = {str(book.token_id): book for book in books}
        except Exception as exc:
            self.store.append_record(self.store.weather_scans_path, {
                "scanned_at": exited_at,
                "status": "paper_exit_error",
                "error": f"{type(exc).__name__}: {exc}",
                "public_data_only": True,
            })
            return 0, 1

        exited = 0
        errors = 0
        for condition_id, position in positions:
            try:
                token_id = str(position["token_id"])
                book = books_by_token.get(token_id)
                if book is None:
                    continue
                market = await self.client.get_market(id=str(position["market_id"]))
                if bool(getattr(market.state, "closed", False)):
                    continue
                context = MarketContext.from_sdk(market, book)
                if (
                    not context.rules_verified
                    or not context.accepting_orders
                    or context.fee_rate is None
                ):
                    continue
                shares = Decimal(str(position["shares"]))
                levels = tuple(
                    BookLevel(
                        price=Decimal(str(level.price)),
                        size=Decimal(str(level.size)),
                    )
                    for level in book.bids
                )
                hybrid = self.settings.hybrid_exit_enabled
                partial_exit = hybrid and not bool(position.get("hybrid_exit_done", False))
                exit_shares = (
                    shares * self.settings.hybrid_exit_fraction
                    if partial_exit
                    else shares
                )
                if exit_shares <= ZERO or exit_shares > shares:
                    continue
                quote = execution_bid_vwap(levels, exit_shares)
                fees = execution_fee(
                    levels, exit_shares, context.fee_rate, descending=True
                )
                net_proceeds = quote.notional - fees
                position_cost = Decimal(str(position["all_in_cost"]))
                entry_cost = (
                    position_cost * self.settings.hybrid_exit_fraction
                    if partial_exit
                    else position_cost
                )
                profit = net_proceeds - entry_cost
                return_on_cost = profit / entry_cost if entry_cost > ZERO else ZERO
                target_return = (
                    self.settings.early_exit_target_return
                    if not hybrid or partial_exit
                    else self.settings.hybrid_runner_target_return
                )
                if (
                    profit < self.settings.early_exit_min_profit
                    or return_on_cost < target_return
                ):
                    continue

                self.state.cash += net_proceeds
                self.state.realized_pnl += profit
                self.state.total_paper_exits += 1
                if partial_exit:
                    position["shares"] = str(shares - exit_shares)
                    position["all_in_cost"] = str(position_cost - entry_cost)
                    position["hybrid_exit_done"] = True
                else:
                    del self.state.open_positions[condition_id]
                exit_id = (
                    f"paper-exit:{condition_id}:"
                    f"{position.get('opened_at', '')}:"
                    f"{'partial' if partial_exit else 'runner' if hybrid else 'full'}"
                )
                self.store.commit_with_audit(
                    self.state,
                    audit_id=exit_id,
                    stream="paper_exits",
                    payload={
                        "exited_at": exited_at,
                        "condition_id": condition_id,
                        "market_id": str(position["market_id"]),
                        "strategy": str(position.get("strategy", "")),
                        "side": str(position.get("side", "")),
                        "token_id": token_id,
                        "shares": str(exit_shares),
                        "entry_cost": str(entry_cost),
                        "exit_vwap": str(quote.vwap),
                        "exit_notional": str(quote.notional),
                        "exit_fee": str(fees),
                        "net_proceeds": str(net_proceeds),
                        "realized_pnl": str(profit),
                        "return_on_cost": str(return_on_cost),
                        "target_return": str(target_return),
                        "minimum_profit": str(self.settings.early_exit_min_profit),
                        "paper_cash_after": str(self.state.cash),
                        "hybrid": hybrid,
                        "hybrid_exit_fraction": (
                            str(self.settings.hybrid_exit_fraction) if hybrid else None
                        ),
                        "runner_target_return": (
                            str(self.settings.hybrid_runner_target_return) if hybrid else None
                        ),
                        "remaining_shares": (
                            str(shares - exit_shares) if partial_exit else "0"
                        ),
                        "remaining_entry_cost": (
                            str(position_cost - entry_cost) if partial_exit else "0"
                        ),
                        "reason": (
                            "paper hybrid partial-exit profit target"
                            if partial_exit
                            else "paper hybrid runner profit target"
                            if hybrid
                            else "paper early-exit profit target"
                        ),
                        "public_data_only": True,
                    },
                )
                exited += 1
            except ValueError:
                # An empty or shallow bid book is not an executable exit.
                continue
            except Exception as exc:
                errors += 1
                self.store.append_record(self.store.weather_scans_path, {
                    "scanned_at": exited_at,
                    "condition_id": condition_id,
                    "market_id": str(position.get("market_id", "")),
                    "status": "paper_exit_error",
                    "error": f"{type(exc).__name__}: {exc}",
                    "public_data_only": True,
                })
        return exited, errors

    async def run_cycle(self, *, now: datetime | None = None) -> CycleSummary:
        now = now or datetime.now(timezone.utc)
        scanned_at = now.isoformat()
        errors = 0
        candidates = 0
        paper_trades = 0
        settlements, settlement_errors = await self._settle_positions(scanned_at)
        errors += settlement_errors
        paper_exits, exit_errors = await self._exit_positions(scanned_at)
        errors += exit_errors
        self._refresh_peak_entry_equity()
        if self.settings.complete_set_enabled:
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
        else:
            markets = ()

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

        weather = await self._scan_weather(now=now, scanned_at=scanned_at)
        candidates += weather.candidates
        paper_trades += weather.paper_trades
        errors += weather.errors

        self.state.cycles += 1
        self.state.total_candidates += candidates
        self.store.save_state(self.state)
        summary = CycleSummary(
            cycle=self.state.cycles,
            markets_discovered=len(markets),
            markets_scanned=scanned,
            weather_markets_scanned=weather.markets_evaluated,
            weather_markets_discovered=weather.markets_discovered,
            weather_forecast_unavailable=weather.forecast_unavailable,
            weather_markets_modeled=weather.markets_modeled,
            weather_side_evaluable=weather.side_evaluable,
            weather_forecast_status=weather.forecast_status,
            candidates=candidates,
            weather_candidates=weather.candidates,
            weather_events_observed=weather.events_observed,
            weather_complete_partitions=weather.complete_partitions,
            weather_indicative_profitable_baskets=(
                weather.indicative_profitable_baskets
            ),
            weather_observations_available=weather.observations_available,
            weather_observation_errors=weather.observation_errors,
            paper_trades=paper_trades,
            paper_exits=paper_exits,
            settlements=settlements,
            errors=errors,
            cash=self.state.cash,
            open_positions=len(self.state.open_positions),
        )
        open_meteo = next(
            (
                provider
                for provider in getattr(self.forecast, "providers", ())
                if getattr(provider, "name", "") == "open-meteo"
            ),
            None,
        )
        self.store.write_status({
            "mode": "PAPER",
            "running": bool(self.store.current_pid() and self.store._pid_alive(self.store.current_pid() or 0)),
            "healthy": errors == 0,
            "public_data_only": True,
            "authenticated_client_initialized": False,
            "account_reads_enabled": False,
            "live_trading_enabled": False,
            "complete_set_enabled": self.settings.complete_set_enabled,
            "weather_directional_enabled": self.settings.weather_policy.enabled,
            "paper_entries_enabled": self.settings.entries_enabled,
            "paper_max_realized_loss": str(self.settings.max_realized_loss),
            "paper_max_drawdown_fraction": str(self.settings.max_drawdown_fraction),
            "paper_peak_entry_equity": str(self.state.peak_entry_equity),
            "paper_entry_block_reason": self._entry_block_reason(),
            "paper_weather_require_healthy_forecast": (
                self.settings.weather_policy.require_healthy_forecast
            ),
            "paper_weather_min_provider_count": (
                self.settings.weather_policy.minimum_provider_count
            ),
            "weather_kelly_sizing_enabled": self.settings.weather_policy.kelly_sizing_enabled,
            "weather_fractional_kelly": str(self.settings.weather_policy.fractional_kelly),
            "weather_sizing_bankroll_basis": "min(current_cash,initial_trading_cash)",
            "weather_order_cap": str(self.settings.weather_policy.max_order_notional),
            "weather_position_cap": self.settings.weather_policy.max_open_positions,
            "open_meteo_request_cap": self.settings.open_meteo_max_requests_per_day,
            "open_meteo_cache_seconds": self.settings.open_meteo_cache_seconds,
            "open_meteo_requests_last_24h": (
                None if open_meteo is None else open_meteo.requests_last_24h
            ),
            "sdk_version": polymarket.__version__,
            "started_at": self.state.started_at,
            "last_scan_at": scanned_at,
            "cycle": summary.cycle,
            "markets_discovered": summary.markets_discovered,
            "markets_scanned": summary.markets_scanned,
            "weather_markets_scanned": summary.weather_markets_scanned,
            "weather_markets_discovered": summary.weather_markets_discovered,
            "weather_forecast_unavailable_this_cycle": (
                summary.weather_forecast_unavailable
            ),
            "weather_markets_modeled_this_cycle": summary.weather_markets_modeled,
            "weather_side_evaluable_this_cycle": summary.weather_side_evaluable,
            "weather_forecast_status": summary.weather_forecast_status,
            "weather_forecast_errors_this_cycle": weather.forecast_errors,
            "weather_provider_names_this_cycle": list(weather.provider_names),
            "weather_provider_failures_this_cycle": {
                source: message
                for source, message in weather.provider_failures
            },
            "candidates_this_cycle": summary.candidates,
            "weather_candidates_this_cycle": summary.weather_candidates,
            "weather_events_observed_this_cycle": summary.weather_events_observed,
            "weather_complete_partitions_this_cycle": (
                summary.weather_complete_partitions
            ),
            "weather_indicative_profitable_baskets_this_cycle": (
                summary.weather_indicative_profitable_baskets
            ),
            "weather_observations_available_this_cycle": (
                summary.weather_observations_available
            ),
            "weather_observation_errors_this_cycle": (
                summary.weather_observation_errors
            ),
            "paper_trades_this_cycle": summary.paper_trades,
            "paper_exits_enabled": self.settings.early_exit_enabled,
            "paper_exit_target_return": str(self.settings.early_exit_target_return),
            "paper_exit_minimum_profit": str(self.settings.early_exit_min_profit),
            "paper_hybrid_enabled": self.settings.hybrid_exit_enabled,
            "paper_hybrid_exit_fraction": str(self.settings.hybrid_exit_fraction),
            "paper_hybrid_runner_target_return": str(
                self.settings.hybrid_runner_target_return
            ),
            "paper_exits_this_cycle": summary.paper_exits,
            "settlements_this_cycle": summary.settlements,
            "errors_this_cycle": summary.errors,
            "paper_cash": str(summary.cash),
            "open_positions": summary.open_positions,
            "total_candidates": self.state.total_candidates,
            "total_paper_trades": self.state.total_paper_trades,
            "total_paper_exits": self.state.total_paper_exits,
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
    client: PaperPublicClient | None = None
    try:
        client = client_factory()
        worker = PaperWorker(
            client=client,
            settings=settings,
            store=store,
            weather_client=OffsetWeatherPublicClient(client),
        )
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
                "paper_exits": summary.paper_exits,
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
        active_exception = sys.exc_info()[0] is not None
        close_error: Exception | None = None
        try:
            if client is not None:
                close = getattr(client, "close", None)
                if callable(close):
                    result = close()
                    if inspect.isawaitable(result):
                        await result
        except Exception as exc:
            close_error = exc
        finally:
            try:
                status = store.read_status()
                status.update({"running": False, "stopped_at": _utc_now()})
                if close_error is not None:
                    status.update({
                        "healthy": False,
                        "last_error": (
                            f"public client cleanup failed: "
                            f"{type(close_error).__name__}: {close_error}"
                        ),
                    })
                store.write_status(status)
            finally:
                # Publish final status while still owning the PID lock so an
                # exiting worker cannot overwrite a replacement worker.
                store.release()
        if close_error is not None and not active_exception:
            raise close_error


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
            "weather_kelly_sizing_enabled": settings.weather_policy.kelly_sizing_enabled,
            "weather_fractional_kelly": str(settings.weather_policy.fractional_kelly),
            "weather_sizing_bankroll_basis": "min(current_cash,initial_trading_cash)",
            "data_dir": str(store.data_dir),
            "state": "not_started",
        }
    status["running"] = running
    status["pid"] = pid if running else None
    return status
