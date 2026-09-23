"""Public-only, same-entry mirror campaigns for V7 exit-policy comparisons.

The mirror consumes entries from an already-running V7 paper ledger. It never
scans weather markets or constructs forecast/observation providers. Each policy
arm keeps its own rebased ledger while replaying the exact same source entries.
"""

from __future__ import annotations

import asyncio
import copy
import json
import os
import signal
import sys
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from polymarket import AsyncPublicClient

from .paper import PaperSettings, PaperState, PaperStore, PaperWorker

ZERO = Decimal("0")


@dataclass(frozen=True)
class MirrorPolicy:
    name: str
    early_exit_enabled: bool
    target_return: Decimal
    hybrid_enabled: bool = False
    hybrid_fraction: Decimal = Decimal("0.75")
    runner_target_return: Decimal = Decimal("0.50")


MIRROR_POLICIES: dict[str, MirrorPolicy] = {
    "hold": MirrorPolicy("hold", False, Decimal("0.25")),
    "full-25": MirrorPolicy("full-25", True, Decimal("0.25")),
    "full-15": MirrorPolicy("full-15", True, Decimal("0.15")),
    "full-10": MirrorPolicy("full-10", True, Decimal("0.10")),
    "hybrid-25": MirrorPolicy(
        "hybrid-25", True, Decimal("0.25"), hybrid_enabled=True,
        hybrid_fraction=Decimal("0.75"), runner_target_return=Decimal("0.50"),
    ),
}


def read_source_trade_rows(path: Path, offset: int) -> tuple[tuple[dict[str, Any], ...], int]:
    """Read complete JSONL records from a byte offset without repairing source data."""
    path = Path(path)
    if offset < 0:
        raise ValueError("source offset cannot be negative")
    if not path.exists():
        if offset:
            raise ValueError("source trade ledger disappeared after the mirror cursor advanced")
        return (), 0
    raw = path.read_bytes()
    if offset > len(raw):
        raise ValueError("source trade ledger is shorter than the saved mirror cursor")
    tail = raw[offset:]
    newline = tail.rfind(b"\n")
    if newline < 0:
        return (), offset
    complete = tail[: newline + 1]
    rows: list[dict[str, Any]] = []
    for line_number, raw_line in enumerate(complete.splitlines(), start=1):
        if not raw_line.strip():
            continue
        try:
            value = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid source trade JSONL record at offset {offset}, line {line_number}") from exc
        if not isinstance(value, dict):
            raise ValueError("source trade record must be a JSON object")
        rows.append(value)
    return tuple(rows), offset + len(complete)


def normalize_source_trade(row: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    """Convert one executed public V7 trade audit into a mirror position.

    Non-executed or non-public rows are ignored. Supported entries are the
    canonical directional weather tickets and complete weather-ladder baskets.
    """
    if row.get("paper_executed") is not True or row.get("public_data_only") is not True:
        return None
    source_id = str(row.get("audit_id") or row.get("candidate_id") or "")
    if not source_id:
        raise ValueError("executed source trade has no stable audit/candidate id")
    strategy = str(row.get("strategy") or "")
    if strategy == "weather_directional":
        required = ("condition_id", "market_id", "token_id", "shares", "all_in_cost", "side")
        if any(row.get(name) in (None, "") for name in required):
            raise ValueError(f"directional source trade {source_id} is missing position fields")
        side = str(row["side"]).upper()
        if side not in {"YES", "NO"}:
            raise ValueError(f"directional source trade {source_id} has invalid side")
        shares = Decimal(str(row["shares"]))
        cost = Decimal(str(row["all_in_cost"]))
        if not shares.is_finite() or shares <= ZERO or not cost.is_finite() or cost <= ZERO:
            raise ValueError(f"directional source trade {source_id} has invalid size/cost")
        position = {
            "strategy": strategy,
            "event_key": str(row.get("event_key") or row["condition_id"]),
            "opened_at": str(row.get("scanned_at") or ""),
            "market_id": str(row["market_id"]),
            "condition_id": str(row["condition_id"]),
            "question": row.get("question"),
            "side": side,
            "token_id": str(row["token_id"]),
            "shares": str(shares),
            "entry_price": str(row.get("best_ask") or row.get("ask") or "0"),
            "all_in_cost": str(cost),
            "model_probability": str(
                row.get("calibrated_probability")
                or row.get("model_probability")
                or row.get("raw_probability")
                or "0.5"
            ),
            "raw_probability": str(row.get("raw_probability") or "0.5"),
            "fee_rate": None if row.get("fee_rate") is None else str(row["fee_rate"]),
            "provider_probabilities": dict(row.get("provider_probabilities") or {}),
            "lead_days": int(row.get("lead_days") or 0),
            "city": str(row.get("city") or ""),
            "target_date": str(row.get("target_date") or ""),
            "source_audit_id": source_id,
        }
        return str(row["condition_id"]), position

    if strategy == "weather_ladder":
        ladder = row.get("ladder")
        if not isinstance(ladder, dict) or not isinstance(ladder.get("legs"), list) or not ladder["legs"]:
            raise ValueError(f"weather-ladder source trade {source_id} has no leg vector")
        legs: list[dict[str, Any]] = []
        for leg in ladder["legs"]:
            if not isinstance(leg, dict):
                raise ValueError(f"weather-ladder source trade {source_id} has a malformed leg")
            required = ("condition_id", "market_id", "token_id", "shares", "all_in_cost")
            if any(leg.get(name) in (None, "") for name in required):
                raise ValueError(f"weather-ladder source trade {source_id} has an incomplete leg")
            legs.append({
                "key": str(leg.get("key") or leg["condition_id"]),
                "condition_id": str(leg["condition_id"]),
                "market_id": str(leg["market_id"]),
                "token_id": str(leg["token_id"]),
                "shares": str(leg["shares"]),
                "all_in_cost": str(leg["all_in_cost"]),
            })
        total_cost = Decimal(str(ladder.get("total_cost", row.get("all_in_cost", "0"))))
        if not total_cost.is_finite() or total_cost <= ZERO:
            raise ValueError(f"weather-ladder source trade {source_id} has invalid total cost")
        key = str(row.get("candidate_id") or source_id)
        position = {
            "strategy": "weather_ladder",
            "event_key": str(ladder.get("event_key") or row.get("event_key") or ""),
            "opened_at": str(row.get("scanned_at") or ""),
            "basket_id": key,
            "all_in_cost": str(total_cost),
            "shares": str(ladder.get("shares") or "0"),
            "model_probability": str(ladder.get("cluster_probability") or "0"),
            "expected_profit": str(ladder.get("expected_profit") or "0"),
            "profit_if_selected_wins": str(ladder.get("profit_if_selected_wins") or "0"),
            "loss_if_outside_cluster": str(ladder.get("loss_if_outside_cluster") or "0"),
            "legs": legs,
            "source_audit_id": source_id,
        }
        return key, position

    # The V7 main profile keeps the independent complete-set lane disabled.
    # Refuse any future unsupported entry type rather than silently mis-account.
    raise ValueError(f"unsupported source trade strategy {strategy!r} for mirror entry {source_id}")


def rebased_paper_state(source_state: dict[str, Any], *, started_at: str) -> dict[str, Any]:
    """Seed a paired mirror from current cash/open inventory, excluding old P&L.

    The pre-cutover open positions are held identical in every arm. Historical
    realized P&L and completed trade counts are deliberately not copied into the
    new comparison sample.
    """
    cash = Decimal(str(source_state.get("cash", "0")))
    open_positions = copy.deepcopy(source_state.get("open_positions") or {})
    open_cost = sum(
        (Decimal(str(position.get("all_in_cost", "0"))) for position in open_positions.values()),
        ZERO,
    )
    if not cash.is_finite() or cash < ZERO or not open_cost.is_finite() or open_cost < ZERO:
        raise ValueError("source state has invalid cash or open-position cost")
    starting_equity = cash + open_cost
    state = PaperState.new(starting_equity).to_json()
    state.update({
        "started_at": started_at,
        "initial_cash": str(starting_equity),
        "cash": str(cash),
        "open_positions": open_positions,
        "traded_conditions": sorted({
            str(leg.get("condition_id"))
            for position in open_positions.values()
            for leg in (position.get("legs") or [position])
            if leg.get("condition_id")
        }),
        "traded_strategy_keys": sorted({
            str(position.get("event_key"))
            for position in open_positions.values()
            if position.get("event_key")
        }),
        "cycles": 0,
        "total_candidates": 0,
        "total_paper_trades": 0,
        "total_paper_exits": 0,
        "realized_pnl": "0",
        "realized_settlement_pnl": "0",
        "realized_exit_pnl": "0",
        "weather_resolved": 0,
        "weather_brier_sum": "0",
        "pending_audits": {},
        "settlement_failures": {},
        "peak_entry_equity": str(starting_equity),
        "peak_mark_equity": str(starting_equity),
        "last_mark_equity": str(starting_equity),
        "peak_mark_equity_migrated": False,
    })
    return state


def mirror_settings_for_policy(settings: PaperSettings, policy: MirrorPolicy, data_dir: Path) -> PaperSettings:
    """Create a no-weather-scanner PaperWorker configuration for one mirror arm."""
    weather = replace(
        settings.weather_policy,
        enabled=False,
        observations_enabled=False,
        ladder_enabled=False,
        kelly_sizing_enabled=False,
    )
    return replace(
        settings,
        data_dir=Path(data_dir),
        entries_enabled=False,
        complete_set_enabled=False,
        early_exit_enabled=policy.early_exit_enabled,
        early_exit_target_return=policy.target_return,
        hybrid_exit_enabled=policy.hybrid_enabled,
        hybrid_exit_fraction=policy.hybrid_fraction,
        hybrid_runner_target_return=policy.runner_target_return,
        weather_policy=weather,
    )


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _complete_offset(data: bytes) -> int:
    newline = data.rfind(b"\n")
    return newline + 1 if newline >= 0 else 0


async def _stable_source_snapshot(source_dir: Path, *, attempts: int = 100) -> tuple[dict[str, Any], dict[str, Any], bytes]:
    """Take a stable, read-only snapshot while the source worker is between writes."""
    source_dir = Path(source_dir).resolve()
    state_path = source_dir / "state.json"
    status_path = source_dir / "status.json"
    trades_path = source_dir / "paper_trades.jsonl"
    for _ in range(attempts):
        state_a = state_path.read_bytes()
        status_a = status_path.read_bytes()
        trades_a = trades_path.read_bytes() if trades_path.exists() else b""
        await asyncio.sleep(0.1)
        state_b = state_path.read_bytes()
        status_b = status_path.read_bytes()
        trades_b = trades_path.read_bytes() if trades_path.exists() else b""
        if (state_a, status_a, trades_a) != (state_b, status_b, trades_b):
            continue
        try:
            source_state = json.loads(state_a)
            source_status = json.loads(status_a)
        except json.JSONDecodeError as exc:
            raise ValueError("source V7 state/status is not valid JSON") from exc
        if not isinstance(source_state, dict) or not isinstance(source_status, dict):
            raise ValueError("source V7 state/status must be JSON objects")
        if source_status.get("running") is not True:
            raise RuntimeError("source V7 paper worker must be running before mirror cutover")
        if str(source_status.get("data_dir", "")).rstrip("/") != str(source_dir):
            raise RuntimeError("source status data_dir does not match the requested V7 ledger")
        if source_status.get("live_trading_enabled") or source_status.get("account_reads_enabled"):
            raise RuntimeError("mirror source must remain paper-only with account reads disabled")
        source_store = PaperStore(source_dir)
        source_pid = source_store.current_pid()
        if source_pid is None or not source_store._pid_alive(source_pid):
            raise RuntimeError("source V7 paper worker PID is not alive")
        if source_status.get("healthy") is not True or source_status.get("errors_this_cycle", 0):
            raise RuntimeError("source V7 must have a clean completed cycle before mirror cutover")
        raw_scan_at = source_status.get("last_scan_at")
        if not raw_scan_at:
            raise RuntimeError("source V7 has no completed scan timestamp")
        try:
            scan_at = datetime.fromisoformat(str(raw_scan_at).replace("Z", "+00:00"))
        except ValueError as exc:
            raise RuntimeError("source V7 scan timestamp is invalid") from exc
        if (datetime.now(timezone.utc) - scan_at.astimezone(timezone.utc)).total_seconds() > 600:
            raise RuntimeError("source V7 last completed scan is stale; wait for a fresh healthy cycle")
        if source_state.get("pending_audits"):
            continue
        return source_state, source_status, trades_a
    raise TimeoutError("could not obtain a stable V7 state/trade snapshot during the cutover window")


@dataclass
class _MirrorArm:
    policy: MirrorPolicy
    settings: PaperSettings
    store: PaperStore
    worker: PaperWorker
    seen_source_ids: set[str]
    baseline_positions: int
    baseline_entry_equity: Decimal
    paired_entries_beyond_slots: int = 0
    paired_entries_beyond_cash: int = 0
    paired_entries_beyond_gross_cap: int = 0


class PaperExitMirror:
    """Replay the V7 main entry stream into paired, no-Meteo exit-policy ledgers."""

    def __init__(
        self,
        *,
        settings: PaperSettings,
        source_data_dir: Path,
        client: Any,
        policies: tuple[MirrorPolicy, ...] = tuple(MIRROR_POLICIES.values()),
    ) -> None:
        self.settings = settings
        self.source_data_dir = Path(source_data_dir).resolve()
        self.root_store = PaperStore(settings.data_dir)
        self.client = client
        self.policies = policies
        self.arms: dict[str, _MirrorArm] = {}
        self.mirror_state_path = settings.data_dir / "mirror_state.json"
        self.source_offset = 0
        self.source_cycle = 0
        self.source_scan_at: str | None = None
        self.baseline_source_trade_count = 0
        self.source_rows_seen = 0
        self.source_rows_skipped = 0
        self.seeded_open_positions = 0
        self.seeded_entry_equity = ZERO
        self.last_mirrored_entries = 0
        self.last_skipped_entries = 0

    async def initialize(self) -> None:
        if self.settings.weather_policy.enabled:
            raise ValueError("paper-mirror refuses weather/forecast providers; source V7 must supply entries")
        if self.settings.entries_enabled or self.settings.complete_set_enabled:
            raise ValueError("paper-mirror cannot discover or independently create entries")
        if self.settings.safety_errors():
            raise RuntimeError("paper-mirror refused unsafe settings: " + "; ".join(self.settings.safety_errors()))
        if self.mirror_state_path.exists():
            meta = json.loads(self.mirror_state_path.read_text(encoding="utf-8"))
            if Path(str(meta.get("source_data_dir", ""))).resolve() != self.source_data_dir:
                raise ValueError("existing mirror ledger is bound to a different V7 source")
            self.source_offset = int(meta["source_offset"])
            self.source_cycle = int(meta["source_cycle"])
            self.source_scan_at = meta.get("source_last_scan_at")
            self.baseline_source_trade_count = int(meta.get("source_trade_count_at_cutover", 0))
            self.source_rows_seen = int(meta.get("source_rows_seen", 0))
            self.source_rows_skipped = int(meta.get("source_rows_skipped", 0))
            self.seeded_open_positions = int(meta.get("seeded_open_positions", 0))
            self.seeded_entry_equity = Decimal(str(meta.get("seeded_entry_equity", "0")))
            await self._load_arms(meta)
            return

        source_state, source_status, trade_bytes = await _stable_source_snapshot(self.source_data_dir)
        now = datetime.now(timezone.utc).isoformat()
        rebased = rebased_paper_state(source_state, started_at=now)
        self.source_offset = _complete_offset(trade_bytes)
        self.source_cycle = int(source_status.get("cycle", 0))
        self.source_scan_at = source_status.get("last_scan_at")
        self.baseline_source_trade_count = sum(1 for line in trade_bytes[: self.source_offset].splitlines() if line.strip())
        self.seeded_open_positions = len(rebased["open_positions"])
        self.seeded_entry_equity = Decimal(str(rebased["initial_cash"]))
        for policy in self.policies:
            arm_dir = self.settings.data_dir / "arms" / policy.name
            arm_settings = mirror_settings_for_policy(self.settings, policy, arm_dir)
            store = PaperStore(arm_dir)
            store.save_state(PaperState.from_json(rebased))
            store._write_json(arm_dir / "mirror_baseline.json", {
                "paired_design": True,
                "policy": policy.name,
                "cutover_at": now,
                "source_cycle": self.source_cycle,
                "source_last_scan_at": self.source_scan_at,
                "source_trade_rows_at_cutover": self.baseline_source_trade_count,
                "seeded_open_positions": self.seeded_open_positions,
                "seeded_entry_equity": str(self.seeded_entry_equity),
                "historical_realized_pnl_copied": False,
            })
        meta = {
            "source_data_dir": str(self.source_data_dir),
            "source_offset": self.source_offset,
            "source_cycle": self.source_cycle,
            "source_last_scan_at": self.source_scan_at,
            "source_trade_count_at_cutover": self.baseline_source_trade_count,
            "source_rows_seen": 0,
            "source_rows_skipped": 0,
            "seeded_open_positions": self.seeded_open_positions,
            "seeded_entry_equity": str(self.seeded_entry_equity),
            "cutover_at": now,
            "paired_design": True,
        }
        _atomic_json(self.mirror_state_path, meta)
        await self._load_arms(meta)

    async def _load_arms(self, meta: dict[str, Any]) -> None:
        for policy in self.policies:
            arm_dir = self.settings.data_dir / "arms" / policy.name
            arm_settings = mirror_settings_for_policy(self.settings, policy, arm_dir)
            store = PaperStore(arm_dir)
            if not store.state_path.is_file():
                raise RuntimeError(f"mirror arm {policy.name} is missing its paired state")
            worker = PaperWorker(
                client=self.client,
                settings=arm_settings,
                store=store,
                forecast=None,
                observation_provider=None,
                weather_client=self.client,
            )
            mirror_rows = store.read_records(store.trades_path)
            seen = {
                str(row["mirror_source_audit_id"])
                for row in mirror_rows
                if row.get("mirror_source_audit_id")
            }
            baseline = json.loads((arm_dir / "mirror_baseline.json").read_text(encoding="utf-8"))
            arm = _MirrorArm(
                policy=policy,
                settings=arm_settings,
                store=store,
                worker=worker,
                seen_source_ids=seen,
                baseline_positions=int(baseline.get("seeded_open_positions", 0)),
                baseline_entry_equity=Decimal(str(baseline.get("seeded_entry_equity", "0"))),
            )
            arm.paired_entries_beyond_slots = sum(
                "position_cap" in (row.get("paired_entry_risk_overrides") or []) for row in mirror_rows
            )
            arm.paired_entries_beyond_cash = sum(
                "cash" in (row.get("paired_entry_risk_overrides") or []) for row in mirror_rows
            )
            arm.paired_entries_beyond_gross_cap = sum(
                "gross_exposure_cap" in (row.get("paired_entry_risk_overrides") or []) for row in mirror_rows
            )
            self.arms[policy.name] = arm

    def _apply_source_entry(self, arm: _MirrorArm, row: dict[str, Any], source_id: str) -> bool:
        if source_id in arm.seen_source_ids:
            return False
        normalized = normalize_source_trade(row)
        if normalized is None:
            return False
        position_key, position = normalized
        if position_key in arm.worker.state.open_positions:
            existing = arm.worker.state.open_positions[position_key]
            if (
                str(existing.get("strategy")) == str(position.get("strategy"))
                and str(existing.get("all_in_cost")) == str(position.get("all_in_cost"))
                and str(existing.get("shares")) == str(position.get("shares"))
            ):
                # A state-first source trade may have landed in the stable
                # cutover snapshot just before its append-only audit row.
                arm.seen_source_ids.add(source_id)
                return False
            raise ValueError(f"paired source entry {source_id} collides with open mirror key {position_key}")
        cost = Decimal(str(position["all_in_cost"]))
        risk_overrides: list[str] = []
        if len(arm.worker.state.open_positions) >= arm.settings.max_open_positions:
            arm.paired_entries_beyond_slots += 1
            risk_overrides.append("position_cap")
        if cost > arm.worker.state.cash:
            arm.paired_entries_beyond_cash += 1
            risk_overrides.append("cash")
        current_mark = arm.worker.state.last_mark_equity
        if (
            current_mark is not None
            and arm.settings.max_gross_exposure_fraction < Decimal("1")
            and arm.worker._open_entry_cost() + cost
            > arm.settings.max_gross_exposure_fraction * current_mark
        ):
            arm.paired_entries_beyond_gross_cap += 1
            risk_overrides.append("gross_exposure_cap")
        arm.worker.state.cash -= cost
        arm.worker.state.open_positions[position_key] = position
        arm.worker.state.total_paper_trades += 1
        arm.worker.state.total_candidates += 1
        if position.get("condition_id"):
            arm.worker.state.traded_conditions.add(str(position["condition_id"]))
        for leg in position.get("legs", ()) or ():
            arm.worker.state.traded_conditions.add(str(leg["condition_id"]))
        if position.get("event_key"):
            arm.worker.state.traded_strategy_keys.add(str(position["event_key"]))
        payload = dict(row)
        payload.update({
            "mirror_source_audit_id": source_id,
            "paired_entry": True,
            "mirror_policy": arm.policy.name,
            "mirror_entry_cost": str(cost),
            "paper_cash_after": str(arm.worker.state.cash),
            "weather_api_used": False,
            "paired_entry_risk_overrides": risk_overrides,
        })
        arm.store.commit_with_audit(
            arm.worker.state,
            audit_id=f"mirror-entry:{source_id}",
            stream="paper_trades",
            payload=payload,
        )
        arm.seen_source_ids.add(source_id)
        return True

    async def _sync_entries(self) -> tuple[int, int]:
        source_path = self.source_data_dir / "paper_trades.jsonl"
        rows, next_offset = read_source_trade_rows(source_path, self.source_offset)
        mirrored = 0
        skipped = 0
        for row in rows:
            source_id = str(row.get("audit_id") or row.get("candidate_id") or "")
            try:
                normalized = normalize_source_trade(row)
            except ValueError as exc:
                self.root_store.append_record(self.settings.data_dir / "mirror_rejections.jsonl", {
                    "source_audit_id": source_id or None,
                    "reason": str(exc),
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                    "public_data_only": True,
                })
                skipped += 1
                continue
            if normalized is None:
                skipped += 1
                continue
            applied_any = False
            for arm in self.arms.values():
                applied_any = self._apply_source_entry(arm, row, source_id) or applied_any
            if not all(source_id in arm.seen_source_ids for arm in self.arms.values()):
                raise RuntimeError(f"source entry {source_id} was not paired across every policy arm")
            mirrored += int(applied_any)
        self.source_offset = next_offset
        self.source_rows_seen += len(rows)
        self.source_rows_skipped += skipped
        return mirrored, skipped

    async def run_cycle(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        # Existing positions are settled/exited before this cycle's fresh V7
        # entries are mirrored, matching the source worker's cycle ordering.
        arm_results: dict[str, dict[str, Any]] = {}
        total_errors = 0
        cycle_work: dict[str, tuple[int, int, int]] = {}
        for name, arm in self.arms.items():
            settlements, settlement_errors = await arm.worker._settle_positions(now)
            exits, exit_errors = await arm.worker._exit_positions(now)
            errors = settlement_errors + exit_errors
            total_errors += errors
            cycle_work[name] = (settlements, exits, errors)

        # Entries created by V7 during its prior scan are fanned out only after
        # each arm has processed its pre-existing positions for this cycle.
        mirrored_entries, skipped_entries = await self._sync_entries()
        self.last_mirrored_entries = mirrored_entries
        self.last_skipped_entries = skipped_entries

        for name, arm in self.arms.items():
            settlements, exits, errors = cycle_work[name]
            mark = await arm.worker._mark_positions(now)
            errors += int(arm.worker._mark_error is not None)
            total_errors += int(arm.worker._mark_error is not None)
            arm.worker.state.cycles += 1
            arm.store.save_state(arm.worker.state)
            pnl = arm.worker._ledger_pnl_split()
            arm_status = {
                "name": name,
                "running": True,
                "healthy": errors == 0,
                "errors_this_cycle": errors,
                "cycle": arm.worker.state.cycles,
                "last_scan_at": now,
                "mode": "PAPER_MIRROR",
                "paired_design": True,
                "entries_enabled": False,
                "weather_api_used": False,
                "paper_exits_enabled": arm.settings.early_exit_enabled,
                "paper_exit_target_return": str(arm.settings.early_exit_target_return),
                "paper_exit_minimum_profit": str(arm.settings.early_exit_min_profit),
                "paper_hybrid_enabled": arm.settings.hybrid_exit_enabled,
                "paper_hybrid_exit_fraction": str(arm.settings.hybrid_exit_fraction),
                "paper_hybrid_runner_target_return": str(arm.settings.hybrid_runner_target_return),
                "account_reads_enabled": False,
                "live_trading_enabled": False,
                "public_data_only": True,
                "initial_cash": str(arm.worker.state.initial_cash),
                "paper_cash": str(arm.worker.state.cash),
                "open_positions": len(arm.worker.state.open_positions),
                "open_entry_cost": str(arm.worker._open_entry_cost()),
                "mark_position_value": str(mark.position_value),
                "mark_equity": str(mark.mark_equity),
                "mark_drawdown": str(arm.worker._mark_drawdown() or ZERO),
                "total_paper_trades": arm.worker.state.total_paper_trades,
                "total_paper_exits": arm.worker.state.total_paper_exits,
                "paired_entries_beyond_position_cap": arm.paired_entries_beyond_slots,
                "paired_entries_beyond_cash": arm.paired_entries_beyond_cash,
                "paired_entries_beyond_gross_cap": arm.paired_entries_beyond_gross_cap,
                "settlements_this_cycle": settlements,
                "paper_exits_this_cycle": exits,
                "paired_entries_this_cycle": mirrored_entries,
                "realized_pnl_provisional": str(arm.worker.state.realized_pnl),
                **pnl,
                "data_dir": str(arm.store.data_dir),
            }
            arm.store.write_status(arm_status)
            arm_results[name] = arm_status

        source_store = PaperStore(self.source_data_dir)
        source_status = source_store.read_status()
        source_pid = source_store.current_pid()
        source_process_alive = source_pid is not None and source_store._pid_alive(source_pid)
        source_running = bool(source_status.get("running")) and source_process_alive
        source_healthy = bool(source_status.get("healthy")) and not source_status.get("errors_this_cycle", 0)
        scan_age_seconds: float | None = None
        if source_status.get("last_scan_at"):
            try:
                source_scan = datetime.fromisoformat(str(source_status["last_scan_at"]).replace("Z", "+00:00"))
                scan_age_seconds = max(0.0, (datetime.now(timezone.utc) - source_scan.astimezone(timezone.utc)).total_seconds())
            except ValueError:
                source_healthy = False
        source_recent = scan_age_seconds is not None and scan_age_seconds <= 600
        meta = {
            "source_data_dir": str(self.source_data_dir),
            "source_offset": self.source_offset,
            "source_cycle": int(source_status.get("cycle", self.source_cycle)),
            "source_last_scan_at": source_status.get("last_scan_at", self.source_scan_at),
            "source_trade_count_at_cutover": self.baseline_source_trade_count,
            "source_rows_seen": self.source_rows_seen,
            "source_rows_skipped": self.source_rows_skipped,
            "seeded_open_positions": self.seeded_open_positions,
            "seeded_entry_equity": str(self.seeded_entry_equity),
            "paired_design": True,
        }
        _atomic_json(self.mirror_state_path, meta)
        self.source_cycle = meta["source_cycle"]
        self.source_scan_at = meta["source_last_scan_at"]
        overall = {
            "running": True,
            "healthy": total_errors == 0 and source_running and source_healthy and source_recent,
            "errors_this_cycle": total_errors,
            "mode": "PAPER_MIRROR",
            "paired_design": True,
            "source_running": source_running,
            "source_process_alive": source_process_alive,
            "source_healthy": source_healthy,
            "source_scan_age_seconds": scan_age_seconds,
            "source_cycle": self.source_cycle,
            "source_last_scan_at": self.source_scan_at,
            "source_data_dir": str(self.source_data_dir),
            "source_trade_rows_seen": self.source_rows_seen,
            "source_trade_rows_skipped": self.source_rows_skipped,
            "source_entries_mirrored_this_cycle": self.last_mirrored_entries,
            "source_entries_skipped_this_cycle": self.last_skipped_entries,
            "source_offset_bytes": self.source_offset,
            "weather_api_used": False,
            "account_reads_enabled": False,
            "live_trading_enabled": False,
            "authenticated_client_initialized": False,
            "public_data_only": True,
            "cycle_at": now,
            "arms": arm_results,
            "data_dir": str(self.settings.data_dir),
        }
        self.root_store.write_status(overall)
        return overall


async def run_paper_mirror(
    settings: PaperSettings,
    *,
    source_data_dir: Path,
    interval: float = 60.0,
    cycles: int = 0,
    client_factory: Callable[[], Any] = AsyncPublicClient,
) -> None:
    """Run paired exit-policy mirrors; never instantiate weather providers."""
    if interval <= 0 or cycles < 0:
        raise ValueError("mirror interval must be positive and cycles nonnegative")
    if settings.weather_policy.enabled:
        raise ValueError("mirror settings must set V3_PAPER_WEATHER_ENABLED=false")
    if settings.entries_enabled or settings.complete_set_enabled:
        raise ValueError("mirror settings must disable independent entries and complete sets")
    errors = settings.safety_errors()
    if errors:
        raise RuntimeError("paper mirror refused unsafe settings: " + "; ".join(errors))
    root_store = PaperStore(settings.data_dir)
    root_store.acquire()
    client: Any | None = None
    mirror: PaperExitMirror | None = None
    try:
        client = client_factory()
        mirror = PaperExitMirror(
            settings=settings,
            source_data_dir=source_data_dir,
            client=client,
        )
        await mirror.initialize()
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, stop_event.set)
            except (NotImplementedError, RuntimeError):
                pass
        completed = 0
        while not stop_event.is_set():
            summary = await mirror.run_cycle()
            print(json.dumps({
                "source_cycle": summary.get("source_cycle"),
                "source_trade_rows_seen": summary.get("source_trade_rows_seen"),
                "arms": {
                    name: {
                        "healthy": arm.get("healthy"),
                        "exits": arm.get("paper_exits_this_cycle"),
                        "settlements": arm.get("settlements_this_cycle"),
                        "open_positions": arm.get("open_positions"),
                    }
                    for name, arm in summary.get("arms", {}).items()
                },
                "weather_api_used": False,
                "errors": summary.get("errors_this_cycle"),
            }, sort_keys=True), flush=True)
            completed += 1
            if cycles and completed >= cycles:
                break
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except TimeoutError:
                pass
    except Exception as exc:
        status = root_store.read_status()
        status.update({
            "running": False,
            "healthy": False,
            "last_error": f"{type(exc).__name__}: {exc}",
            "stopped_at": datetime.now(timezone.utc).isoformat(),
        })
        root_store.write_status(status)
        raise
    finally:
        try:
            if client is not None:
                close = getattr(client, "close", None)
                if callable(close):
                    result = close()
                    if asyncio.iscoroutine(result):
                        await result
        finally:
            status = root_store.read_status()
            status.update({"running": False, "stopped_at": datetime.now(timezone.utc).isoformat()})
            root_store.write_status(status)
            root_store.release()
