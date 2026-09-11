"""Full-cohort shadow settlement and self-reconciling ledger report (item 3).

Additive only: reads a campaign's ``paper_trades.jsonl``, ``paper_exits.jsonl``
and ``settlements.jsonl``, settles every traded condition from the public CLOB
``/markets/<condition_id>`` ``tokens[].winner`` field, and writes a separate
``shadow_reconciliation.json``. Legacy records are never rewritten.

Every trade ends in exactly one terminal class: ``exit_full``, ``settled``,
``shadow_settled`` (resolved on-venue but no ledger settlement), or
``unresolved``. Realized P&L is refused for publication while unresolved stake
exceeds the configured fraction of gross stake.
"""

from __future__ import annotations

import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping

ZERO = Decimal("0")
CLOB_MARKET_URL = "https://clob.polymarket.com/markets/{condition_id}"


@dataclass
class TradeOutcome:
    condition_id: str
    strategy: str
    side: str
    shares: Decimal
    all_in_cost: Decimal
    target_date: str | None
    city: str | None
    model_probability: Decimal | None
    exit_shares: Decimal = ZERO
    exit_proceeds: Decimal = ZERO
    exit_pnl: Decimal = ZERO
    ledger_settlement_pnl: Decimal | None = None
    winner_side: str | None = None
    shadow_source: str | None = None
    shadow_error: str | None = None
    terminal: str = "unresolved"

    @property
    def remaining_shares(self) -> Decimal:
        return self.shares - self.exit_shares

    def hold_pnl(self) -> Decimal | None:
        """P&L if the full position had been held to resolution (no exits)."""
        if self.winner_side is None:
            return None
        payout = self.shares if self.winner_side == self.side else ZERO
        return payout - self.all_in_cost

    def realized_pnl(self) -> Decimal | None:
        """Exit proceeds plus resolution of the remainder, when resolvable."""
        if self.remaining_shares <= ZERO:
            return self.exit_pnl
        if self.winner_side is None:
            return None
        remaining_cost = self.all_in_cost * (self.remaining_shares / self.shares) if self.shares else ZERO
        payout = self.remaining_shares if self.winner_side == self.side else ZERO
        return self.exit_pnl + payout - remaining_cost

    def as_dict(self) -> dict[str, Any]:
        hold = self.hold_pnl()
        realized = self.realized_pnl()
        return {
            "condition_id": self.condition_id,
            "strategy": self.strategy,
            "side": self.side,
            "shares": str(self.shares),
            "all_in_cost": str(self.all_in_cost),
            "target_date": self.target_date,
            "city": self.city,
            "model_probability": None if self.model_probability is None else str(self.model_probability),
            "exit_shares": str(self.exit_shares),
            "exit_pnl": str(self.exit_pnl),
            "ledger_settlement_pnl": None if self.ledger_settlement_pnl is None else str(self.ledger_settlement_pnl),
            "winner_side": self.winner_side,
            "shadow_source": self.shadow_source,
            "shadow_error": self.shadow_error,
            "terminal": self.terminal,
            "hold_pnl": None if hold is None else str(hold),
            "realized_pnl": None if realized is None else str(realized),
        }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"malformed ledger {path.name}") from exc
        if not isinstance(value, dict):
            raise ValueError(f"non-object ledger row {path.name}")
        rows.append(value)
    return rows


def load_cohort(data_dir: Path) -> dict[str, TradeOutcome]:
    trades = _read_jsonl(data_dir / "paper_trades.jsonl")
    exits = _read_jsonl(data_dir / "paper_exits.jsonl")
    settlements = _read_jsonl(data_dir / "settlements.jsonl")
    cohort: dict[str, TradeOutcome] = {}
    seen: set[str] = set()
    for row in trades:
        if not row.get("paper_executed"):
            continue
        audit_id = row.get("audit_id") or row.get("candidate_id")
        if audit_id and audit_id in seen:
            continue
        if audit_id:
            seen.add(audit_id)
        if row.get("strategy") == "complete_set":
            opportunity = row.get("opportunity", {})
            cid = str(row.get("condition_id", ""))
            total_cost = Decimal(str(opportunity["gross_cost"])) + Decimal(str(opportunity["fees"]))
            for side in ("YES", "NO"):
                # Historical baskets lack per-side fee allocation; split cost for reporting only.
                cohort[f"{cid}:{len(cohort)}"] = TradeOutcome(
                    condition_id=cid, strategy="complete_set", side=side,
                    shares=Decimal(str(opportunity["shares"])), all_in_cost=total_cost / 2,
                    target_date=row.get("target_date"), city=row.get("city"), model_probability=None,
                )
            continue
        if row.get("strategy") == "weather_ladder":
            for leg in row.get("ladder", {}).get("legs", []):
                cid = str(leg.get("condition_id", ""))
                if not cid:
                    continue
                key = f"{cid}:{len(cohort)}"
                cohort[key] = TradeOutcome(
                    condition_id=cid, strategy="weather_ladder", side="YES",
                    shares=Decimal(str(leg.get("shares", "0"))),
                    all_in_cost=Decimal(str(leg.get("all_in_cost", "0"))),
                    target_date=row.get("event_key", "").split(":")[-1] or None,
                    city=row.get("event_key", "").split(":")[-2] if ":" in row.get("event_key", "") else None,
                    model_probability=Decimal(str(leg["model_probability"])) if leg.get("model_probability") is not None else None,
                )
            continue
        cid = str(row.get("condition_id", ""))
        if not cid:
            continue
        key = f"{cid}:{len(cohort)}"
        cohort[key] = TradeOutcome(
            condition_id=cid,
            strategy=str(row.get("strategy", "")),
            side=str(row.get("side", "YES")),
            shares=Decimal(str(row.get("shares", "0"))),
            all_in_cost=Decimal(str(row.get("all_in_cost", "0"))),
            target_date=row.get("target_date"),
            city=row.get("city"),
            model_probability=(
                Decimal(str(row.get("model_probability", row.get("calibrated_probability")))) if row.get("model_probability", row.get("calibrated_probability")) is not None else None
            ),
        )
    def match(row):
        matches = [o for o in cohort.values() if o.condition_id == str(row.get("condition_id", ""))]
        if len(matches) > 1:
            raise ValueError("ambiguous repeated-condition exit/settlement; lot attribution required")
        return matches[0] if matches else None

    seen_exits = set()
    for row in exits:
        if row.get("audit_id") and row["audit_id"] in seen_exits:
            continue
        if row.get("audit_id"):
            seen_exits.add(row["audit_id"])
        if row.get("status") == "paper_exit_error":
            continue
        outcome = match(row)
        if outcome is None:
            continue
        outcome.exit_shares += Decimal(str(row.get("shares", "0")))
        outcome.exit_proceeds += Decimal(str(row.get("net_proceeds", "0")))
        outcome.exit_pnl += Decimal(str(row.get("realized_pnl", "0")))
    for row in settlements:
        if row.get("status") == "settlement_error":
            continue
        basket = [o for o in cohort.values() if o.condition_id == str(row.get("condition_id", "")) and o.strategy == "complete_set"]
        if basket:
            if len(basket) != 2:
                raise ValueError("ambiguous repeated complete-set settlement")
            for o in basket:
                o.ledger_settlement_pnl = Decimal(str(row.get("realized_pnl", "0"))) / 2
            continue
        outcome = match(row)
        if outcome is None:
            continue
        outcome.ledger_settlement_pnl = Decimal(str(row.get("realized_pnl", "0")))
    for outcome in cohort.values():
        if outcome.shares <= ZERO or outcome.all_in_cost < ZERO or not ZERO <= outcome.exit_shares <= outcome.shares:
            raise ValueError("invalid cohort quantities")
    return cohort


def fetch_clob_winner(
    condition_id: str,
    *,
    getter: Callable[[str], Any],
    attempts: int = 3,
    base_delay: float = 0.5,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[str | None, str, str | None]:
    """Return (winner_side, source, error) from CLOB tokens[].winner with bounded retry."""
    last_error: str | None = None
    for attempt in range(1, attempts + 1):
        try:
            payload = getter(CLOB_MARKET_URL.format(condition_id=condition_id))
        except Exception as exc:  # network failures are data, not crashes
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < attempts:
                sleep(base_delay * attempt)
            continue
        tokens = payload.get("tokens", []) if isinstance(payload, dict) else []
        winners = [token for token in tokens if token.get("winner") is True]
        if payload.get("disputed") is True or str(payload.get("umaResolutionStatus", "")).lower() in {"disputed", "proposed"}:
            return None, "clob-markets-tokens-winner", "resolution not final"
        if len(winners) == 1:
            outcome = str(winners[0].get("outcome", "")).upper()
            if outcome not in {"YES", "NO"}:
                return None, "clob-markets-tokens-winner", "nonbinary outcome unsupported"
            side = outcome
            return side, "clob-markets-tokens-winner", None
        if not tokens:
            return None, "clob-markets-tokens-winner", "no tokens in payload"
        return None, "clob-markets-tokens-winner", None  # not yet resolved
    return None, "clob-markets-tokens-winner", last_error


def reconcile(
    cohort: Mapping[str, TradeOutcome],
    *,
    getter: Callable[[str], Any],
    unresolved_publish_fraction: Decimal = Decimal("0.05"),
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    fetched = {}
    for outcome in cohort.values():
        if outcome.condition_id not in fetched:
            fetched[outcome.condition_id] = fetch_clob_winner(outcome.condition_id, getter=getter, sleep=sleep)
        winner, source, error = fetched[outcome.condition_id]
        outcome.winner_side = winner
        outcome.shadow_source = source
        outcome.shadow_error = error
        if outcome.remaining_shares <= ZERO:
            outcome.terminal = "exit_full"
        elif outcome.ledger_settlement_pnl is not None and winner is not None:
            outcome.terminal = "settled"
        elif winner is not None:
            outcome.terminal = "shadow_settled"
        else:
            outcome.terminal = "unresolved"

    gross_stake = sum((o.all_in_cost for o in cohort.values()), ZERO)
    unresolved_stake = sum(
        (o.all_in_cost * (o.remaining_shares / o.shares) if o.shares else ZERO
         for o in cohort.values() if o.terminal == "unresolved"),
        ZERO,
    )
    publishable = gross_stake > ZERO and unresolved_stake <= unresolved_publish_fraction * gross_stake

    def summarize(items: list[TradeOutcome]) -> dict[str, Any]:
        realized = [o.realized_pnl() for o in items]
        hold = [o.hold_pnl() for o in items]
        resolved = [o for o in items if o.winner_side is not None]
        hits = [1 if o.winner_side == o.side else 0 for o in resolved]
        briers = [
            (o.model_probability - Decimal(hit)) ** 2
            for o, hit in zip(resolved, hits) if o.model_probability is not None
        ]
        return {
            "n": len(items),
            "n_resolved_on_venue": len(resolved),
            "stake": str(sum((o.all_in_cost for o in items), ZERO)),
            "realized_pnl_known": str(sum((p for p in realized if p is not None), ZERO)),
            "realized_pnl_unknown_count": sum(1 for p in realized if p is None),
            "hold_to_resolution_pnl": str(sum((p for p in hold if p is not None), ZERO)),
            "hit_rate": None if len(resolved) != len(items) or not hits else str(Decimal(sum(hits)) / Decimal(len(hits))),
            "brier": None if len(resolved) != len(items) or not briers else str(sum(briers, ZERO) / Decimal(len(briers))),
        }

    by_terminal: dict[str, list[TradeOutcome]] = defaultdict(list)
    by_date: dict[str, list[TradeOutcome]] = defaultdict(list)
    for outcome in cohort.values():
        by_terminal[outcome.terminal].append(outcome)
        by_date[outcome.target_date or "unknown"].append(outcome)
    all_items = list(cohort.values())
    return {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": "clob-markets-tokens-winner (public, unauthenticated)",
        "cohort": summarize(all_items),
        "by_terminal": {key: summarize(items) for key, items in sorted(by_terminal.items())},
        "hold_vs_exit": {
            "exited_legs_realized_exit_pnl": str(sum((o.exit_pnl for o in all_items), ZERO)),
            "exited_positions_hold_pnl": str(sum(
                (o.hold_pnl() or ZERO for o in all_items if o.exit_shares > ZERO), ZERO
            )),
        },
        "clusters_by_target_date": {
            key: {"n": len(items), **{k: v for k, v in summarize(items).items() if k in ("realized_pnl_known", "hit_rate")}}
            for key, items in sorted(by_date.items())
        },
        "cluster_count": len(set(by_date) - {"unknown"}),
        "statistics_status": "complete_cohort" if all(o.winner_side is not None for o in all_items) and all_items else "insufficient_data",
        "gross_stake": str(gross_stake),
        "unresolved_stake": str(unresolved_stake),
        "unresolved_publish_fraction": str(unresolved_publish_fraction),
        "realized_pnl_publishable": publishable,
        "trades": [o.as_dict() for o in all_items],
    }
