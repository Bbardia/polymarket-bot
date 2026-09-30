"""Pure, network-free calibration bins built from the full scan universe.

``ProbabilityCalibration`` is otherwise fed only by settled *traded* positions,
which is selection-biased. Every evaluated market is logged to
``weather_scans.jsonl``; joining those rows with resolved outcomes yields an
unbiased calibration set. Output uses the exact ``ProbabilityCalibration``
JSON format and key scheme, plus pooled ``source:*:lead:bucket`` keys.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping

from src.v3.paper_weather import POOLED_CITY, ProbabilityCalibration

_KEYER = ProbabilityCalibration(None)


@dataclass(frozen=True)
class Observation:
    condition_id: str
    city: str
    lead_days: int
    source: str
    probability: Decimal  # YES-outcome raw probability


def load_scan_rows(path: Path) -> list[dict[str, Any]]:
    """Read JSONL rows, skipping blank/malformed lines and non-objects."""
    rows: list[dict[str, Any]] = []
    try:
        handle = path.open("r", encoding="utf-8", errors="replace")
    except OSError:
        return rows
    with handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def is_evaluation_row(row: Mapping[str, Any]) -> bool:
    probs = row.get("provider_probabilities")
    return (
        bool(row.get("condition_id"))
        and bool(row.get("city"))
        and isinstance(probs, dict)
        and bool(probs)
        and row.get("lead_days") is not None
    )


def dedupe_observations(rows: Iterable[Mapping[str, Any]]) -> list[Observation]:
    """One observation per (condition_id, lead_days, source): the latest scan."""
    latest: dict[tuple[str, int, str], tuple[str, int, Observation]] = {}
    for index, row in enumerate(rows):
        if not is_evaluation_row(row):
            continue
        try:
            lead = int(row["lead_days"])
        except (TypeError, ValueError):
            continue
        condition_id = str(row["condition_id"])
        stamp = str(row.get("scanned_at") or "")
        for source, raw in row["provider_probabilities"].items():
            try:
                probability = Decimal(str(raw))
            except (InvalidOperation, ValueError):
                continue
            if not probability.is_finite() or not Decimal(0) <= probability <= Decimal(1):
                continue
            key = (condition_id, lead, str(source))
            rank = (stamp, index)
            current = latest.get(key)
            if current is None or rank >= (current[0], current[1]):
                latest[key] = (
                    stamp,
                    index,
                    Observation(condition_id, str(row["city"]), lead, str(source), probability),
                )
    return [item[2] for item in latest.values()]


def build_bins(
    observations: Iterable[Observation],
    outcomes: Mapping[str, int],
) -> dict[str, dict[str, int]]:
    """Bins in ProbabilityCalibration format, with city and pooled keys."""
    bins: dict[str, dict[str, int]] = {}
    for obs in observations:
        outcome = outcomes.get(obs.condition_id)
        if outcome not in (0, 1):
            continue
        for city in (obs.city, POOLED_CITY):
            key = _KEYER._key(obs.source, city, obs.lead_days, obs.probability)
            bucket = bins.setdefault(key, {"successes": 0, "total": 0})
            bucket["successes"] += int(outcome)
            bucket["total"] += 1
    return bins


def write_json_atomic(payload: Mapping[str, Any], path: Path) -> None:
    """Atomically write JSON (tmp + replace)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def parse_gamma_outcome(market: Mapping[str, Any]) -> int | None:
    """YES outcome (1/0) from a Gamma market, or None if unresolved.

    Mirrors PaperWorker settlement: market must be closed, not UMA-disputed,
    and its outcome prices exactly {0, 1}.
    """
    if not market.get("closed"):
        return None
    if "DISPUTED" in str(market.get("umaResolutionStatus") or "").upper():
        return None
    prices = market.get("outcomePrices")
    outcomes = market.get("outcomes")
    try:
        if isinstance(prices, str):
            prices = json.loads(prices)
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if not isinstance(prices, list) or len(prices) != 2:
            return None
        values = [Decimal(str(p)) for p in prices]
    except (ValueError, InvalidOperation, TypeError):
        return None
    if sorted(values) != [Decimal(0), Decimal(1)]:
        return None
    yes_index = 0
    if isinstance(outcomes, list) and len(outcomes) == 2:
        names = [str(o).strip().lower() for o in outcomes]
        if names == ["no", "yes"]:
            yes_index = 1
        elif names != ["yes", "no"]:
            return None
    return int(values[yes_index] == 1)
