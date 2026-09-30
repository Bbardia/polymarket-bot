"""Pure, network-free calibration bins built from the full scan universe.

Settled *traded* positions are a selection-biased calibration sample: they are
exactly the cases where the model disagreed most with the market. Every
evaluated market is logged to ``weather_scans.jsonl``; joining those rows with
resolved outcomes yields an unbiased calibration set.

Only rows tagged with the current ``FORECAST_MODEL_VERSION`` are used, so
probabilities produced by an older forecast model never calibrate newer ones.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from src.v3.paper_weather import FORECAST_MODEL_VERSION, ProbabilityCalibration

ZERO = Decimal(0)
ONE = Decimal(1)


@dataclass(frozen=True)
class Observation:
    condition_id: str
    city: str
    lead_days: int
    source: str
    probability: Decimal  # YES-outcome raw probability
    target_date: str


def iter_scan_rows(path: Path) -> Iterator[dict[str, Any]]:
    """Stream JSONL rows, skipping blank/malformed lines and non-objects."""
    try:
        handle = path.open("r", encoding="utf-8", errors="replace")
    except OSError:
        return
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
                yield row


def is_evaluation_row(
    row: Mapping[str, Any], *, model_version: str = FORECAST_MODEL_VERSION,
) -> bool:
    probs = row.get("provider_probabilities")
    return (
        row.get("forecast_model_version") == model_version
        and bool(row.get("condition_id"))
        and bool(row.get("city"))
        and bool(row.get("target_date"))
        and isinstance(probs, dict)
        and bool(probs)
        and row.get("lead_days") is not None
    )


def dedupe_observations(
    rows: Iterable[Mapping[str, Any]], *, model_version: str = FORECAST_MODEL_VERSION,
) -> list[Observation]:
    """One observation per (condition_id, lead_days, source): the latest scan.

    Consumes ``rows`` in a single pass; memory is bounded by unique keys.
    """
    latest: dict[tuple[str, int, str], tuple[tuple[str, int], Observation]] = {}
    for index, row in enumerate(rows):
        if not is_evaluation_row(row, model_version=model_version):
            continue
        try:
            lead = int(row["lead_days"])
        except (TypeError, ValueError):
            continue
        condition_id = str(row["condition_id"])
        rank = (str(row.get("scanned_at") or ""), index)
        for source, raw in row["provider_probabilities"].items():
            try:
                probability = Decimal(str(raw))
            except (InvalidOperation, ValueError):
                continue
            if not probability.is_finite() or not ZERO <= probability <= ONE:
                continue
            key = (condition_id, lead, str(source))
            current = latest.get(key)
            if current is None or rank >= current[0]:
                latest[key] = (rank, Observation(
                    condition_id, str(row["city"]), lead, str(source), probability,
                    str(row["target_date"])[:10],
                ))
    return [item[1] for item in latest.values()]


def past_condition_ids(observations: Iterable[Observation], today: date) -> list[str]:
    """Condition IDs of usable observations whose target date has passed."""
    found: set[str] = set()
    for obs in observations:
        try:
            if date.fromisoformat(obs.target_date) < today:
                found.add(obs.condition_id)
        except ValueError:
            continue
    return sorted(found)


def build_calibration(
    observations: Iterable[Observation],
    outcomes: Mapping[str, int],
    *,
    min_samples: int = 20,
) -> ProbabilityCalibration:
    """In-memory calibrator fed with every resolved observation."""
    calibration = ProbabilityCalibration(None, min_samples=min_samples)
    for obs in observations:
        outcome = outcomes.get(obs.condition_id)
        if outcome in (0, 1):
            calibration.record(obs.source, obs.city, obs.lead_days, obs.probability, int(outcome))
    return calibration


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
    if sorted(values) != [ZERO, ONE]:
        return None
    yes_index = 0
    if isinstance(outcomes, list) and len(outcomes) == 2:
        names = [str(o).strip().lower() for o in outcomes]
        if names == ["no", "yes"]:
            yes_index = 1
        elif names != ["yes", "no"]:
            return None
    return int(values[yes_index] == 1)
