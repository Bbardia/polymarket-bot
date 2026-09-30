#!/usr/bin/env python3
"""Rebuild weather_calibration.json from the FULL scan universe.

Reads ``<data_dir>/weather_scans.jsonl`` (every evaluated market, not just
traded ones), fetches resolutions of past-dated markets from the public Gamma
API, caches them in ``<data_dir>/scan_outcomes.json`` and atomically rewrites
``<data_dir>/weather_calibration.json``.

NOTE: this rebuild REPLACES the traded-only records written at settlement time
by ``ProbabilityCalibration.record``. Running it periodically removes the
selection bias (only strongly-disagreeing, traded cases) of those records.

Usage:
    python scripts/rebuild_weather_calibration.py [--data-dir DIR] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.v3.scan_calibration import (  # noqa: E402
    build_bins,
    dedupe_observations,
    load_scan_rows,
    parse_gamma_outcome,
    write_json_atomic,
)

GAMMA_URL = "https://gamma-api.polymarket.com/markets"
BATCH_SIZE = 20

Fetcher = Callable[[Sequence[str]], list]


def gamma_fetcher(condition_ids: Sequence[str]) -> list:
    import requests

    params: list[tuple[str, str]] = [("condition_ids", c) for c in condition_ids]
    params.append(("limit", str(len(condition_ids))))
    response = requests.get(GAMMA_URL, params=params, timeout=20)
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else []


def _past_condition_ids(rows: Sequence[dict[str, Any]], today: date) -> list[str]:
    found: set[str] = set()
    for row in rows:
        cid, target = row.get("condition_id"), row.get("target_date")
        if not cid or not target or not isinstance(row.get("provider_probabilities"), dict):
            continue
        try:
            if date.fromisoformat(str(target)[:10]) < today:
                found.add(str(cid))
        except ValueError:
            continue
    return sorted(found)


def _load_cache(path: Path) -> dict[str, int]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(k): int(v) for k, v in payload.items() if v in (0, 1)}


def run(
    data_dir: Path,
    *,
    dry_run: bool = False,
    fetcher: Fetcher = gamma_fetcher,
    today: date | None = None,
    batch_size: int = BATCH_SIZE,
) -> dict[str, int]:
    today = today or datetime.now(timezone.utc).date()
    rows = load_scan_rows(data_dir / "weather_scans.jsonl")
    observations = dedupe_observations(rows)
    cache_path = data_dir / "scan_outcomes.json"
    outcomes = _load_cache(cache_path)
    todo = [c for c in _past_condition_ids(rows, today) if c not in outcomes]
    for start in range(0, len(todo), batch_size):
        batch = todo[start:start + batch_size]
        try:
            markets = fetcher(batch)
        except Exception as exc:  # network failure: keep what we already have
            print(f"warning: fetch failed for batch at {start}: {type(exc).__name__}: {exc}",
                  file=sys.stderr)
            continue
        for market in markets:
            if not isinstance(market, dict):
                continue
            cid = str(market.get("conditionId") or "")
            if cid in batch:
                outcome = parse_gamma_outcome(market)
                if outcome is not None:
                    outcomes[cid] = outcome
    bins = build_bins(observations, outcomes)
    stats = {
        "rows_read": len(rows),
        "observations_total": len(observations),
        "observations_used": sum(1 for o in observations if o.condition_id in outcomes),
        "resolved_markets": len(outcomes),
        "bins": len(bins),
    }
    if not dry_run:
        write_json_atomic(outcomes, cache_path)
        if bins:  # never clobber an existing calibration with an empty one
            write_json_atomic(bins, data_dir / "weather_calibration.json")
    return stats


def main(argv: Sequence[str] | None = None, *, fetcher: Fetcher = gamma_fetcher,
         today: date | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", type=Path,
                        default=Path(os.getenv("V3_PAPER_DATA_DIR", "data/v6-main-paper")))
    parser.add_argument("--dry-run", action="store_true",
                        help="print summary stats only; write nothing")
    args = parser.parse_args(argv)
    stats = run(args.data_dir, dry_run=args.dry_run, fetcher=fetcher, today=today)
    print(f"rows read:          {stats['rows_read']}")
    print(f"observations used:  {stats['observations_used']} (of {stats['observations_total']} deduped)")
    print(f"resolved markets:   {stats['resolved_markets']}")
    print(f"bins:               {stats['bins']}")
    if args.dry_run:
        print("dry run: nothing written")
    elif stats["bins"] == 0:
        print("no bins produced: existing weather_calibration.json left untouched")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
