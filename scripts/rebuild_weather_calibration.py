#!/usr/bin/env python3
"""Rebuild the weather calibration from the FULL scan universe.

Streams ``<data_dir>/weather_scans.jsonl`` (every evaluated market, not just
traded ones), keeps rows produced by the current forecast model version,
fetches resolutions of past-dated markets from the public Gamma API, caches
them in ``<data_dir>/scan_outcomes.json`` and atomically rewrites
``<data_dir>/weather_calibration.<model-version>.json``.

This rebuild is the only writer of the calibration file. The paper worker
re-reads it when it changes, so no restart is needed. Run it periodically
(e.g. daily from cron).

A failed Gamma batch is retried one market at a time. If more than 5% of the
markets to fetch still fail, the outcome cache is saved but the calibration
file is left untouched and the exit code is 1, so a largely failed fetch never
replaces a fuller calibration. A market that stays unresolved or unfetchable
for 7 runs is given up on (tracked in ``scan_outcome_attempts.json``), so one
bad id cannot block rebuilds or grow the request volume forever.

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

from src.v3.paper_weather import CALIBRATION_FILENAME, write_json_atomic  # noqa: E402
from src.v3.scan_calibration import (  # noqa: E402
    build_calibration,
    dedupe_observations,
    iter_scan_rows,
    parse_gamma_outcome,
    past_condition_ids,
)

GAMMA_URL = "https://gamma-api.polymarket.com/markets"
BATCH_SIZE = 20
MAX_ATTEMPTS = 7
MAX_FAILED_FRACTION = 0.05

Fetcher = Callable[[Sequence[str]], list]


def gamma_fetcher(condition_ids: Sequence[str]) -> list:
    import requests

    params: list[tuple[str, str]] = [("condition_ids", c) for c in condition_ids]
    params.append(("limit", str(len(condition_ids))))
    response = requests.get(GAMMA_URL, params=params, timeout=20)
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else []


def _load_int_map(path: Path) -> dict[str, int]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(k): v for k, v in payload.items() if isinstance(v, int) and not isinstance(v, bool)}


def _load_cache(path: Path) -> dict[str, int]:
    return {k: v for k, v in _load_int_map(path).items() if v in (0, 1)}


def _fetch_outcomes(
    ids: Sequence[str], fetcher: Fetcher,
) -> tuple[dict[str, int], set[str]]:
    """Resolved outcomes for ``ids`` and the ids that could not be fetched."""
    try:
        markets = fetcher(ids)
    except Exception as exc:
        if len(ids) == 1:
            print(f"error: fetch failed for {ids[0]}: {type(exc).__name__}: {exc}",
                  file=sys.stderr)
            return {}, set(ids)
        resolved: dict[str, int] = {}
        failed: set[str] = set()
        for cid in ids:  # isolate the bad id(s) instead of losing the batch
            one, bad = _fetch_outcomes([cid], fetcher)
            resolved.update(one)
            failed |= bad
        return resolved, failed
    wanted = set(ids)
    resolved = {}
    for market in markets:
        if not isinstance(market, dict):
            continue
        cid = str(market.get("conditionId") or "")
        if cid in wanted:
            outcome = parse_gamma_outcome(market)
            if outcome is not None:
                resolved[cid] = outcome
    return resolved, set()


def run(
    data_dir: Path,
    *,
    dry_run: bool = False,
    fetcher: Fetcher = gamma_fetcher,
    today: date | None = None,
    batch_size: int = BATCH_SIZE,
) -> dict[str, Any]:
    today = today or datetime.now(timezone.utc).date()
    counter = {"rows": 0}

    def counted_rows():
        for row in iter_scan_rows(data_dir / "weather_scans.jsonl"):
            counter["rows"] += 1
            yield row

    observations = dedupe_observations(counted_rows())
    cache_path = data_dir / "scan_outcomes.json"
    attempts_path = data_dir / "scan_outcome_attempts.json"
    outcomes = _load_cache(cache_path)
    attempts = _load_int_map(attempts_path)
    todo = [
        c for c in past_condition_ids(observations, today)
        if c not in outcomes and attempts.get(c, 0) < MAX_ATTEMPTS
    ]
    failed: set[str] = set()
    for start in range(0, len(todo), batch_size):
        batch = todo[start:start + batch_size]
        resolved, bad = _fetch_outcomes(batch, fetcher)
        outcomes.update(resolved)
        failed |= bad
        for cid in batch:
            if cid in resolved:
                attempts.pop(cid, None)
            else:  # unresolved or unfetchable this run
                attempts[cid] = attempts.get(cid, 0) + 1
    calibration = build_calibration(observations, outcomes)
    bins = calibration.to_json()
    stats: dict[str, Any] = {
        "rows_read": counter["rows"],
        "observations_total": len(observations),
        "observations_used": sum(1 for o in observations if o.condition_id in outcomes),
        "resolved_markets": len(outcomes),
        "bins": len(bins),
        "fetch_attempted": len(todo),
        "fetch_failed": len(failed),
        "gave_up": sum(1 for count in attempts.values() if count >= MAX_ATTEMPTS),
        "calibration_written": False,
    }
    too_many_failures = len(failed) > MAX_FAILED_FRACTION * len(todo)
    stats["too_many_failures"] = too_many_failures
    if not dry_run:
        write_json_atomic(outcomes, cache_path)
        write_json_atomic(attempts, attempts_path)
        # Never replace a calibration with an empty or largely unfetched one.
        if bins and not too_many_failures:
            write_json_atomic(bins, data_dir / CALIBRATION_FILENAME)
            stats["calibration_written"] = True
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
    print(f"fetch failures:     {stats['fetch_failed']} of {stats['fetch_attempted']}"
          f" (gave up on {stats['gave_up']} after {MAX_ATTEMPTS} runs)")
    if args.dry_run:
        print("dry run: nothing written")
    elif stats["too_many_failures"]:
        print("too many fetch failures: calibration left untouched")
        return 1
    elif not stats["calibration_written"]:
        print(f"no bins produced: existing {CALIBRATION_FILENAME} left untouched")
    else:
        print(f"wrote {CALIBRATION_FILENAME}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
