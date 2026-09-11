#!/usr/bin/env python3
"""Full-cohort shadow settlement report from public CLOB winners (item 3).

Usage:
    python scripts/shadow_reconcile.py --data-dir data/<campaign> [--out PATH]

Reads the campaign JSONL ledgers, fetches ``/markets/<condition_id>`` from the
public CLOB for every traded condition (bounded retry), and writes an additive
``shadow_reconciliation.json``. Never modifies the campaign ledgers.
"""

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path

from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.v3.public_cache import PublicCache
from src.v3.shadow_settlement import load_cohort, reconcile  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=None, help="default: <data-dir>/shadow_reconciliation.json")
    parser.add_argument("--publish-fraction", type=Decimal, default=Decimal("0.05"))
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--max-requests", type=int, default=100)
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()

    cache = PublicCache(args.cache_dir, timeout=args.timeout, max_requests=args.max_requests, offline=args.offline)
    getter = cache.get_json

    cohort = load_cohort(args.data_dir)
    report = reconcile(cohort, getter=getter, unresolved_publish_fraction=args.publish_fraction)
    report["data_dir"] = str(args.data_dir)
    out = args.out or (args.data_dir / "shadow_reconciliation.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("x", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=True)
    summary = {k: report[k] for k in ("cohort", "by_terminal", "hold_vs_exit", "cluster_count", "gross_stake",
                                        "unresolved_stake", "realized_pnl_publishable")}
    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"written: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
