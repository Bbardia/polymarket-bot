#!/usr/bin/env python3
"""Read-only V7 P&L diagnostic from existing local ledgers; no network access."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.v3.pnl_eval import build_report  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--mirror-status", type=Path, default=None)
    parser.add_argument("--out", type=Path, default=None, help="optional create-only JSON report path")
    args = parser.parse_args()
    report = build_report(args.data_dir, mirror_status_path=args.mirror_status)
    encoded = json.dumps(report, indent=2, sort_keys=True)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("x", encoding="utf-8") as stream:
            stream.write(encoded + "\n")
    summary = {
        "panel": report["panel"]["status"],
        "resolved_unique_conditions": report["panel"]["n_unique_settled_conditions"],
        "model_brier": report["scores"]["model_brier"],
        "entry_ask_brier": report["scores"]["entry_ask_brier"],
        "threshold_policy": report["threshold_policy"]["status"],
        "proper_bet": report["proper_bet_shadow"]["status"],
        "exit_comparison": report["exit_comparison"]["status"],
    }
    print(json.dumps(summary, sort_keys=True))
    if args.out is not None:
        print(f"created: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
