#!/usr/bin/env python3
"""V8 public-data-only paper worker."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.v8.worker import V8Settings, V8Worker, paper_status

ROOT = Path(__file__).resolve().parent


def main() -> int:
    parser = argparse.ArgumentParser(description="Run V8 public-data-only paper research.")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("paper-run")
    run.add_argument("--cycles", type=int, default=0)
    run.add_argument("--interval", type=float, default=None)
    status = sub.add_parser("paper-status")
    args = parser.parse_args()
    settings = V8Settings.from_env(ROOT)
    if args.command == "paper-status":
        print(json.dumps(paper_status(settings), indent=2, sort_keys=True))
        return 0
    if args.interval is not None:
        settings = V8Settings(**{**settings.__dict__, "scan_interval_seconds": args.interval})
    V8Worker(settings).run(cycles=args.cycles)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
