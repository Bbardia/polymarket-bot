#!/usr/bin/env python3
"""Non-running CLI for the Polymarket V3 foundation."""

from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from src.v3.api import UnifiedPolymarketAPI
from src.v3.config import V3Settings
from src.v3.simulation import (
    evaluate_shadow_candidates,
    load_replay_events,
    load_shadow_candidates,
    replay_maker_events,
)

ROOT = Path(__file__).resolve().parent


def validate_config() -> int:
    load_dotenv(ROOT / ".env", override=False)
    settings = V3Settings.from_env()
    api = UnifiedPolymarketAPI(settings=settings)
    errors = settings.live_client_errors()
    account_errors = settings.account_client_errors()

    print(f"SDK: polymarket-client {api.sdk_version}")
    print("Collateral: pUSD on Polygon")
    print("Mode: PAPER / LIVE CLIENT BLOCKED" if errors else "Mode: LIVE CLIENT GATE SATISFIED")
    print("Account reads: BLOCKED" if account_errors else "Account reads: GATE SATISFIED")
    print(f"Secure client initialized: {str(api.secure_client_initialized).lower()}")
    print(f"Capital cap: {settings.max_capital} pUSD")
    print(f"Order cap: {settings.max_order_notional} pUSD")
    print(f"Cash reserve: {settings.reserve_fraction:.0%}")
    if errors:
        print("Live gate:")
        for error in errors:
            print(f"  - {error}")
    return 0


def show_architecture() -> int:
    print("V3 component boundaries:")
    print("  official SDK -> typed read/account adapter")
    print("  remote snapshot -> read-only reconciliation")
    print("  order/trade events -> append-only SQLite ledger")
    print("  read-only streams -> replay-safe normalization and reconnect supervision")
    print("  Decimal math -> fee/VWAP/uncertainty/Kelly")
    print("  hard risk engine -> post-only short-TTL intents")
    print("  paper evaluators -> weather, complete sets, queue-aware replay")
    print("  execution authorization -> disabled by default")
    return 0


def shadow_report(path: Path) -> int:
    result = evaluate_shadow_candidates(load_shadow_candidates(path))
    print(f"Shadow file: {path}")
    print(f"Candidates: {result.candidates}")
    print(f"Filled candidates: {result.filled_candidates}")
    print(f"Unfilled candidates: {result.unfilled_candidates}")
    print(f"Brier score: {result.brier_score}")
    filled_brier = "n/a" if result.filled_brier_score is None else str(result.filled_brier_score)
    print(f"Filled Brier score: {filled_brier}")
    print(f"Observed fees: {result.total_fees} pUSD")
    print(f"Realized P&L: {result.realized_pnl} pUSD")
    return 0


def replay_report(path: Path) -> int:
    result = replay_maker_events(load_replay_events(path))
    print(f"Replay file: {path}")
    print(f"Events: {result.events}")
    print(f"Quotes placed: {result.quotes_placed}")
    print(f"Trades seen: {result.trades_seen}")
    print(f"Cancellations seen: {result.cancellations_seen}")
    print(f"Paper fills: {len(result.fills)}")
    print(f"Filled size: {result.total_filled_size}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect the paper-first Polymarket V3 foundation.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate-config", help="Validate SDK and safety settings without network access.")
    subparsers.add_parser("architecture", help="Show V3 component boundaries.")
    shadow_parser = subparsers.add_parser(
        "shadow-report",
        help="Evaluate a resolved paper-candidate JSONL file without network access.",
    )
    shadow_parser.add_argument("path", type=Path)
    replay_parser = subparsers.add_parser(
        "replay-report",
        help="Replay a recorded maker quote/trade JSONL file without network access.",
    )
    replay_parser.add_argument("path", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "validate-config":
            return validate_config()
        if args.command == "architecture":
            return show_architecture()
        if args.command == "shadow-report":
            return shadow_report(args.path)
        return replay_report(args.path)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
