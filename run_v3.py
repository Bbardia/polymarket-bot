#!/usr/bin/env python3
"""Non-running CLI for the Polymarket V3 foundation."""

from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from src.v3.api import UnifiedPolymarketAPI
from src.v3.config import V3Settings

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
    print("  Decimal math -> fee/VWAP/uncertainty/Kelly")
    print("  hard risk engine -> post-only short-TTL intents")
    print("  paper evaluators -> weather and complete sets")
    print("  execution authorization -> disabled by default")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect the paper-first Polymarket V3 foundation.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate-config", help="Validate SDK and safety settings without network access.")
    subparsers.add_parser("architecture", help="Show V3 component boundaries.")
    args = parser.parse_args()
    if args.command == "validate-config":
        return validate_config()
    return show_architecture()


if __name__ == "__main__":
    raise SystemExit(main())
