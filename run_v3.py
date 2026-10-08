#!/usr/bin/env python3
"""Paper-first CLI for the Polymarket V3 foundation."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import replace
from pathlib import Path

from dotenv import load_dotenv

from src.v3.api import UnifiedPolymarketAPI
from src.v3.config import V3Settings
from src.v3.live_runner import LiveRunnerSettings, kill_live, resolve_live_duplicate_batch, run_live
from src.v3.live_shadow import LiveShadowSettings, run_live_shadow
from src.v3.paper import PaperSettings, paper_status, run_paper
from src.v3.simulation import (
    evaluate_shadow_candidates,
    load_replay_events,
    load_shadow_candidates,
    replay_maker_events,
)

ROOT = Path(__file__).resolve().parent

PAPER_ENV_NAMES = frozenset({
    "PAPER_TRADING",
    "ENABLE_V3_LIVE_TRADING",
    "ENABLE_V3_ACCOUNT_READS",
    "V3_MAX_CAPITAL",
    "V3_RESERVE_FRACTION",
    "V3_STATION_METADATA_PATH",
})
PAPER_FORBIDDEN_ENV_NAMES = frozenset({
    "POLY_PRIVATE_KEY",
    "POLY_FUNDER_ADDRESS",
    "POLY_SIGNER_ADDRESS",
    "POLY_BUILDER_API_KEY",
    "POLY_BUILDER_SECRET",
    "POLY_BUILDER_PASSPHRASE",
})


def load_paper_environment(path: Path) -> None:
    """Load only non-secret paper settings; never export wallet credentials."""
    for name in PAPER_FORBIDDEN_ENV_NAMES:
        os.environ.pop(name, None)
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").lstrip()
        key, separator, remainder = line.partition("=")
        key = key.strip()
        if not separator or not (key in PAPER_ENV_NAMES or key.startswith("V3_PAPER_")):
            continue
        if key in os.environ:
            continue
        value = remainder.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"\"", "'"}:
            value = value[1:-1]
        os.environ[key] = value


def validate_config() -> int:
    load_dotenv(ROOT / ".env", override=False)
    settings = V3Settings.from_env()
    api = UnifiedPolymarketAPI(settings=settings)
    errors = settings.live_client_errors()
    account_errors = settings.account_client_errors()

    print(f"SDK: polymarket-client {api.sdk_version}")
    print("Collateral: pUSD on Polygon")
    if settings.paper_trading:
        print("Mode: PAPER / LIVE CLIENT BLOCKED" if errors else "Mode: INVALID PAPER/LIVE OVERLAP")
    else:
        print("Mode: NOT PAPER / LIVE CLIENT BLOCKED" if errors else "Mode: LIVE CLIENT GATE SATISFIED")
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
    print("  public paper worker -> durable scans, candidates, simulated positions")
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


def paper_status_report() -> int:
    load_paper_environment(ROOT / ".env")
    settings = PaperSettings.from_env(ROOT)
    print(json.dumps(paper_status(settings), sort_keys=True, indent=2))
    return 0


def paper_run(*, cycles: int, interval: float | None) -> int:
    load_paper_environment(ROOT / ".env")
    settings = PaperSettings.from_env(ROOT)
    if interval is not None:
        settings = replace(settings, scan_interval_seconds=interval)
    asyncio.run(run_paper(settings, cycles=cycles))
    return 0


def live_shadow(*, env_file: Path, cycles: int, interval: float | None) -> int:
    """Read-only shadow of live V7 entries; never builds the order client."""
    load_dotenv(env_file, override=False)
    settings = V3Settings.from_env()
    if settings.live_enabled or not settings.paper_trading:
        raise RuntimeError(
            "live-shadow refuses a live-enabled profile: keep ENABLE_V3_LIVE_TRADING=false "
            "and PAPER_TRADING=true"
        )
    shadow = LiveShadowSettings.from_env(ROOT)
    if interval is not None:
        shadow = replace(shadow, scan_interval_seconds=interval)
    print("Account reads: " + (
        "ENABLED (read-only)" if not settings.account_client_errors()
        else "disabled (" + "; ".join(settings.account_client_errors()) + ")"
    ), flush=True)
    asyncio.run(run_live_shadow(settings, shadow, cycles=cycles))
    return 0


def live_shadow_status() -> int:
    load_dotenv(ROOT / ".env.live", override=False)
    status_path = LiveShadowSettings.from_env(ROOT).data_dir / "status.json"
    if not status_path.is_file():
        print(json.dumps({"mode": "LIVE_SHADOW", "state": "not_started"}))
        return 0
    print(status_path.read_text(encoding="utf-8"))
    return 0


LIVE_CREDENTIAL_NAMES = ("POLY_PRIVATE_KEY", "POLY_FUNDER_ADDRESS")


def load_live_environment(profile: Path, secrets: Path) -> None:
    """Load the non-secret live profile, then only the wallet credentials."""
    if not profile.is_file():
        raise RuntimeError(f"live profile not found: {profile}")
    load_dotenv(profile, override=False)
    if secrets.is_file():
        from dotenv import dotenv_values
        values = dotenv_values(secrets)
        for name in LIVE_CREDENTIAL_NAMES:
            value = values.get(name)
            if value and name not in os.environ:
                os.environ[name] = value


def live_run(*, env_file: Path, secrets_file: Path, cycles: int, interval: float | None) -> int:
    """Real-money V7 weather runner; refuses unless every live gate is satisfied."""
    load_live_environment(env_file, secrets_file)
    settings = V3Settings.from_env()
    runner_settings = LiveRunnerSettings.from_env(ROOT)
    if interval is not None:
        runner_settings = replace(
            runner_settings, shadow=replace(runner_settings.shadow, scan_interval_seconds=interval),
        )
    asyncio.run(run_live(settings, runner_settings, cycles=cycles))
    return 0


def live_kill(*, env_file: Path, secrets_file: Path) -> int:
    load_live_environment(env_file, secrets_file)
    result = asyncio.run(kill_live(V3Settings.from_env(), LiveRunnerSettings.from_env(ROOT)))
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2))
    return 0


def live_resolve_duplicate_batch(*, env_file: Path, secrets_file: Path) -> int:
    """Read authenticated histories/snapshot; append only an exact audited latch proof."""
    import subprocess
    unit = "polymarket-v7-live.service"
    def assert_worker_stopped() -> None:
        result = subprocess.run(
            ["systemctl", "--user", "show", unit,
             "-p", "ActiveState", "-p", "UnitFileState", "-p", "WorkingDirectory"],
            capture_output=True, text=True, check=True,
        )
        properties = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        if (properties.get("ActiveState") != "inactive"
                or properties.get("UnitFileState") != "disabled"
                or Path(properties.get("WorkingDirectory", "/")).resolve() != ROOT.resolve()):
            raise RuntimeError("live resolver requires the exact deployed worker inactive and disabled")
    assert_worker_stopped()
    load_live_environment(env_file, secrets_file)
    result = asyncio.run(resolve_live_duplicate_batch(
        V3Settings.from_env(), LiveRunnerSettings.from_env(ROOT),
        assert_worker_stopped=assert_worker_stopped,
    ))
    print(json.dumps(result, sort_keys=True))
    return 0


def live_status(*, env_file: Path) -> int:
    if env_file.is_file():
        load_dotenv(env_file, override=False)
    status_path = LiveRunnerSettings.from_env(ROOT).shadow.data_dir / "status.json"
    if not status_path.is_file():
        print(json.dumps({"mode": "LIVE", "state": "not_started"}))
        return 0
    print(status_path.read_text(encoding="utf-8"))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Operate the paper-first Polymarket V3 foundation.")
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
    paper_parser = subparsers.add_parser(
        "paper-run",
        help="Run the public-data-only paper worker; zero cycles means continuous.",
    )
    paper_parser.add_argument(
        "--cycles", type=int, default=0,
        help="Number of scan cycles; default 0 runs continuously.",
    )
    paper_parser.add_argument(
        "--interval", type=float,
        help="Override seconds between scans.",
    )
    subparsers.add_parser(
        "paper-status",
        help="Show local paper-worker health without network or account access.",
    )
    live_shadow_parser = subparsers.add_parser(
        "live-shadow",
        help="Read-only live shadow of V7 entries; logs intents, never submits.",
    )
    live_shadow_parser.add_argument("--env-file", type=Path, default=ROOT / ".env.live")
    live_shadow_parser.add_argument("--cycles", type=int, default=0)
    live_shadow_parser.add_argument("--interval", type=float)
    subparsers.add_parser("live-shadow-status", help="Show the last live-shadow status.")
    for name, help_text in (
        ("live-run", "REAL MONEY: run the gated V7 weather maker runner."),
        ("live-kill", "REAL MONEY: latch the kill switch and cancel all open account orders."),
        ("live-status", "Show the last live-runner status without network access."),
        ("live-resolve-duplicate-batch", "Audit authenticated history and resolve one exact duplicate-import latch; no orders."),
    ):
        live_parser = subparsers.add_parser(name, help=help_text)
        live_parser.add_argument("--env-file", type=Path, default=ROOT / ".env.live")
        if name != "live-status":
            live_parser.add_argument("--secrets-file", type=Path, default=ROOT / ".env")
        if name == "live-run":
            live_parser.add_argument("--cycles", type=int, default=0)
            live_parser.add_argument("--interval", type=float)
    args = parser.parse_args()
    try:
        if args.command == "validate-config":
            return validate_config()
        if args.command == "architecture":
            return show_architecture()
        if args.command == "shadow-report":
            return shadow_report(args.path)
        if args.command == "replay-report":
            return replay_report(args.path)
        if args.command == "paper-status":
            return paper_status_report()
        if args.command == "live-shadow":
            return live_shadow(env_file=args.env_file, cycles=args.cycles, interval=args.interval)
        if args.command == "live-shadow-status":
            return live_shadow_status()
        if args.command == "live-run":
            return live_run(env_file=args.env_file, secrets_file=args.secrets_file,
                            cycles=args.cycles, interval=args.interval)
        if args.command == "live-kill":
            return live_kill(env_file=args.env_file, secrets_file=args.secrets_file)
        if args.command == "live-resolve-duplicate-batch":
            return live_resolve_duplicate_batch(env_file=args.env_file, secrets_file=args.secrets_file)
        if args.command == "live-status":
            return live_status(env_file=args.env_file)
        return paper_run(cycles=args.cycles, interval=args.interval)
    except (OSError, RuntimeError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
