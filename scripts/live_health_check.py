#!/usr/bin/env python3
"""Health alerts for the live V7 bot (stdlib only).

Run every 5 minutes by ``deploy/systemd/polymarket-health-check.timer``. It reads
``data/live-v7/status.json`` and ``errors.jsonl`` (read-only), asks systemd whether
``polymarket-v7-live`` is active, and mails problems through Resend.

De-duplication state lives in ``data/alerts-state.json`` (never under data/live-v7):

* stale cycles, a latched lifecycle reconciliation, an inactive unit and an
  unreadable status file are mailed on the first check that sees them;
* ``unhealthy``, ``entry_blocked`` and new ``errors.jsonl`` lines are debounced:
  they are mailed only when seen on 2 consecutive checks (about 10 minutes), so a
  single transient failed cycle does not page;
* a problem is re-mailed at most every 6 hours while it persists, and a
  "recovered" mail is sent only when the status explicitly shows it cleared
  (fields present and good). Missing fields keep the previous state. New
  errors.jsonl lines are events: no "recovered" mail when they stop.

Staleness is measured from the last successful ``cycle_finished_at`` (remembered
in the alert state, because a failed cycle writes a status without it), falling
back to the status file's mtime. ``--dry-run`` prints the mail and saves no state.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENV_FILE = Path("/home/rasbardi/polymarket-bot/.env.alerts")
DEFAULT_LIVE_ENV_FILE = Path("/home/rasbardi/polymarket-bot/.env.live")
DEFAULT_DATA_DIR = ROOT / "data" / "live-v7"
DEFAULT_STATE_FILE = ROOT / "data" / "alerts-state.json"
UNIT = "polymarket-v7-live"
RESEND_URL = "https://api.resend.com/emails"
REALERT_AFTER = timedelta(hours=6)
DEFAULT_SCAN_INTERVAL = 300.0
SCAN_INTERVAL_KEY = "V3_LIVE_SCAN_INTERVAL_SECONDS"
DEBOUNCED = frozenset({"unhealthy", "entry_blocked", "errors"})
DEBOUNCE_CHECKS = 2
MAX_ERROR_LINES_IN_MAIL = 20
MAX_ERROR_LINE_CHARS = 600


@dataclass(frozen=True)
class Problem:
    key: str
    message: str


@dataclass(frozen=True)
class Observation:
    """What one check saw: problems, keys explicitly shown good, last good cycle."""
    problems: list[Problem]
    cleared: set[str]
    last_cycle_finished_at: datetime | None = None


@dataclass(frozen=True)
class Mail:
    subject: str
    body: str


Sender = Callable[[Mail], None]


def _env_value(raw: str) -> str:
    """Value part of KEY=VALUE: surrounding quotes removed, unquoted ` # comment` dropped."""
    value = raw.strip()
    if value[:1] in ("'", '"'):
        end = value.find(value[0], 1)
        if end > 0:
            return value[1:end]
    for index, char in enumerate(value):
        if char == "#" and (index == 0 or value[index - 1] in " \t"):
            value = value[:index]
            break
    return value.strip()


def _env_lines(path: Path):
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, _, value = line.partition("=")
        yield key.strip(), value


def load_env_file(path: Path) -> dict[str, str]:
    """Parse simple KEY=VALUE lines; values are never logged."""
    return {key: _env_value(value) for key, value in _env_lines(path)}


def read_env_key(path: Path, name: str) -> str | None:
    """Return only ``name`` from an env file; other values are never parsed or kept."""
    found = None
    for key, value in _env_lines(path):
        if key == name:
            found = _env_value(value)
    return found


def scan_interval_from_live_env(path: Path) -> float:
    """V3_LIVE_SCAN_INTERVAL_SECONDS from the live profile, else 300. Prints no values."""
    try:
        raw = read_env_key(path, SCAN_INTERVAL_KEY)
    except OSError as exc:
        print(f"cannot read {path} ({type(exc).__name__}); using {SCAN_INTERVAL_KEY}="
              f"{DEFAULT_SCAN_INTERVAL:g}", file=sys.stderr)
        return DEFAULT_SCAN_INTERVAL
    if not raw:
        return DEFAULT_SCAN_INTERVAL
    try:
        value = float(raw)
    except ValueError:
        value = -1.0
    if not (value > 0 and value != float("inf")):
        print(f"{SCAN_INTERVAL_KEY} in {path} is not a positive number; using "
              f"{DEFAULT_SCAN_INTERVAL:g}", file=sys.stderr)
        return DEFAULT_SCAN_INTERVAL
    return value


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def unit_state(unit: str = UNIT) -> str:
    try:
        result = subprocess.run(
            ["systemctl", "--user", "is-active", unit],
            capture_output=True, text=True, timeout=30, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"unknown ({type(exc).__name__})"
    return result.stdout.strip() or f"unknown (exit {result.returncode})"


def check_status(status_path: Path, now: datetime, max_age_seconds: float,
                 unit_active_state: str, last_good_cycle: datetime | None = None) -> Observation:
    """Classify each condition as a problem, explicitly cleared, or unknown (neither)."""
    problems: list[Problem] = []
    cleared: set[str] = set()
    if unit_active_state == "active":
        cleared.add("unit_inactive")
    elif unit_active_state not in {"activating", "reloading"}:
        # A restart passes through "activating" (incl. the DNS wait); a unit stuck
        # there is still caught by the stale-cycle check.
        problems.append(Problem("unit_inactive", f"systemctl --user is-active {UNIT}: {unit_active_state}"))

    status: Mapping | None = None
    try:
        loaded = json.loads(status_path.read_text(encoding="utf-8"))
        if not isinstance(loaded, Mapping):
            raise ValueError("status.json is not an object")
        status = loaded
        cleared.add("status_unreadable")
    except (OSError, ValueError) as exc:
        problems.append(Problem("status_unreadable", f"cannot read {status_path}: {type(exc).__name__}: {exc}"))

    finished = _parse_time(status.get("cycle_finished_at")) if status is not None else None
    if finished is not None and (last_good_cycle is None or finished > last_good_cycle):
        last_good_cycle = finished
    if last_good_cycle is not None:
        reference, source = last_good_cycle, "last successful cycle finished"
    else:
        try:
            reference = datetime.fromtimestamp(status_path.stat().st_mtime, timezone.utc)
            source = "status.json last written (no successful cycle seen yet)"
        except OSError:
            reference, source = None, ""
    if reference is not None:
        age = (now - reference).total_seconds()
        if age > max_age_seconds:
            problems.append(Problem("stale", f"{source} {int(age)}s ago at {reference.isoformat()} "
                                             f"(threshold {int(max_age_seconds)}s)"))
        elif finished is not None and finished == last_good_cycle:
            cleared.add("stale")

    if status is not None:
        if "healthy" in status:
            if status["healthy"] is True:
                cleared.add("unhealthy")
            else:
                problems.append(Problem("unhealthy", f"healthy={status.get('healthy')!r}, "
                                                     f"last_error={status.get('last_error')!r}"))
        reconciliation = status.get("reconciliation")
        if isinstance(reconciliation, Mapping) and "lifecycle_reconciliation_required" in reconciliation:
            if reconciliation["lifecycle_reconciliation_required"] is False:
                cleared.add("reconciliation")
            else:
                problems.append(Problem("reconciliation", "lifecycle_reconciliation_required="
                                        f"{reconciliation['lifecycle_reconciliation_required']!r}, reasons="
                                        f"{reconciliation.get('lifecycle_reconciliation_reasons')!r}"))
        if "entry_block_reason" in status:
            if status["entry_block_reason"]:
                problems.append(Problem("entry_blocked", f"entry_block_reason={status['entry_block_reason']!r}"))
            else:
                cleared.add("entry_blocked")
    return Observation(problems, cleared, last_good_cycle)


def read_new_error_lines(errors_path: Path, state: Mapping) -> tuple[list[str], dict]:
    """Return lines appended since the last run and the updated cursor.

    The first run records a baseline without alerting. A replaced file (new
    inode) is new in its entirety; a shrunk file resets the cursor.
    """
    try:
        lines = errors_path.read_text(encoding="utf-8", errors="replace").splitlines()
        inode = errors_path.stat().st_ino
    except FileNotFoundError:
        return [], {"lines": 0, "inode": None}
    lines = [line for line in lines if line.strip()]
    cursor = state.get("errors") if isinstance(state.get("errors"), Mapping) else None
    new_cursor = {"lines": len(lines), "inode": inode}
    if cursor is None:
        return [], new_cursor
    seen = cursor.get("lines", 0)
    if cursor.get("inode") is None or cursor.get("inode") != inode:
        return lines, new_cursor
    if not isinstance(seen, int) or seen > len(lines):
        return [], new_cursor
    return lines[seen:], new_cursor


def plan(observation: Observation, new_errors: list[str], state: Mapping,
         now: datetime) -> tuple[Mail | None, dict]:
    """Decide what to mail and what the next state is (pure function)."""
    active: dict = {key: dict(value) for key, value in (state.get("active") or {}).items()}
    problems = list(observation.problems)
    cleared = set(observation.cleared)
    if new_errors:
        problems.append(Problem("errors", f"{len(new_errors)} new errors.jsonl line(s)"))
    else:
        cleared.add("errors")

    started, repeated, recovered = [], [], []
    for problem in problems:
        entry = active.get(problem.key)
        if entry is None:
            entry = {"since": now.isoformat(), "count": 0, "alerted": False}
        entry["count"] = int(entry.get("count", 0)) + 1
        entry["message"] = problem.message
        if problem.key == "errors":
            pending = list(entry.get("lines", [])) + [line[:MAX_ERROR_LINE_CHARS] for line in new_errors]
            entry["lines"] = pending[-MAX_ERROR_LINES_IN_MAIL:]
            entry["total"] = int(entry.get("total", 0)) + len(new_errors)
        due = DEBOUNCE_CHECKS if problem.key in DEBOUNCED else 1
        last_alert = _parse_time(entry.get("last_alert"))
        if not entry.get("alerted") and entry["count"] >= due:
            started.append((problem, dict(entry)))
            entry.update(alerted=True, last_alert=now.isoformat())
        elif entry.get("alerted") and (last_alert is None or now - last_alert >= REALERT_AFTER):
            repeated.append((problem, dict(entry)))
            entry["last_alert"] = now.isoformat()
        if problem.key == "errors" and entry.get("last_alert") == now.isoformat():
            entry["lines"], entry["total"] = [], 0
        active[problem.key] = entry
    current = {problem.key for problem in problems}
    for key in list(active):
        if key in cleared and key not in current:
            entry = active.pop(key)
            # errors.jsonl lines are events, not a state: no "recovered" mail for them.
            if entry.get("alerted") and key != "errors":
                recovered.append((key, entry))

    next_state = {**state, "active": active}
    if observation.last_cycle_finished_at is not None:
        next_state["last_cycle_finished_at"] = observation.last_cycle_finished_at.isoformat()
    if not (started or repeated or recovered):
        return None, next_state

    def describe(problem: Problem, entry: Mapping) -> str:
        text = f"- [{problem.key}] since {entry.get('since')}: {problem.message}"
        if problem.key == "errors" and entry.get("lines"):
            text += f" ({entry.get('total')} since last mail, latest shown)\n" + "\n".join(entry["lines"])
        return text

    sections: list[str] = []
    if started:
        sections.append("NEW PROBLEMS:\n" + "\n".join(describe(p, e) for p, e in started))
    if repeated:
        sections.append("STILL FAILING (re-alert every 6h):\n" + "\n".join(describe(p, e) for p, e in repeated))
    if recovered:
        sections.append("RECOVERED:\n" + "\n".join(
            f"- [{key}] (since {entry.get('since')}) {entry.get('message')}" for key, entry in recovered))
    mailed = {p.key for p, _ in started} | {p.key for p, _ in repeated}
    still_active = sorted(key for key, entry in active.items() if entry.get("alerted") and key not in mailed)
    if still_active:
        sections.append("Also still active (already alerted): " + ", ".join(still_active))

    kind = "ALERT" if (started or repeated) else "RECOVERED"
    keys = [p.key for p, _ in started] + [p.key for p, _ in repeated] + [key for key, _ in recovered]
    subject = f"[polymarket-v7 {kind}] {', '.join(keys)}"
    body = (f"Host {os.uname().nodename}, checked {now.isoformat()}\n\n" + "\n\n".join(sections)
            + f"\n\nInspect: systemctl --user status {UNIT}; "
              "cat ~/polymarket-bot/data/live-v7/status.json\n")
    return Mail(subject, body), next_state


def resend_sender(api_key: str, sender: str, recipients: list[str]) -> Sender:
    def send(mail: Mail) -> None:
        payload = json.dumps({"from": sender, "to": recipients, "subject": mail.subject,
                              "text": mail.body}).encode("utf-8")
        request = urllib.request.Request(RESEND_URL, data=payload, method="POST", headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            # Resend sits behind Cloudflare, which rejects urllib's default agent.
            "User-Agent": "polymarket-bot-health-check/1.0",
        })
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read()[:300].decode("utf-8", errors="replace")
            raise RuntimeError(f"Resend HTTP {exc.code}: {detail}") from None
    return send


def print_sender(mail: Mail) -> None:
    print(f"Subject: {mail.subject}\n\n{mail.body}")


def load_state(path: Path) -> dict:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return state if isinstance(state, dict) else {}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def run(*, data_dir: Path, state_file: Path, sender: Sender, now: datetime,
        max_age_seconds: float, unit_active_state: str, persist: bool = True) -> Mail | None:
    """One check. State is saved only after a successful send (or no send)."""
    state = load_state(state_file)
    observation = check_status(data_dir / "status.json", now, max_age_seconds, unit_active_state,
                               _parse_time(state.get("last_cycle_finished_at")))
    new_errors, cursor = read_new_error_lines(data_dir / "errors.jsonl", state)
    mail, next_state = plan(observation, new_errors, state, now)
    next_state["errors"] = cursor
    next_state["checked_at"] = now.isoformat()
    if mail is not None:
        sender(mail)  # raises on failure, so state is not advanced and the next run retries
    if persist:
        save_state(state_file, next_state)
    return mail


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE,
                        help="Resend credentials: RESEND_API_KEY, ALERT_FROM, ALERT_TO")
    parser.add_argument("--live-env-file", type=Path, default=DEFAULT_LIVE_ENV_FILE,
                        help=f"live profile; only {SCAN_INTERVAL_KEY} is read from it")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE)
    parser.add_argument("--scan-interval", type=float, default=None,
                        help="seconds; overrides the live profile (stale threshold is 2x)")
    parser.add_argument("--dry-run", action="store_true", help="print the mail instead of sending; state is not saved")
    args = parser.parse_args(argv)

    env: dict[str, str] = {}
    if args.env_file.exists():
        env = load_env_file(args.env_file)
    elif not args.dry_run:
        print(f"env file {args.env_file} not found", file=sys.stderr)
        return 2
    lookup = lambda name: os.environ.get(name) or env.get(name, "")  # noqa: E731
    interval = args.scan_interval or scan_interval_from_live_env(args.live_env_file)

    if args.dry_run:
        sender: Sender = print_sender
    else:
        api_key, mail_from = lookup("RESEND_API_KEY"), lookup("ALERT_FROM")
        recipients = [item.strip() for item in lookup("ALERT_TO").split(",") if item.strip()]
        missing = [name for name, value in (("RESEND_API_KEY", api_key), ("ALERT_FROM", mail_from),
                                            ("ALERT_TO", recipients)) if not value]
        if missing:
            print(f"missing in {args.env_file}: {', '.join(missing)}", file=sys.stderr)
            return 2
        sender = resend_sender(api_key, mail_from, recipients)

    try:
        mail = run(data_dir=args.data_dir, state_file=args.state_file, sender=sender,
                   now=datetime.now(timezone.utc), max_age_seconds=2 * interval,
                   unit_active_state=unit_state(), persist=not args.dry_run)
    except (RuntimeError, OSError) as exc:
        print(f"alert delivery failed: {exc}", file=sys.stderr)
        return 1
    if mail is None:
        print(f"ok: nothing to report (stale threshold {int(2 * interval)}s)")
    elif not args.dry_run:
        print(f"sent: {mail.subject}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
