"""Explicit, immutable baseline storage for offline risk-state initialization.

This records operator-supplied inputs; it does not prove approval or account reconciliation.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
import re
import sqlite3

_SCHEMA_VERSION = 4
_MAX_TEXT = 256
_MAX_RESERVATION_EXPONENT = 128
_MAX_RESERVATION_DIGITS = 128
_RESERVATION_SCHEMA = (
    "CREATE TABLE reservation_event (event_id INTEGER PRIMARY KEY, reservation_id TEXT NOT NULL, event_type TEXT NOT NULL CHECK(event_type IN ('reserve', 'release')), amount TEXT NOT NULL)",
    "CREATE UNIQUE INDEX reservation_id_event_type_idx ON reservation_event (reservation_id, event_type)",
    "CREATE TRIGGER reservation_event_no_update BEFORE UPDATE ON reservation_event BEGIN SELECT RAISE(ABORT, 'reservation events are append-only'); END",
    "CREATE TRIGGER reservation_event_no_delete BEFORE DELETE ON reservation_event BEGIN SELECT RAISE(ABORT, 'reservation events are append-only'); END",
)
_SCHEMA_STATEMENTS = (
    "CREATE TABLE baseline (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), equity TEXT NOT NULL, recorded_at TEXT NOT NULL, baseline_id TEXT NOT NULL, approval_reference TEXT NOT NULL, evidence_reference TEXT NOT NULL)",
    "CREATE TABLE audit_event (event_id INTEGER PRIMARY KEY, event_type TEXT NOT NULL, baseline_id TEXT NOT NULL, equity TEXT NOT NULL, recorded_at TEXT NOT NULL, approval_reference TEXT NOT NULL, evidence_reference TEXT NOT NULL)",
    "CREATE TABLE equity_observation (event_id INTEGER PRIMARY KEY, equity TEXT NOT NULL, observed_at TEXT NOT NULL, source TEXT NOT NULL, fingerprint TEXT NOT NULL)",
    "CREATE UNIQUE INDEX equity_observation_fingerprint_idx ON equity_observation (fingerprint)",
    "CREATE TABLE cash_flow (event_id INTEGER PRIMARY KEY, flow_id TEXT NOT NULL, amount TEXT NOT NULL, kind TEXT NOT NULL, occurred_at TEXT NOT NULL, source TEXT NOT NULL, fingerprint TEXT NOT NULL)",
    "CREATE UNIQUE INDEX cash_flow_flow_id_idx ON cash_flow (flow_id)",
    "CREATE UNIQUE INDEX cash_flow_fingerprint_idx ON cash_flow (fingerprint)",
    "CREATE TRIGGER baseline_no_update BEFORE UPDATE ON baseline BEGIN SELECT RAISE(ABORT, 'baseline is immutable'); END",
    "CREATE TRIGGER baseline_no_delete BEFORE DELETE ON baseline BEGIN SELECT RAISE(ABORT, 'baseline is immutable'); END",
    "CREATE TRIGGER audit_no_update BEFORE UPDATE ON audit_event BEGIN SELECT RAISE(ABORT, 'audit is append-only'); END",
    "CREATE TRIGGER audit_no_delete BEFORE DELETE ON audit_event BEGIN SELECT RAISE(ABORT, 'audit is append-only'); END",
    "CREATE TRIGGER equity_observation_no_update BEFORE UPDATE ON equity_observation BEGIN SELECT RAISE(ABORT, 'equity observations are append-only'); END",
    "CREATE TRIGGER equity_observation_no_delete BEFORE DELETE ON equity_observation BEGIN SELECT RAISE(ABORT, 'equity observations are append-only'); END",
    "CREATE TRIGGER cash_flow_no_update BEFORE UPDATE ON cash_flow BEGIN SELECT RAISE(ABORT, 'cash flows are append-only'); END",
    "CREATE TRIGGER cash_flow_no_delete BEFORE DELETE ON cash_flow BEGIN SELECT RAISE(ABORT, 'cash flows are append-only'); END",
) + _RESERVATION_SCHEMA
def _normalize_sql(sql: str) -> str:
    """Normalize insignificant whitespace while preserving SQL semantics."""
    return " ".join(sql.split())


def _expected_schema() -> dict[tuple[str, str], str]:
    expected = {}
    for statement in _SCHEMA_STATEMENTS:
        match = re.match(r"CREATE\s+(?:(UNIQUE)\s+)?(TABLE|TRIGGER|INDEX)\s+([A-Za-z_][A-Za-z_0-9]*)\b", statement, re.IGNORECASE)
        if match is None:
            raise ValueError("invalid canonical risk-state schema statement")
        object_type = "index" if match.group(1) else match.group(2).lower()
        name = match.group(3)
        key = (object_type, name)
        expected[key] = _normalize_sql(statement)
    return expected


@dataclass(frozen=True)
class EquityObservation:
    event_id: int
    equity: Decimal
    observed_at: datetime
    source: str
    fingerprint: str


@dataclass(frozen=True)
class CashFlow:
    event_id: int
    flow_id: str
    amount: Decimal
    kind: str
    occurred_at: datetime
    source: str
    fingerprint: str


@dataclass(frozen=True)
class Baseline:
    equity: Decimal
    recorded_at: datetime
    baseline_id: str
    approval_reference: str
    evidence_reference: str


class RiskStateStore:
    """SQLite store with a write-once baseline and append-only audit."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        with self._connect() as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 3, _SCHEMA_VERSION):
                raise ValueError(f"unsupported risk-state schema version: {version}")
            if version == 0:
                objects = db.execute(
                    "SELECT type, name FROM sqlite_master "
                    "WHERE type IN ('table', 'index', 'view', 'trigger')"
                ).fetchall()
                if objects:
                    raise ValueError("unversioned risk-state database is not empty")
                try:
                    db.execute("BEGIN IMMEDIATE")
                    for statement in _SCHEMA_STATEMENTS:
                        db.execute(statement)
                    db.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
                    db.commit()
                except Exception:
                    db.rollback()
                    raise
            elif version == 3:
                try:
                    db.execute("BEGIN IMMEDIATE")
                    for statement in _RESERVATION_SCHEMA:
                        db.execute(statement)
                    db.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
                    db.commit()
                except Exception:
                    db.rollback()
                    raise
            self._validate_schema(db)

    @staticmethod
    def _validate_schema(db: sqlite3.Connection) -> None:
        expected = _expected_schema()
        rows = db.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE type IN ('table', 'index', 'view', 'trigger')"
        ).fetchall()
        actual = {(object_type, name): definition for object_type, name, definition in rows}
        invalid = sorted(key for key, definition in expected.items()
                         if key not in actual or actual[key] is None
                         or _normalize_sql(actual[key]) != definition)
        unexpected = sorted(set(actual) - set(expected))
        if invalid or unexpected:
            names = ", ".join(name for _, name in sorted(set(invalid) | set(unexpected)))
            raise ValueError(f"invalid risk-state schema; missing, altered, or unexpected objects: {names}")

    @contextmanager
    def _connect(self):
        """Open one transaction-scoped connection and always close it."""
        db = sqlite3.connect(self.path, timeout=30)
        try:
            db.execute("PRAGMA journal_mode = WAL")
            db.execute("PRAGMA synchronous = FULL")
            with db:
                yield db
        finally:
            db.close()

    @property
    def status(self) -> str:
        """This baseline-only store cannot authorize risk or trading."""
        return "NO-GO"

    def get_baseline(self) -> Baseline | None:
        with self._connect() as db:
            row = db.execute("SELECT equity, recorded_at, baseline_id, approval_reference, "
                             "evidence_reference FROM baseline WHERE singleton = 1").fetchone()
        if row is None:
            return None
        return Baseline(Decimal(row[0]), datetime.fromisoformat(row[1]), row[2], row[3], row[4])

    def record_equity_observation(self, *, equity: Decimal, observed_at: datetime,
                                  source: str, fingerprint: str) -> EquityObservation:
        _validate_observation(equity, observed_at, source, fingerprint)
        timestamp = observed_at.astimezone(timezone.utc).isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT event_id, equity, observed_at, source, fingerprint "
                             "FROM equity_observation WHERE fingerprint = ?", (fingerprint,)).fetchone()
            if row is not None:
                if (row[1], row[2], row[3]) == (str(equity), timestamp, source):
                    return EquityObservation(row[0], Decimal(row[1]), datetime.fromisoformat(row[2]), row[3], row[4])
                raise ValueError("fingerprint already exists with different observation values")
            cursor = db.execute("INSERT INTO equity_observation (equity, observed_at, source, fingerprint) "
                                "VALUES (?, ?, ?, ?)", (str(equity), timestamp, source, fingerprint))
            return EquityObservation(cursor.lastrowid, equity, datetime.fromisoformat(timestamp), source, fingerprint)

    def get_equity_observations(self) -> list[EquityObservation]:
        with self._connect() as db:
            rows = db.execute("SELECT event_id, equity, observed_at, source, fingerprint "
                              "FROM equity_observation ORDER BY observed_at, event_id").fetchall()
        return [EquityObservation(row[0], Decimal(row[1]), datetime.fromisoformat(row[2]), row[3], row[4]) for row in rows]

    def record_cash_flow(self, *, flow_id: str, amount: Decimal, kind: str,
                         occurred_at: datetime, source: str, fingerprint: str) -> CashFlow:
        _validate_cash_flow(flow_id, amount, kind, occurred_at, source, fingerprint)
        timestamp = occurred_at.astimezone(timezone.utc).isoformat()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT event_id, flow_id, amount, kind, occurred_at, source, fingerprint FROM cash_flow WHERE flow_id = ? OR fingerprint = ?", (flow_id, fingerprint)).fetchone()
            if row is not None:
                if (row[1], row[2], row[3], row[4], row[5], row[6]) == (flow_id, str(amount), kind, timestamp, source, fingerprint):
                    return CashFlow(row[0], flow_id, Decimal(row[2]), kind, datetime.fromisoformat(row[4]), source, fingerprint)
                raise ValueError("flow_id or fingerprint already exists with different cash-flow values")
            cursor = db.execute("INSERT INTO cash_flow (flow_id, amount, kind, occurred_at, source, fingerprint) VALUES (?, ?, ?, ?, ?, ?)", (flow_id, str(amount), kind, timestamp, source, fingerprint))
            return CashFlow(cursor.lastrowid, flow_id, amount, kind, datetime.fromisoformat(timestamp), source, fingerprint)

    def reserve_order(self, *, reservation_id: str, amount: Decimal, max_total: Decimal) -> Decimal:
        _validate_reservation(reservation_id, amount, max_total)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute("SELECT event_type, amount FROM reservation_event WHERE reservation_id=? ORDER BY event_id", (reservation_id,)).fetchall()
            if rows:
                if rows == [("reserve", str(amount))]:
                    active_rows = db.execute(
                        "SELECT e.amount FROM reservation_event e "
                        "WHERE e.event_type='reserve' AND NOT EXISTS ("
                        "SELECT 1 FROM reservation_event r "
                        "WHERE r.reservation_id=e.reservation_id AND r.event_type='release')"
                    ).fetchall()
                    active_amounts = _stored_reservation_amounts(active_rows)
                    if _decimal_sum_exceeds(active_amounts, Decimal(0), max_total):
                        raise ValueError("active reservations exceed max_total")
                    return amount
                raise ValueError("reservation ID already used with different or released values")
            rows = db.execute("SELECT e.amount FROM reservation_event e WHERE e.event_type='reserve' AND NOT EXISTS (SELECT 1 FROM reservation_event r WHERE r.reservation_id=e.reservation_id AND r.event_type='release')").fetchall()
            active_amounts = _stored_reservation_amounts(rows)
            if _decimal_sum_exceeds(active_amounts, amount, max_total):
                raise ValueError("reservation would exceed max_total")
            db.execute("INSERT INTO reservation_event (reservation_id,event_type,amount) VALUES (?, 'reserve', ?)", (reservation_id, str(amount)))
        return amount

    def release_order(self, *, reservation_id: str) -> None:
        _validate_reservation_id(reservation_id)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute("SELECT event_type, amount FROM reservation_event WHERE reservation_id=? ORDER BY event_id", (reservation_id,)).fetchall()
            if not rows or rows[0][0] != "reserve":
                raise ValueError("no active reservation")
            if len(rows) == 1:
                db.execute("INSERT INTO reservation_event (reservation_id,event_type,amount) VALUES (?, 'release', ?)", (reservation_id, rows[0][1]))

    def active_reservation_total(self) -> Decimal:
        with self._connect() as db:
            rows = db.execute("SELECT e.amount FROM reservation_event e WHERE e.event_type='reserve' AND NOT EXISTS (SELECT 1 FROM reservation_event r WHERE r.reservation_id=e.reservation_id AND r.event_type='release') ORDER BY e.reservation_id").fetchall()
        return _exact_decimal_sum(_stored_reservation_amounts(rows))

    def get_cash_flows(self) -> list[CashFlow]:
        with self._connect() as db:
            rows = db.execute("SELECT event_id, flow_id, amount, kind, occurred_at, source, fingerprint FROM cash_flow ORDER BY occurred_at, event_id").fetchall()
        return [CashFlow(row[0], row[1], Decimal(row[2]), row[3], datetime.fromisoformat(row[4]), row[5], row[6]) for row in rows]

    def initialize_baseline(self, *, equity: Decimal, recorded_at: datetime,
                            baseline_id: str, approval_reference: str,
                            evidence_reference: str) -> Baseline:
        _validate(equity, recorded_at, baseline_id, approval_reference, evidence_reference)
        timestamp = recorded_at.astimezone(timezone.utc).isoformat()
        candidate = Baseline(equity, datetime.fromisoformat(timestamp), baseline_id,
                             approval_reference, evidence_reference)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT equity, recorded_at, baseline_id, approval_reference, "
                             "evidence_reference FROM baseline WHERE singleton = 1").fetchone()
            if row is not None:
                existing = Baseline(Decimal(row[0]), datetime.fromisoformat(row[1]),
                                    row[2], row[3], row[4])
                exact_duplicate = (
                    row[0] == str(equity)
                    and existing.recorded_at == candidate.recorded_at
                    and existing.baseline_id == baseline_id
                    and existing.approval_reference == approval_reference
                    and existing.evidence_reference == evidence_reference
                )
                if exact_duplicate:
                    return existing
                raise ValueError("a different baseline is already initialized")
            db.execute("INSERT INTO baseline VALUES (1, ?, ?, ?, ?, ?)",
                       (str(equity), timestamp, baseline_id, approval_reference, evidence_reference))
            db.execute("INSERT INTO audit_event (event_type, baseline_id, equity, recorded_at, "
                        "approval_reference, evidence_reference) VALUES (?, ?, ?, ?, ?, ?)",
                       ("baseline_initialized", baseline_id, str(equity), timestamp,
                        approval_reference, evidence_reference))
        return candidate


def _reservation_decimal(value: Decimal, name: str, *, allow_zero: bool = False) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite() or value < 0 or (value == 0 and not allow_zero):
        requirement = "finite and nonnegative" if allow_zero else "finite and positive"
        raise ValueError(f"{name} must be {requirement}")
    parts = value.as_tuple()
    exponent = parts.exponent
    if not isinstance(exponent, int):
        raise ValueError(f"{name} must be a finite decimal")
    if (
        len(parts.digits) > _MAX_RESERVATION_DIGITS
        or abs(exponent) > _MAX_RESERVATION_EXPONENT
        or abs(value.adjusted()) > _MAX_RESERVATION_EXPONENT
    ):
        raise ValueError(f"{name} exceeds exact-arithmetic bounds")


def _stored_reservation_amounts(rows: list[tuple[object, ...]]) -> list[Decimal]:
    amounts = []
    for row in rows:
        try:
            amount = Decimal(str(row[0]))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("stored reservation amount is invalid") from exc
        _reservation_decimal(amount, "stored reservation amount")
        amounts.append(amount)
    return amounts


def _decimal_exponent(value: Decimal) -> int:
    exponent = value.as_tuple().exponent
    if not isinstance(exponent, int):
        raise ValueError("reservation arithmetic requires finite decimals")
    return exponent


def _scaled_integer(value: Decimal, scale_exponent: int) -> int:
    sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        raise ValueError("reservation arithmetic requires finite decimals")
    coefficient = int("".join(str(digit) for digit in digits))
    if sign:
        coefficient = -coefficient
    return coefficient * (10 ** (exponent - scale_exponent))


def _exact_decimal_sum(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal(0)
    for value in values:
        _reservation_decimal(value, "reservation amount")
    scale_exponent = min(_decimal_exponent(value) for value in values)
    total = sum(_scaled_integer(value, scale_exponent) for value in values)
    return Decimal((0, tuple(int(digit) for digit in str(total)), scale_exponent))


def _decimal_sum_exceeds(values: list[Decimal], extra: Decimal, limit: Decimal) -> bool:
    _reservation_decimal(extra, "additional reservation", allow_zero=True)
    _reservation_decimal(limit, "max_total")
    for value in values:
        _reservation_decimal(value, "stored reservation amount")
    all_values = (*values, extra, limit)
    scale_exponent = min(_decimal_exponent(value) for value in all_values)
    total = sum(_scaled_integer(value, scale_exponent) for value in values)
    total += _scaled_integer(extra, scale_exponent)
    return total > _scaled_integer(limit, scale_exponent)


def _validate_reservation_id(reservation_id: str) -> None:
    if not isinstance(reservation_id, str):
        raise TypeError("reservation_id must be a string")
    if not reservation_id.strip() or len(reservation_id) > _MAX_TEXT:
        raise ValueError("reservation_id must be nonempty and bounded")


def _validate_reservation(reservation_id: str, amount: Decimal, max_total: Decimal) -> None:
    _validate_reservation_id(reservation_id)
    _reservation_decimal(amount, "amount")
    _reservation_decimal(max_total, "max_total")


def _validate(equity: Decimal, recorded_at: datetime, baseline_id: str,
              approval_reference: str, evidence_reference: str) -> None:
    if not isinstance(equity, Decimal):
        raise TypeError("equity must be Decimal")
    if not equity.is_finite() or equity <= 0:
        raise ValueError("equity must be finite and positive")
    if not isinstance(recorded_at, datetime):
        raise TypeError("recorded_at must be a datetime")
    if recorded_at.tzinfo is None or recorded_at.utcoffset() is None:
        raise ValueError("recorded_at must be timezone-aware")
    for name, value in (("baseline_id", baseline_id), ("approval_reference", approval_reference),
                        ("evidence_reference", evidence_reference)):
        if not isinstance(value, str):
            raise TypeError(f"{name} must be a string")
        if not value.strip() or len(value) > _MAX_TEXT:
            raise ValueError(f"{name} must be nonempty and at most {_MAX_TEXT} characters")


def _validate_cash_flow(flow_id: str, amount: Decimal, kind: str, occurred_at: datetime,
                        source: str, fingerprint: str) -> None:
    for name, value in (("flow_id", flow_id), ("source", source), ("fingerprint", fingerprint)):
        if not isinstance(value, str):
            raise TypeError(f"{name} must be a string")
        if not value.strip() or len(value) > _MAX_TEXT:
            raise ValueError(f"{name} must be nonempty and at most {_MAX_TEXT} characters")
    if not isinstance(amount, Decimal):
        raise TypeError("amount must be Decimal")
    if not amount.is_finite() or amount <= 0:
        raise ValueError("amount must be finite and positive (magnitude)")
    if not isinstance(kind, str) or kind not in ("DEPOSIT", "WITHDRAWAL"):
        raise ValueError("kind must be exactly DEPOSIT or WITHDRAWAL")
    if not isinstance(occurred_at, datetime):
        raise TypeError("occurred_at must be a datetime")
    if occurred_at.tzinfo is None or occurred_at.utcoffset() is None:
        raise ValueError("occurred_at must be timezone-aware")


def _validate_observation(equity: Decimal, observed_at: datetime, source: str, fingerprint: str) -> None:
    if not isinstance(equity, Decimal):
        raise TypeError("equity must be Decimal")
    if not equity.is_finite() or equity <= 0:
        raise ValueError("equity must be finite and positive")
    if not isinstance(observed_at, datetime):
        raise TypeError("observed_at must be a datetime")
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    for name, value in (("source", source), ("fingerprint", fingerprint)):
        if not isinstance(value, str):
            raise TypeError(f"{name} must be a string")
        if not value.strip() or len(value) > _MAX_TEXT:
            raise ValueError(f"{name} must be nonempty and at most {_MAX_TEXT} characters")
