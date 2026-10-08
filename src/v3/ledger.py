"""Append-only SQLite event ledger for order/account reconstruction."""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterator, Mapping


@dataclass(frozen=True)
class LedgerEvent:
    event_id: str
    event_type: str
    occurred_at: str
    payload: Mapping[str, Any]

    @classmethod
    def create(
        cls,
        event_type: str,
        payload: Mapping[str, Any],
        *,
        event_id: str | None = None,
        occurred_at: str | None = None,
    ) -> "LedgerEvent":
        return cls(
            event_id=event_id or str(uuid.uuid4()),
            event_type=event_type,
            occurred_at=occurred_at or datetime.now(timezone.utc).isoformat(),
            payload=dict(payload),
        )


class EventLedger:
    def __init__(self, path: str | Path) -> None:
        self._in_memory = str(path) == ":memory:"
        self._memory_connection: sqlite3.Connection | None = None
        self.path = Path(path)
        if not self._in_memory:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        if self._in_memory:
            if self._memory_connection is None:
                connection = sqlite3.connect(":memory:")
                connection.execute("PRAGMA synchronous=FULL")
                self._memory_connection = connection
            return self._memory_connection
        connection = sqlite3.connect(self.path)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )

    def append(self, event: LedgerEvent) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO events(event_id, event_type, occurred_at, payload_json)
                VALUES (?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.event_type,
                    event.occurred_at,
                    json.dumps(
                        dict(event.payload),
                        sort_keys=True,
                        separators=(",", ":"),
                        default=_json_default,
                    ),
                ),
            )
            return cursor.rowcount == 1

    def append_if_unchanged(self, event: LedgerEvent, *, event_count: int, last_event_id: str) -> None:
        """Append only if the exact append-only tip remains unchanged under a write lock."""
        if type(event_count) is not int or event_count <= 0 or not isinstance(last_event_id, str):
            raise ValueError("valid expected ledger tip is required")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            count, tip = connection.execute(
                "SELECT COUNT(*), (SELECT event_id FROM events ORDER BY sequence DESC LIMIT 1) FROM events"
            ).fetchone()
            if count != event_count or tip != last_event_id:
                raise ValueError("ledger changed during audited resolution")
            connection.execute(
                "INSERT INTO events(event_id, event_type, occurred_at, payload_json) VALUES (?, ?, ?, ?)",
                (event.event_id, event.event_type, event.occurred_at,
                 json.dumps(dict(event.payload), sort_keys=True, separators=(",", ":"), default=_json_default)),
            )

    def append_batch(self, events: tuple[LedgerEvent, ...]) -> tuple[str, ...]:
        """Insert an all-or-nothing set of unique events in one SQLite transaction."""
        if len({event.event_id for event in events}) != len(events):
            raise ValueError("duplicate event id in batch")
        with self._connect() as connection:
            for event in events:
                connection.execute(
                    """INSERT INTO events(event_id, event_type, occurred_at, payload_json)
                       VALUES (?, ?, ?, ?)""",
                    (event.event_id, event.event_type, event.occurred_at,
                     json.dumps(dict(event.payload), sort_keys=True,
                                separators=(",", ":"), default=_json_default)),
                )
        return tuple(event.event_id for event in events)

    def events(self) -> Iterator[LedgerEvent]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT event_id, event_type, occurred_at, payload_json FROM events ORDER BY sequence"
            ).fetchall()
        for event_id, event_type, occurred_at, payload_json in rows:
            yield LedgerEvent(event_id, event_type, occurred_at, json.loads(payload_json))


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"unsupported ledger payload value: {type(value)!r}")
