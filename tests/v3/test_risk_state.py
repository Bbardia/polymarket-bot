from datetime import datetime, timedelta, timezone
from decimal import Decimal
import sqlite3

import pytest

from src.v3.risk_state import Baseline, CashFlow, RiskStateStore


def request(**changes):
    values = dict(
        equity=Decimal("12.3400"),
        recorded_at=datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc),
        baseline_id="baseline-001",
        approval_reference="approval:ticket-1",
        evidence_reference="evidence:reconciliation-1",
    )
    values.update(changes)
    return values


def test_store_closes_each_sqlite_connection(tmp_path, monkeypatch):
    original_connect = sqlite3.connect
    connections = []

    class TrackingConnection(sqlite3.Connection):
        closed_by_test = False

        def close(self):
            self.closed_by_test = True
            super().close()

    def tracked_connect(*args, **kwargs):
        connection = original_connect(*args, factory=TrackingConnection, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr("src.v3.risk_state.sqlite3.connect", tracked_connect)
    store = RiskStateStore(tmp_path / "risk.sqlite")
    store.get_baseline()
    assert len(connections) >= 2
    assert all(connection.closed_by_test for connection in connections)


def test_empty_or_uninitialized_store_is_no_go(tmp_path):
    store = RiskStateStore(tmp_path / "risk.sqlite")
    assert store.get_baseline() is None
    assert store.status == "NO-GO"


def test_fresh_store_contains_only_canonical_schema_objects(tmp_path):
    path = tmp_path / "risk.sqlite"
    RiskStateStore(path)
    with sqlite3.connect(path) as db:
        objects = set(db.execute(
            "SELECT type, name FROM sqlite_master "
            "WHERE type IN ('table', 'index', 'view', 'trigger')"
        ))
    assert objects == {
        ("table", "baseline"), ("table", "audit_event"), ("table", "equity_observation"), ("table", "cash_flow"),
        ("index", "equity_observation_fingerprint_idx"),
        ("index", "cash_flow_flow_id_idx"), ("index", "cash_flow_fingerprint_idx"),
        ("trigger", "baseline_no_update"), ("trigger", "baseline_no_delete"),
        ("trigger", "audit_no_update"), ("trigger", "audit_no_delete"),
        ("trigger", "equity_observation_no_update"), ("trigger", "equity_observation_no_delete"),
        ("trigger", "cash_flow_no_update"), ("trigger", "cash_flow_no_delete"),
        ("table", "reservation_event"), ("index", "reservation_id_event_type_idx"),
        ("trigger", "reservation_event_no_update"), ("trigger", "reservation_event_no_delete"),
    }


@pytest.mark.parametrize("changes", [
    {"equity": None}, {"equity": 1}, {"equity": Decimal("NaN")},
    {"equity": Decimal("Infinity")}, {"equity": Decimal("0")},
    {"recorded_at": None}, {"recorded_at": datetime(2026, 1, 1)},
    {"recorded_at": "2026-01-01T00:00:00Z"}, {"baseline_id": ""},
    {"baseline_id": "x" * 257}, {"approval_reference": ""},
    {"evidence_reference": ""}, {"approval_reference": None},
])
def test_invalid_baseline_inputs_rejected(tmp_path, changes):
    store = RiskStateStore(tmp_path / "risk.sqlite")
    with pytest.raises((TypeError, ValueError)):
        store.initialize_baseline(**request(**changes))
    assert store.get_baseline() is None


def test_persists_exact_decimal_and_normalizes_timestamp(tmp_path):
    path = tmp_path / "risk.sqlite"
    store = RiskStateStore(path)
    baseline = store.initialize_baseline(**request(recorded_at=datetime(2026, 1, 2, 5, 4, tzinfo=timezone(timedelta(hours=2)))))
    assert baseline.equity == Decimal("12.3400")
    assert baseline.recorded_at == datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    reopened = RiskStateStore(path).get_baseline()
    assert reopened == baseline
    assert RiskStateStore(path).status == "NO-GO"


def test_exact_duplicate_is_idempotent_but_conflicting_second_baseline_fails(tmp_path):
    store = RiskStateStore(tmp_path / "risk.sqlite")
    first = store.initialize_baseline(**request())
    assert store.initialize_baseline(**request()) == first
    with pytest.raises(ValueError):
        store.initialize_baseline(**request(equity=Decimal("99")))
    with pytest.raises(ValueError):
        store.initialize_baseline(**request(equity=Decimal("12.34000")))
    assert store.get_baseline() == first


def test_baseline_is_immutable_and_schema_version_guarded(tmp_path):
    path = tmp_path / "risk.sqlite"
    store = RiskStateStore(path)
    store.initialize_baseline(**request())
    with sqlite3.connect(path) as db:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("DELETE FROM baseline")
        db.execute("PRAGMA user_version = 999")
    with pytest.raises(ValueError):
        RiskStateStore(path)


def test_audit_event_failure_rolls_back_baseline(tmp_path):
    path = tmp_path / "risk.sqlite"
    store = RiskStateStore(path)
    with sqlite3.connect(path) as db:
        db.execute("CREATE TRIGGER fail_audit BEFORE INSERT ON audit_event "
                   "BEGIN SELECT RAISE(ABORT, 'forced audit failure'); END")
    with pytest.raises(sqlite3.IntegrityError):
        store.initialize_baseline(**request())
    assert store.get_baseline() is None
    assert store.status == "NO-GO"


def test_audit_event_written_atomically_with_baseline(tmp_path):
    path = tmp_path / "risk.sqlite"
    store = RiskStateStore(path)
    store.initialize_baseline(**request())
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0] == 1
        assert db.execute("SELECT baseline_id FROM audit_event").fetchone()[0] == "baseline-001"


def test_versioned_database_with_missing_schema_object_is_rejected(tmp_path):
    path = tmp_path / "corrupt.sqlite"
    RiskStateStore(path)
    with sqlite3.connect(path) as db:
        db.execute("DROP TRIGGER audit_no_delete")
    with pytest.raises(ValueError, match="invalid risk-state schema.*audit_no_delete"):
        RiskStateStore(path)


def test_versioned_database_with_same_name_noop_baseline_trigger_is_rejected(tmp_path):
    path = tmp_path / "corrupt-trigger.sqlite"
    RiskStateStore(path)
    with sqlite3.connect(path) as db:
        db.execute("DROP TRIGGER baseline_no_update")
        db.execute("CREATE TRIGGER baseline_no_update BEFORE UPDATE ON baseline BEGIN SELECT 1; END")
    with pytest.raises(ValueError, match="invalid risk-state schema.*baseline_no_update"):
        RiskStateStore(path)


def test_versioned_database_with_extra_ignore_trigger_is_rejected_before_initialization(tmp_path):
    path = tmp_path / "intercepted-trigger.sqlite"
    RiskStateStore(path)
    with sqlite3.connect(path) as db:
        db.execute("CREATE TRIGGER intercept_baseline BEFORE INSERT ON baseline "
                   "BEGIN SELECT RAISE(IGNORE); END")
    with pytest.raises(ValueError, match="invalid risk-state schema.*intercept_baseline"):
        RiskStateStore(path)
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM baseline").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0] == 0


def test_versioned_database_with_sqlite_like_prefix_ignore_trigger_is_rejected(tmp_path):
    path = tmp_path / "sqlite-prefix-trigger.sqlite"
    RiskStateStore(path)
    with sqlite3.connect(path) as db:
        db.execute("CREATE TRIGGER sqliteX_intercept BEFORE INSERT ON baseline "
                   "BEGIN SELECT RAISE(IGNORE); END")
    with pytest.raises(ValueError, match="invalid risk-state schema.*sqliteX_intercept"):
        RiskStateStore(path)
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM baseline").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0] == 0


def test_forged_reserved_prefix_trigger_in_sqlite_master_is_rejected(tmp_path):
    path = tmp_path / "forged-sqlite-trigger.sqlite"
    RiskStateStore(path)
    with sqlite3.connect(path) as db:
        db.execute("PRAGMA writable_schema = ON")
        db.execute(
            "INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql) "
            "VALUES ('trigger', 'sqlite_evil', 'baseline', 0, "
            "'CREATE TRIGGER sqlite_evil BEFORE INSERT ON baseline "
            "BEGIN SELECT RAISE(IGNORE); END')"
        )
        db.execute("PRAGMA writable_schema = OFF")

    with pytest.raises(ValueError, match="invalid risk-state schema.*sqlite_evil"):
        RiskStateStore(path)

    # Startup must fail before a forged IGNORE trigger can suppress persistence.
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT COUNT(*) FROM baseline").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0] == 0


def test_versioned_database_with_altered_table_definition_is_rejected(tmp_path):
    path = tmp_path / "corrupt-table.sqlite"
    RiskStateStore(path)
    with sqlite3.connect(path) as db:
        db.execute("ALTER TABLE baseline ADD COLUMN unexpected TEXT")
    with pytest.raises(ValueError, match="invalid risk-state schema.*baseline"):
        RiskStateStore(path)


def test_schema_guard_ignores_sql_whitespace_formatting(tmp_path):
    path = tmp_path / "formatted.sqlite"
    RiskStateStore(path)
    with sqlite3.connect(path) as db:
        db.execute("DROP TRIGGER baseline_no_update")
        db.execute("CREATE TRIGGER baseline_no_update BEFORE UPDATE ON baseline BEGIN\n"
                   " SELECT RAISE(ABORT, 'baseline is immutable');\nEND")
    assert RiskStateStore(path).status == "NO-GO"


def test_schema_initialization_failure_rolls_back_and_can_recover(tmp_path, monkeypatch):
    import src.v3.risk_state as risk_state

    path = tmp_path / "retry.sqlite"
    original = risk_state._SCHEMA_STATEMENTS
    monkeypatch.setattr(risk_state, "_SCHEMA_STATEMENTS", original[:-1] + ("INVALID DDL",))
    with pytest.raises(sqlite3.OperationalError):
        RiskStateStore(path)
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 0
        assert db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall() == []
    monkeypatch.setattr(risk_state, "_SCHEMA_STATEMENTS", original)
    assert RiskStateStore(path).status == "NO-GO"


def observation(**changes):
    values = dict(equity=Decimal("12.3400"), observed_at=datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc), source="offline-import", fingerprint="sha256:abc")
    values.update(changes)
    return values


def test_equity_observation_validation(tmp_path):
    store = RiskStateStore(tmp_path / "risk.sqlite")
    for changes in ({"equity": Decimal("NaN")}, {"equity": Decimal("Infinity")}, {"equity": Decimal("0")},
                    {"observed_at": datetime(2026, 1, 1)}, {"source": ""}, {"source": "x" * 257},
                    {"fingerprint": " "}, {"fingerprint": None}):
        with pytest.raises((TypeError, ValueError)):
            store.record_equity_observation(**observation(**changes))
    assert store.get_equity_observations() == []


def test_equity_observation_idempotency_conflict_and_order(tmp_path):
    path = tmp_path / "risk.sqlite"
    store = RiskStateStore(path)
    first = store.record_equity_observation(**observation())
    assert store.record_equity_observation(**observation()) == first
    with pytest.raises(ValueError, match="fingerprint"):
        store.record_equity_observation(**observation(equity=Decimal("12.34")))
    later = store.record_equity_observation(**observation(fingerprint="sha256:def", observed_at=datetime(2026, 1, 3, tzinfo=timezone.utc)))
    same_time = store.record_equity_observation(**observation(fingerprint="sha256:ghi"))
    assert RiskStateStore(path).get_equity_observations() == [first, same_time, later]
    assert RiskStateStore(path).status == "NO-GO"


def test_equity_observation_fingerprint_unique_at_database_level(tmp_path):
    path = tmp_path / "risk.sqlite"
    store = RiskStateStore(path)
    store.record_equity_observation(**observation())
    with sqlite3.connect(path) as db:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("INSERT INTO equity_observation (equity, observed_at, source, fingerprint) "
                       "VALUES (?, ?, ?, ?)", ("12.3400", "2026-01-02T03:04:00+00:00", "offline-import", "sha256:abc"))


def test_equity_observations_are_append_only(tmp_path):
    path = tmp_path / "risk.sqlite"
    store = RiskStateStore(path)
    event = store.record_equity_observation(**observation())
    with sqlite3.connect(path) as db:
        objects = set(db.execute("SELECT type, name FROM sqlite_master WHERE type IN ('table','index','view','trigger')"))
        assert {("table", "equity_observation"), ("trigger", "equity_observation_no_update"), ("trigger", "equity_observation_no_delete")} <= objects
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("UPDATE equity_observation SET equity='99' WHERE event_id=?", (event.event_id,))
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("DELETE FROM equity_observation WHERE event_id=?", (event.event_id,))
    assert store.status == "NO-GO"



def cash_flow(**changes):
    values = dict(flow_id="offline-flow-1", amount=Decimal("2.500"), kind="DEPOSIT",
                  occurred_at=datetime(2026, 1, 2, 5, tzinfo=timezone(timedelta(hours=2))),
                  source="manual-offline", fingerprint="digest-1")
    values.update(changes)
    return values


def test_cash_flow_storage_retry_order_and_no_go(tmp_path):
    path = tmp_path / "risk.sqlite"
    store = RiskStateStore(path)
    first = store.record_cash_flow(**cash_flow())
    later = store.record_cash_flow(**cash_flow(flow_id="offline-flow-2", kind="WITHDRAWAL", fingerprint="digest-2", occurred_at=datetime(2026, 1, 3, tzinfo=timezone.utc)))
    assert isinstance(first, CashFlow) and first.amount == Decimal("2.500")
    assert first.kind == "DEPOSIT" and later.kind == "WITHDRAWAL" and later.amount > 0
    assert first.occurred_at == datetime(2026, 1, 2, 3, tzinfo=timezone.utc)
    assert store.record_cash_flow(**cash_flow()) == first
    assert RiskStateStore(path).get_cash_flows() == [first, later]
    assert store.status == "NO-GO"


@pytest.mark.parametrize("changes", [
    {"flow_id": ""}, {"flow_id": None}, {"flow_id": "x" * 257}, {"source": " "},
    {"source": None}, {"fingerprint": ""}, {"fingerprint": "x" * 257},
    {"amount": Decimal("0")}, {"amount": Decimal("-1")}, {"amount": Decimal("NaN")},
    {"amount": Decimal("Infinity")}, {"amount": 1}, {"kind": "deposit"},
    {"kind": "TRANSFER"}, {"kind": None}, {"occurred_at": datetime(2026, 1, 1)},
    {"occurred_at": None},
])
def test_invalid_cash_flow_inputs_rejected(tmp_path, changes):
    store = RiskStateStore(tmp_path / "risk.sqlite")
    with pytest.raises((TypeError, ValueError)):
        store.record_cash_flow(**cash_flow(**changes))
    assert store.get_cash_flows() == []


@pytest.mark.parametrize("changes", [
    {"flow_id": "offline-flow-other"}, {"fingerprint": "digest-other"},
    {"source": "other"}, {"kind": "WITHDRAWAL"}, {"amount": Decimal("2.50")},
])
def test_cash_flow_identity_conflicts_rejected(tmp_path, changes):
    store = RiskStateStore(tmp_path / "risk.sqlite")
    store.record_cash_flow(**cash_flow())
    with pytest.raises(ValueError):
        store.record_cash_flow(**cash_flow(**changes))


def test_cash_flow_database_indexes_and_immutability(tmp_path):
    path = tmp_path / "risk.sqlite"
    store = RiskStateStore(path)
    event = store.record_cash_flow(**cash_flow())
    with sqlite3.connect(path) as db:
        objects = set(db.execute("SELECT type, name FROM sqlite_master WHERE type IN ('table','index','trigger','view')"))
        assert {("table", "cash_flow"), ("index", "cash_flow_flow_id_idx"),
                ("index", "cash_flow_fingerprint_idx"), ("trigger", "cash_flow_no_update"),
                ("trigger", "cash_flow_no_delete")} <= objects
        for flow_id, fingerprint in ((event.flow_id, "other"), ("other", event.fingerprint)):
            with pytest.raises(sqlite3.IntegrityError):
                db.execute("INSERT INTO cash_flow (flow_id, amount, kind, occurred_at, source, fingerprint) VALUES (?, ?, ?, ?, ?, ?)",
                           (flow_id, "1", "DEPOSIT", "2026-01-01T00:00:00+00:00", "offline", fingerprint))
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("UPDATE cash_flow SET amount='99' WHERE event_id=?", (event.event_id,))
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("DELETE FROM cash_flow WHERE event_id=?", (event.event_id,))


def test_order_reservations_restart_release_and_append_only(tmp_path):
    path=tmp_path / "orders.sqlite"
    store=RiskStateStore(path)
    assert store.reserve_order(reservation_id="a", amount=Decimal("6.00"), max_total=Decimal("10")) == Decimal("6.00")
    store.reserve_order(reservation_id="a", amount=Decimal("6.00"), max_total=Decimal("10"))
    with pytest.raises(ValueError): store.reserve_order(reservation_id="b", amount=Decimal("5"), max_total=Decimal("10"))
    reopened=RiskStateStore(path)
    assert reopened.active_reservation_total() == Decimal("6.00")
    reopened.release_order(reservation_id="a")
    reopened.release_order(reservation_id="a")
    assert RiskStateStore(path).active_reservation_total() == 0
    with pytest.raises(ValueError): reopened.reserve_order(reservation_id="a", amount=Decimal("1"), max_total=Decimal("10"))
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT event_type FROM reservation_event ORDER BY event_id").fetchall() == [("reserve",),("release",)]

def test_reservations_use_exact_decimal_aggregation_at_cap(tmp_path):
    store = RiskStateStore(tmp_path / "risk.sqlite")
    tiny = Decimal("0.00000000000000000000000000001")
    store.reserve_order(reservation_id="one", amount=Decimal("1"), max_total=Decimal("2"))
    store.reserve_order(reservation_id="tiny", amount=tiny, max_total=Decimal("2"))
    assert store.active_reservation_total() == Decimal("1.00000000000000000000000000001")

    capped = RiskStateStore(tmp_path / "capped.sqlite")
    capped.reserve_order(reservation_id="one", amount=Decimal("1"), max_total=Decimal("1"))
    with pytest.raises(ValueError, match="exceed max_total"):
        capped.reserve_order(reservation_id="tiny", amount=tiny, max_total=Decimal("1"))
    assert capped.active_reservation_total() == Decimal("1")


def test_reservation_retry_rejects_tightened_cap(tmp_path):
    store = RiskStateStore(tmp_path / "risk.sqlite")
    store.reserve_order(reservation_id="a", amount=Decimal("6"), max_total=Decimal("10"))
    with pytest.raises(ValueError, match="active reservations exceed max_total"):
        store.reserve_order(reservation_id="a", amount=Decimal("6"), max_total=Decimal("5"))
    assert store.active_reservation_total() == Decimal("6")


def test_order_reservations_race_capacity(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    path=tmp_path / "race.sqlite"
    RiskStateStore(path).reserve_order(reservation_id="seed", amount=Decimal("8"), max_total=Decimal("10"))
    stores=[RiskStateStore(path), RiskStateStore(path)]
    def attempt(i):
        try: stores[i].reserve_order(reservation_id=f"race-{i}", amount=Decimal("2"), max_total=Decimal("10")); return True
        except ValueError: return False
    with ThreadPoolExecutor(2) as pool: results=list(pool.map(attempt, range(2)))
    assert sum(results) == 1
    assert RiskStateStore(path).active_reservation_total() == Decimal("10")
