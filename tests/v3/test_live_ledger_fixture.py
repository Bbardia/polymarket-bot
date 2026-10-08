"""Regression pin: the real live ledger (snapshot 2026-10-08) must replay unlatched.

The live ledger is never committed (the repo is public and ledger rows link to
the trading wallet). On the deployment host the test takes a consistent
sqlite-backup copy of ``data/live-v7/ledger.sqlite`` plus the paired
``live_state.json`` (the resolution validators read it next to the ledger);
elsewhere it is skipped. If a code change makes either durable resolution stop
validating, the live bot would re-latch on its next restart; this test fails first.
"""
import os
import shutil
import sqlite3
from pathlib import Path

import pytest

from src.v3.ledger import EventLedger
from src.v3.streaming import (StreamEventProcessor, _validated_duplicate_batch_replay_resolution)

LIVE = Path(os.environ.get("V3_LIVE_LEDGER_PIN_DIR", Path(__file__).resolve().parents[2] / "data" / "live-v7"))

pytestmark = pytest.mark.skipif(
    not (LIVE / "ledger.sqlite").is_file() or not (LIVE / "live_state.json").is_file(),
    reason="live ledger not present on this host",
)


def load(tmp_path):
    source = sqlite3.connect(f"file:{LIVE / 'ledger.sqlite'}?mode=ro", uri=True)
    target = sqlite3.connect(tmp_path / "ledger.sqlite")
    try:
        source.backup(target)
    finally:
        source.close()
        target.close()
    shutil.copyfile(LIVE / "live_state.json", tmp_path / "live_state.json")
    return EventLedger(tmp_path / "ledger.sqlite")


def test_live_ledger_replays_without_reconciliation_latch(tmp_path):
    ledger = load(tmp_path)
    before = tuple(ledger.events())
    processor = StreamEventProcessor(ledger)
    assert processor.reconciliation_required is False
    assert processor.reconciliation_reasons == []
    # Replay must not append a new latch (e.g. an unresolved submission latch).
    assert tuple(ledger.events()) == before
    required = {row.event_id for row in before if row.event_type == "stream.reconciliation_required"}
    assert len(required) >= 164
    assert required <= processor._resolved_reconciliation_event_ids


def test_both_live_resolutions_validate(tmp_path):
    ledger = load(tmp_path)
    processor = StreamEventProcessor(ledger)
    events = tuple(ledger.events())
    by_id = {row.event_id: row for row in events}
    resolutions = [row for row in events if row.event_type == "stream.reconciliation_resolved"]
    assert [row.payload["resolution"] for row in resolutions] == [
        "verified_legacy_fee_history_reconciliation", "corrected_duplicate_batch_replay_v1"]

    legacy, batch = resolutions
    legacy_ids = processor._validated_legacy_fee_resolution(legacy, events, by_id)
    assert legacy_ids and legacy_ids == frozenset(legacy.payload["resolved_event_ids"])
    batch_ids = _validated_duplicate_batch_replay_resolution(batch, events, set(legacy_ids), ledger)
    assert batch_ids and batch_ids == frozenset(batch.payload["resolved_event_ids"])
