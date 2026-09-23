from __future__ import annotations

import asyncio
import json
import os
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.v3.paper_mirror import (
    MIRROR_POLICIES,
    PaperExitMirror,
    mirror_settings_for_policy,
    normalize_source_trade,
    read_source_trade_rows,
    rebased_paper_state,
)
from src.v3.paper import PaperSettings
from src.v3.paper_weather import WeatherPaperPolicy


def directional_trade(**overrides):
    row = {
        "audit_id": "source-trade-1",
        "candidate_id": "source-trade-1",
        "paper_executed": True,
        "public_data_only": True,
        "strategy": "weather_directional",
        "event_key": "weather:city:2026-09-24",
        "condition_id": "condition-1",
        "market_id": "market-1",
        "question": "Will it be warm?",
        "side": "NO",
        "token_id": "token-no-1",
        "shares": "5",
        "all_in_cost": "2.10",
        "fee_rate": "0.05",
        "model_probability": "0.72",
        "city": "city",
        "target_date": "2026-09-24",
        "lead_days": 1,
        "provider_probabilities": {"p1": "0.70", "p2": "0.74"},
        "scanned_at": "2026-09-23T10:00:00+00:00",
    }
    row.update(overrides)
    return row


def test_read_source_trade_rows_skips_unterminated_tail_without_repair(tmp_path):
    path = tmp_path / "paper_trades.jsonl"
    complete = json.dumps(directional_trade(), sort_keys=True).encode() + b"\n"
    tail = b'{"audit_id":"torn"'
    path.write_bytes(complete + tail)

    rows, offset = read_source_trade_rows(path, 0)

    assert [row["audit_id"] for row in rows] == ["source-trade-1"]
    assert offset == len(complete)
    assert path.read_bytes() == complete + tail


def test_read_source_trade_rows_resumes_at_byte_offset(tmp_path):
    path = tmp_path / "paper_trades.jsonl"
    first = json.dumps(directional_trade(), sort_keys=True).encode() + b"\n"
    second = json.dumps(directional_trade(audit_id="source-trade-2"), sort_keys=True).encode() + b"\n"
    path.write_bytes(first + second)

    rows, offset = read_source_trade_rows(path, len(first))

    assert [row["audit_id"] for row in rows] == ["source-trade-2"]
    assert offset == len(first) + len(second)


def test_normalize_source_trade_accepts_only_public_executed_directional_rows():
    key, position = normalize_source_trade(directional_trade())

    assert key == "condition-1"
    assert position["strategy"] == "weather_directional"
    assert position["side"] == "NO"
    assert position["token_id"] == "token-no-1"
    assert position["shares"] == "5"
    assert position["all_in_cost"] == "2.10"
    assert position["source_audit_id"] == "source-trade-1"
    assert normalize_source_trade(directional_trade(public_data_only=False)) is None
    assert normalize_source_trade(directional_trade(paper_executed=False)) is None


def test_normalize_source_trade_preserves_ladder_legs_as_one_paired_basket():
    row = {
        "audit_id": "ladder-audit",
        "candidate_id": "ladder-audit",
        "paper_executed": True,
        "public_data_only": True,
        "strategy": "weather_ladder",
        "event_key": "weather:city:2026-09-24",
        "scanned_at": "2026-09-23T10:00:00+00:00",
        "paper_cash_after": "35.0",
        "ladder": {
            "event_key": "weather:city:2026-09-24",
            "shares": "5",
            "total_cost": "2.50",
            "cluster_probability": "0.80",
            "legs": [
                {"condition_id": "c1", "market_id": "m1", "token_id": "t1", "shares": "5", "all_in_cost": "0.50"},
                {"condition_id": "c2", "market_id": "m2", "token_id": "t2", "shares": "5", "all_in_cost": "1.00"},
                {"condition_id": "c3", "market_id": "m3", "token_id": "t3", "shares": "5", "all_in_cost": "1.00"},
            ],
        },
    }

    key, position = normalize_source_trade(row)

    assert key == "ladder-audit"
    assert position["strategy"] == "weather_ladder"
    assert position["all_in_cost"] == "2.50"
    assert [leg["condition_id"] for leg in position["legs"]] == ["c1", "c2", "c3"]


def test_rebased_paper_state_keeps_open_inventory_but_excludes_pre_cutover_pnl():
    source = {
        "started_at": "2026-09-01T00:00:00+00:00",
        "initial_cash": "37.50",
        "cash": "31.00",
        "realized_pnl": "4.50",
        "realized_exit_pnl": "1.00",
        "realized_settlement_pnl": "3.50",
        "total_paper_trades": 8,
        "total_paper_exits": 2,
        "open_positions": {
            "condition-1": {
                "strategy": "weather_directional",
                "all_in_cost": "2.00",
                "shares": "5",
                "condition_id": "condition-1",
                "market_id": "market-1",
                "token_id": "token-1",
                "side": "YES",
            }
        },
        "traded_conditions": ["condition-1"],
    }

    state = rebased_paper_state(source, started_at="2026-09-23T10:00:00+00:00")

    assert Decimal(state["initial_cash"]) == Decimal("33.00")
    assert Decimal(state["cash"]) == Decimal("31.00")
    assert state["open_positions"] == source["open_positions"]
    assert state["realized_pnl"] == "0"
    assert state["realized_exit_pnl"] == "0"
    assert state["realized_settlement_pnl"] == "0"
    assert state["total_paper_trades"] == 0
    assert state["total_paper_exits"] == 0
    assert state["started_at"] == "2026-09-23T10:00:00+00:00"


def test_mirror_policy_matrix_is_explicit_and_weather_api_free():
    assert set(MIRROR_POLICIES) == {"hold", "full-25", "full-15", "full-10", "hybrid-25"}
    assert MIRROR_POLICIES["hold"].early_exit_enabled is False
    assert MIRROR_POLICIES["full-15"].target_return == Decimal("0.15")
    assert MIRROR_POLICIES["full-10"].target_return == Decimal("0.10")
    assert MIRROR_POLICIES["hybrid-25"].hybrid_enabled is True
    assert MIRROR_POLICIES["hybrid-25"].hybrid_fraction == Decimal("0.75")
    assert MIRROR_POLICIES["hybrid-25"].runner_target_return == Decimal("0.50")


def test_mirror_settings_disable_all_weather_acquisition_and_independent_entries(tmp_path):
    base = PaperSettings(
        data_dir=tmp_path,
        paper_trading=True,
        live_enabled=False,
        account_reads_enabled=False,
        entries_enabled=True,
        weather_policy=WeatherPaperPolicy(enabled=True, observations_enabled=True),
    )

    settings = mirror_settings_for_policy(base, MIRROR_POLICIES["full-15"], tmp_path / "arm")

    assert settings.data_dir == tmp_path / "arm"
    assert settings.entries_enabled is False
    assert settings.complete_set_enabled is False
    assert settings.weather_policy.enabled is False
    assert settings.weather_policy.observations_enabled is False
    assert settings.early_exit_enabled is True
    assert settings.early_exit_target_return == Decimal("0.15")
    assert settings.live_enabled is False
    assert settings.account_reads_enabled is False
    assert not settings.safety_errors()


def test_mirror_replays_new_v7_entry_into_all_arms_without_weather_scanning(tmp_path):
    from src.v3.paper import PaperSettings, PaperState, PaperStore
    from src.v3.paper_weather import WeatherPaperPolicy

    source = tmp_path / "v7-source"
    source_store = PaperStore(source)
    source_state = PaperState.new(Decimal("37.50"))
    source_store.save_state(source_state)
    source_store.write_status({
        "running": True,
        "healthy": True,
        "errors_this_cycle": 0,
        "cycle": 1,
        "last_scan_at": datetime.now(timezone.utc).isoformat(),
        "data_dir": str(source.resolve()),
        "live_trading_enabled": False,
        "account_reads_enabled": False,
    })
    (source / "worker.pid").write_text(f"{os.getpid()}\n")
    (source / "paper_trades.jsonl").write_bytes(b"")

    class PublicClient:
        def __init__(self):
            self.book_requests = 0

        async def get_order_books(self, *, token_ids):
            self.book_requests += 1
            return ()

        async def get_market(self, *, id):
            raise AssertionError("new entries must not be settlement-queried in the same mirror cycle")

    client = PublicClient()
    settings = PaperSettings(
        data_dir=tmp_path / "mirror",
        paper_trading=True,
        live_enabled=False,
        account_reads_enabled=False,
        entries_enabled=False,
        complete_set_enabled=False,
        weather_policy=WeatherPaperPolicy(enabled=False),
    )

    async def run_once():
        mirror = PaperExitMirror(settings=settings, source_data_dir=source, client=client)
        await mirror.initialize()
        with (source / "paper_trades.jsonl").open("ab") as handle:
            handle.write(json.dumps(directional_trade(), sort_keys=True).encode() + b"\n")
        result = await mirror.run_cycle()
        return mirror, result

    mirror, result = asyncio.run(run_once())

    assert result["weather_api_used"] is False
    assert result["source_entries_mirrored_this_cycle"] == 1
    assert set(result["arms"]) == set(MIRROR_POLICIES)
    for name, arm in mirror.arms.items():
        rows = arm.store.read_records(arm.store.trades_path)
        assert len(rows) == 1
        assert rows[0]["mirror_source_audit_id"] == "source-trade-1"
        assert rows[0]["paired_entry"] is True
        assert arm.settings.weather_policy.enabled is False
        assert arm.settings.early_exit_enabled == MIRROR_POLICIES[name].early_exit_enabled
    assert client.book_requests == len(MIRROR_POLICIES)
