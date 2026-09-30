from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from src.v3.pnl_eval import build_report


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def row(*, cid: str, side: str, scanned_at: str, **extra) -> dict:
    base = {
        "condition_id": cid,
        "market_id": "market-" + cid,
        "side": side,
        "scanned_at": scanned_at,
        "forecast_decision_at": scanned_at,
        "forecast_issuance_at": None,
        "forecast_snapshot_id": f"session:1:{cid}:{side}",
        "forecast_label": None,
        "forecast_label_finalized_at": None,
        "public_data_only": True,
        "calibrated_probability": "0.8",
        "raw_probability": "0.7",
        "best_ask": "0.6",
        "fee_rate": "0.05",
        "all_in_cost": "3.1",
        "shares": "5",
        "target_date": "2026-09-10",
        "city": "Helsinki",
        "strategy": "weather_directional",
    }
    base.update(extra)
    return base


def make_campaign(tmp_path: Path, *, outcome: int = 1, settlement_overrides: dict | None = None):
    cid = "0xabc"
    scanned_at = "2026-09-09T10:00:00+00:00"
    write_jsonl(tmp_path / "forecast_snapshots.jsonl", [row(cid=cid, side="YES", scanned_at=scanned_at)])
    trade = row(cid=cid, side="YES", scanned_at=scanned_at, paper_executed=True, candidate_id="trade-1")
    trade.pop("forecast_snapshot_id")
    write_jsonl(tmp_path / "paper_trades.jsonl", [trade])
    settlement = {
        "condition_id": cid,
        "market_id": "market-" + cid,
        "strategy": "weather_directional",
        "directional_outcome": outcome,
        "payout": "5" if outcome else "0",
        "realized_pnl": "1.9" if outcome else "-3.1",
        "settled_at": "2026-09-10T12:00:00+00:00",
        "settlement_source": "gamma-public-market-outcome-prices",
        "public_data_only": True,
    }
    settlement.update(settlement_overrides or {})
    write_jsonl(tmp_path / "settlements.jsonl", [settlement])
    write_jsonl(tmp_path / "candidates.jsonl", [
        {"candidate_id": "c1", "condition_id": cid, "paper_reason": "weather paper candidate", "scanned_at": scanned_at},
        {"candidate_id": "c2", "condition_id": cid, "paper_reason": "weather paper position cap reached", "scanned_at": scanned_at},
    ])
    return cid, scanned_at


def test_build_report_scores_only_unique_executed_finalized_decisions(tmp_path):
    make_campaign(tmp_path)
    report = build_report(tmp_path)
    assert report["panel"]["status"] == "insufficient_data"
    assert report["panel"]["n_unique_settled_conditions"] == 1
    assert report["panel"]["snapshot_join_methods"] == {"legacy_condition_side_timestamp": 1}
    assert report["tape_coverage"]["snapshot_rows"] == 1
    assert report["tape_coverage"]["labels_written_into_immutable_snapshots"] == 0
    assert report["scores"]["model_brier"] == "0.04"
    assert report["scores"]["raw_brier"] == "0.09"
    assert report["scores"]["entry_ask_brier"] == "0.16"
    assert report["side_concentration"]["all_executed_directional"]["counts"] == {"YES": 1, "NO": 0}
    assert report["side_concentration"]["settled_score_panel"]["dominant_side_share"] == "1.0000"
    assert report["threshold_policy"]["status"] == "blocked_untraded_candidates_unlabeled"
    assert report["threshold_policy"]["candidate_rows_are_not_independent_outcomes"] is True


def test_report_rejects_snapshot_after_settlement(tmp_path):
    cid, _ = make_campaign(tmp_path)
    late = "2026-09-11T10:00:00+00:00"
    write_jsonl(tmp_path / "forecast_snapshots.jsonl", [row(cid=cid, side="YES", scanned_at=late)])
    write_jsonl(tmp_path / "paper_trades.jsonl", [row(cid=cid, side="YES", scanned_at=late, paper_executed=True, candidate_id="trade-1")])
    with pytest.raises(ValueError, match="decision must precede settlement"):
        build_report(tmp_path)


def test_report_uses_matching_forecast_snapshot_id_when_trade_has_it(tmp_path):
    cid, scanned_at = make_campaign(tmp_path)
    trade = row(cid=cid, side="YES", scanned_at=scanned_at, paper_executed=True,
                candidate_id="trade-1", forecast_snapshot_id=f"session:1:{cid}:YES")
    write_jsonl(tmp_path / "paper_trades.jsonl", [trade])
    report = build_report(tmp_path)
    assert report["panel"]["snapshot_join_methods"] == {"forecast_snapshot_id": 1}


def test_report_rejects_duplicate_snapshot_key(tmp_path):
    make_campaign(tmp_path)
    rows = [json.loads(line) for line in (tmp_path / "forecast_snapshots.jsonl").read_text(encoding="utf-8").splitlines()]
    write_jsonl(tmp_path / "forecast_snapshots.jsonl", [rows[0], dict(rows[0])])
    with pytest.raises(ValueError, match="duplicate exact forecast snapshot key"):
        build_report(tmp_path)


def test_report_rejects_forecast_snapshot_id_mismatch(tmp_path):
    cid, scanned_at = make_campaign(tmp_path)
    trade = row(cid=cid, side="YES", scanned_at=scanned_at, paper_executed=True, candidate_id="trade-1", forecast_snapshot_id="wrong-id")
    write_jsonl(tmp_path / "paper_trades.jsonl", [trade])
    with pytest.raises(ValueError, match="trade forecast_snapshot_id has no snapshot"):
        build_report(tmp_path)


def test_report_rejects_forecast_snapshot_id_that_points_to_another_decision(tmp_path):
    cid, scanned_at = make_campaign(tmp_path)
    existing = _rows = [json.loads(line) for line in (tmp_path / "forecast_snapshots.jsonl").read_text(encoding="utf-8").splitlines()]
    existing.append(row(cid="0xother", side="YES", scanned_at=scanned_at, forecast_snapshot_id="wrong-id"))
    write_jsonl(tmp_path / "forecast_snapshots.jsonl", existing)
    trade = row(cid=cid, side="YES", scanned_at=scanned_at, paper_executed=True,
                candidate_id="trade-1", forecast_snapshot_id="wrong-id")
    write_jsonl(tmp_path / "paper_trades.jsonl", [trade])
    with pytest.raises(ValueError, match="forecast_snapshot_id does not match"):
        build_report(tmp_path)


def test_report_rejects_snapshot_from_different_strategy(tmp_path):
    make_campaign(tmp_path)
    rows = [json.loads(line) for line in (tmp_path / "forecast_snapshots.jsonl").read_text(encoding="utf-8").splitlines()]
    rows[0]["strategy"] = "weather_ladder"
    write_jsonl(tmp_path / "forecast_snapshots.jsonl", rows)
    with pytest.raises(ValueError, match="market/strategy mismatch"):
        build_report(tmp_path)


@pytest.mark.parametrize("field,value", [("market_id", "other-market"), ("strategy", "weather_ladder")])
def test_report_rejects_settlement_identity_mismatch(tmp_path, field, value):
    case_dir = tmp_path / field
    case_dir.mkdir()
    make_campaign(case_dir)
    rows = [json.loads(line) for line in (case_dir / "settlements.jsonl").read_text(encoding="utf-8").splitlines()]
    rows[0][field] = value
    write_jsonl(case_dir / "settlements.jsonl", rows)
    with pytest.raises(ValueError, match="market/strategy mismatch"):
        build_report(case_dir)


def test_report_ignores_unresolved_settlement_rows(tmp_path):
    make_campaign(tmp_path, settlement_overrides={"payout": None, "directional_outcome": None})
    report = build_report(tmp_path)
    assert report["panel"]["n_unique_settled_conditions"] == 0
    assert report["panel"]["status"] == "no_finalized_labels"


def test_report_rejects_unsupported_settlement_source(tmp_path):
    make_campaign(tmp_path, settlement_overrides={"settlement_source": "unverified"})
    with pytest.raises(ValueError, match="unsupported final settlement source"):
        build_report(tmp_path)


def test_report_rejects_duplicate_final_settlement_with_different_identity(tmp_path):
    make_campaign(tmp_path)
    rows = [json.loads(line) for line in (tmp_path / "settlements.jsonl").read_text(encoding="utf-8").splitlines()]
    for field, value in (("settled_at", "2026-09-10T13:00:00+00:00"), ("market_id", "other-market")):
        duplicate = dict(rows[0])
        duplicate[field] = value
        write_jsonl(tmp_path / "settlements.jsonl", [rows[0], duplicate])
        with pytest.raises(ValueError, match="conflicting finalized settlement identity"):
            build_report(tmp_path)


def test_exact_duplicate_final_settlement_is_idempotent(tmp_path):
    make_campaign(tmp_path)
    rows = [json.loads(line) for line in (tmp_path / "settlements.jsonl").read_text(encoding="utf-8").splitlines()]
    write_jsonl(tmp_path / "settlements.jsonl", [rows[0], dict(rows[0])])
    assert build_report(tmp_path)["panel"]["n_unique_settled_conditions"] == 1


def test_report_requires_exact_snapshot_match(tmp_path):
    cid, scanned_at = make_campaign(tmp_path)
    write_jsonl(tmp_path / "forecast_snapshots.jsonl", [row(cid=cid, side="NO", scanned_at=scanned_at)])
    with pytest.raises(ValueError, match="exact decision snapshot"):
        build_report(tmp_path)


def test_report_deduplicates_repeated_candidate_rows_and_flags_mirror_cap_override(tmp_path):
    make_campaign(tmp_path)
    mirror = {"arms": {"hold": {"total_paper_trades": 2, "ledger_settlement_count": 1, "paired_entries_beyond_position_cap": 0}, "full-10": {"total_paper_trades": 2, "ledger_settlement_count": 1, "paired_entries_beyond_position_cap": 0}}}
    path = tmp_path / "mirror-status.json"
    path.write_text(json.dumps(mirror), encoding="utf-8")
    for arm in ("hold", "full-10"):
        arm_dir = tmp_path / "arms" / arm
        arm_dir.mkdir(parents=True)
        trades = [
            {"candidate_id": "overridden", "paper_executed": True, "paired_entry": True,
             "paired_entry_risk_overrides": ["position_cap"] if arm == "hold" else [],
             "strategy": "weather_directional", "condition_id": "0xoverride", "market_id": "m-override",
             "side": "YES", "event_key": "event-override", "mirror_source_audit_id": "source-override",
             "scanned_at": "2026-09-09T10:00:00+00:00"},
            {"candidate_id": "feasible", "paper_executed": True, "paired_entry": True,
             "paired_entry_risk_overrides": [], "strategy": "weather_directional", "condition_id": "0xfeasible",
             "market_id": "m-feasible", "side": "YES", "event_key": "event-feasible",
             "mirror_source_audit_id": "source-feasible", "scanned_at": "2026-09-09T10:00:00+00:00"},
        ]
        write_jsonl(arm_dir / "paper_trades.jsonl", trades)
        write_jsonl(arm_dir / "settlements.jsonl", [{
            "condition_id": "0xfeasible", "market_id": "m-feasible", "strategy": "weather_directional",
            "directional_outcome": 1, "payout": "5", "public_data_only": True,
            "settlement_source": "gamma-public-market-outcome-prices", "settled_at": "2026-09-10T12:00:00+00:00"
        }])
    report = build_report(tmp_path, mirror_status_path=path)
    assert report["candidate_pressure"]["rows"] == 2
    assert report["candidate_pressure"]["unique_opportunities"] == 1
    assert report["exit_comparison"]["status"] == "not_cap_comparable"
    assert report["exit_comparison"]["cap_override_count_status"] == 0
    assert report["exit_comparison"]["cap_override_count_ledger"] == 1
    paired = report["exit_comparison"]["paired_cohort"]
    assert paired["common_entry_ids"] == 2
    assert paired["common_entries_with_any_risk_override"] == 1
    assert paired["cap_feasible_common_entries"] == 1
    assert paired["cap_feasible_directional_entries_settled_in_every_arm"] == 1


def test_mirror_report_rejects_candidate_id_collision_with_different_trade_identity(tmp_path):
    make_campaign(tmp_path)
    status = {"arms": {"a": {"total_paper_trades": 1, "ledger_settlement_count": 0},
                       "b": {"total_paper_trades": 1, "ledger_settlement_count": 0}}}
    status_path = tmp_path / "mirror-status.json"
    status_path.write_text(json.dumps(status), encoding="utf-8")
    for arm, market_id in (("a", "m1"), ("b", "m2")):
        arm_dir = tmp_path / "arms" / arm
        arm_dir.mkdir(parents=True)
        write_jsonl(arm_dir / "paper_trades.jsonl", [{
            "candidate_id": "same-id", "paper_executed": True, "paired_entry": True,
            "paired_entry_risk_overrides": [], "strategy": "weather_directional",
            "condition_id": "0xsame", "market_id": market_id, "side": "YES",
            "scanned_at": "2026-09-01T00:00:00+00:00", "event_key": "event-1",
            "mirror_source_audit_id": "source-same",
        }])
        write_jsonl(arm_dir / "settlements.jsonl", [])
    report = build_report(tmp_path, mirror_status_path=status_path)
    assert report["exit_comparison"]["status"] == "entry_cohorts_differ"
    assert report["exit_comparison"]["paired_cohort"]["candidate_id_overlap"] == 1
    assert report["exit_comparison"]["paired_cohort"]["metadata_mismatch_ids"] == 1
    assert report["exit_comparison"]["paired_cohort"]["common_entry_ids"] == 0


def test_mirror_report_requires_nonempty_source_audit_identity(tmp_path):
    make_campaign(tmp_path)
    status_path = tmp_path / "mirror-status.json"
    status_path.write_text(json.dumps({"arms": {
        "hold": {"total_paper_trades": 1, "ledger_settlement_count": 0},
        "exit": {"total_paper_trades": 1, "ledger_settlement_count": 0},
    }}), encoding="utf-8")
    for arm in ("hold", "exit"):
        arm_dir = tmp_path / "arms" / arm
        arm_dir.mkdir(parents=True)
        write_jsonl(arm_dir / "paper_trades.jsonl", [{
            "candidate_id": "same", "paper_executed": True, "paired_entry": True,
            "paired_entry_risk_overrides": [], "strategy": "weather_directional",
            "condition_id": "0xsame", "market_id": "m-same", "side": "YES",
            "scanned_at": "2026-09-09T10:00:00+00:00", "event_key": "event-same",
        }])
        write_jsonl(arm_dir / "settlements.jsonl", [])
    report = build_report(tmp_path, mirror_status_path=status_path)
    assert report["exit_comparison"]["status"] == "entry_cohorts_differ"
    assert report["exit_comparison"]["paired_cohort"]["candidate_id_overlap"] == 1
    assert report["exit_comparison"]["paired_cohort"]["metadata_mismatch_ids"] == 1
    assert report["exit_comparison"]["paired_cohort"]["common_entry_ids"] == 0


def test_mirror_settlement_join_requires_matching_market_strategy_source_and_time(tmp_path):
    make_campaign(tmp_path)
    status_path = tmp_path / "mirror-status.json"
    status_path.write_text(json.dumps({"arms": {
        "hold": {"total_paper_trades": 1, "ledger_settlement_count": 1},
        "exit": {"total_paper_trades": 1, "ledger_settlement_count": 1},
    }}), encoding="utf-8")
    for arm in ("hold", "exit"):
        arm_dir = tmp_path / "arms" / arm
        arm_dir.mkdir(parents=True)
        write_jsonl(arm_dir / "paper_trades.jsonl", [{
            "candidate_id": "shared", "paper_executed": True, "paired_entry": True,
            "paired_entry_risk_overrides": [], "strategy": "weather_directional",
            "condition_id": "0xshared", "market_id": "m-shared", "side": "YES",
            "scanned_at": "2026-09-09T10:00:00+00:00", "event_key": "event-shared",
            "mirror_source_audit_id": "source-shared",
        }])
        settlement = {
            "condition_id": "0xshared", "market_id": "m-shared", "strategy": "weather_directional",
            "directional_outcome": 1, "payout": "5", "public_data_only": True,
            "settlement_source": "gamma-public-market-outcome-prices", "settled_at": "2026-09-10T12:00:00+00:00",
        }
        if arm == "hold":
            settlement["market_id"] = "m-other"
        else:
            settlement["settlement_source"] = "unverified"
        write_jsonl(arm_dir / "settlements.jsonl", [settlement])
    report = build_report(tmp_path, mirror_status_path=status_path)
    assert report["exit_comparison"]["paired_cohort"]["cap_feasible_directional_exit_entries"] == 1
    assert report["exit_comparison"]["paired_cohort"]["cap_feasible_directional_entries_settled_in_every_arm"] == 0


def test_proper_bet_diagnostic_is_not_promoted_as_executable(tmp_path):
    make_campaign(tmp_path)
    report = build_report(tmp_path)
    proper = report["proper_bet_shadow"]
    assert proper["status"] == "theoretical_only"
    assert proper["execution_assumption"] == "best-ask only; no historical depth replay"
    assert proper["venue_minimum_feasible"] is False
    assert Decimal(proper["hypothetical_net_pnl_at_ask_before_depth_and_minimum"]) == Decimal("0.1552")
    assert proper["pnl_claimed"] is False


def test_cli_writes_create_only_report_without_touching_inputs(tmp_path):
    import subprocess
    import sys

    make_campaign(tmp_path)
    out = tmp_path / "report.json"
    result = subprocess.run(
        [sys.executable, "scripts/v7_pnl_report.py", "--data-dir", str(tmp_path), "--out", str(out)],
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(result.stdout.splitlines()[0])["resolved_unique_conditions"] == 1
    before = out.read_text(encoding="utf-8")
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.run(
            [sys.executable, "scripts/v7_pnl_report.py", "--data-dir", str(tmp_path), "--out", str(out)],
            check=True,
            capture_output=True,
            text=True,
        )
    assert out.read_text(encoding="utf-8") == before
