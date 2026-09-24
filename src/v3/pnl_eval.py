"""Offline, read-only V7 forecast and experiment diagnostics.

Forecast snapshots remain immutable. This module joins already-persisted
snapshots, trades and settlements; it never edits campaign data or fetches data.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ZERO = Decimal("0")
ONE = Decimal("1")


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    with path.open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"malformed JSONL row {path.name}:{number}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"non-object JSONL row {path.name}:{number}")
            rows.append(value)
    return rows


def _decimal(value: Any, field: str, *, low: Decimal | None = None, high: Decimal | None = None) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid {field}") from exc
    if not result.is_finite() or (low is not None and result < low) or (high is not None and result > high):
        raise ValueError(f"out-of-range {field}")
    return result


def _time(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing {field}")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid {field}") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return result


def _mean_brier(probabilities: list[Decimal], outcomes: list[int]) -> Decimal | None:
    if not probabilities:
        return None
    if len(probabilities) != len(outcomes):
        raise ValueError("forecast/outcome length mismatch")
    return sum(((p - Decimal(y)) ** 2 for p, y in zip(probabilities, outcomes)), ZERO) / Decimal(len(outcomes))


def _side_concentration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {side: sum(str(row.get("side", "")).upper() == side for row in rows) for side in ("YES", "NO")}
    total = sum(counts.values())
    dominant_share = Decimal(max(counts.values())) / Decimal(total) if total else None
    return {"counts": counts, "total": total,
            "dominant_side_share": None if dominant_share is None else str(dominant_share.quantize(Decimal("0.0001")))}


def _candidate_identity(row: dict[str, Any]) -> str:
    return str(row.get("condition_id") or row.get("event_key") or row.get("market_id") or "")


def _candidate_category(reason: Any) -> str:
    text = str(reason or "").lower()
    if any(x in text for x in ("drawdown", "realized loss", "loss breaker")):
        return "drawdown_or_loss_breaker"
    if any(x in text for x in ("exposure", "position cap", "open-position cap", "cash", "order cap", "insufficient paper")):
        return "capacity_or_cash"
    if any(x in text for x in ("bid", "spread", "book", "depth", "stale", "price age", "venue minimum")):
        return "executable_book_or_depth"
    if any(x in text for x in ("station", "resolver", "provider", "forecast", "discovery", "observation")):
        return "resolver_or_data_coverage"
    if any(x in text for x in ("edge", "probability", "threshold", "tradeable", "cluster", "expected profit")):
        return "signal_or_contract_filter"
    return "other_or_not_selected"


def _mirror_summary(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {"status": "not_supplied"}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("invalid mirror status JSON") from exc
    arms = payload.get("arms") if isinstance(payload, dict) else None
    if not isinstance(arms, dict) or not arms:
        raise ValueError("mirror status has no arms")
    summary, trades, settlements = {}, [], []
    cap_overrides = 0
    for name, arm in sorted(arms.items()):
        if not isinstance(arm, dict):
            raise ValueError("mirror arm status must be an object")
        ntrades = int(arm.get("total_paper_trades", 0))
        nsettled = int(arm.get("ledger_settlement_count", 0))
        cap = int(arm.get("paired_entries_beyond_position_cap", 0))
        trades.append(ntrades)
        settlements.append(nsettled)
        cap_overrides += cap
        summary[str(name)] = {
            "trades": ntrades,
            "settlements": nsettled,
            "provisional_realized_pnl": arm.get("realized_pnl_provisional"),
            "mark_equity": arm.get("mark_equity"),
            "cap_overrides": cap,
        }
    ledger_sets: dict[str, set[str]] = {}
    rows_by_arm: dict[str, dict[str, dict[str, Any]]] = {}
    overrides_by_arm: dict[str, set[str]] = {}
    ledger_available = True
    for name in arms:
        ledger_path = path.parent / "arms" / str(name) / "paper_trades.jsonl"
        if not ledger_path.is_file():
            ledger_available = False
            break
        rows = [row for row in _jsonl(ledger_path) if row.get("paper_executed") is True and row.get("paired_entry") is True]
        rows_by_arm[str(name)] = {}
        for row in rows:
            candidate_id = str(row.get("candidate_id", ""))
            if not candidate_id:
                continue
            if candidate_id in rows_by_arm[str(name)]:
                raise ValueError("duplicate paired candidate id within mirror arm")
            rows_by_arm[str(name)][candidate_id] = row
        ledger_sets[str(name)] = set(rows_by_arm[str(name)])
        overrides_by_arm[str(name)] = {
            candidate_id for candidate_id, row in rows_by_arm[str(name)].items()
            if row.get("paired_entry_risk_overrides")
        }
    candidate_id_overlap = set.intersection(*ledger_sets.values()) if ledger_sets and ledger_available else set()
    signature_fields = ("condition_id", "market_id", "side", "scanned_at", "event_key", "strategy", "mirror_source_audit_id")
    metadata_mismatches = set()
    for candidate_id in candidate_id_overlap:
        arm_rows = [rows_by_arm[str(name)][candidate_id] for name in arms]
        signatures = {tuple(row.get(field) for field in signature_fields) for row in arm_rows}
        identity_complete = all(
            row.get(field) is not None and str(row.get(field)).strip()
            for row in arm_rows
            for field in signature_fields
        )
        if not identity_complete or len(signatures) != 1:
            metadata_mismatches.add(candidate_id)
    common_ids = candidate_id_overlap - metadata_mismatches
    override_ids = set.union(*overrides_by_arm.values()) if overrides_by_arm and ledger_available else set()
    cap_feasible = common_ids - override_ids
    exit_eligible = {
        candidate_id for candidate_id in cap_feasible
        if all(rows_by_arm[str(name)][candidate_id].get("strategy") == "weather_directional" for name in arms)
    }
    settlements_by_arm: dict[str, dict[tuple[str, str, str], datetime]] = {}
    if ledger_available:
        for name in arms:
            settlement_path = path.parent / "arms" / str(name) / "settlements.jsonl"
            finalized: dict[tuple[str, str, str], datetime] = {}
            for row in _jsonl(settlement_path):
                outcome = row.get("directional_outcome")
                if row.get("payout") is None or isinstance(outcome, bool) or not isinstance(outcome, int) or outcome not in (0, 1):
                    continue
                if row.get("public_data_only") is not True or row.get("settlement_source") != "gamma-public-market-outcome-prices":
                    continue
                key = (str(row.get("condition_id", "")), str(row.get("market_id", "")), str(row.get("strategy", "")))
                if not all(key):
                    continue
                settled_at = _time(row.get("settled_at"), "mirror settled_at")
                if key in finalized:
                    raise ValueError("duplicate final mirror settlement identity")
                finalized[key] = settled_at
            settlements_by_arm[str(name)] = finalized
    common_settled_exit_ids = set()
    for candidate_id in exit_eligible:
        matched = True
        for name in arms:
            entry = rows_by_arm[str(name)][candidate_id]
            key = (str(entry.get("condition_id", "")), str(entry.get("market_id", "")), str(entry.get("strategy", "")))
            settled_at = settlements_by_arm.get(str(name), {}).get(key)
            if not all(key) or settled_at is None or settled_at <= _time(entry.get("scanned_at"), "mirror entry scanned_at"):
                matched = False
                break
        if matched:
            common_settled_exit_ids.add(candidate_id)
    paired_detail_status = (
        "arm_ledgers_not_available" if not ledger_available
        else "entry_cohorts_differ" if metadata_mismatches
        else "no_cap_feasible_exit_entries" if not exit_eligible
        else "insufficient_common_settlements" if len(common_settled_exit_ids) < 20
        else "cohort_reconciled"
    )
    paired_detail = {
        "status": paired_detail_status,
        "candidate_id_overlap": len(candidate_id_overlap),
        "common_entry_ids": len(common_ids),
        "metadata_mismatch_ids": len(metadata_mismatches),
        "common_entries_with_any_risk_override": len(common_ids & override_ids),
        "candidate_id_overlaps_with_any_risk_override": len(candidate_id_overlap & override_ids),
        "cap_feasible_common_entries": len(cap_feasible),
        "cap_feasible_directional_exit_entries": len(exit_eligible),
        "cap_feasible_directional_entries_settled_in_every_arm": len(common_settled_exit_ids),
    }
    ledger_override_count = len(override_ids)
    effective_cap_overrides = max(cap_overrides, ledger_override_count)
    if effective_cap_overrides:
        status = "not_cap_comparable"
    elif not ledger_available:
        status = "arm_ledgers_not_available"
    elif metadata_mismatches or len(set(trades)) != 1:
        status = "entry_cohorts_differ"
    elif min(settlements, default=0) < 20:
        status = "insufficient_resolved_sample"
    else:
        status = "descriptive_only_requires_event_clustered_review"
    return {
        "status": status,
        "arms": summary,
        "common_entry_count": len(common_ids),
        "minimum_arm_settlements": min(settlements, default=0),
        "cap_override_count_status": cap_overrides,
        "cap_override_count_ledger": ledger_override_count,
        "paired_cohort": paired_detail,
    }


def build_report(data_dir: Path, *, mirror_status_path: Path | None = None) -> dict[str, Any]:
    """Build a local diagnostic; reads originals and returns an additive report."""
    try:
        runtime_status = json.loads((data_dir / "status.json").read_text(encoding="utf-8"))
    except FileNotFoundError:
        runtime_status = {}
    except json.JSONDecodeError as exc:
        raise ValueError("invalid campaign status JSON") from exc
    worker_fields = ("cycle", "last_scan_at", "running", "healthy", "errors_this_cycle",
                     "mode", "paper_entries_enabled", "live_trading_enabled", "account_reads_enabled",
                     "paper_entry_block_reason", "total_paper_trades", "ledger_settlement_count",
                     "ledger_settlement_pnl", "ledger_exit_count", "ledger_exit_pnl",
                     "realized_pnl_provisional", "open_positions", "unresolved_stake",
                     "mark_equity", "mark_drawdown")
    worker_status = {key: runtime_status.get(key) for key in worker_fields if key in runtime_status}

    snapshots = _jsonl(data_dir / "forecast_snapshots.jsonl")
    tape_coverage = {
        "snapshot_rows": len(snapshots),
        "unique_conditions": len({str(row.get("condition_id")) for row in snapshots if row.get("condition_id")}),
        "labels_written_into_immutable_snapshots": sum(row.get("forecast_label") is not None for row in snapshots),
        "issuance_timestamps_known": sum(row.get("forecast_issuance_at") is not None for row in snapshots),
    }
    trades = _jsonl(data_dir / "paper_trades.jsonl")
    settlements = _jsonl(data_dir / "settlements.jsonl")
    candidates = _jsonl(data_dir / "candidates.jsonl")

    snapshot_index = {}
    snapshot_id_index = {}
    for row in snapshots:
        key = (str(row.get("condition_id", "")), str(row.get("side", "")).upper(), str(row.get("scanned_at", "")))
        if not all(key):
            continue
        if key in snapshot_index:
            raise ValueError("duplicate exact forecast snapshot key")
        snapshot_index[key] = row
        snapshot_id = row.get("forecast_snapshot_id")
        if snapshot_id is not None:
            if str(snapshot_id) in snapshot_id_index:
                raise ValueError("duplicate forecast_snapshot_id")
            snapshot_id_index[str(snapshot_id)] = row

    final_by_condition = {}
    for row in settlements:
        cid = str(row.get("condition_id", ""))
        if not cid or row.get("status") == "settlement_error":
            continue
        if row.get("payout") is None:
            continue
        outcome_value = row.get("directional_outcome")
        if isinstance(outcome_value, bool) or not isinstance(outcome_value, int) or outcome_value not in (0, 1):
            continue
        if row.get("settlement_source") != "gamma-public-market-outcome-prices":
            raise ValueError("unsupported final settlement source")
        if row.get("public_data_only") is not True:
            raise ValueError("settlement row is not marked public_data_only")
        _decimal(row["payout"], "settlement payout", low=ZERO)
        _time(row.get("settled_at"), "settled_at")
        if cid in final_by_condition:
            previous = final_by_condition[cid]
            identity_fields = ("market_id", "strategy", "directional_outcome", "payout", "settled_at",
                               "settlement_source", "public_data_only")
            if any(previous.get(field) != row.get(field) for field in identity_fields):
                raise ValueError("conflicting finalized settlement identity for condition")
            continue
        final_by_condition[cid] = row

    executed = {}
    for trade in trades:
        if trade.get("paper_executed") is not True:
            continue
        if trade.get("public_data_only") is not True:
            raise ValueError("paper trade is not marked public_data_only")
        cid = str(trade.get("condition_id", ""))
        if not cid or trade.get("strategy") != "weather_directional":
            continue
        if str(trade.get("side", "")).upper() not in ("YES", "NO"):
            raise ValueError("directional trade has invalid side")
        if cid in executed:
            raise ValueError("multiple directional trades for one condition require lot-level attribution")
        executed[cid] = trade

    panel, exclusions = [], defaultdict(int)
    for cid, settlement in final_by_condition.items():
        trade = executed.get(cid)
        if trade is None:
            continue
        side, scanned_at = str(trade.get("side", "")).upper(), str(trade.get("scanned_at", ""))
        trade_snapshot_id = trade.get("forecast_snapshot_id")
        if trade_snapshot_id is not None:
            snapshot = snapshot_id_index.get(str(trade_snapshot_id))
            if snapshot is None:
                raise ValueError("trade forecast_snapshot_id has no snapshot")
            if (str(snapshot.get("condition_id", "")), str(snapshot.get("side", "")).upper(), str(snapshot.get("scanned_at", ""))) != (cid, side, scanned_at):
                raise ValueError("forecast_snapshot_id does not match the trade decision identity")
            join_method = "forecast_snapshot_id"
        else:
            snapshot = snapshot_index.get((cid, side, scanned_at))
            join_method = "legacy_condition_side_timestamp"
        if snapshot is None:
            raise ValueError("missing exact decision snapshot for executed trade")
        if (str(snapshot.get("market_id", "")) != str(trade.get("market_id", ""))
                or str(snapshot.get("strategy", "")) != str(trade.get("strategy", ""))
                or str(settlement.get("market_id", "")) != str(trade.get("market_id", ""))
                or str(settlement.get("strategy", "")) != str(trade.get("strategy", ""))):
            raise ValueError("market/strategy mismatch across trade, snapshot and settlement")
        decision_at = _time(snapshot.get("forecast_decision_at") or scanned_at, "forecast decision time")
        settled_at = _time(settlement.get("settled_at"), "settled_at")
        if decision_at >= settled_at:
            raise ValueError("decision must precede settlement")
        if snapshot.get("forecast_label") is not None:
            raise ValueError("input forecast snapshot was mutated with a label")
        outcome = int(settlement["directional_outcome"])
        p_model = _decimal(snapshot.get("calibrated_probability"), "calibrated_probability", low=ZERO, high=ONE)
        p_raw = _decimal(snapshot.get("raw_probability"), "raw_probability", low=ZERO, high=ONE)
        q_ask = _decimal(snapshot.get("best_ask"), "entry best_ask", low=ZERO, high=ONE)
        if q_ask in (ZERO, ONE):
            exclusions["invalid_entry_ask"] += 1
            continue
        panel.append({"condition_id": cid, "market_id": str(trade.get("market_id", "")), "side": side,
                      "target_date": str(snapshot.get("target_date", "")), "city": str(snapshot.get("city", "")),
                      "decision_at": decision_at.isoformat(), "settled_at": settled_at.isoformat(),
                      "outcome_selected_side": outcome, "p_calibrated_selected_side": str(p_model),
                      "p_raw_selected_side": str(p_raw), "entry_ask_selected_side": str(q_ask),
                      "fee_rate": str(_decimal(snapshot.get("fee_rate"), "fee_rate", low=ZERO)),
                      "settlement_pnl": str(settlement.get("realized_pnl")),
                      "snapshot_join_method": join_method})

    outcomes = [int(r["outcome_selected_side"]) for r in panel]
    model = [Decimal(r["p_calibrated_selected_side"]) for r in panel]
    raw = [Decimal(r["p_raw_selected_side"]) for r in panel]
    asks = [Decimal(r["entry_ask_selected_side"]) for r in panel]
    hit_rate = Decimal(sum(outcomes)) / Decimal(len(outcomes)) if outcomes else None
    scores = {"model_brier": _mean_brier(model, outcomes), "raw_brier": _mean_brier(raw, outcomes),
              "entry_ask_brier": _mean_brier(asks, outcomes),
              "fixed_50pct_brier": _mean_brier([Decimal("0.5")] * len(outcomes), outcomes) if outcomes else None,
              "model_minus_entry_ask_brier": None,
              "mean_model_probability": sum(model, ZERO) / Decimal(len(model)) if model else None,
              "mean_entry_ask": sum(asks, ZERO) / Decimal(len(asks)) if asks else None,
              "observed_selected_side_hit_rate": hit_rate}
    if scores["model_brier"] is not None and scores["entry_ask_brier"] is not None:
        scores["model_minus_entry_ask_brier"] = scores["model_brier"] - scores["entry_ask_brier"]

    proper_rows = []
    for row in panel:
        p, q = Decimal(row["p_calibrated_selected_side"]), Decimal(row["entry_ask_selected_side"])
        trade = executed[row["condition_id"]]
        minimum = _decimal(trade.get("venue_minimum_shares", "5"), "venue minimum shares", low=ZERO)
        size = max(ZERO, Decimal("2") * (p - q))
        fee_rate = Decimal(row["fee_rate"])
        outcome = Decimal(row["outcome_selected_side"])
        fee = size * fee_rate * q * (ONE - q)
        hypothetical_pnl = size * (outcome - q) - fee
        proper_rows.append({"condition_id": row["condition_id"], "brier_position_selected_side_shares": str(size),
                            "venue_minimum_shares": str(minimum), "minimum_size_feasible": size >= minimum,
                            "estimated_taker_fee_at_best_ask": str(fee), "hypothetical_pnl_at_ask": str(hypothetical_pnl)})
    proper = {"status": "theoretical_only" if panel else "no_matured_trades",
              "rule": "Brier-score position diagnostic: 2 * (p_selected - entry_ask)",
              "execution_assumption": "best-ask only; no historical depth replay",
              "venue_minimum_feasible": bool(proper_rows) and all(r["minimum_size_feasible"] for r in proper_rows),
              "rows_below_venue_minimum": sum(not r["minimum_size_feasible"] for r in proper_rows),
              "hypothetical_net_pnl_at_ask_before_depth_and_minimum": str(sum((Decimal(r["hypothetical_pnl_at_ask"]) for r in proper_rows), ZERO)),
              "rows": proper_rows, "pnl_claimed": False}

    by_category: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        name = _candidate_category(candidate.get("paper_reason"))
        if name not in by_category:
            by_category[name] = {"rows": 0, "conditions": set()}
        bucket = by_category[name]
        bucket["rows"] += 1
        identity = _candidate_identity(candidate)
        if identity:
            bucket["conditions"].add(identity)
    categories = {name: {"scan_rows": value["rows"], "unique_opportunities": len(value["conditions"])}
                  for name, value in sorted(by_category.items())}
    untraded_candidate_conditions = {
        identity for row in candidates
        if (identity := _candidate_identity(row)) and str(row.get("condition_id", "")) not in executed
    }
    untraded_condition_ids = {
        str(row["condition_id"]) for row in candidates
        if row.get("condition_id") and str(row["condition_id"]) not in executed
    }
    labeled_untraded = untraded_condition_ids & set(final_by_condition)
    threshold = {"status": "blocked_untraded_candidates_unlabeled" if not labeled_untraded else "descriptive_only",
                 "untraded_unique_opportunities": len(untraded_candidate_conditions),
                 "untraded_unique_conditions_with_final_labels": len(labeled_untraded),
                 "candidate_rows_are_not_independent_outcomes": True, "no_policy_pnl_computed": True}
    snapshot_join_counts = {
        method: sum(row["snapshot_join_method"] == method for row in panel)
        for method in {row["snapshot_join_method"] for row in panel}
    }
    status = "no_finalized_labels" if not panel else ("insufficient_data" if len(panel) < 20 else "descriptive_only_requires_out_of_sample")
    side_concentration = {
        "all_executed_directional": _side_concentration(list(executed.values())),
        "settled_score_panel": _side_concentration(panel),
    }
    return {"schema_version": "v7-pnl-eval-v1",
            "worker_status": worker_status,
            "tape_coverage": tape_coverage,
            "input_files": ["forecast_snapshots.jsonl", "paper_trades.jsonl", "settlements.jsonl", "candidates.jsonl"],
            "panel": {"status": status, "n_unique_settled_conditions": len(panel),
                      "snapshot_join_methods": snapshot_join_counts,
                      "date_clusters": len({r["target_date"] for r in panel if r["target_date"]}),
                      "selected_side_hits": sum(outcomes), "exclusions": dict(exclusions), "rows": panel},
            "scores": {k: None if v is None else str(v) for k, v in scores.items()},
            "side_concentration": side_concentration,
            "proper_bet_shadow": proper,
            "candidate_pressure": {"rows": len(candidates),
                                   "unique_opportunities": len({_candidate_identity(r) for r in candidates if _candidate_identity(r)}),
                                   "by_reason_family": categories},
            "threshold_policy": threshold,
            "exit_comparison": _mirror_summary(mirror_status_path),
            "limitations": ["Settled rows are trade-selected, not an unbiased candidate cohort.",
                            "Repeated scans are grouped by condition, not counted as separate outcomes.",
                            "Untraded candidate conditions lack finalized local labels; no entry-threshold P&L is estimated.",
                            "The proper-bet sizing is theoretical only; historical order-book depth is unavailable.",
                            "No strategy promotion or profitability claim is produced by this report."]}
