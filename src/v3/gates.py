"""Machine-readable, pre-registered promotion gates and kill criteria (item 9).

The registry lives in ``config/gates/gate_registry.json``. This module loads
and validates it and evaluates a lane's evidence against its gate. A lane whose
evidence is missing or below the required cluster count returns
``insufficient_data`` rather than ``pass``.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

REQUIRED_GATE_FIELDS = (
    "lane", "status", "metric_hierarchy", "preregistered_cell", "cluster_unit",
    "min_clusters", "threshold", "kill_criterion", "review_date", "pnl_is_gate",
)
FORBIDDEN_STATISTIC_RULES = (
    "no_argmax_cell_without_family_wise_control",
    "no_statistic_when_cohort_exit_rate_differs_from_population_by_over_10_points",
    "min_clusters_before_quoting_z",
    "no_pnl_as_validation_signal",
    "no_settled_only_cohort_statistics",
)


class GateRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class GateVerdict:
    lane: str
    verdict: str  # pass | fail | insufficient_data | blocked
    reason: str
    details: Mapping[str, Any]


def load_registry(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    validate_registry(payload)
    return payload


def validate_registry(payload: Mapping[str, Any]) -> None:
    if payload.get("version") is None or not isinstance(payload.get("gates"), list):
        raise GateRegistryError("registry needs a version and a gates list")
    rules = set(payload.get("global_rules", []))
    missing_rules = set(FORBIDDEN_STATISTIC_RULES) - rules
    if missing_rules:
        raise GateRegistryError(f"registry is missing global rules: {sorted(missing_rules)}")
    seen = set()
    for gate in payload["gates"]:
        for field in REQUIRED_GATE_FIELDS:
            if field not in gate:
                raise GateRegistryError(f"gate {gate.get('lane')!r} missing {field}")
        if gate["status"] not in {"blocked","not_authorized","insufficient_data","evidence_blocked","pass","fail"}:
            raise GateRegistryError("invalid registry status")
        if not isinstance(gate["threshold"], dict) or not {"metric","lower_bound_must_exceed"} <= gate["threshold"].keys():
            raise GateRegistryError("invalid threshold")
        if gate["pnl_is_gate"] is not False:
            raise GateRegistryError(f"gate {gate['lane']!r}: P&L may never be a gate")
        if gate["lane"] in seen:
            raise GateRegistryError(f"duplicate lane {gate['lane']!r}")
        seen.add(gate["lane"])
        if gate["metric_hierarchy"] and gate["metric_hierarchy"][0] == "pnl":
            raise GateRegistryError(f"gate {gate['lane']!r}: P&L cannot lead the metric hierarchy")
        if int(gate["min_clusters"]) < 15:
            raise GateRegistryError(f"gate {gate['lane']!r}: min_clusters below 15")


def evaluate_gate(gate: Mapping[str, Any], evidence: Mapping[str, Any] | None) -> GateVerdict:
    lane = str(gate["lane"])
    if gate.get("status") in {"not_authorized", "blocked"}:
        return GateVerdict(lane, "blocked", str(gate.get("block_reason", "lane blocked by registry")), {})
    if not evidence:
        return GateVerdict(lane, "insufficient_data", "no evidence supplied", {})
    rules = [
        (evidence.get("settled_only") is True, "no_settled_only_cohort_statistics"),
        (evidence.get("argmax_selected") is True and evidence.get("family_wise_control") is not True, "no_argmax_cell_without_family_wise_control"),
        (evidence.get("validation_metric") == "pnl", "no_pnl_as_validation_signal"),
        (evidence.get("price_age_seconds", 0) > evidence.get("decision_interval_seconds", 300), "source_timestamp_freshness"),
        (evidence.get("crossable_quotes") == 0, "executable_depth_required"),
    ]
    for rejected, rule in rules:
        if rejected:
            return GateVerdict(lane, "fail", rule, {})
    if evidence.get("denominator_complete") is False:
        return GateVerdict(lane, "identifiability_blocked", "unfilled quote denominator unavailable", {})
    clusters = int(evidence.get("clusters", 0))
    if clusters < int(gate["min_clusters"]):
        return GateVerdict(
            lane, "insufficient_data",
            f"{clusters} clusters < required {gate['min_clusters']}",
            {"clusters": clusters},
        )
    if evidence.get("cell") != gate["preregistered_cell"]:
        return GateVerdict(lane, "fail", "evidence is not from the pre-registered parameter cell", {})
    exit_gap = evidence.get("cohort_exit_rate_gap_points")
    if exit_gap is not None and abs(float(exit_gap)) > 10:
        return GateVerdict(lane, "fail", "cohort exit rate differs from population by > 10 points", {})
    if evidence.get("samples", 0) < gate.get("required_samples", 0):
        return GateVerdict(lane, "insufficient_data", "required sample count unavailable", {})
    for key, required in gate.get("required_checks", {}).items():
        if key not in evidence:
            return GateVerdict(lane, "insufficient_data", f"required check {key} missing", {})
        if evidence[key] != required:
            return GateVerdict(lane, "fail", f"required check {key} failed", {})
    threshold = gate["threshold"]
    metric = str(threshold["metric"])
    value = evidence.get(metric)
    if value is None:
        return GateVerdict(lane, "insufficient_data", f"metric {metric} missing from evidence", {})
    bound = float(threshold["lower_bound_must_exceed"])
    lower = evidence.get(f"{metric}_ci_lower")
    if lower is None:
        return GateVerdict(lane, "insufficient_data", f"clustered CI for {metric} missing", {})
    if not all(math.isfinite(float(x)) for x in (value, lower, bound)):
        return GateVerdict(lane, "fail", "nonfinite evidence", {})
    if float(lower) > bound:
        return GateVerdict(lane, "pass", f"{metric} clustered lower bound {lower} > {bound}", dict(evidence))
    return GateVerdict(lane, "fail", f"{metric} clustered lower bound {lower} <= {bound}", dict(evidence))
