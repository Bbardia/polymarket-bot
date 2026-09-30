import importlib.util
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from src.v3.paper_weather import ProbabilityCalibration
from src.v3.scan_calibration import (
    build_bins,
    dedupe_observations,
    load_scan_rows,
    parse_gamma_outcome,
    write_json_atomic,
)

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "rebuild_weather_calibration.py"
_spec = importlib.util.spec_from_file_location("rebuild_weather_calibration", _SCRIPT)
rebuild = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rebuild)


def row(cid, at, prob="0.72", side="YES", lead=1, city="Paris", target="2026-01-01", source="gfs"):
    return {"condition_id": cid, "scanned_at": at, "side": side, "lead_days": lead,
            "city": city, "target_date": target,
            "provider_probabilities": {source: prob}}


def test_dedupe_latest_per_condition_lead_source():
    rows = [
        row("c1", "2026-01-01T00:00:00", "0.10"),
        row("c1", "2026-01-01T02:00:00", "0.72", side="NO"),
        row("c1", "2026-01-01T01:00:00", "0.30"),
        row("c1", "2026-01-01T02:00:00", "0.72", lead=2),
    ]
    obs = dedupe_observations(rows)
    assert len(obs) == 2
    by_lead = {o.lead_days: o for o in obs}
    assert by_lead[1].probability == Decimal("0.72")


def test_skips_non_evaluation_and_malformed(tmp_path):
    path = tmp_path / "weather_scans.jsonl"
    path.write_text(
        json.dumps({"scanned_at": "x", "error": "boom"}) + "\n"
        "not json{\n\n[1,2]\n"
        + json.dumps(row("c1", "2026-01-01T00:00:00")) + "\n"
        + json.dumps({**row("c2", "t"), "provider_probabilities": {}}) + "\n"
        + json.dumps(row("c3", "t", prob="bad")) + "\n",
        encoding="utf-8",
    )
    rows = load_scan_rows(path)
    assert len(rows) == 4
    assert [o.condition_id for o in dedupe_observations(rows)] == ["c1"]
    assert load_scan_rows(tmp_path / "missing.jsonl") == []


def test_unresolved_skipped_and_pooled_keys():
    obs = dedupe_observations([
        row("c1", "t", city="Paris"), row("c2", "t", city="Rome"), row("c3", "t", city="Rome"),
    ])
    bins = build_bins(obs, {"c1": 1, "c2": 0})
    assert bins["gfs:Paris:1:7"] == {"successes": 1, "total": 1}
    assert bins["gfs:Rome:1:7"] == {"successes": 0, "total": 1}
    assert bins["gfs:*:1:7"] == {"successes": 1, "total": 2}


def test_round_trip_with_calibration(tmp_path):
    obs = dedupe_observations([row(f"c{i}", "t") for i in range(20)])
    bins = build_bins(obs, {f"c{i}": 0 for i in range(20)})
    path = tmp_path / "weather_calibration.json"
    write_json_atomic(bins, path)
    cal = ProbabilityCalibration(path)
    assert cal.calibrate("gfs", "Paris", 1, Decimal("0.72")) == Decimal(1) / Decimal(22)
    # unseen city falls back to pooled evidence
    assert cal.calibrate("gfs", "Oslo", 1, Decimal("0.72")) == Decimal(1) / Decimal(22)


def test_pooled_fallback_and_layering():
    cal = ProbabilityCalibration(None, min_samples=10)
    for _ in range(10):
        cal.record("gfs", "Paris", 1, Decimal("0.75"), 0)
    for _ in range(5):
        cal.record("gfs", "Rome", 1, Decimal("0.75"), 1)
    p = Decimal("0.75")
    pooled_rate = Decimal(6) / Decimal(17)  # pooled 5/15 successes, weight capped at 1
    assert cal.calibrate("gfs", "Oslo", 1, p) == pooled_rate
    rome_rate = Decimal(6) / Decimal(7)
    expected = pooled_rate * Decimal("0.5") + rome_rate * Decimal("0.5")
    assert cal.calibrate("gfs", "Rome", 1, p) == expected
    assert cal.samples() == 15


def test_backward_compat_no_pooled_keys(tmp_path):
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"gfs:Paris:1:7": {"successes": 5, "total": 10}}))
    cal = ProbabilityCalibration(path)
    expected = Decimal("0.75") * Decimal("0.5") + (Decimal(6) / Decimal(12)) * Decimal("0.5")
    assert cal.calibrate("gfs", "Paris", 1, Decimal("0.75")) == expected
    assert cal.calibrate("gfs", "Rome", 1, Decimal("0.75")) == Decimal("0.75")
    assert cal.samples() == 10


def test_record_writes_pooled(tmp_path):
    path = tmp_path / "c.json"
    cal = ProbabilityCalibration(path)
    cal.record("gfs", "Paris", 1, Decimal("0.75"), 1)
    data = json.loads(path.read_text())
    assert data["gfs:*:1:7"] == {"successes": 1, "total": 1}
    assert data["gfs:Paris:1:7"] == {"successes": 1, "total": 1}


def test_parse_gamma_outcome():
    base = {"closed": True, "outcomePrices": '["1","0"]'}
    assert parse_gamma_outcome(base) == 1
    assert parse_gamma_outcome({**base, "outcomePrices": ["0", "1"]}) == 0
    assert parse_gamma_outcome({**base, "closed": False}) is None
    assert parse_gamma_outcome({**base, "outcomePrices": '["0.6","0.4"]'}) is None
    assert parse_gamma_outcome({**base, "umaResolutionStatus": "disputed"}) is None
    assert parse_gamma_outcome({**base, "outcomes": '["No","Yes"]'}) == 0


def test_cli_main(tmp_path, capsys):
    scans = tmp_path / "weather_scans.jsonl"
    lines = [row("c1", "a"), row("c1", "b", side="NO"), row("c2", "a"), row("c3", "a"),
             row("cf", "a", target="2999-01-01"), {"error": "x"}]
    scans.write_text("\n".join(json.dumps(x) for x in lines) + "\n")
    calls = []

    def fetcher(ids):
        calls.append(list(ids))
        table = {"c1": '["1","0"]', "c2": '["0","1"]'}
        return [{"conditionId": c, "closed": True, "outcomePrices": table[c]}
                for c in ids if c in table]

    today = date(2026, 6, 1)
    assert rebuild.main(["--data-dir", str(tmp_path), "--dry-run"], fetcher=fetcher, today=today) == 0
    assert not (tmp_path / "weather_calibration.json").exists()
    assert calls == [["c1", "c2", "c3"]]

    assert rebuild.main(["--data-dir", str(tmp_path)], fetcher=fetcher, today=today) == 0
    out = capsys.readouterr().out
    assert "resolved markets:   2" in out
    cal = json.loads((tmp_path / "weather_calibration.json").read_text())
    assert cal["gfs:*:1:7"] == {"successes": 1, "total": 2}
    assert json.loads((tmp_path / "scan_outcomes.json").read_text()) == {"c1": 1, "c2": 0}
    assert not list(tmp_path.glob("*.tmp"))
    # resolved ids are cached; only unresolved c3 is refetched
    rebuild.main(["--data-dir", str(tmp_path)], fetcher=fetcher, today=today)
    assert calls[-1] == ["c3"]
