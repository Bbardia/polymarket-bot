import importlib.util
import json
import os
from datetime import date
from decimal import Decimal
from pathlib import Path

from src.v3.paper_weather import (
    CALIBRATION_FILENAME,
    FORECAST_MODEL_VERSION,
    ProbabilityCalibration,
    write_json_atomic,
)
from src.v3.scan_calibration import (
    build_calibration,
    dedupe_observations,
    iter_scan_rows,
    parse_gamma_outcome,
    past_condition_ids,
)

D = Decimal

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "rebuild_weather_calibration.py"
_spec = importlib.util.spec_from_file_location("rebuild_weather_calibration", _SCRIPT)
rebuild = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rebuild)


def row(cid, at, prob="0.72", side="YES", lead=1, city="Paris", target="2026-01-01",
        source="gfs", version=FORECAST_MODEL_VERSION):
    return {"condition_id": cid, "scanned_at": at, "side": side, "lead_days": lead,
            "city": city, "target_date": target, "forecast_model_version": version,
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
    assert by_lead[1].probability == D("0.72")


def test_skips_non_evaluation_malformed_and_old_model_rows(tmp_path):
    path = tmp_path / "weather_scans.jsonl"
    path.write_text(
        json.dumps({"scanned_at": "x", "error": "boom"}) + "\n"
        "not json{\n\n[1,2]\n"
        + json.dumps(row("c1", "2026-01-01T00:00:00")) + "\n"
        + json.dumps({**row("c2", "t"), "provider_probabilities": {}}) + "\n"
        + json.dumps(row("c3", "t", prob="bad")) + "\n"
        + json.dumps(row("c4", "t", version="legacy")) + "\n"
        + json.dumps({k: v for k, v in row("c5", "t").items()
                      if k != "forecast_model_version"}) + "\n",
        encoding="utf-8",
    )
    assert len(list(iter_scan_rows(path))) == 6
    assert [o.condition_id for o in dedupe_observations(iter_scan_rows(path))] == ["c1"]
    assert list(iter_scan_rows(tmp_path / "missing.jsonl")) == []


def test_past_condition_ids_come_from_usable_observations_only():
    obs = dedupe_observations([
        row("past", "t", target="2026-01-01"),
        row("future", "t", target="2999-01-01"),
        {**row("unusable", "t"), "provider_probabilities": {}},
    ])
    assert past_condition_ids(obs, date(2026, 6, 1)) == ["past"]


def test_build_calibration_counts_city_and_pooled_with_probability_sum():
    obs = dedupe_observations([
        row("c1", "t", city="Paris", prob="0.72"),
        row("c2", "t", city="Rome", prob="0.74"),
        row("c3", "t", city="Oslo", prob="0.70"),
    ])
    bins = build_calibration(obs, {"c1": 1, "c2": 0}).to_json()
    assert bins["gfs:Paris:1:7"] == {"successes": 1, "total": 1, "probability_sum": "0.72"}
    assert bins["gfs:Rome:1:7"] == {"successes": 0, "total": 1, "probability_sum": "0.74"}
    assert bins["gfs:*:1:7"] == {"successes": 1, "total": 2, "probability_sum": "1.46"}
    assert "gfs:Oslo:1:7" not in bins  # unresolved


def test_calibration_shifts_instead_of_flattening_bucket():
    # Review finding: a dense pooled bin used to replace every probability in
    # its 0.1-wide bucket with one rate, creating a step function.
    cal = ProbabilityCalibration(None)
    for index in range(500):
        cal.record("gfs", f"city{index % 25}", 1, D("0.05"), int(index % 50 == 0))
    low = cal.calibrate("gfs", "unseen", 1, D("0.001"))
    mid = cal.calibrate("gfs", "unseen", 1, D("0.05"))
    high = cal.calibrate("gfs", "unseen", 1, D("0.099"))
    assert D(0) < low < mid < high
    assert low < D("0.001")
    # Observed rate 0.02 vs forecast 0.05: the bucket centre moves to ~0.021.
    assert D("0.019") < mid < D("0.023")
    assert D("0.035") < high < D("0.05")


def test_city_evidence_is_not_double_counted_through_pool():
    cal = ProbabilityCalibration(None, min_samples=10)
    for _ in range(15):
        cal.record("gfs", "Paris", 1, D("0.50"), 1)
    for _ in range(5):
        cal.record("gfs", "Rome", 1, D("0.50"), 0)
    # Paris offset = (15*0.5 + 10*pooled_ex_paris) / 25 where the pool
    # excludes Paris; at the bucket centre (0.55) that is a plain shift.
    pooled_ex_paris = (D(0) - D("2.5")) / D(15)
    offset = (D("7.5") + 10 * pooled_ex_paris) / D(25)
    assert abs(cal.calibrate("gfs", "Paris", 1, D("0.55")) - (D("0.55") + offset)) < D("1e-9")
    # Counting Paris twice (old behaviour) would give a larger shift.
    double_counted = (D("7.5") + 10 * (D("5") / D(30))) / D(25)
    assert offset < double_counted
    assert cal.samples() == 20


def test_no_evidence_leaves_probability_unchanged():
    cal = ProbabilityCalibration(None)
    assert cal.calibrate("gfs", "Paris", 1, D("0.37")) == D("0.37")


def test_legacy_bins_without_probability_sum_use_bucket_midpoint(tmp_path):
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"gfs:Paris:1:7": {"successes": 5, "total": 10}}))
    cal = ProbabilityCalibration(path, min_samples=10)
    # residual = 5 - 0.75*10 = -2.5, shrunk by 10/(10+10), at the centre
    assert abs(cal.calibrate("gfs", "Paris", 1, D("0.75")) - D("0.625")) < D("1e-9")
    assert cal.samples() == 10


def test_running_calibrator_picks_up_rebuilt_file(tmp_path):
    # Review finding: the worker held stale bins and overwrote rebuilds.
    path = tmp_path / CALIBRATION_FILENAME
    cal = ProbabilityCalibration(path)
    assert cal.calibrate("gfs", "Paris", 1, D("0.50")) == D("0.50")
    rebuilt = ProbabilityCalibration(None)
    for _ in range(40):
        rebuilt.record("gfs", "Paris", 1, D("0.50"), 1)
    write_json_atomic(rebuilt.to_json(), path)
    os.utime(path, ns=(1, 1))  # force a distinct mtime on coarse filesystems
    assert cal.calibrate("gfs", "Paris", 1, D("0.50")) > D("0.80")
    assert cal.samples() == 40


def test_persisted_record_round_trips(tmp_path):
    path = tmp_path / "c.json"
    cal = ProbabilityCalibration(path)
    cal.record("gfs", "Paris", 1, D("0.75"), 1)
    data = json.loads(path.read_text())
    assert data["gfs:*:1:7"] == {"successes": 1, "total": 1, "probability_sum": "0.75"}
    reloaded = ProbabilityCalibration(path)
    assert reloaded.calibrate("gfs", "Paris", 1, D("0.75")) == cal.calibrate(
        "gfs", "Paris", 1, D("0.75"),
    )
    assert not list(tmp_path.glob("*.tmp"))


def test_parse_gamma_outcome():
    base = {"closed": True, "outcomePrices": '["1","0"]'}
    assert parse_gamma_outcome(base) == 1
    assert parse_gamma_outcome({**base, "outcomePrices": ["0", "1"]}) == 0
    assert parse_gamma_outcome({**base, "closed": False}) is None
    assert parse_gamma_outcome({**base, "outcomePrices": '["0.6","0.4"]'}) is None
    assert parse_gamma_outcome({**base, "umaResolutionStatus": "disputed"}) is None
    assert parse_gamma_outcome({**base, "outcomes": '["No","Yes"]'}) == 0


def _write_scans(tmp_path):
    scans = tmp_path / "weather_scans.jsonl"
    lines = [row("c1", "a"), row("c1", "b", side="NO"), row("c2", "a"), row("c3", "a"),
             row("cf", "a", target="2999-01-01"), {"error": "x"}]
    scans.write_text("\n".join(json.dumps(x) for x in lines) + "\n")


def test_cli_main(tmp_path, capsys):
    _write_scans(tmp_path)
    calls = []

    def fetcher(ids):
        calls.append(list(ids))
        table = {"c1": '["1","0"]', "c2": '["0","1"]'}
        return [{"conditionId": c, "closed": True, "outcomePrices": table[c]}
                for c in ids if c in table]

    today = date(2026, 6, 1)
    target = tmp_path / CALIBRATION_FILENAME
    assert rebuild.main(["--data-dir", str(tmp_path), "--dry-run"], fetcher=fetcher, today=today) == 0
    assert not target.exists()
    assert calls == [["c1", "c2", "c3"]]

    assert rebuild.main(["--data-dir", str(tmp_path)], fetcher=fetcher, today=today) == 0
    out = capsys.readouterr().out
    assert "rows read:          6" in out
    assert "resolved markets:   2" in out
    cal = json.loads(target.read_text())
    assert cal["gfs:*:1:7"] == {"successes": 1, "total": 2, "probability_sum": "1.44"}
    assert json.loads((tmp_path / "scan_outcomes.json").read_text()) == {"c1": 1, "c2": 0}
    assert not list(tmp_path.glob("*.tmp"))
    # resolved ids are cached; only unresolved c3 is refetched
    rebuild.main(["--data-dir", str(tmp_path)], fetcher=fetcher, today=today)
    assert calls[-1] == ["c3"]


def test_cli_fetch_failure_keeps_existing_calibration_and_fails(tmp_path, capsys):
    # Review finding: a partial fetch used to replace a fuller calibration.
    _write_scans(tmp_path)
    target = tmp_path / CALIBRATION_FILENAME
    target.write_text('{"sentinel:x:1:1": {"successes": 1, "total": 1}}\n')

    def fetcher(ids):
        if "c1" in ids and len(ids) == 1:
            return [{"conditionId": "c1", "closed": True, "outcomePrices": '["1","0"]'}]
        raise OSError("gamma down")

    code = rebuild.main(["--data-dir", str(tmp_path)], fetcher=fetcher,
                        today=date(2026, 6, 1))
    assert code == 1
    assert "sentinel" in target.read_text()
    assert "calibration left untouched" in capsys.readouterr().out


def test_rebuild_ignores_rows_from_older_forecast_model(tmp_path):
    scans = tmp_path / "weather_scans.jsonl"
    scans.write_text(json.dumps(row("old", "a", version="legacy")) + "\n")
    calls = []

    def fetcher(ids):
        calls.append(ids)
        return []

    assert rebuild.main(["--data-dir", str(tmp_path)], fetcher=fetcher,
                        today=date(2026, 6, 1)) == 0
    assert calls == []
    assert not (tmp_path / CALIBRATION_FILENAME).exists()
