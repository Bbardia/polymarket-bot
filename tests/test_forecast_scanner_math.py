from dataclasses import dataclass

from src.forecast_scanner import ForecastScanner


@dataclass
class FakeMarket:
    weather_type: str = "temperature"
    temperature_bucket: dict = None
    yes_token_id: str = "token-1"
    condition_id: str = "cond-1"
    city: str = "bern"
    yes_price: float = 0.10
    question: str = "Will Bern be exactly 20°C tomorrow?"
    target_date: str = "2026-05-19"

    def __post_init__(self):
        if self.temperature_bucket is None:
            self.temperature_bucket = {"type": "exact", "unit": "C", "value": 20}


class FakePM:
    def fetch_weather_markets(self, **kwargs):
        return [FakeMarket()]


class FakeForecast:
    def check_signal(self, question):
        return 0.80, {"ensemble_std": 0.5, "n_members": 100, "lead_days": 1}


def test_forecast_scanner_shrinks_raw_probability_and_records_uncertainty(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()

    scanner = ForecastScanner()
    signals = scanner.scan(FakePM(), FakeForecast(), existing_tokens=set())

    assert len(signals) == 1
    sig = signals[0]
    assert sig.raw_forecast_prob == 0.80
    assert 0.10 < sig.forecast_prob < 0.80
    assert sig.n_eff > 0
    assert sig.prob_uncertainty > 0
    assert sig.edge == sig.forecast_prob - sig.market_price
