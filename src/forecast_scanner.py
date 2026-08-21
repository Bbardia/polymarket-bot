"""Forecast Scanner — standalone weather strategy.

Scans active Polymarket temperature markets, compares Open-Meteo multi-model
ensemble probability to executable market price, and returns signals when the
forecast-market disagreement clears a dynamic uncertainty buffer.

Core rules:
- Use multi-model ensemble probabilities with Gaussian smoothing.
- Prefer exact-temperature contracts; range/above/below contracts need separate calibration.
- Require larger edge when ensemble spread, lead time, or orderbook spread is high.
- Avoid ultra-low dust prices and expensive entries where payoff asymmetry is poor.
"""

import json
import time
from dataclasses import dataclass
from pathlib import Path
from loguru import logger

from src.edge_math import (
    dynamic_min_edge,
    effective_ensemble_size,
    probability_standard_error,
    shrink_probability,
)


@dataclass
class ForecastSignal:
    """A trade signal from forecast-vs-price disagreement."""
    token_id: str
    condition_id: str
    question: str
    city: str
    target_date: str
    market_price: float       # Current YES price on Polymarket
    forecast_prob: float      # Shrunk/calibrated probability used for decisions
    edge: float               # forecast_prob - market_price
    temp_info: str            # e.g. "14°C exact"
    raw_forecast_prob: float = 0.0  # Raw Open-Meteo probability before shrinkage
    ensemble_std: float = 0.0     # ensemble spread (lower = more confident)
    n_members: int = 0            # number of ensemble members used
    n_eff: float = 0.0             # effective sample size after ensemble-agreement penalty
    prob_uncertainty: float = 0.0  # standard error of final probability estimate
    lead_days: int = 0            # days until market resolution
    dynamic_min_edge: float = 0.0 # computed min edge for this signal


# Cities excluded from this strategy because their resolution source or local
# microclimate has been hard to calibrate reliably in local testing.
AVOID_CITIES = {
    "beijing",      # continental extremes
    "chongqing",    # basin topography
    "london",       # Atlantic micro-fronts
    "paris",        # spring volatility
    "shenzhen",     # resolution data source can diverge from model station
    "wellington",   # windy/coastal microclimate
    "chengdu",      # cloudy basin
}


class ForecastScanner:
    """Scan weather markets for forecast-vs-price edge."""

    # Dynamic edge: base minimum, scales with ensemble spread
    BASE_MIN_EDGE = 0.05      # 5% floor (tight ensemble = high confidence)
    MAX_MIN_EDGE = 0.15       # 15% ceiling (wide spread = need big edge)
    # Price range for entries
    MIN_PRICE = 0.03          # Avoid ultra-low dust prices and thin books
    MAX_PRICE = 0.20          # Cheap enough for lottery math
    # Max simultaneous positions from this strategy
    MAX_POSITIONS = 10
    # Scan interval: don't hit Gamma API more than once per 5 min
    SCAN_INTERVAL = 300
    # Dedup file
    SEEN_FILE = "forecast_scanner_seen.json"

    def __init__(self):
        self._last_scan_ts = 0
        self._cached_markets = []
        self._seen = self._load_seen()

    @staticmethod
    def compute_dynamic_edge(ensemble_std: float, lead_days: int = 1) -> float:
        """Compute minimum required edge based on ensemble spread and lead time.

        Tight ensemble agreement = lower edge needed (more confident).
        Wide spread = need bigger edge to compensate for uncertainty.
        Longer lead time = need bigger edge (less accurate forecasts).

        Returns min_edge between BASE_MIN_EDGE and MAX_MIN_EDGE.
        """
        return dynamic_min_edge(
            base_edge=ForecastScanner.BASE_MIN_EDGE,
            max_edge=ForecastScanner.MAX_MIN_EDGE,
            ensemble_std=ensemble_std,
            lead_days=lead_days,
        )

    def _load_seen(self) -> set:
        path = Path("data") / self.SEEN_FILE
        try:
            data = json.loads(path.read_text())
            return set(data) if isinstance(data, list) else set()
        except (FileNotFoundError, json.JSONDecodeError):
            return set()

    def _save_seen(self):
        path = Path("data") / self.SEEN_FILE
        # Keep most recent 500
        seen_list = list(self._seen)
        if len(seen_list) > 1000:
            seen_list = seen_list[-500:]
            self._seen = set(seen_list)
        path.write_text(json.dumps(seen_list))

    def scan(self, pm_client, forecast, existing_tokens: set = None) -> list[ForecastSignal]:
        """Scan all weather markets for forecast edge.

        Args:
            pm_client: PolymarketClient with fetch_weather_markets()
            forecast: WeatherForecast instance for ensemble lookups
            existing_tokens: set of token_ids we already hold

        Returns:
            List of ForecastSignal with positive edge, sorted by edge descending.
        """
        from src.weather_forecast import parse_market_title

        if existing_tokens is None:
            existing_tokens = set()

        now = time.time()

        # Refresh market list every SCAN_INTERVAL
        if now - self._last_scan_ts > self.SCAN_INTERVAL or not self._cached_markets:
            try:
                self._cached_markets = pm_client.fetch_weather_markets(
                    weather_types=["temperature"],
                    max_horizon_days=3,
                )
                self._last_scan_ts = now
                logger.info(f"  Forecast scanner: fetched {len(self._cached_markets)} temperature markets")
            except Exception as e:
                logger.warning(f"  Forecast scanner: market fetch failed: {e}")
                return []

        signals = []
        checked = 0
        skipped_structure = 0
        skipped_price = 0
        skipped_seen = 0
        skipped_no_forecast = 0
        skipped_low_edge = 0
        skipped_city = 0

        for market in self._cached_markets:
            # Only temperature markets
            if market.weather_type != "temperature":
                continue

            # Prefer exact temperature markets; range/above/below markets need separate calibration.
            bucket = market.temperature_bucket
            if not bucket or bucket.get("type") not in ("exact",):
                skipped_structure += 1
                continue

            # Must have token IDs
            if not market.yes_token_id:
                continue

            # City avoid list (data-driven: proven losers)
            city_lower = (market.city or "").lower().strip()
            if city_lower in AVOID_CITIES:
                skipped_city += 1
                continue

            # Skip if already holding
            if market.yes_token_id in existing_tokens:
                continue

            # Dedup: one trade per market
            dedup_key = market.condition_id or market.yes_token_id
            if dedup_key in self._seen:
                skipped_seen += 1
                continue

            # Price filter: $0.03-$0.20 sweet spot
            market_price = market.yes_price
            if market_price < self.MIN_PRICE or market_price > self.MAX_PRICE:
                skipped_price += 1
                continue

            # Get forecast probability + ensemble metadata
            checked += 1
            try:
                result = forecast.check_signal(market.question)
            except Exception as e:
                logger.debug(f"  Forecast scanner: check failed for {market.question[:40]}: {e}")
                continue

            if result is None:
                skipped_no_forecast += 1
                continue

            raw_prob, meta = result
            ensemble_std = meta.get("ensemble_std", 2.0)
            n_members = meta.get("n_members", 31)
            lead_days = meta.get("lead_days", 2)

            # Empirical-Bayes shrinkage: a noisy weather model should not get
            # full credit until enough ensemble agreement exists. The market
            # price is the anchor because it is the executable outside view.
            n_eff = effective_ensemble_size(n_members, ensemble_std)
            prob = shrink_probability(
                raw_prob,
                market_price,
                n_eff=n_eff,
                prior_strength=25.0,
            )
            prob_uncertainty = probability_standard_error(prob, n_eff)

            # Dynamic edge threshold based on ensemble spread, lead time,
            # probability uncertainty, and tail-risk around extreme prices.
            min_edge = dynamic_min_edge(
                base_edge=self.BASE_MIN_EDGE,
                max_edge=self.MAX_MIN_EDGE,
                ensemble_std=ensemble_std,
                lead_days=lead_days,
                market_price=market_price,
                n_eff=n_eff,
            )

            # Edge = calibrated/shrunk forecast probability - executable market price
            edge = prob - market_price
            if edge < min_edge:
                skipped_low_edge += 1
                continue

            # Build temp description
            unit = bucket.get("unit", "C")
            temp_val = bucket.get("value", "?")
            temp_info = f"{temp_val}°{unit} exact"

            signals.append(ForecastSignal(
                token_id=market.yes_token_id,
                condition_id=market.condition_id,
                question=market.question,
                city=city_lower,
                target_date=str(market.target_date),
                market_price=market_price,
                forecast_prob=prob,
                edge=edge,
                temp_info=temp_info,
                raw_forecast_prob=raw_prob,
                ensemble_std=ensemble_std,
                n_members=n_members,
                n_eff=n_eff,
                prob_uncertainty=prob_uncertainty,
                lead_days=lead_days,
                dynamic_min_edge=min_edge,
            ))

        # Sort by edge descending (best opportunities first)
        signals.sort(key=lambda s: s.edge, reverse=True)

        logger.info(
            f"  Forecast scanner: {len(self._cached_markets)} markets, "
            f"{checked} checked, {len(signals)} signals "
            f"(skip: {skipped_structure} non-exact, {skipped_price} price, "
            f"{skipped_seen} seen, {skipped_city} avoid-city, "
            f"{skipped_no_forecast} no-forecast, {skipped_low_edge} low-edge)"
        )

        return signals

    def mark_seen(self, condition_id: str):
        """Mark a market as traded so we don't signal it again."""
        self._seen.add(condition_id)
        self._save_seen()
