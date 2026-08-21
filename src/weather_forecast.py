"""Open-Meteo multi-model ensemble forecast for weather trading.

Calls Open-Meteo ensemble API with 4 models (ECMWF + GFS + ICON + GEM)
to get ~143 ensemble members for daily high temperatures. Uses Gaussian
CDF probability (not raw counting) with lead-time sigma inflation for
calibrated probability estimates.

Upgrade history:
- 2026-04-09: Created with GFS 31-member raw counting
- 2026-04-11: Multi-model (143 members), Gaussian CDF, sigma inflation

Usage:
    forecast = WeatherForecast()
    result = forecast.check_signal("Will the highest temperature in Tokyo be 16°C on April 10?")
    if result is not None:
        prob, meta = result  # meta has ensemble_std, n_members, lead_days
"""

import re
import statistics
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from loguru import logger
from scipy.stats import norm

from src.config import Config


# Market title patterns
# "Will the highest temperature in {City} be {X}°C on {Month} {Day}?"
# "Will the highest temperature in {City} be between {X}-{Y}°F on {Month} {Day}?"
# "Will the highest temperature in {City} be {X}°C or higher on {Month} {Day}?"
# "Will the highest temperature in {City} be {X}°F or below on {Month} {Day}?"

_RE_EXACT_C = re.compile(
    r"highest temperature in (.+?) be (-?\d+)°C on (\w+ \d+)",
    re.IGNORECASE,
)
_RE_EXACT_F = re.compile(
    r"highest temperature in (.+?) be (-?\d+)°F on (\w+ \d+)",
    re.IGNORECASE,
)
_RE_RANGE_F = re.compile(
    r"highest temperature in (.+?) be between (\d+)-(\d+)°F on (\w+ \d+)",
    re.IGNORECASE,
)
_RE_RANGE_C = re.compile(
    r"highest temperature in (.+?) be between (\d+)-(\d+)°C on (\w+ \d+)",
    re.IGNORECASE,
)
_RE_ABOVE_C = re.compile(
    r"highest temperature in (.+?) be (-?\d+)°C or higher on (\w+ \d+)",
    re.IGNORECASE,
)
_RE_ABOVE_F = re.compile(
    r"highest temperature in (.+?) be (-?\d+)°F or higher on (\w+ \d+)",
    re.IGNORECASE,
)
_RE_BELOW_C = re.compile(
    r"highest temperature in (.+?) be (-?\d+)°C or below on (\w+ \d+)",
    re.IGNORECASE,
)
_RE_BELOW_F = re.compile(
    r"highest temperature in (.+?) be (-?\d+)°F or below on (\w+ \d+)",
    re.IGNORECASE,
)

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _parse_date(date_str: str) -> Optional[str]:
    """Parse 'April 10' → '2026-04-10' (ISO date)."""
    parts = date_str.strip().split()
    if len(parts) != 2:
        return None
    month_name, day_str = parts
    month = MONTH_MAP.get(month_name.lower())
    if not month:
        return None
    try:
        day = int(day_str.rstrip("?.,"))
    except ValueError:
        return None
    year = datetime.now(timezone.utc).year
    return f"{year}-{month:02d}-{day:02d}"


def parse_market_title(title: str) -> Optional[dict]:
    """Parse a weather market title into structured data.

    Returns dict with keys:
        city: str (lowercase, matching Config.CITY_COORDS keys)
        date: str (ISO date, e.g. '2026-04-10')
        type: 'exact' | 'range' | 'above' | 'below'
        unit: 'C' | 'F'
        temp_low: float (°C always — converted from °F if needed)
        temp_high: float (°C always — for exact/above/below, same as temp_low)
    """
    if not title:
        return None

    result = None

    # Try range patterns first (more specific)
    m = _RE_RANGE_F.search(title)
    if m:
        city, lo, hi, date_s = m.group(1), int(m.group(2)), int(m.group(3)), m.group(4)
        result = {
            "city": city.strip().lower(),
            "date": _parse_date(date_s),
            "type": "range",
            "unit": "F",
            "temp_low": (lo - 32) * 5 / 9,
            "temp_high": (hi - 32) * 5 / 9,
        }

    if not result:
        m = _RE_RANGE_C.search(title)
        if m:
            city, lo, hi, date_s = m.group(1), int(m.group(2)), int(m.group(3)), m.group(4)
            result = {
                "city": city.strip().lower(),
                "date": _parse_date(date_s),
                "type": "range",
                "unit": "C",
                "temp_low": float(lo),
                "temp_high": float(hi),
            }

    # Above/below patterns
    if not result:
        m = _RE_ABOVE_C.search(title)
        if m:
            city, temp, date_s = m.group(1), int(m.group(2)), m.group(3)
            result = {
                "city": city.strip().lower(),
                "date": _parse_date(date_s),
                "type": "above",
                "unit": "C",
                "temp_low": float(temp),
                "temp_high": float(temp),
            }

    if not result:
        m = _RE_ABOVE_F.search(title)
        if m:
            city, temp, date_s = m.group(1), int(m.group(2)), m.group(3)
            tc = (int(temp) - 32) * 5 / 9
            result = {
                "city": city.strip().lower(),
                "date": _parse_date(date_s),
                "type": "above",
                "unit": "F",
                "temp_low": tc,
                "temp_high": tc,
            }

    if not result:
        m = _RE_BELOW_C.search(title)
        if m:
            city, temp, date_s = m.group(1), int(m.group(2)), m.group(3)
            result = {
                "city": city.strip().lower(),
                "date": _parse_date(date_s),
                "type": "below",
                "unit": "C",
                "temp_low": float(temp),
                "temp_high": float(temp),
            }

    if not result:
        m = _RE_BELOW_F.search(title)
        if m:
            city, temp, date_s = m.group(1), int(m.group(2)), m.group(3)
            tc = (int(temp) - 32) * 5 / 9
            result = {
                "city": city.strip().lower(),
                "date": _parse_date(date_s),
                "type": "below",
                "unit": "F",
                "temp_low": tc,
                "temp_high": tc,
            }

    # Exact patterns (least specific)
    if not result:
        m = _RE_EXACT_C.search(title)
        if m:
            city, temp, date_s = m.group(1), int(m.group(2)), m.group(3)
            result = {
                "city": city.strip().lower(),
                "date": _parse_date(date_s),
                "type": "exact",
                "unit": "C",
                "temp_low": float(temp),
                "temp_high": float(temp),
            }

    if not result:
        m = _RE_EXACT_F.search(title)
        if m:
            city, temp, date_s = m.group(1), int(m.group(2)), m.group(3)
            tc = (int(temp) - 32) * 5 / 9
            result = {
                "city": city.strip().lower(),
                "date": _parse_date(date_s),
                "type": "exact",
                "unit": "F",
                "temp_low": tc,
                "temp_high": tc,
            }

    if result and result["date"] is None:
        return None

    return result


class WeatherForecast:
    """Multi-model ensemble forecasts with Gaussian CDF probability.

    Upgrade 2026-04-11: 4 models (ECMWF+GFS+ICON+GEM = ~143 members),
    Gaussian CDF instead of raw counting, lead-time sigma inflation.
    """

    # Cache TTL: 30 min (models update 4x/day)
    CACHE_TTL = 1800
    MIN_PROBABILITY = 0.10
    API_TIMEOUT = 15  # slightly longer for multi-model responses

    # 4 independent global models — one API call returns all members
    # ECMWF(51) + GFS(31) + ICON(40) + GEM(21) = 143 total
    ENSEMBLE_MODELS = "ecmwf_ifs025,gfs_seamless,icon_seamless,gem_global"

    # Lead-time sigma inflation: raw ensembles are overconfident.
    # Factor multiplied into ensemble stdev to correct underdispersion.
    SIGMA_INFLATION = {
        0: 1.05,   # Same-day (nearly resolved)
        1: 1.15,   # 1-day ahead
        2: 1.25,   # 2-day ahead
        3: 1.40,   # 3-day ahead
    }

    # Minimum irreducible uncertainty floor (°C) even when ensemble agrees perfectly
    SIGMA_FLOOR = 0.5

    def __init__(self):
        # Cache: (city, date) → (timestamp, ensemble_highs_list)
        self._cache: dict[tuple[str, str], tuple[float, list[float]]] = {}

    def _get_city_coords(self, city: str) -> Optional[tuple[float, float]]:
        """Look up city coordinates from config."""
        coords = Config.CITY_COORDS.get(city)
        if coords:
            return coords[0], coords[1]
        # Try partial match
        for key, val in Config.CITY_COORDS.items():
            if key in city or city in key:
                return val[0], val[1]
        return None

    def _fetch_ensemble(self, lat: float, lon: float, date: str) -> Optional[list[float]]:
        """Fetch multi-model ensemble forecast for daily max temperature.

        Returns list of ~143 ensemble member predictions (°C) for daily high.
        Uses ECMWF(51) + GFS(31) + ICON(40) + GEM(21) in a single API call.
        """
        try:
            resp = requests.get(
                Config.OPEN_METEO_ENSEMBLE,
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "daily": "temperature_2m_max",
                    "models": self.ENSEMBLE_MODELS,
                    "start_date": date,
                    "end_date": date,
                },
                timeout=self.API_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            # Multi-model: keys like temperature_2m_max_member01, ..._member51 (per model)
            daily = data.get("daily", {})
            highs = []
            for key, vals in daily.items():
                if key.startswith("temperature_2m_max") and vals:
                    val = vals[0]
                    if val is not None:
                        highs.append(float(val))

            if len(highs) < 10:
                logger.debug(f"  Forecast: only {len(highs)} members for ({lat:.1f},{lon:.1f}) on {date}")
                return None

            return highs

        except requests.exceptions.RequestException as e:
            logger.debug(f"  Forecast: API error: {e}")
            return None
        except (KeyError, IndexError, ValueError) as e:
            logger.debug(f"  Forecast: parse error: {e}")
            return None

    def _get_ensemble_highs(self, city: str, date: str) -> Optional[list[float]]:
        """Get ensemble highs with caching."""
        cache_key = (city, date)
        now = time.time()

        if cache_key in self._cache:
            ts, highs = self._cache[cache_key]
            if now - ts < self.CACHE_TTL:
                return highs

        coords = self._get_city_coords(city)
        if not coords:
            logger.debug(f"  Forecast: unknown city '{city}'")
            return None

        lat, lon = coords
        highs = self._fetch_ensemble(lat, lon, date)
        if highs:
            self._cache[cache_key] = (now, highs)
        return highs

    def _get_lead_days(self, date_str: str) -> int:
        """Calculate days between now and market resolution date."""
        try:
            target = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            delta = (target.date() - now.date()).days
            return max(0, delta)
        except (ValueError, TypeError):
            return 2  # default to 2-day if can't parse

    def _get_sigma_inflation(self, lead_days: int) -> float:
        """Get sigma inflation factor for forecast lead time."""
        if lead_days in self.SIGMA_INFLATION:
            return self.SIGMA_INFLATION[lead_days]
        # Interpolate for values beyond our table
        max_key = max(self.SIGMA_INFLATION.keys())
        if lead_days > max_key:
            return self.SIGMA_INFLATION[max_key] + 0.15 * (lead_days - max_key)
        return 1.25  # safe default

    def _gaussian_probability(
        self, highs: list[float], temp_low_c: float, temp_high_c: float, lead_days: int
    ) -> float:
        """Compute probability using Gaussian CDF fit to ensemble.

        Instead of counting discrete members in a bin (coarse: each of 143
        members = 0.7%), fit N(mu, sigma) to all members and integrate
        the probability density over the target temperature bin.
        """
        mu = statistics.mean(highs)
        raw_sigma = statistics.stdev(highs) if len(highs) > 1 else 1.0

        # Apply underdispersion correction (ensembles are systematically too narrow)
        inflation = self._get_sigma_inflation(lead_days)
        sigma = raw_sigma * inflation

        # Floor: even perfect ensemble agreement has irreducible uncertainty
        sigma = max(sigma, self.SIGMA_FLOOR)

        # Integrate Gaussian over the target bin
        prob = norm.cdf(temp_high_c, mu, sigma) - norm.cdf(temp_low_c, mu, sigma)

        return max(0.001, min(0.95, prob))

    def check_signal(self, title: str) -> Optional[tuple[float, dict]]:
        """Check forecast probability for a market.

        Args:
            title: Market question string

        Returns:
            Tuple of (probability, metadata_dict) if forecast available.
            metadata_dict has: ensemble_std, n_members, lead_days, ensemble_mean
            Returns None if can't parse or fetch forecast.
        """
        parsed = parse_market_title(title)
        if not parsed:
            logger.debug(f"  Forecast: can't parse title: {title[:50]}")
            return None

        city = parsed["city"]
        date = parsed["date"]
        mtype = parsed["type"]
        temp_low_c = parsed["temp_low"]
        temp_high_c = parsed["temp_high"]

        highs = self._get_ensemble_highs(city, date)
        if not highs:
            return None

        n = len(highs)
        lead_days = self._get_lead_days(date)
        mu = statistics.mean(highs)
        raw_sigma = statistics.stdev(highs) if n > 1 else 1.0

        if mtype == "exact":
            # Exact °C: bin is [target - 0.5, target + 0.5)
            # Exact °F: 2°F buckets ≈ 1.11°C wide
            if parsed["unit"] == "C":
                bin_lo = temp_low_c - 0.5
                bin_hi = temp_low_c + 0.5
            else:
                bin_lo = temp_low_c - 0.56
                bin_hi = temp_high_c + 0.56
            prob = self._gaussian_probability(highs, bin_lo, bin_hi, lead_days)

        elif mtype == "range":
            bin_lo = temp_low_c - 0.56
            bin_hi = temp_high_c + 0.56
            prob = self._gaussian_probability(highs, bin_lo, bin_hi, lead_days)

        elif mtype == "above":
            # P(T >= threshold) = 1 - CDF(threshold)
            inflation = self._get_sigma_inflation(lead_days)
            sigma = max(raw_sigma * inflation, self.SIGMA_FLOOR)
            prob = 1.0 - norm.cdf(temp_low_c, mu, sigma)
            prob = max(0.001, min(0.95, prob))

        elif mtype == "below":
            # P(T <= threshold) = CDF(threshold)
            inflation = self._get_sigma_inflation(lead_days)
            sigma = max(raw_sigma * inflation, self.SIGMA_FLOOR)
            prob = norm.cdf(temp_low_c, mu, sigma)
            prob = max(0.001, min(0.95, prob))

        else:
            return None

        meta = {
            "ensemble_std": raw_sigma,
            "ensemble_mean": mu,
            "n_members": n,
            "lead_days": lead_days,
        }

        logger.info(
            f"  Forecast: {city} {date} — {mtype} "
            f"{temp_low_c:.1f}°C: {prob:.0%} (μ={mu:.1f} σ={raw_sigma:.1f}, "
            f"{n} members, {lead_days}d ahead)"
        )
        return prob, meta
