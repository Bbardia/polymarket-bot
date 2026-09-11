"""Polymarket API client for weather market discovery and trading."""
import json
import re
from datetime import datetime, date
from typing import Optional
import requests
from loguru import logger

from src.config import Config

# Legacy V1 SDK removed. Discovery and historical parsing remain available.


class WeatherMarket:
    """Represents a single weather market on Polymarket (temperature, precipitation, snow, wind)."""

    def __init__(self, raw: dict):
        self.raw = raw
        self.question = raw.get("question", "")
        self.condition_id = raw.get("conditionId", "")
        self.slug = raw.get("slug", "")
        self.active = raw.get("active", False)
        self.closed = raw.get("closed", False)
        self.volume = float(raw.get("volume", 0))
        self.volume_24h = float(raw.get("volume24hr", 0))
        self.liquidity = float(raw.get("liquidity", 0))
        self.end_date = raw.get("endDate", "")

        # Parse outcomes and prices
        outcomes_raw = raw.get("outcomes", "[]")
        prices_raw = raw.get("outcomePrices", "[]")
        self.outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])
        self.prices = json.loads(prices_raw) if isinstance(prices_raw, str) else (prices_raw or [])
        self.prices = [float(p) for p in self.prices]

        # Parse CLOB token IDs
        tokens_raw = raw.get("clobTokenIds", "[]")
        self.token_ids = json.loads(tokens_raw) if isinstance(tokens_raw, str) else (tokens_raw or [])

        # Detect weather type from question
        self.weather_type = self._detect_weather_type()

        # Extract city and date from question
        self.city = self._extract_city()
        self.target_date = self._extract_date()

        # Extract the relevant bucket based on weather type
        if self.weather_type == "temperature":
            self.temperature_bucket = self._extract_temperature()
            self.weather_bucket = self.temperature_bucket
        elif self.weather_type in ("precipitation", "snowfall"):
            self.temperature_bucket = None
            self.weather_bucket = self._extract_precipitation()
        elif self.weather_type in ("wind_speed", "wind_gusts"):
            self.temperature_bucket = None
            self.weather_bucket = self._extract_wind()
        else:
            self.temperature_bucket = self._extract_temperature()
            self.weather_bucket = self.temperature_bucket

    def _detect_weather_type(self) -> str:
        """Detect whether this is a temperature, precipitation, snow, or wind market."""
        q = self.question.lower()
        if any(w in q for w in ["rain", "precipitation", "rainfall"]):
            return "precipitation"
        if any(w in q for w in ["snow", "snowfall"]):
            return "snowfall"
        if any(w in q for w in ["wind gust", "gust"]):
            return "wind_gusts"
        if "wind" in q:
            return "wind_speed"
        if any(w in q for w in ["temperature", "°f", "°c"]):
            return "temperature"
        return "temperature"  # default

    def _extract_city(self) -> str:
        """Extract city name from weather questions like:
        - 'Highest temperature in London on March 24?' (event-level)
        - 'Will the highest temperature in London be 15°C on March 24?' (market-level)
        - 'Will it rain in New York on March 28?' (precipitation)
        - 'Will wind speeds in Chicago exceed 30 mph on April 1?' (wind)
        """
        # Temperature pattern: "temperature in CITY"
        match = re.search(r"temperature in (.+?)\s+on\s+\w+\s+\d", self.question, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            city = re.sub(r"\s+be\s+.*$", "", city, flags=re.IGNORECASE)
            return city
        # Generic "in CITY on DATE" / "in CITY exceed" pattern (rain, snow, wind)
        match = re.search(r"\bin\s+(.+?)\s+(?:on\s+\w+\s+\d|exceed|be\s+above|be\s+below)", self.question, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        # Fallback: just grab between "in" and "on"/"be"
        match = re.search(r"in\s+(.+?)\s+(?:on|be)\s", self.question, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return ""

    def _extract_date(self) -> Optional[date]:
        """Extract target date from the question."""
        months = r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
        match = re.search(rf"on\s+({months})\s+(\d{{1,2}})", self.question, re.IGNORECASE)
        if not match:
            return None
        month_str = match.group(1)
        day_str = match.group(2)
        current_year = date.today().year
        for year in [current_year, current_year + 1, current_year - 1]:
            try:
                return datetime.strptime(f"{month_str} {day_str} {year}", "%B %d %Y").date()
            except ValueError:
                continue
        return None

    def _extract_temperature(self) -> Optional[dict]:
        """Extract temperature bucket info from outcomes or question text.
        Returns dict like:
          {'type': 'exact', 'value': 15, 'unit': 'C'}
          {'type': 'range', 'low': 42, 'high': 43, 'unit': 'F'}
          {'type': 'or_higher', 'value': 60, 'unit': 'F'}
        """
        q = self.question

        # Detect unit from question text
        unit = "F" if "°F" in q else "C"

        # Pattern 1: "between X-Y°F" range buckets (US markets)
        match = re.search(r"between\s+(-?\d+)\s*-\s*(-?\d+)\s*°[FC]", q, re.IGNORECASE)
        if match:
            return {"type": "range", "low": int(match.group(1)), "high": int(match.group(2)), "unit": unit}

        # Pattern 2: "be X°C/F or higher/below"
        match = re.search(r"be\s+(-?\d+)\s*°[FC]?\s*(or higher|or below)", q, re.IGNORECASE)
        if match:
            temp = int(match.group(1))
            qualifier = match.group(2).lower()
            if "higher" in qualifier:
                return {"type": "or_higher", "value": temp, "unit": unit}
            else:
                return {"type": "or_below", "value": temp, "unit": unit}

        # Pattern 3: "be X°C/F on" (exact)
        match = re.search(r"be\s+(-?\d+)\s*°[FC]?\s+on", q, re.IGNORECASE)
        if match:
            return {"type": "exact", "value": int(match.group(1)), "unit": unit}

        # Fallback: try from outcomes
        if len(self.outcomes) == 2:
            outcome = self.outcomes[0]
            match = re.search(r"(-?\d+)\s*°?[FC]?\s*(or higher|or below)?", outcome, re.IGNORECASE)
            if match:
                temp = int(match.group(1))
                qualifier = match.group(2)
                if qualifier and "higher" in qualifier.lower():
                    return {"type": "or_higher", "value": temp, "unit": unit}
                elif qualifier and "below" in qualifier.lower():
                    return {"type": "or_below", "value": temp, "unit": unit}
                else:
                    return {"type": "exact", "value": temp, "unit": unit}
            match = re.search(r"^(-?\d+)$", outcome.strip())
            if match:
                return {"type": "exact", "value": int(match.group(1)), "unit": unit}

        return None

    def _extract_precipitation(self) -> Optional[dict]:
        """Extract precipitation/snow bucket from question.
        Examples:
          'Will it rain in NYC on March 28?' → yes_no, threshold 0.1mm
          'Will precipitation exceed 5mm in London on April 1?' → or_higher, 5mm
          'Will snowfall exceed 2 inches in Chicago on March 30?' → or_higher, 2in
        """
        q = self.question
        variable = "snowfall" if self.weather_type == "snowfall" else "precipitation"

        # Detect unit
        unit = "mm"
        if "inch" in q.lower():
            unit = "in"
        elif "cm" in q.lower():
            unit = "cm"

        # Pattern: "exceed/above/more than X mm/inches"
        match = re.search(r"(?:exceed|above|more than|over|greater than)\s+(\d+(?:\.\d+)?)\s*(?:mm|inches?|cm|in)?",
                          q, re.IGNORECASE)
        if match:
            return {"variable": variable, "type": "or_higher", "value": float(match.group(1)), "unit": unit}

        # Pattern: "below/under/less than X"
        match = re.search(r"(?:below|under|less than)\s+(\d+(?:\.\d+)?)\s*(?:mm|inches?|cm|in)?",
                          q, re.IGNORECASE)
        if match:
            return {"variable": variable, "type": "or_below", "value": float(match.group(1)), "unit": unit}

        # Pattern: "between X-Y mm"
        match = re.search(r"between\s+(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*(?:mm|inches?|cm|in)?",
                          q, re.IGNORECASE)
        if match:
            return {"variable": variable, "type": "range", "low": float(match.group(1)),
                    "high": float(match.group(2)), "unit": unit}

        # Default: "Will it rain/snow?" → any measurable amount
        return {"variable": variable, "type": "yes_no", "value": 0.1, "unit": unit}

    def _extract_wind(self) -> Optional[dict]:
        """Extract wind speed bucket from question.
        Examples:
          'Will wind speeds in Chicago exceed 30 mph on April 1?'
          'Will wind gusts be above 50 km/h in London on March 30?'
        """
        q = self.question
        variable = "wind_gusts" if self.weather_type == "wind_gusts" else "wind_speed"

        unit = "mph" if "mph" in q.lower() else "km/h"

        # Pattern: "exceed/above X mph/km/h"
        match = re.search(r"(?:exceed|above|over|greater than)\s+(\d+(?:\.\d+)?)\s*(?:mph|km/?h|knots?)?",
                          q, re.IGNORECASE)
        if match:
            return {"variable": variable, "type": "or_higher", "value": float(match.group(1)), "unit": unit}

        # Pattern: "below/under X"
        match = re.search(r"(?:below|under|less than)\s+(\d+(?:\.\d+)?)\s*(?:mph|km/?h|knots?)?",
                          q, re.IGNORECASE)
        if match:
            return {"variable": variable, "type": "or_below", "value": float(match.group(1)), "unit": unit}

        # Pattern: "between X-Y"
        match = re.search(r"between\s+(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*(?:mph|km/?h)?",
                          q, re.IGNORECASE)
        if match:
            return {"variable": variable, "type": "range", "low": float(match.group(1)),
                    "high": float(match.group(2)), "unit": unit}

        # Default: any significant wind
        return {"variable": variable, "type": "or_higher", "value": 20.0, "unit": unit}

    @property
    def yes_price(self) -> float:
        return self.prices[0] if self.prices else 0.0

    @property
    def no_price(self) -> float:
        return self.prices[1] if len(self.prices) > 1 else 0.0

    @property
    def yes_token_id(self) -> str:
        return self.token_ids[0] if self.token_ids else ""

    @property
    def no_token_id(self) -> str:
        return self.token_ids[1] if len(self.token_ids) > 1 else ""

    @property
    def implied_probability(self) -> float:
        """Market-implied probability of YES outcome."""
        return self.yes_price

    def __repr__(self):
        bucket = self.weather_bucket or self.temperature_bucket
        return (
            f"WeatherMarket(type={self.weather_type}, city={self.city!r}, date={self.target_date}, "
            f"bucket={bucket}, yes={self.yes_price:.3f}, "
            f"vol24h=${self.volume_24h:,.0f})"
        )


class PolymarketClient:
    """Client for Polymarket Gamma (discovery) and CLOB (trading) APIs."""

    def __init__(self):
        self.gamma_host = Config.GAMMA_HOST
        self.clob_client = None
        self._api_creds_set = False

    def init_trading_client(self):
        """Legacy live client is permanently disabled after the CLOB V2 cutover."""
        logger.error("Legacy live trading disabled; use src.v3.api")
        return False

    def test_connection(self) -> bool:
        """Test basic API connectivity."""
        try:
            if self.clob_client:
                ok = self.clob_client.get_ok()
                logger.info(f"CLOB API status: {ok}")
            resp = requests.get(f"{self.gamma_host}/markets", params={"limit": 1})
            resp.raise_for_status()
            logger.info(f"Gamma API: OK ({resp.status_code})")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    # ── Market Discovery (Gamma API — no auth) ──

    def fetch_weather_markets(self, target_date: Optional[date] = None,
                              weather_types: Optional[list[str]] = None,
                              max_horizon_days: int = 2) -> list[WeatherMarket]:
        """Fetch active weather markets within the trading horizon.

        weather_types: filter to specific types, e.g. ["temperature", "precipitation"].
                       None means all weather types.
        max_horizon_days: only return markets resolving within this many days (default 2).
                          Avoids fetching 1000+ markets we'll never trade.
        """
        from datetime import date as date_cls, timedelta
        today = date_cls.today()
        max_date = today + timedelta(days=max_horizon_days)

        all_markets = []
        offset = 0
        limit = 100
        max_retries = 3
        skipped_future = 0

        while True:
            params = {
                "tag": "weather",
                "active": "true",
                "closed": "false",
                "limit": limit,
                "offset": offset,
            }
            batch = None
            for attempt in range(max_retries):
                try:
                    resp = requests.get(f"{self.gamma_host}/markets", params=params, timeout=10)
                    resp.raise_for_status()
                    batch = resp.json()
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(1.0 * (attempt + 1))
                        logger.debug(f"Gamma API retry {attempt + 1} at offset {offset}: {e}")
                    else:
                        logger.error(f"Gamma API failed after {max_retries} retries at offset {offset}: {e}")
            if batch is None:
                break

            if not batch:
                break

            for raw in batch:
                market = WeatherMarket(raw)
                if not market.city or not market.target_date:
                    continue
                # Skip markets beyond our trading horizon early
                if market.target_date > max_date:
                    skipped_future += 1
                    continue
                if market.target_date < today:
                    continue
                if weather_types and market.weather_type not in weather_types:
                    continue
                if target_date and market.target_date != target_date:
                    continue
                if not market.weather_bucket:
                    continue
                all_markets.append(market)

            if len(batch) < limit:
                break
            offset += limit

        type_counts = {}
        for m in all_markets:
            type_counts[m.weather_type] = type_counts.get(m.weather_type, 0) + 1
        logger.info(f"Found {len(all_markets)} weather markets: {type_counts}"
                    + (f" (skipped {skipped_future} beyond {max_horizon_days}d)" if skipped_future else ""))
        return all_markets

    def fetch_event_markets(self, event_slug: str) -> list[WeatherMarket]:
        """Fetch all markets for a specific event (e.g., all temperature buckets for one city/date)."""
        try:
            resp = requests.get(
                f"{self.gamma_host}/events",
                params={"slug": event_slug, "active": "true"}
            )
            resp.raise_for_status()
            events = resp.json()
            if not events:
                return []

            markets = []
            event = events[0]
            for m_raw in event.get("markets", []):
                market = WeatherMarket(m_raw)
                markets.append(market)
            return markets
        except Exception as e:
            logger.error(f"Error fetching event {event_slug}: {e}")
            return []

    def group_markets_by_event(self, markets: list[WeatherMarket]) -> dict:
        """Group markets by city+date (they belong to the same event)."""
        groups = {}
        for m in markets:
            key = f"{m.city.lower()}_{m.target_date}"
            if key not in groups:
                groups[key] = []
            groups[key].append(m)
        return groups

    # ── Order Book (CLOB — read-only) ──

    def get_order_book(self, token_id: str):
        """V1 public execution adapter retired; use the V3 public SDK."""
        raise RuntimeError("Legacy V1 order-book adapter parked; use src.v3.api")

    def get_price(self, token_id: str, side: str = "BUY") -> float:
        """V1 public execution adapter retired; historical prices stay in ledgers."""
        raise RuntimeError("Legacy V1 price adapter parked; use src.v3.api")

    # ── Trading (CLOB — authenticated) ──

    def place_limit_order(self, token_id: str, price: float, size: float,
                          side: str = "BUY") -> dict:
        """Legacy order placement is permanently disabled."""
        logger.error("Legacy live trading disabled; use the V3 executor after certification")
        return {"error": "Legacy live trading disabled"}

    def place_market_order(self, token_id: str, amount: float,
                           side: str = "BUY") -> dict:
        """Legacy market-order placement is permanently disabled."""
        logger.error("Legacy live trading disabled; use the V3 executor after certification")
        return {"error": "Legacy live trading disabled"}

    def get_positions(self) -> list:
        """Legacy account access permanently disabled."""
        return []
