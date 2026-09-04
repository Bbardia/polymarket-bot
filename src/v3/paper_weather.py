"""Public weather discovery and tiny directional paper evaluation."""

from __future__ import annotations

import asyncio
import json
import math
import re
import statistics
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from statistics import NormalDist
from typing import Any, Protocol
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import requests
from polymarket.models.gamma.market import Market

from .market_context import MarketContext
from .maker_shadow import MakerShadowQuote, propose_buy_quote
from .math import BookLevel, execution_fee, execution_vwap
from .strategies.weather import WeatherDecision, WeatherMarketInput, evaluate_weather_market
from .weather_surface import EventSurface, SurfaceBucket, analyze_event_surface

ZERO = Decimal("0")
ONE = Decimal("1")
HALF = Decimal("0.5")
OPEN_METEO_ENSEMBLE = "https://ensemble-api.open-meteo.com/v1/ensemble"
NOAA_METAR = "https://aviationweather.gov/api/data/metar"
ENSEMBLE_MODELS = "ecmwf_ifs025,gfs_seamless,icon_seamless,gem_global"
MODEL_KEY_MARKERS: Mapping[str, str] = {
    "ecmwf": "ecmwf_ifs025_ensemble",
    "gfs": "ncep_gefs_seamless",
    "icon": "icon_seamless_eps",
    "gem": "gem_global_ensemble",
}

CITY_CONTINENTS: Mapping[str, str] = {
    "new york": "north_america", "nyc": "north_america", "chicago": "north_america",
    "seattle": "north_america", "atlanta": "north_america", "dallas": "north_america",
    "miami": "north_america", "los angeles": "north_america", "austin": "north_america",
    "houston": "north_america", "denver": "north_america", "san francisco": "north_america",
    "toronto": "north_america", "mexico city": "north_america", "panama city": "north_america",
    "ankara": "europe", "istanbul": "europe", "munich": "europe", "milan": "europe",
    "madrid": "europe", "warsaw": "europe", "amsterdam": "europe", "helsinki": "europe",
    "berlin": "europe", "moscow": "europe", "stockholm": "europe",
    "tokyo": "asia", "seoul": "asia", "shanghai": "asia", "singapore": "asia",
    "hong kong": "asia", "taipei": "asia", "wuhan": "asia", "lucknow": "asia",
    "kuala lumpur": "asia", "jakarta": "asia", "mumbai": "asia", "jeddah": "asia",
    "karachi": "asia", "busan": "asia", "qingdao": "asia", "guangzhou": "asia",
    "tel aviv": "asia", "buenos aires": "south_america", "sao paulo": "south_america",
    "cape town": "africa", "lagos": "africa", "sydney": "oceania",
}

CONTINENT_WEIGHTS: Mapping[str, Mapping[str, Decimal]] = {
    "north_america": {"nws": Decimal("0.45"), "open-meteo": Decimal("0.30"), "met-no": Decimal("0.15"), "jma": Decimal("0.10")},
    "europe": {"met-no": Decimal("0.45"), "open-meteo": Decimal("0.35"), "jma": Decimal("0.10"), "nws": Decimal("0.10")},
    "asia": {"jma": Decimal("0.40"), "open-meteo": Decimal("0.35"), "met-no": Decimal("0.20"), "nws": Decimal("0.05")},
    "oceania": {"open-meteo": Decimal("0.45"), "met-no": Decimal("0.35"), "jma": Decimal("0.10"), "nws": Decimal("0.10")},
    "south_america": {"open-meteo": Decimal("0.45"), "met-no": Decimal("0.35"), "jma": Decimal("0.10"), "nws": Decimal("0.10")},
    "africa": {"open-meteo": Decimal("0.45"), "met-no": Decimal("0.35"), "jma": Decimal("0.10"), "nws": Decimal("0.10")},
    "global": {"open-meteo": Decimal("0.35"), "met-no": Decimal("0.30"), "nws": Decimal("0.20"), "jma": Decimal("0.15")},
}

# Airport/station coordinates matching the locations used by weather contracts.
CITY_COORDS: Mapping[str, tuple[float, float]] = {
    "new york": (40.7772, -73.8726),
    "nyc": (40.7772, -73.8726),
    "chicago": (41.7868, -87.7522),
    "seattle": (47.4502, -122.3088),
    "atlanta": (33.6407, -84.4277),
    "dallas": (32.8998, -97.0403),
    "miami": (25.7959, -80.2870),
    "los angeles": (33.9425, -118.4081),
    "austin": (30.1975, -97.6664),
    "houston": (29.6454, -95.2789),
    "denver": (39.7174, -104.7506),
    "san francisco": (37.6213, -122.3790),
    "tokyo": (35.5494, 139.7798),
    "seoul": (37.4602, 126.4407),
    "shanghai": (31.1443, 121.8083),
    "toronto": (43.6772, -79.6306),
    "singapore": (1.3644, 103.9915),
    "hong kong": (22.3080, 113.9185),
    "taipei": (25.0777, 121.2328),
    "buenos aires": (-34.8222, -58.5358),
    "sao paulo": (-23.4356, -46.4731),
    "ankara": (40.1281, 32.9951),
    "istanbul": (40.9828, 28.8108),
    "munich": (48.3538, 11.7861),
    "tel aviv": (32.0114, 34.8867),
    "milan": (45.6306, 8.7231),
    "madrid": (40.4719, -3.5626),
    "warsaw": (52.1657, 20.9671),
    "wuhan": (30.7838, 114.2081),
    "lucknow": (26.7606, 80.8893),
    "mexico city": (19.4363, -99.0721),
    "amsterdam": (52.3086, 4.7639),
    "helsinki": (60.3172, 24.9633),
    "panama city": (9.0714, -79.3835),
    "kuala lumpur": (2.7456, 101.7099),
    "jakarta": (-6.1256, 106.6559),
    "berlin": (52.3667, 13.5033),
    "sydney": (-33.9461, 151.1772),
    "mumbai": (19.0896, 72.8656),
    "moscow": (55.9726, 37.4146),
    "stockholm": (59.6519, 17.9186),
    "cape town": (-33.9715, 18.6021),
    "jeddah": (21.6805, 39.1747),
    "lagos": (6.5774, 3.3215),
    "karachi": (24.9065, 67.1608),
    "busan": (35.1795, 128.9382),
    "qingdao": (36.2661, 120.3744),
    "guangzhou": (23.3924, 113.2988),
}

CITY_STATIONS: Mapping[str, str] = {
    "new york": "KLGA",
    "nyc": "KLGA",
    "chicago": "KMDW",
    "seattle": "KSEA",
    "atlanta": "KATL",
    "dallas": "KDFW",
    "miami": "KMIA",
    "los angeles": "KLAX",
    "austin": "KAUS",
    "houston": "KHOU",
    "denver": "KBKF",
    "san francisco": "KSFO",
    "tokyo": "RJTT",
    "seoul": "RKSI",
    "shanghai": "ZSPD",
    "toronto": "CYYZ",
    "singapore": "WSSS",
    "taipei": "RCTP",
    "buenos aires": "SAEZ",
    "sao paulo": "SBGR",
    "ankara": "LTAC",
    "munich": "EDDM",
    "tel aviv": "LLBG",
    "milan": "LIMC",
    "madrid": "LEMD",
    "warsaw": "EPWA",
    "wuhan": "ZHHH",
    "lucknow": "VILK",
    "mexico city": "MMMX",
    "amsterdam": "EHAM",
    "helsinki": "EFHK",
    "kuala lumpur": "WMKK",
    "jakarta": "WIII",
    "berlin": "EDDB",
    "sydney": "YSSY",
    "mumbai": "VABB",
    "stockholm": "ESSA",
    "cape town": "FACT",
    "jeddah": "OEJN",
    "lagos": "DNMM",
    "karachi": "OPKC",
    "busan": "RKPK",
    "qingdao": "ZSQD",
    "guangzhou": "ZGGG",
}

CITY_TIMEZONES: Mapping[str, str] = {
    "new york": "America/New_York",
    "nyc": "America/New_York",
    "chicago": "America/Chicago",
    "seattle": "America/Los_Angeles",
    "atlanta": "America/New_York",
    "dallas": "America/Chicago",
    "miami": "America/New_York",
    "los angeles": "America/Los_Angeles",
    "austin": "America/Chicago",
    "houston": "America/Chicago",
    "denver": "America/Denver",
    "san francisco": "America/Los_Angeles",
    "tokyo": "Asia/Tokyo",
    "seoul": "Asia/Seoul",
    "shanghai": "Asia/Shanghai",
    "toronto": "America/Toronto",
    "singapore": "Asia/Singapore",
    "taipei": "Asia/Taipei",
    "buenos aires": "America/Argentina/Buenos_Aires",
    "sao paulo": "America/Sao_Paulo",
    "ankara": "Europe/Istanbul",
    "munich": "Europe/Berlin",
    "tel aviv": "Asia/Jerusalem",
    "milan": "Europe/Rome",
    "madrid": "Europe/Madrid",
    "warsaw": "Europe/Warsaw",
    "wuhan": "Asia/Shanghai",
    "lucknow": "Asia/Kolkata",
    "mexico city": "America/Mexico_City",
    "amsterdam": "Europe/Amsterdam",
    "helsinki": "Europe/Helsinki",
    "kuala lumpur": "Asia/Kuala_Lumpur",
    "jakarta": "Asia/Jakarta",
    "berlin": "Europe/Berlin",
    "sydney": "Australia/Sydney",
    "mumbai": "Asia/Kolkata",
    "stockholm": "Europe/Stockholm",
    "cape town": "Africa/Johannesburg",
    "jeddah": "Asia/Riyadh",
    "lagos": "Africa/Lagos",
    "karachi": "Asia/Karachi",
    "busan": "Asia/Seoul",
    "qingdao": "Asia/Shanghai",
    "guangzhou": "Asia/Shanghai",
}

# Keep known station/model mismatches out until resolved paper history proves calibration.
AVOID_CITIES = frozenset({
    "beijing",
    "chengdu",
    "chongqing",
    "london",
    "paris",
    "shenzhen",
    "wellington",
})
CITY_ALIASES = {"nyc": "new york"}

_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_HIGH_QUESTION_RE = re.compile(
    r"^Will the highest temperature in (?P<city>.+?) be (?P<outcome>.+?) on "
    r"(?P<month>[A-Za-z]+) (?P<day>\d{1,2})\?$",
    re.IGNORECASE,
)
_EXACT_OUTCOME_RE = re.compile(
    r"^(?P<temperature>-?\d+)°(?P<unit>[CF])$",
    re.IGNORECASE,
)
_RANGE_OUTCOME_RE = re.compile(
    r"^between (?P<lower>-?\d+)\s*-\s*(?P<upper>-?\d+)°(?P<unit>[CF])$",
    re.IGNORECASE,
)
_TAIL_OUTCOME_RE = re.compile(
    r"^(?P<temperature>-?\d+)°(?P<unit>[CF]) or (?P<tail>higher|below|lower)$",
    re.IGNORECASE,
)


def _display_to_c(value: Decimal, unit: str) -> Decimal:
    if unit == "C":
        return value
    return (value - Decimal("32")) * Decimal("5") / Decimal("9")


@dataclass(frozen=True)
class HighTemperatureContract:
    """A whole-degree daily-high outcome in its display and model units."""

    city: str
    target_date: str
    unit: str
    display_lower: Decimal | None
    display_upper: Decimal | None
    probability_lower_c: Decimal | None
    probability_upper_c: Decimal | None

    def __post_init__(self) -> None:
        if self.unit not in {"C", "F"}:
            raise ValueError("weather contract unit must be C or F")
        if self.display_lower is None and self.display_upper is None:
            raise ValueError("weather contract must have at least one display bound")
        for bound in (self.display_lower, self.display_upper):
            if bound is not None and (
                not bound.is_finite() or bound != bound.to_integral_value()
            ):
                raise ValueError("weather contract display bounds must be finite whole degrees")
        if (
            self.display_lower is not None
            and self.display_upper is not None
            and self.display_lower > self.display_upper
        ):
            raise ValueError("weather contract display bounds are inverted")
        expected_lower = (
            None
            if self.display_lower is None
            else _display_to_c(self.display_lower - HALF, self.unit)
        )
        expected_upper = (
            None
            if self.display_upper is None
            else _display_to_c(self.display_upper + HALF, self.unit)
        )
        for bound in (self.probability_lower_c, self.probability_upper_c):
            if bound is not None and not bound.is_finite():
                raise ValueError("weather probability bounds must be finite")
        if (
            self.probability_lower_c != expected_lower
            or self.probability_upper_c != expected_upper
        ):
            raise ValueError("weather probability bounds must match display bounds and unit")
        if (
            self.probability_lower_c is not None
            and self.probability_upper_c is not None
            and self.probability_lower_c >= self.probability_upper_c
        ):
            raise ValueError("weather probability bounds must be ordered")

    @property
    def event_key(self) -> str:
        return f"weather:{self.city}:{self.target_date}"

    @property
    def is_exact(self) -> bool:
        return self.display_lower is not None and self.display_lower == self.display_upper

    @property
    def contract_kind(self) -> str:
        if self.is_exact:
            return "exact"
        if self.display_lower is None:
            return "lower_tail"
        if self.display_upper is None:
            return "upper_tail"
        return "range"

    @property
    def display_temperature(self) -> Decimal:
        """Compatibility accessor for contracts returned by the exact parser."""
        if not self.is_exact:
            raise AttributeError("non-exact weather contract has no display_temperature")
        assert self.display_lower is not None
        return self.display_lower

    @property
    def target_c(self) -> Decimal:
        """Compatibility accessor for contracts returned by the exact parser."""
        return _display_to_c(self.display_temperature, self.unit)


# Keep the public type name while using one generic frozen representation.
ExactHighContract = HighTemperatureContract


@dataclass(frozen=True)
class StationObservation:
    station_id: str
    observed_at: datetime
    temperature_c: Decimal
    display_temperature: Decimal


@dataclass(frozen=True)
class ObservationBoundResult:
    probability: Decimal
    same_day_observation_available: bool
    current_high_display: Decimal | None


class ObservationUnavailableError(RuntimeError):
    """A cached NOAA adapter failure is still inside its request backoff."""


class ForecastUnavailableError(RuntimeError):
    """Forecast data is unavailable without implying an empty candidate set."""

    def __init__(
        self,
        message: str,
        *,
        provider_global: bool = False,
        provider_covered: bool = True,
    ) -> None:
        super().__init__(message)
        self.provider_global = provider_global
        self.provider_covered = provider_covered


@dataclass(frozen=True)
class EnsembleForecast:
    raw_probability: Decimal
    ensemble_mean_c: Decimal
    ensemble_std_c: Decimal
    n_members: int
    lead_days: int
    model_count: int = 4
    distribution_probability: Decimal | None = None
    source: str = "provider"
    provider_count: int = 1
    provider_names: tuple[str, ...] = ()
    provider_probabilities: tuple[tuple[str, Decimal], ...] = ()
    provider_failures: tuple[tuple[str, str], ...] = ()
    calibration_samples: int = 0
    continent: str = "global"
    provider_weights: tuple[tuple[str, Decimal], ...] = ()


class ProbabilityCalibration:
    """Small, persistent, shrinkage calibrator for resolved paper forecasts.

    Calibration is deliberately conservative: until a source/city/horizon bucket
    has 20 resolved outcomes, its empirical rate is blended only partially with
    the raw model probability. This makes outages safe and prevents a handful of
    paper outcomes from overfitting the next forecast.
    """

    def __init__(self, path: Path | None = None, *, min_samples: int = 20) -> None:
        if min_samples < 1:
            raise ValueError("calibration minimum samples must be positive")
        self.path = path
        self.min_samples = min_samples
        self._bins: dict[str, dict[str, int]] = {}
        if path is not None and path.is_file():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                payload = {}
            if isinstance(payload, dict):
                for key, value in payload.items():
                    if isinstance(value, dict):
                        try:
                            self._bins[str(key)] = {
                                "successes": int(value.get("successes", 0)),
                                "total": int(value.get("total", 0)),
                            }
                        except (TypeError, ValueError):
                            continue

    @staticmethod
    def _bucket(probability: Decimal) -> int:
        return min(9, max(0, int(probability * Decimal("10"))))

    def _key(self, source: str, city: str, lead_days: int, probability: Decimal) -> str:
        return f"{source}:{city}:{lead_days}:{self._bucket(probability)}"

    def calibrate(
        self,
        source: str,
        city: str,
        lead_days: int,
        probability: Decimal,
    ) -> Decimal:
        if not ZERO <= probability <= ONE:
            raise ValueError("forecast probability must be in [0, 1]")
        bucket = self._bins.get(self._key(source, city, lead_days, probability))
        if not bucket or bucket["total"] <= 0:
            return probability
        total = bucket["total"]
        empirical = Decimal(bucket["successes"] + 1) / Decimal(total + 2)
        blend = min(ONE, Decimal(total) / Decimal(self.min_samples))
        return min(ONE, max(ZERO, probability * (ONE - blend) + empirical * blend))

    def record(
        self,
        source: str,
        city: str,
        lead_days: int,
        probability: Decimal,
        outcome: int,
    ) -> None:
        if outcome not in {0, 1} or not ZERO <= probability <= ONE:
            raise ValueError("invalid calibration observation")
        key = self._key(source, city, lead_days, probability)
        bucket = self._bins.setdefault(key, {"successes": 0, "total": 0})
        bucket["successes"] += outcome
        bucket["total"] += 1
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_suffix(self.path.suffix + ".tmp")
            temporary.write_text(
                json.dumps(self._bins, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            temporary.replace(self.path)

    def samples(self) -> int:
        return sum(item["total"] for item in self._bins.values())


@dataclass(frozen=True)
class WeatherPaperPolicy:
    enabled: bool = True
    horizon_days: int = 3
    discovery_limit: int = 1_500
    market_limit: int = 100
    min_liquidity: Decimal = Decimal("1000")
    min_price: Decimal = Decimal("0.02")
    max_price: Decimal = Decimal("0.98")
    max_order_notional: Decimal = Decimal("5")
    max_open_positions: int = 15
    base_edge: Decimal = Decimal("0.03")
    intraclass_correlation: Decimal = Decimal("0.05")
    prior_strength: Decimal = Decimal("10")
    fractional_kelly: Decimal = Decimal("0.05")
    uncertainty_z: Decimal = ONE
    observations_enabled: bool = False
    require_healthy_forecast: bool = False

    def __post_init__(self) -> None:
        if self.horizon_days < 1 or self.horizon_days > 14:
            raise ValueError("weather horizon must be in [1, 14]")
        if not (1 <= self.discovery_limit <= 5_000):
            raise ValueError("weather discovery limit must be in [1, 5000]")
        if not (1 <= self.market_limit <= 500):
            raise ValueError("weather market limit must be in [1, 500]")
        if not (ZERO < self.min_price <= self.max_price < ONE):
            raise ValueError("weather price range must be inside (0, 1)")
        if self.max_order_notional <= ZERO:
            raise ValueError("weather paper order cap must be positive")
        if not (1 <= self.max_open_positions <= 20):
            raise ValueError("weather paper position cap must be in [1, 20]")
        if self.base_edge < ZERO or self.uncertainty_z < ZERO:
            raise ValueError("weather edge and uncertainty settings cannot be negative")
        if not (ZERO <= self.intraclass_correlation < ONE):
            raise ValueError("weather ICC must be in [0, 1)")
        if self.prior_strength < ZERO:
            raise ValueError("weather prior strength cannot be negative")
        if not (ZERO < self.fractional_kelly <= ONE):
            raise ValueError("weather fractional Kelly must be in (0, 1]")


@dataclass(frozen=True)
class WeatherEvaluation:
    strategy: str
    event_key: str
    market_id: str
    condition_id: str
    question: str
    side: str
    token_id: str
    city: str
    target_date: str
    target_c: Decimal | None
    unit: str
    display_lower: Decimal | None
    display_upper: Decimal | None
    contract_kind: str
    bid: Decimal
    ask: Decimal
    shares: Decimal
    fee: Decimal
    all_in_cost: Decimal
    raw_probability: Decimal
    forecast: EnsembleForecast
    decision: WeatherDecision
    paper_tradeable: bool
    paper_reason: str
    same_day_contract: bool = False
    same_day_observation_available: bool = False
    current_high_display: Decimal | None = None
    observation_error: str | None = None
    same_day_observation_status: str = "not_applicable"
    maker_shadow: MakerShadowQuote | None = None


@dataclass(frozen=True)
class WeatherEventContract:
    market_id: str
    condition_id: str
    question: str
    yes_token_id: str
    contract_kind: str
    display_lower: Decimal | None
    display_upper: Decimal | None
    model_probability: Decimal | None
    maker_shadow: MakerShadowQuote | None


@dataclass(frozen=True)
class WeatherEventEvaluation:
    event_key: str
    unit: str
    contracts: tuple[WeatherEventContract, ...]
    surface: EventSurface
    negative_risk_verified: bool
    resolution_station_verified: bool
    unit_verified: bool = True
    parsed_event_membership_verified: bool = True
    event_membership_verified: bool = False
    public_data_only: bool = True
    execution_status: str = "not_executed"


@dataclass(frozen=True)
class WeatherUniverseResult:
    markets_discovered: int
    markets_evaluated: int
    evaluations: tuple[WeatherEvaluation, ...]
    errors: tuple[str, ...] = field(default_factory=tuple)
    events: tuple[WeatherEventEvaluation, ...] = field(default_factory=tuple)
    observations_available: int = 0
    observation_errors: int = 0
    markets_forecast_unavailable: int = 0
    markets_modeled: int = 0
    markets_side_evaluable: int = 0
    forecast_status: str = "not_requested"
    forecast_errors: tuple[str, ...] = field(default_factory=tuple)
    provider_names: tuple[str, ...] = field(default_factory=tuple)
    provider_failures: tuple[tuple[str, str], ...] = field(default_factory=tuple)


class WeatherPublicClient(Protocol):
    async def get_tag(self, *, slug: str) -> Any: ...

    def list_markets(self, **kwargs: Any) -> Any: ...

    async def get_order_books(self, *, token_ids: list[str]) -> tuple[Any, ...]: ...


GammaJsonFetcher = Callable[..., Any]


def _default_gamma_fetch_json(
    url: str,
    *,
    params: Mapping[str, Any],
    headers: Mapping[str, str],
    timeout: float,
) -> Any:
    response = requests.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


class _OffsetMarketPaginator:
    def __init__(
        self,
        *,
        fetch_json: GammaJsonFetcher,
        params: Mapping[str, Any],
        page_size: int,
        timeout_seconds: float,
        user_agent: str,
    ) -> None:
        self._fetch_json = fetch_json
        self._params = dict(params)
        self._page_size = page_size
        self._timeout_seconds = timeout_seconds
        self._user_agent = user_agent

    def iter_items(self):
        async def iterate():
            offset = 0
            seen_ids: set[str] = set()
            while True:
                params = dict(self._params)
                params.update({"limit": self._page_size, "offset": offset})
                payload = await asyncio.to_thread(
                    self._fetch_json,
                    "https://gamma-api.polymarket.com/markets",
                    params=params,
                    headers={
                        "User-Agent": self._user_agent,
                        "Accept": "application/json",
                    },
                    timeout=self._timeout_seconds,
                )
                markets = Market.parse_response_list(payload)
                if not markets:
                    return
                yielded = 0
                for market in markets:
                    market_id = str(getattr(market, "id", ""))
                    if market_id and market_id in seen_ids:
                        continue
                    if market_id:
                        seen_ids.add(market_id)
                    yielded += 1
                    yield market
                offset += len(markets)
                if len(markets) < self._page_size or yielded == 0:
                    return

        return iterate()


class OffsetWeatherPublicClient:
    """Read-only weather market client using Gamma offset pagination.

    The official SDK's market paginator uses Gamma keyset cursors. Gamma's
    Weather-tag cursor continuation is currently rejected by Cloudflare, while
    the offset endpoint remains available. This adapter changes only the weather
    discovery route; tags and order books still use the official public client.
    """

    def __init__(
        self,
        public_client: WeatherPublicClient,
        *,
        fetch_json: GammaJsonFetcher = _default_gamma_fetch_json,
        page_size: int = 100,
        timeout_seconds: float = 20,
        user_agent: str = "polymarket-bot-weather-research/4.1",
    ) -> None:
        if not 1 <= page_size <= 100:
            raise ValueError("Gamma offset page size must be in [1, 100]")
        if timeout_seconds <= 0 or not user_agent.strip():
            raise ValueError("invalid Gamma offset client settings")
        self._public_client = public_client
        self._fetch_json = fetch_json
        self._page_size = page_size
        self._timeout_seconds = timeout_seconds
        self._user_agent = user_agent

    async def get_tag(self, *, slug: str) -> Any:
        return await self._public_client.get_tag(slug=slug)

    async def get_order_books(self, *, token_ids: list[str]) -> tuple[Any, ...]:
        return await self._public_client.get_order_books(token_ids=token_ids)

    def list_markets(self, **kwargs: Any) -> _OffsetMarketPaginator:
        page_size = int(kwargs.pop("page_size", self._page_size))
        if not 1 <= page_size <= 100:
            raise ValueError("Gamma offset page size must be in [1, 100]")
        params: dict[str, Any] = {}
        for key, value in kwargs.items():
            if value is None:
                continue
            if isinstance(value, bool):
                params[key] = "true" if value else "false"
            else:
                params[key] = value.isoformat() if isinstance(value, datetime) else value
        return _OffsetMarketPaginator(
            fetch_json=self._fetch_json,
            params=params,
            page_size=page_size,
            timeout_seconds=self._timeout_seconds,
            user_agent=self._user_agent,
        )


class ForecastProvider(Protocol):
    async def forecast(
        self,
        contract: HighTemperatureContract,
        *,
        now: datetime | None = None,
    ) -> EnsembleForecast | None: ...


class ObservationProvider(Protocol):
    async def adjust_probability(
        self,
        contract: HighTemperatureContract,
        base_probability: Decimal,
        *,
        station_id: str,
    ) -> ObservationBoundResult: ...


def _contract_target_date(match: re.Match[str], end_date: datetime) -> date | None:
    month = _MONTHS.get(match.group("month").lower())
    if month is None:
        return None
    day = int(match.group("day"))
    candidates = []
    for year in (end_date.year - 1, end_date.year, end_date.year + 1):
        try:
            candidates.append(date(year, month, day))
        except ValueError:
            continue
    if not candidates:
        return None
    target = min(candidates, key=lambda value: abs((value - end_date.date()).days))
    if abs((target - end_date.date()).days) > 2:
        return None
    return target


def parse_high_temperature_contract(
    question: str | None,
    *,
    end_date: datetime | None,
) -> HighTemperatureContract | None:
    """Parse modeled whole-degree daily-high exact, range, and tail outcomes."""
    if not question or end_date is None:
        return None
    match = _HIGH_QUESTION_RE.fullmatch(question.strip())
    if match is None:
        return None
    raw_city = match.group("city").strip().lower()
    city = CITY_ALIASES.get(raw_city, raw_city)
    if city in AVOID_CITIES or city not in CITY_COORDS:
        return None
    target = _contract_target_date(match, end_date)
    if target is None:
        return None

    outcome = match.group("outcome").strip()
    exact = _EXACT_OUTCOME_RE.fullmatch(outcome)
    bounded = _RANGE_OUTCOME_RE.fullmatch(outcome)
    tail = _TAIL_OUTCOME_RE.fullmatch(outcome)
    unit: str
    display_lower: Decimal | None
    display_upper: Decimal | None
    if exact is not None:
        unit = exact.group("unit").upper()
        display_lower = display_upper = Decimal(exact.group("temperature"))
    elif bounded is not None:
        unit = bounded.group("unit").upper()
        display_lower = Decimal(bounded.group("lower"))
        display_upper = Decimal(bounded.group("upper"))
        if display_lower >= display_upper:
            return None
    elif tail is not None:
        unit = tail.group("unit").upper()
        threshold = Decimal(tail.group("temperature"))
        if tail.group("tail").lower() == "higher":
            display_lower, display_upper = threshold, None
        else:
            display_lower, display_upper = None, threshold
    else:
        return None

    probability_lower_c = (
        _display_to_c(display_lower - HALF, unit)
        if display_lower is not None
        else None
    )
    probability_upper_c = (
        _display_to_c(display_upper + HALF, unit)
        if display_upper is not None
        else None
    )
    return HighTemperatureContract(
        city=city,
        target_date=target.isoformat(),
        unit=unit,
        display_lower=display_lower,
        display_upper=display_upper,
        probability_lower_c=probability_lower_c,
        probability_upper_c=probability_upper_c,
    )


def parse_exact_high_contract(
    question: str | None,
    *,
    end_date: datetime | None,
) -> ExactHighContract | None:
    """Compatibility wrapper that intentionally accepts exact outcomes only."""
    contract = parse_high_temperature_contract(question, end_date=end_date)
    if contract is None or not contract.is_exact:
        return None
    match = _HIGH_QUESTION_RE.fullmatch((question or "").strip())
    if match is None or _EXACT_OUTCOME_RE.fullmatch(match.group("outcome").strip()) is None:
        return None
    return contract


ObservationJsonFetcher = Callable[..., Any]


def _default_noaa_fetch_json(
    url: str,
    *,
    params: Mapping[str, Any],
    headers: Mapping[str, str],
    timeout: float,
) -> Any:
    response = requests.get(url, params=params, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


JsonFetcher = Callable[..., Mapping[str, Any]]


def _default_fetch_json(
    url: str,
    *,
    params: Mapping[str, Any],
    timeout: float,
) -> Mapping[str, Any]:
    response = requests.get(
        url,
        params=params,
        headers={
            "User-Agent": "polymarket-bot-weather-research/5.0",
            "Accept": "application/json",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, Mapping):
        raise ValueError("weather provider response must be a JSON object")
    return payload


def _parse_report_time(raw_value: Any) -> datetime:
    if not isinstance(raw_value, str):
        raise ValueError("NOAA METAR reportTime must be an ISO timestamp")
    try:
        observed_at = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("NOAA METAR reportTime must be an ISO timestamp") from exc
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("NOAA METAR reportTime must include a timezone")
    return observed_at


def _round_display_temperature(temperature_c: Decimal, unit: str) -> Decimal:
    display = (
        temperature_c
        if unit == "C"
        else temperature_c * Decimal("9") / Decimal("5") + Decimal("32")
    )
    return display.quantize(ONE, rounding=ROUND_HALF_UP)


def apply_observation_bounds(
    contract: HighTemperatureContract,
    base_probability: Decimal,
    observations: Sequence[StationObservation] | None,
) -> ObservationBoundResult:
    """Apply only resolver-certain implications of an observed running high."""
    if not ZERO <= base_probability <= ONE:
        raise ValueError("base weather probability must be in [0, 1]")
    if not observations:
        return ObservationBoundResult(base_probability, False, None)

    current_high = max(item.display_temperature for item in observations)
    if contract.display_upper is not None and current_high > contract.display_upper:
        probability = ZERO
    elif (
        contract.display_upper is None
        and contract.display_lower is not None
        and current_high >= contract.display_lower
    ):
        probability = ONE
    else:
        probability = base_probability
    return ObservationBoundResult(probability, True, current_high)


class NOAAStationObservations:
    """Cached public METAR readings for a resolver-verified station.

    ``observations`` returns a (possibly empty) validated tuple on a successful
    NOAA response and raises on adapter/request failure. During failure backoff it
    raises ``ObservationUnavailableError`` rather than returning an ambiguous
    ``None``. Concurrent callers for the same station/date/unit share one request.
    """

    def __init__(
        self,
        *,
        fetch_json: ObservationJsonFetcher = _default_noaa_fetch_json,
        hours: int = 24,
        user_agent: str = "polymarket-bot-weather-research/4.0",
        cache_seconds: float = 300,
        timeout_seconds: float = 20,
        failure_backoff_seconds: float = 300,
    ) -> None:
        if not 1 <= hours <= 24:
            raise ValueError("NOAA METAR hours must be in [1, 24]")
        if not user_agent.strip():
            raise ValueError("NOAA METAR user agent cannot be empty")
        if cache_seconds <= 0 or timeout_seconds <= 0 or failure_backoff_seconds <= 0:
            raise ValueError("invalid NOAA METAR client settings")
        self._fetch_json = fetch_json
        self._hours = hours
        self._user_agent = user_agent
        self._cache_seconds = cache_seconds
        self._timeout_seconds = timeout_seconds
        self._failure_backoff_seconds = failure_backoff_seconds
        self._cache: dict[
            tuple[str, str, str],
            tuple[float, tuple[StationObservation, ...]],
        ] = {}
        self._failure_until: dict[tuple[str, str, str], float] = {}
        self._failure_messages: dict[tuple[str, str, str], str] = {}
        self._inflight: dict[
            tuple[str, str, str],
            asyncio.Task[tuple[StationObservation, ...]],
        ] = {}

    def _prune_expired(self, monotonic_now: float) -> None:
        for key, (expires_at, _observations) in tuple(self._cache.items()):
            if expires_at <= monotonic_now:
                self._cache.pop(key, None)
        for key, expires_at in tuple(self._failure_until.items()):
            if expires_at <= monotonic_now:
                self._failure_until.pop(key, None)
                self._failure_messages.pop(key, None)

    async def _request_observations(
        self,
        contract: HighTemperatureContract,
        *,
        station_id: str,
        timezone_name: str,
        cache_key: tuple[str, str, str],
    ) -> tuple[StationObservation, ...]:
        try:
            payload = await asyncio.to_thread(
                self._fetch_json,
                NOAA_METAR,
                params={"ids": station_id, "format": "json", "hours": self._hours},
                headers={"User-Agent": self._user_agent},
                timeout=self._timeout_seconds,
            )
            if not isinstance(payload, list):
                raise ValueError("NOAA METAR response must be a JSON list")
            target_date = date.fromisoformat(contract.target_date)
            local_timezone = ZoneInfo(timezone_name)
            parsed: list[StationObservation] = []
            for raw in payload:
                if not isinstance(raw, dict):
                    raise ValueError("NOAA METAR observation must be an object")
                raw_station = raw.get("icaoId")
                if not isinstance(raw_station, str) or raw_station.upper() != station_id:
                    raise ValueError("NOAA METAR observation station does not match request")
                observed_at = _parse_report_time(raw.get("reportTime"))
                raw_temperature = raw.get("temp")
                # NOAA returns null for an otherwise valid report when the
                # station did not publish a temperature. Ignore that report;
                # retain strict validation for malformed non-null values.
                if raw_temperature is None:
                    continue
                if (
                    isinstance(raw_temperature, bool)
                    or not isinstance(raw_temperature, (int, float, Decimal))
                ):
                    raise ValueError("NOAA METAR temp must be numeric Celsius")
                try:
                    temperature_c = Decimal(str(raw_temperature))
                except Exception as exc:
                    raise ValueError("NOAA METAR temp must be numeric Celsius") from exc
                if not temperature_c.is_finite():
                    raise ValueError("NOAA METAR temp must be finite Celsius")
                if observed_at.astimezone(local_timezone).date() != target_date:
                    continue
                parsed.append(StationObservation(
                    station_id=station_id,
                    observed_at=observed_at,
                    temperature_c=temperature_c,
                    display_temperature=_round_display_temperature(temperature_c, contract.unit),
                ))
        except Exception as exc:
            self._failure_until[cache_key] = (
                time.monotonic() + self._failure_backoff_seconds
            )
            self._failure_messages[cache_key] = f"{type(exc).__name__}: {exc}"
            raise

        observations = tuple(sorted(parsed, key=lambda item: item.observed_at))
        self._failure_until.pop(cache_key, None)
        self._failure_messages.pop(cache_key, None)
        self._cache[cache_key] = (
            time.monotonic() + self._cache_seconds,
            observations,
        )
        return observations

    async def observations(
        self,
        contract: HighTemperatureContract,
        *,
        station_id: str,
    ) -> tuple[StationObservation, ...]:
        expected_station = CITY_STATIONS.get(contract.city)
        timezone_name = CITY_TIMEZONES.get(contract.city)
        verified_station = station_id.strip().upper()
        if (
            expected_station is None
            or timezone_name is None
            or verified_station != expected_station
        ):
            raise ValueError("NOAA observations require the resolver-verified station identity")

        cache_key = (verified_station, contract.target_date, contract.unit)
        monotonic_now = time.monotonic()
        self._prune_expired(monotonic_now)
        if monotonic_now < self._failure_until.get(cache_key, 0):
            detail = self._failure_messages.get(cache_key, "prior request failed")
            raise ObservationUnavailableError(
                f"NOAA observation request backoff active: {detail}"
            )
        cached = self._cache.get(cache_key)
        if cached is not None and monotonic_now < cached[0]:
            return cached[1]

        task = self._inflight.get(cache_key)
        if task is None:
            task = asyncio.create_task(self._request_observations(
                contract,
                station_id=verified_station,
                timezone_name=timezone_name,
                cache_key=cache_key,
            ))
            self._inflight[cache_key] = task
        try:
            return await asyncio.shield(task)
        finally:
            if task.done() and self._inflight.get(cache_key) is task:
                self._inflight.pop(cache_key, None)

    async def adjust_probability(
        self,
        contract: HighTemperatureContract,
        base_probability: Decimal,
        *,
        station_id: str,
    ) -> ObservationBoundResult:
        observations = await self.observations(contract, station_id=station_id)
        return apply_observation_bounds(contract, base_probability, observations)


def _verified_resolution_station(
    contract: HighTemperatureContract,
    source: str | None,
) -> str | None:
    expected = CITY_STATIONS.get(contract.city)
    if not expected or not source:
        return None
    parsed = urlparse(source)
    if parsed.scheme != "https" or parsed.hostname != "www.weather.gov":
        return None
    sites = parse_qs(parsed.query).get("site", ())
    if not sites or sites[0].upper() != expected:
        return None
    return expected


def _resolution_station_matches(
    contract: HighTemperatureContract,
    source: str | None,
) -> bool:
    return _verified_resolution_station(contract, source) is not None


def _ensemble_probability(
    contract: HighTemperatureContract,
    model_members: tuple[tuple[float, ...], ...],
    lead_days: int,
) -> EnsembleForecast:
    inflation = 1.05 + 0.15 * max(0, lead_days)
    lower_c = (
        float(contract.probability_lower_c)
        if contract.probability_lower_c is not None
        else None
    )
    upper_c = (
        float(contract.probability_upper_c)
        if contract.probability_upper_c is not None
        else None
    )
    model_probabilities: list[float] = []
    model_means: list[float] = []
    model_variances: list[float] = []
    for members in model_members:
        model_mean = statistics.mean(members)
        model_std = statistics.stdev(members) if len(members) >= 2 else 0.0
        sigma = max(model_std * inflation, 0.5)
        distribution = NormalDist(mu=model_mean, sigma=sigma)
        if lower_c is None:
            assert upper_c is not None
            probability = distribution.cdf(upper_c)
        elif upper_c is None:
            probability = 1 - distribution.cdf(lower_c)
        else:
            probability = distribution.cdf(upper_c) - distribution.cdf(lower_c)
        model_probabilities.append(probability)
        model_means.append(model_mean)
        model_variances.append(model_std ** 2)
    distribution_probability = statistics.mean(model_probabilities)
    directional_probability = min(
        0.999,
        max(0.001, distribution_probability),
    )
    mean = statistics.mean(model_means)
    mixture_second_moment = statistics.mean(
        variance + model_mean ** 2
        for variance, model_mean in zip(model_variances, model_means, strict=True)
    )
    raw_std = math.sqrt(max(0, mixture_second_moment - mean ** 2))
    return EnsembleForecast(
        raw_probability=Decimal(str(directional_probability)),
        ensemble_mean_c=Decimal(str(mean)),
        ensemble_std_c=Decimal(str(raw_std)),
        n_members=sum(len(members) for members in model_members),
        lead_days=lead_days,
        model_count=len(model_members),
        distribution_probability=Decimal(str(distribution_probability)),
    )


class OpenMeteoEnsemble:
    """Quota-capped Open-Meteo multi-model forecast adapter."""

    name = "open-meteo"

    def __init__(
        self,
        *,
        fetch_json: JsonFetcher = _default_fetch_json,
        min_members: int = 10,
        cache_seconds: float = 21_600,
        timeout_seconds: float = 20,
        failure_backoff_seconds: float = 300,
        max_requests_per_day: int = 24,
        quota_path: Path | None = None,
    ) -> None:
        if (
            min_members < 2
            or cache_seconds <= 0
            or timeout_seconds <= 0
            or failure_backoff_seconds <= 0
            or max_requests_per_day < 1
        ):
            raise ValueError("invalid Open-Meteo client settings")
        self._fetch_json = fetch_json
        self._min_members = min_members
        self._cache_seconds = cache_seconds
        self._timeout_seconds = timeout_seconds
        self._failure_backoff_seconds = failure_backoff_seconds
        self._max_requests_per_day = max_requests_per_day
        self._quota_path = quota_path
        self._cache: dict[
            tuple[str, str],
            tuple[float, tuple[tuple[float, ...], ...]],
        ] = {}
        self._request_times: list[float] = []
        self._failure_until: dict[tuple[str, str], float] = {}
        self._failure_messages: dict[tuple[str, str], str] = {}
        self._load_quota_state()

    def _load_quota_state(self) -> None:
        if self._quota_path is None or not self._quota_path.is_file():
            return
        try:
            payload = json.loads(self._quota_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        if not isinstance(payload, dict):
            return
        now = time.time()
        raw_times = payload.get("request_times", [])
        if isinstance(raw_times, list):
            self._request_times = [
                float(value)
                for value in raw_times
                if isinstance(value, (int, float)) and now - float(value) < 86_400
            ]
        raw_cache = payload.get("cache", {})
        if not isinstance(raw_cache, dict):
            return
        for raw_key, raw_value in raw_cache.items():
            if not isinstance(raw_value, dict):
                continue
            expires_at = raw_value.get("expires_at")
            raw_members = raw_value.get("members")
            if not isinstance(expires_at, (int, float)) or expires_at <= now:
                continue
            if not isinstance(raw_members, list):
                continue
            try:
                members = tuple(
                    tuple(float(value) for value in family)
                    for family in raw_members
                    if isinstance(family, list)
                )
            except (TypeError, ValueError):
                continue
            cache_key = str(raw_key).split("|", 1)
            if len(members) == len(MODEL_KEY_MARKERS) and len(cache_key) == 2:
                self._cache[(cache_key[0], cache_key[1])] = (
                    float(expires_at),
                    members,
                )

    def _save_quota_state(self) -> None:
        if self._quota_path is None:
            return
        self._quota_path.parent.mkdir(parents=True, exist_ok=True)
        now = time.time()
        cache = {
            "|".join(key): {
                "expires_at": expires_at,
                "members": [list(family) for family in members],
            }
            for key, (expires_at, members) in self._cache.items()
            if expires_at > now
        }
        payload = {
            "request_times": [value for value in self._request_times if now - value < 86_400],
            "cache": cache,
        }
        temporary = self._quota_path.with_suffix(self._quota_path.suffix + ".tmp")
        temporary.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(self._quota_path)

    def _prune(self, now: float) -> None:
        self._request_times = [value for value in self._request_times if now - value < 86_400]
        for key, (expires_at, _members) in tuple(self._cache.items()):
            if expires_at <= now:
                self._cache.pop(key, None)
        for key, expires_at in tuple(self._failure_until.items()):
            if expires_at <= now:
                self._failure_until.pop(key, None)
                self._failure_messages.pop(key, None)

    def _reserve_request(self, now: float) -> None:
        self._prune(now)
        if len(self._request_times) >= self._max_requests_per_day:
            raise ForecastUnavailableError(
                f"Open-Meteo application quota exhausted ({self._max_requests_per_day} requests/24h)",
                provider_global=True,
            )
        self._request_times.append(now)
        self._save_quota_state()

    @property
    def requests_last_24h(self) -> int:
        self._prune(time.time())
        return len(self._request_times)

    async def forecast(
        self,
        contract: HighTemperatureContract,
        *,
        now: datetime | None = None,
    ) -> EnsembleForecast:
        now = now or datetime.now(timezone.utc)
        cache_key = (contract.city, contract.target_date)
        monotonic_now = time.monotonic()
        cached = self._cache.get(cache_key)
        if cached is not None and cached[0] > time.time():
            model_members = cached[1]
        else:
            failure_until = self._failure_until.get(cache_key, 0.0)
            if monotonic_now < failure_until:
                raise ForecastUnavailableError(
                    f"Open-Meteo forecast backoff active for {contract.event_key}: "
                    f"{self._failure_messages.get(cache_key, 'prior request failed')}"
                )
            self._reserve_request(time.time())
            latitude, longitude = CITY_COORDS[contract.city]
            try:
                payload = await asyncio.to_thread(
                    self._fetch_json,
                    OPEN_METEO_ENSEMBLE,
                    params={
                        "latitude": latitude,
                        "longitude": longitude,
                        "daily": "temperature_2m_max",
                        "models": ENSEMBLE_MODELS,
                        "timezone": "auto",
                        "start_date": contract.target_date,
                        "end_date": contract.target_date,
                    },
                    timeout=self._timeout_seconds,
                )
                daily = payload.get("daily")
                units = payload.get("daily_units")
                if not isinstance(daily, dict) or not isinstance(units, dict):
                    raise ValueError("Open-Meteo response has no daily object")
                if daily.get("time") != [contract.target_date]:
                    raise ValueError("Open-Meteo response date does not match the request")
                groups: dict[str, list[float]] = {
                    name: [] for name in MODEL_KEY_MARKERS
                }
                for raw_key, raw_values in daily.items():
                    key = str(raw_key)
                    if key == "time":
                        continue
                    if not key.startswith("temperature_2m_max"):
                        raise ValueError(f"unexpected Open-Meteo daily field: {key}")
                    model = next(
                        (
                            name
                            for name, marker in MODEL_KEY_MARKERS.items()
                            if marker in key
                        ),
                        None,
                    )
                    if model is None or units.get(key) != "°C":
                        raise ValueError(f"invalid Open-Meteo model field: {key}")
                    if (
                        not isinstance(raw_values, list)
                        or len(raw_values) != 1
                        or raw_values[0] is None
                        or isinstance(raw_values[0], bool)
                    ):
                        raise ValueError(f"invalid Open-Meteo values for {key}")
                    value = float(raw_values[0])
                    if not math.isfinite(value):
                        raise ValueError(f"non-finite Open-Meteo value for {key}")
                    groups[model].append(value)
                if any(len(values) < 2 for values in groups.values()):
                    raise ValueError("Open-Meteo response omitted an ensemble model family")
                total_members = sum(len(values) for values in groups.values())
                if total_members < self._min_members:
                    raise ValueError(f"Open-Meteo returned only {total_members} ensemble members")
                model_members = tuple(tuple(groups[name]) for name in MODEL_KEY_MARKERS)
            except Exception as exc:
                detail = f"{type(exc).__name__}: {exc}"
                self._failure_until[cache_key] = monotonic_now + self._failure_backoff_seconds
                self._failure_messages[cache_key] = detail
                raise ForecastUnavailableError(
                    f"Open-Meteo forecast unavailable for {contract.event_key}: {detail}",
                    provider_global=getattr(getattr(exc, "response", None), "status_code", None) == 429,
                ) from exc
            self._failure_until.pop(cache_key, None)
            self._failure_messages.pop(cache_key, None)
            self._cache[cache_key] = (
                time.time() + self._cache_seconds,
                model_members,
            )
            self._save_quota_state()
        target = date.fromisoformat(contract.target_date)
        timezone_name = CITY_TIMEZONES.get(contract.city)
        if timezone_name is None:
            raise ValueError(f"no resolver timezone for {contract.city}")
        lead_days = max(0, (target - now.astimezone(ZoneInfo(timezone_name)).date()).days)
        result = _ensemble_probability(contract, model_members, lead_days)
        return replace(
            result,
            source=self.name,
            provider_count=1,
            provider_names=(self.name,),
            provider_probabilities=((self.name, result.raw_probability),),
            continent=CITY_CONTINENTS.get(contract.city, "global"),
            provider_weights=((self.name, ONE),),
        )


class JMAForecast:
    """Direct Japan Meteorological Agency forecast adapter for Tokyo."""

    name = "jma"
    covered_cities = frozenset({"tokyo"})
    area_codes = {"tokyo": "130000"}
    temperature_area_codes = {"tokyo": "44132"}

    def __init__(
        self,
        *,
        fetch_json: Callable[..., Any] = _default_noaa_fetch_json,
        cache_seconds: float = 21_600,
        timeout_seconds: float = 20,
        user_agent: str = "polymarket-bot-weather-research/5.0",
    ) -> None:
        if cache_seconds <= 0 or timeout_seconds <= 0 or not user_agent.strip():
            raise ValueError("invalid JMA client settings")
        self._fetch_json = fetch_json
        self._cache_seconds = cache_seconds
        self._timeout_seconds = timeout_seconds
        self._user_agent = user_agent
        self._cache: dict[tuple[str, str], tuple[float, float]] = {}

    async def forecast(
        self,
        contract: HighTemperatureContract,
        *,
        now: datetime | None = None,
    ) -> EnsembleForecast:
        now = now or datetime.now(timezone.utc)
        if contract.city not in self.covered_cities:
            raise ForecastUnavailableError(
                f"JMA has no configured coverage for {contract.city}",
                provider_covered=False,
            )
        key = (contract.city, contract.target_date)
        cached = self._cache.get(key)
        if cached is not None and cached[0] > time.monotonic():
            maximum = cached[1]
        else:
            try:
                payload = await asyncio.to_thread(
                    self._fetch_json,
                    f"https://www.jma.go.jp/bosai/forecast/data/forecast/{self.area_codes[contract.city]}.json",
                    params={},
                    headers={"User-Agent": self._user_agent, "Accept": "application/json"},
                    timeout=self._timeout_seconds,
                )
                if not isinstance(payload, list):
                    raise ValueError("JMA response must be a JSON list")
                values: list[float] = []
                for report in payload:
                    if not isinstance(report, dict):
                        continue
                    for series in report.get("timeSeries", []):
                        if not isinstance(series, dict):
                            continue
                        times = series.get("timeDefines", [])
                        for area in series.get("areas", []):
                            if not isinstance(area, dict):
                                continue
                            area_info = area.get("area", {})
                            if str(area_info.get("code", "")) != self.temperature_area_codes[contract.city]:
                                continue
                            temperatures = area.get("temps")
                            if isinstance(temperatures, list):
                                for raw_time, raw_temperature in zip(times, temperatures):
                                    if (
                                        isinstance(raw_time, str)
                                        and raw_time[:10] == contract.target_date
                                        and raw_temperature not in (None, "")
                                    ):
                                        value = float(raw_temperature)
                                        if math.isfinite(value):
                                            values.append(value)
                            maxima = area.get("tempsMax")
                            if not isinstance(maxima, list):
                                continue
                            for index, raw_time in enumerate(times):
                                if (
                                    isinstance(raw_time, str)
                                    and raw_time[:10] == contract.target_date
                                    and index < len(maxima)
                                    and maxima[index] not in (None, "")
                                ):
                                    value = float(maxima[index])
                                    if math.isfinite(value):
                                        values.append(value)
                if not values:
                    raise ValueError("JMA response has no target-date maximum temperature")
                maximum = max(values)
            except Exception as exc:
                detail = f"{type(exc).__name__}: {exc}"
                raise ForecastUnavailableError(
                    f"JMA forecast unavailable for {contract.event_key}: {detail}",
                    provider_global=getattr(getattr(exc, "response", None), "status_code", None) == 429,
                ) from exc
            self._cache[key] = (time.monotonic() + self._cache_seconds, maximum)
        return _deterministic_forecast(contract, maximum, now=now, source=self.name)


class MetNoLocationForecast:
    """JSON forecast adapter for the independent MET Norway service."""

    name = "met-no"

    def __init__(
        self,
        *,
        fetch_json: Callable[..., Mapping[str, Any]] = _default_noaa_fetch_json,
        cache_seconds: float = 1_800,
        timeout_seconds: float = 20,
        user_agent: str = "polymarket-bot-weather-research/4.1",
    ) -> None:
        if cache_seconds <= 0 or timeout_seconds <= 0 or not user_agent.strip():
            raise ValueError("invalid MET Norway client settings")
        self._fetch_json = fetch_json
        self._cache_seconds = cache_seconds
        self._timeout_seconds = timeout_seconds
        self._user_agent = user_agent
        self._cache: dict[tuple[str, str], tuple[float, float]] = {}

    async def forecast(
        self,
        contract: HighTemperatureContract,
        *,
        now: datetime | None = None,
    ) -> EnsembleForecast:
        now = now or datetime.now(timezone.utc)
        key = (contract.city, contract.target_date)
        cached = self._cache.get(key)
        if cached is not None and cached[0] > time.monotonic():
            maximum = cached[1]
        else:
            latitude, longitude = CITY_COORDS[contract.city]
            try:
                payload = await asyncio.to_thread(
                    self._fetch_json,
                    "https://api.met.no/weatherapi/locationforecast/2.0/compact",
                    params={"lat": latitude, "lon": longitude},
                    headers={
                        "User-Agent": self._user_agent,
                        "Accept": "application/json",
                    },
                    timeout=self._timeout_seconds,
                )
                series = payload.get("properties", {}).get("timeseries")
                if not isinstance(series, list):
                    raise ValueError("MET Norway response has no timeseries")
                timezone_name = CITY_TIMEZONES.get(contract.city)
                if timezone_name is None:
                    raise ValueError(f"no resolver timezone for {contract.city}")
                target = date.fromisoformat(contract.target_date)
                values: list[float] = []
                for item in series:
                    if not isinstance(item, dict):
                        continue
                    raw_time = item.get("time")
                    details = item.get("data", {}).get("instant", {}).get("details", {})
                    raw_temperature = details.get("air_temperature")
                    if not isinstance(raw_time, str) or raw_temperature is None:
                        continue
                    observed_at = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
                    if observed_at.astimezone(ZoneInfo(timezone_name)).date() == target:
                        value = float(raw_temperature)
                        if math.isfinite(value):
                            values.append(value)
                if not values:
                    raise ValueError("MET Norway response has no target-date temperatures")
                maximum = max(values)
            except Exception as exc:
                response = getattr(exc, "response", None)
                detail = f"{type(exc).__name__}: {exc}"
                raise ForecastUnavailableError(
                    f"MET Norway forecast unavailable for {contract.event_key}: {detail}",
                    provider_global=getattr(response, "status_code", None) == 429,
                ) from exc
            self._cache[key] = (time.monotonic() + self._cache_seconds, maximum)
        return _deterministic_forecast(
            contract,
            maximum,
            now=now,
            source=self.name,
        )


class NWSGridForecast:
    """US-only NWS hourly grid forecast adapter with no credentials."""

    name = "nws"
    covered_cities = frozenset({
        "new york", "nyc", "chicago", "seattle", "atlanta", "dallas", "miami",
        "los angeles", "austin", "houston", "denver", "san francisco",
    })

    def __init__(
        self,
        *,
        fetch_json: Callable[..., Mapping[str, Any]] = _default_noaa_fetch_json,
        cache_seconds: float = 1_800,
        timeout_seconds: float = 20,
        user_agent: str = "polymarket-bot-weather-research/4.1",
    ) -> None:
        if cache_seconds <= 0 or timeout_seconds <= 0 or not user_agent.strip():
            raise ValueError("invalid NWS client settings")
        self._fetch_json = fetch_json
        self._cache_seconds = cache_seconds
        self._timeout_seconds = timeout_seconds
        self._user_agent = user_agent
        self._cache: dict[tuple[str, str], tuple[float, float]] = {}

    async def forecast(
        self,
        contract: HighTemperatureContract,
        *,
        now: datetime | None = None,
    ) -> EnsembleForecast:
        now = now or datetime.now(timezone.utc)
        if contract.city not in self.covered_cities:
            raise ForecastUnavailableError(
                f"NWS has no configured coverage for {contract.city}",
                provider_covered=False,
            )
        key = (contract.city, contract.target_date)
        cached = self._cache.get(key)
        if cached is not None and cached[0] > time.monotonic():
            maximum = cached[1]
        else:
            latitude, longitude = CITY_COORDS[contract.city]
            headers = {
                "User-Agent": self._user_agent,
                "Accept": "application/geo+json, application/json",
            }
            try:
                point = await asyncio.to_thread(
                    self._fetch_json,
                    f"https://api.weather.gov/points/{latitude:.4f},{longitude:.4f}",
                    params={},
                    headers=headers,
                    timeout=self._timeout_seconds,
                )
                forecast_url = point.get("properties", {}).get("forecastHourly")
                if not isinstance(forecast_url, str) or not forecast_url:
                    raise ValueError("NWS points response has no forecastHourly URL")
                payload = await asyncio.to_thread(
                    self._fetch_json,
                    forecast_url,
                    params={},
                    headers=headers,
                    timeout=self._timeout_seconds,
                )
                periods = payload.get("properties", {}).get("periods")
                if not isinstance(periods, list):
                    raise ValueError("NWS hourly response has no periods")
                target = date.fromisoformat(contract.target_date)
                timezone_name = CITY_TIMEZONES[contract.city]
                values: list[float] = []
                for period in periods:
                    if not isinstance(period, dict):
                        continue
                    raw_start = period.get("startTime")
                    raw_temperature = period.get("temperature")
                    if not isinstance(raw_start, str) or raw_temperature is None:
                        continue
                    start = datetime.fromisoformat(raw_start.replace("Z", "+00:00"))
                    if start.astimezone(ZoneInfo(timezone_name)).date() != target:
                        continue
                    value = float(raw_temperature)
                    if str(period.get("temperatureUnit", "F")).upper() == "F":
                        value = (value - 32.0) * 5.0 / 9.0
                    if math.isfinite(value):
                        values.append(value)
                if not values:
                    raise ValueError("NWS response has no target-date temperatures")
                maximum = max(values)
            except Exception as exc:
                response = getattr(exc, "response", None)
                detail = f"{type(exc).__name__}: {exc}"
                raise ForecastUnavailableError(
                    f"NWS forecast unavailable for {contract.event_key}: {detail}",
                    provider_global=getattr(response, "status_code", None) == 429,
                ) from exc
            self._cache[key] = (time.monotonic() + self._cache_seconds, maximum)
        return _deterministic_forecast(
            contract,
            maximum,
            now=now,
            source=self.name,
        )


def _deterministic_forecast(
    contract: HighTemperatureContract,
    maximum_c: float,
    *,
    now: datetime,
    source: str,
) -> EnsembleForecast:
    timezone_name = CITY_TIMEZONES.get(contract.city)
    if timezone_name is None:
        raise ValueError(f"no resolver timezone for {contract.city}")
    target = date.fromisoformat(contract.target_date)
    resolver_today = now.astimezone(ZoneInfo(timezone_name)).date()
    lead_days = max(0, (target - resolver_today).days)
    result = _ensemble_probability(
        contract,
        ((maximum_c,),),
        lead_days,
    )
    return replace(
        result,
        source=source,
        provider_count=1,
        provider_names=(source,),
        provider_probabilities=((source, result.raw_probability),),
        continent=CITY_CONTINENTS.get(contract.city, "global"),
        provider_weights=((source, ONE),),
    )


class ResilientForecastEnsemble:
    """Combine independent providers and continue with available sources."""

    name = "ensemble"

    def __init__(
        self,
        providers: Sequence[ForecastProvider],
        *,
        weights: Mapping[str, Decimal] | None = None,
        calibrator: ProbabilityCalibration | None = None,
        failure_backoff_seconds: float = 300,
    ) -> None:
        if not providers:
            raise ValueError("at least one forecast provider is required")
        if failure_backoff_seconds <= 0:
            raise ValueError("forecast failure backoff must be positive")
        self.providers = tuple(providers)
        self.weights_by_continent = {
            continent: dict(values)
            for continent, values in CONTINENT_WEIGHTS.items()
        }
        self._explicit_weights = None if weights is None else dict(weights)
        self.weights = dict(weights or self.weights_by_continent["global"])
        self.calibrator = calibrator or ProbabilityCalibration()
        self._failure_backoff_seconds = failure_backoff_seconds
        self._failure_until: dict[str, float] = {}
        self._failure_messages: dict[str, str] = {}

    async def forecast(
        self,
        contract: HighTemperatureContract,
        *,
        now: datetime | None = None,
    ) -> EnsembleForecast:
        successful: list[tuple[str, EnsembleForecast]] = []
        failures: list[tuple[str, str]] = []
        continent = CITY_CONTINENTS.get(contract.city, "global")
        selected_weights = self._explicit_weights or self.weights_by_continent.get(
            continent,
            self.weights,
        )
        for provider in self.providers:
            name = str(getattr(provider, "name", type(provider).__name__.lower()))
            monotonic_now = time.monotonic()
            failure_until = self._failure_until.get(name, 0.0)
            if monotonic_now < failure_until:
                failures.append((name, self._failure_messages.get(name, "provider backoff active")))
                continue
            try:
                result = await provider.forecast(contract, now=now)
            except ForecastUnavailableError as exc:
                if exc.provider_covered:
                    message = f"{type(exc).__name__}: {exc}"
                    self._failure_until[name] = monotonic_now + self._failure_backoff_seconds
                    self._failure_messages[name] = message
                    failures.append((name, message))
                continue
            except Exception as exc:
                message = f"{type(exc).__name__}: {exc}"
                self._failure_until[name] = monotonic_now + self._failure_backoff_seconds
                self._failure_messages[name] = message
                failures.append((name, message))
                continue
            self._failure_until.pop(name, None)
            self._failure_messages.pop(name, None)
            if result is not None:
                successful.append((name, result))
        if not successful:
            detail = "; ".join(f"{name}: {message}" for name, message in failures)
            raise ForecastUnavailableError(
                f"all forecast providers unavailable for {contract.event_key}: {detail}",
                provider_global=True,
            )

        values: list[tuple[str, EnsembleForecast, Decimal, Decimal]] = []
        for name, result in successful:
            calibrated = self.calibrator.calibrate(
                name,
                contract.city,
                result.lead_days,
                result.raw_probability,
            )
            weight = selected_weights.get(name, ONE)
            if weight > ZERO:
                values.append((name, result, calibrated, weight))
        if not values:
            raise ForecastUnavailableError("all forecast provider weights are zero")
        total_weight = sum(item[3] for item in values)
        weighted_probability = sum(
            (
                probability * weight / total_weight
                for (_name, _result, probability, weight) in values
            ),
            ZERO,
        )
        weighted_mean = sum(
            (
                result.ensemble_mean_c * weight / total_weight
                for (_name, result, _probability, weight) in values
            ),
            ZERO,
        )
        second_moment = sum(
            (
                (result.ensemble_std_c ** 2 + result.ensemble_mean_c ** 2)
                * weight / total_weight
                for (_name, result, _probability, weight) in values
            ),
            ZERO,
        )
        spread = (second_moment - weighted_mean ** 2).sqrt() if second_moment > weighted_mean ** 2 else ZERO
        if len(values) == 1:
            spread *= Decimal("1.35")
        first = values[0][1]
        return EnsembleForecast(
            raw_probability=min(ONE, max(ZERO, weighted_probability)),
            ensemble_mean_c=weighted_mean,
            ensemble_std_c=spread,
            n_members=sum(result.n_members for _name, result, _p, _w in values),
            lead_days=first.lead_days,
            model_count=sum(result.model_count for _name, result, _p, _w in values),
            distribution_probability=min(ONE, max(ZERO, weighted_probability)),
            source=self.name,
            provider_count=len(values),
            provider_names=tuple(name for name, _result, _p, _w in values),
            provider_probabilities=tuple(
                (name, result.raw_probability)
                for name, result, _calibrated, _weight in values
            ),
            provider_failures=tuple(failures),
            calibration_samples=self.calibrator.samples(),
            continent=continent,
            provider_weights=tuple(
                (name, weight / total_weight)
                for name, _result, _probability, weight in values
            ),
        )

    def record_outcome(
        self,
        *,
        city: str,
        lead_days: int,
        outcome: int,
        provider_probabilities: Sequence[tuple[str, Decimal]],
    ) -> None:
        for source, probability in provider_probabilities:
            self.calibrator.record(source, city, lead_days, probability, outcome)


def _best_price(levels: Any, *, ask: bool) -> Decimal | None:
    prices = tuple(Decimal(str(level.price)) for level in levels)
    if not prices:
        return None
    return min(prices) if ask else max(prices)


def _book_levels(levels: Any) -> tuple[BookLevel, ...]:
    return tuple(
        BookLevel(Decimal(str(level.price)), Decimal(str(level.size)))
        for level in levels
    )


def _maker_shadow(
    *,
    book: Any,
    context: MarketContext,
    size: Decimal,
    expected_probability: Decimal,
) -> MakerShadowQuote | None:
    try:
        return propose_buy_quote(
            bids=_book_levels(book.bids),
            asks=_book_levels(book.asks),
            tick_size=context.tick_size,
            size=size,
            expected_probability=expected_probability,
        )
    except ValueError:
        return None


def _same_day_at_resolver(contract: HighTemperatureContract, now: datetime) -> bool:
    timezone_name = CITY_TIMEZONES.get(contract.city)
    if timezone_name is None:
        return False
    return (
        date.fromisoformat(contract.target_date)
        == now.astimezone(ZoneInfo(timezone_name)).date()
    )


def _side_evaluation(
    *,
    market: Any,
    contract: HighTemperatureContract,
    forecast: EnsembleForecast,
    yes_probability: Decimal,
    side: str,
    book: Any,
    context: MarketContext,
    policy: WeatherPaperPolicy,
    same_day_contract: bool,
    observation: ObservationBoundResult,
    observation_error: str | None,
    observation_status: str,
    trade_block_reason: str | None,
) -> WeatherEvaluation | None:
    if not context.accepting_orders or context.fee_rate is None:
        return None
    bid = _best_price(book.bids, ask=False)
    shares = Decimal(str(book.min_order_size))
    ask_levels = _book_levels(book.asks)
    if bid is None or not ask_levels:
        return None
    try:
        executable = execution_vwap(ask_levels, shares)
    except ValueError:
        return None
    ask = executable.vwap
    if not (policy.min_price <= ask <= policy.max_price):
        return None
    raw_probability = yes_probability if side == "YES" else ONE - yes_probability
    resolver_certain = (
        observation.same_day_observation_available
        and observation.current_high_display is not None
        and yes_probability in {ZERO, ONE}
    )
    outcome = market.outcomes.yes if side == "YES" else market.outcomes.no
    anchor = Decimal(str(outcome.price if outcome.price is not None else ask))
    fee = execution_fee(ask_levels, shares, context.fee_rate)
    decision = evaluate_weather_market(WeatherMarketInput(
        raw_probability=raw_probability,
        anchor_probability=anchor,
        n_members=forecast.n_members,
        intraclass_correlation=policy.intraclass_correlation,
        best_bid=bid,
        best_ask=ask,
        fee_rate=context.fee_rate,
        executable_fee_per_share=fee / shares,
        lead_days=forecast.lead_days,
        resolution_source_verified=context.rules_verified,
        # A resolver-certain running-high implication is a hard logical bound,
        # not a noisy ensemble estimate. Do not shrink it back toward the book.
        prior_strength=ZERO if resolver_certain else policy.prior_strength,
        fractional_kelly=policy.fractional_kelly,
        base_edge=policy.base_edge,
        uncertainty_z=policy.uncertainty_z,
    ))
    all_in_cost = executable.notional + fee
    tradeable = bool(
        decision.tradeable
        and all_in_cost <= policy.max_order_notional
        and trade_block_reason is None
    )
    if trade_block_reason is not None:
        reason = trade_block_reason
    elif not decision.tradeable:
        reason = decision.reason
    elif all_in_cost > policy.max_order_notional:
        reason = "weather paper order cap exceeded"
    else:
        reason = "weather paper candidate"
    return WeatherEvaluation(
        strategy="weather_directional",
        event_key=contract.event_key,
        market_id=str(market.id),
        condition_id=str(market.condition_id),
        question=str(market.question),
        side=side,
        token_id=str(book.token_id),
        city=contract.city,
        target_date=contract.target_date,
        target_c=contract.target_c if contract.is_exact else None,
        unit=contract.unit,
        display_lower=contract.display_lower,
        display_upper=contract.display_upper,
        contract_kind=contract.contract_kind,
        bid=bid,
        ask=ask,
        shares=shares,
        fee=fee,
        all_in_cost=all_in_cost,
        raw_probability=raw_probability,
        forecast=forecast,
        decision=decision,
        paper_tradeable=tradeable,
        paper_reason=reason,
        same_day_contract=same_day_contract,
        same_day_observation_available=observation.same_day_observation_available,
        current_high_display=observation.current_high_display,
        observation_error=observation_error,
        same_day_observation_status=observation_status,
        maker_shadow=_maker_shadow(
            book=book,
            context=context,
            size=shares,
            expected_probability=raw_probability,
        ),
    )


@dataclass(frozen=True)
class _SurfaceComponent:
    market: Any
    contract: HighTemperatureContract
    forecast: EnsembleForecast
    yes_book: Any
    context: MarketContext


def _unavailable_surface(
    *,
    event_key: str,
    unit: str,
    bucket_count: int,
    probability_sum: Decimal,
    reason: str,
) -> EventSurface:
    return EventSurface(
        event_key=event_key,
        unit=unit,
        bucket_count=bucket_count,
        complete_partition=False,
        executable=False,
        tradeable=False,
        common_shares=ZERO,
        model_probability_sum=probability_sum,
        model_probability_residual=ONE - probability_sum,
        gross_cost=ZERO,
        fees=ZERO,
        payout=ZERO,
        net_profit=ZERO,
        reason=reason,
    )


def _build_event_evaluations(
    discovered: Mapping[
        tuple[str, str],
        Sequence[tuple[Any, HighTemperatureContract, str]],
    ],
    components: Mapping[str, _SurfaceComponent],
) -> tuple[WeatherEventEvaluation, ...]:
    events: list[WeatherEventEvaluation] = []
    for (event_key, unit), entries in sorted(discovered.items()):
        ordered_entries = sorted(
            entries,
            key=lambda item: (
                item[1].display_lower is not None,
                ZERO if item[1].display_lower is None else item[1].display_lower,
                str(item[0].id),
            ),
        )
        contracts: list[WeatherEventContract] = []
        buckets: list[SurfaceBucket] = []
        complete_data = True
        probability_sum = ZERO
        for market, contract, _station_id in ordered_entries:
            component = components.get(str(market.id))
            probability: Decimal | None = None
            maker: MakerShadowQuote | None = None
            if component is not None:
                probability = (
                    component.forecast.distribution_probability
                    if component.forecast.distribution_probability is not None
                    else component.forecast.raw_probability
                )
                probability_sum += probability
                minimum_size = Decimal(str(component.yes_book.min_order_size))
                maker = _maker_shadow(
                    book=component.yes_book,
                    context=component.context,
                    size=minimum_size,
                    expected_probability=probability,
                )
                asks = _book_levels(component.yes_book.asks)
                if asks and component.context.fee_rate is not None:
                    buckets.append(SurfaceBucket(
                        key=str(market.condition_id),
                        lower_display=contract.display_lower,
                        upper_display=contract.display_upper,
                        model_probability=probability,
                        yes_asks=asks,
                        minimum_size=minimum_size,
                        fee_rate=component.context.fee_rate,
                    ))
                else:
                    complete_data = False
            else:
                complete_data = False
            contracts.append(WeatherEventContract(
                market_id=str(market.id),
                condition_id=str(market.condition_id),
                question=str(market.question),
                yes_token_id=str(market.outcomes.yes.token_id),
                contract_kind=contract.contract_kind,
                display_lower=contract.display_lower,
                display_upper=contract.display_upper,
                model_probability=probability,
                maker_shadow=maker,
            ))

        negative_risk_verified = all(
            bool(getattr(market.state, "neg_risk", False))
            for market, _contract, _station in ordered_entries
        )
        resolution_station_verified = all(
            _verified_resolution_station(
                contract,
                getattr(market.resolution, "source", None),
            ) == station_id
            for market, contract, station_id in ordered_entries
        )
        if (
            complete_data
            and negative_risk_verified
            and resolution_station_verified
            and len(buckets) == len(ordered_entries)
        ):
            surface = analyze_event_surface(
                event_key=event_key,
                unit=unit,
                buckets=tuple(buckets),
            )
        else:
            surface = _unavailable_surface(
                event_key=event_key,
                unit=unit,
                bucket_count=len(ordered_entries),
                probability_sum=probability_sum,
                reason="event market data or resolver verification incomplete",
            )
        events.append(WeatherEventEvaluation(
            event_key=event_key,
            unit=unit,
            contracts=tuple(contracts),
            surface=surface,
            negative_risk_verified=negative_risk_verified,
            resolution_station_verified=resolution_station_verified,
        ))
    return tuple(events)


async def evaluate_weather_universe(
    *,
    client: WeatherPublicClient,
    forecast: ForecastProvider,
    policy: WeatherPaperPolicy,
    observation_provider: ObservationProvider | None = None,
    now: datetime | None = None,
) -> WeatherUniverseResult:
    if not policy.enabled:
        return WeatherUniverseResult(0, 0, ())
    now = now or datetime.now(timezone.utc)
    tag = await client.get_tag(slug="weather")
    markets: list[tuple[Any, HighTemperatureContract, str]] = []
    examined = 0
    paginator = client.list_markets(
        closed=False,
        tag_id=int(tag.id),
        liquidity_num_min=float(policy.min_liquidity),
        order="liquidityNum",
        ascending=False,
        page_size=100,
    )
    async for market in paginator.iter_items():
        examined += 1
        contract = parse_high_temperature_contract(
            getattr(market, "question", None),
            end_date=getattr(market.state, "end_date", None),
        )
        source = getattr(market.resolution, "source", None)
        station_id = (
            _verified_resolution_station(contract, source)
            if contract is not None
            else None
        )
        yes_token = getattr(market.outcomes.yes, "token_id", None)
        no_token = getattr(market.outcomes.no, "token_id", None)
        end_date = getattr(market.state, "end_date", None)
        liquidity = Decimal(str(getattr(market.metrics, "liquidity_num", 0) or 0))
        within_horizon = bool(
            end_date
            and now < end_date
            and 0 <= (end_date.date() - now.date()).days <= policy.horizon_days
        )
        if (
            contract is not None
            and station_id is not None
            and within_horizon
            and bool(getattr(market.state, "active", False))
            and not bool(getattr(market.state, "closed", False))
            and bool(getattr(market.state, "accepting_orders", False))
            and bool(getattr(market.state, "neg_risk", False))
            and yes_token
            and no_token
            and liquidity >= policy.min_liquidity
        ):
            markets.append((market, contract, station_id))
        if len(markets) >= policy.market_limit or examined >= policy.discovery_limit:
            break

    discovered_events: dict[
        tuple[str, str],
        list[tuple[Any, HighTemperatureContract, str]],
    ] = {}
    for item in markets:
        discovered_events.setdefault((item[1].event_key, item[1].unit), []).append(item)

    evaluations: list[WeatherEvaluation] = []
    errors: list[str] = []
    forecast_errors: list[str] = []
    forecast_error_keys: set[str] = set()
    components: dict[str, _SurfaceComponent] = {}
    observations_available = 0
    observation_errors = 0
    markets_forecast_unavailable = 0
    markets_modeled = 0
    markets_side_evaluable = 0
    provider_names: set[str] = set()
    provider_failures: dict[str, str] = {}
    for market, contract, station_id in markets:
        try:
            try:
                point_forecast = await forecast.forecast(contract, now=now)
            except ForecastUnavailableError as exc:
                markets_forecast_unavailable += 1
                error_key = "provider-global" if exc.provider_global else contract.event_key
                if error_key not in forecast_error_keys:
                    forecast_error_keys.add(error_key)
                    message = f"forecast unavailable: {exc}"
                    forecast_errors.append(message)
                    errors.append(message)
                continue
            except Exception as exc:
                markets_forecast_unavailable += 1
                error_key = f"market:{getattr(market, 'id', '')}"
                if error_key not in forecast_error_keys:
                    forecast_error_keys.add(error_key)
                    message = (
                        f"{getattr(market, 'id', '')}: forecast error: "
                        f"{type(exc).__name__}: {exc}"
                    )
                    forecast_errors.append(message)
                    errors.append(message)
                continue
            if point_forecast is None:
                markets_forecast_unavailable += 1
                error_key = "provider-empty"
                if error_key not in forecast_error_keys:
                    forecast_error_keys.add(error_key)
                    message = "forecast provider returned no forecast data"
                    forecast_errors.append(message)
                    errors.append(message)
                continue
            provider_names.update(point_forecast.provider_names)
            provider_failures.update(dict(point_forecast.provider_failures))
            markets_modeled += 1
            same_day_contract = _same_day_at_resolver(contract, now)
            observation = ObservationBoundResult(
                point_forecast.raw_probability,
                False,
                None,
            )
            observation_error: str | None = None
            observation_status = "not_applicable"
            trade_block_reason: str | None = None
            if same_day_contract:
                if not policy.observations_enabled:
                    observation_status = "disabled"
                    trade_block_reason = "same-day station observations are disabled"
                elif observation_provider is None:
                    observation_status = "provider_unavailable"
                    trade_block_reason = (
                        "same-day station observation provider unavailable"
                    )
                else:
                    try:
                        observation = await observation_provider.adjust_probability(
                            contract,
                            point_forecast.raw_probability,
                            station_id=station_id,
                        )
                    except Exception as exc:
                        observation_errors += 1
                        observation_status = "error"
                        observation_error = f"{type(exc).__name__}: {exc}"
                        errors.append(
                            f"{getattr(market, 'id', '')}: observation error: "
                            f"{observation_error}"
                        )
                        trade_block_reason = "same-day station observation error"
                    else:
                        if observation.same_day_observation_available:
                            observation_status = "available"
                            observations_available += 1
                        else:
                            observation_status = "unavailable"
                            trade_block_reason = (
                                "required same-day station observation unavailable"
                            )

            yes_token = str(market.outcomes.yes.token_id)
            no_token = str(market.outcomes.no.token_id)
            books = await client.get_order_books(token_ids=[yes_token, no_token])
            by_token = {str(book.token_id): book for book in books}
            if yes_token not in by_token or no_token not in by_token:
                raise ValueError("weather book response omitted an outcome token")
            context = MarketContext.from_sdk(market, by_token[yes_token])
            if not context.negative_risk:
                continue
            if (
                context.accepting_orders
                and context.rules_verified
                and context.fee_rate is not None
            ):
                components[str(market.id)] = _SurfaceComponent(
                    market=market,
                    contract=contract,
                    forecast=point_forecast,
                    yes_book=by_token[yes_token],
                    context=context,
                )
            options = tuple(filter(None, (
                _side_evaluation(
                    market=market,
                    contract=contract,
                    forecast=point_forecast,
                    yes_probability=observation.probability,
                    side="YES",
                    book=by_token[yes_token],
                    context=context,
                    policy=policy,
                    same_day_contract=same_day_contract,
                    observation=observation,
                    observation_error=observation_error,
                    observation_status=observation_status,
                    trade_block_reason=trade_block_reason,
                ),
                _side_evaluation(
                    market=market,
                    contract=contract,
                    forecast=point_forecast,
                    yes_probability=observation.probability,
                    side="NO",
                    book=by_token[no_token],
                    context=context,
                    policy=policy,
                    same_day_contract=same_day_contract,
                    observation=observation,
                    observation_error=observation_error,
                    observation_status=observation_status,
                    trade_block_reason=trade_block_reason,
                ),
            )))
            if options:
                markets_side_evaluable += 1
                evaluations.append(max(options, key=lambda item: item.decision.net_edge))
        except Exception as exc:
            errors.append(f"{getattr(market, 'id', '')}: {type(exc).__name__}: {exc}")

    if not markets:
        forecast_status = "not_requested"
    elif markets_forecast_unavailable == len(markets):
        forecast_status = "unavailable"
    elif markets_forecast_unavailable:
        forecast_status = "degraded"
    elif provider_failures:
        forecast_status = "degraded"
    else:
        forecast_status = "available"
    events = _build_event_evaluations(discovered_events, components)
    return WeatherUniverseResult(
        markets_discovered=len(markets),
        markets_evaluated=len(markets),
        evaluations=tuple(evaluations),
        errors=tuple(errors),
        events=events,
        observations_available=observations_available,
        observation_errors=observation_errors,
        markets_forecast_unavailable=markets_forecast_unavailable,
        markets_modeled=markets_modeled,
        markets_side_evaluable=markets_side_evaluable,
        forecast_status=forecast_status,
        forecast_errors=tuple(forecast_errors),
        provider_names=tuple(sorted(provider_names)),
        provider_failures=tuple(sorted(provider_failures.items())),
    )
