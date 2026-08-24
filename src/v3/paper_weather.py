"""Public weather discovery and tiny directional paper evaluation."""

from __future__ import annotations

import asyncio
import math
import re
import statistics
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from statistics import NormalDist
from typing import Any, Protocol
from urllib.parse import parse_qs, urlparse

import requests

from .market_context import MarketContext
from .math import BookLevel, execution_fee, execution_vwap
from .strategies.weather import WeatherDecision, WeatherMarketInput, evaluate_weather_market

ZERO = Decimal("0")
ONE = Decimal("1")
OPEN_METEO_ENSEMBLE = "https://ensemble-api.open-meteo.com/v1/ensemble"
ENSEMBLE_MODELS = "ecmwf_ifs025,gfs_seamless,icon_seamless,gem_global"
MODEL_KEY_MARKERS: Mapping[str, str] = {
    "ecmwf": "ecmwf_ifs025",
    "gfs": "ncep_gefs_seamless",
    "icon": "icon_seamless",
    "gem": "gem_global",
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
_EXACT_HIGH_RE = re.compile(
    r"^Will the highest temperature in (?P<city>.+?) be "
    r"(?P<temperature>-?\d+(?:\.\d+)?)°(?P<unit>[CF]) on "
    r"(?P<month>[A-Za-z]+) (?P<day>\d{1,2})\?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ExactHighContract:
    city: str
    target_date: str
    target_c: Decimal
    unit: str
    display_temperature: Decimal

    @property
    def event_key(self) -> str:
        return f"weather:{self.city}:{self.target_date}"


@dataclass(frozen=True)
class EnsembleForecast:
    raw_probability: Decimal
    ensemble_mean_c: Decimal
    ensemble_std_c: Decimal
    n_members: int
    lead_days: int
    model_count: int = 4


@dataclass(frozen=True)
class WeatherPaperPolicy:
    enabled: bool = True
    horizon_days: int = 3
    discovery_limit: int = 1_500
    market_limit: int = 100
    min_liquidity: Decimal = Decimal("1000")
    min_price: Decimal = Decimal("0.03")
    max_price: Decimal = Decimal("0.20")
    max_order_notional: Decimal = ONE
    max_open_positions: int = 5
    base_edge: Decimal = Decimal("0.03")
    intraclass_correlation: Decimal = Decimal("0.05")
    prior_strength: Decimal = Decimal("10")
    fractional_kelly: Decimal = Decimal("0.05")
    uncertainty_z: Decimal = ONE

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
        if not (1 <= self.max_open_positions <= 5):
            raise ValueError("weather paper position cap must be in [1, 5]")
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
    target_c: Decimal
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


@dataclass(frozen=True)
class WeatherUniverseResult:
    markets_discovered: int
    markets_evaluated: int
    evaluations: tuple[WeatherEvaluation, ...]
    errors: tuple[str, ...] = field(default_factory=tuple)


class WeatherPublicClient(Protocol):
    async def get_tag(self, *, slug: str) -> Any: ...

    def list_markets(self, **kwargs: Any) -> Any: ...

    async def get_order_books(self, *, token_ids: list[str]) -> tuple[Any, ...]: ...


class ForecastProvider(Protocol):
    async def forecast(
        self,
        contract: ExactHighContract,
        *,
        now: datetime | None = None,
    ) -> EnsembleForecast | None: ...


JsonFetcher = Callable[..., Mapping[str, Any]]


def parse_exact_high_contract(
    question: str | None,
    *,
    end_date: datetime | None,
) -> ExactHighContract | None:
    if not question or end_date is None:
        return None
    match = _EXACT_HIGH_RE.fullmatch(question.strip())
    if match is None:
        return None
    city = match.group("city").strip().lower()
    city = CITY_ALIASES.get(city, city)
    if city in AVOID_CITIES or city not in CITY_COORDS:
        return None
    month = _MONTHS.get(match.group("month").lower())
    if month is None:
        return None
    day = int(match.group("day"))
    candidates = []
    for year in (end_date.year - 1, end_date.year, end_date.year + 1):
        try:
            candidates.append(datetime(year, month, day, tzinfo=timezone.utc).date())
        except ValueError:
            continue
    if not candidates:
        return None
    target = min(candidates, key=lambda value: abs((value - end_date.date()).days))
    if abs((target - end_date.date()).days) > 2:
        return None
    display = Decimal(match.group("temperature"))
    unit = match.group("unit").upper()
    target_c = display if unit == "C" else (display - Decimal("32")) * Decimal("5") / Decimal("9")
    return ExactHighContract(
        city=city,
        target_date=target.isoformat(),
        target_c=target_c,
        unit=unit,
        display_temperature=display,
    )


def _default_fetch_json(url: str, *, params: Mapping[str, Any], timeout: float) -> Mapping[str, Any]:
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Open-Meteo response must be an object")
    return payload


def _resolution_station_matches(contract: ExactHighContract, source: str | None) -> bool:
    expected = CITY_STATIONS.get(contract.city)
    if not expected or not source:
        return False
    parsed = urlparse(source)
    if parsed.scheme != "https" or parsed.hostname != "www.weather.gov":
        return False
    sites = parse_qs(parsed.query).get("site", ())
    return bool(sites and sites[0].upper() == expected)


class OpenMeteoEnsemble:
    def __init__(
        self,
        *,
        fetch_json: JsonFetcher = _default_fetch_json,
        min_members: int = 10,
        cache_seconds: float = 1_800,
        timeout_seconds: float = 20,
        failure_backoff_seconds: float = 300,
    ) -> None:
        if (
            min_members < 2
            or cache_seconds <= 0
            or timeout_seconds <= 0
            or failure_backoff_seconds <= 0
        ):
            raise ValueError("invalid Open-Meteo client settings")
        self._fetch_json = fetch_json
        self._min_members = min_members
        self._cache_seconds = cache_seconds
        self._timeout_seconds = timeout_seconds
        self._failure_backoff_seconds = failure_backoff_seconds
        self._cache: dict[
            tuple[str, str],
            tuple[float, tuple[tuple[float, ...], ...]],
        ] = {}
        self._failure_until: dict[tuple[str, str], float] = {}

    @staticmethod
    def _probability(
        contract: ExactHighContract,
        model_members: tuple[tuple[float, ...], ...],
        lead_days: int,
    ) -> EnsembleForecast:
        inflation = 1.05 + 0.15 * max(0, lead_days)
        half_width = 0.5 if contract.unit == "C" else 5 / 18
        model_probabilities: list[float] = []
        model_means: list[float] = []
        model_variances: list[float] = []
        for members in model_members:
            model_mean = statistics.mean(members)
            model_std = statistics.stdev(members)
            sigma = max(model_std * inflation, 0.5)
            distribution = NormalDist(mu=model_mean, sigma=sigma)
            probability = distribution.cdf(float(contract.target_c) + half_width)
            probability -= distribution.cdf(float(contract.target_c) - half_width)
            model_probabilities.append(probability)
            model_means.append(model_mean)
            model_variances.append(model_std ** 2)
        probability = statistics.mean(model_probabilities)
        probability = min(0.999, max(0.001, probability))
        mean = statistics.mean(model_means)
        mixture_second_moment = statistics.mean(
            variance + model_mean ** 2
            for variance, model_mean in zip(model_variances, model_means, strict=True)
        )
        raw_std = math.sqrt(max(0, mixture_second_moment - mean ** 2))
        return EnsembleForecast(
            raw_probability=Decimal(str(probability)),
            ensemble_mean_c=Decimal(str(mean)),
            ensemble_std_c=Decimal(str(raw_std)),
            n_members=sum(len(members) for members in model_members),
            lead_days=lead_days,
            model_count=len(model_members),
        )

    async def forecast(
        self,
        contract: ExactHighContract,
        *,
        now: datetime | None = None,
    ) -> EnsembleForecast | None:
        now = now or datetime.now(timezone.utc)
        cache_key = (contract.city, contract.target_date)
        cached = self._cache.get(cache_key)
        monotonic_now = time.monotonic()
        if monotonic_now < self._failure_until.get(cache_key, 0):
            return None
        if cached is not None and monotonic_now < cached[0]:
            model_members = cached[1]
        else:
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
                daily_units = payload.get("daily_units")
                if not isinstance(daily, dict) or not isinstance(daily_units, dict):
                    raise ValueError("Open-Meteo response has no daily object")
                if daily.get("time") != [contract.target_date]:
                    raise ValueError("Open-Meteo response date does not match the request")
                groups: dict[str, list[float]] = {
                    model: [] for model in MODEL_KEY_MARKERS
                }
                for key, raw_values in daily.items():
                    key = str(key)
                    if key == "time":
                        continue
                    if not key.startswith("temperature_2m_max"):
                        raise ValueError(f"unexpected Open-Meteo daily field: {key}")
                    model = next(
                        (name for name, marker in MODEL_KEY_MARKERS.items() if marker in key),
                        None,
                    )
                    if model is None:
                        raise ValueError(f"unknown Open-Meteo model field: {key}")
                    if daily_units.get(key) != "°C":
                        raise ValueError(f"unexpected Open-Meteo unit for {key}")
                    if not isinstance(raw_values, list) or len(raw_values) != 1 or raw_values[0] is None:
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
            except Exception:
                self._failure_until[cache_key] = monotonic_now + self._failure_backoff_seconds
                raise
            model_members = tuple(tuple(groups[name]) for name in MODEL_KEY_MARKERS)
            self._failure_until.pop(cache_key, None)
            self._cache[cache_key] = (monotonic_now + self._cache_seconds, model_members)
        target = datetime.fromisoformat(contract.target_date).date()
        lead_days = max(0, (target - now.date()).days)
        return self._probability(contract, model_members, lead_days)


def _best_price(levels: Any, *, ask: bool) -> Decimal | None:
    prices = tuple(Decimal(str(level.price)) for level in levels)
    if not prices:
        return None
    return min(prices) if ask else max(prices)


def _side_evaluation(
    *,
    market: Any,
    contract: ExactHighContract,
    forecast: EnsembleForecast,
    side: str,
    book: Any,
    context: MarketContext,
    policy: WeatherPaperPolicy,
) -> WeatherEvaluation | None:
    bid = _best_price(book.bids, ask=False)
    shares = Decimal(str(book.min_order_size))
    ask_levels = tuple(
        BookLevel(Decimal(str(level.price)), Decimal(str(level.size)))
        for level in book.asks
    )
    if bid is None or not ask_levels:
        return None
    try:
        executable = execution_vwap(ask_levels, shares)
    except ValueError:
        return None
    ask = executable.vwap
    if not (policy.min_price <= ask <= policy.max_price):
        return None
    raw_probability = forecast.raw_probability if side == "YES" else ONE - forecast.raw_probability
    outcome = market.outcomes.yes if side == "YES" else market.outcomes.no
    anchor = Decimal(str(outcome.price if outcome.price is not None else ask))
    decision = evaluate_weather_market(WeatherMarketInput(
        raw_probability=raw_probability,
        anchor_probability=anchor,
        n_members=forecast.n_members,
        intraclass_correlation=policy.intraclass_correlation,
        best_bid=bid,
        best_ask=ask,
        fee_rate=context.fee_rate or ZERO,
        lead_days=forecast.lead_days,
        resolution_source_verified=context.rules_verified,
        prior_strength=policy.prior_strength,
        fractional_kelly=policy.fractional_kelly,
        base_edge=policy.base_edge,
        uncertainty_z=policy.uncertainty_z,
    ))
    fee = execution_fee(ask_levels, shares, context.fee_rate or ZERO)
    all_in_cost = executable.notional + fee
    tradeable = bool(decision.tradeable and all_in_cost <= policy.max_order_notional)
    if not decision.tradeable:
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
        target_c=contract.target_c,
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
    )


async def evaluate_weather_universe(
    *,
    client: WeatherPublicClient,
    forecast: ForecastProvider,
    policy: WeatherPaperPolicy,
    now: datetime | None = None,
) -> WeatherUniverseResult:
    if not policy.enabled:
        return WeatherUniverseResult(0, 0, ())
    now = now or datetime.now(timezone.utc)
    tag = await client.get_tag(slug="weather")
    markets: list[tuple[Any, ExactHighContract]] = []
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
        contract = parse_exact_high_contract(
            getattr(market, "question", None),
            end_date=getattr(market.state, "end_date", None),
        )
        source = getattr(market.resolution, "source", None)
        yes_token = getattr(market.outcomes.yes, "token_id", None)
        no_token = getattr(market.outcomes.no, "token_id", None)
        reference_prices = tuple(
            Decimal(str(value))
            for value in (
                getattr(market.outcomes.yes, "price", None),
                getattr(market.outcomes.no, "price", None),
            )
            if value is not None
        )
        price_in_range = any(
            policy.min_price <= price <= policy.max_price
            for price in reference_prices
        )
        end_date = getattr(market.state, "end_date", None)
        liquidity = Decimal(str(getattr(market.metrics, "liquidity_num", 0) or 0))
        within_horizon = bool(
            end_date
            and now < end_date
            and 0 <= (end_date.date() - now.date()).days <= policy.horizon_days
        )
        if (
            contract is not None
            and within_horizon
            and bool(getattr(market.state, "active", False))
            and not bool(getattr(market.state, "closed", False))
            and bool(getattr(market.state, "accepting_orders", False))
            and bool(getattr(market.state, "neg_risk", False))
            and _resolution_station_matches(contract, source)
            and yes_token
            and no_token
            and price_in_range
            and liquidity >= policy.min_liquidity
        ):
            markets.append((market, contract))
        if len(markets) >= policy.market_limit or examined >= policy.discovery_limit:
            break

    evaluations: list[WeatherEvaluation] = []
    errors: list[str] = []
    for market, contract in markets:
        try:
            point_forecast = await forecast.forecast(contract, now=now)
            if point_forecast is None:
                continue
            yes_token = str(market.outcomes.yes.token_id)
            no_token = str(market.outcomes.no.token_id)
            books = await client.get_order_books(token_ids=[yes_token, no_token])
            by_token = {str(book.token_id): book for book in books}
            if yes_token not in by_token or no_token not in by_token:
                raise ValueError("weather book response omitted an outcome token")
            context = MarketContext.from_sdk(market, by_token[yes_token])
            if not context.negative_risk:
                continue
            options = tuple(filter(None, (
                _side_evaluation(
                    market=market,
                    contract=contract,
                    forecast=point_forecast,
                    side="YES",
                    book=by_token[yes_token],
                    context=context,
                    policy=policy,
                ),
                _side_evaluation(
                    market=market,
                    contract=contract,
                    forecast=point_forecast,
                    side="NO",
                    book=by_token[no_token],
                    context=context,
                    policy=policy,
                ),
            )))
            if options:
                evaluations.append(max(options, key=lambda item: item.decision.net_edge))
        except Exception as exc:
            errors.append(f"{getattr(market, 'id', '')}: {type(exc).__name__}: {exc}")
    return WeatherUniverseResult(
        markets_discovered=len(markets),
        markets_evaluated=len(evaluations),
        evaluations=tuple(evaluations),
        errors=tuple(errors),
    )
