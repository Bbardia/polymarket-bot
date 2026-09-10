from __future__ import annotations

import json
import math
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from src.v3.sanity import (
    SanityError,
    assert_newest_at_index,
    assert_partition_sums_to_one,
    book_hard_reject,
    check_price_age,
    check_probability,
    geography_gate,
)


UTC = timezone.utc

# Remediation item 4: US CPI (BLS CUUR0000SA0) may only price markets that
# name the United States. Everything else fails closed.
US_GEOGRAPHY_MARKERS = (
    "united states", "u.s.", "us cpi", "us inflation", "bureau of labor statistics", "bls",
    "cuur0000sa0", "cpi-u",
)
FOREIGN_GEOGRAPHY_MARKERS = (
    "argentina", "argentine", "canada", "canadian", "united kingdom", "uk ", "eurozone", "europe",
    "china", "chinese", "japan", "japanese", "australia", "australian", "brazil", "mexico",
    "india", "korea", "russia", "switzerland", "germany", "france", "italy", "spain", "turkey",
    "new zealand", "venezuela", "nigeria", "south africa", "indonesia", "egypt",
)


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def money(value: float) -> str:
    return f"{value:.8f}"


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    with temporary.open("rb") as handle:
        os.fsync(handle.fileno())
    temporary.replace(path)


class PublicHTTP:
    """Unauthenticated public HTTP client; no wallet/account headers are allowed."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "polymarket-v8-public-paper/1.0",
                "Accept": "application/json,text/plain,*/*",
            }
        )

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        last: Exception | None = None
        for attempt in range(3):
            try:
                response = self.session.get(url, params=params, timeout=25)
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                last = exc
                if attempt < 2:
                    time.sleep(1.5 * (attempt + 1))
        raise RuntimeError(f"public request failed: {url}: {last}")


@dataclass(frozen=True)
class V8Settings:
    root: Path
    data_dir: Path
    scan_interval_seconds: float = 300.0
    initial_capital: float = 100.0
    max_order_notional: float = 5.0
    reserve_fraction: float = 0.25
    min_edge: float = 0.05
    max_positions: int = 15
    max_positions_per_lane: int = 6
    minimum_liquidity: float = 100.0
    maximum_event_days: int = 370
    max_discovery_pages: int = 8
    fed_enabled: bool = True
    cpi_enabled: bool = True
    sports_enabled: bool = True
    # Remediation item 1: paper entries are frozen unless a profile opts in.
    # Discovery, model telemetry and settlement keep running.
    entries_enabled: bool = False
    # Remediation item 4: a book older than this at decision time is stale.
    max_price_age_seconds: float = 300.0
    fed_realized_cuts_ytd: int = 0

    @classmethod
    def from_env(cls, root: Path) -> "V8Settings":
        def flag(name: str, default: bool) -> bool:
            value = os.getenv(name, str(default)).strip().lower()
            if value not in {"1", "true", "yes", "on", "0", "false", "no", "off"}:
                raise ValueError(f"{name} must be an explicit boolean")
            return value in {"1", "true", "yes", "on"}

        data_dir = Path(os.getenv("V8_DATA_DIR", str(root / "data" / "v8-paper"))).expanduser()
        interval = float(os.getenv("V8_SCAN_INTERVAL_SECONDS", "300"))
        return cls(
            entries_enabled=flag("V8_ENTRIES_ENABLED", False),
            max_price_age_seconds=float(os.getenv("V8_MAX_PRICE_AGE_SECONDS", str(interval))),
            fed_realized_cuts_ytd=int(os.getenv("V8_FED_REALIZED_CUTS_YTD", "0")),
            root=root,
            data_dir=data_dir,
            scan_interval_seconds=float(os.getenv("V8_SCAN_INTERVAL_SECONDS", "300")),
            initial_capital=float(os.getenv("V8_INITIAL_CAPITAL", "100")),
            max_order_notional=float(os.getenv("V8_MAX_ORDER_NOTIONAL", "5")),
            reserve_fraction=float(os.getenv("V8_RESERVE_FRACTION", "0.25")),
            min_edge=float(os.getenv("V8_MIN_EDGE", "0.05")),
            max_positions=int(os.getenv("V8_MAX_POSITIONS", "15")),
            max_positions_per_lane=int(os.getenv("V8_MAX_POSITIONS_PER_LANE", "6")),
            minimum_liquidity=float(os.getenv("V8_MINIMUM_LIQUIDITY", "100")),
            maximum_event_days=int(os.getenv("V8_MAX_EVENT_DAYS", "370")),
            max_discovery_pages=int(os.getenv("V8_MAX_DISCOVERY_PAGES", "8")),
            fed_enabled=flag("V8_FED_ENABLED", True),
            cpi_enabled=flag("V8_CPI_ENABLED", True),
            sports_enabled=flag("V8_SPORTS_ENABLED", True),
        )

    def safety(self) -> dict[str, Any]:
        return {
            "paper_trading": True,
            "account_reads": False,
            "live_trading": False,
            "authenticated_client": False,
            "public_data_only": True,
            "orders_submitted": False,
            "paper_entries_enabled": self.entries_enabled,
            "starting_capital": self.initial_capital,
        }


@dataclass
class Position:
    position_id: str
    lane: str
    event_id: str
    market_id: str
    question: str
    side: str
    token_id: str
    quantity: float
    entry_price: float
    entry_fee: float
    total_cost: float
    model_probability: float
    edge: float
    opened_at: str
    resolved: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "position_id": self.position_id,
            "lane": self.lane,
            "event_id": self.event_id,
            "market_id": self.market_id,
            "question": self.question,
            "side": self.side,
            "token_id": self.token_id,
            "quantity": money(self.quantity),
            "entry_price": money(self.entry_price),
            "entry_fee": money(self.entry_fee),
            "total_cost": money(self.total_cost),
            "model_probability": f"{self.model_probability:.8f}",
            "edge": f"{self.edge:.8f}",
            "opened_at": self.opened_at,
            "resolved": self.resolved,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Position":
        return cls(
            position_id=str(data["position_id"]),
            lane=str(data["lane"]),
            event_id=str(data["event_id"]),
            market_id=str(data["market_id"]),
            question=str(data["question"]),
            side=str(data["side"]),
            token_id=str(data["token_id"]),
            quantity=float(data["quantity"]),
            entry_price=float(data["entry_price"]),
            entry_fee=float(data["entry_fee"]),
            total_cost=float(data["total_cost"]),
            model_probability=float(data["model_probability"]),
            edge=float(data["edge"]),
            opened_at=str(data["opened_at"]),
            resolved=bool(data.get("resolved", False)),
        )


@dataclass
class State:
    cash: float
    cycle: int = 0
    positions: list[Position] = field(default_factory=list)
    trades: int = 0
    settlements: int = 0
    realized_pnl: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "cash": money(self.cash),
            "cycle": self.cycle,
            "positions": [position.as_dict() for position in self.positions],
            "trades": self.trades,
            "settlements": self.settlements,
            "realized_pnl": money(self.realized_pnl),
        }

    @classmethod
    def from_path(cls, path: Path, initial_capital: float) -> "State":
        if not path.is_file():
            return cls(cash=initial_capital)
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            cash=float(data["cash"]),
            cycle=int(data.get("cycle", 0)),
            positions=[Position.from_dict(item) for item in data.get("positions", [])],
            trades=int(data.get("trades", 0)),
            settlements=int(data.get("settlements", 0)),
            realized_pnl=float(data.get("realized_pnl", 0)),
        )


def fee_for(quantity: float, price: float, schedule: dict[str, Any] | None) -> float:
    schedule = schedule or {}
    rate = float(schedule.get("rate", 0.0) or 0.0)
    exponent = float(schedule.get("exponent", 1.0) or 1.0)
    if not schedule or not schedule.get("takerOnly", False):
        return 0.0
    return quantity * rate * (price**exponent) * ((1.0 - price) ** exponent)


def normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def poisson_pmf(lam: float, k: int) -> float:
    return math.exp(-lam) * (lam**k) / math.factorial(k)


def soccer_probabilities(home_golo: float, away_golo: float) -> dict[str, float]:
    home = draw = away = 0.0
    for h in range(11):
        for a in range(11):
            probability = poisson_pmf(home_golo, h) * poisson_pmf(away_golo, a)
            if h > a:
                home += probability
            elif h == a:
                draw += probability
            else:
                away += probability
    total = home + draw + away
    return {"home": home / total, "draw": draw / total, "away": away / total}


class FedModel:
    MONTHS = {"JAN": 1, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}

    def __init__(self, http: PublicHTTP, *, realized_cuts: int = 0) -> None:
        self.http = http
        self.cached_at = 0.0
        self.distribution: dict[int, float] = {0: 1.0}
        self.source = "unavailable"
        self.meetings: list[dict[str, Any]] = []
        self.realized_cuts = realized_cuts

    def refresh(self) -> None:
        if time.time() - self.cached_at < 240:
            return
        markets = self.http.get_json(
            "https://api.elections.kalshi.com/trade-api/v2/markets",
            {"series_ticker": "KXFEDDECISION", "status": "open"},
        ).get("markets", [])
        grouped: dict[str, dict[str, dict[str, Any]]] = {}
        now = datetime.now(UTC)
        for market in markets:
            ticker = str(market.get("event_ticker", ""))
            match = re.fullmatch(r"KXFEDDECISION-(\d{2})([A-Z]{3})", ticker)
            if not match or match.group(1) != str(now.year)[-2:]:
                continue
            month = self.MONTHS.get(match.group(2))
            if month is None:
                continue
            close = parse_dt(market.get("close_time"))
            if close and close <= now:
                continue
            strike = market.get("custom_strike") or {}
            if strike.get("Cut") == "25":
                key = "cut25"
            elif strike.get("Cut") == ">25":
                key = "cut50"
            elif strike.get("Hike") == "0":
                key = "hold"
            else:
                continue
            grouped.setdefault(ticker, {})[key] = market
        distribution: dict[int, float] = {0: 1.0}
        meetings: list[dict[str, Any]] = []
        for ticker, row in sorted(grouped.items()):
            def midpoint(key: str) -> float:
                market = row.get(key)
                if not market:
                    return 0.0
                bid = float(market.get("yes_bid_dollars") or 0.0)
                ask = float(market.get("yes_ask_dollars") or 0.0)
                return max(0.0, min(1.0, (bid + ask) / 2.0))

            cut25 = midpoint("cut25")
            cut50 = midpoint("cut50")
            none = max(0.0, 1.0 - cut25 - cut50)
            mass = none + cut25 + cut50
            if mass <= 0.0:
                continue
            none, cut25, cut50 = none / mass, cut25 / mass, cut50 / mass
            next_distribution: dict[int, float] = {}
            for count, probability in distribution.items():
                next_distribution[count] = next_distribution.get(count, 0.0) + probability * none
                next_distribution[count + 1] = next_distribution.get(count + 1, 0.0) + probability * cut25
                next_distribution[count + 2] = next_distribution.get(count + 2, 0.0) + probability * cut50
            distribution = next_distribution
            meetings.append({"event": ticker, "month": self.MONTHS.get(ticker[-3:], 12), "cut25": cut25, "cut50": cut50, "none": none})
        self.distribution = distribution
        self.meetings = meetings
        self.source = "kalshi-public-fed-decision-midpoints" if meetings else "unavailable"
        self.cached_at = time.time()

    MONTH_NAMES = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }

    @classmethod
    def classify_predicate(cls, question: str, group_item_title: str = "") -> tuple[str, int | None] | None:
        """Remediation item 4: explicit Fed predicate semantics, fail closed.

        Returns one of ``("exact", n)``, ``("at_least", n)``, ``("cut_by", month)``,
        ``("any_cut", None)`` or ``None`` when the question cannot be classified
        unambiguously. Count buckets are never routed to the P(>=1) branch.
        """
        text = f"{question} {group_item_title}".lower().strip()
        if not text:
            return None
        if re.search(r"\b(\d+)\s*\+\s*(?:fed\s+)?rate\s+cuts?\b", text) or re.search(
            r"\b(\d+)\s+or\s+more\s+(?:fed\s+)?rate\s+cuts?\b", text
        ):
            match = re.search(r"\b(\d+)\s*(?:\+|or\s+more)", text)
            return ("at_least", int(match.group(1))) if match else None
        exact = re.search(r"\bwill\s+(\d+)\s+fed\s+rate\s+cuts?\s+happen\b", text) or re.search(
            r"\bexactly\s+(\d+)\s+(?:fed\s+)?(?:rate\s+)?cuts?\b", text
        )
        if exact:
            return ("exact", int(exact.group(1)))
        if re.search(r"\bno\s+(?:fed\s+)?rate\s+cuts?\b", text) or re.search(r"\b0\s+(?:fed\s+)?rate\s+cuts?\b", text):
            return ("exact", 0)
        if "how many" in text:
            group = re.fullmatch(r"\s*(\d+)\s*(?:\(.*\))?\s*", group_item_title.lower())
            return ("exact", int(group.group(1))) if group else None
        if "rate cut" in text:
            for name, month in cls.MONTH_NAMES.items():
                if re.search(rf"\bby\s+(?:the\s+)?{name}\b", text):
                    return ("cut_by", month)
            if re.search(r"\bby\s+(?:end\s+of\s+)?20\d{2}\b", text) or re.search(r"\bin\s+20\d{2}\b", text):
                return ("any_cut", None)
        return None

    def probability(self, market: dict[str, Any]) -> tuple[float, str, dict[str, Any]] | None:
        self.refresh()
        if not self.meetings:
            return None
        predicate = self.classify_predicate(str(market.get("question", "")), str(market.get("groupItemTitle", "")))
        if predicate is None:
            return None
        # The cut-count vector must be a verified partition before any use.
        assert_partition_sums_to_one(self.distribution.values(), name="fed cut-count distribution")
        kind, argument = predicate
        realized = self.realized_cuts
        shifted = {count + realized: probability for count, probability in self.distribution.items()}
        metadata: dict[str, Any] = {
            "predicate": kind,
            "argument": argument,
            "distribution": shifted,
            "meetings": self.meetings,
            "realized_cuts_ytd_assumed": realized,
        }
        if kind == "exact":
            return shifted.get(argument, 0.0), self.source, metadata
        if kind == "at_least":
            return sum(p for count, p in shifted.items() if count >= argument), self.source, metadata
        if kind == "cut_by":
            meetings = [item for item in self.meetings if item["month"] <= argument]
        else:
            meetings = list(self.meetings)
        none = 1.0
        for meeting in meetings:
            none *= meeting["none"]
        if realized > 0:
            return 1.0, self.source, {**metadata, "cutoff_month": argument, "meetings_used": meetings}
        return 1.0 - none, self.source, {**metadata, "cutoff_month": argument, "meetings_used": meetings}


class CPIModel:
    def __init__(self, http: PublicHTTP) -> None:
        self.http = http
        self.cached_at = 0.0
        self.yoy: dict[tuple[int, int], float] = {}
        self.source = "unavailable"

    def refresh(self) -> None:
        if time.time() - self.cached_at < 900:
            return
        year = datetime.now(UTC).year
        payload = self.http.get_json(
            "https://api.bls.gov/publicAPI/v2/timeseries/data/CUUR0000SA0",
            {"startyear": str(year - 2), "endyear": str(year)},
        )
        self.yoy = self.yoy_from_payload(payload)
        self.source = "bls-public-CUUR0000SA0"
        self.cached_at = time.time()

    @staticmethod
    def yoy_from_payload(payload: dict[str, Any]) -> dict[tuple[int, int], float]:
        """Parse a BLS v2 timeseries payload into chronologically sorted YoY prints.

        Remediation item 4: BLS returns newest-first. The declared newest index
        (0) is asserted against the data, and every downstream consumer reads
        the sorted dict so "last" always means "latest", never "January".
        """
        rows = payload.get("Results", {}).get("series", [{}])[0].get("data", [])
        monthly = [
            item for item in rows
            if str(item.get("period", "")).startswith("M") and str(item.get("period")) != "M13"
        ]
        if not monthly:
            return {}

        def key(item: dict[str, Any]) -> tuple[int, int]:
            return (int(item["year"]), int(str(item["period"])[1:]))

        assert_newest_at_index(monthly, newest_index=0, key=key, name="BLS CUUR0000SA0 series")
        values: dict[tuple[int, int], float] = {}
        for item in monthly:
            try:
                values[key(item)] = float(item["value"])
            except (KeyError, TypeError, ValueError):
                continue
        yoy: dict[tuple[int, int], float] = {}
        for (item_year, month) in sorted(values):
            previous = values.get((item_year - 1, month))
            if previous:
                yoy[(item_year, month)] = (values[(item_year, month)] / previous - 1.0) * 100.0
        return yoy

    def probability(
        self, market: dict[str, Any], event_title: str = "", event_description: str = ""
    ) -> tuple[float, str, dict[str, Any]] | None:
        self.refresh()
        title = f"{event_title} {market.get('question', '')} {market.get('groupItemTitle', '')}".lower()
        if "cpi" not in title and "inflation" not in title and "pce" not in title:
            return None
        # Remediation item 4: geography fails closed. The market must name the
        # United States (or the BLS series) and must not name another country.
        geography_text = f"{title} {event_description}".lower()
        if geography_gate(
            geography_text,
            required_markers=US_GEOGRAPHY_MARKERS,
            forbidden_markers=FOREIGN_GEOGRAPHY_MARKERS,
        ) is not None:
            return None
        if "pce" in title and "cpi" not in title:
            return None
        threshold_match = re.search(r"(?:above|more than|over|reach(?:es)?|at least)\s*([0-9]+(?:\.[0-9]+)?)", title)
        if not threshold_match:
            return None
        threshold = float(threshold_match.group(1))
        year = datetime.now(UTC).year
        ordered = sorted(self.yoy.items())
        known = [value for (item_year, _), value in ordered if item_year == year]
        if not known:
            return None
        observed_max = max(known)
        whitelist: str | None = None
        if observed_max > threshold:
            probability = 1.0
            whitelist = "observed_threshold_already_exceeded"
        else:
            months_observed = len(known)
            remaining = max(0, 12 - months_observed)
            history = [value for _, value in ordered][-24:]
            mean = known[-1]  # latest print: ``known`` is chronologically sorted
            if len(history) >= 3:
                slope = (history[-1] - history[-3]) / 2.0
                mean += max(-0.25, min(0.25, slope))
            sigma = max(0.35, (sum((x - sum(history) / len(history)) ** 2 for x in history) / len(history)) ** 0.5)
            survival = 1.0
            for step in range(1, remaining + 1):
                forecast = mean + (step - 1) * 0.02
                chance = 1.0 - normal_cdf((threshold - forecast) / sigma)
                survival *= 1.0 - max(0.0, min(1.0, chance))
            probability = 1.0 - survival
        return probability, self.source, {
            "threshold_percent": threshold,
            "observed_max_percent": observed_max,
            "known_yoy": known,
            "latest_known_period": f"{ordered[-1][0][0]}-{ordered[-1][0][1]:02d}" if ordered else None,
            "forecast_method": "latest-yoy-normal-max-with-history-volatility",
            "probability_whitelist_reason": whitelist,
        }


class ClubEloModel:
    FEDERATIONS = ("USA", "CHN", "COL")

    def __init__(self, http: PublicHTTP) -> None:
        self.http = http
        self.cached_at = 0.0
        self.teams: dict[str, dict[str, float | str]] = {}

    @staticmethod
    def normalize(name: str) -> str:
        name = name.lower().replace("football club", "").replace(" fc", "").replace(" cf", "")
        return re.sub(r"[^a-z0-9]", "", name)

    def refresh(self) -> None:
        if time.time() - self.cached_at < 1800:
            return
        teams: dict[str, dict[str, float | str]] = {}
        pattern = re.compile(
            r'"Elo"\s*:\s*([0-9.]+).*?"Golo"\s*:\s*([0-9.]+).*?"Name"\s*:\s*"([^"]+)"',
            re.S,
        )
        for federation in self.FEDERATIONS:
            try:
                text = self.http.session.get(
                    f"https://clubelo.com/{federation}", timeout=30
                ).text
            except requests.RequestException:
                continue
            for elo, golo, name in pattern.findall(text):
                teams[self.normalize(name)] = {
                    "name": name,
                    "elo": float(elo),
                    "golo": float(golo),
                    "federation": federation,
                }
        self.teams = teams
        self.cached_at = time.time()

    def probability(self, title: str) -> tuple[dict[str, float], dict[str, Any]] | None:
        self.refresh()
        match = re.split(r"\s+vs\.?\s+", title, maxsplit=1, flags=re.I)
        if len(match) != 2:
            return None
        home_name = match[0].strip()
        away_name = re.split(r"\s+-\s+", match[1], maxsplit=1)[0].strip()
        home = self.teams.get(self.normalize(home_name))
        away = self.teams.get(self.normalize(away_name))
        if not home or not away:
            return None
        probabilities = soccer_probabilities(float(home["golo"]), float(away["golo"]))
        return probabilities, {
            "home": home,
            "away": away,
            "model": "clubelo-golo-independent-poisson-score-grid",
        }

    def outcome_probability(self, event_title: str, question: str) -> tuple[float, dict[str, Any]] | None:
        lowered = question.lower()
        if ("win" not in lowered and "draw" not in lowered) or any(
            term in lowered for term in ("spread", "halftime", "second half", "first team", "exact score", "total")
        ):
            return None
        result = self.probability(event_title)
        if result is None:
            return None
        probabilities, metadata = result
        parts = re.split(r"\s+vs\.?\s+", event_title, maxsplit=1, flags=re.I)
        home = parts[0].strip().lower()
        away = re.split(r"\s+-\s+", parts[1], maxsplit=1)[0].strip().lower()
        lowered = question.lower()
        if "draw" in lowered or "end in a draw" in lowered:
            key = "draw"
        elif home in lowered or "win on" in lowered and self.normalize(parts[0]) in self.normalize(question):
            key = "home"
        elif away in lowered or self.normalize(parts[1]) in self.normalize(question):
            key = "away"
        else:
            return None
        return probabilities[key], {**metadata, "outcome": key, "probabilities": probabilities}


class V8Worker:
    def __init__(self, settings: V8Settings) -> None:
        self.settings = settings
        self.http = PublicHTTP()
        self.state_path = settings.data_dir / "state.json"
        self.state = State.from_path(self.state_path, settings.initial_capital)
        self.fed = FedModel(self.http, realized_cuts=settings.fed_realized_cuts_ytd)
        self.cpi = CPIModel(self.http)
        self.sports = ClubEloModel(self.http)
        self.lock_path = settings.data_dir / "worker.lock"
        self.last_error: str | None = None

    def acquire_lock(self) -> None:
        self.settings.data_dir.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise RuntimeError(f"V8 worker lock exists: {self.lock_path}") from exc
        os.write(fd, str(os.getpid()).encode("ascii"))
        os.close(fd)

    def release_lock(self) -> None:
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            pass

    def write_status(self, running: bool, healthy: bool, extra: dict[str, Any] | None = None) -> None:
        payload: dict[str, Any] = {
            "version": "V8",
            "updated_at": now_iso(),
            "running": running,
            "healthy": healthy,
            "last_error": self.last_error,
            "cycle": self.state.cycle,
            "cash": money(self.state.cash),
            "open_positions": len(self.state.positions),
            "paper_trades": self.state.trades,
            "settlements": self.state.settlements,
            "realized_pnl": money(self.state.realized_pnl),
            "safety": self.settings.safety(),
            "lanes": {"fed": self.settings.fed_enabled, "cpi": self.settings.cpi_enabled, "sports": self.settings.sports_enabled},
            "data_dir": str(self.settings.data_dir),
        }
        if extra:
            payload.update(extra)
        atomic_json(self.settings.data_dir / "status.json", payload)

    def discover_events(self, tag_id: str | None = None) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        seen: set[str] = set()
        for page in range(self.settings.max_discovery_pages):
            params: dict[str, Any] = {"closed": "false", "limit": 100, "offset": page * 100}
            if tag_id:
                params["tag_id"] = tag_id
            rows = self.http.get_json("https://gamma-api.polymarket.com/events", params)
            if not rows:
                break
            for event in rows:
                event_id = str(event.get("id", ""))
                if event_id and event_id not in seen:
                    seen.add(event_id)
                    events.append(event)
        return events

    def relevant_markets(self) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
        now = datetime.now(UTC)
        output: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
        event_sets: list[tuple[str, list[dict[str, Any]]]] = []
        if self.settings.fed_enabled or self.settings.cpi_enabled:
            event_sets.append(("macro", self.discover_events()))
        if self.settings.sports_enabled:
            event_sets.append(("sports", self.discover_events("1")))
        for source_group, events in event_sets:
            for event in events:
                title = str(event.get("title", ""))
                lowered = f"{title} {event.get('description', '')}".lower()
                end = parse_dt(event.get("endDate"))
                if not end or end <= now or (end - now).days > self.settings.maximum_event_days:
                    continue
                if source_group == "macro" and not any(word in lowered for word in ("fed", "inflation", "cpi", "pce")):
                    continue
                if source_group == "sports":
                    date_match = re.search(r"20\d{2}-\d{2}-\d{2}", title)
                    if date_match and date_match.group(0) < now.date().isoformat():
                        continue
                    if " vs " not in title.lower() and " vs. " not in title.lower():
                        continue
                for market in event.get("markets", []):
                    if not market.get("acceptingOrders") or not market.get("enableOrderBook", True):
                        continue
                    if source_group == "sports":
                        question = str(market.get("question", "")).lower()
                        if "win" not in question and "draw" not in question:
                            continue
                        if any(term in question for term in ("spread", "halftime", "second half", "first team", "exact score", "total")):
                            continue
                    if float(market.get("liquidityNum", market.get("liquidity", 0)) or 0) < self.settings.minimum_liquidity:
                        continue
                    output.append((source_group, event, market))
        return output

    def book(self, token_id: str) -> dict[str, Any]:
        return self.http.get_json("https://clob.polymarket.com/book", {"token_id": token_id})

    def book_block_reason(self, book: dict[str, Any], *, now: datetime | None = None) -> str | None:
        """Remediation item 4: stale, thin or wide books are hard rejects."""
        asks = sorted(((float(x["price"]), float(x["size"])) for x in book.get("asks", [])), key=lambda row: row[0])
        bids = sorted(((float(x["price"]), float(x["size"])) for x in book.get("bids", [])), key=lambda row: -row[0])
        reject = book_hard_reject(
            best_bid=bids[0][0] if bids else None,
            best_ask=asks[0][0] if asks else None,
            best_ask_size=asks[0][1] if asks else None,
        )
        if reject is not None:
            return reject
        raw_timestamp = book.get("timestamp")
        price_at: datetime | None = None
        if raw_timestamp not in (None, ""):
            try:
                stamp = float(raw_timestamp)
                price_at = datetime.fromtimestamp(stamp / 1000.0 if stamp > 1e11 else stamp, tz=UTC)
            except (TypeError, ValueError):
                price_at = None
        try:
            check_price_age(
                decision_at=now or datetime.now(UTC),
                price_at=price_at,
                max_age_seconds=self.settings.max_price_age_seconds,
            )
        except SanityError as exc:
            return f"stale or untimestamped price: {exc}"
        return None

    @staticmethod
    def best_ask(book: dict[str, Any], quantity: float) -> tuple[float, float] | None:
        asks = sorted(((float(x["price"]), float(x["size"])) for x in book.get("asks", [])), key=lambda row: row[0])
        remaining = quantity
        cost = 0.0
        for price, size in asks:
            fill = min(remaining, size)
            cost += fill * price
            remaining -= fill
            if remaining <= 1e-9:
                return cost / quantity, quantity
        return None

    def model_probability(
        self, lane: str, event: dict[str, Any], market: dict[str, Any]
    ) -> tuple[float, str, dict[str, Any]] | None:
        if lane == "macro":
            title = f"{event.get('title', '')} {market.get('question', '')}"
            if "cpi" in title.lower() or "inflation" in title.lower() or "pce" in title.lower():
                return self.cpi.probability(
                    market, str(event.get("title", "")), str(event.get("description", "") or "")
                )
            return self.fed.probability(market)
        sports_probability = self.sports.outcome_probability(str(event.get("title", "")), str(market.get("question", "")))
        if sports_probability is None:
            return None
        probability, metadata = sports_probability
        return probability, "clubelo-public-golo", metadata

    def settle_positions(self) -> int:
        settled = 0
        for position in list(self.state.positions):
            if position.resolved:
                continue
            try:
                market = self.http.get_json(f"https://gamma-api.polymarket.com/markets/{position.market_id}")
            except RuntimeError as exc:
                self.last_error = str(exc)
                continue
            if not market.get("closed") or market.get("umaResolutionStatus") not in {"resolved", "proposed", "disputed", None}:
                continue
            try:
                prices = json.loads(market.get("outcomePrices", "[]"))
                selected = float(prices[0 if position.side == "YES" else 1])
            except (ValueError, TypeError, IndexError, json.JSONDecodeError):
                continue
            if selected not in {0.0, 1.0} and not (selected <= 0.001 or selected >= 0.999):
                continue
            payout = position.quantity if selected >= 0.999 else 0.0
            pnl = payout - position.total_cost
            self.state.cash += payout
            self.state.realized_pnl += pnl
            self.state.settlements += 1
            position.resolved = True
            append_jsonl(
                self.settings.data_dir / "settlements.jsonl",
                {
                    "timestamp": now_iso(),
                    "position_id": position.position_id,
                    "market_id": position.market_id,
                    "side": position.side,
                    "payout": money(payout),
                    "entry_cost": money(position.total_cost),
                    "realized_pnl": money(pnl),
                    "resolution_price": selected,
                    "source": "public-gamma-market-resolution",
                },
            )
            settled += 1
        self.state.positions = [position for position in self.state.positions if not position.resolved]
        return settled

    def evaluate_market(
        self, lane: str, event: dict[str, Any], market: dict[str, Any], occupied_events: set[str], lane_counts: dict[str, int]
    ) -> dict[str, Any]:
        question = str(market.get("question", ""))
        base = {"timestamp": now_iso(), "lane": lane, "event_id": str(event.get("id")), "market_id": str(market.get("id")), "question": question}
        try:
            model = self.model_probability(lane, event, market)
        except SanityError as exc:
            return {**base, "status": "rejected", "reason": "sanity_invariant_violation", "detail": str(exc)}
        if model is None:
            return {**base, "status": "rejected", "reason": "model_unavailable"}
        probability, source, metadata = model
        # Remediation item 4: clamp-and-flag. Extreme model outputs are only
        # tradeable when the model names a whitelisted deterministic reason.
        sanity = check_probability(
            float(probability),
            whitelist_reason=metadata.get("probability_whitelist_reason") if isinstance(metadata, dict) else None,
        )
        probability = float(sanity.value)
        if sanity.flagged:
            return {
                **base, "status": "rejected", "reason": "probability_out_of_bounds_flagged",
                "detail": sanity.reason, "model_probability": probability, "model_source": source,
            }
        try:
            token_ids = json.loads(market.get("clobTokenIds", "[]"))
        except (TypeError, json.JSONDecodeError):
            token_ids = []
        if len(token_ids) != 2:
            return {**base, "status": "rejected", "reason": "token_ids_unavailable", "model_probability": probability, "model_source": source}
        if str(event.get("id")) in occupied_events:
            return {**base, "status": "rejected", "reason": "event_already_held", "model_probability": probability, "model_source": source}
        if len(self.state.positions) >= self.settings.max_positions:
            return {**base, "status": "rejected", "reason": "global_position_cap", "model_probability": probability, "model_source": source}
        if lane_counts.get(lane, 0) >= self.settings.max_positions_per_lane:
            return {**base, "status": "rejected", "reason": "lane_position_cap", "model_probability": probability, "model_source": source}
        quantity = max(5.0, float(market.get("orderMinSize", 5) or 5))
        side_rows: list[tuple[str, float, float, float, str]] = []
        blocked_reasons: list[dict[str, Any]] = []
        for index, side in enumerate(("YES", "NO")):
            side_probability = probability if side == "YES" else 1.0 - probability
            try:
                book = self.book(str(token_ids[index]))
                quote = self.best_ask(book, quantity)
                book_hash = str(book.get("hash", ""))
            except RuntimeError as exc:
                self.last_error = str(exc)
                quote = None
                book_hash = ""
            if quote is None:
                continue
            price, filled_quantity = quote
            fee = fee_for(filled_quantity, price, market.get("feeSchedule"))
            all_in = filled_quantity * price + fee
            edge = side_probability - (all_in / filled_quantity)
            side_rows.append((side, side_probability, price, fee, book_hash))
            block_reason = self.book_block_reason(book)
            if block_reason is not None:
                blocked_reasons.append({"side": side, "reason": block_reason})
                continue
            if edge >= self.settings.min_edge and all_in <= self.settings.max_order_notional:
                if not self.settings.entries_enabled:
                    # Remediation item 1: real gate. Telemetry continues; no state mutation.
                    blocked_reasons.append({"side": side, "reason": "entries_disabled_by_profile", "edge": edge})
                    continue
                if self.state.cash - all_in < self.settings.initial_capital * self.settings.reserve_fraction:
                    continue
                position = Position(
                    position_id=f"v8-{self.state.cycle}-{market.get('id')}-{side}",
                    lane=lane,
                    event_id=str(event.get("id")),
                    market_id=str(market.get("id")),
                    question=question,
                    side=side,
                    token_id=str(token_ids[index]),
                    quantity=filled_quantity,
                    entry_price=price,
                    entry_fee=fee,
                    total_cost=all_in,
                    model_probability=side_probability,
                    edge=edge,
                    opened_at=now_iso(),
                )
                self.state.cash -= all_in
                self.state.positions.append(position)
                self.state.trades += 1
                occupied_events.add(str(event.get("id")))
                lane_counts[lane] = lane_counts.get(lane, 0) + 1
                record = {
                    **base,
                    "status": "paper_entry",
                    "side": side,
                    "quantity": money(filled_quantity),
                    "price": money(price),
                    "fee": money(fee),
                    "all_in_cost": money(all_in),
                    "model_probability": side_probability,
                    "edge": edge,
                    "model_source": source,
                    "model_metadata": metadata,
                    "book_hash": book_hash,
                    "paper_only": True,
                    "submitted": False,
                }
                append_jsonl(self.settings.data_dir / "paper_trades.jsonl", record)
                return record
        reason = "insufficient_edge_or_budget"
        if any(item["reason"] == "entries_disabled_by_profile" for item in blocked_reasons):
            reason = "entries_disabled_by_profile"
        elif blocked_reasons:
            reason = "book_sanity_reject"
        return {
            **base,
            "status": "rejected",
            "reason": reason,
            "blocked": blocked_reasons,
            "model_probability": probability,
            "model_source": source,
            "model_metadata": metadata,
            "quotes": [
                {"side": side, "probability": p, "ask": price, "fee": fee, "book_hash": book_hash}
                for side, p, price, fee, book_hash in side_rows
            ],
        }

    def cycle(self) -> dict[str, Any]:
        self.last_error = None
        settled = self.settle_positions()
        markets = self.relevant_markets()
        occupied_events = {position.event_id for position in self.state.positions}
        lane_counts: dict[str, int] = {}
        for position in self.state.positions:
            lane_counts[position.lane] = lane_counts.get(position.lane, 0) + 1
        records: list[dict[str, Any]] = []
        entries = 0
        for lane, event, market in markets:
            try:
                record = self.evaluate_market(lane, event, market, occupied_events, lane_counts)
            except (RuntimeError, ValueError, TypeError, KeyError) as exc:
                self.last_error = str(exc)
                record = {
                    "timestamp": now_iso(),
                    "lane": lane,
                    "event_id": str(event.get("id")),
                    "market_id": str(market.get("id")),
                    "status": "error",
                    "reason": str(exc),
                }
            append_jsonl(self.settings.data_dir / "candidates.jsonl", record)
            records.append(record)
            if record.get("status") == "paper_entry":
                entries += 1
        self.state.cycle += 1
        atomic_json(self.state_path, self.state.as_dict())
        summary = {
            "timestamp": now_iso(),
            "cycle": self.state.cycle,
            "markets_evaluated": len(markets),
            "paper_entries": entries,
            "settlements": settled,
            "open_positions": len(self.state.positions),
            "paper_trades": self.state.trades,
            "cash": money(self.state.cash),
            "realized_pnl": money(self.state.realized_pnl),
            "rejections": sum(record.get("status") == "rejected" for record in records),
            "errors": sum(record.get("status") == "error" for record in records),
            "lane_counts": lane_counts,
        }
        append_jsonl(self.settings.data_dir / "scans.jsonl", summary)
        self.write_status(running=True, healthy=self.last_error is None, extra=summary)
        return summary

    def run(self, cycles: int = 0) -> None:
        self.acquire_lock()
        self.write_status(running=True, healthy=True)
        completed = 0
        try:
            while cycles == 0 or completed < cycles:
                self.cycle()
                completed += 1
                if cycles == 0:
                    time.sleep(self.settings.scan_interval_seconds)
        finally:
            self.write_status(running=False, healthy=self.last_error is None)
            self.release_lock()


def paper_status(settings: V8Settings) -> dict[str, Any]:
    status_path = settings.data_dir / "status.json"
    if not status_path.is_file():
        return {"version": "V8", "running": False, "data_dir": str(settings.data_dir), "safety": settings.safety()}
    return json.loads(status_path.read_text(encoding="utf-8"))
