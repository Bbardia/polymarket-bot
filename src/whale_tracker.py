"""
Whale Consensus Signal — Track top Polymarket wallets for edge amplification.

Monitors 10-15 top-performing wallets via Polymarket's public data API.
When 3+ whales take the same side on a market, generates a confidence boost
that amplifies edge in other strategies (weather, sports, Fed, etc.).

NOT a standalone strategy — purely a signal amplifier.

Data source: https://data-api.polymarket.com/activity
"""
import json
import time
import requests
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional
from loguru import logger


@dataclass
class WhalePosition:
    """A single trade by a tracked whale wallet."""
    wallet: str          # Abbreviated address
    token_id: str
    side: str            # "BUY" or "SELL"
    usdc_size: float
    timestamp: str
    question: str
    condition_id: str


@dataclass
class WhaleConsensus:
    """Consensus signal: multiple whales agree on the same market side."""
    token_id: str
    question: str
    side: str            # "BUY_YES" or "BUY_NO" (normalized for our strategy use)
    whale_count: int
    total_usd: float
    confidence_boost: float
    wallets: list[str] = field(default_factory=list)


class WhaleTracker:
    """
    Tracks top Polymarket wallets and detects consensus signals.

    Usage:
        tracker = WhaleTracker(data_dir="data")
        tracker.fetch_whale_activity(hours_back=4)  # Poll every 10 min
        boost = tracker.get_confidence_boost(token_id, "BUY_YES")
        # boost is 0.0, 0.05, 0.07, or 0.10
    """

    # Top-performing Polymarket wallet addresses (funder/proxy addresses)
    # Verified from Polymarket leaderboard, PolymarketAnalytics, and PolygonScan.
    # Sources: polymarket.com/leaderboard, polymarketanalytics.com, public profile pages.
    WHALE_WALLETS: dict[str, str] = {
        "0x56687bf447db6ffa42ffe2204a05edaa20f55839": "Theo4",
        "0x1f2dd6d473f3e824cd2f8a89d9c69fb96f6ad0cf": "Fredi9999",
        "0x204f72f35326db932158cba6adff0b9a1da95e14": "swisstony",
        "0x6a72f61820b26b1fe4d956e17b6dc2a1ea3033ee": "kch123",
        "0x9d84ce0306f8551e02efef1680475fc0f1dc1344": "Domer",
        "0xf2f6af4f27ec2dcf4072095ab804016e14cd5817": "weather_wallet_1",
        "0x492442EaB586F242B53bDa933fD5dE859c8A3782": "leaderboard_5",
        "0x2a2C53bD278c04DA9962Fcf96490E17F3DfB9Bc1": "leaderboard_6",
        "0x9f47f1fcb1701bf9eaf31236ad39875e5d60af93": "TheGuru",        # Leaderboard trader
        "0xd218e474776403a330142299f7796e8ba32eb5c9": "leaderboard_7",
        "0x10032987bbc6ec6a754a4f98cb7282ad25474b76": "atrader1",       # Leaderboard trader
    }

    # Copy-trade wallets — DISABLED 2026-03-31 (esports timing issues)
    COPY_TRADE_WALLETS: dict[str, str] = {
        "0x25e28169faea17421fcd4cc361f6436d1e449a09": "xdd07070",
        "0x2005d16a84ceefa912d4e380cd32e7ff827875ea": "rn1",
    }
    COPY_MIN_USDC = 1000
    COPY_MAX_POSITIONS = 3
    COPY_MAX_COST = 3.0

    # Weather whale wallets — top weather traders on Polymarket.
    # Their recent BUY activity is used as a confirmation signal for our weather trades.
    # When a weather whale buys the same token we see edge on → boost confidence.
    WEATHER_WHALE_WALLETS: dict[str, str] = {
        "0xf2f6af4f27ec2dcf4072095ab804016e14cd5817": "weather_wallet_1",
        "0x594edb9112f526fa6a80b8f858a6379c8a2c1c11": "weather_wallet_2",
        "0x0f37cb80dee49d55b5f6d9e595d52591d6371410": "weather_wallet_3",
        "0x05e70727a2e2dcd079baa2ef1c0b88af06bb9641": "weather_wallet_4",
        "0xd8f8c13644ea84d62e1ec88c5d1215e436eb0f11": "weather_wallet_5",
        "0xacc8e9dcabf9d65a5c78e3bec6941ed53a2b7d08": "weather_wallet_6",
        "0x15ceffed7bf820cd2d90f90ea24ae9909f5cd5fa": "weather_wallet_7",
    }

    # Copy-trade targets: selected public weather wallets.
    # These are the signal source for the optional whale copy-trade strategy.
    # Labels are generic to keep the public repo free of performance claims.
    COPY_WEATHER_WALLETS: dict[str, str] = {
        "0x594edb9112f526fa6a80b8f858a6379c8a2c1c11": "copy_weather_1",
        "0x1f66796b45581868376365aef54b51eb84184c8d": "copy_weather_2",
        "0x331bf91c132af9d921e1908ca0979363fc47193f": "copy_weather_3",
        "0x02f34a597773e64dee60726d893f9651151794cb": "copy_weather_4",
    }
    COPY_WEATHER_MAX_PRICE = 0.05     # Only copy low-priced contracts
    COPY_WEATHER_MAX_POSITIONS = 15   # Max simultaneous copy positions (raised from 10 for 5 wallets)
    COPY_WEATHER_SEEN_FILE = "copy_weather_seen.json"  # Persist dedup across restarts

    DATA_API_HOST = "https://data-api.polymarket.com"
    REQUEST_DELAY = 0.3  # seconds between API calls (rate limiting)

    def __init__(self, data_dir: str, cache_ttl: int = 600):
        self._data_dir = Path(data_dir)
        self._cache_ttl = cache_ttl  # 10 minutes
        self._trades: list[WhalePosition] = []
        self._consensus: list[WhaleConsensus] = []
        self._last_scan: Optional[str] = None
        self._last_scan_ts: float = 0
        self._persist_path = self._data_dir / "whale_activity.json"
        # Copy-trade state
        self._copy_trades: list[WhalePosition] = []
        self._last_copy_scan_ts: float = 0
        self._seen_copy_ids: set[str] = set()  # (wallet+token_id+timestamp) dedup
        # Weather whale state — scanned every 2 min, provides confirmation signal
        self._weather_whale_trades: list[WhalePosition] = []
        self._weather_whale_tokens: dict[str, dict] = {}  # token_id → {wallets, total_usdc, count}
        self._last_weather_whale_scan_ts: float = 0
        # Weather copy-trade state — persisted dedup across restarts
        # Use list for insertion order + set for O(1) lookup
        self._copy_weather_seen_list: list[str] = []
        self._copy_weather_seen: set[str] = set()
        self._last_copy_weather_scan_ts: float = 0
        self._load()
        self._load_copy_weather_seen()

    def fetch_whale_activity(self, hours_back: int = 4) -> list[WhalePosition]:
        """
        Poll data API for recent trades from tracked wallets.
        Call every 10 minutes from the main loop.
        """
        if time.time() - self._last_scan_ts < self._cache_ttl:
            return self._trades

        cutoff_ts = time.time() - (hours_back * 3600)
        all_trades: list[WhalePosition] = []
        wallets_scanned = 0

        for addr, label in self.WHALE_WALLETS.items():
            try:
                resp = requests.get(
                    f"{self.DATA_API_HOST}/activity",
                    params={"user": addr, "limit": 20},
                    timeout=8,
                )
                if resp.status_code != 200:
                    logger.debug(f"Whale {label}: HTTP {resp.status_code}")
                    time.sleep(self.REQUEST_DELAY)
                    continue

                activities = resp.json()
                if not isinstance(activities, list):
                    time.sleep(self.REQUEST_DELAY)
                    continue

                for act in activities:
                    if act.get("type") != "TRADE":
                        continue

                    # Timestamp is Unix seconds (int) from the API
                    ts_raw = act.get("timestamp", 0)
                    try:
                        ts_val = int(ts_raw) if ts_raw else 0
                    except (ValueError, TypeError):
                        ts_val = 0
                    if ts_val and ts_val < cutoff_ts:
                        continue

                    token_id = act.get("asset", act.get("tokenId", ""))
                    if not token_id:
                        continue

                    ts_iso = datetime.fromtimestamp(ts_val, tz=timezone.utc).isoformat() if ts_val else ""

                    trade = WhalePosition(
                        wallet=f"{addr[:6]}...{addr[-4:]}",
                        token_id=token_id,
                        side=act.get("side", "BUY"),
                        usdc_size=float(act.get("usdcSize", 0)),
                        timestamp=ts_iso,
                        question=act.get("title", act.get("question", "")),
                        condition_id=act.get("conditionId", ""),
                    )
                    all_trades.append(trade)

                wallets_scanned += 1

            except requests.exceptions.RequestException as e:
                logger.debug(f"Whale {label} fetch failed: {e}")
            except Exception as e:
                logger.debug(f"Whale {label} parse error: {e}")

            time.sleep(self.REQUEST_DELAY)

        self._trades = all_trades
        self._last_scan = datetime.now(timezone.utc).isoformat()
        self._last_scan_ts = time.time()

        # Recompute consensus
        self._consensus = self._compute_consensus()

        logger.info(
            f"  Whale scan: {wallets_scanned} wallets, {len(all_trades)} trades, "
            f"{len(self._consensus)} consensus signals"
        )

        self._persist()
        return self._trades

    def _compute_consensus(self, min_whales: int = 3) -> list[WhaleConsensus]:
        """Group trades by token_id+side, return consensus where 3+ wallets agree."""
        # Group by (token_id, side)
        groups: dict[tuple[str, str], list[WhalePosition]] = {}
        for t in self._trades:
            key = (t.token_id, t.side)
            groups.setdefault(key, []).append(t)

        consensus = []
        for (token_id, side), trades in groups.items():
            # Deduplicate by wallet (same whale may have multiple trades)
            unique_wallets = list({t.wallet for t in trades})
            if len(unique_wallets) < min_whales:
                continue

            total_usd = sum(t.usdc_size for t in trades)
            whale_count = len(unique_wallets)

            # Map raw side to our strategy side
            if side == "BUY":
                normalized_side = "BUY_YES"
            elif side == "SELL":
                normalized_side = "BUY_NO"
            else:
                normalized_side = "BUY_YES"

            # Boost scales with whale count
            if whale_count >= 6:
                boost = 0.10
            elif whale_count >= 4:
                boost = 0.07
            else:
                boost = 0.05

            question = trades[0].question if trades else ""
            consensus.append(WhaleConsensus(
                token_id=token_id,
                question=question,
                side=normalized_side,
                whale_count=whale_count,
                total_usd=total_usd,
                confidence_boost=boost,
                wallets=unique_wallets,
            ))

        # Sort by whale count descending
        consensus.sort(key=lambda c: c.whale_count, reverse=True)
        return consensus

    def find_consensus(self, min_whales: int = 3) -> list[WhaleConsensus]:
        """Return current consensus signals."""
        return [c for c in self._consensus if c.whale_count >= min_whales]

    def get_confidence_boost(self, token_id: str, side: str) -> float:
        """
        Get the confidence boost for a specific token/side combination.
        Returns 0.0 if no whale consensus, or 0.05/0.07/0.10 if consensus exists.
        """
        for c in self._consensus:
            if c.token_id == token_id and c.side == side:
                return c.confidence_boost
        return 0.0

    def fetch_weather_whale_activity(self, hours_back: int = 4) -> dict[str, dict]:
        """
        Poll top weather wallets for recent BUY activity.
        Returns {token_id: {"wallets": [...], "total_usdc": float, "count": int}}

        Call every 2 minutes from the main loop. Cached for 120s.
        Only returns BUY trades — when weather whales are buying a token,
        it confirms our ensemble forecast's edge on that market.
        """
        if time.time() - self._last_weather_whale_scan_ts < 120:
            return self._weather_whale_tokens

        cutoff_ts = time.time() - (hours_back * 3600)
        all_trades: list[WhalePosition] = []
        wallets_scanned = 0

        for addr, label in self.WEATHER_WHALE_WALLETS.items():
            try:
                resp = requests.get(
                    f"{self.DATA_API_HOST}/activity",
                    params={"user": addr, "limit": 50},
                    timeout=8,
                )
                if resp.status_code != 200:
                    time.sleep(self.REQUEST_DELAY)
                    continue

                activities = resp.json()
                if not isinstance(activities, list):
                    time.sleep(self.REQUEST_DELAY)
                    continue

                for act in activities:
                    if act.get("type") != "TRADE":
                        continue
                    if act.get("side") != "BUY":
                        continue

                    ts_raw = act.get("timestamp", 0)
                    try:
                        ts_val = int(ts_raw) if ts_raw else 0
                    except (ValueError, TypeError):
                        ts_val = 0
                    if ts_val and ts_val < cutoff_ts:
                        continue

                    token_id = act.get("asset", act.get("tokenId", ""))
                    if not token_id:
                        continue

                    ts_iso = datetime.fromtimestamp(ts_val, tz=timezone.utc).isoformat() if ts_val else ""
                    all_trades.append(WhalePosition(
                        wallet=label,
                        token_id=token_id,
                        side="BUY",
                        usdc_size=float(act.get("usdcSize", 0)),
                        timestamp=ts_iso,
                        question=act.get("title", act.get("question", "")),
                        condition_id=act.get("conditionId", ""),
                    ))

                wallets_scanned += 1
            except Exception as e:
                logger.debug(f"Weather whale {label} fetch failed: {e}")

            time.sleep(self.REQUEST_DELAY)

        # Build token_id → aggregated info
        token_map: dict[str, dict] = {}
        for t in all_trades:
            if t.token_id not in token_map:
                token_map[t.token_id] = {"wallets": [], "total_usdc": 0.0, "count": 0}
            entry = token_map[t.token_id]
            if t.wallet not in entry["wallets"]:
                entry["wallets"].append(t.wallet)
            entry["total_usdc"] += t.usdc_size
            entry["count"] += 1

        self._weather_whale_trades = all_trades
        self._weather_whale_tokens = token_map
        self._last_weather_whale_scan_ts = time.time()

        weather_tokens = len(token_map)
        if weather_tokens > 0:
            logger.info(
                f"  Weather whale scan: {wallets_scanned} wallets, "
                f"{len(all_trades)} buys across {weather_tokens} tokens"
            )
        else:
            logger.debug(f"  Weather whale scan: {wallets_scanned} wallets, 0 weather buys")

        return token_map

    def get_weather_whale_signal(self, token_id: str) -> dict:
        """
        Check if weather whales are buying a specific token.

        Returns:
            {"confirmed": bool, "whale_count": int, "wallets": [...],
             "total_usdc": float, "boost": float}

        Boost values:
            0 whales: 0.00 (no confirmation)
            1 whale:  0.03 (mild confirmation)
            2 whales: 0.05 (strong confirmation)
            3+ whales: 0.08 (very strong — multiple pros agree)
        """
        info = self._weather_whale_tokens.get(token_id)
        if not info or not info["wallets"]:
            return {"confirmed": False, "whale_count": 0, "wallets": [],
                    "total_usdc": 0.0, "boost": 0.0}

        whale_count = len(info["wallets"])
        if whale_count >= 3:
            boost = 0.08
        elif whale_count >= 2:
            boost = 0.05
        else:
            boost = 0.03

        return {
            "confirmed": True,
            "whale_count": whale_count,
            "wallets": info["wallets"],
            "total_usdc": info["total_usdc"],
            "boost": boost,
        }

    def fetch_copy_weather_signals(self, hours_back: int = 6) -> list[WhalePosition]:
        """
        Poll the 3 best weather wallets for NEW BUY trades on weather markets.
        Returns only unseen trades (deduped by wallet+token_id, persisted across restarts).

        This is the PRIMARY signal source for the whale copy-trade strategy.
        Called every cycle (60s). Internal 90s cache to avoid hammering the API.

        Filters:
        - BUY side only (ignore sells)
        - Weather markets only (title contains temperature/°F/°C keywords)
        - Within hours_back window
        """
        if time.time() - self._last_copy_weather_scan_ts < 90:
            return []  # Rate limit: scan at most every 90s

        cutoff_ts = time.time() - (hours_back * 3600)
        new_signals: list[WhalePosition] = []
        wallets_scanned = 0
        total_fetched = 0

        for addr, label in self.COPY_WEATHER_WALLETS.items():
            try:
                resp = requests.get(
                    f"{self.DATA_API_HOST}/activity",
                    params={"user": addr, "limit": 30},
                    timeout=10,
                )
                if resp.status_code != 200:
                    logger.warning(f"Copy-weather {label}: HTTP {resp.status_code}")
                    time.sleep(self.REQUEST_DELAY)
                    continue

                activities = resp.json()
                if not isinstance(activities, list):
                    logger.warning(f"Copy-weather {label}: unexpected response type")
                    time.sleep(self.REQUEST_DELAY)
                    continue

                wallets_scanned += 1

                for act in activities:
                    if act.get("type") != "TRADE":
                        continue
                    if act.get("side") != "BUY":
                        continue

                    ts_raw = act.get("timestamp", 0)
                    try:
                        ts_val = int(ts_raw) if ts_raw else 0
                    except (ValueError, TypeError):
                        ts_val = 0
                    if ts_val and ts_val < cutoff_ts:
                        continue

                    token_id = act.get("asset", act.get("tokenId", ""))
                    if not token_id:
                        continue

                    title = act.get("title", act.get("question", ""))

                    # Filter: weather markets only
                    if not self._is_weather_market(title):
                        continue

                    total_fetched += 1

                    # Dedup key: wallet + token_id (one signal per wallet per token)
                    dedup_key = f"{addr}:{token_id}"
                    if dedup_key in self._copy_weather_seen:
                        continue

                    # NEW signal!
                    self._copy_weather_seen.add(dedup_key)
                    self._copy_weather_seen_list.append(dedup_key)

                    ts_iso = datetime.fromtimestamp(ts_val, tz=timezone.utc).isoformat() if ts_val else ""
                    usdc_size = float(act.get("usdcSize", 0))

                    trade = WhalePosition(
                        wallet=label,
                        token_id=token_id,
                        side="BUY",
                        usdc_size=usdc_size,
                        timestamp=ts_iso,
                        question=title,
                        condition_id=act.get("conditionId", ""),
                    )
                    new_signals.append(trade)
                    logger.info(
                        f"  📡 COPY-WEATHER signal: {label} BUY ${usdc_size:.2f} — "
                        f"{title[:60]}"
                    )

            except requests.exceptions.RequestException as e:
                logger.warning(f"Copy-weather {label} fetch failed: {e}")
            except Exception as e:
                logger.warning(f"Copy-weather {label} parse error: {e}")

            time.sleep(self.REQUEST_DELAY)

        self._last_copy_weather_scan_ts = time.time()

        # Persist seen IDs so we don't re-signal after restart
        self._save_copy_weather_seen()

        # Trim seen set: remove entries older than 48h to prevent unbounded growth.
        # We can't check timestamps from the set alone, so just cap the size.
        if len(self._copy_weather_seen_list) > 1000:
            # Keep most recent 500 (ordered — newest entries)
            self._copy_weather_seen_list = self._copy_weather_seen_list[-500:]
            self._copy_weather_seen = set(self._copy_weather_seen_list)
            self._save_copy_weather_seen()

        logger.info(
            f"  Copy-weather scan: {wallets_scanned}/{len(self.COPY_WEATHER_WALLETS)} wallets, "
            f"{total_fetched} weather buys fetched, {len(new_signals)} NEW signals, "
            f"{len(self._copy_weather_seen)} total seen"
        )

        return new_signals

    @staticmethod
    def _is_weather_market(title: str) -> bool:
        """Check if a market title is a weather market."""
        if not title:
            return False
        tl = title.lower()
        # Weather markets contain temperature keywords
        weather_keywords = [
            "temperature", "°f", "°c", "highest temp", "lowest temp",
            "precipitation", "rainfall", "snowfall", "wind speed",
            "wind gust",
        ]
        return any(kw in tl for kw in weather_keywords)

    def _load_copy_weather_seen(self):
        """Load persisted dedup set from disk."""
        path = self._data_dir / self.COPY_WEATHER_SEEN_FILE
        if not path.exists():
            return
        try:
            with open(path) as f:
                data = json.load(f)
            self._copy_weather_seen_list = data.get("seen", [])
            self._copy_weather_seen = set(self._copy_weather_seen_list)
            logger.debug(f"Loaded {len(self._copy_weather_seen)} copy-weather seen IDs")
        except Exception as e:
            logger.debug(f"Copy-weather seen load error: {e}")

    def _save_copy_weather_seen(self):
        """Persist dedup set to disk (atomic write)."""
        path = self._data_dir / self.COPY_WEATHER_SEEN_FILE
        tmp = path.with_suffix(".tmp")
        try:
            with open(tmp, "w") as f:
                json.dump({"seen": self._copy_weather_seen_list,
                           "updated": datetime.now(timezone.utc).isoformat()}, f)
            tmp.rename(path)
        except Exception as e:
            logger.debug(f"Copy-weather seen save error: {e}")

    def get_consensus_for_condition(self, condition_id: str) -> list[WhaleConsensus]:
        """Get consensus signals for a market by condition_id (matches any token)."""
        # First try matching condition_id from trades
        matching_tokens = set()
        for t in self._trades:
            if t.condition_id == condition_id:
                matching_tokens.add(t.token_id)

        return [c for c in self._consensus if c.token_id in matching_tokens]

    def get_summary(self) -> dict:
        """Return summary for status line."""
        return {
            "last_scan": self._last_scan,
            "trades_tracked": len(self._trades),
            "consensus_signals": len(self._consensus),
            "top_consensus": [
                f"{c.question[:30]}... ({c.whale_count} whales, {c.side})"
                for c in self._consensus[:3]
            ],
        }

    def fetch_copy_trade_activity(self, hours_back: int = 2) -> list[WhalePosition]:
        """
        Poll copy-trade wallets for recent large buys.
        Returns only NEW trades not seen before (deduped by wallet+token+timestamp).
        """
        if time.time() - self._last_copy_scan_ts < self._cache_ttl:
            return []

        cutoff_ts = time.time() - (hours_back * 3600)
        new_signals: list[WhalePosition] = []

        for addr, label in self.COPY_TRADE_WALLETS.items():
            try:
                resp = requests.get(
                    f"{self.DATA_API_HOST}/activity",
                    params={"user": addr, "limit": 20},
                    timeout=8,
                )
                if resp.status_code != 200:
                    logger.debug(f"Copy-trade {label}: HTTP {resp.status_code}")
                    time.sleep(self.REQUEST_DELAY)
                    continue

                activities = resp.json()
                if not isinstance(activities, list):
                    time.sleep(self.REQUEST_DELAY)
                    continue

                for act in activities:
                    if act.get("type") != "TRADE":
                        continue

                    # Only BUY side (not sells/exits)
                    if act.get("side") != "BUY":
                        continue

                    ts_raw = act.get("timestamp", 0)
                    try:
                        ts_val = int(ts_raw) if ts_raw else 0
                    except (ValueError, TypeError):
                        ts_val = 0
                    if ts_val and ts_val < cutoff_ts:
                        continue

                    usdc_size = float(act.get("usdcSize", 0))
                    if usdc_size < self.COPY_MIN_USDC:
                        continue

                    token_id = act.get("asset", act.get("tokenId", ""))
                    if not token_id:
                        continue

                    # Dedup key
                    dedup_key = f"{addr}:{token_id}:{ts_val}"
                    if dedup_key in self._seen_copy_ids:
                        continue
                    self._seen_copy_ids.add(dedup_key)

                    ts_iso = datetime.fromtimestamp(ts_val, tz=timezone.utc).isoformat() if ts_val else ""

                    trade = WhalePosition(
                        wallet=label,
                        token_id=token_id,
                        side="BUY",
                        usdc_size=usdc_size,
                        timestamp=ts_iso,
                        question=act.get("title", act.get("question", "")),
                        condition_id=act.get("conditionId", ""),
                    )
                    new_signals.append(trade)
                    logger.info(
                        f"  COPY signal: {label} BUY ${usdc_size:,.0f} — "
                        f"{trade.question[:60]}"
                    )

            except requests.exceptions.RequestException as e:
                logger.debug(f"Copy-trade {label} fetch failed: {e}")
            except Exception as e:
                logger.debug(f"Copy-trade {label} parse error: {e}")

            time.sleep(self.REQUEST_DELAY)

        self._last_copy_scan_ts = time.time()
        self._copy_trades.extend(new_signals)

        # Trim dedup set to prevent unbounded growth (keep last 500)
        if len(self._seen_copy_ids) > 500:
            self._seen_copy_ids = set(list(self._seen_copy_ids)[-300:])

        if new_signals:
            logger.info(f"  Copy-trade scan: {len(new_signals)} new signals from {len(self.COPY_TRADE_WALLETS)} wallets")

        return new_signals

    def _persist(self):
        """Atomic write to whale_activity.json."""
        data = {
            "last_scan": self._last_scan,
            "wallets_scanned": len(self.WHALE_WALLETS),
            "trades": [
                {
                    "wallet": t.wallet,
                    "token_id": t.token_id,
                    "side": t.side,
                    "usdc_size": t.usdc_size,
                    "timestamp": t.timestamp,
                    "question": t.question,
                    "condition_id": t.condition_id,
                }
                for t in self._trades
            ],
            "consensus": [
                {
                    "token_id": c.token_id,
                    "question": c.question,
                    "side": c.side,
                    "whale_count": c.whale_count,
                    "total_usd": c.total_usd,
                    "confidence_boost": c.confidence_boost,
                    "wallets": c.wallets,
                }
                for c in self._consensus
            ],
        }
        tmp_path = self._persist_path.with_suffix(".tmp")
        try:
            with open(tmp_path, "w") as f:
                json.dump(data, f, indent=2)
            tmp_path.rename(self._persist_path)
        except Exception as e:
            logger.debug(f"Whale persist error: {e}")

    def _load(self):
        """Load persisted whale activity."""
        if not self._persist_path.exists():
            return
        try:
            with open(self._persist_path) as f:
                data = json.load(f)
            self._last_scan = data.get("last_scan")
            self._trades = [
                WhalePosition(**t) for t in data.get("trades", [])
            ]
            self._consensus = [
                WhaleConsensus(**c) for c in data.get("consensus", [])
            ]
        except Exception as e:
            logger.debug(f"Whale load error: {e}")
