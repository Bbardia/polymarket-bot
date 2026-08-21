"""
BTC 5-Minute Straddle — Buy both sides of BTC Up/Down markets at low prices.

Strategy:
  1. Discover active BTC 5-minute Up/Down windows (Series 10684)
  2. Place maker limit buy orders on BOTH Up and Down tokens at low prices
  3. When BTC oscillates within the window, both legs fill cheaply
  4. One side always wins ($1.00 payout) — profit = $1.00 - combined cost
  5. If only one leg fills, accept the small loss

Market: "Bitcoin Up or Down - {date}, {time range} ET"
  - Resolves via Chainlink BTC/USD data stream (automatic, ~20s after window close)
  - "Up" wins if end price >= start price, "Down" otherwise
  - New window every 5 minutes (Unix timestamps divisible by 300)

Fees: 7.2% taker on crypto markets. We use MAKER orders (0% fee + 20% rebate).
"""
import json
import time
import requests
from pathlib import Path
from loguru import logger

GAMMA_EVENTS = "https://gamma-api.polymarket.com/events"


class BTCStraddle:
    """
    BTC 5-minute Up/Down volatility straddle.

    Usage:
        straddle = BTCStraddle(data_dir="data")
        markets = straddle.scan()  # Returns list of tradeable windows
    """

    # Tuning
    TARGET_BUY_PRICE = 0.20        # Limit buy price for each leg
    SHARES_PER_LEG = 5             # Minimum CLOB order size
    MAX_ACTIVE_STRADDLES = 2       # Max windows traded simultaneously
    MIN_REMAINING_SECONDS = 90     # Don't enter windows with < 90s left
    WINDOW_SECONDS = 300           # 5-minute windows
    MARKET_CACHE_TTL = 60          # Cache market data 60s (same window)
    SEEN_FILE = "btc_straddle_seen.json"

    def __init__(self, data_dir: str):
        self._data_dir = Path(data_dir)
        self._seen: set[int] = set()
        self._seen_path = self._data_dir / self.SEEN_FILE
        self._load_seen()

        # Market data cache
        self._cache: dict = {}         # window_ts -> market_data
        self._cache_ts: float = 0

    def _load_seen(self):
        try:
            if self._seen_path.exists():
                self._seen = set(json.loads(self._seen_path.read_text()))
        except Exception:
            self._seen = set()

    def _save_seen(self):
        self._data_dir.mkdir(parents=True, exist_ok=True)
        # Trim if too large
        seen_list = sorted(self._seen)
        if len(seen_list) > 500:
            seen_list = seen_list[-250:]
            self._seen = set(seen_list)
        self._seen_path.write_text(json.dumps(seen_list))

    def mark_seen(self, window_ts: int):
        self._seen.add(window_ts)
        self._save_seen()

    @staticmethod
    def get_current_window() -> int:
        """Return current 5-min window start timestamp."""
        now = int(time.time())
        return now - (now % 300)

    @staticmethod
    def window_remaining(window_ts: int) -> int:
        """Seconds remaining in this window."""
        window_end = window_ts + 300
        return max(0, window_end - int(time.time()))

    def fetch_market(self, window_ts: int) -> dict | None:
        """
        Fetch market data for a specific 5-min window from Gamma API.

        Returns dict with: up_token_id, down_token_id, condition_id, slug, question
        or None if not found / error.
        """
        # Check cache
        if window_ts in self._cache and (time.time() - self._cache_ts) < self.MARKET_CACHE_TTL:
            return self._cache[window_ts]

        slug = f"btc-updown-5m-{window_ts}"
        try:
            resp = requests.get(GAMMA_EVENTS, params={"slug": slug}, timeout=10)
            if resp.status_code != 200:
                logger.debug(f"Straddle: Gamma API {resp.status_code} for {slug}")
                return None

            events = resp.json()
            if not events:
                logger.debug(f"Straddle: no event found for {slug}")
                return None

            event = events[0] if isinstance(events, list) else events
            markets = event.get("markets", [])
            if not markets:
                logger.debug(f"Straddle: no markets in event {slug}")
                return None

            market = markets[0]
            outcomes = market.get("outcomes", [])
            token_ids = market.get("clobTokenIds", [])
            # Gamma API returns these as JSON strings, not arrays
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(token_ids, str):
                token_ids = json.loads(token_ids)

            if len(outcomes) < 2 or len(token_ids) < 2:
                logger.debug(f"Straddle: incomplete market data for {slug}")
                return None

            # outcomes[0] = "Up", outcomes[1] = "Down"
            # token_ids match outcomes order
            up_idx = 0
            down_idx = 1
            for i, o in enumerate(outcomes):
                if o.lower() == "up":
                    up_idx = i
                elif o.lower() == "down":
                    down_idx = i

            result = {
                "window_ts": window_ts,
                "slug": slug,
                "question": market.get("question", event.get("title", slug)),
                "condition_id": market.get("conditionId", ""),
                "up_token_id": token_ids[up_idx],
                "down_token_id": token_ids[down_idx],
                "accepting_orders": market.get("acceptingOrders", True),
            }

            self._cache[window_ts] = result
            self._cache_ts = time.time()
            return result

        except Exception as e:
            logger.debug(f"Straddle: fetch failed for {slug}: {e}")
            return None

    def scan(self) -> list[dict]:
        """
        Find tradeable 5-minute windows.

        Returns list of market dicts (usually 0 or 1 — the current active window).
        """
        window_ts = self.get_current_window()
        remaining = self.window_remaining(window_ts)

        # Skip if already traded this window
        if window_ts in self._seen:
            return []

        # Skip if too little time remaining
        if remaining < self.MIN_REMAINING_SECONDS:
            return []

        market = self.fetch_market(window_ts)
        if market is None:
            return []

        if not market.get("accepting_orders", True):
            logger.debug(f"Straddle: {market['slug']} not accepting orders")
            return []

        logger.info(
            f"  Straddle: found window {market['slug']} "
            f"({remaining}s remaining)"
        )
        return [market]
