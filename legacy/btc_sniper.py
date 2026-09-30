"""
BTC Price Sniper — Trade Polymarket BTC threshold markets using real-time Binance data.

Strategy:
  1. Binance WebSocket for sub-second BTC price (free, no key, read-only)
  2. Find open BTC threshold markets on Polymarket (Gamma API, cached 5 min)
  3. Detect when BTC crosses a market threshold → buy the winning side before market catches up
  4. Hold to resolution

Market types handled:
  - "Will Bitcoin dip to $X on Date?"      → YES when Binance 1-min candle LOW touches X
  - "Will Bitcoin reach $X on Date?"       → YES when Binance 1-min candle HIGH touches X
  - "Will the price of Bitcoin be above $X on Date?" → YES if CLOSE at 12:00 PM ET noon > X
  - "Will Bitcoin dip to $X in Month?"     → YES when candle LOW touches X (monthly window)
  - "Will Bitcoin reach $X in Month?"      → YES when candle HIGH touches X (monthly window)

Data source: Binance WebSocket (primary) + REST ticker (fallback)
"""
import re
import json
import time
import threading
import requests
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
from loguru import logger


BINANCE_TICKER = "https://api.binance.com/api/v3/ticker/price"
BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdt@trade"
GAMMA_MARKETS = "https://gamma-api.polymarket.com/markets"


@dataclass
class BTCSignal:
    """A BTC sniper trade signal."""
    token_id: str
    condition_id: str
    question: str
    market_type: str        # "dip_to", "reach", "above"
    threshold: float        # Dollar threshold (e.g., 66000)
    side: str               # "BUY_YES" or "BUY_NO"
    confidence: float       # 0.0-1.0 how sure we are
    btc_price: float        # BTC price when signal generated


class BTCSniper:
    """
    Monitors BTC price and finds mispriced Polymarket threshold markets.

    Usage:
        sniper = BTCSniper(data_dir="data")
        signals = sniper.scan()  # Returns list of BTCSignal
    """

    # Tuning
    MAX_BUY_PRICE = 0.50        # Don't buy YES above this (need margin after 1.8% taker fee)
    MIN_CONFIDENCE = 0.95       # Only trade confirmed events (no speculative "close to" bets)
    MAX_POSITIONS = 5           # Max simultaneous BTC positions
    MARKET_CACHE_TTL = 300      # Refresh Gamma market list every 5 min
    SEEN_FILE = "btc_sniper_seen.json"  # Dedup across restarts

    # Thresholds for "above $X" markets — how far BTC must be from threshold
    # "Above" resolves at 12:00 PM ET noon (not end of day!) — need large margin
    ABOVE_SAFE_MARGIN_PCT = 0.08  # BTC must be >8% above threshold to buy YES

    def __init__(self, data_dir: str):
        self._data_dir = Path(data_dir)
        self._market_cache: list[dict] = []
        self._market_cache_ts: float = 0
        self._seen: set[str] = set()
        self._seen_path = self._data_dir / self.SEEN_FILE
        self._load_seen()

        # WebSocket state
        self._ws_price: Optional[float] = None
        self._ws_price_ts: float = 0
        self._ws_thread: Optional[threading.Thread] = None
        self._ws_running = False
        self._start_ws()

    def _start_ws(self):
        """Start Binance WebSocket in a background daemon thread."""
        if self._ws_running:
            return
        self._ws_running = True
        self._ws_thread = threading.Thread(target=self._ws_loop, daemon=True)
        self._ws_thread.start()

    def _ws_loop(self):
        """WebSocket reconnect loop. Runs in background thread forever."""
        import websocket

        while self._ws_running:
            try:
                ws = websocket.WebSocketApp(
                    BINANCE_WS,
                    on_message=self._on_ws_message,
                    on_error=self._on_ws_error,
                    on_close=self._on_ws_close,
                    on_open=self._on_ws_open,
                )
                ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.debug(f"BTC WebSocket error: {e}")
            # Reconnect after 5s on any disconnect
            if self._ws_running:
                time.sleep(5)

    def _on_ws_open(self, ws):
        logger.info("  BTC WebSocket: connected to Binance btcusdt@trade")

    def _on_ws_message(self, ws, message):
        """Handle incoming trade message — update price."""
        try:
            data = json.loads(message)
            self._ws_price = float(data["p"])  # "p" = price field in trade stream
            self._ws_price_ts = time.time()
        except Exception:
            pass

    def _on_ws_error(self, ws, error):
        logger.debug(f"BTC WebSocket error: {error}")

    def _on_ws_close(self, ws, close_status, close_msg):
        logger.debug(f"BTC WebSocket closed: {close_status} {close_msg}")

    def _load_seen(self):
        """Load dedup set from disk."""
        try:
            if self._seen_path.exists():
                self._seen = set(json.loads(self._seen_path.read_text()))
        except Exception:
            self._seen = set()

    def _save_seen(self):
        """Persist dedup set."""
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._seen_path.write_text(json.dumps(list(self._seen)))

    def get_btc_price(self) -> Optional[float]:
        """
        Get current BTC/USDT price.
        Primary: WebSocket (sub-second updates).
        Fallback: REST API (if WebSocket is stale >30s).
        """
        # Use WebSocket price if fresh (within 30 seconds)
        if self._ws_price is not None and (time.time() - self._ws_price_ts) < 30:
            return self._ws_price

        # Fallback to REST
        try:
            resp = requests.get(BINANCE_TICKER, params={"symbol": "BTCUSDT"}, timeout=5)
            if resp.status_code == 200:
                price = float(resp.json()["price"])
                logger.debug(f"BTC price via REST fallback: ${price:,.2f}")
                return price
        except Exception as e:
            logger.debug(f"BTC price fetch failed: {e}")
        return self._ws_price  # Return last known WS price even if stale

    def _fetch_btc_markets(self) -> list[dict]:
        """Fetch open BTC threshold markets from Gamma API. Cached 5 min."""
        now = time.time()
        if now - self._market_cache_ts < self.MARKET_CACHE_TTL and self._market_cache:
            return self._market_cache

        try:
            markets = []
            # Gamma doesn't support text search — fetch top markets by volume and filter client-side.
            # limit=500 works, so 2 calls covers 1000 markets (all BTC threshold markets are in top ~600).
            for offset in [0, 500]:
                resp = requests.get(GAMMA_MARKETS, params={
                    "closed": "false", "limit": 500, "offset": offset,
                    "order": "volume24hr", "ascending": "false",
                }, timeout=15)
                if resp.status_code != 200:
                    continue
                batch = resp.json()
                if not batch:
                    break
                markets.extend(batch)

            # Filter to BTC threshold markets only
            btc_markets = []
            for m in markets:
                q = (m.get("question") or "").lower()
                if "bitcoin" not in q and "btc" not in q:
                    continue
                # Skip hourly up/down (coin flip, no edge)
                if "up or down" in q:
                    continue
                # Must have a price threshold
                if not re.search(r'\$[\d,]+', m.get("question", "")):
                    continue
                btc_markets.append(m)

            self._market_cache = btc_markets
            self._market_cache_ts = now
            logger.debug(f"BTC sniper: cached {len(btc_markets)} threshold markets")
            return btc_markets

        except Exception as e:
            logger.debug(f"BTC market fetch failed: {e}")
            return self._market_cache

    @staticmethod
    def _parse_market(question: str) -> Optional[tuple[str, float]]:
        """
        Parse a BTC market question into (market_type, threshold).

        Returns:
            ("dip_to", 66000.0)  — "Will Bitcoin dip to $66,000 on April 5?"
            ("reach", 68000.0)   — "Will Bitcoin reach $68,000 on April 5?"
            ("above", 76000.0)   — "Will the price of Bitcoin be above $76,000 on April 6?"
            None if unparseable
        """
        q = question.lower()

        # Extract dollar amount (handle $1m, $1M, $500K etc. — suffix must be attached)
        match = re.search(r'\$([\d,]+(?:\.\d+)?)([mMkK])?(?:\s|$|\?)', question)
        if not match:
            return None
        threshold = float(match.group(1).replace(",", ""))
        suffix = (match.group(2) or "").lower()
        if suffix == "m":
            threshold *= 1_000_000
        elif suffix == "k":
            threshold *= 1_000
        # Sanity: BTC thresholds should be > $1000
        if threshold < 1000:
            return None

        if "dip to" in q or "dip below" in q or "fall to" in q or "drop to" in q:
            return ("dip_to", threshold)
        elif "reach" in q or "hit" in q:
            return ("reach", threshold)
        elif "above" in q:
            return ("above", threshold)
        elif "below" in q or "less than" in q:
            return ("below", threshold)

        return None

    def scan(self) -> list[BTCSignal]:
        """
        Scan for BTC trading opportunities.

        Returns list of BTCSignal for markets where BTC price has crossed
        a threshold but the market hasn't fully priced it in yet.
        """
        btc_price = self.get_btc_price()
        if btc_price is None:
            return []

        markets = self._fetch_btc_markets()
        if not markets:
            return []

        signals = []

        for m in markets:
            question = m.get("question", "")
            parsed = self._parse_market(question)
            if not parsed:
                continue

            market_type, threshold = parsed

            # Get token IDs — YES token is first, NO token is second
            token_ids = m.get("clobTokenIds", "")
            if isinstance(token_ids, str):
                try:
                    token_ids = json.loads(token_ids) if token_ids.startswith("[") else []
                except Exception:
                    token_ids = []
            if not token_ids or len(token_ids) < 1:
                continue

            yes_token = token_ids[0]
            condition_id = m.get("conditionId", "")

            # Current market price
            try:
                best_ask = float(m.get("bestAsk") or 0)
                best_bid = float(m.get("bestBid") or 0)
            except (TypeError, ValueError):
                continue

            if best_ask <= 0:
                continue

            # Dedup key: market question (not token, since we want one trade per market)
            dedup_key = f"btc:{condition_id}"
            if dedup_key in self._seen:
                continue

            signal = None

            if market_type == "dip_to":
                # BTC needs to touch threshold going DOWN.
                # Resolution uses Binance 1-min candle LOW — spot price is conservative.
                # ONLY trade confirmed crosses (spot already at/below threshold).
                if btc_price <= threshold and best_ask < self.MAX_BUY_PRICE:
                    signal = BTCSignal(
                        token_id=yes_token, condition_id=condition_id,
                        question=question, market_type=market_type,
                        threshold=threshold, side="BUY_YES",
                        confidence=0.99, btc_price=btc_price,
                    )

            elif market_type == "reach":
                # BTC needs to touch threshold going UP.
                # Resolution uses Binance 1-min candle HIGH.
                if btc_price >= threshold and best_ask < self.MAX_BUY_PRICE:
                    signal = BTCSignal(
                        token_id=yes_token, condition_id=condition_id,
                        question=question, market_type=market_type,
                        threshold=threshold, side="BUY_YES",
                        confidence=0.99, btc_price=btc_price,
                    )

            elif market_type == "above":
                # "Above $X on Date" resolves at 12:00 PM ET noon (NOT end of day).
                # Uses Binance 1-min candle CLOSE at exactly noon.
                # Much riskier — BTC can crash between now and noon.
                # Only trade with large margin (>8%) and cheap price.
                margin = (btc_price - threshold) / threshold if threshold > 0 else 0
                if margin > self.ABOVE_SAFE_MARGIN_PCT and best_ask < 0.30:
                    confidence = min(0.95, 0.85 + margin)
                    signal = BTCSignal(
                        token_id=yes_token, condition_id=condition_id,
                        question=question, market_type=market_type,
                        threshold=threshold, side="BUY_YES",
                        confidence=confidence, btc_price=btc_price,
                    )

            elif market_type == "below":
                # Same as "above" but inverted.
                margin = (threshold - btc_price) / threshold if threshold > 0 else 0
                if margin > self.ABOVE_SAFE_MARGIN_PCT and best_ask < 0.30:
                    confidence = min(0.95, 0.85 + margin)
                    signal = BTCSignal(
                        token_id=yes_token, condition_id=condition_id,
                        question=question, market_type=market_type,
                        threshold=threshold, side="BUY_YES",
                        confidence=confidence, btc_price=btc_price,
                    )

            if signal and signal.confidence >= self.MIN_CONFIDENCE:
                signals.append(signal)

        logger.info(
            f"  BTC sniper: ${btc_price:,.0f} | {len(markets)} markets scanned | "
            f"{len(signals)} signals"
        )
        return signals

    def mark_seen(self, condition_id: str):
        """Mark a market as traded (dedup)."""
        self._seen.add(f"btc:{condition_id}")
        self._save_seen()
