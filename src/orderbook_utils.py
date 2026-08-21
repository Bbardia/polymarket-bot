"""Orderbook quality checks — spread, slippage, and liquidity verification."""
from loguru import logger


# Thresholds — scale with price
# Weather markets are illiquid; we place MAKER orders so spread doesn't affect
# our execution price. Wide limits just filter truly dead markets.
# At $0.50: max 20% spread. At extremes ($0.05): max ~80%.
# Lottery tickets (< $0.05): spreads of 100%+ are normal ($0.01 bid-ask gap).
MAX_SPREAD_PCT_BASE = 20.0
MAX_SLIPPAGE_PCT = 5.0


def _max_spread_for_price(price: float) -> float:
    """Dynamic max spread: wider tolerance at extreme prices (near $0 or $1).

    Since we place maker orders, the spread doesn't affect our fill price —
    it only indicates market activity. We use generous limits to avoid
    blocking trades in illiquid weather markets.
    """
    # Lottery tickets: essentially no spread limit (just need a live book)
    if price < 0.05:
        return 500.0
    # At 50/50 ($0.50): 20% max
    # At extremes ($0.10 or $0.90): ~80% max
    distance_from_center = abs(price - 0.50) * 2  # 0.0 at center, 1.0 at extremes
    return MAX_SPREAD_PCT_BASE + distance_from_center * 60.0  # 20% → 80%


def check_orderbook_quality(clob_client, token_id: str, side: str,
                            price: float, size: float,
                            book=None, maker: bool = True) -> dict:
    """
    Analyze orderbook quality before placing an order.

    Args:
        clob_client: Polymarket CLOB client instance
        token_id: Market token ID
        side: "BUY" or "SELL"
        price: Limit price we intend to place
        size: Number of shares we want
        book: Pre-fetched orderbook (optional — avoids redundant REST call)
        maker: If True (default), we're placing a maker order — only check
               that the market is alive (has bids+asks), skip liquidity/slippage
               checks since we're adding liquidity, not consuming it.

    Returns dict with:
        spread_pct: bid-ask spread as percentage
        effective_fill_price: weighted-average fill price walking the book
        slippage_pct: difference between limit price and effective fill price
        can_fill: bool — enough liquidity at acceptable price
        reason: explanation string if can't fill
        book: the orderbook object (for reuse by caller)
    """
    result = {
        "spread_pct": None,
        "effective_fill_price": None,
        "slippage_pct": None,
        "can_fill": False,
        "reason": "unknown",
        "book": None,
    }

    if book is None:
        try:
            book = clob_client.get_order_book(token_id)
        except Exception as e:
            result["reason"] = f"Failed to fetch orderbook: {e}"
            return result
    result["book"] = book

    # Parse bids and asks — handle both object attributes and dict access
    bids = _parse_levels(getattr(book, "bids", None) or
                         (book.get("bids") if isinstance(book, dict) else []))
    asks = _parse_levels(getattr(book, "asks", None) or
                         (book.get("asks") if isinstance(book, dict) else []))

    # Sort: bids descending, asks ascending
    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])

    if not bids and not asks:
        result["reason"] = "Orderbook empty (no bids and no asks)"
        return result

    best_bid = bids[0][0] if bids else 0
    best_ask = asks[0][0] if asks else 1

    # Spread calculation
    midpoint = (best_bid + best_ask) / 2.0
    spread = best_ask - best_bid
    spread_pct = (spread / midpoint) * 100.0 if midpoint > 0 else 999.0
    result["spread_pct"] = round(spread_pct, 2)

    max_spread = _max_spread_for_price(price)
    if spread_pct > max_spread:
        result["reason"] = (f"Spread too wide: {spread_pct:.1f}% > {max_spread:.0f}% max "
                            f"(bid={best_bid:.3f}, ask={best_ask:.3f})")
        return result

    # For maker orders: spread check is enough — we're adding liquidity,
    # not consuming it. No need to walk the book or check fill capacity.
    if maker:
        result["can_fill"] = True
        result["effective_fill_price"] = price
        result["slippage_pct"] = 0.0
        result["reason"] = "OK (maker)"
        return result

    # --- Taker-only checks below ---

    # Walk the book to compute effective fill price
    if side == "BUY":
        levels = [(p, s) for p, s in asks if p <= price]
        if not levels:
            result["reason"] = (f"No asks at or below limit {price:.3f} "
                                f"(best ask={best_ask:.3f})")
            return result
        effective, filled = _walk_book(levels, size)
    else:
        levels = [(p, s) for p, s in bids if p >= price]
        if not levels:
            result["reason"] = (f"No bids at or above limit {price:.3f} "
                                f"(best bid={best_bid:.3f})")
            return result
        effective, filled = _walk_book(levels, size)

    result["effective_fill_price"] = round(effective, 4) if effective else None

    if filled < size:
        result["reason"] = (f"Insufficient liquidity: can fill {filled:.1f} "
                            f"of {size:.1f} shares")
        return result

    if side == "BUY":
        slippage_pct = ((effective - price) / price) * 100.0 if price > 0 else 0
    else:
        slippage_pct = ((price - effective) / price) * 100.0 if price > 0 else 0

    result["slippage_pct"] = round(slippage_pct, 2)

    if slippage_pct > MAX_SLIPPAGE_PCT:
        result["reason"] = (f"Slippage too high: {slippage_pct:.1f}% "
                            f"(limit={price:.3f}, effective={effective:.3f})")
        return result

    result["can_fill"] = True
    result["reason"] = "OK"
    return result


def _parse_levels(raw_levels) -> list[tuple[float, float]]:
    """Parse orderbook levels into (price, size) tuples.

    Handles both object attributes (.price/.size) and dict access.
    """
    if not raw_levels:
        return []
    parsed = []
    for level in raw_levels:
        try:
            if isinstance(level, dict):
                p = float(level.get("price", 0))
                s = float(level.get("size", 0))
            else:
                p = float(getattr(level, "price", 0))
                s = float(getattr(level, "size", 0))
            if p > 0 and s > 0:
                parsed.append((p, s))
        except (ValueError, TypeError):
            continue
    return parsed


def _walk_book(levels: list[tuple[float, float]],
               target_size: float) -> tuple[float | None, float]:
    """Walk orderbook levels to compute volume-weighted average fill price.

    Returns (effective_fill_price, total_filled_size).
    """
    total_cost = 0.0
    total_filled = 0.0

    for price, available in levels:
        take = min(available, target_size - total_filled)
        total_cost += take * price
        total_filled += take
        if total_filled >= target_size:
            break

    if total_filled <= 0:
        return None, 0.0
    return total_cost / total_filled, total_filled
