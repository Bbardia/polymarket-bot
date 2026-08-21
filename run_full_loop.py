#!/usr/bin/env python3
"""
Polymarket weather and short-horizon market research bot.

Dry-run mode is the default. Live trading requires the `--live` flag plus the
local `.env` safety switches validated by `Config.assert_live_trading_allowed()`.
"""
import os
import sys
import json
import time
import uuid
import argparse
from datetime import datetime, date, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from loguru import logger
from src.config import Config
from src.kelly import KellySizer, DynamicKellySizer
from src.portfolio import PortfolioTracker, Position, ClosedTrade
from src.whale_tracker import WhaleTracker
from src.btc_sniper import BTCSniper
from src.btc_straddle import BTCStraddle
from src.weather_forecast import WeatherForecast
from src.forecast_scanner import ForecastScanner
from src.edge_math import dynamic_min_edge, executable_buy_price

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
logger.add(LOG_DIR / "loop_v2_{time}.log", rotation="1 day", retention="7 days", level="DEBUG")

TRADES_FILE = Path(__file__).parent / "data" / "loop_v2_trades.jsonl"


# ═══════════════════════════════════════════════════════════════
#  Trade Execution
# ═══════════════════════════════════════════════════════════════

def _execute_trade(pm, token_id, price, cost, signal, portfolio=None) -> dict:
    """Execute a trade via CLOB API with orderbook quality checks."""
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY
    from src.orderbook_utils import check_orderbook_quality

    # Execution-time BUY_NO cap re-check (signal price may have drifted)
    if hasattr(signal, 'side') and signal.side == "BUY_NO" and 0.25 < price < 0.80:
        logger.warning(
            f"  ⛔ BUY_NO cap re-check: ${price:.2f} in dead zone ($0.25-$0.80) — skipping"
        )
        return {}

    shares = max(5.0, cost / price) if price > 0 else 5.0

    # --- Orderbook quality gate ---
    ob_check = check_orderbook_quality(
        pm.clob_client, token_id, side="BUY", price=price, size=shares
    )
    if not ob_check["can_fill"]:
        logger.warning(
            f"  Orderbook check FAILED — skipping trade: {ob_check['reason']} "
            f"(spread={ob_check['spread_pct']}%)"
        )
        return {}

    logger.info(
        f"  Orderbook OK: spread={ob_check['spread_pct']:.1f}%, "
        f"eff_price={ob_check['effective_fill_price']:.3f}, "
        f"slippage={ob_check['slippage_pct']:.1f}%"
    )

    # ── Maker-friendly pricing: place $0.01 below best ask to avoid taker fees ──
    # Maker = 0% fee + 20% rebate. Taker = up to 12.5% fee.
    maker_price = price
    best_ask = price
    try:
        book = pm.clob_client.get_order_book(token_id)
        asks = sorted(book.asks, key=lambda a: float(a.price)) if book.asks else []
        bids = sorted(book.bids, key=lambda b: float(b.price), reverse=True) if book.bids else []
        best_ask = float(asks[0].price) if asks else price
        best_bid = float(bids[0].price) if bids else price - 0.02

        # Place at best_ask - $0.01 (just under ask = maker, much better fill rate)
        ask_minus = round(best_ask - 0.01, 2)
        bid_plus = round(best_bid + 0.01, 2)
        if ask_minus <= round(price, 2):
            maker_price = ask_minus
        else:
            maker_price = min(bid_plus, round(price, 2))
        maker_price = max(maker_price, 0.01)

        fee_saved = 0.25 * price * (1 - price) * 2 * shares
        if maker_price < best_ask:
            logger.info(f"  Maker pricing: ${price:.2f} → ${maker_price:.2f} "
                       f"(below ask ${best_ask:.2f}, saves ~${fee_saved:.3f} in fees)")
    except Exception:
        maker_price = round(price, 2)

    # Pre-flight: CLOB minimum is 5 shares. $1 min only for taker orders.
    rounded_shares = round(shares, 1)
    if rounded_shares < 5.0:
        logger.debug(f"  ⏭️ Order too few shares ({rounded_shares:.1f} < 5 min) — skipping")
        return {}
    order_value = maker_price * rounded_shares
    if maker_price >= best_ask and order_value < 1.0:
        logger.debug(f"  ⏭️ Marketable order too small (${order_value:.2f} < $1 min) — skipping")
        return {}

    order_args = OrderArgs(
        token_id=token_id,
        price=maker_price,
        size=round(shares, 1),
        side=BUY,
    )
    try:
        signed = pm.clob_client.create_order(order_args)
        result = pm.clob_client.post_order(signed, OrderType.GTC)
        success = result.get("success", False) if isinstance(result, dict) else False
        status = result.get("status", "") if isinstance(result, dict) else ""
        if success or status in ("live", "matched", "delayed"):
            logger.success(f"  ✅ Trade executed: {status}")
            if portfolio:
                portfolio.record_order_success()
            return result
        else:
            logger.error(f"  ❌ Trade failed: {result}")
            if portfolio:
                portfolio.record_order_failure()
            return {}
    except Exception as e:
        logger.error(f"  ❌ Order error: {e}")
        if portfolio:
            portfolio.record_order_failure()
        return {}


# ═══════════════════════════════════════════════════════════════
#  Whale Copy-Trade Strategy (ONLY active strategy)
# ═══════════════════════════════════════════════════════════════

def run_whale_weather_copy_strategy(
    portfolio: PortfolioTracker,
    whale_tracker: "WhaleTracker",
    pm=None,
    dry_run: bool = True,
    forecast: "WeatherForecast" = None,
) -> list[dict]:
    """
    Whale copy-trade strategy — follows selected public weather wallets.

    Flow:
    1. Poll configured public wallets for new weather BUY trades (deduped, persisted)
    2. Filter by price, weather market type, and existing exposure
    3. Place maker order at best_ask - $0.01 when live trading is enabled
    4. Hold according to the configured exit rules
    """
    # Fetch new signals (90s cache inside — safe to call every cycle)
    new_signals = whale_tracker.fetch_copy_weather_signals(hours_back=6)
    if not new_signals:
        return []

    # Check available capital
    usdc_balance = 0.0
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        bal_params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL, signature_type=Config.SIGNATURE_TYPE,
        )
        clob = pm.clob_client if pm and pm.clob_client else None
        bal_resp = clob.get_balance_allowance(bal_params) if clob else {}
        usdc_balance = int(bal_resp.get("balance", "0")) / 1e6
    except Exception:
        usdc_balance = max(0, portfolio.initial_bankroll - portfolio.total_deployed)
    remaining = usdc_balance

    # Count current open copy-weather positions
    open_copy = sum(
        1 for p in portfolio.positions.values()
        if p.market_type == "WHALE_COPY" and p.status == "OPEN"
    )

    existing_tokens = {p.token_id for p in portfolio.positions.values() if p.status == "OPEN"}
    trades = []

    for sig in new_signals:
        if open_copy >= WhaleTracker.COPY_WEATHER_MAX_POSITIONS:
            logger.info(f"  Whale-copy: max {WhaleTracker.COPY_WEATHER_MAX_POSITIONS} positions — stopping")
            break

        if remaining < 0.05:
            logger.info(f"  Whale-copy: capital depleted (${remaining:.2f}) — stopping")
            break

        # Skip if we already hold this token
        if sig.token_id in existing_tokens:
            logger.debug(f"  Whale-copy: already holding {sig.question[:40]}")
            continue

        # Skip low-conviction whale trades (tiny bets < $0.10 USDC)
        if sig.usdc_size < 0.10:
            logger.debug(f"  Whale-copy: {sig.wallet} only spent ${sig.usdc_size:.2f} — low conviction, skipping")
            continue

        # Market structure filter: prefer exact temperature markets; range-style
        # contracts use different calibration and are skipped by this strategy.
        q_lower = sig.question.lower()
        if any(phrase in q_lower for phrase in ["or higher", "or below", "or above", "between"]):
            logger.debug(f"  Whale-copy: non-exact market structure — skipping: {sig.question[:50]}")
            continue

        # Get current market price
        current_price = None
        best_ask = None
        try:
            book = pm.clob_client.get_order_book(sig.token_id)
            asks = sorted(book.asks, key=lambda a: float(a.price)) if book.asks else []
            bids = sorted(book.bids, key=lambda b: float(b.price), reverse=True) if book.bids else []
            best_ask = float(asks[0].price) if asks else None
            best_bid = float(bids[0].price) if bids else None
            if best_bid is not None and best_ask is not None:
                current_price = executable_buy_price(best_bid=best_bid, best_ask=best_ask)
            elif best_ask is not None:
                current_price = executable_buy_price(best_bid=None, best_ask=best_ask)
            elif best_bid is not None:
                # No asks = whale consumed all sell orders — strong signal
                # Place at bid + $0.01 as maker
                current_price = round(best_bid + 0.01, 2)
        except Exception as e:
            logger.debug(f"  Whale-copy: orderbook fetch failed for {sig.question[:30]}: {e}")

        if current_price is None or current_price < 0.01:
            logger.debug(f"  Whale-copy: no valid price for {sig.question[:40]} — skipping")
            continue

        # Price filter: only copy low-priced contracts where payoff asymmetry can
        # justify the variance; avoid ultra-low/no-liquidity dust prices.
        if current_price < 0.03:
            logger.debug(f"  Whale-copy: price ${current_price:.2f} < $0.03 min — dead zone, skipping")
            continue
        if current_price > WhaleTracker.COPY_WEATHER_MAX_PRICE:
            logger.debug(f"  Whale-copy: price ${current_price:.2f} > ${WhaleTracker.COPY_WEATHER_MAX_PRICE} — skipping")
            continue

        # Forecast filter: skip if Open-Meteo ensemble says <10% probability
        if forecast:
            try:
                result = forecast.check_signal(sig.question)
                if result is not None:
                    prob, _meta = result
                    if prob < forecast.MIN_PROBABILITY:
                        logger.info(
                            f"  Whale-copy: forecast says {prob:.0%} for {sig.question[:40]} — skipping"
                        )
                        continue
            except Exception as e:
                logger.debug(f"  Whale-copy: forecast check failed: {e}")

        # Size: buy 5 shares at maker price
        shares = 5.0
        cost = round(shares * current_price, 2)
        if cost > remaining:
            shares = max(5.0, remaining / current_price) if current_price > 0 else 5.0
            cost = round(shares * current_price, 2)
            if shares < 5.0:
                logger.debug(f"  Whale-copy: can't afford 5 shares at ${current_price:.2f} — skipping")
                continue

        logger.info(
            f"  🐋 WHALE_COPY: {sig.wallet} bought ${sig.usdc_size:.2f} → "
            f"we copy {shares:.0f} shares @ ${current_price:.2f} = ${cost:.2f} | "
            f"{sig.question[:50]}"
        )

        if dry_run:
            trades.append({
                "type": "WHALE_COPY", "question": sig.question[:60],
                "source": sig.wallet, "cost": cost, "status": "DRY_RUN",
            })
            continue

        if not pm or not pm.clob_client:
            logger.warning("  Whale-copy: no trading client — skipping execution")
            continue

        # Execute via _execute_trade (handles maker pricing, orderbook checks)
        result = _execute_trade(pm, sig.token_id, current_price, cost, sig, portfolio=portfolio)
        if result and "error" not in result:
            pos_id = f"WC-{uuid.uuid4().hex[:8]}"
            portfolio.open_position(Position(
                id=pos_id,
                market_type="WHALE_COPY",
                description=f"COPY {sig.wallet}: {sig.question[:50]}",
                side="BUY_YES",
                token_id=sig.token_id,
                entry_price=current_price,
                shares=shares,
                cost=cost,
                entry_time=datetime.now(timezone.utc).isoformat(),
                edge_at_entry=0.0,
                kelly_fraction=0.0,
                current_price=current_price,
                peak_price=current_price,
                order_id=result.get("orderID", ""),
                condition_id=sig.condition_id,
            ))
            existing_tokens.add(sig.token_id)
            open_copy += 1
            remaining -= cost
            trades.append({
                "type": "WHALE_COPY", "question": sig.question[:60],
                "source": sig.wallet, "source_usdc": sig.usdc_size,
                "cost": cost, "price": current_price, "status": "LIVE",
            })
        else:
            trades.append({
                "type": "WHALE_COPY", "question": sig.question[:60],
                "source": sig.wallet, "cost": cost, "status": "FAILED",
            })

    return trades


# ═══════════════════════════════════════════════════════════════
#  BTC Price Sniper Strategy
# ═══════════════════════════════════════════════════════════════

def run_btc_sniper_strategy(
    portfolio: PortfolioTracker,
    btc_sniper: "BTCSniper",
    pm=None,
    dry_run: bool = True,
) -> list[dict]:
    """
    BTC price sniper — buys Polymarket BTC threshold markets when
    Binance price crosses a threshold before the market catches up.

    Uses free Binance API (no rate limits) for real-time BTC price.
    """
    signals = btc_sniper.scan()
    if not signals:
        return []

    # Check available capital
    usdc_balance = 0.0
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        bal_params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL, signature_type=Config.SIGNATURE_TYPE,
        )
        clob = pm.clob_client if pm and pm.clob_client else None
        bal_resp = clob.get_balance_allowance(bal_params) if clob else {}
        usdc_balance = int(bal_resp.get("balance", "0")) / 1e6
    except Exception:
        usdc_balance = max(0, portfolio.initial_bankroll - portfolio.total_deployed)
    remaining = usdc_balance

    # Count current open BTC sniper positions
    open_btc = sum(
        1 for p in portfolio.positions.values()
        if p.market_type == "BTC_SNIPE" and p.status == "OPEN"
    )

    existing_tokens = {p.token_id for p in portfolio.positions.values() if p.status == "OPEN"}
    trades = []

    for sig in signals:
        if open_btc >= BTCSniper.MAX_POSITIONS:
            logger.info(f"  BTC sniper: max {BTCSniper.MAX_POSITIONS} positions — stopping")
            break

        if remaining < 0.05:
            logger.info(f"  BTC sniper: capital depleted (${remaining:.2f}) — stopping")
            break

        if sig.token_id in existing_tokens:
            logger.debug(f"  BTC sniper: already holding {sig.question[:40]}")
            continue

        # Get current market price from orderbook
        current_price = None
        best_ask = None
        try:
            book = pm.clob_client.get_order_book(sig.token_id)
            asks = sorted(book.asks, key=lambda a: float(a.price)) if book.asks else []
            bids = sorted(book.bids, key=lambda b: float(b.price), reverse=True) if book.bids else []
            best_ask = float(asks[0].price) if asks else None
            best_bid = float(bids[0].price) if bids else None
            if best_bid is not None and best_ask is not None:
                current_price = executable_buy_price(best_bid=best_bid, best_ask=best_ask)
            elif best_ask is not None:
                current_price = executable_buy_price(best_bid=None, best_ask=best_ask)
        except Exception as e:
            logger.debug(f"  BTC sniper: orderbook fetch failed for {sig.question[:30]}: {e}")

        if current_price is None:
            logger.debug(f"  BTC sniper: no price for {sig.question[:40]} — skipping")
            continue

        # Price filter: don't buy if too expensive (need margin after fees)
        if current_price > BTCSniper.MAX_BUY_PRICE:
            logger.debug(f"  BTC sniper: price ${current_price:.2f} > max ${BTCSniper.MAX_BUY_PRICE} — skipping")
            continue

        # Size: 5 shares at market price, cap at $2 per trade
        shares = 5.0
        cost = round(shares * current_price, 2)
        max_cost = 2.0
        if cost > max_cost and current_price > 0:
            shares = max(5.0, max_cost / current_price)
            cost = round(shares * current_price, 2)
        if cost > remaining:
            logger.debug(f"  BTC sniper: can't afford ${cost:.2f} (${remaining:.2f} left) — skipping")
            continue

        logger.info(
            f"  ₿ BTC_SNIPE: {sig.market_type} ${sig.threshold:,.0f} "
            f"(BTC=${sig.btc_price:,.0f}, conf={sig.confidence:.0%}) → "
            f"{shares:.0f} shares @ ${current_price:.2f} = ${cost:.2f} | "
            f"{sig.question[:50]}"
        )

        if dry_run:
            trades.append({
                "type": "BTC_SNIPE", "question": sig.question[:60],
                "market_type": sig.market_type, "threshold": sig.threshold,
                "btc_price": sig.btc_price, "confidence": sig.confidence,
                "cost": cost, "status": "DRY_RUN",
            })
            continue

        if not pm or not pm.clob_client:
            logger.warning("  BTC sniper: no trading client — skipping execution")
            continue

        # Execute trade
        result = _execute_trade(pm, sig.token_id, current_price, cost, sig, portfolio=portfolio)
        if result and "error" not in result:
            pos_id = f"BTC-{uuid.uuid4().hex[:8]}"
            portfolio.open_position(Position(
                id=pos_id,
                market_type="BTC_SNIPE",
                description=f"BTC {sig.market_type} ${sig.threshold:,.0f}: {sig.question[:40]}",
                side="BUY_YES",
                token_id=sig.token_id,
                entry_price=current_price,
                shares=shares,
                cost=cost,
                entry_time=datetime.now(timezone.utc).isoformat(),
                edge_at_entry=sig.confidence,
                kelly_fraction=0.0,
                current_price=current_price,
                peak_price=current_price,
                order_id=result.get("orderID", ""),
                condition_id=sig.condition_id,
            ))
            existing_tokens.add(sig.token_id)
            open_btc += 1
            remaining -= cost
            btc_sniper.mark_seen(sig.condition_id)
            trades.append({
                "type": "BTC_SNIPE", "question": sig.question[:60],
                "market_type": sig.market_type, "threshold": sig.threshold,
                "btc_price": sig.btc_price, "confidence": sig.confidence,
                "cost": cost, "price": current_price, "status": "LIVE",
            })
        else:
            trades.append({
                "type": "BTC_SNIPE", "question": sig.question[:60],
                "market_type": sig.market_type, "threshold": sig.threshold,
                "cost": cost, "status": "FAILED",
            })

    return trades


# ═══════════════════════════════════════════════════════════════
#  Forecast Scanner Strategy (NEW 2026-04-10)
# ═══════════════════════════════════════════════════════════════

def run_forecast_scanner_strategy(
    portfolio: PortfolioTracker,
    scanner: "ForecastScanner",
    forecast: "WeatherForecast",
    pm=None,
    dry_run: bool = True,
) -> list[dict]:
    """
    Forecast scanner — scans ALL weather temp markets, buys when
    Open-Meteo ensemble probability > market price + edge.

    This strategy compares calibrated forecast probabilities with executable
    orderbook prices and trades only when the estimated edge clears the dynamic
    uncertainty threshold.
    """
    from src.polymarket_client import PolymarketClient

    # Need a PolymarketClient for Gamma API market discovery
    pm_client = PolymarketClient()

    # Existing tokens to avoid duplicates
    existing_tokens = {p.token_id for p in portfolio.positions.values() if p.status == "OPEN"}

    # Count open forecast positions
    open_forecast = sum(
        1 for p in portfolio.positions.values()
        if p.market_type == "FORECAST" and p.status == "OPEN"
    )
    if open_forecast >= ForecastScanner.MAX_POSITIONS:
        return []

    signals = scanner.scan(pm_client, forecast, existing_tokens)
    if not signals:
        return []

    # Check available capital
    usdc_balance = 0.0
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        bal_params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL, signature_type=Config.SIGNATURE_TYPE,
        )
        clob = pm.clob_client if pm and pm.clob_client else None
        bal_resp = clob.get_balance_allowance(bal_params) if clob else {}
        usdc_balance = int(bal_resp.get("balance", "0")) / 1e6
    except Exception:
        usdc_balance = max(0, portfolio.initial_bankroll - portfolio.total_deployed)
    remaining = usdc_balance

    trades = []

    for sig in signals:
        if open_forecast >= ForecastScanner.MAX_POSITIONS:
            break
        if remaining < 0.10:
            break

        # Get live orderbook price (Gamma prices can be stale)
        current_price = None
        try:
            book = pm.clob_client.get_order_book(sig.token_id)
            asks = sorted(book.asks, key=lambda a: float(a.price)) if book.asks else []
            bids = sorted(book.bids, key=lambda b: float(b.price), reverse=True) if book.bids else []
            best_ask = float(asks[0].price) if asks else None
            best_bid = float(bids[0].price) if bids else None
            if best_bid is not None and best_ask is not None:
                current_price = executable_buy_price(best_bid=best_bid, best_ask=best_ask)
            elif best_ask is not None:
                current_price = executable_buy_price(best_bid=None, best_ask=best_ask)
            elif best_bid is not None:
                current_price = round(best_bid + 0.01, 2)
        except Exception as e:
            logger.debug(f"  Forecast scanner: orderbook failed for {sig.question[:30]}: {e}")

        if current_price is None or current_price < 0.01:
            continue

        # Re-check price filter with live price
        if current_price < ForecastScanner.MIN_PRICE or current_price > ForecastScanner.MAX_PRICE:
            continue

        # Re-check edge with live executable price and current spread. This avoids
        # trading an apparent midpoint edge that disappears at the ask.
        live_spread = None
        if best_bid is not None and best_ask is not None:
            live_spread = max(0.0, best_ask - best_bid)
        live_min_edge = dynamic_min_edge(
            base_edge=ForecastScanner.BASE_MIN_EDGE,
            max_edge=ForecastScanner.MAX_MIN_EDGE,
            ensemble_std=sig.ensemble_std,
            lead_days=sig.lead_days,
            market_price=current_price,
            n_eff=sig.n_eff or sig.n_members or 1,
            spread=live_spread,
        )
        live_edge = sig.forecast_prob - current_price
        if live_edge < live_min_edge:
            logger.debug(f"  Forecast scanner: live edge {live_edge:.0%} < min {live_min_edge:.0%} — skipping")
            continue

        # Size with uncertainty-adjusted fractional Kelly, then enforce CLOB's
        # 5-share maker minimum. This keeps the old tiny-ticket behavior for a
        # small bankroll while reducing size when the probability estimate is
        # noisy or the edge is thin.
        sizer = KellySizer(
            bankroll=max(remaining, 0.0),
            kelly_fraction=0.20,
            max_position_pct=0.25,
            min_position_dollars=0.05,
            min_edge=live_min_edge,
            reserve_pct=0.0,
        )
        kelly = sizer.size_position(
            forecast_prob=sig.forecast_prob,
            market_price=current_price,
            side="BUY_YES",
            current_positions=open_forecast,
            horizon_days=sig.lead_days,
            probability_n_eff=sig.n_eff or None,
            uncertainty_z=1.0,
        )
        if kelly.bet_size_dollars <= 0:
            logger.debug(f"  Forecast scanner: Kelly sizing skipped — {kelly.reason}")
            continue
        shares = max(5.0, kelly.bet_size_dollars / current_price)
        cost = round(shares * current_price, 2)
        if cost > remaining:
            if remaining / current_price >= 5.0:
                shares = 5.0
                cost = round(shares * current_price, 2)
            else:
                continue

        logger.info(
            f"  🔮 FORECAST: {sig.city} {sig.temp_info} | "
            f"forecast={sig.forecast_prob:.0%} (raw={sig.raw_forecast_prob:.0%}) vs ask=${current_price:.2f} "
            f"(edge={live_edge:.0%}, min={live_min_edge:.0%}) "
            f"σ={sig.ensemble_std:.1f} n_eff={sig.n_eff:.1f} {sig.lead_days}d "
            f"Kelly={kelly.adjusted_fraction:.2%} → "
            f"{shares:.0f} shares @ ${current_price:.2f}"
        )

        if dry_run:
            trades.append({
                "type": "FORECAST", "question": sig.question[:60],
                "city": sig.city, "forecast_prob": sig.forecast_prob,
                "raw_forecast_prob": sig.raw_forecast_prob,
                "n_eff": sig.n_eff, "min_edge": live_min_edge,
                "kelly_fraction": kelly.adjusted_fraction,
                "market_price": current_price, "edge": live_edge,
                "cost": cost, "status": "DRY_RUN",
            })
            continue

        if not pm or not pm.clob_client:
            continue

        result = _execute_trade(pm, sig.token_id, current_price, cost, sig, portfolio=portfolio)
        if result and "error" not in result:
            pos_id = f"FC-{uuid.uuid4().hex[:8]}"
            portfolio.open_position(Position(
                id=pos_id,
                market_type="FORECAST",
                description=f"FORECAST {sig.city}: {sig.question[:45]}",
                side="BUY_YES",
                token_id=sig.token_id,
                entry_price=current_price,
                shares=shares,
                cost=cost,
                entry_time=datetime.now(timezone.utc).isoformat(),
                edge_at_entry=live_edge,
                kelly_fraction=kelly.adjusted_fraction,
                current_price=current_price,
                peak_price=current_price,
                order_id=result.get("orderID", ""),
                condition_id=sig.condition_id,
            ))
            existing_tokens.add(sig.token_id)
            open_forecast += 1
            remaining -= cost
            scanner.mark_seen(sig.condition_id)
            trades.append({
                "type": "FORECAST", "question": sig.question[:60],
                "city": sig.city, "forecast_prob": sig.forecast_prob,
                "raw_forecast_prob": sig.raw_forecast_prob,
                "n_eff": sig.n_eff, "min_edge": live_min_edge,
                "kelly_fraction": kelly.adjusted_fraction,
                "market_price": current_price, "edge": live_edge,
                "cost": cost, "price": current_price, "status": "LIVE",
            })
        else:
            trades.append({
                "type": "FORECAST", "question": sig.question[:60],
                "city": sig.city, "cost": cost, "status": "FAILED",
            })

    return trades


# ═══════════════════════════════════════════════════════════════
#  BTC 5-Minute Straddle
# ═══════════════════════════════════════════════════════════════

def run_btc_straddle_strategy(
    portfolio: PortfolioTracker,
    straddle: "BTCStraddle",
    pm=None,
    dry_run: bool = True,
    budget: float = 0.0,
) -> list[dict]:
    """
    BTC 5-minute straddle — buy both Up and Down at low prices.
    When BTC oscillates within the window, both legs fill and one pays $1.00.
    """
    markets = straddle.scan()
    if not markets:
        return []

    # Count active straddle positions (each window has up to 2 legs)
    open_straddles = len({
        p.id.rsplit("-", 1)[0]  # STR-{ts} prefix
        for p in portfolio.positions.values()
        if p.market_type == "BTC_STRADDLE" and p.status == "OPEN"
    })

    trades = []

    for market in markets:
        if open_straddles >= BTCStraddle.MAX_ACTIVE_STRADDLES:
            logger.info(f"  Straddle: max {BTCStraddle.MAX_ACTIVE_STRADDLES} active — stopping")
            break

        # Cost for full straddle: 2 legs
        leg_cost = round(BTCStraddle.SHARES_PER_LEG * BTCStraddle.TARGET_BUY_PRICE, 2)
        full_cost = leg_cost * 2
        if full_cost > budget:
            logger.debug(
                f"  Straddle: need ${full_cost:.2f} but budget is ${budget:.2f} — skipping"
            )
            continue

        window_ts = market["window_ts"]
        question = market["question"]

        logger.info(
            f"  🔀 STRADDLE: {question[:50]} | "
            f"2 legs @ ${BTCStraddle.TARGET_BUY_PRICE:.2f} x "
            f"{BTCStraddle.SHARES_PER_LEG} shares = ${full_cost:.2f}"
        )

        if dry_run:
            straddle.mark_seen(window_ts)
            trades.append({
                "type": "BTC_STRADDLE", "question": question[:60],
                "cost": full_cost, "status": "DRY_RUN",
            })
            continue

        if not pm or not pm.clob_client:
            logger.warning("  Straddle: no trading client — skipping")
            continue

        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        placed = 0
        for leg, token_id in [("UP", market["up_token_id"]), ("DOWN", market["down_token_id"])]:
            try:
                order_args = OrderArgs(
                    token_id=token_id,
                    price=BTCStraddle.TARGET_BUY_PRICE,
                    size=float(BTCStraddle.SHARES_PER_LEG),
                    side=BUY,
                )
                signed = pm.clob_client.create_order(order_args)
                result = pm.clob_client.post_order(signed, OrderType.GTC)

                success = result.get("success", False) if isinstance(result, dict) else False
                status = result.get("status", "") if isinstance(result, dict) else ""

                if success or status in ("live", "matched", "delayed"):
                    logger.success(f"  ✅ Straddle {leg} leg placed: {status}")
                    pos_id = f"STR-{window_ts}-{leg}"
                    portfolio.open_position(Position(
                        id=pos_id,
                        market_type="BTC_STRADDLE",
                        description=f"Straddle {leg}: {question[:45]}",
                        side="BUY_YES",
                        token_id=token_id,
                        entry_price=BTCStraddle.TARGET_BUY_PRICE,
                        shares=float(BTCStraddle.SHARES_PER_LEG),
                        cost=leg_cost,
                        entry_time=datetime.now(timezone.utc).isoformat(),
                        edge_at_entry=0.0,
                        kelly_fraction=0.0,
                        current_price=BTCStraddle.TARGET_BUY_PRICE,
                        peak_price=BTCStraddle.TARGET_BUY_PRICE,
                        order_id=result.get("orderID", ""),
                        condition_id=market["condition_id"],
                    ))
                    placed += 1
                    if portfolio:
                        portfolio.record_order_success()
                else:
                    logger.error(f"  ❌ Straddle {leg} leg failed: {result}")
                    if portfolio:
                        portfolio.record_order_failure()

            except Exception as e:
                logger.error(f"  ❌ Straddle {leg} order error: {e}")
                if portfolio:
                    portfolio.record_order_failure()

        if placed > 0:
            straddle.mark_seen(window_ts)
            open_straddles += 1
            trades.append({
                "type": "BTC_STRADDLE", "question": question[:60],
                "cost": leg_cost * placed, "legs_placed": placed,
                "window_ts": window_ts, "status": "LIVE",
            })
        else:
            trades.append({
                "type": "BTC_STRADDLE", "question": question[:60],
                "cost": 0, "status": "FAILED",
            })

    return trades


# ═══════════════════════════════════════════════════════════════
#  Position Management (drains legacy WEATHER positions)
# ═══════════════════════════════════════════════════════════════

def manage_weather_positions(
    portfolio: PortfolioTracker,
    dry_run: bool = True,
    pm=None,
) -> list[dict]:
    """
    Monitor open WEATHER + WHALE_COPY positions.
    WHALE_COPY: sell at 10x gain, otherwise hold to resolution.
    Legacy WEATHER: tiered take-profit + stop-loss.
    """
    actions = []
    weather_positions = [
        (pid, p) for pid, p in portfolio.positions.items()
        if p.market_type in ("WEATHER", "WHALE_COPY", "FORECAST")
    ]

    if not weather_positions:
        return actions

    if dry_run:
        logger.debug("Weather position sell monitor skipped in dry-run mode")
        return actions

    Config.assert_live_trading_allowed()

    try:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import SELL as SELL_SIDE

        clob = None
        if pm and pm.clob_client:
            clob = pm.clob_client
        else:
            from py_clob_client.client import ClobClient
            clob = ClobClient(Config.CLOB_HOST, key=Config.PRIVATE_KEY,
                             chain_id=Config.CHAIN_ID, signature_type=Config.SIGNATURE_TYPE,
                             funder=Config.FUNDER_ADDRESS)
            creds = clob.create_or_derive_api_creds()
            clob.set_api_creds(creds)
    except Exception as e:
        logger.debug(f"Weather position monitor: CLOB init failed: {e}")
        return actions

    for pid, pos in weather_positions:
        token_id = pos.token_id
        entry_price = pos.entry_price
        shares = pos.shares

        try:
            book = clob.get_order_book(token_id)
            bids = sorted(book.bids, key=lambda b: float(b.price), reverse=True) if book.bids else []
            if not bids:
                continue
            current_price = float(bids[0].price)
        except Exception:
            continue

        portfolio.update_position_price(pid, current_price)

        if current_price <= 0 or entry_price <= 0:
            continue

        gain_multiple = current_price / entry_price
        gain_pct = (current_price - entry_price) / entry_price

        action = None
        reason = ""

        if pos.market_type in ("WHALE_COPY", "FORECAST"):
            # Whale-copy / Forecast: 10x take-profit only, no stop-loss (hold to resolution)
            if gain_multiple >= 10:
                action = "TAKE_PROFIT"
                reason = f"{pos.market_type} 10x: ${entry_price:.3f} → ${current_price:.3f} ({gain_multiple:.0f}x)"
        else:
            # Legacy WEATHER positions
            if entry_price < 0.05:
                if gain_multiple >= 10:
                    action = "TAKE_PROFIT"
                    reason = f"lottery 10x: ${entry_price:.3f} → ${current_price:.3f} ({gain_multiple:.0f}x)"
                else:
                    continue
            elif entry_price < 0.08:
                if gain_multiple >= 5:
                    action = "TAKE_PROFIT"
                    reason = f"cheap 5x: ${entry_price:.3f} → ${current_price:.3f} ({gain_multiple:.0f}x)"
                else:
                    continue
            elif entry_price < 0.30:
                if gain_multiple >= 2:
                    action = "TAKE_PROFIT"
                    reason = f"mid 2x: ${entry_price:.3f} → ${current_price:.3f} ({gain_multiple:.1f}x)"
            else:
                if gain_multiple >= 1.5:
                    action = "TAKE_PROFIT"
                    reason = f"1.5x profit: ${entry_price:.3f} → ${current_price:.3f} ({gain_multiple:.1f}x)"

            if not action and pos.side == "BUY_NO" and current_price >= 0.90:
                action = "TAKE_PROFIT"
                reason = f"NO near-certain: ${entry_price:.3f} → ${current_price:.3f} ({gain_pct:+.0%})"

            if not action and gain_pct <= -0.50:
                action = "STOP_LOSS"
                reason = f"price dropped {gain_pct:.0%}: ${entry_price:.3f} → ${current_price:.3f}"

        if not action:
            continue

        sell_price = round(current_price - 0.01, 2)
        sell_price = max(0.01, min(0.99, sell_price))

        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            bal_check = clob.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL,
                                      token_id=token_id, signature_type=Config.SIGNATURE_TYPE))
            actual_shares = int(int(bal_check.get("balance", "0")) / 1e6)
            if actual_shares < 1:
                continue
            shares = float(actual_shares)
        except Exception:
            shares = float(int(shares))

        logger.info(f"  💰 {pos.market_type} {action}: {pos.description[:40]} — {reason}")

        if dry_run:
            logger.info(f"  📝 DRY RUN SELL {shares:.0f} @ ${sell_price:.3f}")
        else:
            try:
                order_args = OrderArgs(
                    token_id=token_id, price=sell_price, size=shares, side=SELL_SIDE,
                )
                signed = clob.create_order(order_args)
                result = clob.post_order(signed, OrderType.GTC)
                success = result.get("success", False) if isinstance(result, dict) else False
                status = result.get("status", "") if isinstance(result, dict) else ""
                if success or status in ("live", "matched", "delayed"):
                    logger.success(f"  ✅ SOLD: {status}")
                    portfolio.record_order_success()
                else:
                    logger.error(f"  ❌ Sell failed: {result}")
                    portfolio.record_order_failure()
                    continue
            except Exception as e:
                logger.error(f"  ❌ Sell error: {e}")
                portfolio.record_order_failure()
                continue

        profit = (sell_price - entry_price) * shares
        portfolio.close_position(pid, sell_price, exit_reason=action)

        actions.append({
            "type": "WEATHER_SELL", "action": action,
            "description": pos.description[:40],
            "entry_price": entry_price, "exit_price": sell_price,
            "profit": profit, "shares": shares,
            "status": "DRY_RUN" if dry_run else "EXECUTED",
        })

    return actions


# ═══════════════════════════════════════════════════════════════
#  Housekeeping: Phantom Cleanup + Auto-Redeem
# ═══════════════════════════════════════════════════════════════

def _cleanup_phantom_positions(portfolio, pm=None, dry_run: bool = True):
    """
    Remove positions with 0 on-chain balance (phantom positions).
    Maker orders show 0 shares until filled — 60 min grace period.
    This can cancel orders, so it is disabled in dry-run mode.
    """
    if dry_run:
        logger.debug("Phantom cleanup skipped in dry-run mode")
        return
    Config.assert_live_trading_allowed()
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

        clob = None
        if pm and pm.clob_client:
            clob = pm.clob_client
        else:
            clob = ClobClient(Config.CLOB_HOST, key=Config.PRIVATE_KEY,
                             chain_id=Config.CHAIN_ID, signature_type=Config.SIGNATURE_TYPE,
                             funder=Config.FUNDER_ADDRESS)
            creds = clob.create_or_derive_api_creds()
            clob.set_api_creds(creds)

        now = datetime.now(timezone.utc)
        GRACE_PERIOD = timedelta(minutes=60)

        removed = 0
        for pid in list(portfolio.positions.keys()):
            p = portfolio.positions[pid]
            try:
                entry_time = p.entry_time if hasattr(p, 'entry_time') else None
                if entry_time:
                    if isinstance(entry_time, str):
                        entry_time = datetime.fromisoformat(entry_time)
                    if now - entry_time < GRACE_PERIOD:
                        continue

                params = BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=p.token_id,
                    signature_type=Config.SIGNATURE_TYPE,
                )
                bal = clob.get_balance_allowance(params)
                shares = int(bal.get("balance", "0")) / 1e6
                if shares <= 0:
                    if p.order_id and clob:
                        try:
                            clob.cancel(p.order_id)
                        except Exception:
                            pass

                    logger.info(f"  Removing phantom: {p.description[:35]} (0 shares on-chain)")
                    try:
                        entry_dt = datetime.fromisoformat(p.entry_time) if isinstance(p.entry_time, str) else p.entry_time
                        hold_hours = (now - entry_dt).total_seconds() / 3600
                        trade = ClosedTrade(
                            id=p.id, market_type=p.market_type, description=p.description,
                            side=p.side, entry_price=p.entry_price, exit_price=0.0,
                            shares=p.shares, cost=p.cost, revenue=0.0,
                            pnl=0.0, pnl_pct=0.0,
                            entry_time=p.entry_time if isinstance(p.entry_time, str) else p.entry_time.isoformat(),
                            exit_time=now.isoformat(), exit_reason="PHANTOM",
                            hold_duration_hours=round(hold_hours, 2),
                        )
                        portfolio._append_history(trade)
                    except Exception:
                        pass
                    del portfolio.positions[pid]
                    removed += 1
                elif abs(shares - p.shares) > 1.0:
                    p.shares = shares
                    p.cost = shares * p.entry_price
            except Exception:
                continue

        if removed > 0:
            portfolio._save_positions()
            logger.info(f"  Phantom cleanup: removed {removed} phantom positions")

    except Exception as e:
        logger.debug(f"Phantom cleanup failed: {e}")


_redeem_last_full_log = 0
_redeem_rate_limited_until = 0  # timestamp: skip relayer calls until this time

# Persist already-redeemed conditionIds to disk so we don't re-check on restart
_REDEEM_CACHE_FILE = os.path.join("data", "redeem_already_redeemed.json")
def _load_redeem_cache():
    try:
        with open(_REDEEM_CACHE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_redeem_cache(cache):
    with open(_REDEEM_CACHE_FILE, "w") as f:
        json.dump(cache, f)

_redeem_already_redeemed = _load_redeem_cache()
_redeem_seen_ids = set(_redeem_already_redeemed.keys())  # suppress "NEW LOSS" for cached entries

# Polygon contract addresses for on-chain redemption
_CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
_NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
_USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"


_POLYGON_RPC = "https://polygon-rpc.com"

def _check_onchain_balance(proxy_wallet, token_id):
    """Check on-chain CTF token balance. Returns balance in shares (float)."""
    import requests
    from eth_abi import encode as eth_encode
    from eth_utils import keccak

    selector = keccak(text="balanceOf(address,uint256)")[:4]
    args = eth_encode(["address", "uint256"], [proxy_wallet, int(token_id)])
    data = "0x" + (selector + args).hex()

    resp = requests.post(_POLYGON_RPC, json={
        "jsonrpc": "2.0", "id": 1,
        "method": "eth_call",
        "params": [{"to": _CTF_ADDRESS, "data": data}, "latest"]
    }, timeout=10)
    result = resp.json().get("result", "0x0")
    return int(result, 16) / 1e6


def _build_redeem_txn(pos):
    """Build a relayer transaction to redeem a resolved position on-chain."""
    from eth_abi import encode as eth_encode
    from eth_utils import keccak
    from py_builder_relayer_client.models import Transaction

    cid = pos.get("conditionId", pos.get("condition_id", ""))
    if cid.startswith("0x"):
        cid = cid[2:]
    condition_bytes = bytes.fromhex(cid)
    neg_risk = pos.get("negRisk", pos.get("neg_risk"))

    if neg_risk:
        # Neg-risk markets (multi-outcome) → NegRiskAdapter.redeemPositions(bytes32, uint256[])
        selector = keccak(text="redeemPositions(bytes32,uint256[])")[:4]
        size_raw = int(float(pos.get("size", 0)) * 1e6)
        outcome_index = int(pos.get("outcomeIndex", pos.get("outcome_index", 0)))
        amounts = [0, 0]
        amounts[outcome_index] = size_raw
        args = eth_encode(["bytes32", "uint256[]"], [condition_bytes, amounts])
        return Transaction(
            to=_NEG_RISK_ADAPTER,
            data="0x" + (selector + args).hex(),
            value="0",
        )
    else:
        # Standard binary markets → CTF.redeemPositions(address, bytes32, bytes32, uint256[])
        selector = keccak(text="redeemPositions(address,bytes32,bytes32,uint256[])")[:4]
        args = eth_encode(
            ["address", "bytes32", "bytes32", "uint256[]"],
            [_USDC_ADDRESS, b"\x00" * 32, condition_bytes, [1, 2]],
        )
        return Transaction(
            to=_CTF_ADDRESS,
            data="0x" + (selector + args).hex(),
            value="0",
        )


def _get_relay_client(pm):
    """Create a RelayClient using Builder API creds from .env."""
    Config.assert_live_trading_allowed()
    from py_builder_relayer_client.client import RelayClient
    from py_builder_relayer_client.models import RelayerTxType
    from py_builder_signing_sdk.config import BuilderConfig, BuilderApiKeyCreds

    builder_key = os.getenv("POLY_BUILDER_API_KEY", "")
    builder_secret = os.getenv("POLY_BUILDER_SECRET", "")
    builder_passphrase = os.getenv("POLY_BUILDER_PASSPHRASE", "")
    if not (builder_key and builder_secret and builder_passphrase):
        logger.debug("Builder API creds not configured, skipping on-chain redeem")
        return None

    wallet_type = (RelayerTxType.PROXY if Config.SIGNATURE_TYPE == 1
                   else RelayerTxType.SAFE)

    return RelayClient(
        "https://relayer-v2.polymarket.com",
        chain_id=Config.CHAIN_ID,
        private_key=Config.PRIVATE_KEY,
        builder_config=BuilderConfig(
            local_builder_creds=BuilderApiKeyCreds(
                key=builder_key,
                secret=builder_secret,
                passphrase=builder_passphrase,
            )
        ),
        relay_tx_type=wallet_type,
    )


def _auto_redeem_resolved(portfolio, pm=None, dry_run: bool = True):
    """
    Monitor resolved positions, redeem on-chain via relayer, and clean local tracker.
    Uses Polymarket's relayer (no gas needed) to convert outcome tokens back to USDC.
    Batches all redeems into a single relayer call. Falls back to one-by-one if batch fails.
    This has account side effects, so it is disabled in dry-run mode.
    """
    if dry_run:
        logger.debug("Auto-redeem skipped in dry-run mode")
        return
    Config.assert_live_trading_allowed()
    global _redeem_seen_ids, _redeem_last_full_log, _redeem_already_redeemed
    try:
        import requests

        addr = Config.FUNDER_ADDRESS
        if not addr:
            return

        r = requests.get(
            "https://data-api.polymarket.com/positions",
            params={"user": addr, "limit": 200, "redeemable": "true",
                    "sizeThreshold": 0},
            timeout=10,
        )
        if r.status_code != 200:
            return

        positions = r.json()
        if not isinstance(positions, list) or not positions:
            return

        redeemable = [p for p in positions
                      if p.get("redeemable") and float(p.get("size", 0) or 0) > 0]
        if not redeemable:
            return

        now = time.time()
        current_ids = {p.get("conditionId", "") for p in redeemable}
        new_ids = current_ids - _redeem_seen_ids

        wins = sum(1 for p in redeemable if float(p.get("curPrice", 0) or 0) > 0.5)
        losses = len(redeemable) - wins
        pending_usdc = sum(
            float(p.get("size", 0) or 0)
            for p in redeemable
            if float(p.get("curPrice", 0) or 0) > 0.5
        )

        if new_ids:
            new_positions = [p for p in redeemable if p.get("conditionId") in new_ids]
            for p in new_positions:
                price = float(p.get("curPrice", 0) or 0)
                status = "WIN" if price > 0.5 else "LOSS"
                logger.info(f"  Redeem: NEW {status} - {p.get('title', '?')[:50]}")
            _redeem_seen_ids.update(new_ids)

        if now - _redeem_last_full_log > 1800 or wins > 0:
            logger.info(f"  Redeem monitor: {len(redeemable)} pending "
                        f"({wins}W/{losses}L, ${pending_usdc:.2f} USDC incoming)")
            _redeem_last_full_log = now

        # ── On-chain redemption via relayer (with balance check) ──
        global _redeem_rate_limited_until
        if now < _redeem_rate_limited_until:
            pass  # Skip relayer calls during cooldown
        else:
            retry_after = 1800  # 30 minutes
            to_redeem = [p for p in redeemable
                         if p.get("conditionId", "") not in _redeem_already_redeemed
                         or now - _redeem_already_redeemed.get(p.get("conditionId", ""), 0) > retry_after]
            if to_redeem:
                # Check on-chain balances — skip already-redeemed (API is stale)
                proxy_wallet = to_redeem[0].get("proxyWallet", "")
                actually_need_redeem = []
                skipped = 0
                for pos in to_redeem:
                    try:
                        balance = _check_onchain_balance(proxy_wallet, pos.get("asset", "0"))
                        if balance > 0:
                            actually_need_redeem.append(pos)
                        else:
                            skipped += 1
                            _redeem_already_redeemed[pos.get("conditionId", "")] = 9999999999  # permanent
                    except Exception:
                        actually_need_redeem.append(pos)  # If check fails, try anyway
                if skipped > 0:
                    _save_redeem_cache(_redeem_already_redeemed)
                    logger.info(f"  Redeem: {skipped} already redeemed on-chain (stale API), "
                                f"{len(actually_need_redeem)} need redeem")

                if actually_need_redeem:
                    try:
                        relay_client = _get_relay_client(pm)
                        if relay_client:
                            txns = []
                            for pos in actually_need_redeem:
                                try:
                                    txns.append(_build_redeem_txn(pos))
                                except Exception as e:
                                    logger.debug(f"  Redeem txn build failed: {e}")
                            if txns:
                                try:
                                    resp = relay_client.execute(txns, f"batch-redeem {len(txns)}")
                                    for pos in actually_need_redeem:
                                        _redeem_already_redeemed[pos.get("conditionId", "")] = now
                                    _save_redeem_cache(_redeem_already_redeemed)
                                    w = sum(1 for p in actually_need_redeem if float(p.get("curPrice", 0) or 0) > 0.5)
                                    l = len(actually_need_redeem) - w
                                    logger.success(
                                        f"  REDEEMED batch: {len(txns)} positions ({w}W/{l}L) "
                                        f"txn={resp.transaction_id}")
                                except Exception as e:
                                    err_str = str(e)
                                    if "429" in err_str or "rate" in err_str.lower():
                                        _redeem_rate_limited_until = now + 900
                                        logger.warning(f"  Redeem rate-limited, cooling down 15 min")
                                    else:
                                        # Batch failed — fall back to one-by-one
                                        logger.debug(f"  Batch redeem failed ({e}), trying individually...")
                                        redeemed = 0
                                        for pos in actually_need_redeem[:25]:
                                            cid = pos.get("conditionId", "")
                                            title = pos.get("title", "?")[:40]
                                            try:
                                                txn = _build_redeem_txn(pos)
                                                resp = relay_client.execute([txn], f"redeem {cid[:12]}")
                                                _redeem_already_redeemed[cid] = now
                                                redeemed += 1
                                                size = float(pos.get("size", 0) or 0)
                                                price = float(pos.get("curPrice", 0) or 0)
                                                result = "WIN" if price > 0.5 else "LOSS"
                                                logger.success(
                                                    f"  REDEEMED on-chain: {result} {title} "
                                                    f"({size:.1f} shares, txn={resp.transaction_id})")
                                                time.sleep(2)
                                            except Exception as e2:
                                                if "429" in str(e2) or "rate" in str(e2).lower():
                                                    _redeem_rate_limited_until = now + 900
                                                    logger.warning(f"  Redeem rate-limited after {redeemed}, cooling down 15 min")
                                                    break
                                                _redeem_already_redeemed[cid] = now
                                                logger.warning(f"  Redeem failed for {title}: {e2}")
                    except Exception as e:
                        logger.debug(f"Relayer init failed: {e}")

        # ── Remove matching local portfolio entries for resolved positions ──
        redeemable_by_token = {}
        for p in redeemable:
            token = p.get("asset", "")
            if token:
                price = float(p.get("curPrice", 0) or 0)
                redeemable_by_token[token] = price

        removed = 0
        for pid in list(portfolio.positions.keys()):
            pos = portfolio.positions[pid]
            if pos.token_id in redeemable_by_token:
                resolved_price = redeemable_by_token[pos.token_id]
                exit_price = 1.0 if resolved_price > 0.5 else 0.0
                try:
                    portfolio.close_position(pid, exit_price, exit_reason="RESOLVED")
                except Exception:
                    del portfolio.positions[pid]
                    portfolio._save_positions()
                removed += 1

        if removed > 0:
            logger.info(f"  Cleared {removed} resolved positions from local tracker")

    except Exception as e:
        logger.debug(f"Auto-redeem failed: {e}")


# ═══════════════════════════════════════════════════════════════
#  V1 Position Sync (startup only)
# ═══════════════════════════════════════════════════════════════

def _sync_untracked_positions(portfolio, dry_run: bool = True):
    """Find legacy local trades that still exist on-chain and add them to local state."""
    if dry_run:
        logger.debug("Position sync skipped in dry-run mode")
        return
    Config.assert_live_trading_allowed()
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        from py_clob_client.client import ClobClient

        v1_file = Path(__file__).parent / "data" / "live_trades.jsonl"
        if not v1_file.exists():
            return

        tracked_tokens = {p.token_id for p in portfolio.positions.values()}

        clob = ClobClient(Config.CLOB_HOST, key=Config.PRIVATE_KEY,
                         chain_id=Config.CHAIN_ID, signature_type=Config.SIGNATURE_TYPE,
                         funder=Config.FUNDER_ADDRESS)
        creds = clob.create_or_derive_api_creds()
        clob.set_api_creds(creds)

        import uuid as _uuid
        with open(v1_file) as f:
            for line in f:
                try:
                    t = json.loads(line)
                    tid = t["token_id"]
                    if tid in tracked_tokens:
                        continue

                    bal = clob.get_balance_allowance(
                        BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL,
                                              token_id=tid, signature_type=Config.SIGNATURE_TYPE))
                    shares = int(bal.get("balance", "0")) / 1e6
                    if shares < 0.5:
                        continue

                    pos = Position(
                        id=f"V1-{_uuid.uuid4().hex[:8]}",
                        market_type="WEATHER",
                        description=f"{t['city']} {t['bucket']}",
                        side=t["side"], token_id=tid,
                        entry_price=t["price"], shares=shares,
                        cost=shares * t["price"],
                        entry_time=t.get("timestamp", ""),
                        edge_at_entry=t.get("edge", 0),
                    )
                    portfolio.positions[pos.id] = pos
                    tracked_tokens.add(tid)
                    logger.info(f"  Synced untracked v1 position: {t['city']} {t['bucket'].get('type','')} "
                               f"{t['bucket'].get('value','')} | {shares:.1f} shares")
                except Exception:
                    continue

        portfolio._save_positions()
    except Exception as e:
        logger.debug(f"V1 sync error: {e}")


# ═══════════════════════════════════════════════════════════════
#  Trade Logger
# ═══════════════════════════════════════════════════════════════

def save_trade(trade: dict):
    TRADES_FILE.parent.mkdir(exist_ok=True)
    trade["timestamp"] = datetime.now(timezone.utc).isoformat()
    with open(TRADES_FILE, "a") as f:
        f.write(json.dumps(trade) + "\n")


# ═══════════════════════════════════════════════════════════════
#  Main Loop
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Whale Copy-Trade Bot")
    parser.add_argument("--live", action="store_true", help="Live trading (real money!)")
    parser.add_argument("--budget", type=float, default=50.0, help="Max $ to deploy")
    parser.add_argument("--max-positions", type=int, default=999, help="Max open positions")
    args = parser.parse_args()

    if args.live:
        try:
            Config.assert_live_trading_allowed()
        except RuntimeError as e:
            parser.error(str(e))

    dry_run = not args.live
    mode = "LIVE" if args.live else "DRY RUN"

    base_kelly = KellySizer(
        bankroll=args.budget,
        kelly_fraction=0.25,
        max_position_pct=0.15,
        min_position_dollars=0.25,
        min_edge=0.05,
        max_positions=args.max_positions,
        reserve_pct=0.0,
    )
    kelly = DynamicKellySizer(
        inner=base_kelly,
        peak_bankroll=args.budget,
        history_path=str(Path(__file__).parent / "data" / "kelly_history.json"),
    )

    portfolio = PortfolioTracker(
        data_dir=str(Path(__file__).parent / "data"),
        max_positions=args.max_positions,
        daily_loss_limit_pct=0.15,
        max_drawdown_pct=0.25,
        reserve_pct=0.0,
        max_per_category=999,
        cooldown_minutes=60,
    )
    portfolio.initial_bankroll = args.budget
    portfolio.peak_portfolio_value = args.budget

    kelly._portfolio_value = args.budget
    base_kelly.bankroll = args.budget

    CASH_FLOOR = 0.0

    whale_tracker = WhaleTracker(data_dir=str(Path(__file__).parent / "data"))
    btc_sniper = BTCSniper(data_dir=str(Path(__file__).parent / "data"))
    btc_straddle = BTCStraddle(data_dir=str(Path(__file__).parent / "data"))
    weather_forecast = WeatherForecast()
    forecast_scanner = ForecastScanner()

    pm = None
    if not dry_run:
        try:
            from src.polymarket_client import PolymarketClient
            pm = PolymarketClient()
            pm.init_trading_client()
        except Exception as e:
            logger.warning(f"CLOB client init failed: {e}")
            pm = None

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║     🐋 WHALE COPY + ₿ BTC SNIPER + 🔀 STRADDLE BOT          ║
║                                                              ║
║  Mode: {mode:<53s} ║
║  Budget: ${args.budget:<51.2f} ║
║  Strategy 1: whale copy-trade (weather, max $0.10)           ║
║  Strategy 2: BTC price sniper (Binance real-time)            ║
║  Strategy 3: BTC 5-min straddle (both sides @ $0.20)         ║
║  Capital: 50% weather / 50% straddle                         ║
║  Mode: never-stop (no circuit breakers)                      ║
╚══════════════════════════════════════════════════════════════╝
    """)

    # Sync untracked legacy positions on startup only in live mode.
    try:
        _sync_untracked_positions(portfolio, dry_run=dry_run)
    except Exception as e:
        logger.debug(f"V1 sync on startup failed: {e}")

    # ── Continuous loop ──
    iteration = 0
    last_phantom_cleanup = 0
    last_whale_scan = 0
    total_trades = {"WHALE_COPY": 0, "BTC_SNIPE": 0, "BTC_STRADDLE": 0, "FORECAST": 0}

    try:
        while True:
            iteration += 1
            now_ts = time.time()

            # ── Periodic housekeeping (every 5 min) ──
            if now_ts - last_phantom_cleanup > 300:
                try:
                    _auto_redeem_resolved(portfolio, pm=pm, dry_run=dry_run)
                except Exception as e:
                    logger.debug(f"Auto-redeem error: {e}")

                try:
                    _cleanup_phantom_positions(portfolio, pm=pm, dry_run=dry_run)
                except Exception as e:
                    logger.debug(f"Phantom cleanup error: {e}")

                last_phantom_cleanup = now_ts

            # ── Whale tracker scans — DISABLED 2026-04-11 (whale copy disabled) ──
            # No need to poll wallet activity when we're not copying trades
            # if now_ts - last_whale_scan > 600:
            #     try:
            #         whale_tracker.fetch_whale_activity(hours_back=4)
            #     except Exception as e:
            #         logger.debug(f"Whale scan error: {e}")
            #     last_whale_scan = now_ts
            # try:
            #     whale_tracker.fetch_weather_whale_activity(hours_back=4)
            # except Exception as e:
            #     logger.debug(f"Weather whale scan error: {e}")

            # ── Calculate available capital ──
            try:
                from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                bal_params = BalanceAllowanceParams(
                    asset_type=AssetType.COLLATERAL,
                    signature_type=Config.SIGNATURE_TYPE,
                )
                clob_for_bal = pm.clob_client if pm and pm.clob_client else None
                bal_resp = clob_for_bal.get_balance_allowance(bal_params) if clob_for_bal else {}
                usdc_balance = int(bal_resp.get("balance", "0")) / 1e6
            except Exception:
                usdc_balance = max(0, args.budget - portfolio.total_deployed)

            available = max(0, usdc_balance - CASH_FLOOR)
            straddle_budget = available / 2   # Half for straddle
            weather_budget = available / 2    # Half for weather/whale-copy

            # Update Kelly with full portfolio value
            total_portfolio = usdc_balance + portfolio.total_deployed
            base_kelly.bankroll = total_portfolio
            kelly._portfolio_value = total_portfolio
            kelly.peak_bankroll = max(kelly.peak_bankroll, total_portfolio)

            # ── Legacy weather position monitor (every cycle) ──
            try:
                wx_sells = manage_weather_positions(portfolio, dry_run=dry_run, pm=pm)
                for t in wx_sells:
                    save_trade(t)
                    act = t.get("action", "SELL")
                    profit = t.get("profit", 0)
                    print(f"  💰 WEATHER {act}: {t.get('description','?')[:30]} "
                          f"| profit=${profit:+.2f} | {t['status']}")
            except Exception as e:
                logger.error(f"Weather position monitor error: {e}")

            # ── Whale copy-trade — disabled by default in the public template ──
            # Forecast scanner covers the same market family with explicit edge calculations.
            # try:
            #     whale_copy_trades = run_whale_weather_copy_strategy(
            #         portfolio, whale_tracker, pm=pm, dry_run=dry_run,
            #         forecast=weather_forecast,
            #     )
            #     for t in whale_copy_trades:
            #         save_trade(t)
            #         total_trades["WHALE_COPY"] += 1
            #         status = t.get("status", "?")
            #         src = t.get("source", "?")
            #         print(f"  🐋 WHALE_COPY: {src} → ${t.get('cost', 0):.2f} | "
            #               f"{t.get('question', '?')[:40]} | {status}")
            # except Exception as e:
            #     logger.error(f"Whale copy-trade error: {e}")

            # ── Forecast scanner (every cycle — scans markets for forecast edge) ──
            try:
                fc_trades = run_forecast_scanner_strategy(
                    portfolio, forecast_scanner, weather_forecast,
                    pm=pm, dry_run=dry_run,
                )
                for t in fc_trades:
                    save_trade(t)
                    total_trades["FORECAST"] += 1
                    status = t.get("status", "?")
                    edge = t.get("edge", 0)
                    prob = t.get("forecast_prob", 0)
                    print(f"  🔮 FORECAST: {t.get('city', '?')} | "
                          f"prob={prob:.0%} edge={edge:.0%} → ${t.get('cost', 0):.2f} | "
                          f"{t.get('question', '?')[:40]} | {status}")
            except Exception as e:
                logger.error(f"Forecast scanner error: {e}")

            # ── BTC price sniper (every cycle — Binance is free + unlimited) ──
            try:
                btc_trades = run_btc_sniper_strategy(
                    portfolio, btc_sniper, pm=pm, dry_run=dry_run,
                )
                for t in btc_trades:
                    save_trade(t)
                    total_trades["BTC_SNIPE"] += 1
                    status = t.get("status", "?")
                    mtype = t.get("market_type", "?")
                    print(f"  ₿ BTC_SNIPE: {mtype} ${t.get('threshold', 0):,.0f} → "
                          f"${t.get('cost', 0):.2f} | {t.get('question', '?')[:40]} | {status}")
            except Exception as e:
                logger.error(f"BTC sniper error: {e}")

            # ── BTC 5-min straddle — experimental and disabled by default ──
            # try:
            #     straddle_trades = run_btc_straddle_strategy(
            #         portfolio, btc_straddle, pm=pm, dry_run=dry_run,
            #         budget=straddle_budget,
            #     )
            #     for t in straddle_trades:
            #         save_trade(t)
            #         total_trades["BTC_STRADDLE"] += 1
            #         status = t.get("status", "?")
            #         legs = t.get("legs_placed", 0)
            #         print(f"  🔀 STRADDLE: {legs} legs @ ${t.get('cost', 0):.2f} | "
            #               f"{t.get('question', '?')[:40]} | {status}")
            # except Exception as e:
            #     logger.error(f"BTC straddle error: {e}")

            # ── Status update ──
            if iteration % 20 == 0:
                portfolio.take_snapshot()
                stats = portfolio.get_daily_stats()
                print(
                    f"  Cycle {iteration} "
                    f"| USDC=${usdc_balance:.2f} "
                    f"| Available=${available:.2f} "
                    f"| Pos: {portfolio.open_position_count}/{args.max_positions} "
                    f"| P&L: ${stats['total_pnl']:+.2f} "
                    f"| Trades: FC={total_trades['FORECAST']} BTC={total_trades['BTC_SNIPE']}"
                )

            time.sleep(60)

    except KeyboardInterrupt:
        print(f"\n  🛑 Stopped after {iteration} cycles.")
        print(f"  Trades: {total_trades}")
        print(f"  Deployed: ${portfolio.total_deployed:.2f}")
        print(f"  Open positions: {portfolio.open_position_count}")
        stats = portfolio.get_daily_stats()
        print(f"  Today P&L: ${stats['total_pnl']:+.2f}")
        portfolio.take_snapshot()


if __name__ == "__main__":
    main()
