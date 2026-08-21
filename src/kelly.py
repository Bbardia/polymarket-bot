"""
Kelly Criterion Position Sizing — Mathematically optimal bet sizing.

Replaces the rigid HIGH/MEDIUM/LOW tier system with continuous, edge-aware sizing.

Core formula for binary prediction markets:
  kelly_fraction = (p * b - q) / b

Where:
  p = estimated probability of winning (our forecast)
  q = 1 - p = probability of losing
  b = net odds received on the wager (payout / cost - 1)

For a YES token at price `c`:
  b = (1 - c) / c    (pay c, win 1.00)
  kelly_fraction = p - q/b = p - (1-p)*c / (1-c)

Safety: We use FRACTIONAL Kelly (default 0.25 = Quarter-Kelly) because:
  - Full Kelly assumes perfect probability estimates (we don't have that)
  - Quarter-Kelly returns ~51% of full-Kelly profit with 1/11th the variance
  - Half-Kelly returns ~75% of profit with 1/4th the variance
  - Professional traders universally use fractional Kelly

References:
  - Kelly (1956), "A New Interpretation of Information Rate"
  - Thorp (2006), "The Kelly Criterion in Blackjack, Sports Betting, and the Stock Market"
  - Polymarket 2025 whale analysis: top wallets size 2-5% per position
"""
import json
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from loguru import logger

from src.edge_math import conservative_probability


@dataclass
class KellyResult:
    """Result of Kelly Criterion position sizing calculation."""
    fraction: float          # Raw Kelly fraction (0-1)
    adjusted_fraction: float # After applying fractional multiplier
    bet_size_dollars: float  # Actual dollar amount to bet
    edge: float              # Our edge (forecast_prob - market_prob)
    expected_value: float    # Expected profit per dollar risked
    confidence: str          # Derived confidence label
    reason: str              # Human-readable explanation

    def __repr__(self):
        return (
            f"Kelly(f={self.fraction:.3f}, adj={self.adjusted_fraction:.3f}, "
            f"${self.bet_size_dollars:.2f}, edge={self.edge:+.1%}, "
            f"EV={self.expected_value:+.3f}, {self.confidence})"
        )


class KellySizer:
    """
    Kelly Criterion position sizer for Polymarket binary markets.

    Computes mathematically optimal bet size given:
    - Our estimated probability of the event
    - The market price (implied probability)
    - Our bankroll and risk parameters
    """

    def __init__(
        self,
        bankroll: float = 100.0,
        kelly_fraction: float = 0.25,       # Quarter-Kelly default
        max_position_pct: float = 0.10,      # Never bet >10% of bankroll
        min_position_dollars: float = 1.00,  # Minimum $1 bet
        min_edge: float = 0.05,              # Minimum 5% edge to trade
        max_positions: int = 12,             # Max simultaneous positions
        reserve_pct: float = 0.25,           # Keep 25% cash reserve
    ):
        self.bankroll = bankroll
        self.kelly_fraction = kelly_fraction
        self.max_position_pct = max_position_pct
        self.min_position_dollars = min_position_dollars
        self.min_edge = min_edge
        self.max_positions = max_positions
        self.reserve_pct = reserve_pct

    @property
    def available_capital(self) -> float:
        """Capital available for trading after reserve."""
        return self.bankroll * (1.0 - self.reserve_pct)

    def update_bankroll(self, new_bankroll: float):
        """Update bankroll (e.g., after wins/losses)."""
        self.bankroll = max(0.0, new_bankroll)

    def compute_kelly_fraction(self, prob: float, price: float) -> float:
        """
        Compute raw Kelly fraction for a binary market.

        For BUY_YES at price `c` with true probability `p`:
          b = (1 - c) / c  (net odds)
          f* = (p * b - (1-p)) / b = p - (1-p)*c / (1-c)

        For BUY_NO at price `(1-c)` with true probability `(1-p)`:
          Same formula applied to the NO side.

        Returns fraction of bankroll to bet (can be negative = don't bet).
        """
        if price <= 0.0 or price >= 1.0:
            return 0.0
        if prob <= 0.0 or prob >= 1.0:
            return max(0.0, min(1.0, prob - price))

        # Net odds: win (1-price) for every price risked
        b = (1.0 - price) / price
        q = 1.0 - prob

        # Kelly formula
        f = (prob * b - q) / b

        return max(0.0, f)

    def size_position(
        self,
        forecast_prob: float,
        market_price: float,
        side: str = "BUY_YES",
        current_positions: int = 0,
        horizon_days: int = 0,
        market_liquidity: float = 1000.0,
        probability_n_eff: float | None = None,
        uncertainty_z: float = 1.0,
    ) -> KellyResult:
        """
        Calculate optimal position size for a trade.

        Args:
            forecast_prob: Our estimated probability of YES outcome (0-1)
            market_price: Current YES token price (0-1)
            side: "BUY_YES" or "BUY_NO"
            current_positions: Number of current open positions
            horizon_days: Days until resolution (longer = more uncertainty)
            market_liquidity: Market liquidity in dollars
            probability_n_eff: Optional effective sample size for uncertainty-adjusted sizing
            uncertainty_z: Standard-error multiplier subtracted from forecast probability
        """
        # Use lower-confidence probability for sizing/filtering when the caller
        # knows the effective sample size. This protects Kelly from noisy model
        # edges; reported edge remains based on the original forecast below.
        sizing_forecast_prob = forecast_prob
        used_conservative_prob = False
        if probability_n_eff is not None:
            sizing_forecast_prob = conservative_probability(
                forecast_prob,
                n_eff=probability_n_eff,
                z=uncertainty_z,
            )
            used_conservative_prob = True

        # Determine effective probability and price based on side
        if side == "BUY_YES":
            prob = sizing_forecast_prob
            price = market_price
            edge = sizing_forecast_prob - market_price
        else:  # BUY_NO
            prob = 1.0 - sizing_forecast_prob
            price = 1.0 - market_price
            edge = (1.0 - sizing_forecast_prob) - (1.0 - market_price)

        # Check minimum edge
        if edge < self.min_edge:
            return KellyResult(
                fraction=0.0, adjusted_fraction=0.0, bet_size_dollars=0.0,
                edge=edge, expected_value=0.0,
                confidence="SKIP", reason=f"Edge {edge:.1%} below minimum {self.min_edge:.1%}"
            )

        # Compute raw Kelly fraction
        raw_kelly = self.compute_kelly_fraction(prob, price)

        if raw_kelly <= 0:
            return KellyResult(
                fraction=0.0, adjusted_fraction=0.0, bet_size_dollars=0.0,
                edge=edge, expected_value=0.0,
                confidence="SKIP", reason=f"Kelly fraction non-positive ({raw_kelly:.4f})"
            )

        # Apply fractional Kelly
        adjusted = raw_kelly * self.kelly_fraction

        # ── Adjustment factors ──

        # 1. Horizon discount: longer horizons = more forecast uncertainty
        if horizon_days >= 3:
            adjusted *= 0.50  # Heavy discount for 3+ days
        elif horizon_days >= 2:
            adjusted *= 0.70  # Moderate discount
        elif horizon_days >= 1:
            adjusted *= 0.85  # Slight discount

        # 2. Position count scaling: reduce size as we add more positions
        if current_positions >= self.max_positions:
            return KellyResult(
                fraction=raw_kelly, adjusted_fraction=0.0, bet_size_dollars=0.0,
                edge=edge, expected_value=0.0,
                confidence="SKIP", reason=f"Max positions ({self.max_positions}) reached"
            )
        if current_positions > 0:
            # Gradually reduce: 100% at 0 positions, ~50% at max_positions
            diversity_factor = 1.0 - (current_positions / (self.max_positions * 2))
            adjusted *= max(0.3, diversity_factor)

        # 3. Liquidity cap: don't take more than 5% of market liquidity
        max_from_liquidity = market_liquidity * 0.05

        # 4. Cap at max position percentage
        adjusted = min(adjusted, self.max_position_pct)

        # Convert to dollars
        bet_dollars = adjusted * self.available_capital
        bet_dollars = min(bet_dollars, max_from_liquidity)

        # Enforce minimum (but don't bet more than we have)
        if bet_dollars < self.min_position_dollars:
            if edge > 0.20:
                # Strong edge but small Kelly → bet minimum
                bet_dollars = self.min_position_dollars
            else:
                return KellyResult(
                    fraction=raw_kelly, adjusted_fraction=adjusted, bet_size_dollars=0.0,
                    edge=edge, expected_value=0.0,
                    confidence="SKIP",
                    reason=f"Size ${bet_dollars:.2f} below minimum ${self.min_position_dollars:.2f}"
                )

        # Cap at available capital
        bet_dollars = min(bet_dollars, self.available_capital)

        # Calculate expected value per dollar
        ev = prob * (1.0 - price) - (1.0 - prob) * price

        # Derive confidence label from Kelly fraction
        if raw_kelly > 0.30:
            confidence = "HIGH"
        elif raw_kelly > 0.15:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        sizing_note = (
            f", p_conservative={sizing_forecast_prob:.1%}"
            if used_conservative_prob else ""
        )

        return KellyResult(
            fraction=raw_kelly,
            adjusted_fraction=adjusted,
            bet_size_dollars=round(bet_dollars, 2),
            edge=edge,
            expected_value=ev,
            confidence=confidence,
            reason=f"Kelly={raw_kelly:.3f} × {self.kelly_fraction} → {adjusted:.3f} "
                   f"→ ${bet_dollars:.2f} (edge={edge:+.1%}, EV={ev:+.3f}{sizing_note})"
        )

    def rank_opportunities(self, opportunities: list[dict]) -> list[dict]:
        """
        Rank and size a list of opportunities optimally.

        Each opportunity should have:
          - forecast_prob, market_price, side
          - Optional: horizon_days, market_liquidity

        Returns sorted by expected value, with sizes adjusted for portfolio.
        """
        sized = []
        positions_so_far = 0

        # First pass: compute Kelly for all
        for opp in opportunities:
            result = self.size_position(
                forecast_prob=opp["forecast_prob"],
                market_price=opp["market_price"],
                side=opp.get("side", "BUY_YES"),
                current_positions=0,  # Size independently first
                horizon_days=opp.get("horizon_days", 0),
                market_liquidity=opp.get("market_liquidity", 1000.0),
            )
            opp["kelly"] = result
            opp["ev_per_dollar"] = result.expected_value
            sized.append(opp)

        # Sort by EV per dollar (best opportunities first)
        sized.sort(key=lambda x: x["ev_per_dollar"], reverse=True)

        # Second pass: re-size with position count scaling
        total_allocated = 0.0
        for opp in sized:
            if opp["kelly"].confidence == "SKIP":
                continue

            result = self.size_position(
                forecast_prob=opp["forecast_prob"],
                market_price=opp["market_price"],
                side=opp.get("side", "BUY_YES"),
                current_positions=positions_so_far,
                horizon_days=opp.get("horizon_days", 0),
                market_liquidity=opp.get("market_liquidity", 1000.0),
            )

            # Check if we still have capital
            if total_allocated + result.bet_size_dollars > self.available_capital:
                result = KellyResult(
                    fraction=result.fraction, adjusted_fraction=0.0,
                    bet_size_dollars=0.0, edge=result.edge,
                    expected_value=result.expected_value,
                    confidence="SKIP", reason="Budget exhausted"
                )

            opp["kelly"] = result
            if result.bet_size_dollars > 0:
                positions_so_far += 1
                total_allocated += result.bet_size_dollars

        return sized


# ── Convenience function for quick sizing ──

def kelly_size(
    forecast_prob: float,
    market_price: float,
    bankroll: float = 100.0,
    side: str = "BUY_YES",
    fraction: float = 0.25,
) -> float:
    """Quick Kelly sizing — returns dollar amount to bet."""
    sizer = KellySizer(bankroll=bankroll, kelly_fraction=fraction)
    result = sizer.size_position(forecast_prob, market_price, side)
    return result.bet_size_dollars


# ── Dynamic Kelly Sizer — performance-adaptive wrapper ──

KELLY_HISTORY_PATH = str(Path(__file__).parent.parent / "data" / "kelly_history.json")


class DynamicKellySizer:
    """
    Wraps KellySizer with dynamic multipliers derived from recent trading
    performance.  All adjustments are multiplicative on the base Kelly output.

    Multipliers applied (in order):
      1. Drawdown ramp        — linear reduction starting at 5% drawdown
      2. Win/loss streak       — boost after 3+ wins, cut after 2+ losses
      3. Category performance  — per-category win-rate vs overall
      4. Sample size discount  — conservative sizing with few trades
      5. Volatility scaling    — target a stable P&L volatility
    """

    def __init__(
        self,
        inner: KellySizer,
        peak_bankroll: Optional[float] = None,
        target_volatility: float = 0.02,   # 2% of bankroll per-trade target
        lookback: int = 30,                 # rolling window for vol calc
        history_path: str = KELLY_HISTORY_PATH,
    ):
        self.inner = inner
        self.peak_bankroll = peak_bankroll or inner.bankroll
        self.target_volatility = target_volatility
        self.lookback = lookback
        self.history_path = history_path

        # Trade history: list of {"pnl": float, "category": str, "won": bool}
        self.trades: list[dict] = []
        self._load_history()

    # ── Delegate bankroll to inner KellySizer ────────────────────

    @property
    def bankroll(self) -> float:
        return self.inner.bankroll

    @bankroll.setter
    def bankroll(self, value: float):
        self.inner.bankroll = value

    # ── Persistence ──────────────────────────────────────────────

    def _load_history(self):
        """Load trade history from disk if it exists."""
        if os.path.exists(self.history_path):
            try:
                with open(self.history_path, "r") as f:
                    data = json.load(f)
                self.trades = data.get("trades", [])
                self.peak_bankroll = data.get(
                    "peak_bankroll", self.peak_bankroll
                )
                logger.info(
                    f"DynamicKelly: loaded {len(self.trades)} historical trades"
                )
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"DynamicKelly: could not load history: {e}")
                self.trades = []

    def _save_history(self):
        """Persist trade history to disk."""
        os.makedirs(os.path.dirname(self.history_path), exist_ok=True)
        payload = {
            "trades": self.trades,
            "peak_bankroll": self.peak_bankroll,
        }
        try:
            with open(self.history_path, "w") as f:
                json.dump(payload, f, indent=2)
        except IOError as e:
            logger.error(f"DynamicKelly: failed to save history: {e}")

    # ── Recording trades ─────────────────────────────────────────

    def record_trade(self, pnl: float, category: str, won: bool):
        """
        Record a resolved trade outcome and update peak bankroll.

        Args:
            pnl: Profit/loss in dollars (positive = profit).
            category: Market category, e.g. "WEATHER", "BTC".
            won: Whether the trade was a win.
        """
        self.trades.append({
            "pnl": pnl,
            "category": category.upper(),
            "won": won,
        })
        # Update peak bankroll
        self.peak_bankroll = max(self.peak_bankroll, self.inner.bankroll)
        self._save_history()

    # ── Multiplier computations ──────────────────────────────────

    def _drawdown_multiplier(self) -> float:
        """
        Linear ramp: 1.0 at <=5% drawdown, 0.5 at >=15% drawdown.
        Formula: max(0.5, 1.0 - (dd - 0.05) / 0.20)

        Uses total_portfolio_value (bankroll + deployed) not just bankroll,
        so capital locked in open positions doesn't falsely trigger drawdown.
        """
        if self.peak_bankroll <= 0:
            return 1.0
        # Use total portfolio value if available, otherwise just bankroll
        current_value = getattr(self, '_portfolio_value', self.inner.bankroll)
        drawdown_pct = 1.0 - (current_value / self.peak_bankroll)
        if drawdown_pct <= 0.05:
            return 1.0
        return max(0.5, 1.0 - (drawdown_pct - 0.05) / 0.20)

    def _streak_multiplier(self) -> float:
        """
        Boost after 3+ consecutive wins (up to 1.25x).
        Reduce after 2+ consecutive losses.
        """
        if not self.trades:
            return 1.0

        # Walk backwards to find current streak
        streak_type = self.trades[-1]["won"]
        streak_len = 0
        for t in reversed(self.trades):
            if t["won"] == streak_type:
                streak_len += 1
            else:
                break

        if streak_type:  # winning streak
            if streak_len >= 3:
                # Linear ramp: 1.0 at 3, 1.25 at 6+
                return min(1.25, 1.0 + (streak_len - 3) * 0.083)
            return 1.0
        else:  # losing streak
            if streak_len >= 2:
                return max(0.5, 1.0 - streak_len * 0.1)
            return 1.0

    def _category_multiplier(self, category: str) -> float:
        """
        Boost categories with win rate >10% above overall.
        Reduce categories with win rate >10% below overall.
        Requires >= 5 trades in the category.
        """
        category = category.upper()
        if len(self.trades) < 5:
            return 1.0

        # Overall win rate
        total_wins = sum(1 for t in self.trades if t["won"])
        overall_wr = total_wins / len(self.trades)

        # Category win rate
        cat_trades = [t for t in self.trades if t["category"] == category]
        if len(cat_trades) < 5:
            return 1.0

        cat_wins = sum(1 for t in cat_trades if t["won"])
        cat_wr = cat_wins / len(cat_trades)

        diff = cat_wr - overall_wr
        if diff > 0.10:
            return 1.15
        elif diff < -0.10:
            return 0.85
        return 1.0

    def _sample_size_multiplier(self) -> float:
        """
        Discount when we have fewer than 10 total trades.
        Formula: 0.7 + (trade_count / 10) * 0.3
        Starts at 70% (not 50%) so small bankrolls can still trade.
        """
        n = len(self.trades)
        if n >= 10:
            return 1.0
        return 0.7 + (n / 10.0) * 0.3

    def _volatility_multiplier(self) -> float:
        """
        Scale Kelly by target_volatility / recent_volatility.
        Uses rolling stdev of P&L (as fraction of portfolio).
        Clamped to [0.5, 1.5].  Requires >= 10 trades.
        """
        recent = self.trades[-self.lookback:]
        if len(recent) < 10:
            return 1.0  # Not enough data — don't penalize

        # Use total portfolio value (not just USDC) for normalization
        portfolio_val = getattr(self, '_portfolio_value', self.peak_bankroll)
        bankroll = max(portfolio_val, 1.0)
        pnl_fracs = [t["pnl"] / bankroll for t in recent]

        mean = sum(pnl_fracs) / len(pnl_fracs)
        variance = sum((x - mean) ** 2 for x in pnl_fracs) / len(pnl_fracs)
        stdev = math.sqrt(variance) if variance > 0 else 0.0

        if stdev <= 0:
            return 1.0

        ratio = self.target_volatility / stdev
        return max(0.5, min(1.5, ratio))

    # ── Main sizing method ───────────────────────────────────────

    def size_position(
        self,
        forecast_prob: float,
        market_price: float,
        side: str = "BUY_YES",
        current_positions: int = 0,
        horizon_days: int = 0,
        market_liquidity: float = 1000.0,
        category: str = "WEATHER",
    ) -> KellyResult:
        """
        Compute position size via the inner KellySizer, then apply dynamic
        performance-based multipliers to the dollar amount.

        Accepts the same arguments as KellySizer.size_position, plus
        `category` for category-specific adjustments.

        Returns a KellyResult with bet_size_dollars adjusted by all
        multipliers.  The reason field includes multiplier breakdown.
        """
        # Update peak bankroll
        self.peak_bankroll = max(self.peak_bankroll, self.inner.bankroll)

        # Get base sizing from inner KellySizer
        base_result = self.inner.size_position(
            forecast_prob=forecast_prob,
            market_price=market_price,
            side=side,
            current_positions=current_positions,
            horizon_days=horizon_days,
            market_liquidity=market_liquidity,
        )

        # If inner sizer says SKIP, respect that
        if base_result.confidence == "SKIP" or base_result.bet_size_dollars <= 0:
            return base_result

        # Compute all multipliers
        dd_mult = self._drawdown_multiplier()
        streak_mult = self._streak_multiplier()
        cat_mult = self._category_multiplier(category)
        sample_mult = self._sample_size_multiplier()
        vol_mult = self._volatility_multiplier()

        combined = dd_mult * streak_mult * cat_mult * sample_mult * vol_mult

        adjusted_dollars = base_result.bet_size_dollars * combined
        adjusted_dollars = round(adjusted_dollars, 2)

        # Enforce minimum — if adjusted size drops below minimum, skip
        if adjusted_dollars < self.inner.min_position_dollars:
            return KellyResult(
                fraction=base_result.fraction,
                adjusted_fraction=base_result.adjusted_fraction * combined,
                bet_size_dollars=0.0,
                edge=base_result.edge,
                expected_value=base_result.expected_value,
                confidence="SKIP",
                reason=(
                    f"Dynamic adjustment reduced ${base_result.bet_size_dollars:.2f} "
                    f"to ${adjusted_dollars:.2f} (below min). "
                    f"Mults: dd={dd_mult:.2f} streak={streak_mult:.2f} "
                    f"cat={cat_mult:.2f} sample={sample_mult:.2f} vol={vol_mult:.2f}"
                ),
            )

        mult_detail = (
            f"dd={dd_mult:.2f} streak={streak_mult:.2f} cat={cat_mult:.2f} "
            f"sample={sample_mult:.2f} vol={vol_mult:.2f} → combined={combined:.2f}"
        )

        logger.debug(
            f"DynamicKelly: ${base_result.bet_size_dollars:.2f} × {combined:.2f} "
            f"= ${adjusted_dollars:.2f} [{mult_detail}]"
        )

        return KellyResult(
            fraction=base_result.fraction,
            adjusted_fraction=base_result.adjusted_fraction * combined,
            bet_size_dollars=adjusted_dollars,
            edge=base_result.edge,
            expected_value=base_result.expected_value,
            confidence=base_result.confidence,
            reason=f"{base_result.reason} | Dynamic: {mult_detail}",
        )
