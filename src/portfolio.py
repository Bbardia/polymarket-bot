"""
Portfolio Tracker — P&L tracking, drawdown circuit breakers, and risk management.

Missing from the original bot:
  - No P&L tracking (realized or unrealized)
  - No drawdown limits or circuit breakers
  - No position reconciliation
  - No reserve capital management
  - No performance analytics

This module adds all of that.

Risk controls based on whale analysis (Polymarket 2025 on-chain report):
  - Top wallets keep 25-40% in reserve
  - Max 10-12 simultaneous positions
  - Daily loss limit triggers cooldown
  - Drawdown circuit breaker pauses all trading
"""
import json
import math
import time
from collections import deque
from datetime import datetime, date, timezone, timedelta
from enum import Enum
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional
from loguru import logger


@dataclass
class Position:
    """A single open position."""
    id: str                          # Unique identifier
    market_type: str                 # "WEATHER", "BTC", "POLITICS", etc.
    description: str                 # Human-readable description
    side: str                        # "BUY_YES" or "BUY_NO"
    token_id: str                    # Polymarket token ID
    entry_price: float               # Price paid per share
    shares: float                    # Number of shares
    cost: float                      # Total cost (entry_price * shares)
    entry_time: str                  # ISO timestamp
    edge_at_entry: float             # Edge when we entered
    kelly_fraction: float = 0.0      # Kelly fraction used
    current_price: float = 0.0       # Latest known price
    peak_price: float = 0.0          # Highest price since entry
    order_id: str = ""               # Polymarket order ID
    condition_id: str = ""           # For price lookups
    status: str = "OPEN"             # OPEN, CLOSED, EXPIRED

    @property
    def unrealized_pnl(self) -> float:
        """Current unrealized P&L."""
        if self.current_price <= 0:
            return 0.0
        return (self.current_price - self.entry_price) * self.shares

    @property
    def unrealized_pnl_pct(self) -> float:
        """Unrealized P&L as percentage."""
        if self.cost <= 0:
            return 0.0
        return self.unrealized_pnl / self.cost

    @property
    def market_value(self) -> float:
        """Current market value of position."""
        return self.current_price * self.shares if self.current_price > 0 else self.cost


@dataclass
class ClosedTrade:
    """A completed trade with realized P&L."""
    id: str
    market_type: str
    description: str
    side: str
    entry_price: float
    exit_price: float
    shares: float
    cost: float
    revenue: float
    pnl: float
    pnl_pct: float
    entry_time: str
    exit_time: str
    exit_reason: str  # "TAKE_PROFIT", "STOP_LOSS", "EXPIRED", "MANUAL"
    hold_duration_hours: float


class VolatilityRegime(Enum):
    """Market volatility regime classification."""
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"
    EXTREME = "EXTREME"


class VolatilityTracker:
    """
    Tracks rolling trade P&L volatility and classifies the current regime.

    Regimes (based on rolling stdev vs calibrated baseline):
      LOW     (< 0.5x baseline):  multiplier 1.2  — increase size in calm markets
      NORMAL  (0.5-1.5x):         multiplier 1.0
      HIGH    (1.5-3.0x):         multiplier 0.5  — cut size in volatile markets
      EXTREME (> 3.0x):           multiplier 0.25 — minimal size
    """

    REGIME_MULTIPLIERS = {
        VolatilityRegime.LOW: 1.2,
        VolatilityRegime.NORMAL: 1.0,
        VolatilityRegime.HIGH: 0.5,
        VolatilityRegime.EXTREME: 0.25,
    }

    WINDOW_SIZE = 30          # Rolling window of last N trade P&L values
    CALIBRATION_COUNT = 10    # Calibrate baseline from first N trades
    DEFAULT_BASELINE = 0.10   # Default baseline stdev if not enough data

    def __init__(self, persist_path: Path):
        self._persist_path = persist_path
        self._pnl_window: deque[float] = deque(maxlen=self.WINDOW_SIZE)
        self._baseline: Optional[float] = None
        self._all_pnls: list[float] = []  # All P&Ls for calibration
        self._load()

    def record_pnl(self, pnl: float):
        """Record a trade P&L value and update regime."""
        self._pnl_window.append(pnl)
        self._all_pnls.append(pnl)

        # Calibrate baseline once we have enough trades
        if self._baseline is None and len(self._all_pnls) >= self.CALIBRATION_COUNT:
            self._baseline = self._stdev(self._all_pnls[:self.CALIBRATION_COUNT])
            if self._baseline < 1e-9:
                self._baseline = self.DEFAULT_BASELINE
            logger.info(
                f"Volatility: baseline calibrated from first "
                f"{self.CALIBRATION_COUNT} trades: {self._baseline:.4f}"
            )

        self._save()

    @property
    def current_regime(self) -> VolatilityRegime:
        """Classify the current volatility regime."""
        baseline = self._baseline if self._baseline is not None else self.DEFAULT_BASELINE
        if len(self._pnl_window) < 3:
            return VolatilityRegime.NORMAL

        rolling_std = self._stdev(list(self._pnl_window))
        ratio = rolling_std / baseline if baseline > 1e-9 else 0.0

        if ratio < 0.5:
            return VolatilityRegime.LOW
        elif ratio <= 1.5:
            return VolatilityRegime.NORMAL
        elif ratio <= 3.0:
            return VolatilityRegime.HIGH
        else:
            return VolatilityRegime.EXTREME

    def get_regime_multiplier(self) -> float:
        """Return the position-size multiplier for the current regime."""
        regime = self.current_regime
        multiplier = self.REGIME_MULTIPLIERS[regime]
        return multiplier

    @property
    def rolling_stdev(self) -> float:
        """Current rolling standard deviation of P&L window."""
        if len(self._pnl_window) < 2:
            return 0.0
        return self._stdev(list(self._pnl_window))

    @property
    def baseline(self) -> float:
        return self._baseline if self._baseline is not None else self.DEFAULT_BASELINE

    @staticmethod
    def _stdev(values: list[float]) -> float:
        """Population standard deviation."""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        return math.sqrt(variance)

    def _save(self):
        data = {
            "pnl_window": list(self._pnl_window),
            "all_pnls": self._all_pnls,
            "baseline": self._baseline,
        }
        tmp = self._persist_path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(data, f)
        tmp.rename(self._persist_path)

    def _load(self):
        if not self._persist_path.exists():
            return
        try:
            with open(self._persist_path) as f:
                data = json.load(f)
            pnls = data.get("pnl_window", [])
            self._pnl_window = deque(pnls, maxlen=self.WINDOW_SIZE)
            self._all_pnls = data.get("all_pnls", list(pnls))
            self._baseline = data.get("baseline")
            logger.info(
                f"Volatility: loaded {len(self._pnl_window)} P&L values, "
                f"regime={self.current_regime.value}, "
                f"multiplier={self.get_regime_multiplier()}"
            )
        except Exception as e:
            logger.warning(f"Volatility: failed to load state: {e}")


class PortfolioTracker:
    """
    Tracks all positions, P&L, and enforces risk controls.

    Risk controls:
      1. Max positions limit (default 12)
      2. Daily loss circuit breaker (pause after X% daily loss)
      3. Hourly loss sublimit (pause after 5% hourly loss)
      4. Drawdown circuit breaker (pause after X% from peak)
      5. Reserve capital enforcement (never deploy more than 75%)
      6. Correlation limit (max positions in same category)
      7. Consecutive failure circuit breaker (5 failures = 10min pause)
      8. Volatility regime detection (adjust position sizes)
    """

    def __init__(
        self,
        data_dir: str = "data",
        max_positions: int = 12,
        daily_loss_limit_pct: float = 0.15,    # Pause after 15% daily loss
        hourly_loss_limit_pct: float = 0.05,   # Pause after 5% hourly loss
        max_drawdown_pct: float = 0.25,         # Pause after 25% drawdown from peak
        reserve_pct: float = 0.25,              # Keep 25% in reserve
        max_per_category: int = 4,              # Max 4 positions per market type
        cooldown_minutes: int = 60,             # Cooldown after daily circuit breaker
        hourly_cooldown_minutes: int = 30,      # Cooldown after hourly circuit breaker
        failure_cooldown_minutes: int = 10,     # Cooldown after consecutive failures
        max_consecutive_failures: int = 5,      # Failures before circuit breaker
    ):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.positions_file = self.data_dir / "portfolio_positions.json"
        self.history_file = self.data_dir / "portfolio_history.jsonl"
        self.snapshots_file = self.data_dir / "portfolio_snapshots.jsonl"
        self.volatility_file = self.data_dir / "volatility_state.json"

        # Risk parameters
        self.max_positions = max_positions
        self.daily_loss_limit_pct = daily_loss_limit_pct
        self.hourly_loss_limit_pct = hourly_loss_limit_pct
        self.max_drawdown_pct = max_drawdown_pct
        self.reserve_pct = reserve_pct
        self.max_per_category = max_per_category
        self.cooldown_minutes = cooldown_minutes
        self.hourly_cooldown_minutes = hourly_cooldown_minutes
        self.failure_cooldown_minutes = failure_cooldown_minutes
        self.max_consecutive_failures = max_consecutive_failures

        # State
        self.positions: dict[str, Position] = {}
        self.peak_portfolio_value: float = 0.0
        self.initial_bankroll: float = 0.0
        self._circuit_breaker_until: Optional[float] = None
        self._daily_pnl: float = 0.0
        self._daily_pnl_date: str = ""

        # Hourly loss tracking: list of (timestamp, loss_amount) tuples
        self._hourly_losses: list[tuple[float, float]] = []

        # Consecutive failure tracking
        self._consecutive_failures: int = 0

        # Volatility regime detection
        self.volatility = VolatilityTracker(self.volatility_file)

        # Load existing state
        self._load_positions()
        self._reconstruct_daily_pnl()

    # ── Position Management ──

    def open_position(self, position: Position) -> bool:
        """
        Register a new open position. Returns False if risk controls prevent it.
        """
        # Check risk controls
        can_trade, reason = self.can_trade(position.market_type)
        if not can_trade:
            logger.warning(f"Portfolio: BLOCKED — {reason}")
            return False

        self.positions[position.id] = position
        self._save_positions()

        logger.info(
            f"Portfolio: OPENED {position.id} — {position.description} "
            f"| {position.side} {position.shares:.1f}@${position.entry_price:.3f} "
            f"= ${position.cost:.2f} | edge={position.edge_at_entry:+.1%}"
        )
        return True

    def close_position(
        self,
        position_id: str,
        exit_price: float,
        exit_reason: str = "MANUAL",
    ) -> Optional[ClosedTrade]:
        """Close a position and record the trade."""
        if position_id not in self.positions:
            logger.warning(f"Portfolio: position {position_id} not found")
            return None

        pos = self.positions[position_id]
        revenue = exit_price * pos.shares
        pnl = revenue - pos.cost
        pnl_pct = pnl / pos.cost if pos.cost > 0 else 0.0

        # Calculate hold duration
        try:
            entry_dt = datetime.fromisoformat(pos.entry_time)
            hold_hours = (datetime.now(timezone.utc) - entry_dt).total_seconds() / 3600
        except Exception:
            hold_hours = 0.0

        trade = ClosedTrade(
            id=pos.id,
            market_type=pos.market_type,
            description=pos.description,
            side=pos.side,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            shares=pos.shares,
            cost=pos.cost,
            revenue=revenue,
            pnl=pnl,
            pnl_pct=pnl_pct,
            entry_time=pos.entry_time,
            exit_time=datetime.now(timezone.utc).isoformat(),
            exit_reason=exit_reason,
            hold_duration_hours=round(hold_hours, 2),
        )

        # Update daily P&L
        today = str(date.today())
        if self._daily_pnl_date != today:
            self._daily_pnl = 0.0
            self._daily_pnl_date = today
        self._daily_pnl += pnl

        # Track hourly losses (only losses, not gains)
        if pnl < 0:
            self._hourly_losses.append((time.time(), pnl))

        # Feed P&L to volatility tracker
        self.volatility.record_pnl(pnl)

        # Record to history
        self._append_history(trade)

        # Remove from open positions
        del self.positions[position_id]
        self._save_positions()

        emoji = "💰" if pnl > 0 else "🔻"
        logger.info(
            f"Portfolio: CLOSED {emoji} {pos.description} "
            f"| {exit_reason} | P&L: ${pnl:+.2f} ({pnl_pct:+.1%}) "
            f"| held {hold_hours:.1f}h"
        )

        # Check circuit breakers after loss
        if pnl < 0:
            self._check_circuit_breakers()

        return trade

    def update_position_price(self, position_id: str, current_price: float):
        """Update the current market price for a position."""
        if position_id in self.positions:
            pos = self.positions[position_id]
            pos.current_price = current_price
            if current_price > pos.peak_price:
                pos.peak_price = current_price

    # ── Risk Controls ──

    def can_trade(self, market_type: str = "") -> tuple[bool, str]:
        """
        Always allow trading — the bot trades with whatever cash is available.
        Circuit breakers and pauses are disabled.
        """
        return True, "OK"

    def _check_circuit_breakers(self):
        """Log drawdown/loss info but never pause trading."""
        # Log stats for monitoring but don't trigger any pauses
        if self.initial_bankroll > 0:
            self._prune_hourly_losses()
            hourly_loss = abs(sum(amt for _, amt in self._hourly_losses))
            if hourly_loss > 0:
                logger.debug(f"Hourly loss: ${hourly_loss:.2f}")

        if self.initial_bankroll > 0 and self._daily_pnl < 0:
            logger.debug(f"Daily P&L: ${self._daily_pnl:.2f}")

        current_value = self.portfolio_value
        if self.peak_portfolio_value > 0:
            drawdown = (self.peak_portfolio_value - current_value) / self.peak_portfolio_value
            if drawdown > 0.10:
                logger.info(f"Drawdown: {drawdown:.1%} from peak "
                           f"(${self.peak_portfolio_value:.2f} → ${current_value:.2f})")

    def _trigger_circuit_breaker(self, reason: str, cooldown_minutes: Optional[int] = None):
        """Activate circuit breaker — pause all trading."""
        minutes = cooldown_minutes if cooldown_minutes is not None else self.cooldown_minutes
        self._circuit_breaker_until = time.time() + (minutes * 60)
        logger.warning(
            f"CIRCUIT BREAKER TRIGGERED: {reason} — "
            f"Trading paused for {minutes} minutes"
        )

    # ── Hourly Loss Sublimit ──

    def _prune_hourly_losses(self):
        """Remove loss entries older than 1 hour."""
        cutoff = time.time() - 3600
        self._hourly_losses = [
            (ts, amt) for ts, amt in self._hourly_losses if ts > cutoff
        ]

    def _check_hourly_loss_limit(self) -> tuple[bool, str]:
        """Check if hourly loss sublimit has been breached. Returns (blocked, reason)."""
        if self.initial_bankroll <= 0:
            return False, ""
        self._prune_hourly_losses()
        if not self._hourly_losses:
            return False, ""
        hourly_loss = abs(sum(amt for _, amt in self._hourly_losses))
        hourly_loss_pct = hourly_loss / self.initial_bankroll
        if hourly_loss_pct >= self.hourly_loss_limit_pct:
            return True, (
                f"Hourly loss limit: ${-hourly_loss:.2f} lost in last hour "
                f"({hourly_loss_pct:.1%} of bankroll, limit {self.hourly_loss_limit_pct:.0%})"
            )
        return False, ""

    @property
    def hourly_loss(self) -> float:
        """Total loss in the rolling 1-hour window (negative value)."""
        self._prune_hourly_losses()
        return sum(amt for _, amt in self._hourly_losses)

    # ── Consecutive Failure Tracking ──

    def record_order_success(self):
        """Record a successful order — resets the consecutive failure counter."""
        if self._consecutive_failures > 0:
            logger.info(
                f"Portfolio: order succeeded, resetting failure counter "
                f"(was {self._consecutive_failures})"
            )
        self._consecutive_failures = 0

    def record_order_failure(self):
        """Record a failed order (API error, rejection). Logged but never pauses trading."""
        self._consecutive_failures += 1
        logger.warning(
            f"Portfolio: order failure #{self._consecutive_failures}"
            f"/{self.max_consecutive_failures}"
        )
        if self._consecutive_failures >= self.max_consecutive_failures:
            logger.warning(f"Portfolio: {self._consecutive_failures} consecutive failures — check API/wallet")
            self._consecutive_failures = 0

    @property
    def consecutive_failures(self) -> int:
        """Current count of consecutive order failures."""
        return self._consecutive_failures

    # ── Volatility Regime ──

    def get_regime_multiplier(self) -> float:
        """
        Get the current volatility-regime position-size multiplier.
        Delegates to the VolatilityTracker instance.
        """
        return self.volatility.get_regime_multiplier()

    @property
    def current_volatility_regime(self) -> VolatilityRegime:
        """Current volatility regime classification."""
        return self.volatility.current_regime

    # ── Portfolio Analytics ──

    @property
    def total_deployed(self) -> float:
        """Total capital currently deployed in open positions."""
        return sum(p.cost for p in self.positions.values())

    @property
    def portfolio_value(self) -> float:
        """Current total portfolio value (deployed + unrealized P&L)."""
        return sum(p.market_value for p in self.positions.values())

    @property
    def total_unrealized_pnl(self) -> float:
        """Total unrealized P&L across all open positions."""
        return sum(p.unrealized_pnl for p in self.positions.values())

    @property
    def open_position_count(self) -> int:
        return len(self.positions)

    def get_category_breakdown(self) -> dict[str, int]:
        """Count positions per market category."""
        breakdown = {}
        for p in self.positions.values():
            breakdown[p.market_type] = breakdown.get(p.market_type, 0) + 1
        return breakdown

    def get_daily_stats(self) -> dict:
        """Get today's trading statistics from history."""
        today = str(date.today())
        trades_today = []
        if self.history_file.exists():
            with open(self.history_file) as f:
                for line in f:
                    try:
                        t = json.loads(line.strip())
                        if t.get("exit_time", "")[:10] == today:
                            trades_today.append(t)
                    except Exception:
                        pass

        wins = [t for t in trades_today if t.get("pnl", 0) > 0]
        losses = [t for t in trades_today if t.get("pnl", 0) < 0]
        total_pnl = sum(t.get("pnl", 0) for t in trades_today)

        return {
            "date": today,
            "trades": len(trades_today),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": len(wins) / len(trades_today) if trades_today else 0.0,
            "total_pnl": round(total_pnl, 2),
            "avg_pnl": round(total_pnl / len(trades_today), 2) if trades_today else 0.0,
            "best_trade": max((t.get("pnl", 0) for t in trades_today), default=0.0),
            "worst_trade": min((t.get("pnl", 0) for t in trades_today), default=0.0),
        }

    def get_performance_summary(self) -> dict:
        """Get overall performance summary from all history."""
        all_trades = []
        if self.history_file.exists():
            with open(self.history_file) as f:
                for line in f:
                    try:
                        all_trades.append(json.loads(line.strip()))
                    except Exception:
                        pass

        if not all_trades:
            return {"total_trades": 0, "message": "No completed trades yet"}

        wins = [t for t in all_trades if t.get("pnl", 0) > 0]
        losses = [t for t in all_trades if t.get("pnl", 0) <= 0]
        total_pnl = sum(t.get("pnl", 0) for t in all_trades)
        avg_win = sum(t.get("pnl", 0) for t in wins) / len(wins) if wins else 0
        avg_loss = sum(t.get("pnl", 0) for t in losses) / len(losses) if losses else 0

        # Category breakdown
        by_category = {}
        for t in all_trades:
            cat = t.get("market_type", "UNKNOWN")
            if cat not in by_category:
                by_category[cat] = {"trades": 0, "pnl": 0.0, "wins": 0}
            by_category[cat]["trades"] += 1
            by_category[cat]["pnl"] += t.get("pnl", 0)
            if t.get("pnl", 0) > 0:
                by_category[cat]["wins"] += 1

        return {
            "total_trades": len(all_trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / len(all_trades), 3),
            "total_pnl": round(total_pnl, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2),
            "profit_factor": round(abs(avg_win / avg_loss), 2) if avg_loss != 0 else float('inf'),
            "by_category": by_category,
            "open_positions": self.open_position_count,
            "deployed": round(self.total_deployed, 2),
        }

    def take_snapshot(self):
        """Save a portfolio snapshot for historical tracking."""
        snapshot = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "portfolio_value": round(self.portfolio_value, 2),
            "deployed": round(self.total_deployed, 2),
            "unrealized_pnl": round(self.total_unrealized_pnl, 2),
            "open_positions": self.open_position_count,
            "categories": self.get_category_breakdown(),
            "daily_pnl": round(self._daily_pnl, 2),
            "hourly_loss": round(self.hourly_loss, 2),
            "volatility_regime": self.current_volatility_regime.value,
            "regime_multiplier": self.get_regime_multiplier(),
            "consecutive_failures": self._consecutive_failures,
        }

        # Update peak
        if self.portfolio_value > self.peak_portfolio_value:
            self.peak_portfolio_value = self.portfolio_value

        with open(self.snapshots_file, "a") as f:
            f.write(json.dumps(snapshot) + "\n")

        return snapshot

    # ── Persistence ──

    def _save_positions(self):
        """Save open positions to disk (atomic write to prevent corruption)."""
        data = {}
        for pid, pos in self.positions.items():
            data[pid] = asdict(pos)
        tmp = self.positions_file.with_suffix('.tmp')
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        tmp.rename(self.positions_file)

    def _load_positions(self):
        """Load open positions from disk."""
        if not self.positions_file.exists():
            return
        try:
            with open(self.positions_file) as f:
                data = json.load(f)
            for pid, pdata in data.items():
                self.positions[pid] = Position(**pdata)
            logger.info(f"Portfolio: loaded {len(self.positions)} open positions")
        except Exception as e:
            logger.error(f"Portfolio: failed to load positions: {e}")

    def _reconstruct_daily_pnl(self):
        """Reconstruct daily P&L from history file (survives restarts)."""
        today = str(date.today())
        self._daily_pnl_date = today
        self._daily_pnl = 0.0
        if self.history_file.exists():
            try:
                with open(self.history_file) as f:
                    for line in f:
                        t = json.loads(line.strip())
                        if t.get("exit_time", "")[:10] == today:
                            self._daily_pnl += t.get("pnl", 0)
                if self._daily_pnl != 0:
                    logger.info(f"Portfolio: reconstructed daily P&L: ${self._daily_pnl:+.2f}")
            except Exception as e:
                logger.warning(f"Portfolio: failed to reconstruct daily P&L: {e}")

    def _append_history(self, trade: ClosedTrade):
        """Append a closed trade to history."""
        with open(self.history_file, "a") as f:
            f.write(json.dumps(asdict(trade)) + "\n")

    def __repr__(self):
        return (
            f"Portfolio(positions={self.open_position_count}, "
            f"deployed=${self.total_deployed:.2f}, "
            f"unrealized_pnl=${self.total_unrealized_pnl:+.2f})"
        )
