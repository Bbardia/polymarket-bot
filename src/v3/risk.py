"""Hard, account-level risk limits for V3 entry orders."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from .math import is_tick_aligned

ZERO = Decimal("0")
ONE = Decimal("1")


@dataclass(frozen=True)
class RiskLimits:
    max_capital: Decimal
    reserve_fraction: Decimal
    max_order_notional: Decimal
    max_event_exposure: Decimal
    max_open_orders: int
    max_positions: int
    daily_loss_limit: Decimal
    max_drawdown_fraction: Decimal
    max_quote_age_seconds: int
    max_order_ttl_seconds: int

    def __post_init__(self) -> None:
        if self.max_capital <= ZERO or self.max_order_notional <= ZERO:
            raise ValueError("capital and order limits must be positive")
        if not (ZERO <= self.reserve_fraction < ONE):
            raise ValueError("reserve fraction must be in [0, 1)")
        if not (ZERO < self.max_drawdown_fraction < ONE):
            raise ValueError("max drawdown fraction must be in (0, 1)")


@dataclass(frozen=True)
class AccountRiskState:
    equity: Decimal
    cash: Decimal
    total_exposure: Decimal
    event_exposure: dict[str, Decimal] = field(default_factory=dict)
    open_orders: int = 0
    open_positions: int = 0
    daily_pnl: Decimal = ZERO
    peak_equity: Decimal = ZERO
    reconciled: bool = False
    unknown_remote_positions: int = 0
    unknown_remote_orders: int = 0


@dataclass(frozen=True)
class OrderIntent:
    condition_id: str
    token_id: str
    side: str
    price: Decimal
    shares: Decimal
    estimated_fee: Decimal
    post_only: bool
    ttl_seconds: int
    quote_age_seconds: int
    tick_size: Decimal = Decimal("0.01")
    min_order_size: Decimal = Decimal("5")
    market_accepting_orders: bool = True
    rules_verified: bool = True
    disputed: bool = False

    @property
    def all_in_notional(self) -> Decimal:
        return self.price * self.shares + self.estimated_fee


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    reason: str
    capital_base: Decimal
    deployable_capital: Decimal
    order_notional: Decimal


class RiskEngine:
    def __init__(self, limits: RiskLimits) -> None:
        self.limits = limits

    def evaluate(self, intent: OrderIntent, state: AccountRiskState) -> RiskDecision:
        capital_base = min(max(state.equity, ZERO), self.limits.max_capital)
        deployable = capital_base * (ONE - self.limits.reserve_fraction)
        notional = intent.all_in_notional

        def reject(reason: str) -> RiskDecision:
            return RiskDecision(False, reason, capital_base, deployable, notional)

        if not state.reconciled:
            return reject("account state is not reconciled")
        if state.unknown_remote_positions:
            return reject("unknown remote positions must be acknowledged")
        if state.unknown_remote_orders:
            return reject("unknown remote orders must be reconciled")
        if intent.side not in {"BUY", "SELL"} or intent.price <= ZERO or intent.shares <= ZERO:
            return reject("invalid order intent")
        if not intent.market_accepting_orders:
            return reject("market is not accepting orders")
        if not intent.rules_verified:
            return reject("market rules or resolution source are unverified")
        if intent.disputed:
            return reject("market is disputed")
        if not is_tick_aligned(intent.price, intent.tick_size):
            return reject("order price is not aligned to market tick size")
        if intent.shares < intent.min_order_size:
            return reject("order size is below market minimum")
        if not intent.post_only:
            return reject("initial V3 live entries must be post-only")
        if intent.quote_age_seconds > self.limits.max_quote_age_seconds:
            return reject("quote is stale")
        if intent.ttl_seconds <= 0 or intent.ttl_seconds > self.limits.max_order_ttl_seconds:
            return reject("order TTL exceeds configured limit")
        if state.open_orders >= self.limits.max_open_orders:
            return reject("maximum open orders reached")
        if state.open_positions >= self.limits.max_positions:
            return reject("maximum positions reached")
        if state.daily_pnl <= -self.limits.daily_loss_limit:
            return reject("daily loss limit reached")
        if state.peak_equity > ZERO:
            drawdown = (state.peak_equity - state.equity) / state.peak_equity
            if drawdown >= self.limits.max_drawdown_fraction:
                return reject("maximum drawdown reached")
        if notional > self.limits.max_order_notional:
            return reject("order exceeds max order notional")
        if state.cash < notional:
            return reject("insufficient cash")
        if state.event_exposure.get(intent.condition_id, ZERO) + notional > self.limits.max_event_exposure:
            return reject("order exceeds max event exposure")
        if state.total_exposure + notional > deployable:
            return reject("order exceeds deployable capital after reserve")
        return RiskDecision(True, "OK", capital_base, deployable, notional)
