"""Fill-aware order aggregate for CLOB V2 order and trade events."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum

ZERO = Decimal("0")


class OrderReconciliationRequired(ValueError):
    """A valid-looking lifecycle event conflicts with order state."""


class OrderState(str, Enum):
    CREATED = "CREATED"
    LIVE = "LIVE"
    DELAYED = "DELAYED"
    MATCHED = "MATCHED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    FAILED = "FAILED"


class TradeStatus(str, Enum):
    MATCHED = "MATCHED"
    MATCHED_NOT_BROADCASTED = "MATCHED_NOT_BROADCASTED"
    MINED = "MINED"
    CONFIRMED = "CONFIRMED"
    RETRYING = "RETRYING"
    FAILED = "FAILED"


@dataclass
class TradeRecord:
    trade_id: str
    size: Decimal
    price: Decimal
    fee: Decimal
    status: TradeStatus
    accounted: bool = False


@dataclass
class OrderAggregate:
    client_order_id: str
    token_id: str
    side: str
    requested_size: Decimal
    order_id: str | None = None
    state: OrderState = OrderState.CREATED
    confirmed_size: Decimal = ZERO
    confirmed_notional: Decimal = ZERO
    confirmed_fees: Decimal = ZERO
    trades: dict[str, TradeRecord] = field(default_factory=dict)
    canceled_at: datetime | None = None

    @classmethod
    def new(cls, *, client_order_id: str, token_id: str, side: str, requested_size: Decimal) -> "OrderAggregate":
        if requested_size <= ZERO:
            raise ValueError("requested size must be positive")
        if side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        return cls(client_order_id, token_id, side, requested_size)

    @property
    def average_fill_price(self) -> Decimal | None:
        if self.confirmed_size == ZERO:
            return None
        return self.confirmed_notional / self.confirmed_size

    def accept(self, *, order_id: str, status: str) -> None:
        mapping = {
            "live": OrderState.LIVE,
            "unmatched": OrderState.LIVE,
            "delayed": OrderState.DELAYED,
            "matched": OrderState.MATCHED,
        }
        if status not in mapping:
            raise ValueError(f"unsupported accepted order status: {status}")
        if self.order_id and self.order_id != order_id:
            raise ValueError("exchange order id cannot change")
        self.order_id = order_id
        self.state = mapping[status]

    def cancel(self, *, canceled_at: datetime | None = None) -> None:
        """Record exchange cancellation without performing an account action.

        A confirmed fill may arrive after this message only when its verified
        match time is strictly earlier than the cancellation time.
        """
        if self.state is OrderState.FILLED:
            raise ValueError("filled order cannot transition to canceled")
        if canceled_at is not None and (
            not isinstance(canceled_at, datetime)
            or canceled_at.tzinfo is None
            or canceled_at.utcoffset() is None
        ):
            raise ValueError("cancellation time must be timezone-aware")
        if self.state is not OrderState.CANCELED:
            self.state = OrderState.CANCELED
            self.canceled_at = canceled_at
        elif canceled_at is not None and (
            self.canceled_at is None or canceled_at < self.canceled_at
        ):
            self.canceled_at = canceled_at

    def record_trade(
        self,
        trade_id: str,
        *,
        size: Decimal,
        price: Decimal,
        fee: Decimal,
        status: TradeStatus,
        matched_at: datetime | None = None,
    ) -> None:
        self.validate_trade(
            trade_id, size=size, price=price, fee=fee, status=status,
            matched_at=matched_at,
        )
        existing = self.trades.get(trade_id)
        if existing:
            if not existing.accounted:
                # Early lifecycle events may omit fee_rate_bps.
                existing.fee = fee
                existing.status = status
            elif status is TradeStatus.CONFIRMED:
                existing.status = status
            record = existing
        else:
            record = TradeRecord(trade_id, size, price, fee, status)
            self.trades[trade_id] = record

        if status is TradeStatus.CONFIRMED and not record.accounted:
            was_canceled = self.state is OrderState.CANCELED
            self.confirmed_size += size
            self.confirmed_notional += size * price
            self.confirmed_fees += fee
            record.accounted = True
            if self.confirmed_size == self.requested_size:
                self.state = OrderState.FILLED
            elif not was_canceled:
                self.state = OrderState.PARTIALLY_FILLED

    def validate_trade(
        self,
        trade_id: str,
        *,
        size: Decimal,
        price: Decimal,
        fee: Decimal,
        status: TradeStatus,
        matched_at: datetime | None = None,
    ) -> None:
        """Validate trade economics and chronology without changing aggregate state."""
        if size <= ZERO or price <= ZERO or fee < ZERO:
            raise ValueError("invalid trade values")
        if self.state is OrderState.CANCELED and status is TradeStatus.CONFIRMED:
            if (
                self.canceled_at is None
                or not isinstance(matched_at, datetime)
                or matched_at.tzinfo is None
                or matched_at.utcoffset() is None
                or matched_at >= self.canceled_at
            ):
                raise OrderReconciliationRequired("confirmed trade after cancellation requires reconciliation")
        existing = self.trades.get(trade_id)
        if existing:
            if (existing.size, existing.price) != (size, price):
                raise ValueError("trade payload changed for an existing trade id")
            if existing.accounted and existing.fee != fee:
                raise ValueError("fee changed after confirmed trade accounting")
        if (
            status is TradeStatus.CONFIRMED
            and not (existing and existing.accounted)
            and self.confirmed_size + size > self.requested_size
        ):
            raise ValueError("confirmed fill exceeds requested size")
