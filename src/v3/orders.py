"""Fill-aware order aggregate for CLOB V2 order and trade events."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

ZERO = Decimal("0")


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

    def cancel(self) -> None:
        """Record exchange cancellation without performing an account action."""
        if self.state is OrderState.FILLED:
            raise ValueError("filled order cannot transition to canceled")
        self.state = OrderState.CANCELED

    def record_trade(
        self,
        trade_id: str,
        *,
        size: Decimal,
        price: Decimal,
        fee: Decimal,
        status: TradeStatus,
    ) -> None:
        if size <= ZERO or price <= ZERO or fee < ZERO:
            raise ValueError("invalid trade values")
        existing = self.trades.get(trade_id)
        if existing:
            if (existing.size, existing.price) != (size, price):
                raise ValueError("trade payload changed for an existing trade id")
            if existing.accounted and existing.fee != fee:
                raise ValueError("fee changed after confirmed trade accounting")
            if not existing.accounted:
                # The SDK can omit fee_rate_bps on early lifecycle events and
                # include it when the same trade reaches CONFIRMED.
                existing.fee = fee
                existing.status = status
            elif status is TradeStatus.CONFIRMED:
                existing.status = status
            record = existing
        else:
            record = TradeRecord(trade_id, size, price, fee, status)
            self.trades[trade_id] = record

        if status is TradeStatus.FAILED:
            return
        if status is not TradeStatus.CONFIRMED or record.accounted:
            return
        if self.confirmed_size + size > self.requested_size:
            raise ValueError("confirmed fill exceeds requested size")

        self.confirmed_size += size
        self.confirmed_notional += size * price
        self.confirmed_fees += fee
        record.accounted = True
        self.state = (
            OrderState.FILLED
            if self.confirmed_size == self.requested_size
            else OrderState.PARTIALLY_FILLED
        )
