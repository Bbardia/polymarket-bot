"""Chronological replay of confirmed managed order fills."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from .orders import TradeStatus
from .streaming import _venue_match_time

ZERO = Decimal("0")


@dataclass(frozen=True)
class ConfirmedFill:
    order_id: str
    token_id: str
    side: str
    trade_id: str
    matched_at: datetime
    ledger_index: int
    size: Decimal
    price: Decimal
    fee: Decimal

    @property
    def notional(self) -> Decimal:
        return self.size * self.price


def confirmed_fills(processor: Any) -> tuple[ConfirmedFill, ...]:
    """Bind each accounted aggregate fill to exactly one durable trade row.

    Sort on venue match time, using append-only ledger order as a stable tie
    breaker. Any mismatch between the ledger and replayed aggregates blocks
    accounting rather than guessing an order or timestamp.
    """
    orders = processor.orders
    by_key: dict[tuple[str, str], list[ConfirmedFill]] = {}
    unique_trades: dict[str, tuple[dict[str, Any], int]] = {}
    for index, event in enumerate(processor.ledger.events()):
        if event.event_type != "user.trade" or event.payload.get("status") != "CONFIRMED":
            continue
        payload = event.payload
        trade_id = payload.get("id")
        if not isinstance(trade_id, str) or not trade_id:
            raise ValueError("confirmed trade has no stable trade ID")
        previous = unique_trades.get(trade_id)
        if previous is not None:
            prior, first_index = previous
            left, right = dict(prior), dict(payload)
            left_market, right_market = left.pop("market", None), right.pop("market", None)
            if left != right or (left_market is not None and right_market is not None and left_market != right_market):
                raise ValueError("duplicate confirmed trade ID has conflicting ledger rows")
            if left_market is None and right_market is not None:
                unique_trades[trade_id] = (dict(payload), first_index)
            continue
        unique_trades[trade_id] = (dict(payload), index)
    for trade_id, (payload, index) in unique_trades.items():
        matched_at = _venue_match_time(payload)
        if matched_at is None:
            raise ValueError("confirmed trade has no explicit valid venue match time")
        targets: list[tuple[str, Any, str]] = []
        taker_id = payload.get("taker_order_id")
        if isinstance(taker_id, str) and taker_id in orders:
            targets.append((taker_id, payload, "size"))
        maker_rows = payload.get("maker_orders", ())
        if not isinstance(maker_rows, (list, tuple)):
            raise ValueError("confirmed trade maker associations are malformed")
        for row in maker_rows:
            if not isinstance(row, dict):
                raise ValueError("confirmed trade maker association is malformed")
            order_id = row.get("order_id", row.get("id"))
            if isinstance(order_id, str) and order_id in orders:
                targets.append((order_id, row, "matched_amount"))
        if len({order_id for order_id, _, _ in targets}) != len(targets):
            raise ValueError("confirmed trade repeats a managed order association")
        for order_id, row, size_key in targets:
            order = orders[order_id]
            record = order.trades.get(trade_id)
            if record is None or not record.accounted or record.status is not TradeStatus.CONFIRMED:
                raise ValueError("confirmed trade row is not accounted by its managed order")
            try:
                size = Decimal(str(row[size_key]))
                price = Decimal(str(row["price"]))
                token_id = row.get("asset_id", row.get("token_id", payload.get("asset_id", payload.get("token_id"))))
                side = row.get("side", payload.get("side"))
            except (ArithmeticError, KeyError, TypeError, ValueError) as exc:
                raise ValueError("confirmed trade economics are malformed") from exc
            if (
                not isinstance(token_id, str) or token_id != order.token_id
                or side != order.side
                or not size.is_finite() or size <= ZERO
                or not price.is_finite() or price <= ZERO
                or (record.size, record.price) != (size, price)
                or not record.fee.is_finite() or record.fee < ZERO
            ):
                raise ValueError("confirmed trade economics conflict with managed fill")
            fill = ConfirmedFill(
                order_id=order_id, token_id=order.token_id, side=order.side,
                trade_id=trade_id, matched_at=matched_at, ledger_index=index,
                size=size, price=price, fee=record.fee,
            )
            by_key.setdefault((order_id, trade_id), []).append(fill)

    fills: list[ConfirmedFill] = []
    for order_id, order in orders.items():
        for trade_id, record in order.trades.items():
            if record.accounted:
                matches = by_key.get((order_id, trade_id), ())
                if len(matches) != 1:
                    raise ValueError("accounted fill lacks unique confirmed ledger chronology")
                fills.append(matches[0])
            elif record.status is TradeStatus.CONFIRMED:
                raise ValueError("confirmed fill is not durably accounted")
    return tuple(sorted(fills, key=lambda fill: (fill.matched_at, fill.ledger_index, fill.order_id)))
