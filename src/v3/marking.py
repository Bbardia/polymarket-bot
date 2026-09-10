"""Executable bid-side mark-to-market for paper positions (remediation item 2).

Marks are deliberately conservative:

* a leg is valued at the notional a market sell into the current bids would
  realise, minus the exit fee on those fills;
* depth is walked level by level, so shares beyond the resting bid depth are
  counted as ``unmarkable`` and valued at zero rather than skipped;
* a leg with no bid book at all is valued at zero and flagged.

The mark never rewrites ledger records; it is telemetry and a breaker input.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Mapping, Sequence

from .math import BookLevel, taker_fee

ZERO = Decimal("0")

# Legacy positions predate per-position fee provenance. Weather markets on the
# venue charge 5% x p x (1-p); using it for unknown legs errs conservative.
CONSERVATIVE_FEE_RATE = Decimal("0.05")


@dataclass(frozen=True)
class LegMark:
    key: str
    token_id: str
    shares: Decimal
    all_in_cost: Decimal
    marked_shares: Decimal
    unmarkable_shares: Decimal
    gross_notional: Decimal
    exit_fee: Decimal
    value: Decimal
    book_available: bool
    fee_rate: Decimal
    fee_rate_assumed: bool

    @property
    def partial(self) -> bool:
        return self.unmarkable_shares > ZERO

    def as_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "token_id": self.token_id,
            "shares": str(self.shares),
            "all_in_cost": str(self.all_in_cost),
            "marked_shares": str(self.marked_shares),
            "unmarkable_shares": str(self.unmarkable_shares),
            "gross_notional": str(self.gross_notional),
            "exit_fee": str(self.exit_fee),
            "value": str(self.value),
            "book_available": self.book_available,
            "partial": self.partial,
            "fee_rate": str(self.fee_rate),
            "fee_rate_assumed": self.fee_rate_assumed,
        }


def mark_leg(
    *,
    key: str,
    token_id: str,
    shares: Decimal,
    all_in_cost: Decimal,
    bids: Sequence[BookLevel] | Iterable[BookLevel] | None,
    fee_rate: Decimal | None,
) -> LegMark:
    """Depth-aware partial bid quote: fill what the book holds, zero the rest."""
    if shares <= ZERO:
        raise ValueError("shares must be positive")
    assumed = fee_rate is None
    rate = CONSERVATIVE_FEE_RATE if fee_rate is None else fee_rate
    if bids is None:
        return LegMark(
            key=key, token_id=token_id, shares=shares, all_in_cost=all_in_cost,
            marked_shares=ZERO, unmarkable_shares=shares, gross_notional=ZERO,
            exit_fee=ZERO, value=ZERO, book_available=False, fee_rate=rate,
            fee_rate_assumed=assumed,
        )
    remaining = shares
    notional = ZERO
    fee = ZERO
    for level in sorted(tuple(bids), key=lambda item: item.price, reverse=True):
        if remaining <= ZERO:
            break
        take = min(level.size, remaining)
        notional += take * level.price
        fee += taker_fee(shares=take, price=level.price, fee_rate=rate)
        remaining -= take
    marked = shares - remaining
    return LegMark(
        key=key, token_id=token_id, shares=shares, all_in_cost=all_in_cost,
        marked_shares=marked, unmarkable_shares=remaining, gross_notional=notional,
        exit_fee=fee, value=max(ZERO, notional - fee), book_available=True,
        fee_rate=rate, fee_rate_assumed=assumed,
    )


@dataclass(frozen=True)
class PortfolioMark:
    cash: Decimal
    mark_equity: Decimal
    position_value: Decimal
    gross_exposure: Decimal
    unresolved_stake: Decimal
    unmarkable_legs: int
    partial_legs: int
    legs: tuple[LegMark, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "cash": str(self.cash),
            "mark_equity": str(self.mark_equity),
            "position_value": str(self.position_value),
            "gross_exposure": str(self.gross_exposure),
            "unresolved_stake": str(self.unresolved_stake),
            "unmarkable_legs": self.unmarkable_legs,
            "partial_legs": self.partial_legs,
            "leg_count": len(self.legs),
        }


def mark_portfolio(*, cash: Decimal, legs: Iterable[LegMark]) -> PortfolioMark:
    legs_tuple = tuple(legs)
    position_value = sum((leg.value for leg in legs_tuple), ZERO)
    gross = sum((leg.all_in_cost for leg in legs_tuple), ZERO)
    return PortfolioMark(
        cash=cash,
        mark_equity=cash + position_value,
        position_value=position_value,
        gross_exposure=gross,
        unresolved_stake=gross,
        unmarkable_legs=sum(1 for leg in legs_tuple if not leg.book_available),
        partial_legs=sum(1 for leg in legs_tuple if leg.partial and leg.book_available),
        legs=legs_tuple,
    )


def position_legs(position_key: str, position: Mapping[str, object]) -> tuple[tuple[str, str, Decimal, Decimal], ...]:
    """Expand a stored paper position into (leg_key, token_id, shares, all_in_cost)."""
    if str(position.get("strategy", "")) == "weather_ladder":
        legs = []
        for index, leg in enumerate(position.get("legs", ()) or ()):
            legs.append((
                f"{position_key}:leg{index}",
                str(leg["token_id"]),
                Decimal(str(leg["shares"])),
                Decimal(str(leg["all_in_cost"])),
            ))
        return tuple(legs)
    token_id = position.get("token_id")
    if not token_id:
        return ()
    return ((
        position_key,
        str(token_id),
        Decimal(str(position["shares"])),
        Decimal(str(position["all_in_cost"])),
    ),)


def migrated_peak(
    *,
    stored_peak: Decimal | None,
    initial_cash: Decimal,
    peak_entry_equity: Decimal,
    current_mark_equity: Decimal,
) -> tuple[Decimal, bool]:
    """Safe high-water-mark migration.

    A missing persisted peak is never replaced by ``initial_cash`` alone: the
    true peak is at least the larger of the starting cash, the highest
    entry-cost equity ever recorded, and the current mark. Returns the peak and
    whether a migration occurred.
    """
    if stored_peak is not None:
        return max(stored_peak, current_mark_equity), False
    return max(initial_cash, peak_entry_equity, current_mark_equity), True
