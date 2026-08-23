"""Executable complete-set research evaluator; never submits either leg."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from ..math import BookLevel, CompleteSetOpportunity, complete_set_opportunity

ZERO = Decimal("0")


@dataclass(frozen=True)
class CompleteSetDecision:
    tradeable: bool
    reason: str
    opportunity: CompleteSetOpportunity | None
    paper_only: bool = True


def _book_levels(levels: Any) -> tuple[BookLevel, ...]:
    return tuple(BookLevel(Decimal(str(level.price)), Decimal(str(level.size))) for level in levels)


def evaluate_complete_set(
    *,
    yes_book: Any,
    no_book: Any,
    shares: Decimal,
    fee_rate: Decimal,
    min_net_return: Decimal,
) -> CompleteSetDecision:
    if yes_book.condition_id != no_book.condition_id:
        return CompleteSetDecision(False, "outcome books are not from the same condition", None)
    if bool(yes_book.neg_risk) or bool(no_book.neg_risk):
        return CompleteSetDecision(False, "negative-risk events require a separate adapter", None)
    minimum_size = max(Decimal(str(yes_book.min_order_size)), Decimal(str(no_book.min_order_size)))
    if shares < minimum_size:
        return CompleteSetDecision(False, "requested shares are below market minimum", None)
    try:
        opportunity = complete_set_opportunity(
            yes_asks=_book_levels(yes_book.asks),
            no_asks=_book_levels(no_book.asks),
            shares=shares,
            fee_rate=fee_rate,
        )
    except ValueError as exc:
        return CompleteSetDecision(False, str(exc), None)
    if opportunity.net_profit <= ZERO:
        return CompleteSetDecision(False, "complete set has no fee-adjusted profit", opportunity)
    if opportunity.return_on_cost < min_net_return:
        return CompleteSetDecision(False, "complete-set return is below minimum", opportunity)
    return CompleteSetDecision(True, "paper candidate", opportunity)
