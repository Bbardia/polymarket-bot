"""Coherent de-vigged ladder probability mass function (remediation item 7).

Per event: raw price per rung ``p_i = (b_i + a_i)/2`` when two-sided, ``a_i/2``
when ask-only, ``b_i`` when bid-only; de-vig by the power method, solving
``sum_i p_i**lambda = 1`` for lambda by bisection, ``q_i = p_i**lambda / sum``.

Coverage policy: the pmf is only ``complete`` when at least ``min_priced`` of
``total_rungs`` rungs carry a price; otherwise the missing mass is reported and
the pmf is flagged and must not feed a decision. The censoring of rungs by
``min_price``/``max_price`` belongs to the trade filter, never to pmf
construction, so callers must pass every rung of the partition here.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Sequence

ZERO = Decimal("0")
ONE = Decimal("1")


@dataclass(frozen=True)
class RungQuote:
    label: str
    bid: Decimal | None
    ask: Decimal | None

    def raw_price(self) -> Decimal | None:
        if self.bid is not None and self.ask is not None:
            return (self.bid + self.ask) / Decimal("2")
        if self.ask is not None:
            return self.ask / Decimal("2")
        if self.bid is not None:
            return self.bid
        return None


@dataclass(frozen=True)
class LadderPMF:
    q: Mapping[str, Decimal]
    raw: Mapping[str, Decimal]
    lambda_power: Decimal
    priced_rungs: int
    total_rungs: int
    missing_rungs: tuple[str, ...]
    raw_sum: Decimal
    complete: bool
    reason: str

    @property
    def missing_mass_estimate(self) -> Decimal:
        """Mass the raw prices leave unexplained (negative when overround)."""
        return ONE - self.raw_sum


def _power_lambda(prices: Sequence[Decimal], *, tolerance: Decimal = Decimal("1e-9")) -> Decimal:
    """Bisection for lambda with sum p_i^lambda = 1 (prices in (0, 1))."""
    low, high = Decimal("0.01"), Decimal("100")

    def total(power: Decimal) -> Decimal:
        return sum((Decimal(str(float(p) ** float(power))) for p in prices), ZERO)

    # sum p^lambda decreases in lambda for p in (0,1).
    if total(low) < ONE:
        return low
    if total(high) > ONE:
        return high
    for _ in range(200):
        mid = (low + high) / Decimal("2")
        value = total(mid)
        if abs(value - ONE) <= tolerance:
            return mid
        if value > ONE:
            low = mid
        else:
            high = mid
    return (low + high) / Decimal("2")


def build_ladder_pmf(
    rungs: Sequence[RungQuote],
    *,
    min_priced: int = 9,
    total_rungs: int | None = None,
) -> LadderPMF:
    total = total_rungs if total_rungs is not None else len(rungs)
    if total != len(rungs) or len({r.label for r in rungs}) != len(rungs):
        raise ValueError("provide every uniquely labelled rung of the partition")
    if min_priced < 9 or min_priced < (9 * total + 10) // 11:
        raise ValueError("coverage must be at least 9/11")
    for rung in rungs:
        if any(p is not None and (not p.is_finite() or not ZERO <= p <= ONE) for p in (rung.bid, rung.ask)):
            raise ValueError("invalid book price")
        if rung.bid is not None and rung.ask is not None and rung.bid > rung.ask:
            raise ValueError("crossed book")
    raw: dict[str, Decimal] = {}
    missing: list[str] = []
    for rung in rungs:
        price = rung.raw_price()
        if price is None or not (ZERO < price < ONE):
            missing.append(rung.label)
            continue
        raw[rung.label] = price
    raw_sum = sum(raw.values(), ZERO)
    if len(raw) < min_priced or len(raw) < 2:
        return LadderPMF(
            q={}, raw=raw, lambda_power=ONE, priced_rungs=len(raw), total_rungs=total,
            missing_rungs=tuple(missing), raw_sum=raw_sum, complete=False,
            reason=f"only {len(raw)} of {total} rungs priced (need {min_priced})",
        )
    prices = tuple(raw.values())
    power = _power_lambda(prices)
    powered = {label: Decimal(str(float(p) ** float(power))) for label, p in raw.items()}
    norm = sum(powered.values(), ZERO)
    q = {label: value / norm for label, value in powered.items()}
    return LadderPMF(
        q=q, raw=raw, lambda_power=power, priced_rungs=len(raw), total_rungs=total,
        missing_rungs=tuple(missing), raw_sum=raw_sum, complete=True,
        reason="power de-vig over priced rungs",
    )


def rung_edge(pmf: LadderPMF, label: str, all_in_cost_per_share: Decimal) -> Decimal | None:
    """Downstream edge is always ``q_i - all_in_cost_i``; None when pmf is incomplete."""
    if not pmf.complete or label not in pmf.q:
        return None
    return pmf.q[label] - all_in_cost_per_share
