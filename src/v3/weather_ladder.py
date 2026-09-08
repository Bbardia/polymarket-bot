"""Fee- and depth-aware adjacent weather ladder mathematics.

A ladder is a directional basket, not a guaranteed arbitrage. The selected YES
buckets are mutually exclusive, but outcomes outside the selected cluster pay
zero. All probabilities and costs are therefore carried explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

from .math import BookLevel, execution_fee, execution_vwap

ZERO = Decimal("0")
ONE = Decimal("1")


@dataclass(frozen=True)
class LadderBucket:
    key: str
    market_id: str
    condition_id: str
    token_id: str
    question: str
    lower_display: Decimal
    upper_display: Decimal
    model_probability: Decimal
    yes_asks: tuple[BookLevel, ...]
    minimum_size: Decimal
    fee_rate: Decimal

    def __post_init__(self) -> None:
        if not self.key or not self.market_id or not self.condition_id or not self.token_id:
            raise ValueError("ladder identifiers are required")
        if self.lower_display != self.upper_display:
            raise ValueError("ladder buckets must be exact outcomes")
        if not ZERO <= self.model_probability <= ONE:
            raise ValueError("ladder probability must be in [0, 1]")
        if not self.yes_asks:
            raise ValueError("ladder YES ask depth is required")
        if self.minimum_size <= ZERO or self.fee_rate < ZERO:
            raise ValueError("ladder size and fee rate must be nonnegative/positive")


@dataclass(frozen=True)
class LadderLeg:
    key: str
    market_id: str
    condition_id: str
    token_id: str
    question: str
    model_probability: Decimal
    shares: Decimal
    ask_vwap: Decimal
    fee: Decimal
    all_in_cost: Decimal


@dataclass(frozen=True)
class LadderResult:
    event_key: str
    unit: str
    width: int
    legs: tuple[LadderLeg, ...]
    cluster_probability: Decimal
    outside_probability: Decimal
    shares: Decimal
    total_cost: Decimal
    expected_payout: Decimal
    expected_profit: Decimal
    payout_if_selected_wins: Decimal
    profit_if_selected_wins: Decimal
    loss_if_outside_cluster: Decimal
    executable: bool
    tradeable: bool
    reason: str


def _invalid(event_key: str, unit: str, width: int, reason: str) -> LadderResult:
    return LadderResult(
        event_key=event_key,
        unit=unit,
        width=width,
        legs=(),
        cluster_probability=ZERO,
        outside_probability=ONE,
        shares=ZERO,
        total_cost=ZERO,
        expected_payout=ZERO,
        expected_profit=ZERO,
        payout_if_selected_wins=ZERO,
        profit_if_selected_wins=ZERO,
        loss_if_outside_cluster=ZERO,
        executable=False,
        tradeable=False,
        reason=reason,
    )


def evaluate_ladder(
    *,
    event_key: str,
    unit: str,
    buckets: Sequence[LadderBucket],
    min_expected_profit: Decimal = Decimal("0.01"),
    min_cluster_probability: Decimal = Decimal("0.50"),
    max_basket_cost: Decimal = Decimal("5"),
    require_positive_selected_outcome: bool = True,
) -> LadderResult:
    """Evaluate equal-share adjacent buckets at executable ask depth.

    For equal shares ``s`` and selected-bucket probabilities ``q_i``:

    ``EV = s * sum(q_i) - total_cost``.

    The result is still a loss of ``total_cost`` when the resolved outcome is
    outside the selected cluster. This function never calls that outcome risk
    arbitrage.
    """
    width = len(buckets)
    if not event_key or unit not in {"C", "F"}:
        return _invalid(event_key, unit, width, "invalid ladder identity")
    if width not in {3, 4}:
        return _invalid(event_key, unit, width, "ladder width must be 3 or 4")
    if min_expected_profit < ZERO or not ZERO <= min_cluster_probability <= ONE:
        raise ValueError("invalid ladder thresholds")
    if max_basket_cost <= ZERO:
        raise ValueError("ladder basket cost cap must be positive")
    ordered = tuple(buckets)
    for left, right in zip(ordered, ordered[1:], strict=False):
        if right.lower_display != left.upper_display + ONE:
            return _invalid(event_key, unit, width, "ladder buckets are not adjacent")
    if len({bucket.condition_id for bucket in ordered}) != width:
        return _invalid(event_key, unit, width, "ladder condition IDs must be unique")

    shares = max(bucket.minimum_size for bucket in ordered)
    legs: list[LadderLeg] = []
    total_cost = ZERO
    try:
        for bucket in ordered:
            execution = execution_vwap(bucket.yes_asks, shares)
            fee = execution_fee(bucket.yes_asks, shares, bucket.fee_rate)
            cost = execution.notional + fee
            total_cost += cost
            legs.append(LadderLeg(
                key=bucket.key,
                market_id=bucket.market_id,
                condition_id=bucket.condition_id,
                token_id=bucket.token_id,
                question=bucket.question,
                model_probability=bucket.model_probability,
                shares=shares,
                ask_vwap=execution.vwap,
                fee=fee,
                all_in_cost=cost,
            ))
    except ValueError as exc:
        return _invalid(event_key, unit, width, f"ladder depth unavailable: {exc}")

    cluster_probability = sum((leg.model_probability for leg in legs), ZERO)
    outside_probability = max(ZERO, ONE - cluster_probability)
    expected_payout = shares * cluster_probability
    expected_profit = expected_payout - total_cost
    payout_if_selected_wins = shares
    profit_if_selected_wins = payout_if_selected_wins - total_cost
    loss_if_outside_cluster = -total_cost
    executable = True
    tradeable = (
        cluster_probability >= min_cluster_probability
        and expected_profit >= min_expected_profit
        and total_cost <= max_basket_cost
        and (not require_positive_selected_outcome or profit_if_selected_wins > ZERO)
    )
    if tradeable:
        reason = "fee- and depth-adjusted weather ladder candidate"
    elif total_cost > max_basket_cost:
        reason = "ladder basket cost cap exceeded"
    elif cluster_probability < min_cluster_probability:
        reason = "ladder cluster probability below threshold"
    elif expected_profit < min_expected_profit:
        reason = "ladder expected profit below threshold"
    else:
        reason = "ladder selected-outcome payout does not cover basket cost"
    return LadderResult(
        event_key=event_key,
        unit=unit,
        width=width,
        legs=tuple(legs),
        cluster_probability=cluster_probability,
        outside_probability=outside_probability,
        shares=shares,
        total_cost=total_cost,
        expected_payout=expected_payout,
        expected_profit=expected_profit,
        payout_if_selected_wins=payout_if_selected_wins,
        profit_if_selected_wins=profit_if_selected_wins,
        loss_if_outside_cluster=loss_if_outside_cluster,
        executable=executable,
        tradeable=tradeable,
        reason=reason,
    )
