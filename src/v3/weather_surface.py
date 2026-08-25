"""Pure event-surface checks for mutually exclusive weather buckets."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .math import BookLevel, execution_fee, execution_vwap

ZERO = Decimal("0")
ONE = Decimal("1")


@dataclass(frozen=True)
class SurfaceBucket:
    """One YES outcome in a whole-degree mutually exclusive event."""

    key: str
    lower_display: Decimal | None
    upper_display: Decimal | None
    model_probability: Decimal
    yes_asks: tuple[BookLevel, ...]
    minimum_size: Decimal
    fee_rate: Decimal

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("bucket key is required")
        if self.lower_display is None and self.upper_display is None:
            raise ValueError("bucket bounds cannot both be open")
        if (
            self.lower_display is not None
            and self.upper_display is not None
            and self.lower_display > self.upper_display
        ):
            raise ValueError("invalid bucket bounds")
        if not (ZERO <= self.model_probability <= ONE):
            raise ValueError("model probability must be in [0, 1]")
        if not self.yes_asks:
            raise ValueError("YES ask book is required")
        if self.minimum_size <= ZERO:
            raise ValueError("minimum size must be positive")
        if self.fee_rate < ZERO:
            raise ValueError("fee rate cannot be negative")


@dataclass(frozen=True)
class EventSurface:
    event_key: str
    unit: str
    bucket_count: int
    complete_partition: bool
    executable: bool
    tradeable: bool
    common_shares: Decimal
    model_probability_sum: Decimal
    model_probability_residual: Decimal
    gross_cost: Decimal
    fees: Decimal
    payout: Decimal
    net_profit: Decimal
    reason: str
    partition_violations: tuple[str, ...] = ()
    monotonic_violations: tuple[str, ...] = ()


def _partition_violations(buckets: tuple[SurfaceBucket, ...]) -> tuple[str, ...]:
    violations: list[str] = []
    lower_tails = sum(bucket.lower_display is None for bucket in buckets)
    upper_tails = sum(bucket.upper_display is None for bucket in buckets)
    if lower_tails == 0:
        violations.append("missing_lower_tail")
    elif lower_tails > 1:
        violations.append("multiple_lower_tails")
    if upper_tails == 0:
        violations.append("missing_upper_tail")
    elif upper_tails > 1:
        violations.append("multiple_upper_tails")

    ordered = sorted(
        buckets,
        key=lambda bucket: (
            bucket.lower_display is not None,
            ZERO if bucket.lower_display is None else bucket.lower_display,
            bucket.key,
        ),
    )
    for current, following in zip(ordered, ordered[1:], strict=False):
        if current.upper_display is None or following.lower_display is None:
            violations.append(f"overlap:{current.key}:{following.key}")
            continue
        expected = current.upper_display + ONE
        if following.lower_display > expected:
            violations.append(f"gap:{current.key}:{following.key}")
        elif following.lower_display < expected:
            violations.append(f"overlap:{current.key}:{following.key}")
    return tuple(violations)


def _best_ask(bucket: SurfaceBucket) -> Decimal:
    return min(level.price for level in bucket.yes_asks)


def _monotonic_violations(buckets: tuple[SurfaceBucket, ...]) -> tuple[str, ...]:
    """Check prices only for nested tails with the same event semantics."""

    violations: list[str] = []
    lower_tails = sorted(
        (bucket for bucket in buckets if bucket.lower_display is None),
        key=lambda bucket: (bucket.upper_display, bucket.key),
    )
    for narrower, wider in zip(lower_tails, lower_tails[1:], strict=False):
        if _best_ask(narrower) > _best_ask(wider):
            violations.append(
                f"lower_tail_price_inversion:{narrower.key}:{wider.key}"
            )

    upper_tails = sorted(
        (bucket for bucket in buckets if bucket.upper_display is None),
        key=lambda bucket: (bucket.lower_display, bucket.key),
    )
    for wider, narrower in zip(upper_tails, upper_tails[1:], strict=False):
        if _best_ask(wider) < _best_ask(narrower):
            violations.append(
                f"upper_tail_price_inversion:{wider.key}:{narrower.key}"
            )
    return tuple(violations)


def analyze_event_surface(
    *,
    event_key: str,
    unit: str,
    buckets: tuple[SurfaceBucket, ...],
) -> EventSurface:
    """Price one equal-share basket only when buckets form a full partition."""

    if not event_key:
        raise ValueError("event key is required")
    if unit not in {"C", "F"}:
        raise ValueError("weather event unit must be C or F")
    if not buckets:
        raise ValueError("weather event requires at least one bucket")
    keys = tuple(bucket.key for bucket in buckets)
    if len(keys) != len(set(keys)):
        raise ValueError("weather event bucket keys must be unique")

    probability_sum = sum((bucket.model_probability for bucket in buckets), ZERO)
    probability_residual = ONE - probability_sum
    partition_violations = _partition_violations(buckets)
    monotonic_violations = _monotonic_violations(buckets)
    if partition_violations:
        return EventSurface(
            event_key=event_key,
            unit=unit,
            bucket_count=len(buckets),
            complete_partition=False,
            executable=False,
            tradeable=False,
            common_shares=ZERO,
            model_probability_sum=probability_sum,
            model_probability_residual=probability_residual,
            gross_cost=ZERO,
            fees=ZERO,
            payout=ZERO,
            net_profit=ZERO,
            reason="event partition violations: " + ", ".join(partition_violations),
            partition_violations=partition_violations,
            monotonic_violations=monotonic_violations,
        )

    common_shares = max(bucket.minimum_size for bucket in buckets)
    gross_cost = ZERO
    fees = ZERO
    try:
        for bucket in buckets:
            quote = execution_vwap(bucket.yes_asks, common_shares)
            gross_cost += quote.notional
            fees += execution_fee(bucket.yes_asks, common_shares, bucket.fee_rate)
    except ValueError as exc:
        return EventSurface(
            event_key=event_key,
            unit=unit,
            bucket_count=len(buckets),
            complete_partition=True,
            executable=False,
            tradeable=False,
            common_shares=common_shares,
            model_probability_sum=probability_sum,
            model_probability_residual=probability_residual,
            gross_cost=ZERO,
            fees=ZERO,
            payout=common_shares,
            net_profit=ZERO,
            reason=str(exc),
            partition_violations=partition_violations,
            monotonic_violations=monotonic_violations,
        )

    payout = common_shares
    net_profit = payout - gross_cost - fees
    tradeable = net_profit > ZERO
    return EventSurface(
        event_key=event_key,
        unit=unit,
        bucket_count=len(buckets),
        complete_partition=True,
        executable=True,
        tradeable=tradeable,
        common_shares=common_shares,
        model_probability_sum=probability_sum,
        model_probability_residual=probability_residual,
        gross_cost=gross_cost,
        fees=fees,
        payout=payout,
        net_profit=net_profit,
        reason=(
            "fee-adjusted complete basket"
            if tradeable
            else "complete basket has no fee-adjusted profit"
        ),
        partition_violations=partition_violations,
        monotonic_violations=monotonic_violations,
    )
