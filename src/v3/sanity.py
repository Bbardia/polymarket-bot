"""Invariant and input-sanity layer (remediation item 4).

Every check here is fail-closed: a violated invariant raises ``SanityError``
or returns a flagged result that callers must treat as "do not trade". Nothing
in this module silently clamps a value and lets the trade proceed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Iterable, Sequence

ZERO = Decimal("0")
ONE = Decimal("1")

PROBABILITY_FLOOR = Decimal("0.01")
PROBABILITY_CEILING = Decimal("0.99")
PARTITION_TOLERANCE = Decimal("0.000001")
MAX_RELATIVE_SPREAD_FOR_MID = Decimal("0.20")
MIN_BEST_LEVEL_SIZE = Decimal("5")
MAX_ABSOLUTE_SPREAD = Decimal("0.10")

# Whitelisted reasons that permit a probability outside [0.01, 0.99]. Each one
# names a deterministic, resolver-grounded fact rather than a model output.
PROBABILITY_EXTREME_WHITELIST = frozenset({
    "resolver_certain_observation_bound",
    "observed_threshold_already_exceeded",
    "market_already_resolved",
})


class SanityError(ValueError):
    """An external input violated a hard invariant."""


def _decimal(value: Any) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def assert_newest_at_index(
    series: Sequence[Any],
    *,
    newest_index: int,
    key: Callable[[Any], Any],
    name: str = "series",
) -> Any:
    """Assert the print at ``newest_index`` is the maximum of ``key``.

    External time series arrive newest-first (BLS) or oldest-first (most
    others). Callers must name which index they believe is newest; if the data
    disagrees the pipeline stops instead of silently reading a stale print.
    """
    if not series:
        raise SanityError(f"{name} is empty")
    try:
        candidate = series[newest_index]
    except IndexError as exc:
        raise SanityError(f"{name} has no element at index {newest_index}") from exc
    newest_key = key(candidate)
    for item in series:
        if key(item) > newest_key:
            raise SanityError(
                f"{name} ordering violated: element {key(item)!r} is newer than "
                f"the declared newest print {newest_key!r} at index {newest_index}"
            )
    return candidate


def assert_strictly_chronological(
    series: Sequence[Any],
    *,
    key: Callable[[Any], Any],
    name: str = "series",
) -> None:
    previous = None
    for item in series:
        current = key(item)
        if previous is not None and current <= previous:
            raise SanityError(f"{name} is not strictly chronological at {current!r}")
        previous = current


@dataclass(frozen=True)
class ProbabilityCheck:
    value: Decimal
    flagged: bool
    reason: str | None

    @property
    def tradeable(self) -> bool:
        return not self.flagged


def check_probability(
    probability: Any,
    *,
    floor: Decimal = PROBABILITY_FLOOR,
    ceiling: Decimal = PROBABILITY_CEILING,
    whitelist_reason: str | None = None,
) -> ProbabilityCheck:
    """Clamp-and-flag. A flagged probability must never be traded.

    Values outside [0, 1] are always flagged. Values in [0, floor) or
    (ceiling, 1] are flagged unless ``whitelist_reason`` is one of
    ``PROBABILITY_EXTREME_WHITELIST``.
    """
    value = _decimal(probability)
    if not value.is_finite():
        return ProbabilityCheck(ZERO, True, "probability is not finite")
    if value < ZERO or value > ONE:
        clamped = min(ONE, max(ZERO, value))
        return ProbabilityCheck(clamped, True, f"probability {value} outside [0, 1]")
    if value < floor or value > ceiling:
        if whitelist_reason in PROBABILITY_EXTREME_WHITELIST:
            return ProbabilityCheck(value, False, None)
        clamped = min(ceiling, max(floor, value))
        return ProbabilityCheck(
            clamped,
            True,
            f"probability {value} outside [{floor}, {ceiling}] without whitelisted reason",
        )
    return ProbabilityCheck(value, False, None)


def assert_partition_sums_to_one(
    values: Iterable[Any],
    *,
    tolerance: Decimal = PARTITION_TOLERANCE,
    name: str = "partition",
) -> Decimal:
    """A bucket vector over a verified exhaustive partition must sum to 1."""
    total = ZERO
    count = 0
    for value in values:
        decimal_value = _decimal(value)
        if not decimal_value.is_finite():
            raise SanityError(f"{name} has nonfinite mass")
        if decimal_value < ZERO:
            raise SanityError(f"{name} has a negative mass {decimal_value}")
        total += decimal_value
        count += 1
    if count == 0:
        raise SanityError(f"{name} is empty")
    if abs(total - ONE) > tolerance:
        raise SanityError(f"{name} sums to {total}, not 1 within {tolerance}")
    return total


def check_price_age(
    *,
    decision_at: datetime,
    price_at: datetime | None,
    max_age_seconds: float,
) -> float:
    """Return the price age in seconds; fail when the price is older than allowed."""
    if price_at is None:
        raise SanityError("price carries no source timestamp")
    if decision_at.tzinfo is None or price_at.tzinfo is None:
        raise SanityError("price and decision timestamps must be timezone-aware")
    age = (decision_at - price_at).total_seconds()
    if age < -5:
        raise SanityError(f"price timestamp is {-age:.0f}s in the future")
    if age > max_age_seconds:
        raise SanityError(
            f"price age {age:.0f}s exceeds the decision interval {max_age_seconds:.0f}s"
        )
    return age


def mid_as_probability(
    *,
    bid: Any,
    ask: Any,
    max_relative_spread: Decimal = MAX_RELATIVE_SPREAD_FOR_MID,
) -> Decimal | None:
    """Return the mid only when the book is tight enough to read as a probability."""
    bid_value, ask_value = _decimal(bid), _decimal(ask)
    if not (ZERO <= bid_value <= ONE and ZERO <= ask_value <= ONE) or ask_value < bid_value:
        return None
    mid = (bid_value + ask_value) / Decimal("2")
    if mid <= ZERO:
        return None
    if (ask_value - bid_value) / mid > max_relative_spread:
        return None
    return mid


def book_hard_reject(
    *,
    best_bid: Any | None,
    best_ask: Any | None,
    best_ask_size: Any | None,
    min_size: Decimal = MIN_BEST_LEVEL_SIZE,
    max_spread: Decimal = MAX_ABSOLUTE_SPREAD,
) -> str | None:
    """Return a rejection reason for books that cannot support a venue-minimum order."""
    if best_ask is None or best_ask_size is None:
        return "no executable ask"
    if not _decimal(best_ask_size).is_finite() or _decimal(best_ask_size) < min_size:
        return f"best ask size {best_ask_size} below venue minimum {min_size}"
    if best_bid is None:
        return "no bid; spread undefined"
    bid, ask, size = _decimal(best_bid), _decimal(best_ask), _decimal(best_ask_size)
    if not all(v.is_finite() for v in (bid, ask, size)) or not ZERO <= bid <= ONE or not ZERO <= ask <= ONE:
        return "invalid book values"
    spread = ask - bid
    if spread < ZERO:
        return "crossed book"
    if spread > max_spread:
        return f"spread {spread} exceeds {max_spread}"
    return None


def geography_gate(
    text: str,
    *,
    required_markers: Sequence[str],
    forbidden_markers: Sequence[str] = (),
) -> str | None:
    """Fail closed: a data series may only price markets that name its geography."""
    lowered = text.lower()
    for marker in forbidden_markers:
        if marker.lower() in lowered:
            return f"forbidden geography marker present: {marker}"
    if not any(marker.lower() in lowered for marker in required_markers):
        return "required geography marker missing"
    return None
