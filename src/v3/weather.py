"""Paper-research weather utilities: batching, backoff, and calibration."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Sequence

ZERO = Decimal("0")
ONE = Decimal("1")


@dataclass(frozen=True, order=True)
class ForecastRequest:
    city: str
    target_date: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "city", self.city.strip().lower())
        if not self.city or not self.target_date:
            raise ValueError("city and target_date are required")


def deduplicate_forecast_requests(requests: Iterable[ForecastRequest]) -> tuple[ForecastRequest, ...]:
    return tuple(dict.fromkeys(requests))


class ForecastFailureCache:
    def __init__(self, *, base_delay_seconds: int = 60, max_delay_seconds: int = 3600) -> None:
        if base_delay_seconds <= 0 or max_delay_seconds < base_delay_seconds:
            raise ValueError("invalid backoff configuration")
        self.base_delay_seconds = base_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self._state: dict[ForecastRequest, tuple[int, float]] = {}

    def record_failure(self, request: ForecastRequest, *, now: float) -> None:
        failures = self._state.get(request, (0, now))[0] + 1
        delay = min(self.max_delay_seconds, self.base_delay_seconds * (2 ** (failures - 1)))
        self._state[request] = (failures, now + delay)

    def record_success(self, request: ForecastRequest) -> None:
        self._state.pop(request, None)

    def can_request(self, request: ForecastRequest, *, now: float) -> bool:
        state = self._state.get(request)
        return state is None or now >= state[1]


def brier_score(probabilities: Sequence[Decimal], outcomes: Sequence[int]) -> Decimal:
    if not probabilities or len(probabilities) != len(outcomes):
        raise ValueError("probabilities and outcomes must have equal non-zero length")
    total = ZERO
    for probability, outcome in zip(probabilities, outcomes, strict=True):
        if not (ZERO <= probability <= ONE) or outcome not in {0, 1}:
            raise ValueError("invalid probability or outcome")
        total += (probability - Decimal(outcome)) ** 2
    return total / Decimal(len(probabilities))
