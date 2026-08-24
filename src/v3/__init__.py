"""Polymarket V3: paper-first, pUSD-aware trading foundation.

This package intentionally has no import-time network calls and no automatic
service/worker startup. Authenticated clients are created only after explicit
live-safety validation.
"""

__all__ = [
    "api",
    "config",
    "execution",
    "ledger",
    "market_context",
    "math",
    "orders",
    "paper",
    "paper_weather",
    "reconciliation",
    "risk",
    "simulation",
    "streaming",
    "strategies",
    "weather",
]
