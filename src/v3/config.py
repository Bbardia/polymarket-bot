"""V3 settings with a strict, three-part live-client gate."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

LIVE_CONFIRMATION = "I_UNDERSTAND_REAL_MONEY"


def _env_decimal(name: str, default: str) -> Decimal:
    value = os.getenv(name, default).strip()
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be a decimal number") from exc


def _env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class V3Settings:
    account_reads_enabled: bool = False
    live_enabled: bool = False
    paper_trading: bool = True
    live_confirmation: str = ""
    private_key: str = ""
    wallet_address: str = ""
    builder_api_key: str = ""
    builder_secret: str = ""
    builder_passphrase: str = ""
    external_condition_ids: frozenset[str] = frozenset()
    max_capital: Decimal = Decimal("100")
    max_order_notional: Decimal = Decimal("2")
    reserve_fraction: Decimal = Decimal("0.25")
    max_daily_loss: Decimal | None = None
    max_drawdown_amount: Decimal | None = None
    max_drawdown_fraction: Decimal | None = None

    @classmethod
    def from_env(cls) -> "V3Settings":
        external = frozenset(
            item.strip()
            for item in os.getenv("POLY_EXTERNAL_CONDITION_IDS", "").split(",")
            if item.strip()
        )
        return cls(
            account_reads_enabled=_env_bool("ENABLE_V3_ACCOUNT_READS", False),
            live_enabled=_env_bool("ENABLE_V3_LIVE_TRADING", False),
            paper_trading=_env_bool("PAPER_TRADING", True),
            live_confirmation=os.getenv("V3_LIVE_CONFIRMATION", "").strip(),
            private_key=os.getenv("POLY_PRIVATE_KEY", "").strip(),
            wallet_address=os.getenv("POLY_FUNDER_ADDRESS", "").strip(),
            builder_api_key=os.getenv("POLY_BUILDER_API_KEY", "").strip(),
            builder_secret=os.getenv("POLY_BUILDER_SECRET", "").strip(),
            builder_passphrase=os.getenv("POLY_BUILDER_PASSPHRASE", "").strip(),
            external_condition_ids=external,
            max_capital=_env_decimal("V3_MAX_CAPITAL", "100"),
            max_order_notional=_env_decimal("V3_MAX_ORDER_NOTIONAL", "2"),
            reserve_fraction=_env_decimal("V3_RESERVE_FRACTION", "0.25"),
            max_daily_loss=(
                _env_decimal("V3_MAX_DAILY_LOSS", "0")
                if os.getenv("V3_MAX_DAILY_LOSS", "").strip()
                else None
            ),
            max_drawdown_amount=(
                _env_decimal("V3_MAX_DRAWDOWN_AMOUNT", "0")
                if os.getenv("V3_MAX_DRAWDOWN_AMOUNT", "").strip()
                else None
            ),
            max_drawdown_fraction=(
                _env_decimal("V3_MAX_DRAWDOWN_FRACTION", "0")
                if os.getenv("V3_MAX_DRAWDOWN_FRACTION", "").strip()
                else None
            ),
        )

    def account_client_errors(self) -> tuple[str, ...]:
        errors: list[str] = []
        if not self.account_reads_enabled:
            errors.append("ENABLE_V3_ACCOUNT_READS must be true")
        if not re.fullmatch(r"0x[0-9a-fA-F]{64}", self.private_key):
            errors.append("POLY_PRIVATE_KEY is missing or malformed")
        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", self.wallet_address):
            errors.append("POLY_FUNDER_ADDRESS is missing or malformed")
        return tuple(errors)

    def live_client_errors(self) -> tuple[str, ...]:
        errors: list[str] = []
        if not self.live_enabled:
            errors.append("ENABLE_V3_LIVE_TRADING must be true")
        if self.paper_trading:
            errors.append("PAPER_TRADING must be false")
        if self.live_confirmation != LIVE_CONFIRMATION:
            errors.append("V3_LIVE_CONFIRMATION is missing or incorrect")
        if not re.fullmatch(r"0x[0-9a-fA-F]{64}", self.private_key):
            errors.append("POLY_PRIVATE_KEY is missing or malformed")
        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", self.wallet_address):
            errors.append("POLY_FUNDER_ADDRESS is missing or malformed")

        if not self.max_capital.is_finite() or self.max_capital <= 0:
            errors.append("V3_MAX_CAPITAL must be finite and > 0")
        if (
            not self.max_order_notional.is_finite()
            or self.max_order_notional <= 0
            or not self.max_capital.is_finite()
            or self.max_order_notional > self.max_capital
        ):
            errors.append("V3_MAX_ORDER_NOTIONAL must be finite, > 0, and <= V3_MAX_CAPITAL")
        if (
            not self.reserve_fraction.is_finite()
            or self.reserve_fraction < 0
            or self.reserve_fraction >= 1
        ):
            errors.append("V3_RESERVE_FRACTION must be finite and in [0, 1)")
        if self.max_daily_loss is None:
            errors.append("V3_MAX_DAILY_LOSS must be explicitly set")
        elif (
            not self.max_daily_loss.is_finite()
            or self.max_daily_loss <= 0
            or self.max_daily_loss > self.max_capital
        ):
            errors.append("V3_MAX_DAILY_LOSS must be > 0 and <= V3_MAX_CAPITAL")
        if self.max_drawdown_amount is None:
            errors.append("V3_MAX_DRAWDOWN_AMOUNT must be explicitly set")
        elif (
            not self.max_drawdown_amount.is_finite()
            or self.max_drawdown_amount <= 0
            or self.max_drawdown_amount > self.max_capital
        ):
            errors.append("V3_MAX_DRAWDOWN_AMOUNT must be finite, > 0, and <= V3_MAX_CAPITAL")
        if self.max_drawdown_fraction is not None and (
            not self.max_drawdown_fraction.is_finite()
            or self.max_drawdown_fraction <= 0
            or self.max_drawdown_fraction >= 1
        ):
            errors.append("V3_MAX_DRAWDOWN_FRACTION must be finite and in (0, 1)")
        return tuple(errors)
