"""V3 settings with a strict, three-part live-client gate."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from decimal import Decimal

LIVE_CONFIRMATION = "I_UNDERSTAND_REAL_MONEY"


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
    max_capital: Decimal = Decimal("50")
    max_order_notional: Decimal = Decimal("2")
    reserve_fraction: Decimal = Decimal("0.25")

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
            max_capital=Decimal(os.getenv("V3_MAX_CAPITAL", "50")),
            max_order_notional=Decimal(os.getenv("V3_MAX_ORDER_NOTIONAL", "2")),
            reserve_fraction=Decimal(os.getenv("V3_RESERVE_FRACTION", "0.25")),
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
        return tuple(errors)
