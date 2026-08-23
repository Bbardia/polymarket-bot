"""Official unified Polymarket SDK adapter.

Construction is network-free. Public access is always read-only; authenticated
account reads are separately gated, while the live-order client requires the
full V3 live gate. This module exposes no automatic account mutation.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any

import polymarket
from polymarket import AsyncPublicClient, AsyncSecureClient, BuilderApiKey

from .config import V3Settings
from .reconciliation import RemoteOrder, RemotePosition, RemoteSnapshot

PUSD_BASE_UNITS = Decimal("1000000")

SecureClientFactory = Callable[..., Awaitable[Any]]


class UnifiedPolymarketAPI:
    def __init__(
        self,
        *,
        settings: V3Settings | None = None,
        secure_client_factory: SecureClientFactory | None = None,
    ) -> None:
        self.settings = settings or V3Settings.from_env()
        self.public_client = AsyncPublicClient()
        self._secure_client_factory = secure_client_factory or AsyncSecureClient.create
        self._secure_client: Any | None = None

    @property
    def sdk_version(self) -> str:
        return polymarket.__version__

    @property
    def secure_client_initialized(self) -> bool:
        return self._secure_client is not None

    def _authenticated_client(self) -> Any:
        if self._secure_client is None:
            raise RuntimeError("authenticated client has not been initialized")
        return self._secure_client

    async def initialize_account_client(self) -> None:
        """Create contained account-read access; never expose the SDK client."""
        errors = self.settings.account_client_errors()
        if errors:
            raise RuntimeError("Account-read client refused: " + "; ".join(errors))
        self._secure_client = await self._secure_client_factory(
            private_key=self.settings.private_key,
            wallet=self.settings.wallet_address,
        )

    async def initialize_secure_client(self) -> Any:
        errors = self.settings.live_client_errors()
        if errors:
            raise RuntimeError("Live client refused: " + "; ".join(errors))

        kwargs: dict[str, Any] = {
            "private_key": self.settings.private_key,
            "wallet": self.settings.wallet_address,
        }
        if (
            self.settings.builder_api_key
            and self.settings.builder_secret
            and self.settings.builder_passphrase
        ):
            kwargs["api_key"] = BuilderApiKey(
                key=self.settings.builder_api_key,
                secret=self.settings.builder_secret,
                passphrase=self.settings.builder_passphrase,
            )
        self._secure_client = await self._secure_client_factory(**kwargs)
        return self._secure_client

    async def fetch_remote_snapshot(self, *, max_items: int = 2_000) -> RemoteSnapshot:
        """Read pUSD, positions, and open orders; never mutates the account."""
        client = self._authenticated_client()
        balance = await client.get_balance_allowance(asset_type="COLLATERAL")

        positions: list[RemotePosition] = []
        async for page in client.list_positions(size_threshold=0):
            for position in page.items:
                size = Decimal(str(position.size or 0))
                if size > 0 and position.token_id:
                    positions.append(RemotePosition(
                        condition_id=str(position.condition_id),
                        token_id=str(position.token_id),
                        size=size,
                        current_value=Decimal(str(position.current_value or 0)),
                    ))
                if len(positions) > max_items:
                    raise RuntimeError("position reconciliation limit exceeded")

        orders: list[RemoteOrder] = []
        async for page in client.list_open_orders():
            for order in page.items:
                orders.append(RemoteOrder(
                    order_id=str(order.id),
                    condition_id=str(order.condition_id),
                    token_id=str(order.token_id),
                ))
                if len(orders) > max_items:
                    raise RuntimeError("open-order reconciliation limit exceeded")

        return RemoteSnapshot(
            cash=Decimal(balance.balance) / PUSD_BASE_UNITS,
            positions=tuple(positions),
            open_orders=tuple(orders),
        )

    async def get_order_book(self, token_id: str):
        """Read-only typed Decimal order book from CLOB V2."""
        return await self.public_client.get_order_book(token_id=token_id)

    async def get_market(self, *, market_id: str | None = None, slug: str | None = None):
        if not market_id and not slug:
            raise ValueError("market_id or slug is required")
        return await self.public_client.get_market(id=market_id, slug=slug)
