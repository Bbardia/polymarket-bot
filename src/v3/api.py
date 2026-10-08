"""Official unified Polymarket SDK adapter.

Construction is network-free. Public access is always read-only; authenticated
account reads are separately gated, while the live-order client requires the
full V3 live gate. This module exposes no automatic account mutation.
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import polymarket
from polymarket import AsyncPublicClient, AsyncSecureClient, BuilderApiKey

from .config import V3Settings
from .reconciliation import (
    CompleteAccountTradeHistory, TRADE_STATUSES, TRADE_TRADER_SIDES,
    RemoteAccountOrder, RemoteOrder, RemotePosition, RemoteSnapshot, RemoteTrade, RemoteTradeMaker,
)

PUSD_BASE_UNITS = Decimal("1000000")
ACTIVITY_PAGE_SIZE_CAP = 500



@dataclass(frozen=True)
class AccountCashFlow:
    event_id: str
    event_type: str
    timestamp: datetime
    transaction_hash: str
    amount: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, str) or not self.event_id:
            raise ValueError("event_id must be a nonempty string")
        if self.event_type not in ("DEPOSIT", "WITHDRAWAL"):
            raise ValueError("event_type must be DEPOSIT or WITHDRAWAL")
        if not isinstance(self.timestamp, datetime) or self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        if not isinstance(self.transaction_hash, str) or not self.transaction_hash:
            raise ValueError("transaction_hash must be a nonempty string")
        if not isinstance(self.amount, Decimal) or not self.amount.is_finite() or self.amount <= 0:
            raise ValueError("amount must be a finite positive Decimal")

    @property
    def signed_amount(self) -> Decimal:
        if self.event_type == "DEPOSIT":
            return self.amount
        if self.event_type == "WITHDRAWAL":
            return -self.amount
        raise ValueError("event_type must be DEPOSIT or WITHDRAWAL")


@dataclass(frozen=True)
class CompleteAccountCashFlowHistory:
    """Complete bounded public deposit/withdrawal history after a baseline."""

    after: int
    flows: tuple[AccountCashFlow, ...]
    fetched_at: datetime
    max_items: int
    page_size: int

    def __post_init__(self) -> None:
        if type(self.after) is not int or self.after < 0:
            raise ValueError("cash-flow baseline must be a nonnegative Unix timestamp")
        if not isinstance(self.flows, tuple) or not all(isinstance(row, AccountCashFlow) for row in self.flows):
            raise ValueError("cash flows must be an immutable tuple of validated records")
        if len({row.event_id for row in self.flows}) != len(self.flows):
            raise ValueError("cash-flow event IDs must be unique")
        if not isinstance(self.fetched_at, datetime) or self.fetched_at.tzinfo is None or self.fetched_at.utcoffset() is None:
            raise ValueError("cash-flow retrieval time must be timezone-aware")
        if type(self.max_items) is not int or self.max_items <= 0 or type(self.page_size) is not int or self.page_size <= 0:
            raise ValueError("cash-flow pagination bounds must be positive integers")
        if len(self.flows) > self.max_items or any(flow.timestamp.timestamp() < self.after for flow in self.flows):
            raise ValueError("cash-flow history exceeds its bound or includes a pre-baseline record")

    @property
    def net_amount(self) -> Decimal:
        return sum((flow.signed_amount for flow in self.flows), Decimal("0"))


SecureClientFactory = Callable[..., Awaitable[Any]]


class MarketClosedError(RuntimeError):
    """The condition's market is closed; no live order can be placed in it."""


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

    async def fetch_remote_snapshot(
        self, *, max_items: int = 2_000, page_limit: int = 100,
    ) -> RemoteSnapshot:
        """Read pUSD, positions, and open orders; never mutates the account."""
        if type(max_items) is not int or max_items <= 0:
            raise ValueError("max_items must be a positive integer")
        if type(page_limit) is not int or page_limit <= 0:
            raise ValueError("page_limit must be a positive integer")
        client = self._authenticated_client()
        balance = await client.get_balance_allowance(asset_type="COLLATERAL")

        positions: list[RemotePosition] = []
        position_tokens: set[str] = set()
        position_pages = position_rows = 0
        async for page in client.list_positions(size_threshold=0):
            position_pages += 1
            if position_pages > page_limit:
                raise RuntimeError("position reconciliation page limit exceeded")
            items = getattr(page, "items", None)
            if items is None or isinstance(items, (str, bytes)):
                raise RuntimeError("position reconciliation page has malformed items")
            try:
                iterator = iter(items)
            except TypeError as exc:
                raise RuntimeError("position reconciliation page has malformed items") from exc
            for position in iterator:
                position_rows += 1
                if position_rows > max_items:
                    raise RuntimeError("position reconciliation item limit exceeded")
                size = Decimal(str(position.size or 0))
                if not size.is_finite() or size < 0:
                    raise RuntimeError("position size is invalid")
                token_id = str(getattr(position, "token_id", "") or "")
                if token_id:
                    if token_id in position_tokens:
                        raise RuntimeError("duplicate position identity in account snapshot")
                    position_tokens.add(token_id)
                if size > 0:
                    condition_id = str(position.condition_id or "")
                    if not condition_id or not token_id:
                        raise RuntimeError("position is missing condition/token identity")
                    if position.current_value is None:
                        raise RuntimeError("position is missing current value")
                    if position.initial_value is None:
                        raise RuntimeError("position is missing initial value")
                    current_value = Decimal(str(position.current_value))
                    initial_value = Decimal(str(position.initial_value))
                    if (
                        not current_value.is_finite() or current_value < 0
                        or not initial_value.is_finite() or initial_value < 0
                    ):
                        raise RuntimeError("position value is invalid")
                    positions.append(RemotePosition(
                        condition_id=condition_id,
                        token_id=token_id,
                        size=size,
                        current_value=current_value,
                        initial_value=initial_value,
                        redeemable=getattr(position, "redeemable", None) is True,
                    ))

        orders: list[RemoteOrder] = []
        order_ids: set[str] = set()
        order_pages = order_rows = 0
        async for page in client.list_open_orders():
            order_pages += 1
            if order_pages > page_limit:
                raise RuntimeError("open-order reconciliation page limit exceeded")
            items = getattr(page, "items", None)
            if items is None or isinstance(items, (str, bytes)):
                raise RuntimeError("open-order reconciliation page has malformed items")
            try:
                iterator = iter(items)
            except TypeError as exc:
                raise RuntimeError("open-order reconciliation page has malformed items") from exc
            for order in iterator:
                order_rows += 1
                if order_rows > max_items:
                    raise RuntimeError("open-order reconciliation item limit exceeded")
                order_id = str(order.id or "")
                condition_id = str(order.condition_id or "")
                token_id = str(order.token_id or "")
                if not order_id or not condition_id or not token_id:
                    raise RuntimeError("open order is missing identity")
                price = Decimal(str(order.price))
                original_size = Decimal(str(order.original_size))
                size_matched = Decimal(str(order.size_matched))
                if (
                    not price.is_finite()
                    or not original_size.is_finite()
                    or not size_matched.is_finite()
                    or price <= 0
                    or original_size < 0
                    or size_matched < 0
                    or size_matched > original_size
                ):
                    raise RuntimeError("open order has invalid price or size")
                if order_id in order_ids:
                    raise RuntimeError("duplicate open-order identity in account snapshot")
                order_ids.add(order_id)
                orders.append(RemoteOrder(
                    order_id=order_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    remaining_notional=(original_size - size_matched) * price,
                ))

        cash = Decimal(balance.balance) / PUSD_BASE_UNITS
        if not cash.is_finite() or cash < 0:
            raise RuntimeError("collateral balance is invalid")
        return RemoteSnapshot(
            cash=cash,
            positions=tuple(positions),
            open_orders=tuple(orders),
        )

    async def fetch_account_trades(
        self, *, max_items: int, page_limit: int, after: int | None = None,
    ) -> tuple[RemoteTrade, ...]:
        """Fetch bounded authenticated trade-history rows; never applies fills to local state.

        ``after`` (Unix seconds) limits the read to trades matched at or after a
        recorded account baseline, so pre-bot manual history is not replayed.
        """
        if after is not None and (type(after) is not int or after < 0):
            raise ValueError("after must be a nonnegative integer epoch")
        if type(max_items) is not int or max_items <= 0:
            raise ValueError("max_items must be a positive integer")
        if type(page_limit) is not int or page_limit <= 0:
            raise ValueError("page_limit must be a positive integer")
        client = self._authenticated_client()
        records: dict[str, RemoteTrade] = {}
        raw_rows = pages = maker_rows = 0

        def timestamp(value: Any, *, required: bool) -> datetime | None:
            if value is None and not required:
                return None
            if isinstance(value, datetime):
                result = value
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                result = datetime.fromtimestamp(value, tz=timezone.utc)
            else:
                raise ValueError("timestamp is missing or malformed")
            if result.tzinfo is None or result.utcoffset() is None:
                raise ValueError("timestamp is timezone-naive")
            return result.astimezone(timezone.utc)

        def decimal(value: Any, name: str, *, optional: bool = False) -> Decimal | None:
            if value is None and optional:
                return None
            result = Decimal(str(value))
            if not result.is_finite() or (result < 0 if "fee" in name else result <= 0):
                raise ValueError(f"{name} is invalid")
            return result

        try:
            trade_pages = (
                client.list_account_trades() if after is None
                else client.list_account_trades(after=str(after))
            )
            async for page in trade_pages:
                pages += 1
                if pages > page_limit:
                    raise RuntimeError("account trade page limit exceeded")
                items = getattr(page, "items", None)
                if items is None or isinstance(items, (str, bytes)):
                    raise ValueError("page items are malformed")
                try:
                    iterator = iter(items)
                except TypeError as exc:
                    raise ValueError("page items are malformed") from exc
                for row in iterator:
                    if raw_rows >= max_items:
                        raise RuntimeError("account trade item limit exceeded")
                    raw_rows += 1
                    trade_id = getattr(row, "id")
                    condition = getattr(row, "condition_id")
                    token = getattr(row, "token_id")
                    taker_order_id = getattr(row, "taker_order_id")
                    side, trader_side, status = getattr(row, "side"), getattr(row, "trader_side"), getattr(row, "status")
                    if not all(isinstance(x, str) and x.strip() for x in (trade_id, condition, token, taker_order_id, side, trader_side, status)):
                        raise ValueError("identity or status missing")
                    if status not in TRADE_STATUSES:
                        raise ValueError("unknown trade status")
                    if trader_side not in TRADE_TRADER_SIDES:
                        raise ValueError("unknown trader side")
                    price, size = decimal(getattr(row, "price"), "price"), decimal(getattr(row, "size"), "size")
                    fee = decimal(getattr(row, "fee_rate_bps"), "fee_rate_bps", optional=True)
                    makers = []
                    raw_makers = getattr(row, "maker_orders")
                    if raw_makers is None or isinstance(raw_makers, (str, bytes)):
                        raise ValueError("maker_orders malformed")
                    for maker in raw_makers:
                        if maker_rows >= max_items:
                            raise RuntimeError("account trade maker order limit exceeded")
                        maker_rows += 1
                        makers.append(RemoteTradeMaker(
                            order_id=getattr(maker, "order_id"), token_id=getattr(maker, "token_id"),
                            side=getattr(maker, "side"), price=decimal(getattr(maker, "price"), "maker price"),
                            matched_amount=decimal(getattr(maker, "matched_amount"), "maker matched amount"),
                            fee_rate_bps=decimal(getattr(maker, "fee_rate_bps", None), "maker fee_rate_bps", optional=True),
                        ))
                    tx_hash = getattr(row, "transaction_hash", None)
                    if tx_hash is not None and (not isinstance(tx_hash, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", tx_hash)):
                        raise ValueError("transaction hash is invalid")
                    current = RemoteTrade(
                        trade_id=trade_id, condition_id=condition, token_id=token, taker_order_id=taker_order_id, side=side,
                        trader_side=trader_side, price=price, size=size, status=status,
                        matched_at=timestamp(getattr(row, "matched_at"), required=True),
                        updated_at=timestamp(getattr(row, "updated_at", None), required=False),
                        fee_rate_bps=fee, transaction_hash=tx_hash.lower() if tx_hash else None,
                        maker_orders=tuple(makers),
                    )
                    if after is not None and current.matched_at.timestamp() < after:
                        continue
                    previous = records.get(trade_id)
                    if previous is not None and previous != current:
                        raise RuntimeError("conflicting duplicate account trade ID")
                    records[trade_id] = current
        except RuntimeError:
            raise
        except (AttributeError, TypeError, ValueError, InvalidOperation, OverflowError, OSError) as exc:
            raise RuntimeError(f"invalid account trade: {exc}") from exc
        return tuple(sorted(records.values(), key=lambda trade: (trade.matched_at, trade.trade_id)))

    async def fetch_complete_account_trade_history(
        self, *, after: int, max_items: int, page_limit: int,
    ) -> CompleteAccountTradeHistory:
        """Fetch post-baseline history; no object is returned on truncation or parse errors."""
        trades = await self.fetch_account_trades(
            after=after, max_items=max_items, page_limit=page_limit,
        )
        return CompleteAccountTradeHistory(
            after=after, trades=trades, fetched_at=datetime.now(timezone.utc),
            max_items=max_items, page_limit=page_limit,
        )

    async def fetch_account_cash_flows(
        self, *, max_items: int = 10_000, page_size: int = ACTIVITY_PAGE_SIZE_CAP
    ) -> tuple[AccountCashFlow, ...]:
        """Read bounded public wallet deposit/withdrawal history; not an authoritative risk source."""
        if type(max_items) is not int or max_items <= 0:
            raise ValueError("max_items must be positive")
        if type(page_size) is not int or page_size <= 0 or page_size > ACTIVITY_PAGE_SIZE_CAP:
            raise ValueError("page_size must be between 1 and 500")
        wallet = str(self.settings.wallet_address or "").strip()
        if not wallet:
            raise RuntimeError("configured wallet is required for cash-flow activity")

        records: dict[str, AccountCashFlow] = {}
        raw_rows = 0
        effective_page_size = min(page_size, max_items)
        page_limit = (max_items + effective_page_size - 1) // effective_page_size + 1
        pages_consumed = 0
        async for page in self.public_client.list_activity(
            user=wallet, activity_types=["DEPOSIT", "WITHDRAWAL"], start=1, page_size=effective_page_size,
        ):
            pages_consumed += 1
            if pages_consumed > page_limit:
                raise RuntimeError("cash-flow activity page limit exceeded")
            items = getattr(page, "items", None)
            if items is None or isinstance(items, (str, bytes)):
                raise RuntimeError("cash-flow activity page has malformed items")
            try:
                iterator = iter(items)
            except TypeError as exc:
                raise RuntimeError("cash-flow activity page has malformed items") from exc
            for row in iterator:
                if raw_rows >= max_items:
                    raise RuntimeError("cash-flow activity item limit exceeded")
                raw_rows += 1
                try:
                    kind = getattr(row, "type")
                    row_wallet = getattr(row, "wallet")
                    tx_hash = getattr(row, "transaction_hash")
                    raw_time = getattr(row, "timestamp")
                    raw_amount = getattr(row, "amount")
                    if kind not in ("DEPOSIT", "WITHDRAWAL"):
                        raise ValueError("unexpected activity type")
                    if not isinstance(row_wallet, str) or row_wallet.lower() != wallet.lower():
                        raise ValueError("activity wallet mismatch")
                    if not isinstance(tx_hash, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", tx_hash):
                        raise ValueError("transaction hash must be 0x-prefixed 32-byte hex")
                    if isinstance(raw_time, datetime):
                        timestamp = raw_time
                    elif isinstance(raw_time, (int, float)) and not isinstance(raw_time, bool):
                        timestamp = datetime.fromtimestamp(raw_time, tz=timezone.utc)
                    else:
                        raise ValueError("timestamp is missing or malformed")
                    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
                        raise ValueError("timestamp is timezone-naive")
                    timestamp = timestamp.astimezone(timezone.utc)
                    amount = Decimal(str(raw_amount))
                    if not amount.is_finite() or amount <= 0:
                        raise ValueError("amount must be finite and positive")
                    normalized_hash = tx_hash.strip().lower()
                    stable_id = f"{normalized_hash}:{kind}"
                    event = AccountCashFlow(stable_id, kind, timestamp, normalized_hash, amount)
                except (AttributeError, TypeError, ValueError, InvalidOperation, OverflowError, OSError) as exc:
                    raise RuntimeError(f"invalid cash-flow activity row: {exc}") from exc
                previous = records.get(event.event_id)
                if previous is not None:
                    if previous != event:
                        raise RuntimeError("conflicting duplicate cash-flow activity event")
                    continue
                records[event.event_id] = event
        return tuple(sorted(records.values(), key=lambda event: (event.timestamp, event.event_id)))

    async def fetch_complete_account_cash_flow_history(
        self, *, after: int, max_items: int = 10_000, page_size: int = ACTIVITY_PAGE_SIZE_CAP,
    ) -> CompleteAccountCashFlowHistory:
        """Return the post-baseline flow set only after bounded pagination finishes."""
        if type(after) is not int or after < 0:
            raise ValueError("after must be a nonnegative Unix timestamp")
        flows = await self.fetch_account_cash_flows(max_items=max_items, page_size=page_size)
        filtered = tuple(flow for flow in flows if flow.timestamp.timestamp() >= after)
        return CompleteAccountCashFlowHistory(
            after=after, flows=filtered, fetched_at=datetime.now(timezone.utc),
            max_items=max_items, page_size=page_size,
        )

    async def fetch_account_cash_flows_window(
        self, *, start: int, end: int, max_items: int = 20_000,
        page_size: int = ACTIVITY_PAGE_SIZE_CAP, page_limit: int = 10_000,
        window_limit: int = 1_024,
    ) -> tuple[AccountCashFlow, ...]:
        """Fetch a bounded explicit epoch window; this is not proof of complete history."""
        for name, value in (("start", start), ("end", end), ("max_items", max_items),
                            ("page_size", page_size), ("page_limit", page_limit),
                            ("window_limit", window_limit)):
            if type(value) is not int:
                raise ValueError(f"{name} must be an integer")
        if start < 0 or end < start:
            raise ValueError("start and end must define a nonnegative inclusive window")
        if max_items <= 0 or page_limit <= 0 or window_limit <= 0:
            raise ValueError("item, page, and window limits must be positive")
        if not 0 < page_size <= ACTIVITY_PAGE_SIZE_CAP:
            raise ValueError("page_size must be between 1 and 500")
        wallet = str(self.settings.wallet_address or "").strip()
        if not wallet:
            raise RuntimeError("configured wallet is required for cash-flow activity")

        records: dict[str, AccountCashFlow] = {}
        raw_rows = pages_consumed = windows_consumed = 0
        pending = [(start, end)]
        try:
            while pending:
                lo, hi = pending.pop()
                windows_consumed += 1
                if windows_consumed > window_limit:
                    raise RuntimeError("cash-flow activity window limit exceeded")
                current_rows = 0
                current_pages = 0
                overflow = False
                previous_timestamp: datetime | None = None
                async for page in self.public_client.list_activity(
                    user=wallet, activity_types=["DEPOSIT", "WITHDRAWAL"],
                    start=lo, end=hi, sort_direction="ASC", page_size=page_size,
                ):
                    current_pages += 1
                    pages_consumed += 1
                    if pages_consumed > page_limit:
                        raise RuntimeError("cash-flow activity page limit exceeded")
                    items = getattr(page, "items", None)
                    if items is None or isinstance(items, (str, bytes)):
                        raise RuntimeError("cash-flow activity page has malformed items")
                    try:
                        rows = list(iter(items))
                    except TypeError as exc:
                        raise RuntimeError("cash-flow activity page has malformed items") from exc
                    if len(rows) > page_size:
                        raise RuntimeError("cash-flow activity page exceeds requested page size")
                    for row in rows:
                        if raw_rows >= max_items:
                            raise RuntimeError("cash-flow activity item limit exceeded")
                        raw_rows += 1
                        current_rows += 1
                        try:
                            kind, row_wallet = getattr(row, "type"), getattr(row, "wallet")
                            tx_hash, raw_time, raw_amount = getattr(row, "transaction_hash"), getattr(row, "timestamp"), getattr(row, "amount")
                            if kind not in ("DEPOSIT", "WITHDRAWAL"):
                                raise ValueError("unexpected activity type")
                            if not isinstance(row_wallet, str) or row_wallet.lower() != wallet.lower():
                                raise ValueError("activity wallet mismatch")
                            if not isinstance(tx_hash, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", tx_hash):
                                raise ValueError("invalid transaction hash")
                            if isinstance(raw_time, datetime):
                                timestamp = raw_time
                            elif isinstance(raw_time, int) and not isinstance(raw_time, bool):
                                timestamp = datetime.fromtimestamp(raw_time, tz=timezone.utc)
                            else:
                                raise ValueError("timestamp must be an integer epoch or datetime")
                            if timestamp.tzinfo is None or timestamp.utcoffset() is None:
                                raise ValueError("timestamp is timezone-naive")
                            timestamp = timestamp.astimezone(timezone.utc)
                            epoch = int(timestamp.timestamp())
                            if not lo <= epoch <= hi:
                                raise ValueError("activity timestamp outside requested window")
                            if previous_timestamp is not None and timestamp < previous_timestamp:
                                raise ValueError("activity timestamps are not ascending")
                            previous_timestamp = timestamp
                            amount = Decimal(str(raw_amount))
                            if not amount.is_finite() or amount <= 0:
                                raise ValueError("amount must be finite and positive")
                            normalized_hash = tx_hash.lower()
                            event = AccountCashFlow(f"{normalized_hash}:{kind}", kind, timestamp, normalized_hash, amount)
                        except (AttributeError, TypeError, ValueError, InvalidOperation, OverflowError, OSError) as exc:
                            raise RuntimeError(f"invalid cash-flow activity row: {exc}") from exc
                        previous = records.get(event.event_id)
                        if previous is not None and previous != event:
                            raise RuntimeError("conflicting duplicate cash-flow activity event")
                        records[event.event_id] = event
                    if current_pages * page_size >= 5_000 and len(rows) == page_size:
                        overflow = True
                        break
                if overflow:
                    if lo == hi:
                        raise RuntimeError("single-second cash-flow window exceeds 5000 offset cap")
                    mid = (lo + hi) // 2
                    pending.append((mid + 1, hi))
                    pending.append((lo, mid))
        except RuntimeError:
            raise
        except (AttributeError, TypeError, ValueError, InvalidOperation, OverflowError, OSError) as exc:
            raise RuntimeError(f"cash-flow activity retrieval failed: {exc}") from exc
        return tuple(sorted(records.values(), key=lambda event: (event.timestamp, event.event_id)))

    async def fetch_redemption_activity(
        self, *, after: int, end: int, max_items: int = 4000, page_limit: int = 40,
    ) -> tuple[Any, ...]:
        """Bounded public wallet activity; never use a truncated feed as evidence."""
        if (type(after) is not int or type(end) is not int or after < 0 or end < after
                or type(max_items) is not int or not 0 < max_items < 5000
                or type(page_limit) is not int or page_limit <= 0):
            raise ValueError("invalid redemption activity bounds")
        wallet = str(self.settings.wallet_address or "").strip()
        if not wallet:
            raise RuntimeError("configured wallet required for redemption activity")
        rows: list[Any] = []
        pages = 0
        async for page in self.public_client.list_activity(
            user=wallet, activity_types=["REDEEM", "DEPOSIT", "WITHDRAWAL"],
            start=after, end=end, sort_direction="ASC", page_size=100,
        ):
            pages += 1
            if pages > page_limit:
                raise RuntimeError("redemption activity page limit exceeded")
            items = getattr(page, "items", None)
            if items is None or isinstance(items, (str, bytes)):
                raise RuntimeError("malformed redemption activity page")
            batch = list(items)
            if len(batch) > 100 or len(rows) + len(batch) >= max_items:
                raise RuntimeError("redemption activity bound reached")
            rows.extend(batch)
        return tuple(rows)

    async def fetch_market_is_neg_risk(self, condition_id: str) -> bool | None:
        """Return explicit market neg-risk metadata; refuse unknown/ambiguous markets."""
        if not isinstance(condition_id, str) or not condition_id:
            raise ValueError("condition_id is required")
        matches: list[Any] = []
        pages = 0
        # Redemption only concerns resolved markets; Gamma omits closed markets
        # unless they are requested explicitly.
        async for page in self.public_client.list_markets(
            condition_ids=[condition_id], closed=True, page_size=20,
        ):
            pages += 1
            if pages > 5:
                raise RuntimeError("market metadata page limit exceeded")
            items = getattr(page, "items", None)
            if items is None or isinstance(items, (str, bytes)):
                raise RuntimeError("malformed market metadata page")
            matches.extend(m for m in items if str(getattr(m, "condition_id", "")) == condition_id)
            if len(matches) > 1:
                raise RuntimeError("condition resolved to multiple market records")
        if len(matches) != 1:
            raise RuntimeError("condition did not resolve to exactly one market")
        value = getattr(getattr(matches[0], "state", None), "neg_risk", None)
        return value if type(value) is bool else None

    async def fetch_resolved_winner(self, condition_id: str) -> str:
        """Require exactly one closed Gamma market with one-hot outcome prices."""
        matches: list[Any] = []
        pages = 0
        async for page in self.public_client.list_markets(condition_ids=[condition_id], closed=True, page_size=20):
            pages += 1
            if pages > 5:
                raise RuntimeError("resolution market page limit exceeded")
            items = getattr(page, "items", None)
            if items is None or isinstance(items, (str, bytes)):
                raise RuntimeError("malformed resolution market page")
            batch = list(items)
            if len(batch) > 20:
                raise RuntimeError("resolution market page size exceeded")
            matches.extend(m for m in batch if str(getattr(m, "condition_id", "")) == condition_id)
        if len(matches) != 1 or getattr(matches[0].state, "closed", None) is not True:
            raise RuntimeError("condition lacks a unique closed market")
        resolution = getattr(matches[0], "resolution", None)
        status = getattr(resolution, "uma_resolution_status", None)
        if getattr(status, "value", status) != "resolved":
            raise RuntimeError("condition resolution is not finalized")
        outcomes = matches[0].outcomes
        yes, no = outcomes.yes, outcomes.no
        prices = (yes.price, no.price)
        tokens = (yes.token_id, no.token_id)
        if (not all(isinstance(p, Decimal) for p in prices)
                or set(prices) != {Decimal("0"), Decimal("1")}
                or not all(isinstance(t, str) and t for t in tokens)
                or tokens[0] == tokens[1]):
            raise RuntimeError("market lacks an unambiguous one-hot winner")
        return tokens[prices.index(Decimal("1"))]

    async def get_verified_market_context(self, condition_id: str, token_id: str):
        """Resolve a condition and token to fresh public market/book constraints."""
        from .market_context import MarketContext

        if not condition_id or not token_id:
            raise ValueError("condition_id and token_id are required")
        matches = []
        async for page in self.public_client.list_markets(
            condition_ids=[condition_id], page_size=5,
        ):
            for market in page.items:
                if str(getattr(market, "condition_id", "") or "") == condition_id:
                    matches.append(market)
                    if len(matches) > 1:
                        raise RuntimeError("condition resolved to multiple market records")
        if not matches and await self._market_is_closed(condition_id):
            raise MarketClosedError("market closed")
        if len(matches) != 1:
            raise RuntimeError("condition did not resolve to exactly one market")

        fetched_at = datetime.now(timezone.utc)
        book = await self.public_client.get_order_book(token_id=token_id)
        if str(getattr(book, "token_id", "") or "") != token_id:
            raise RuntimeError("order book token does not match requested token")
        # The read time is taken before the request, so age is never understated.
        context = replace(MarketContext.from_sdk(matches[0], book), fetched_at=fetched_at, book=book)
        if not context.condition_matches:
            raise RuntimeError("market and order book condition IDs do not match")
        if not context.token_matches:
            raise RuntimeError("requested token is not one of the market's outcome tokens")
        if context.book_timestamp is None or context.book_timestamp.tzinfo is None:
            raise RuntimeError("order book timestamp is missing or timezone-naive")
        if not context.book_hash:
            raise RuntimeError("order book hash is missing")
        return context

    async def _market_is_closed(self, condition_id: str) -> bool:
        """True only for exactly one explicitly closed market record."""
        matches: list[Any] = []
        async for page in self.public_client.list_markets(
            condition_ids=[condition_id], closed=True, page_size=5,
        ):
            matches.extend(
                market for market in page.items
                if str(getattr(market, "condition_id", "") or "") == condition_id
            )
            if len(matches) > 1:
                return False
        return len(matches) == 1 and getattr(getattr(matches[0], "state", None), "closed", None) is True

    async def get_order_book(self, token_id: str):
        """Read-only typed Decimal order book from CLOB V2."""
        return await self.public_client.get_order_book(token_id=token_id)

    async def fetch_account_order(self, order_id: str) -> RemoteAccountOrder:
        """Read one exact authenticated order record; never submits or cancels it."""
        if not isinstance(order_id, str) or not order_id.strip():
            raise ValueError("order_id must be a nonempty string")
        raw = await self._authenticated_client().get_order(order_id=order_id)
        raw_id = str(getattr(raw, "id", "") or "")
        if raw_id != order_id:
            raise RuntimeError("account order lookup returned a different order ID")
        condition_id = str(
            getattr(raw, "condition_id", None) or getattr(raw, "market", "") or ""
        )
        token_id = str(
            getattr(raw, "token_id", None) or getattr(raw, "asset_id", "") or ""
        )
        try:
            return RemoteAccountOrder(
                order_id=raw_id,
                condition_id=condition_id,
                token_id=token_id,
                side=str(getattr(raw, "side", "") or "").upper(),
                price=Decimal(str(getattr(raw, "price"))),
                original_size=Decimal(str(getattr(raw, "original_size"))),
                size_matched=Decimal(str(getattr(raw, "size_matched"))),
                status=str(getattr(raw, "status", "") or "").upper(),
            )
        except (ArithmeticError, TypeError, ValueError, InvalidOperation) as exc:
            raise RuntimeError("account order detail is malformed") from exc

    async def get_market(self, *, market_id: str | None = None, slug: str | None = None):
        if not market_id and not slug:
            raise ValueError("market_id or slug is required")
        return await self.public_client.get_market(id=market_id, slug=slug)
