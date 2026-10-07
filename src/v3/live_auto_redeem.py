"""Fail-closed, bot-managed auto-redemption for finalized winning CTF outcomes."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from .ledger import EventLedger, LedgerEvent
from .reconciliation import Reconciler, RemoteSnapshot
from .streaming import StreamEventProcessor

ZERO = Decimal("0")


@dataclass(frozen=True)
class ManagedRedemptionCandidate:
    condition_id: str
    token_id: str
    quantity: Decimal


def pending_auto_redemption_tokens(ledger: EventLedger) -> frozenset[str]:
    """Return submission intents not followed by matching redemption evidence."""
    pending: list[tuple[str, str, str]] = []
    for event in ledger.events():
        payload = event.payload
        if event.event_type == "auto_redemption.submission_started":
            token, condition, quantity = (
                payload.get("token_id"), payload.get("condition_id"), payload.get("quantity"),
            )
            if (not isinstance(token, str) or not token
                    or not isinstance(condition, str) or not condition
                    or not isinstance(quantity, str) or not quantity):
                raise ValueError("malformed auto-redemption submission intent")
            pending.append((token, condition, quantity))
        elif event.event_type == "position.redeemed":
            token, condition, quantity = (
                payload.get("token_id"), payload.get("condition_id"), payload.get("quantity"),
            )
            if not all(isinstance(value, str) and value for value in (token, condition, quantity)):
                continue
            key = (token, condition, quantity)
            for index in range(len(pending) - 1, -1, -1):
                if pending[index] == key:
                    del pending[index]
                    break
    return frozenset(token for token, _, _ in pending)


def managed_winning_candidates(
    ledger: EventLedger,
    processor: StreamEventProcessor,
    baseline_cash: Decimal,
    remote: RemoteSnapshot,
    reconciler: Reconciler,
    external_condition_ids: frozenset[str],
) -> tuple[ManagedRedemptionCandidate, ...]:
    """Select exact, reconciled bot holdings that are redeemable and marked > 0.

    Zero-value losing tokens are deliberately not burned. Conditions with any
    external ownership, unresolved prior submissions, or account mismatch are
    excluded; winner identity and market type must be independently checked by
    the caller before submitting.
    """
    from .live_runner import local_snapshot

    if processor.reconciliation_required:
        return ()
    local = local_snapshot(processor, baseline_cash)
    if local.position_quantities is None:
        return ()
    report = reconciler.compare(local, remote)
    if not report.safe_to_trade:
        return ()
    external_conditions = set(external_condition_ids) | {
        position.condition_id for position in report.external_positions
    }
    pending = pending_auto_redemption_tokens(ledger)
    token_conditions: dict[str, set[str]] = {}
    for event in ledger.events():
        if event.event_type != "order.accepted" or event.payload.get("side") != "BUY":
            continue
        token, condition = event.payload.get("token_id"), event.payload.get("condition_id")
        if isinstance(token, str) and token and isinstance(condition, str) and condition:
            token_conditions.setdefault(token, set()).add(condition)
    remote_positions = {position.token_id: position for position in remote.positions}
    conditions_with_open_orders = {order.condition_id for order in remote.open_orders}
    managed_tokens_by_condition: dict[str, set[str]] = {}
    for token in local.position_quantities:
        for condition in token_conditions.get(token, set()):
            managed_tokens_by_condition.setdefault(condition, set()).add(token)
    candidates: list[ManagedRedemptionCandidate] = []
    for token, quantity in sorted(local.position_quantities.items()):
        conditions = token_conditions.get(token, set())
        if len(conditions) != 1 or token in pending:
            continue
        condition = next(iter(conditions))
        remote_condition_tokens = {
            position.token_id for position in remote.positions
            if position.condition_id == condition
        }
        if (condition in external_conditions or condition in conditions_with_open_orders
                or len(managed_tokens_by_condition.get(condition, ())) != 1
                or remote_condition_tokens != {token}):
            continue
        position = remote_positions.get(token)
        if (position is None or position.condition_id != condition
                or position.size != quantity or not position.redeemable
                or position.current_value <= ZERO):
            continue
        candidates.append(ManagedRedemptionCandidate(condition, token, quantity))
    # Redemptions are condition-wide. Never submit if two managed tokens share
    # a condition, even if only one currently appears to be winning.
    by_condition: dict[str, int] = {}
    for candidate in candidates:
        by_condition[candidate.condition_id] = by_condition.get(candidate.condition_id, 0) + 1
    return tuple(c for c in candidates if by_condition[c.condition_id] == 1)


async def auto_redeem_one_managed_winner(
    *,
    ledger: EventLedger,
    processor: StreamEventProcessor,
    baseline_cash: Decimal,
    remote: RemoteSnapshot,
    api: Any,
    reconciler: Reconciler,
    external_condition_ids: frozenset[str],
    baseline_epoch: int,
    now: datetime,
) -> tuple[RemoteSnapshot, tuple[str, ...], str | None]:
    """Submit at most one strictly verified winning holding; never retry ambiguously."""
    candidates = managed_winning_candidates(
        ledger, processor, baseline_cash, remote, reconciler, external_condition_ids,
    )
    pending = pending_auto_redemption_tokens(ledger)
    if pending:
        # Existing redemption recognizer runs before this helper. An unresolved
        # intent is never resubmitted automatically after a timeout/crash.
        return remote, (), "auto-redemption submission needs manual reconciliation"
    if candidates:
        settings = getattr(api, "settings", None)
        if not all(getattr(settings, name, "") for name in (
            "builder_api_key", "builder_secret", "builder_passphrase",
        )):
            raise RuntimeError("auto-redemption requires complete Builder relay credentials")
    for candidate in candidates:
        if await api.fetch_market_is_neg_risk(candidate.condition_id) is not False:
            continue
        winner = await api.fetch_resolved_winner(candidate.condition_id)
        if winner != candidate.token_id:
            continue
        # Persist before crossing the submission boundary. If submission times
        # out, this durable intent prevents a second transaction on restart.
        ledger.append(LedgerEvent.create("auto_redemption.submission_started", {
            "condition_id": candidate.condition_id,
            "token_id": candidate.token_id,
            "quantity": str(candidate.quantity),
            "at": now.isoformat(),
        }))
        client = api._authenticated_client()
        handle = await client.redeem_positions(
            condition_id=candidate.condition_id,
            metadata="Auto-redeem verified bot-managed winning position",
        )
        submitted_hash = str(getattr(handle, "transaction_hash", "") or "")
        submitted_id = str(getattr(handle, "transaction_id", "") or "")
        ledger.append(LedgerEvent.create("auto_redemption.submission_accepted", {
            "condition_id": candidate.condition_id,
            "token_id": candidate.token_id,
            "quantity": str(candidate.quantity),
            "transaction_hash": submitted_hash,
            "transaction_id": submitted_id,
            "at": now.isoformat(),
        }))
        outcome = await handle.wait()
        transaction_hash = str(getattr(outcome, "transaction_hash", "") or "")
        if not transaction_hash or (submitted_hash and transaction_hash.lower() != submitted_hash.lower()):
            raise RuntimeError("redemption transaction hash is missing or changed")
        from .live_redemption_chain import verify_ctf_redemption_transaction
        await verify_ctf_redemption_transaction(
            tx_hash=transaction_hash,
            wallet=api.settings.wallet_address,
            token_id=candidate.token_id,
            quantity=candidate.quantity,
            condition_id=candidate.condition_id,
        )
        fresh_remote = await api.fetch_remote_snapshot()
        from .live_redemption import recognize_remote_redemptions
        fresh_processor = StreamEventProcessor(ledger)
        appended = await recognize_remote_redemptions(
            ledger, fresh_processor, baseline_cash, fresh_remote, api,
            baseline_epoch=baseline_epoch, now=now, reconciler=reconciler,
        )
        if not appended:
            raise RuntimeError("submitted redemption lacks independently verified accounting")
        return fresh_remote, appended, None
    return remote, (), None
