"""Append-only accounting for verified winning-token redemptions.

The automatic runner derives wallet activity and a unique resolved winner from
public read-only providers; the entire resulting account snapshot must reconcile
before any audit event is appended. Offline callers may supply the same evidence.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import re
from typing import Any, Iterable

from .ledger import EventLedger, LedgerEvent
from .reconciliation import Reconciler, RemoteSnapshot
from .streaming import StreamEventProcessor, remote_snapshot_sha256

ZERO = Decimal('0')
_HASH = re.compile(r'0x[0-9a-fA-F]{64}\Z')
_DIGEST = re.compile(r'[0-9a-f]{64}\Z')


@dataclass(frozen=True)
class RedemptionEvidence:
    condition_id: str
    token_id: str
    quantity: Decimal
    payout: Decimal
    transaction_hash: str
    redeemed_at: datetime
    activity_type: str
    winning_token_id: str

    def __post_init__(self) -> None:
        if (not all(isinstance(x, str) and x.strip() for x in
                    (self.condition_id, self.token_id, self.winning_token_id))
                or self.token_id != self.winning_token_id
                or self.activity_type != 'REDEEM'
                or not isinstance(self.transaction_hash, str)
                or not _HASH.fullmatch(self.transaction_hash)
                or not isinstance(self.redeemed_at, datetime)
                or self.redeemed_at.tzinfo is None
                or self.redeemed_at.utcoffset() is None
                or not isinstance(self.quantity, Decimal)
                or not self.quantity.is_finite() or self.quantity <= ZERO
                or not isinstance(self.payout, Decimal)
                or not self.payout.is_finite() or self.payout != self.quantity):
            raise ValueError('redemption requires exact wallet activity and winning-token evidence')

    def payload(self, snapshot_digest: str) -> dict[str, str]:
        return {
            'condition_id': self.condition_id, 'token_id': self.token_id,
            'quantity': str(self.quantity), 'payout': str(self.payout),
            'transaction_hash': self.transaction_hash,
            'redeemed_at': self.redeemed_at.isoformat(),
            'activity_type': self.activity_type, 'winning_token_id': self.winning_token_id,
            'account_snapshot_sha256': snapshot_digest,
        }


def _parse(event: LedgerEvent) -> RedemptionEvidence:
    p = event.payload
    if set(p) != {
        'condition_id', 'token_id', 'quantity', 'payout', 'transaction_hash',
        'redeemed_at', 'activity_type', 'winning_token_id', 'account_snapshot_sha256',
    } or not isinstance(p.get('account_snapshot_sha256'), str) or not _DIGEST.fullmatch(p['account_snapshot_sha256']):
        raise ValueError('malformed redemption audit event')
    try:
        if not all(isinstance(p[key], str) for key in p):
            raise ValueError('non-string redemption audit field')
        return RedemptionEvidence(
            condition_id=p['condition_id'], token_id=p['token_id'],
            quantity=Decimal(p['quantity']), payout=Decimal(p['payout']),
            transaction_hash=p['transaction_hash'],
            redeemed_at=datetime.fromisoformat(p['redeemed_at']),
            activity_type=p['activity_type'], winning_token_id=p['winning_token_id'],
        )
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise ValueError('malformed redemption audit event') from exc


def redemption_adjustments(ledger: EventLedger, processor: StreamEventProcessor) -> tuple[dict[str, Decimal], Decimal]:
    """Replay audit rows against chronologically confirmed managed inventory."""
    from .live_accounting import confirmed_fills

    remaining: dict[str, Decimal] = {}
    conditions: dict[str, set[str]] = {}
    events = tuple(ledger.events())
    for event in events:
        if event.event_type == 'order.accepted':
            payload = event.payload
            if (payload.get('side') == 'BUY'
                    and isinstance(payload.get('token_id'), str)
                    and isinstance(payload.get('condition_id'), str)):
                conditions.setdefault(payload['token_id'], set()).add(payload['condition_id'])

    try:
        fills = confirmed_fills(processor)
    except (ArithmeticError, KeyError, TypeError, ValueError):
        processor.require_reconciliation('confirmed fill chronology or ledger association is invalid')
        raise ValueError('confirmed fill chronology or ledger association is invalid')
    for fill in fills:
        if fill.side == 'BUY':
            remaining[fill.token_id] = remaining.get(fill.token_id, ZERO) + fill.size
        elif fill.side == 'SELL':
            held = remaining.get(fill.token_id, ZERO)
            if fill.size > held or held <= ZERO:
                processor.require_reconciliation(
                    'chronological confirmed SELL exceeds managed inventory during redemption replay'
                )
                raise ValueError('confirmed SELL exceeds managed inventory')
            quantity = held - fill.size
            if quantity:
                remaining[fill.token_id] = quantity
            else:
                remaining.pop(fill.token_id, None)
        else:
            raise ValueError('redemption cannot cover an invalid order side')

    redeemed: dict[str, Decimal] = {}
    hashes: set[str] = set()
    payout = ZERO
    for event in events:
        if event.event_type != 'position.redeemed':
            continue
        row = _parse(event)
        if (row.transaction_hash.lower() in hashes
                or conditions.get(row.token_id) != {row.condition_id}
                or row.token_id in redeemed
                or remaining.get(row.token_id, ZERO) != row.quantity):
            raise ValueError('redemption is duplicate, unattributed, or not an exact full holding')
        hashes.add(row.transaction_hash.lower())
        redeemed[row.token_id] = row.quantity
        payout += row.payout
    return redeemed, payout


def record_verified_redemptions(
    ledger: EventLedger, processor: StreamEventProcessor, baseline_cash: Decimal,
    remote: RemoteSnapshot, evidence: Iterable[RedemptionEvidence], *,
    reconciler: Reconciler | None = None,
) -> tuple[str, ...]:
    """Atomic audit append after exact full-account parity.

    Caller must independently check that each wallet activity is for the account
    and condition and that winning_token_id is the one-hot resolved winner.
    This function requires exact full-account cash parity (not an inflow waiver).
    """
    from .live_runner import local_snapshot
    if processor.reconciliation_required:
        raise ValueError('lifecycle reconciliation is latched')
    existing, _ = redemption_adjustments(ledger, processor)
    rows = tuple(evidence)
    if not rows:
        return ()
    for row in rows:
        if not isinstance(row, RedemptionEvidence):
            raise ValueError('unverified redemption evidence')
        row.__post_init__()  # guard against object mutation/bypassed construction
    if (len({r.token_id for r in rows}) != len(rows)
            or len({r.transaction_hash.lower() for r in rows}) != len(rows)):
        raise ValueError('duplicate redemption evidence')
    current = local_snapshot(processor, baseline_cash)
    new = [r for r in rows if r.token_id not in existing]
    if not new:
        recorded = {e.payload['token_id']: e.payload for e in ledger.events()
                    if e.event_type == 'position.redeemed'}
        if all(r.token_id in recorded and all(
            recorded[r.token_id].get(key) == value
            for key, value in r.payload(recorded[r.token_id]['account_snapshot_sha256']).items()
        ) for r in rows):
            comparator = reconciler or Reconciler(cash_tolerance=ZERO)
            report = comparator.compare(current, remote)
            if not report.safe_to_trade or report.cash_delta != ZERO:
                raise ValueError('idempotent redemption retry has lost account parity')
            return ()
        raise ValueError('conflicting redemption evidence')
    if len(new) != len(rows):
        raise ValueError('partially applied redemption batch')
    old_hashes = { _parse(e).transaction_hash.lower() for e in ledger.events() if e.event_type == 'position.redeemed' }
    if any(r.transaction_hash.lower() in old_hashes for r in new):
        raise ValueError('reused redemption transaction')
    conditions: dict[str, set[str]] = {}
    for e in ledger.events():
        if e.event_type == 'order.accepted' and e.payload.get('side') == 'BUY':
            conditions.setdefault(e.payload.get('token_id'), set()).add(e.payload.get('condition_id'))
    quantities = dict(current.position_quantities or {})
    costs = dict(current.position_cost_basis or {})
    for row in new:
        if (conditions.get(row.token_id) != {row.condition_id}
                or quantities.get(row.token_id) != row.quantity):
            raise ValueError('redemption does not cover an exact managed holding')
        quantities.pop(row.token_id)
        costs.pop(row.token_id)
    from .reconciliation import LocalSnapshot
    candidate = LocalSnapshot(
        cash=current.cash + sum((r.payout for r in new), ZERO),
        position_tokens=frozenset(quantities), order_ids=current.order_ids,
        position_quantities=quantities, position_cost_basis=costs,
    )
    comparator = reconciler or Reconciler(cash_tolerance=ZERO)
    report = comparator.compare(candidate, remote)
    if not report.safe_to_trade or report.cash_delta != ZERO:
        raise ValueError('full account cash, positions, or orders fail reconciliation')
    digest = remote_snapshot_sha256(remote)
    events = tuple(LedgerEvent.create('position.redeemed', row.payload(digest)) for row in new)
    return ledger.append_batch(events)


async def recognize_remote_redemptions(
    ledger: EventLedger, processor: StreamEventProcessor, baseline_cash: Decimal,
    remote: RemoteSnapshot, api: Any, *, baseline_epoch: int, now: datetime,
    reconciler: Reconciler,
) -> tuple[str, ...]:
    """Append only proven full winning-token redemptions with exact account parity.

    Public REDEEM activities are condition-level cash amounts (no token field);
    a unique locally held token and independently resolved Gamma winner provide
    the token identity. Any incomplete/ambiguous evidence blocks the entire batch.
    """
    if (processor.reconciliation_required or not isinstance(now, datetime)
            or now.tzinfo is None or now.utcoffset() is None):
        raise ValueError('redemption requires a valid lifecycle and time')
    if type(baseline_epoch) is not int or baseline_epoch < 0 or now.timestamp() < baseline_epoch:
        raise ValueError('invalid redemption baseline')
    from .live_runner import local_snapshot
    current = local_snapshot(processor, baseline_cash)
    if current.position_quantities is None:
        raise ValueError('managed position quantities unavailable')
    missing = set(current.position_tokens) - {p.token_id for p in remote.positions}
    if not missing:
        return ()
    conditions: dict[str, set[str]] = {}
    for event in ledger.events():
        if event.event_type == 'order.accepted' and event.payload.get('side') == 'BUY':
            token_id, condition_id = event.payload.get('token_id'), event.payload.get('condition_id')
            if isinstance(token_id, str) and isinstance(condition_id, str):
                conditions.setdefault(token_id, set()).add(condition_id)
    missing_conditions: dict[str, str] = {}
    for token in missing:
        ids = conditions.get(token)
        if ids is None or len(ids) != 1:
            raise ValueError('missing token lacks unique managed condition')
        condition = next(iter(ids))
        if condition in reconciler.external_condition_ids:
            raise ValueError('redemption condition overlaps external inventory')
        if condition in missing_conditions:
            raise ValueError('multiple missing tokens share a condition')
        missing_conditions[condition] = token
    activities = await api.fetch_redemption_activity(after=baseline_epoch, end=int(now.timestamp()))
    wallet = str(api.settings.wallet_address or '').lower()
    if not wallet:
        raise ValueError('redemption wallet missing')
    by_condition: dict[str, list[Any]] = {condition: [] for condition in missing_conditions}
    for row in activities:
        kind = getattr(row, 'type', None)
        timestamp = getattr(row, 'timestamp', None)
        tx = getattr(row, 'transaction_hash', None)
        if (kind not in {'REDEEM', 'DEPOSIT', 'WITHDRAWAL'}
                or not isinstance(getattr(row, 'wallet', None), str)
                or row.wallet.lower() != wallet
                or not isinstance(timestamp, datetime) or timestamp.tzinfo is None
                or not baseline_epoch <= timestamp.timestamp() <= now.timestamp()
                or not isinstance(tx, str) or not _HASH.fullmatch(tx)):
            raise ValueError('invalid wallet activity evidence')
        if kind != 'REDEEM':
            raise ValueError('unknown cash flow prevents automatic redemption recognition')
        condition = getattr(row, 'condition_id', None)
        amount = getattr(row, 'amount', None)
        if (not isinstance(condition, str) or not condition
                or not isinstance(amount, Decimal) or not amount.is_finite() or amount <= ZERO):
            raise ValueError('malformed redemption activity')
        if condition in by_condition:
            by_condition[condition].append(row)
    evidence = []
    hashes: set[str] = set()
    for condition, token in sorted(missing_conditions.items()):
        rows = by_condition[condition]
        if len(rows) != 1:
            raise ValueError('missing token has no unique wallet redemption')
        row = rows[0]
        if row.transaction_hash.lower() in hashes:
            raise ValueError('shared redemption transaction is ambiguous')
        hashes.add(row.transaction_hash.lower())
        quantity = current.position_quantities[token]
        if row.amount != quantity:
            raise ValueError('redemption payout does not equal full managed holding')
        winner = await api.fetch_resolved_winner(condition)
        if winner != token:
            raise ValueError('missing token is not the unique resolved winner')
        from .live_redemption_chain import verify_ctf_redemption_transaction
        await verify_ctf_redemption_transaction(
            tx_hash=row.transaction_hash, wallet=wallet,
            token_id=token, quantity=quantity, condition_id=condition,
        )
        evidence.append(RedemptionEvidence(
            condition_id=condition, token_id=token, quantity=quantity,
            payout=row.amount, transaction_hash=row.transaction_hash,
            redeemed_at=row.timestamp.astimezone(timezone.utc),
            activity_type='REDEEM', winning_token_id=winner,
        ))
    return record_verified_redemptions(
        ledger, processor, baseline_cash, remote, evidence, reconciler=reconciler,
    )
