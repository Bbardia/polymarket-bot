"""Explicit, append-only accounting for independently verified winning-token redemptions.

No market resolution lookup or redemption is performed here. A reviewer must
supply wallet REDEEM activity and an independently verified winning token; the
entire resulting account snapshot must reconcile before any event is appended.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import re
from typing import Iterable

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
    """Replay audit rows against confirmed managed BUY inventory, failing closed."""
    remaining: dict[str, Decimal] = {}
    conditions: dict[str, set[str]] = {}
    for order in processor.orders.values():
        if order.confirmed_size > ZERO:
            if order.side != 'BUY':
                raise ValueError('redemption cannot cover a non-BUY order')
            remaining[order.token_id] = remaining.get(order.token_id, ZERO) + order.confirmed_size
    for event in ledger.events():
        if event.event_type == 'order.accepted':
            p = event.payload
            if p.get('side') == 'BUY' and isinstance(p.get('token_id'), str) and isinstance(p.get('condition_id'), str):
                conditions.setdefault(p['token_id'], set()).add(p['condition_id'])
    redeemed: dict[str, Decimal] = {}
    hashes: set[str] = set()
    payout = ZERO
    for event in ledger.events():
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
    """Explicit offline audit append; never invoked automatically by the runner.

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
    ids = []
    for row in new:
        event = LedgerEvent.create('position.redeemed', row.payload(digest))
        if not ledger.append(event):
            raise ValueError('redemption audit append failed')
        ids.append(event.event_id)
    return tuple(ids)
