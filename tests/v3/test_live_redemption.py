from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal as D

import pytest

from src.v3.ledger import EventLedger, LedgerEvent
from src.v3.live_runner import local_snapshot
from src.v3.live_redemption import RedemptionEvidence, record_verified_redemptions
from src.v3.reconciliation import LocalSnapshot, Reconciler, RemotePosition, RemoteSnapshot
from src.v3.streaming import StreamEventProcessor

NOW = datetime(2026, 10, 5, 16, 38, 29, tzinfo=timezone.utc)
TX = '0x' + 'a' * 64


def ledger_with_fill():
    ledger = EventLedger(':memory:')
    ledger.append(LedgerEvent.create('order.accepted', {
        'client_order_id': 'c1', 'order_id': 'o1', 'status': 'live',
        'condition_id': 'cond', 'token_id': 'tok', 'side': 'BUY',
        'price': '0.50', 'requested_size': '5.26', 'post_only': True,
    }))
    ledger.append(LedgerEvent.create('user.trade', {
        'id': 't1', 'taker_order_id': 'o1', 'market': 'cond', 'asset_id': 'tok',
        'side': 'BUY', 'size': '5.26', 'price': '0.50', 'status': 'CONFIRMED',
        'owner': 'wallet', 'fee_rate_bps': '0',
    }))
    return ledger


def evidence(**changes):
    return replace(RedemptionEvidence(
        condition_id='cond', token_id='tok', quantity=D('5.26'),
        payout=D('5.26'), transaction_hash=TX, redeemed_at=NOW,
        activity_type='REDEEM', winning_token_id='tok',
    ), **changes)


def test_redemption_requires_explicit_evidence_and_full_parity():
    ledger = ledger_with_fill()
    processor = StreamEventProcessor(ledger)
    before = local_snapshot(processor, D('100'))
    assert before.position_tokens == frozenset({'tok'})
    remote = RemoteSnapshot(D('102.63'), (), ())
    assert not Reconciler().compare(before, remote).safe_to_trade
    records = record_verified_redemptions(ledger, processor, D('100'), remote, (evidence(),))
    assert len(records) == 1
    after = local_snapshot(StreamEventProcessor(ledger), D('100'))
    assert after.cash == D('102.63') and after.position_tokens == frozenset()
    assert Reconciler().compare(after, remote).safe_to_trade
    assert record_verified_redemptions(ledger, StreamEventProcessor(ledger), D('100'), remote, (evidence(),)) == ()


@pytest.mark.parametrize('changes', [
    {'activity_type': 'GAMMA'}, {'winning_token_id': 'other'},
    {'payout': D('5.25')}, {'quantity': D('5.25')},
    {'transaction_hash': 'not-a-tx'},
    {'redeemed_at': NOW.replace(tzinfo=None)},
])
def test_invalid_evidence_cannot_append(changes):
    ledger = ledger_with_fill()
    with pytest.raises(ValueError):
        record_verified_redemptions(ledger, StreamEventProcessor(ledger), D('100'),
                                    RemoteSnapshot(D('102.63'), (), ()), (evidence(**changes),))
    assert not any(e.event_type == 'position.redeemed' for e in ledger.events())


@pytest.mark.parametrize('remote', [
    RemoteSnapshot(D('102.62'), (), ()),
    RemoteSnapshot(D('102.63'), (RemotePosition('cond', 'tok', D('5.26'), D('5.26'), D('2.63')),), ()),
    RemoteSnapshot(D('102.63'), (RemotePosition('other', 'unknown', D('1'), D('1'), D('1')),), ()),
])
def test_redemption_refuses_snapshot_mismatches(remote):
    ledger = ledger_with_fill()
    with pytest.raises(ValueError):
        record_verified_redemptions(ledger, StreamEventProcessor(ledger), D('100'), remote, (evidence(),))
    assert not any(e.event_type == 'position.redeemed' for e in ledger.events())


def test_replay_rejects_tampering_duplicate_and_unattributed_redemption():
    ledger = ledger_with_fill()
    record_verified_redemptions(ledger, StreamEventProcessor(ledger), D('100'),
                                RemoteSnapshot(D('102.63'), (), ()), (evidence(),))
    ledger.append(LedgerEvent.create('position.redeemed', {'condition_id': 'cond', 'token_id': 'tok',
        'quantity': '5.26', 'payout': '5.26', 'transaction_hash': '0x' + 'b' * 64,
        'redeemed_at': NOW.isoformat(), 'activity_type': 'REDEEM', 'winning_token_id': 'tok'}))
    with pytest.raises(ValueError):
        local_snapshot(StreamEventProcessor(ledger), D('100'))


def test_two_distinct_redemptions_reconcile_together_only():
    ledger = ledger_with_fill()
    ledger.append(LedgerEvent.create('order.accepted', {
        'client_order_id': 'c2', 'order_id': 'o2', 'status': 'live',
        'condition_id': 'cond-2', 'token_id': 'tok-2', 'side': 'BUY',
        'price': '0.50', 'requested_size': '5.96', 'post_only': True,
    }))
    ledger.append(LedgerEvent.create('user.trade', {
        'id': 't2', 'taker_order_id': 'o2', 'market': 'cond-2', 'asset_id': 'tok-2',
        'side': 'BUY', 'size': '5.96', 'price': '0.50', 'status': 'CONFIRMED',
        'owner': 'wallet', 'fee_rate_bps': '0',
    }))
    processor = StreamEventProcessor(ledger)
    assert local_snapshot(processor, D('100')).cash == D('94.39')
    remote = RemoteSnapshot(D('105.61'), (), ())
    second = replace(evidence(), condition_id='cond-2', token_id='tok-2',
                     winning_token_id='tok-2', quantity=D('5.96'), payout=D('5.96'),
                     transaction_hash='0x' + 'b' * 64,
                     redeemed_at=datetime(2026, 10, 5, 16, 20, 50, tzinfo=timezone.utc))
    with pytest.raises(ValueError):
        record_verified_redemptions(ledger, processor, D('100'), remote, (evidence(),))
    assert not any(e.event_type == 'position.redeemed' for e in ledger.events())
    ids = record_verified_redemptions(ledger, processor, D('100'), remote, (evidence(), second))
    assert len(ids) == 2
    assert local_snapshot(StreamEventProcessor(ledger), D('100')).cash == D('105.61')
    assert Reconciler(cash_tolerance=D('0')).compare(local_snapshot(StreamEventProcessor(ledger), D('100')), remote).safe_to_trade
    assert len([e for e in ledger.events() if e.event_type == 'position.redeemed']) == 2


def test_redemption_requires_exact_cash_even_with_inflow_waiver():
    ledger = ledger_with_fill()
    with pytest.raises(ValueError):
        record_verified_redemptions(ledger, StreamEventProcessor(ledger), D('100'),
                                    RemoteSnapshot(D('102.64'), (), ()), (evidence(),),
                                    reconciler=Reconciler(allow_cash_inflows=True))


def test_quantity_display_precision_is_narrow_and_cash_remains_strict():
    local = LocalSnapshot(D('98.344471'), frozenset({'tok'}), frozenset(),
                          {'tok': D('5.173528')}, {'tok': D('1.655529')})
    remote = RemoteSnapshot(D('98.344471'),
        (RemotePosition('cond', 'tok', D('5.1735'), D('2'), D('1.6555')),), ())
    reconciler = Reconciler(cost_tolerance=D('0.01'))
    assert reconciler.compare(local, remote).safe_to_trade
    for changed in (
        replace(remote, cash=D('98.33')),
        replace(remote, positions=(replace(remote.positions[0], size=D('5.1736')),)),
        replace(remote, positions=(replace(remote.positions[0], size=D('5.17')),)),
        replace(remote, positions=(replace(remote.positions[0], initial_value=D('1.64')),)),
    ):
        assert not reconciler.compare(local, changed).safe_to_trade
