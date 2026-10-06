import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal as D
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace as NS

import pytest

from src.v3.ledger import EventLedger, LedgerEvent
from src.v3.live_runner import local_snapshot
from src.v3.live_redemption import RedemptionEvidence, record_verified_redemptions, recognize_remote_redemptions
from src.v3.api import UnifiedPolymarketAPI
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


def test_idempotent_retry_requires_fresh_account_parity():
    ledger = ledger_with_fill()
    original = RemoteSnapshot(D('102.63'), (), ())
    record_verified_redemptions(ledger, StreamEventProcessor(ledger), D('100'), original, (evidence(),))
    with pytest.raises(ValueError, match='lost account parity'):
        record_verified_redemptions(ledger, StreamEventProcessor(ledger), D('100'),
                                    RemoteSnapshot(D('102.64'), (), ()), (evidence(),))


def test_redemption_batch_rolls_back_if_second_insert_fails():
    ledger = EventLedger(':memory:')
    first = LedgerEvent.create('position.redeemed', {'token_id': 'one'})
    existing = LedgerEvent.create('audit.existing', {'token_id': 'two'})
    ledger.append(existing)
    with pytest.raises(Exception):
        ledger.append_batch((first, existing))
    assert not any(e.event_type == 'position.redeemed' for e in ledger.events())


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


class PublicFeed:
    def __init__(self, activities, markets):
        self.activities = activities
        self.markets = markets

    async def list_activity(self, **kwargs):
        assert kwargs['activity_types'] == ['REDEEM', 'DEPOSIT', 'WITHDRAWAL']
        yield NS(items=self.activities)

    async def list_markets(self, **kwargs):
        assert kwargs['closed'] is True
        yield NS(items=self.markets)


def fake_api(*, rows=None, prices=(D('1'), D('0')), closed=True, resolution_status='resolved'):
    api = object.__new__(UnifiedPolymarketAPI)
    api.settings = NS(wallet_address='0x' + '1' * 40)
    activity = NS(type='REDEEM', wallet=api.settings.wallet_address,
                  condition_id='cond', amount=D('5.26'), transaction_hash=TX,
                  timestamp=NOW)
    market = NS(condition_id='cond', state=NS(closed=closed),
                resolution=NS(uma_resolution_status=resolution_status),
                outcomes=NS(yes=NS(price=prices[0], token_id='tok'),
                            no=NS(price=prices[1], token_id='other')))
    api.public_client = PublicFeed([activity] if rows is None else rows, [market])
    return api, activity


async def auto_redeem(ledger, api, remote=None):
    with patch('src.v3.live_redemption_chain.verify_ctf_redemption_transaction', new_callable=AsyncMock):
        return await recognize_remote_redemptions(
            ledger, StreamEventProcessor(ledger), D('100'),
            remote or RemoteSnapshot(D('102.63'), (), ()), api,
            baseline_epoch=int(NOW.timestamp()) - 10, now=NOW,
            reconciler=Reconciler(cash_tolerance=D('0')),
        )


def test_public_evidence_recognizes_once_and_replays():
    ledger = ledger_with_fill()
    api, _ = fake_api()
    assert len(asyncio.run(auto_redeem(ledger, api))) == 1
    assert asyncio.run(auto_redeem(ledger, api)) == ()
    assert local_snapshot(StreamEventProcessor(ledger), D('100')).cash == D('102.63')


@pytest.mark.parametrize('failure', ['missing', 'duplicate', 'wrong_wallet', 'partial',
    'loser', 'unresolved', 'pending_resolution', 'deposit', 'cash', 'position', 'provider'])
def test_automatic_redemption_fails_closed(failure):
    ledger = ledger_with_fill()
    api, activity = fake_api()
    remote = RemoteSnapshot(D('102.63'), (), ())
    if failure == 'missing': api.public_client.activities = []
    if failure == 'duplicate': api.public_client.activities = [activity, activity]
    if failure == 'wrong_wallet': activity.wallet = '0x' + '2' * 40
    if failure == 'partial': activity.amount = D('5.25')
    if failure == 'loser': api.public_client.markets[0].outcomes.yes.price, api.public_client.markets[0].outcomes.no.price = D('0'), D('1')
    if failure == 'unresolved': api.public_client.markets[0].outcomes.no.price = D('0.5')
    if failure == 'pending_resolution': api.public_client.markets[0].resolution.uma_resolution_status = 'proposed'
    if failure == 'deposit': api.public_client.activities.append(NS(type='DEPOSIT', wallet=api.settings.wallet_address,
        timestamp=NOW, transaction_hash='0x' + 'b' * 64))
    if failure == 'cash': remote = RemoteSnapshot(D('102.64'), (), ())
    if failure == 'position': remote = RemoteSnapshot(D('102.63'), (RemotePosition('x', 'x', D('1'), D('1'), D('1')),), ())
    if failure == 'provider':
        async def broken(**kwargs):
            raise RuntimeError('public provider unavailable')
            yield
        api.public_client.list_activity = broken
    with pytest.raises((ValueError, RuntimeError)):
        asyncio.run(auto_redeem(ledger, api, remote))
    assert not any(e.event_type == 'position.redeemed' for e in ledger.events())


def test_public_activity_bound_and_resolution_uniqueness():
    api, activity = fake_api()
    api.public_client.activities = [activity] * 100
    with pytest.raises(RuntimeError, match='bound'):
        asyncio.run(api.fetch_redemption_activity(after=int(NOW.timestamp()) - 1,
                                                  end=int(NOW.timestamp()), max_items=100))
    api.public_client.markets *= 2
    with pytest.raises(RuntimeError, match='unique'):
        asyncio.run(api.fetch_resolved_winner('cond'))


def test_automatic_redemption_ignores_held_remote_token_without_activity_read():
    ledger = ledger_with_fill()
    api, _ = fake_api()
    remote = RemoteSnapshot(D('97.37'),
        (RemotePosition('cond', 'tok', D('5.26'), D('5.26'), D('2.63')),), ())
    assert asyncio.run(auto_redeem(ledger, api, remote)) == ()
    assert not any(e.event_type == 'position.redeemed' for e in ledger.events())
