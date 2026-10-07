from decimal import Decimal as D

import pytest

from src.v3.live_redemption_chain import (
    BATCH, CTF, PAYOUT_REDEMPTION, PUSD, TRANSFER, confirm_ctf_redemption_receipt,
)

TX = '0x' + 'a' * 64
WALLET = '0x' + '1' * 40
ROUTER = '0x' + '2' * 40
ZERO = '0x' + '0' * 40
TOKEN = '123456789'


def word(value):
    return f'{value:064x}'


def topic(address):
    return '0x' + '0' * 24 + address[2:]


def transfer(sender, recipient, *, token=TOKEN, amount=5260000, contract=CTF):
    # ABI encoding of (uint256[] ids, uint256[] amounts).
    return {'address': contract, 'topics': [BATCH, topic(ROUTER), topic(sender), topic(recipient)],
            'data': '0x' + word(64) + word(128) + word(1) + word(int(token)) + word(1) + word(amount)}


def payout_mint(*, recipient=WALLET, amount=5260000):
    return {'address': PUSD, 'topics': [TRANSFER, topic(ZERO), topic(recipient)],
            'data': '0x' + word(amount)}


def redemption_event(*, condition='c' * 64, redeemer=WALLET, collateral=PUSD,
                     parent='0x' + '0' * 64, amount=5260000, index_sets=(1, 2)):
    return {
        'address': CTF,
        'topics': [PAYOUT_REDEMPTION, topic(redeemer), topic(collateral), parent],
        'data': '0x' + word(int(condition.removeprefix('0x'), 16))
                + word(96) + word(amount) + word(len(index_sets))
                + ''.join(word(i) for i in index_sets),
    }


def receipt(logs):
    return {'status': '0x1', 'transactionHash': TX, 'blockNumber': '0x1',
            'logs': [*logs, redemption_event(), payout_mint()]}


def check(logs):
    return confirm_ctf_redemption_receipt(receipt(logs), tx_hash=TX, wallet=WALLET,
                                          token_id=TOKEN, quantity=D('5.26'),
                                          condition_id='0x' + 'c' * 64)


def test_matching_wallet_outflow_and_burn():
    assert check([transfer(WALLET, ROUTER), transfer(ROUTER, ZERO)])


def test_without_exact_wallet_pusd_credit_is_refused():
    proof = receipt([transfer(WALLET, ROUTER), transfer(ROUTER, ZERO)])
    proof['logs'][-1] = payout_mint(recipient=ROUTER)
    with pytest.raises(ValueError, match='mint'):
        confirm_ctf_redemption_receipt(proof, tx_hash=TX, wallet=WALLET,
                                       token_id=TOKEN, quantity=D('5.26'),
                                       condition_id='0x' + 'c' * 64)


@pytest.mark.parametrize('changed', [
    redemption_event(condition='d' * 64),
    redemption_event(redeemer=ROUTER),
    redemption_event(collateral=ROUTER),
    redemption_event(amount=5260001),
    redemption_event(index_sets=(1,)),
])
def test_payout_event_must_bind_wallet_condition_and_exact_payout(changed):
    proof = receipt([transfer(WALLET, ROUTER), transfer(ROUTER, ZERO)])
    proof['logs'][-2] = changed
    with pytest.raises(ValueError):
        confirm_ctf_redemption_receipt(
            proof, tx_hash=TX, wallet=WALLET, token_id=TOKEN,
            quantity=D('5.26'), condition_id='0x' + 'c' * 64,
        )


@pytest.mark.parametrize('logs', [
    [transfer(ROUTER, ZERO)],
    [transfer(WALLET, ROUTER)],
    [transfer(WALLET, ROUTER, amount=5250000), transfer(ROUTER, ZERO)],
    [transfer(WALLET, ROUTER, token='123'), transfer(ROUTER, ZERO)],
    [transfer(WALLET, ROUTER, contract='0x'+'3'*40), transfer(ROUTER, ZERO)],
    [transfer(WALLET, ROUTER), transfer(WALLET, ROUTER), transfer(ROUTER, ZERO)],
    [transfer(WALLET, ROUTER), transfer('0x'+'4'*40, ZERO)],
    [transfer('0x'+'4'*40, ROUTER), transfer(WALLET, ROUTER), transfer(ROUTER, ZERO)],
])
def test_unattributed_or_duplicate_transfers_fail_closed(logs):
    with pytest.raises(ValueError):
        check(logs)


def test_failed_or_wrong_transaction_cannot_prove_redemption():
    bad = receipt([transfer(WALLET, ROUTER), transfer(ROUTER, ZERO)])
    bad['status'] = '0x0'
    with pytest.raises(ValueError):
        confirm_ctf_redemption_receipt(bad, tx_hash=TX, wallet=WALLET,
                                       token_id=TOKEN, quantity=D('5.26'),
                                       condition_id='0x' + 'c' * 64)
    bad['status'] = '0x1'
    bad['transactionHash'] = '0x' + 'b' * 64
    with pytest.raises(ValueError):
        confirm_ctf_redemption_receipt(bad, tx_hash=TX, wallet=WALLET,
                                       token_id=TOKEN, quantity=D('5.26'),
                                       condition_id='0x' + 'c' * 64)
