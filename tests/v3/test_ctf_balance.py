"""Offline regression tests for fresh, exact CTF inventory proof."""
import asyncio
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from src.v3 import ctf_balance
from src.v3.reconciliation import LocalSnapshot, Reconciler, RemotePosition, RemoteSnapshot

WALLET = "0x" + "2" * 40


class FakeAPI:
    def __init__(self):
        self.settings = SimpleNamespace(wallet_address=WALLET)
        self.calls = []
        self.balances = {"11": D("7.964043"), "22": D("2")}
        self.fail = False

    async def fetch_ctf_balances(self, wallet, tokens):
        self.calls.append((wallet, tokens))
        if self.fail:
            raise TimeoutError("RPC unavailable")
        return dict(self.balances)


def snapshots(display="7.964", cash="10"):
    local = LocalSnapshot(D("10"), frozenset({"11", "22"}), frozenset(),
                          {"11": D("7.964043"), "22": D("2")},
                          {"11": D("3"), "22": D("1")})
    remote = RemoteSnapshot(D(cash), (RemotePosition("c1", "11", D(display), D("4"), D("3")),
                                       RemotePosition("c2", "22", D("2"), D("1"), D("1"))), ())
    return local, remote


def test_three_decimal_display_requires_new_all_managed_proof_every_comparison():
    async def scenario():
        api = FakeAPI()
        local, remote = snapshots()
        reconciler = Reconciler()
        assert not reconciler.compare(local, remote).safe_to_trade
        assert (await reconciler.compare_with_chain(local, remote, api)).safe_to_trade
        assert (await reconciler.compare_with_chain(local, remote, api)).safe_to_trade
        assert api.calls == [(WALLET, ("11", "22"))] * 2
        api.balances["22"] = D("1.999999")
        assert not (await reconciler.compare_with_chain(local, remote, api)).safe_to_trade
        api.balances["22"] = D("2")
        api.balances["11"] = D("7.964")
        assert not (await reconciler.compare_with_chain(local, remote, api)).safe_to_trade
        api.fail = True
        assert not (await reconciler.compare_with_chain(local, remote, api)).safe_to_trade
    asyncio.run(scenario())


def test_all_managed_tokens_must_match_not_only_displayed_token():
    async def scenario():
        quantities = {str(i): D("2") for i in range(1, 37)}
        quantities["1"] = D("7.964043")
        local = LocalSnapshot(D("10"), frozenset(quantities), frozenset(),
                              quantities, {token: D("1") for token in quantities})
        remote = RemoteSnapshot(D("10"), tuple(
            RemotePosition("c" + token, token, D("7.964") if token == "1" else D("2"),
                           D("1"), D("1")) for token in quantities), ())
        api = FakeAPI()
        api.balances = dict(quantities)
        reconciler = Reconciler()
        assert (await reconciler.compare_with_chain(local, remote, api)).safe_to_trade
        assert len(api.calls[0][1]) == 36
        api.balances["36"] = D("1.999999")
        assert not (await reconciler.compare_with_chain(local, remote, api)).safe_to_trade
    asyncio.run(scenario())


@pytest.mark.parametrize("display,cash", [("7.963", "10"), ("7.965", "10"),
                                           ("7.96400", "10"), ("7.964", "9")])
def test_chain_cannot_override_unrelated_mismatch(display, cash):
    async def scenario():
        api = FakeAPI()
        local, remote = snapshots(display, cash)
        assert not (await Reconciler().compare_with_chain(local, remote, api)).safe_to_trade
    asyncio.run(scenario())


def test_rpc_binds_chain_ctf_wallet_token_fresh_block_and_independent_results(monkeypatch):
    calls = []
    def rpc(endpoint, requests):
        calls.append((endpoint, requests))
        if requests[0]["method"] == "eth_chainId":
            return {1: "0x89", 2: {"number": "0x123", "timestamp": hex(int(ctf_balance.time.time()))}}
        assert all(row["params"][0]["to"] == ctf_balance.CTF for row in requests)
        assert all(row["params"][1] == "0x123" for row in requests)
        assert requests[0]["params"][0]["data"] == "0x00fdd58e" + WALLET[2:].zfill(64) + f"{11:064x}"
        return {1: "0x" + f"{7964043:064x}", 2: "0x" + f"{2000000:064x}"}
    monkeypatch.setattr(ctf_balance, "_rpc", rpc)
    assert ctf_balance._read_balances(WALLET, ("11", "22")) == {"11": D("7.964043"), "22": D("2")}
    assert len(calls) == 4
    assert {endpoint for endpoint, _ in calls} == set(ctf_balance.RPC_ENDPOINTS)


@pytest.mark.parametrize("fault", ["wrong_chain", "stale", "disagree", "malformed"])
def test_rpc_rejects_wrong_chain_stale_disagreement_and_malformed(monkeypatch, fault):
    def rpc(endpoint, requests):
        if requests[0]["method"] == "eth_chainId":
            chain = "0x1" if fault == "wrong_chain" else "0x89"
            timestamp = int(ctf_balance.time.time()) - (300 if fault == "stale" else 0)
            return {1: chain, 2: {"number": "0x123", "timestamp": hex(timestamp)}}
        if fault == "malformed":
            return {1: "0x12"}
        value = 7964044 if fault == "disagree" and endpoint == ctf_balance.RPC_ENDPOINTS[1] else 7964043
        return {1: "0x" + f"{value:064x}"}
    monkeypatch.setattr(ctf_balance, "_rpc", rpc)
    with pytest.raises(ValueError):
        ctf_balance._read_balances(WALLET, ("11",))


def test_rpc_rechecks_freshness_after_balance_batches(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(ctf_balance.time, "time", lambda: clock[0])
    def rpc(endpoint, requests):
        if requests[0]["method"] == "eth_chainId":
            return {1: "0x89", 2: {"number": "0x123", "timestamp": hex(1000)}}
        clock[0] = 1600.0
        return {1: "0x" + f"{7964043:064x}"}
    monkeypatch.setattr(ctf_balance, "_rpc", rpc)
    with pytest.raises(ValueError, match="stale"):
        ctf_balance._read_balances(WALLET, ("11",))


def test_chain_proof_cannot_hide_other_incompatible_position(monkeypatch):
    async def scenario():
        api = FakeAPI()
        local, remote = snapshots()
        remote = RemoteSnapshot(remote.cash, (remote.positions[0],
            RemotePosition("c2", "22", D("1.5"), D("1"), D("1"))), ())
        report = await Reconciler().compare_with_chain(local, remote, api)
        assert not report.safe_to_trade
        assert report.proof_failure is not None
        assert report.proof_failure.reason == "proof_ineligible"
        assert report.position_mismatches == ("11", "22")
        assert report.proof_failure.token_ids == ("11", "22")
        assert api.calls == []
    asyncio.run(scenario())


@pytest.mark.parametrize("error,category", [
    (TimeoutError("private key / HTTP body"), "rpc_timeout"),
    (ctf_balance.CTFProofError("rpc_invalid", "invalid CTF RPC response"), "rpc_invalid"),
    (RuntimeError("secret HTTP body"), "rpc_error"),
])
def test_failed_proof_records_fixed_category_and_safe_token_ids(error, category):
    async def scenario():
        api = FakeAPI()
        async def fail(wallet, tokens):
            raise error
        api.fetch_ctf_balances = fail
        local, remote = snapshots()
        report = await Reconciler().compare_with_chain(local, remote, api)
        assert not report.safe_to_trade
        assert report.position_mismatches == ("11",)
        assert report.proof_failure is not None
        assert report.proof_failure.reason == category
        assert report.proof_failure.token_ids == ("11",)
        assert "secret" not in repr(report.proof_failure)
        assert "private" not in repr(report.proof_failure)
    asyncio.run(scenario())


def test_partial_proof_and_chain_disagreement_remain_strict():
    async def scenario():
        api = FakeAPI()
        local, remote = snapshots()
        api.balances.pop("22")
        partial = await Reconciler().compare_with_chain(local, remote, api)
        assert not partial.safe_to_trade
        assert partial.proof_failure is not None
        assert partial.proof_failure.reason == "rpc_incomplete"
        assert partial.proof_failure.token_ids == ("22",)
        api.balances["22"] = D("1.999999")
        drift = await Reconciler().compare_with_chain(local, remote, api)
        assert not drift.safe_to_trade
        assert drift.proof_failure is not None
        assert drift.proof_failure.reason == "rpc_disagreement"
        assert drift.proof_failure.token_ids == ("22",)
        api.balances["22"] = D("2")
        success = await Reconciler().compare_with_chain(local, remote, api)
        assert success.safe_to_trade and success.proof_failure is None
    asyncio.run(scenario())


def test_proof_diagnostic_ids_are_bounded_and_validated():
    async def scenario():
        quantities = {str(i): D("2") for i in range(1, 31)}
        quantities["1"] = D("7.964043")
        quantities["leak:credential"] = D("2")
        local = LocalSnapshot(D("10"), frozenset(quantities), frozenset(), quantities,
                              {token: D("1") for token in quantities})
        remote = RemoteSnapshot(D("10"), tuple(
            RemotePosition("c", token, D("7.964") if token == "1" else D("2"),
                           D("1"), D("1")) for token in quantities), ())
        api = FakeAPI()
        async def reject(wallet, tokens):
            raise ValueError("invalid CTF balance request: leak:credential")
        api.fetch_ctf_balances = reject
        result = await Reconciler().compare_with_chain(local, remote, api)
        assert not result.safe_to_trade
        assert result.proof_failure is not None
        assert result.proof_failure.reason == "rpc_error"
        assert result.proof_failure.token_ids == ("1",)
    asyncio.run(scenario())


def test_proof_diagnostic_ids_never_lose_late_offender_or_include_unaffected_positions():
    async def scenario():
        quantities = {str(i): D("2") for i in range(1, 31)}
        quantities["1"] = D("7.964043")
        local = LocalSnapshot(D("10"), frozenset(quantities), frozenset(), quantities,
                              {token: D("1") for token in quantities})
        remote = RemoteSnapshot(D("10"), tuple(
            RemotePosition("c" + token, token, D("7.964") if token == "1" else D("2"),
                           D("1"), D("1")) for token in quantities), ())
        api = FakeAPI()
        api.balances = dict(quantities)
        reconciler = Reconciler()
        api.fail = True
        timeout = await reconciler.compare_with_chain(local, remote, api)
        assert not timeout.safe_to_trade
        assert timeout.position_mismatches == ("1",)
        assert timeout.proof_failure is not None
        assert timeout.proof_failure.reason == "rpc_timeout"
        assert timeout.proof_failure.token_ids == ("1",)
        api.fail = False
        api.balances["30"] = D("1.999999")
        drift = await reconciler.compare_with_chain(local, remote, api)
        assert not drift.safe_to_trade
        assert drift.proof_failure is not None
        assert drift.proof_failure.reason == "rpc_disagreement"
        assert drift.proof_failure.token_ids == ("30",)
        del api.balances["30"]
        missing = await reconciler.compare_with_chain(local, remote, api)
        assert not missing.safe_to_trade
        assert missing.proof_failure is not None
        assert missing.proof_failure.reason == "rpc_incomplete"
        assert missing.proof_failure.token_ids == ("30",)
        api.balances["30"] = D("NaN")
        invalid = await reconciler.compare_with_chain(local, remote, api)
        assert not invalid.safe_to_trade
        assert invalid.proof_failure is not None
        assert invalid.proof_failure.reason == "rpc_invalid"
        assert invalid.proof_failure.token_ids == ("30",)
        assert all(len(proof.token_ids) <= 16 for proof in
                   (timeout.proof_failure, drift.proof_failure,
                    missing.proof_failure, invalid.proof_failure))
        api.balances = {token: D("1.999999") for token in quantities}
        widespread = await reconciler.compare_with_chain(local, remote, api)
        assert not widespread.safe_to_trade
        assert widespread.proof_failure is not None
        assert widespread.proof_failure.reason == "rpc_disagreement"
        assert widespread.proof_failure.token_ids == tuple(sorted(quantities)[:16])
        api.balances = dict(quantities)
        api.balances["999"] = D("2")
        extra = await reconciler.compare_with_chain(local, remote, api)
        assert not extra.safe_to_trade
        assert extra.proof_failure is not None
        assert extra.proof_failure.reason == "rpc_incomplete"
        assert extra.proof_failure.token_ids == ("1",)
    asyncio.run(scenario())


@pytest.mark.parametrize("fault,category", [
    ("malformed", "rpc_invalid"), ("stale", "rpc_stale"),
    ("disagree", "rpc_disagreement"),
])
def test_rpc_proof_errors_carry_fixed_categories(monkeypatch, fault, category):
    def rpc(endpoint, requests):
        if requests[0]["method"] == "eth_chainId":
            stamp = int(ctf_balance.time.time()) - (300 if fault == "stale" else 0)
            return {1: "0x89", 2: {"number": "0x123", "timestamp": hex(stamp)}}
        if fault == "malformed":
            raise ctf_balance.CTFProofError("rpc_invalid", "invalid CTF RPC response")
        value = 7964044 if endpoint == ctf_balance.RPC_ENDPOINTS[1] else 7964043
        return {1: "0x" + f"{value:064x}"}
    monkeypatch.setattr(ctf_balance, "_rpc", rpc)
    with pytest.raises(ctf_balance.CTFProofError) as raised:
        ctf_balance._read_balances(WALLET, ("11",))
    assert raised.value.category == category
