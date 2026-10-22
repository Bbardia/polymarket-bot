"""Fresh, independent read-only Polygon CTF ERC-1155 inventory proof."""
from __future__ import annotations

import asyncio
import re
import time
from decimal import Decimal

import requests

CTF = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
RPC_ENDPOINTS = ("https://polygon-bor-rpc.publicnode.com", "https://polygon.drpc.org")
BALANCE_OF = "00fdd58e"
UNITS = Decimal("1000000")
WALLET = re.compile(r"0x[0-9a-fA-F]{40}\Z")
TOKEN = re.compile(r"[0-9]+\Z")


def _rpc(endpoint: str, calls: list[dict]) -> dict[int, object]:
    session = requests.Session()
    session.trust_env = False
    try:
        response = session.post(endpoint, json=calls, timeout=(5, 15))
        response.raise_for_status()
        if len(response.content) > 250_000:
            raise ValueError("CTF RPC response exceeds size bound")
        payload = response.json()
    finally:
        session.close()
    if not isinstance(payload, list) or len(payload) != len(calls):
        raise ValueError("incomplete CTF RPC batch")
    results: dict[int, object] = {}
    for item in payload:
        if (not isinstance(item, dict) or item.get("jsonrpc") != "2.0"
                or type(item.get("id")) is not int or item["id"] in results
                or item.get("error") is not None):
            raise ValueError("invalid CTF RPC response")
        results[item["id"]] = item.get("result")
    if set(results) != {call["id"] for call in calls}:
        raise ValueError("CTF RPC request mismatch")
    return results


def _hex(value: object, *, word: bool = False) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]+", value):
        raise ValueError("invalid CTF RPC hex")
    if word and len(value) != 66:
        raise ValueError("invalid ERC-1155 ABI word")
    return int(value, 16)


def _read_balances(wallet: str, tokens: tuple[str, ...]) -> dict[str, Decimal]:
    if (not isinstance(wallet, str) or not WALLET.fullmatch(wallet)
            or not tokens or len(tokens) > 500 or len(set(tokens)) != len(tokens)
            or any(not isinstance(t, str) or not TOKEN.fullmatch(t) or int(t) >= 2**256
                   for t in tokens)):
        raise ValueError("invalid CTF balance request")
    started = time.monotonic()
    snapshots = []
    for endpoint in RPC_ENDPOINTS:
        header = _rpc(endpoint, [
            {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []},
            {"jsonrpc": "2.0", "id": 2, "method": "eth_getBlockByNumber", "params": ["latest", False]},
        ])
        if _hex(header[1]) != 137 or not isinstance(header[2], dict):
            raise ValueError("CTF RPC wrong chain or malformed block")
        block = header[2]
        height, stamp = _hex(block.get("number")), _hex(block.get("timestamp"))
        # Bor block timestamps may run a few seconds ahead of wall time.
        if not (-5 <= time.time() - stamp <= 45):
            raise ValueError("CTF RPC stale or future block")
        calls = []
        for index, token in enumerate(tokens, 1):
            data = "0x" + BALANCE_OF + wallet[2:].zfill(64) + f"{int(token):064x}"
            calls.append({"jsonrpc": "2.0", "id": index, "method": "eth_call",
                          "params": [{"to": CTF, "data": data}, hex(height)]})
        # Public RPCs may reject large batches; bind each small batch to the
        # same block so a position cannot change midway through this proof.
        balances = {}
        for offset in range(0, len(calls), 4):
            balances.update(_rpc(endpoint, calls[offset:offset + 4]))
        snapshots.append((height, stamp, {token: Decimal(_hex(balances[index], word=True)) / UNITS
                                          for index, token in enumerate(tokens, 1)}))
    if (time.monotonic() - started > 30
            or any(not (-5 <= time.time() - stamp <= 45) for _, stamp, _ in snapshots)
            or abs(snapshots[0][0] - snapshots[1][0]) > 12
            or snapshots[0][2] != snapshots[1][2]):
        raise ValueError("independent CTF RPC balances disagree or proof is stale")
    return snapshots[0][2]


async def read_ctf_balances(wallet: str, tokens: tuple[str, ...]) -> dict[str, Decimal]:
    return await asyncio.to_thread(_read_balances, wallet, tokens)
