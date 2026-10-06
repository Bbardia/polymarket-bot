"""Read-only Polygon receipt proof for a managed CTF redemption.

Legacy public REDEEM activity can omit its ``asset`` token. Require a successful
transaction with the wallet's exact ERC-1155 token/quantity outflow and a burn
of the same token before crediting an otherwise unattributable redemption.
"""
from __future__ import annotations

import asyncio
import re
from decimal import Decimal

import requests

RPC = "https://polygon-bor-rpc.publicnode.com"
CTF = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
SINGLE = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
BATCH = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
_HASH = re.compile(r"0x[0-9a-fA-F]{64}\Z")
_WALLET = re.compile(r"0x[0-9a-fA-F]{40}\Z")
_UNITS = Decimal("1000000")


def _array(data: bytes, offset: int) -> list[int]:
    if offset % 32 or offset + 32 > len(data):
        raise ValueError("invalid ERC-1155 ABI offset")
    length = int.from_bytes(data[offset:offset + 32])
    if length > 200 or offset + 32 + 32 * length > len(data):
        raise ValueError("invalid ERC-1155 ABI array length")
    return [int.from_bytes(data[offset + 32 + i * 32:offset + 64 + i * 32]) for i in range(length)]


def _transfers(log: dict) -> list[tuple[str, str, int, int]]:
    topics = log.get("topics")
    if not isinstance(topics, list) or len(topics) != 4 or topics[0] not in (SINGLE, BATCH):
        return []
    if not all(isinstance(t, str) and re.fullmatch(r"0x[0-9a-fA-F]{64}", t) for t in topics):
        raise ValueError("malformed ERC-1155 topics")
    raw = log.get("data")
    if not isinstance(raw, str) or not raw.startswith("0x") or len(raw) > 26000:
        raise ValueError("malformed ERC-1155 data")
    data = bytes.fromhex(raw[2:])
    if topics[0] == SINGLE:
        if len(data) != 64:
            raise ValueError("malformed TransferSingle")
        pairs = [(int.from_bytes(data[:32]), int.from_bytes(data[32:]))]
    else:
        if len(data) < 64:
            raise ValueError("malformed TransferBatch")
        ids = _array(data, int.from_bytes(data[:32]))
        amounts = _array(data, int.from_bytes(data[32:64]))
        if len(ids) != len(amounts):
            raise ValueError("mismatched TransferBatch arrays")
        pairs = list(zip(ids, amounts))
    sender, recipient = "0x" + topics[2][-40:].lower(), "0x" + topics[3][-40:].lower()
    return [(sender, recipient, token, amount) for token, amount in pairs]


def _receipt(tx_hash: str) -> dict:
    response = requests.post(RPC, json={"jsonrpc": "2.0", "id": 1,
        "method": "eth_getTransactionReceipt", "params": [tx_hash]},
        timeout=15, stream=True, headers={"Accept": "application/json"})
    response.raise_for_status()
    with response:
        chunks, size = [], 0
        for chunk in response.iter_content(chunk_size=65536):
            size += len(chunk)
            if size > 2000000:
                raise ValueError("receipt response exceeds byte cap")
            chunks.append(chunk)
    import json
    payload = json.loads(b"".join(chunks))
    if not isinstance(payload, dict) or payload.get("error") or not isinstance(payload.get("result"), dict):
        raise ValueError("receipt unavailable")
    return payload["result"]


def confirm_ctf_redemption_receipt(receipt: dict, *, tx_hash: str,
                                   wallet: str, token_id: str, quantity: Decimal) -> bool:
    """Fail closed unless the exact wallet outflow and token burn are in this tx."""
    if (not isinstance(tx_hash, str) or not _HASH.fullmatch(tx_hash)
            or not isinstance(wallet, str) or not _WALLET.fullmatch(wallet)
            or not isinstance(token_id, str) or not token_id.isdecimal()
            or not isinstance(quantity, Decimal) or not quantity.is_finite()
            or quantity <= 0 or quantity * _UNITS != (quantity * _UNITS).to_integral_value()):
        raise ValueError("invalid redemption receipt request")
    if (not isinstance(receipt, dict) or receipt.get("status") != "0x1"
            or str(receipt.get("transactionHash", "")).lower() != tx_hash.lower()
            or not receipt.get("blockNumber")):
        raise ValueError("unconfirmed or failed redemption transaction")
    logs = receipt.get("logs")
    if not isinstance(logs, list) or len(logs) > 2000:
        raise ValueError("malformed or unbounded receipt logs")
    wanted, owner = int(token_id), wallet.lower()
    units = int(quantity * _UNITS)
    outflows, burns = 0, 0
    for log in logs:
        if not isinstance(log, dict) or str(log.get("address", "")).lower() != CTF:
            continue
        for sender, recipient, token, amount in _transfers(log):
            if token == wanted and amount == units:
                outflows += sender == owner and recipient != owner
                burns += recipient == "0x" + "0" * 40
    if outflows != 1 or burns < 1:
        raise ValueError("wallet token outflow and burn are not proven by receipt")
    return True


async def verify_ctf_redemption_transaction(*, tx_hash: str, wallet: str,
                                            token_id: str, quantity: Decimal) -> None:
    if not isinstance(tx_hash, str) or not _HASH.fullmatch(tx_hash):
        raise ValueError("invalid redemption transaction hash")
    receipt = await asyncio.to_thread(_receipt, tx_hash)
    confirm_ctf_redemption_receipt(receipt, tx_hash=tx_hash, wallet=wallet,
                                   token_id=token_id, quantity=quantity)
