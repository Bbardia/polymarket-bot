"""Fail-closed, read-only registry entries for raw Gamma market records.

This module deliberately has no network, SDK, credential, or order imports. It
normalizes one already-acquired Gamma record and preserves only provenance
metadata plus a hash of the canonical raw record.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping
from urllib.parse import urlparse

from .resolver import ResolverIdentity, parse_resolver_identity

_ALLOWED_GAMMA_HOSTS = {"gamma-api.polymarket.com"}


def _required_text(record: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if value is not None and not isinstance(value, (dict, list, tuple)):
            text = str(value).strip()
            if text:
                return text
    raise ValueError(f"missing required market field: {'/'.join(keys)}")


def _list_field(record: Mapping[str, Any], *keys: str) -> tuple[str, ...]:
    value: Any = None
    found = False
    for key in keys:
        if key in record:
            value = record[key]
            found = True
            break
    if not found:
        raise ValueError(f"missing required market field: {'/'.join(keys)}")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"market field {keys[0]!r} is not valid JSON") from exc
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"market field {keys[0]!r} must be a list")
    result = tuple(str(item).strip() for item in value)
    if not result or any(not item for item in result):
        raise ValueError(f"market field {keys[0]!r} contains an empty item")
    if len(set(result)) != len(result):
        raise ValueError(f"market field {keys[0]!r} contains duplicates")
    return result


def _canonical_sha256(record: Mapping[str, Any]) -> str:
    try:
        encoded = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("raw Gamma record is not JSON-canonicalizable") from exc
    return hashlib.sha256(encoded).hexdigest()


def _validate_source_url(source_url: str, market_id: str) -> str:
    parsed = urlparse(source_url)
    if parsed.scheme != "https" or parsed.hostname not in _ALLOWED_GAMMA_HOSTS:
        raise ValueError("source_url must be an HTTPS Gamma API URL")
    if parsed.port not in (None, 443) or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("source_url must not contain a nonstandard port, credentials, query, or fragment")
    if parsed.path != f"/markets/{market_id}":
        raise ValueError("source_url market_id must match the Gamma record")
    return source_url


def _validate_fetched_at(fetched_at: str) -> None:
    try:
        timestamp = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("fetched_at must be an ISO-8601 timestamp") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("fetched_at must include a timezone")


@dataclass(frozen=True)
class ResolverRegistryEntry:
    market_id: str
    slug: str
    question: str
    condition_id: str
    outcomes: tuple[str, ...]
    token_ids: tuple[str, ...]
    resolution_source: str
    description: str
    fetched_at: str
    source_url: str
    raw_sha256: str
    resolver: ResolverIdentity

    def __post_init__(self) -> None:
        if len(self.outcomes) != len(self.token_ids):
            raise ValueError("outcomes and token_ids must have the same length")
        if len(self.outcomes) < 2:
            raise ValueError("a market must have at least two outcomes")
        if len(self.raw_sha256) != 64 or re.fullmatch(r"[0-9a-f]{64}", self.raw_sha256) is None:
            raise ValueError("raw_sha256 must be a SHA-256 hex digest")

    def as_dict(self) -> dict[str, Any]:
        return {
            "market_id": self.market_id,
            "slug": self.slug,
            "question": self.question,
            "condition_id": self.condition_id,
            "outcomes": list(self.outcomes),
            "token_ids": list(self.token_ids),
            "resolution_source": self.resolution_source,
            "description": self.description,
            "fetched_at": self.fetched_at,
            "source_url": self.source_url,
            "raw_sha256": self.raw_sha256,
            "resolver": self.resolver.as_dict(),
        }


def registry_entry_from_gamma(
    record: Mapping[str, Any],
    *,
    fetched_at: str = "",
    source_url: str | None = None,
) -> ResolverRegistryEntry:
    """Normalize one already-fetched Gamma record; fail closed on uncertainty."""
    if not isinstance(record, Mapping):
        raise ValueError("Gamma record must be an object")
    market_id = _required_text(record, "id", "market_id")
    slug = _required_text(record, "slug")
    question = _required_text(record, "question")
    condition_id = _required_text(record, "conditionId", "condition_id")
    outcomes = _list_field(record, "outcomes")
    token_ids = _list_field(record, "clobTokenIds", "clob_token_ids", "token_ids")
    if len(outcomes) != len(token_ids):
        raise ValueError("outcomes and token_ids must have the same length")
    resolution_source = str(record.get("resolutionSource") or record.get("resolution_source") or "").strip()
    description = str(record.get("description") or "")
    if not fetched_at.strip():
        raise ValueError("fetched_at is required for provenance")
    _validate_fetched_at(fetched_at)
    if source_url is None:
        source_url = f"https://gamma-api.polymarket.com/markets/{market_id}"
    source_url = _validate_source_url(source_url, market_id)
    resolver = parse_resolver_identity(resolution_source, description)
    if not resolver.supported or not resolver.station:
        raise ValueError(f"resolver metadata is unsupported or ambiguous: {resolver.reason}")
    return ResolverRegistryEntry(
        market_id=market_id,
        slug=slug,
        question=question,
        condition_id=condition_id,
        outcomes=outcomes,
        token_ids=token_ids,
        resolution_source=resolution_source,
        description=description,
        fetched_at=fetched_at,
        source_url=source_url,
        raw_sha256=_canonical_sha256(record),
        resolver=resolver,
    )
