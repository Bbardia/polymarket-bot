"""Bounded metadata access for the public PendulumFlow V3 archive.

This module only fetches small hourly manifests. It never downloads Parquet by
itself, so callers cannot accidentally pull the multi-terabyte archive onto the
bot host. The manifest is an integrity/provenance gate for a separate offline
reader or DuckDB query.
"""
from __future__ import annotations

import json
import re
from datetime import date
from typing import Any, Callable
from urllib.request import Request, urlopen

HOST = "archive.pendulumflow.com"
BASE = f"https://{HOST}/v3"
_HOUR_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})T(\d{2})$")
_HEX64_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_MAX_MANIFEST_BYTES = 2_000_000


def parse_hour(value: str) -> tuple[date, int]:
    """Parse the archive's canonical UTC hour identifier."""
    match = _HOUR_RE.fullmatch(value)
    if not match:
        raise ValueError("hour must use YYYY-MM-DDTHH")
    try:
        target = date.fromisoformat(match.group(1))
    except ValueError as exc:
        raise ValueError("hour contains an invalid date") from exc
    hour = int(match.group(2))
    if not 0 <= hour <= 23:
        raise ValueError("hour must be between 00 and 23")
    return target, hour


def archive_hour_url(hour: str) -> str:
    target, number = parse_hour(hour)
    canonical = f"{target.isoformat()}T{number:02d}"
    return f"{BASE}/{target.isoformat()}/{number:02d}/{canonical}.parquet"


def manifest_url(hour: str) -> str:
    target, number = parse_hour(hour)
    return f"{BASE}/{target.isoformat()}/{number:02d}/manifest.json"


def _validate_digest(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _HEX64_RE.fullmatch(value):
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    return value.lower()


def validate_manifest(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate the small subset required before consuming an archive hour."""
    if not isinstance(payload, dict):
        raise ValueError("manifest must be an object")
    row_count = payload.get("row_count")
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
        raise ValueError("manifest row_count must be a non-negative integer")
    _validate_digest(payload.get("sha256"), "manifest sha256")
    if payload.get("served_verbatim") is not True:
        raise ValueError("manifest is not marked served_verbatim")
    products = payload.get("products", {})
    if not isinstance(products, dict):
        raise ValueError("manifest products must be an object")
    for name, product in products.items():
        if not isinstance(name, str) or not isinstance(product, dict):
            raise ValueError("manifest product entries must be objects")
        count = product.get("row_count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError(f"product {name} row_count must be non-negative")
        if "sha256" in product:
            _validate_digest(product["sha256"], f"product {name} sha256")
        if "order_by" in product and not isinstance(product["order_by"], list):
            raise ValueError(f"product {name} order_by must be a list")
        if "columns" in product and not isinstance(product["columns"], list):
            raise ValueError(f"product {name} columns must be a list")
    return payload


def _fetch_manifest_bytes(target: str) -> bytes:
    request = Request(target, headers={"User-Agent": "polymarket-paper-research/1.0", "Accept": "application/json"})
    with urlopen(request, timeout=20) as response:
        body = response.read(_MAX_MANIFEST_BYTES + 1)
    if len(body) > _MAX_MANIFEST_BYTES:
        raise ValueError("manifest exceeds bounded response size")
    return body


def fetch_manifest(
    hour: str,
    *,
    getter: Callable[[str], bytes] | None = None,
) -> dict[str, Any]:
    """Fetch and validate one bounded manifest using an unauthenticated GET."""
    url = manifest_url(hour)
    fetcher = getter or _fetch_manifest_bytes
    try:
        payload = json.loads(fetcher(url))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(f"manifest fetch failed: {exc}") from exc
    return validate_manifest(payload)
