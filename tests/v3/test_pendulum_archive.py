import json
from datetime import date
from pathlib import Path

import pytest

from src.v3.pendulum_archive import (
    archive_hour_url,
    fetch_manifest,
    manifest_url,
    parse_hour,
    validate_manifest,
)


def test_parse_hour_accepts_only_canonical_utc_hour():
    assert parse_hour("2026-09-11T17") == (date(2026, 9, 11), 17)
    with pytest.raises(ValueError):
        parse_hour("2026-09-11T17:00")
    with pytest.raises(ValueError):
        parse_hour("2026-09-11T24")


def test_archive_urls_are_fixed_to_expected_host_and_path():
    assert archive_hour_url("2026-09-11T17").endswith("/v3/2026-09-11/17/2026-09-11T17.parquet")
    assert manifest_url("2026-09-11T17").endswith("/v3/2026-09-11/17/manifest.json")


def test_manifest_requires_integrity_and_ordering_metadata():
    payload = {
        "row_count": 3,
        "sha256": "a" * 64,
        "served_verbatim": True,
        "products": {
            "book": {
                "row_count": 2,
                "sha256": "b" * 64,
                "columns": ["event_type", "bids", "asks"],
                "order_by": ["timestamp_received", "sequence"],
            }
        },
    }
    assert validate_manifest(payload)["row_count"] == 3
    for bad in ({}, {**payload, "sha256": "not-a-digest"}, {**payload, "served_verbatim": False}):
        with pytest.raises(ValueError):
            validate_manifest(bad)


def test_manifest_rejects_unbounded_or_malformed_products():
    payload = {"row_count": 1, "sha256": "a" * 64, "served_verbatim": True}
    with pytest.raises(ValueError):
        validate_manifest({**payload, "products": {"book": {"row_count": -1}}})
    with pytest.raises(ValueError):
        validate_manifest({**payload, "products": {"book": {"row_count": 1, "sha256": "z" * 64}}})


def test_manifest_round_trip_from_fixture(tmp_path: Path):
    payload = {"row_count": 0, "sha256": "0" * 64, "served_verbatim": True, "products": {}}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload))
    assert validate_manifest(json.loads(path.read_text()))["products"] == {}


def test_fetch_manifest_uses_canonical_url_and_validates_response():
    seen = []
    payload = {"row_count": 0, "sha256": "0" * 64, "served_verbatim": True, "products": {}}

    def getter(url: str) -> bytes:
        seen.append(url)
        return json.dumps(payload).encode()

    assert fetch_manifest("2026-09-11T17", getter=getter) == payload
    assert seen == [manifest_url("2026-09-11T17")]
