from __future__ import annotations

import json
import subprocess
import sys

import pytest

from src.v3.resolver_registry import ResolverRegistryEntry, registry_entry_from_gamma


def gamma_record(**overrides):
    row = {
        "id": "4633021",
        "slug": "helsinki-high-15c-september-19",
        "question": "Will the highest temperature in Helsinki be 15°C on September 19?",
        "conditionId": "0xcondition",
        "clobTokenIds": json.dumps(["yes-token", "no-token"]),
        "outcomes": json.dumps(["Yes", "No"]),
        "resolutionSource": "https://www.weather.gov/wrh/timeseries?site=EFHK",
        "description": "Resolves using the official station.",
    }
    row.update(overrides)
    return row


def test_registry_normalizes_gamma_identity_and_preserves_hash():
    entry = registry_entry_from_gamma(
        gamma_record(),
        fetched_at="2026-09-18T10:00:00+00:00",
        source_url="https://gamma-api.polymarket.com/markets/4633021",
    )

    assert isinstance(entry, ResolverRegistryEntry)
    assert entry.market_id == "4633021"
    assert entry.condition_id == "0xcondition"
    assert entry.outcomes == ("Yes", "No")
    assert entry.token_ids == ("yes-token", "no-token")
    assert entry.resolver.station == "EFHK"
    assert entry.resolver.supported is True
    assert len(entry.raw_sha256) == 64
    assert entry.source_url.startswith("https://gamma-api.polymarket.com/")


def test_registry_rejects_missing_token_outcome_alignment():
    with pytest.raises(ValueError, match="same length"):
        registry_entry_from_gamma(gamma_record(clobTokenIds=json.dumps(["yes-token"])))


def test_registry_rejects_missing_or_ambiguous_resolver():
    with pytest.raises(ValueError, match="resolver"):
        registry_entry_from_gamma(gamma_record(resolutionSource=""), fetched_at="2026-09-18T10:00:00+00:00")

    with pytest.raises(ValueError, match="resolver"):
        registry_entry_from_gamma(
            gamma_record(
                description=(
                    "https://www.weather.gov/wrh/timeseries?site=KORD "
                    "and https://www.weather.gov/wrh/timeseries?site=KMDW"
                )
            ),
            fetched_at="2026-09-18T10:00:00+00:00",
        )


def test_registry_rejects_non_gamma_source_url():
    with pytest.raises(ValueError, match="source_url"):
        registry_entry_from_gamma(
            gamma_record(),
            fetched_at="2026-09-18T10:00:00+00:00",
            source_url="https://evil.example/markets/4633021",
        )


def test_registry_serializes_without_raw_payload_or_credentials():
    entry = registry_entry_from_gamma(gamma_record(), fetched_at="2026-09-18T10:00:00+00:00")
    payload = entry.as_dict()
    serialized = json.dumps(payload, sort_keys=True)

    assert "private_key" not in serialized
    assert "api_key" not in serialized
    assert "raw_record" not in payload
    assert payload["resolver"]["station"] == "EFHK"


def test_registry_cli_is_offline_and_writes_normalized_entry(tmp_path):
    source = tmp_path / "gamma.json"
    output = tmp_path / "registry.json"
    source.write_text(json.dumps(gamma_record()), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/remediation_research.py",
            "registry",
            "--input",
            str(source),
            "--out",
            str(output),
            "--fetched-at",
            "2026-09-18T10:00:00+00:00",
            "--source-url",
            "https://gamma-api.polymarket.com/markets/4633021",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    assert payload["market_id"] == "4633021"
    assert payload["resolver"]["station"] == "EFHK"
    assert "raw_record" not in payload
