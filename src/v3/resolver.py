"""Per-market resolver identity parsing (remediation item 5).

The station a market resolves on is read from the market's own
``resolutionSource``/description, never guessed from the city name. Sources
that are not METAR-backed (Weather Underground, non-METAR authorities,
unrecognised URLs) are refused. This module never edits the station map; it
reports disagreements so a human can fix the map with evidence.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

WEATHER_GOV_TIMESERIES_RE = re.compile(
    r"weather\.gov/wrh/timeseries\?(?:[^\s\"'<>]*&)?site=([A-Za-z0-9]{3,4})",
    re.IGNORECASE,
)
WUNDERGROUND_RE = re.compile(
    r"wunderground\.com/(?:history|weather|dashboard)/[^\s\"'<>]*?/([A-Za-z0-9]{3,4})\b",
    re.IGNORECASE,
)
NON_METAR_AUTHORITY_MARKERS = (
    "hko.gov.hk",  # Hong Kong Observatory: not a METAR resolver; refuse.
    "weather.gov.hk",
)


@dataclass(frozen=True)
class ResolverIdentity:
    station: str | None
    authority: str
    supported: bool
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "station": self.station,
            "authority": self.authority,
            "supported": self.supported,
            "reason": self.reason,
        }


def parse_resolver_identity(source: str | None, description: str | None = None) -> ResolverIdentity:
    """Return the METAR station a market resolves on, or an unsupported identity."""
    haystack = " ".join(part for part in (source or "", description or "") if part)
    if not haystack.strip():
        return ResolverIdentity(None, "none", False, "no resolution source or description")
    lowered = haystack.lower()
    for marker in NON_METAR_AUTHORITY_MARKERS:
        if marker in lowered:
            return ResolverIdentity(None, "non-metar", False, f"non-METAR authority {marker} refused")
    match = WEATHER_GOV_TIMESERIES_RE.search(haystack)
    if match:
        return ResolverIdentity(match.group(1).upper(), "weather.gov-timeseries", True, "METAR station parsed")
    match = WUNDERGROUND_RE.search(haystack)
    if match:
        return ResolverIdentity(
            match.group(1).upper(),
            "wunderground",
            False,
            "Weather Underground resolver is not a supported METAR authority",
        )
    if "weather.gov" in lowered:
        return ResolverIdentity(None, "weather.gov-unparsed", False, "weather.gov source without a site parameter")
    return ResolverIdentity(None, "unknown", False, "unrecognised resolution authority")


@dataclass(frozen=True)
class StationRecord:
    icao: str
    latitude: float
    longitude: float
    elevation_m: float | None
    name: str
    source: str


class StationMetadata:
    """Offline station metadata cache (aviationweather.gov ``stationinfo`` JSON)."""

    def __init__(self, records: Mapping[str, StationRecord], *, source_path: Path | None = None) -> None:
        self._records = dict(records)
        self.source_path = source_path

    @classmethod
    def from_path(cls, path: Path) -> "StationMetadata":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        rows = payload["stations"] if isinstance(payload, dict) and "stations" in payload else payload
        records: dict[str, StationRecord] = {}
        for row in rows:
            icao = str(row.get("icaoId") or row.get("icao") or "").upper()
            if not icao:
                continue
            try:
                latitude = float(row["lat"])
                longitude = float(row["lon"])
            except (KeyError, TypeError, ValueError):
                continue
            elevation = row.get("elev")
            records[icao] = StationRecord(
                icao=icao,
                latitude=latitude,
                longitude=longitude,
                elevation_m=float(elevation) if elevation is not None else None,
                name=str(row.get("site") or row.get("name") or ""),
                source=str(row.get("source") or "aviationweather.gov/api/data/stationinfo"),
            )
        return cls(records, source_path=Path(path))

    def get(self, icao: str) -> StationRecord | None:
        return self._records.get(icao.upper())

    def __len__(self) -> int:
        return len(self._records)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


@dataclass(frozen=True)
class StationVerification:
    city: str
    expected_station: str | None
    parsed: ResolverIdentity
    metadata_found: bool
    distance_km: float | None
    verified: bool
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "city": self.city,
            "expected_station": self.expected_station,
            "parsed": self.parsed.as_dict(),
            "metadata_found": self.metadata_found,
            "distance_km": self.distance_km,
            "verified": self.verified,
            "reason": self.reason,
        }


def verify_station_for_city(
    *,
    city: str,
    expected_station: str | None,
    city_coordinates: tuple[float, float] | None,
    parsed: ResolverIdentity,
    metadata: StationMetadata | None,
    max_distance_km: float = 60.0,
) -> StationVerification:
    """Fail closed. Verified only if the parsed METAR station equals the mapped
    station AND (when metadata is available) sits within ``max_distance_km`` of
    the city's forecast coordinates."""
    if not parsed.supported or parsed.station is None:
        return StationVerification(city, expected_station, parsed, False, None, False, parsed.reason)
    if expected_station is None:
        return StationVerification(city, None, parsed, False, None, False, "city has no mapped station; refuse")
    if parsed.station != expected_station:
        return StationVerification(
            city, expected_station, parsed, False, None, False,
            f"resolver station {parsed.station} differs from mapped {expected_station}; map fix requires evidence",
        )
    if metadata is None:
        return StationVerification(city, expected_station, parsed, False, None, True, "station matches map; metadata unavailable")
    record = metadata.get(parsed.station)
    if record is None:
        return StationVerification(city, expected_station, parsed, False, None, False, "station missing from metadata; refuse")
    if city_coordinates is None:
        return StationVerification(city, expected_station, parsed, True, None, False, "city has no forecast coordinates; refuse")
    distance = haversine_km(record.latitude, record.longitude, city_coordinates[0], city_coordinates[1])
    if distance > max_distance_km:
        return StationVerification(
            city, expected_station, parsed, True, distance, False,
            f"station {parsed.station} is {distance:.1f} km from the city forecast point (> {max_distance_km} km)",
        )
    return StationVerification(city, expected_station, parsed, True, distance, True, "station matches map and metadata")
