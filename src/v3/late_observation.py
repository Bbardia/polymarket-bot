"""Paper-only as-of hourly resolver shadow; no authenticated or order interfaces.

Input evidence is operator-supplied JSON. In particular NOAA METAR reportTime is
NOT a publication time and cannot silently be promoted to one. Neither a
running high nor a model's upper scenario is a settled label.
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, DecimalException, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

from .paper_weather import CITY_STATIONS, CITY_TIMEZONES, parse_high_temperature_contract
from .resolver_registry import registry_entry_from_gamma


def instant(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp required")
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("invalid timestamp") from exc
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("timezone-aware timestamp required")
    return dt.astimezone(timezone.utc)


def number(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError("numeric value required")
    try:
        d = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid numeric value") from exc
    # Decimal accepts huge finite exponents which overflow during arithmetic.
    # Limit both input representation and magnitude/precision before any operation.
    if not d.is_finite():
        raise ValueError("bounded finite numeric value required")
    exponent = d.as_tuple().exponent
    if (len(d.as_tuple().digits) > 18 or not (-12 <= d.adjusted() <= 12)
            or not isinstance(exponent, int) or not (-12 <= exponent <= 12)):
        raise ValueError("bounded finite numeric value required")
    return d


def _require(ok: bool, reason: str) -> None:
    if not ok:
        raise ValueError(reason)


# Only these literal field names may appear in a missing-field rejection.
_FIELDS = {"decision_at", "market", "market_fetched_at", "target_end_date",
           "observations", "observed_at", "published_at", "temperature_f",
           "temperature_c", "remaining_day_model", "probability_lower",
           "plausible_remaining_max_f", "plausible_remaining_max_c", "source_url",
           "issued_at", "book", "fetched_at", "timestamp", "latency_seconds",
           "shares", "min_order_size", "tick_size", "fee_rate", "fee_verified_at",
           "price", "size"}


def _audit_path(path: Path) -> Path:
    root = Path(__file__).resolve().parents[2]
    resolved = path.resolve()
    if not resolved.is_relative_to(root):
        raise ValueError("audit output must stay inside isolated worktree")
    return resolved

def late_contract(question: str, *, end_date: datetime):
    """Normalize this lane's NYC wording without changing active weather discovery."""
    if isinstance(question, str):
        question = re.sub(r'\bin New York City(?= be )', 'in New York', question, flags=re.I)
    return parse_high_temperature_contract(question, end_date=end_date)


def evaluate(payload: object) -> dict:
    """Evaluate one YES buy, returning audit even for rejected evidence."""
    result = {"status": "rejected", "reason": "invalid input", "paper_only": True,
              "decision_at": None, "market_id": None,
              "condition_id": None, "token_id": None}
    if not isinstance(payload, dict):
        result["reason"] = "candidate must be a JSON object"
        return result
    try:
        decision = instant(payload["decision_at"])
        result["decision_at"] = decision.isoformat()
        entry = registry_entry_from_gamma(payload["market"], fetched_at=payload["market_fetched_at"])
        result.update(market_id=entry.market_id, condition_id=entry.condition_id,
                      market_sha256=entry.raw_sha256, resolver=entry.resolver.as_dict(),
                      rule_text=entry.description, rule_source=entry.resolution_source,
                      market_source_url=entry.source_url)
        _require(instant(entry.fetched_at) <= decision, "market fetched after decision")
        _require((decision - instant(entry.fetched_at)).total_seconds() <= 120, "stale market metadata")
        _require(payload.get("rule_kind") == "hourly_temp_whole_degree", "hourly Temp rule must be explicitly selected")
        description = entry.description.lower()
        _require("show hourly data" in description and '"temp" column' in description
                 and "whole degrees" in description, "unverified hourly Temp/whole-degree rule")
        _require("weather underground" not in description or payload.get("primary_source_available") is True,
                 "fallback resolver not ruled out")
        market_end = payload["market"].get("endDate")
        _require(isinstance(market_end, str) and bool(market_end), "market endDate missing")
        try:
            snapshot_end = instant(market_end)
        except ValueError as exc:
            raise ValueError("market endDate invalid or not timezone-aware") from exc
        end = instant(payload["target_end_date"])
        _require(snapshot_end == end, "market endDate differs from target_end_date")
        contract = late_contract(entry.question, end_date=end)
        _require(contract is not None and contract.target_date == payload.get("target_date"), "market question/date not exact")
        assert contract is not None
        zone = ZoneInfo(CITY_TIMEZONES[contract.city])
        target = datetime.fromisoformat(contract.target_date).date()
        _require(decision.astimezone(zone).date() == target, "decision outside target local day")
        _require(entry.resolver.station == CITY_STATIONS[contract.city], "resolver station differs from city station")
        _require(payload.get("station_id") == entry.resolver.station, "observation station mismatch")
        _require(payload.get("outcome") == "Yes", "only YES token supported")
        yes = entry.outcomes.index("Yes")
        _require(payload.get("token_id") == entry.token_ids[yes], "YES token identity mismatch")
        result["token_id"] = entry.token_ids[yes]
        _require(payload.get("condition_id") == entry.condition_id and payload.get("market_id") == entry.market_id,
                 "market identity mismatch")
        _require(payload.get("rule_sha256") == entry.raw_sha256, "rule snapshot hash mismatch")
        _require(payload.get("market_active") is True and payload.get("market_closed") is False
                 and payload["market"].get("active") is True and payload["market"].get("closed") is False,
                 "market snapshot not open")
        _require(payload["market"].get("acceptingOrders") is True,
                 "market snapshot not accepting orders")
        observations = payload["observations"]
        _require(bool(isinstance(observations, list) and observations), "no hourly observations")
        running = None
        latest_publication = None
        for row in observations:
            _require(isinstance(row, dict), "hourly observation must be an object")
            _require(row.get("station_id") == entry.resolver.station and
                     row.get("source_url") == entry.resolution_source and
                     row.get("column") == "Temp" and row.get("hourly") is True,
                     "wrong station or non-resolver hourly Temp observation")
            observed = instant(row["observed_at"])
            published = instant(row["published_at"])  # no report-time substitution
            _require(observed <= published <= decision, "observation not available as of decision")
            _require(observed.astimezone(zone).date() == target, "observation outside target local day")
            temp = number(row["temperature_f"] if contract.unit == "F" else row["temperature_c"])
            _require(temp == temp.to_integral_value(), "resolver Temp must be whole degree")
            running = max(running, temp) if running is not None else temp
            latest_publication = max(latest_publication, published) if latest_publication else published
        assert latest_publication is not None and running is not None
        result.update(running_high=str(running), latest_published_at=latest_publication.isoformat(),
                      observation_count=len(observations), final_high=None)
        _require((decision - latest_publication).total_seconds() <= 5400, "stale observations")
        _require((decision - max(instant(r["observed_at"]) for r in observations)).total_seconds() <= 5400,
                 "stale observed time")
        # A later high can invalidate any below/inside outcome. Never call running high final.
        if contract.display_upper is not None and running > contract.display_upper:
            result.update(reason="running high already exceeds bucket", probability_lower="0")
            return result
        model = payload["remaining_day_model"]
        _require(isinstance(model, dict), "remaining-day model must be an object")
        _require(all(model.get(key) == value for key, value in {
            "station_id": entry.resolver.station, "target_date": contract.target_date,
            "market_id": entry.market_id, "condition_id": entry.condition_id,
            "token_id": entry.token_ids[yes], "outcome": "Yes",
            "bucket": entry.question, "rule_sha256": entry.raw_sha256,
        }.items()), "remaining-day model identity mismatch")
        _require(instant(model["published_at"]) <= decision and instant(model["issued_at"]) <= instant(model["published_at"]),
                 "remaining-day model unavailable as of decision")
        _require((decision - instant(model["published_at"])).total_seconds() <= 3600, "stale remaining-day model")
        lower = number(model["probability_lower"])
        ceiling = number(model["plausible_remaining_max_f"] if contract.unit == "F" else model["plausible_remaining_max_c"])
        _require(0 <= lower <= 1 and ceiling >= running, "invalid remaining-day uncertainty")
        # The scenario ceiling is not a hard cap or settlement label: lower probability must
        # be operator-provided from an independently as-of issued uncertainty model.
        result.update(probability_lower=str(lower), plausible_remaining_max=str(ceiling),
                      model_source=model["source_url"], model_issued_at=model["issued_at"])
        book = payload["book"]
        _require(isinstance(book, dict), "order book must be an object")
        _require(book.get("token_id") == entry.token_ids[yes] and book.get("condition_id") == entry.condition_id,
                 "order book identity mismatch")
        fetched = instant(book["fetched_at"])
        changed = instant(book["timestamp"])
        latency = number(payload["latency_seconds"])
        _require(1 <= latency <= 60, "latency must be conservative and bounded")
        _require(changed <= fetched <= decision and (decision - fetched).total_seconds() <= 10
                 and (decision - changed).total_seconds() <= 60, "stale/future order book")
        _require(isinstance(book.get("bids"), list) and bool(book["bids"])
                 and isinstance(book.get("asks"), list) and bool(book["asks"]), "missing bid or ask")
        quantity = number(payload["shares"])
        minimum = number(book["min_order_size"])
        tick = number(book["tick_size"])
        fee_rate = number(payload["fee_rate"])
        fee_verified = instant(payload["fee_verified_at"])
        _require(fee_verified <= decision and (decision - fee_verified).total_seconds() <= 3600
                 and payload.get("fee_source_url") == "https://docs.polymarket.com/trading/fees",
                 "fee provenance missing or stale")
        _require(quantity >= minimum > 0 and tick > 0 and fee_rate >= Decimal("0.05"), "fee/minimum invalid")
        # Reserve one extra tick for latency/slippage and require full visible size
        # even after a 50% haircut; no partial fills or midpoint proxy.
        asks = sorted(((number(x["price"]), number(x["size"])) for x in book["asks"]), key=lambda x: x[0])
        bids = [(number(x["price"]), number(x["size"])) for x in book["bids"]]
        _require(all(0 < p < 1 and s > 0 for p, s in asks + bids), "invalid book levels")
        _require(max(p for p, _ in bids) < min(p for p, _ in asks), "crossed book")
        remaining = quantity
        cost = Decimal(0)
        fees = Decimal(0)
        for price, size in asks:
            used = min(remaining, size / 2)
            effective = price + tick
            _require(effective < 1, "latency buffer reaches payout")
            cost += used * effective
            fees += used * fee_rate * effective * (1 - effective)
            remaining -= used
            if remaining == 0:
                break
        _require(remaining == 0, "insufficient haircut ask depth")
        net_cost = cost + fees
        edge = quantity * lower - net_cost
        result.update(shares=str(quantity), ask_best=str(asks[0][0]), bid_best=str(max(p for p, _ in bids)),
                      buffered_vwap=str(cost / quantity), fee=str(fees), net_cost=str(net_cost),
                      baseline_market_ask=str(asks[0][0]), conservative_edge=str(edge),
                      latency_seconds=str(latency), depth_haircut="0.5")
        _require(edge > 0, "no conservative edge over executable ask plus fee")
        result.update(status="eligible", reason="paper-only positive conservative lower-bound edge")
    except (KeyError, ValueError, TypeError, IndexError, AttributeError,
            DecimalException, OverflowError, UnicodeError) as exc:
        if isinstance(exc, KeyError):
            name = exc.args[0] if exc.args else None
            reason = f"missing {name}" if name in _FIELDS else "invalid candidate evidence"
        elif isinstance(exc, ValueError) and str(exc) in _SAFE_REASONS:
            reason = str(exc)
        else:
            reason = "invalid candidate evidence"
        # Never expose untrusted metadata or replayable input on malformed evidence.
        result = dict(status="rejected", reason=reason, paper_only=True,
                      decision_at=result["decision_at"], market_id=None,
                      condition_id=None, token_id=None)
    return result


_SAFE_REASONS = {
    "timestamp required", "invalid timestamp", "timezone-aware timestamp required",
    "numeric value required", "invalid numeric value", "bounded finite numeric value required",
    "market fetched after decision", "stale market metadata", "hourly Temp rule must be explicitly selected",
    "unverified hourly Temp/whole-degree rule", "fallback resolver not ruled out",
    "market endDate missing", "market endDate invalid or not timezone-aware",
    "market endDate differs from target_end_date", "market question/date not exact",
    "decision outside target local day", "resolver station differs from city station",
    "observation station mismatch", "only YES token supported", "YES token identity mismatch",
    "market identity mismatch", "rule snapshot hash mismatch", "market snapshot not open",
    "market snapshot not accepting orders", "no hourly observations", "hourly observation must be an object",
    "wrong station or non-resolver hourly Temp observation", "observation not available as of decision",
    "observation outside target local day", "resolver Temp must be whole degree", "stale observations",
    "stale observed time", "remaining-day model must be an object", "remaining-day model identity mismatch",
    "remaining-day model unavailable as of decision", "stale remaining-day model",
    "invalid remaining-day uncertainty", "order book must be an object", "order book identity mismatch",
    "latency must be conservative and bounded", "stale/future order book", "missing bid or ask",
    "fee provenance missing or stale", "fee/minimum invalid", "invalid book levels", "crossed book",
    "latency buffer reaches payout", "insufficient haircut ask depth",
    "no conservative edge over executable ask plus fee",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="paper-only hourly resolver shadow (no trading)")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--audit", required=True, type=Path)
    args = parser.parse_args()
    args.audit = _audit_path(args.audit)
    if args.audit.exists():
        parser.error("audit path already exists; append-only captures require a new path")
    payload = json.loads(args.input.read_text())
    if isinstance(payload, dict) and "input" in payload and "evidence" in payload:
        payload = payload["input"]  # replay the exact public probe's recorded inputs
    result = evaluate(payload)
    # Exclusive creation, never touch live state or overwrite an audit.
    with args.audit.open("x") as f:
        json.dump(({"input": payload, "result": result} if result["status"] == "eligible"
                   else {"result": result}), f, sort_keys=True, indent=2)
        f.write("\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
