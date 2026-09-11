"""Conservative maker-fill replay and paper-shadow evaluation."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

from .weather import brier_score

ZERO = Decimal("0")
ONE = Decimal("1")
DEFAULT_MAX_RECORDS = 100_000
_MISSING = object()


@dataclass
class MakerQuote:
    token_id: str
    side: str
    price: Decimal
    size: Decimal
    queue_ahead: Decimal
    quote_id: str = ""
    remaining: Decimal = field(init=False)
    active: bool = field(default=True, init=False)

    def __post_init__(self) -> None:
        if self.side not in {"BUY", "SELL"}:
            raise ValueError("maker quote side must be BUY or SELL")
        if not (ZERO < self.price < ONE):
            raise ValueError("maker quote price must be in (0, 1)")
        if self.size <= ZERO or self.queue_ahead < ZERO:
            raise ValueError("invalid maker quote size or queue")
        self.remaining = self.size


@dataclass(frozen=True)
class PaperFill:
    quote_id: str
    token_id: str
    side: str
    price: Decimal
    size: Decimal


class QueueAwareMakerSimulator:
    """Fill only after prints consume modeled queue at the quoted level."""

    def __init__(self) -> None:
        self.quotes: list[MakerQuote] = []
        self._next_quote = 1

    def place(self, quote: MakerQuote) -> MakerQuote:
        if not quote.quote_id:
            quote.quote_id = f"paper-quote-{self._next_quote}"
            self._next_quote += 1
        if any(existing.quote_id == quote.quote_id for existing in self.quotes):
            raise ValueError(f"duplicate paper quote id: {quote.quote_id}")
        self.quotes.append(quote)
        return quote

    def cancel(self, quote_id: str) -> bool:
        for quote in self.quotes:
            if quote.quote_id == quote_id:
                if not quote.active:
                    return False
                quote.active = False
                return True
        return False

    def on_trade(
        self,
        *,
        token_id: str,
        aggressor_side: str,
        price: Decimal,
        size: Decimal,
    ) -> tuple[PaperFill, ...]:
        if aggressor_side not in {"BUY", "SELL"}:
            raise ValueError("trade aggressor side must be BUY or SELL")
        if not (ZERO < price < ONE) or size <= ZERO:
            raise ValueError("invalid trade print")
        remaining_print = size
        fills: list[PaperFill] = []
        for quote in self.quotes:
            if not quote.active or quote.token_id != token_id:
                continue
            if quote.side == aggressor_side or quote.price != price:
                continue
            queue_before_print = quote.queue_ahead
            quote.queue_ahead = max(ZERO, queue_before_print - size)
            volume_reaching_quote = max(ZERO, size - queue_before_print)
            filled = min(quote.remaining, volume_reaching_quote, remaining_print)
            if filled <= ZERO:
                continue
            quote.remaining -= filled
            remaining_print -= filled
            fills.append(
                PaperFill(
                    quote.quote_id,
                    quote.token_id,
                    quote.side,
                    quote.price,
                    filled,
                )
            )
            if quote.remaining == ZERO:
                quote.active = False
        return tuple(fills)


@dataclass(frozen=True)
class ShadowCandidate:
    candidate_id: str
    expected_probability: Decimal
    entry_price: Decimal
    outcome: int
    filled_size: Decimal
    fees_paid: Decimal = ZERO

    def __post_init__(self) -> None:
        if not self.candidate_id:
            raise ValueError("candidate id is required")
        if not (ZERO <= self.expected_probability <= ONE):
            raise ValueError("expected probability must be in [0, 1]")
        if not (ZERO < self.entry_price < ONE):
            raise ValueError("entry price must be in (0, 1)")
        if self.outcome not in {0, 1}:
            raise ValueError("shadow outcome must be 0 or 1")
        if self.filled_size < ZERO or self.fees_paid < ZERO:
            raise ValueError("shadow fill and fees cannot be negative")
        if self.filled_size == ZERO and self.fees_paid != ZERO:
            raise ValueError("unfilled candidate cannot have observed fees")


@dataclass(frozen=True)
class ShadowEvaluation:
    candidates: int
    filled_candidates: int
    unfilled_candidates: int
    brier_score: Decimal
    filled_brier_score: Decimal | None
    total_fees: Decimal
    realized_pnl: Decimal


def evaluate_shadow_candidates(
    candidates: Sequence[ShadowCandidate] | Iterable[ShadowCandidate],
) -> ShadowEvaluation:
    rows = tuple(candidates)
    if not rows:
        raise ValueError("at least one shadow candidate is required")
    candidate_ids = [row.candidate_id for row in rows]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise ValueError("shadow candidate ids must be unique")
    filled = tuple(row for row in rows if row.filled_size > ZERO)
    all_brier = brier_score(
        [row.expected_probability for row in rows],
        [row.outcome for row in rows],
    )
    filled_brier = (
        brier_score(
            [row.expected_probability for row in filled],
            [row.outcome for row in filled],
        )
        if filled
        else None
    )
    total_fees = sum((row.fees_paid for row in filled), ZERO)
    realized = sum(
        (
            row.filled_size
            * (
                (ONE - row.entry_price)
                if row.outcome
                else -row.entry_price
            )
            - row.fees_paid
            for row in filled
        ),
        ZERO,
    )
    return ShadowEvaluation(
        candidates=len(rows),
        filled_candidates=len(filled),
        unfilled_candidates=len(rows) - len(filled),
        brier_score=all_brier,
        filled_brier_score=filled_brier,
        total_fees=total_fees,
        realized_pnl=realized,
    )


@dataclass(frozen=True)
class PaperReplayEvent:
    event_type: str
    quote_id: str = ""
    token_id: str = ""
    side: str = ""
    price: Decimal = ZERO
    size: Decimal = ZERO
    queue_ahead: Decimal = ZERO

    def __post_init__(self) -> None:
        if self.event_type not in {"quote", "trade", "cancel"}:
            raise ValueError(f"unsupported replay event type: {self.event_type}")
        if self.event_type == "cancel":
            if not self.quote_id:
                raise ValueError("cancel event requires quote_id")
            return
        if not self.token_id or self.side not in {"BUY", "SELL"}:
            raise ValueError(f"{self.event_type} event requires token_id and side")
        if not (ZERO < self.price < ONE) or self.size <= ZERO:
            raise ValueError(f"invalid {self.event_type} event price or size")
        if self.event_type == "quote":
            if not self.quote_id:
                raise ValueError("quote event requires quote_id")
            if self.queue_ahead < ZERO:
                raise ValueError("quote queue cannot be negative")
        elif self.queue_ahead != ZERO:
            raise ValueError("trade event cannot carry queue_ahead")


@dataclass(frozen=True)
class ReplayEvaluation:
    events: int
    quotes_placed: int
    trades_seen: int
    cancellations_seen: int
    fills: tuple[PaperFill, ...]
    total_filled_size: Decimal


def replay_maker_events(
    events: Sequence[PaperReplayEvent] | Iterable[PaperReplayEvent],
) -> ReplayEvaluation:
    rows = tuple(events)
    simulator = QueueAwareMakerSimulator()
    fills: list[PaperFill] = []
    quotes_placed = 0
    trades_seen = 0
    cancellations_seen = 0
    for event in rows:
        if event.event_type == "quote":
            simulator.place(
                MakerQuote(
                    token_id=event.token_id,
                    side=event.side,
                    price=event.price,
                    size=event.size,
                    queue_ahead=event.queue_ahead,
                    quote_id=event.quote_id,
                )
            )
            quotes_placed += 1
        elif event.event_type == "trade":
            fills.extend(
                simulator.on_trade(
                    token_id=event.token_id,
                    aggressor_side=event.side,
                    price=event.price,
                    size=event.size,
                )
            )
            trades_seen += 1
        else:
            if not simulator.cancel(event.quote_id):
                raise ValueError(f"cancel references unknown quote: {event.quote_id}")
            cancellations_seen += 1
    return ReplayEvaluation(
        events=len(rows),
        quotes_placed=quotes_placed,
        trades_seen=trades_seen,
        cancellations_seen=cancellations_seen,
        fills=tuple(fills),
        total_filled_size=sum((fill.size for fill in fills), ZERO),
    )


def _load_jsonl(
    path: str | Path,
    *,
    max_records: int,
) -> tuple[Mapping[str, Any], ...]:
    if max_records < 1:
        raise ValueError("max_records must be positive")
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"JSONL file not found: {source}")
    rows: list[Mapping[str, Any]] = []
    with source.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            if len(rows) >= max_records:
                raise ValueError(f"JSONL record limit exceeded: {max_records}")
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON at {source}:{line_number}: {exc.msg}") from exc
            if not isinstance(value, Mapping):
                raise ValueError(f"JSONL row must be an object at {source}:{line_number}")
            rows.append(value)
    return tuple(rows)


def _decimal(
    row: Mapping[str, Any],
    key: str,
    default: Any = _MISSING,
) -> Decimal:
    if key not in row:
        if default is _MISSING:
            raise ValueError(f"missing decimal field {key!r}")
        raw_value = default
    else:
        raw_value = row[key]
    try:
        value = Decimal(str(raw_value))
    except (ArithmeticError, ValueError) as exc:
        raise ValueError(f"invalid decimal field {key!r}") from exc
    if not value.is_finite():
        raise ValueError(f"decimal field {key!r} must be finite")
    return value


def _outcome(value: Any) -> int:
    if isinstance(value, bool) or value not in (0, 1, "0", "1"):
        raise ValueError("outcome must be 0 or 1")
    return int(value)


def load_shadow_candidates(
    path: str | Path,
    *,
    max_records: int = DEFAULT_MAX_RECORDS,
) -> tuple[ShadowCandidate, ...]:
    candidates: list[ShadowCandidate] = []
    for row in _load_jsonl(path, max_records=max_records):
        try:
            candidate_id = row["candidate_id"]
            if not isinstance(candidate_id, str):
                raise ValueError("candidate_id must be a string")
            candidate = ShadowCandidate(
                candidate_id=candidate_id,
                expected_probability=_decimal(row, "expected_probability"),
                entry_price=_decimal(row, "entry_price"),
                outcome=_outcome(row["outcome"]),
                filled_size=_decimal(row, "filled_size"),
                fees_paid=_decimal(row, "fees_paid", "0"),
            )
        except KeyError as exc:
            raise ValueError(f"missing shadow field: {exc.args[0]}") from exc
        candidates.append(candidate)
    return tuple(candidates)


def load_replay_events(
    path: str | Path,
    *,
    max_records: int = DEFAULT_MAX_RECORDS,
) -> tuple[PaperReplayEvent, ...]:
    events: list[PaperReplayEvent] = []
    for row in _load_jsonl(path, max_records=max_records):
        try:
            event = PaperReplayEvent(
                event_type=str(row["event_type"]),
                quote_id=str(row.get("quote_id", "")),
                token_id=str(row.get("token_id", "")),
                side=str(row.get("side", "")),
                price=_decimal(row, "price", "0"),
                size=_decimal(row, "size", "0"),
                queue_ahead=_decimal(row, "queue_ahead", "0"),
            )
        except KeyError as exc:
            raise ValueError(f"missing replay field: {exc.args[0]}") from exc
        events.append(event)
    return tuple(events)


class TapeReplay:
    """Timestamped maker-side replay, separate from historical aggressor replay.

    Queue events must be observed or explicitly labelled model assumptions.
    Aggregate public trades cannot generate the quote denominator. Markouts use
    the first bid at/after each horizon, within 60 seconds; absent marks are null.
    """
    def __init__(self, *, ghost_probability=0., seed=7, queue_decay_per_second=0.):
        import random
        if not 0<=ghost_probability<=1 or not 0<=queue_decay_per_second<=1:
            raise ValueError('invalid simulation probability')
        self.ghost_probability=ghost_probability
        self.decay=queue_decay_per_second
        self.rng=random.Random(seed)

    def run(self, events):
        import math
        if len(events)>DEFAULT_MAX_RECORDS: raise ValueError('event budget exceeded')
        quotes={}; fills=[]; marks=[]; previous=float('-inf')
        for event in events:
            at=float(event['at'])
            if not math.isfinite(at) or at<previous: raise ValueError('tape must be time ordered')
            if previous!=float('-inf'):
                for q in quotes.values(): q['queue_ahead']*=((1-self.decay)**(at-previous))
            previous=at
            for q in quotes.values():
                if at>=q['expires_at']: q['active']=False
            kind=event['type']
            if kind=='quote':
                q=dict(event)
                if q['quote_id'] in quotes: raise ValueError('duplicate quote id; explicit cancel and new id on repricing')
                if not all(math.isfinite(q[k]) for k in ('price','size','queue_ahead','expires_at')) or not 0<q['price']<1 or q['size']<=0 or q['queue_ahead']<0 or q['expires_at']<=at:
                    raise ValueError('invalid quote')
                # Multiple simultaneous hypothetical orders at the same level would double-count queue.
                if any(x['active'] and x['token_id']==q['token_id'] and x['price']==q['price'] for x in quotes.values()):
                    raise ValueError('one hypothetical quote per token/level')
                q.update(remaining=q['size'],active=True)
                quotes[q['quote_id']]=q
            elif kind in ('cancel','cancel_ahead','join_ahead'):
                q=quotes[event['quote_id']]
                if kind=='cancel': q['active']=False
                else:
                    size=float(event['size'])
                    if not math.isfinite(size) or size<0: raise ValueError('invalid queue delta')
                    q['queue_ahead']=max(0,q['queue_ahead']+size*(1 if kind=='join_ahead' else -1))
            elif kind=='maker_fill':
                if event['maker_side'] not in ('BUY','SELL'): raise ValueError('explicit maker side required')
                if event['maker_side']!='BUY': continue
                volume=float(event['size'])
                if not math.isfinite(volume) or volume<=0: raise ValueError('invalid maker fill')
                for q in quotes.values():
                    if not q['active'] or q['token_id']!=event['token_id'] or q['price']!=event['price']: continue
                    ahead=q['queue_ahead']; q['queue_ahead']=max(0,ahead-volume)
                    size=min(q['remaining'],max(0,volume-ahead))
                    if size and self.rng.random()>=self.ghost_probability:
                        q['remaining']-=size
                        fills.append({'quote_id':q['quote_id'],'token_id':q['token_id'],'at':at,'price':q['price'],'size':size,'markouts':{}})
                        if q['remaining']==0: q['active']=False
            elif kind=='mark': marks.append(event)
            else: raise ValueError('unknown tape event')
        for fill in fills:
            for horizon in (300,3600,10800):
                matches=[m for m in marks if m['token_id']==fill['token_id'] and fill['at']+horizon<=m['at']<=fill['at']+horizon+60]
                fill['markouts'][str(horizon)]=matches[0]['bid']-fill['price'] if matches else None
        return {'quotes':len(quotes),'fills':fills,'filled_size':sum(f['size'] for f in fills),'maker_fee':0,'rewards':0,
                'calibration_status':'insufficient_data','parameters_usable':False,'queue_decay_per_second':self.decay,'ghost_probability':self.ghost_probability}


def calibration_report(panel):
    """Compare independent held-out observed/modelled metrics, never fit and grade same tape."""
    required=('population_markout','simulated_markout','observed_fill_curve','simulated_fill_curve','winner_fill','loser_fill','fills','held_out','denominator_complete')
    if not panel or any(k not in panel for k in required):
        return {'status':'insufficient_data','parameters_usable':False,'reason':'independent held-out tape and quote denominator required'}
    if panel['denominator_complete'] is not True:
        return {'status':'identifiability_blocked','parameters_usable':False}
    if panel['held_out'] is not True or panel['fills']<65932:
        return {'status':'insufficient_data','parameters_usable':False}
    observed=panel['observed_fill_curve']; simulated=panel['simulated_fill_curve']
    if set(observed)!=set(simulated) or not observed:
        raise ValueError('matched queue strata required')
    import math
    values=[panel['population_markout'],panel['simulated_markout'],panel['winner_fill'],panel['loser_fill'],*observed.values(),*simulated.values()]
    if not all(math.isfinite(v) for v in values): raise ValueError('nonfinite calibration')
    mark_error=abs(panel['population_markout']-panel['simulated_markout'])
    curve_error=max(abs(observed[k]-simulated[k]) for k in observed)
    asymmetry=abs(panel['winner_fill']-.88)<=.05 and abs(panel['loser_fill']-1)<=.05
    passed=mark_error<=.005 and curve_error<=.05 and asymmetry
    return {'status':'pass' if passed else 'fail','parameters_usable':passed,'markout_error':mark_error,'fill_curve_error':curve_error,'asymmetry_reproduced':asymmetry}
