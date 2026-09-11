"""Honest passive-quote diagnostics that never claim simulated fills."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from .math import BookLevel

ZERO = Decimal("0")
ONE = Decimal("1")


@dataclass(frozen=True)
class MakerShadowQuote:
    side: str
    price: Decimal
    size: Decimal
    queue_ahead: Decimal
    expected_probability: Decimal
    best_bid: Decimal
    best_ask: Decimal
    edge: Decimal
    fill_status: str = field(default="unobserved", init=False)
    execution_status: str = field(default="not_submitted", init=False)
    cash_delta: Decimal = field(default=ZERO, init=False)
    inventory_delta: Decimal = field(default=ZERO, init=False)

    @property
    def expected_edge(self) -> Decimal:
        return self.edge

    @property
    def paper_tradeable(self) -> bool:
        return self.edge > ZERO


def propose_buy_quote(
    *,
    bids: tuple[BookLevel, ...],
    asks: tuple[BookLevel, ...],
    tick_size: Decimal,
    size: Decimal,
    expected_probability: Decimal,
) -> MakerShadowQuote:
    """Join or improve the bid without crossing the best ask."""

    if not bids or not asks:
        raise ValueError("maker shadow requires a two-sided book")
    if tick_size <= ZERO:
        raise ValueError("tick size must be positive")
    if size <= ZERO:
        raise ValueError("quote size must be positive")
    if not (ZERO <= expected_probability <= ONE):
        raise ValueError("expected probability must be in [0, 1]")

    best_bid = max(level.price for level in bids)
    best_ask = min(level.price for level in asks)
    if not (ZERO < best_bid < best_ask < ONE):
        raise ValueError("maker shadow requires a valid uncrossed book")

    improved = best_bid + tick_size
    price = improved if improved < best_ask else best_bid
    queue_ahead = sum(
        (level.size for level in bids if level.price == price),
        ZERO,
    )
    return MakerShadowQuote(
        side="BUY",
        price=price,
        size=size,
        queue_ahead=queue_ahead,
        expected_probability=expected_probability,
        best_bid=best_bid,
        best_ask=best_ask,
        edge=expected_probability - price,
    )


def t1_shadow_decision(candidate, *, pmf, phi_evidence, now, extremization=None):
    """Isolated never-submitted lane. Phi report must be generated from intentions."""
    from datetime import date, time, timedelta, datetime
    from zoneinfo import ZoneInfo
    from .research import decision_gate, admissible_clip, power_pmf
    if now.tzinfo is None: raise ValueError("aware decision timestamp required")
    base={'execution_status':'not_submitted','cash_delta':0,'inventory_delta':0,'status':'disabled','cancel_all_event':candidate.get('event_id')}
    if not phi_evidence or phi_evidence.get('status')!='pass' or phi_evidence.get('denominator_complete') is not True:
        return dict(base,reason='10b not passed')
    evidence=phi_evidence.get('strata',{}).get('0-5',{})
    phi=evidence.get('phi',{}).get('mean')
    if phi is None or not 0<phi<=1 or evidence.get('phi',{}).get('lower',0)>phi:
        return dict(base,reason='invalid conditional fill probability')
    if evidence.get('clusters',0)<60 or evidence.get('quotes',0)<330 or evidence.get('phi',{}).get('lower',0)<=.816 or evidence.get('loser_fill',{}).get('mean')!=1:
        return dict(base,reason='invalid phi evidence')
    if not pmf or not pmf.complete or candidate['rung'] not in pmf.q:
        return dict(base,reason='full-ladder PMF unavailable')
    local=now.astimezone(ZoneInfo(candidate['timezone']))
    day=date.fromisoformat(candidate['target_date'])
    start=datetime.combine(day-timedelta(days=1),time(12),local.tzinfo)
    end=datetime.combine(day,time(10),local.tzinfo)
    if local>=end:
        return dict(base,status='cancel_intent',reason='D0 local 10:00 cutoff',cancel_all_event=candidate['event_id'])
    if local<start: return dict(base,reason='outside T+1 quote window')
    bid=float(candidate['best_bid']); ask=float(candidate['best_ask']); tick=float(candidate['tick_size'])
    spread=ask-bid; queue=float(candidate['queue_ahead'])
    import math
    if not all(math.isfinite(x) for x in (bid,ask,tick,queue,candidate['best_ask_size'],candidate['rewards_min_size'])) or not 0<bid<ask<1:
        return dict(base,reason='invalid book values')
    if abs(float(pmf.raw[candidate['rung']])-(bid+ask)/2)>1e-8:
        return dict(base,reason='PMF and candidate book differ')
    if not candidate.get('resolver_verified') or tick!=.01 or spread<.02-1e-9 or spread>.10+1e-9 or candidate['best_ask_size']<5 or candidate['rewards_min_size']>20 or queue<0 or queue>5:
        return dict(base,reason='resolver/book/queue gate; phi only measured for 0-5')
    if candidate.get('price_age_seconds',float('inf'))>300: return dict(base,reason='stale book')
    labels=list(pmf.q)
    power=1.
    if extremization and extremization.get('status')=='pass' and extremization.get('dates',0)>=40 and extremization.get('lead')==1 and extremization.get('as_of','9999')<=local.date().isoformat():
        power=extremization['power']
    q=power_pmf([float(pmf.q[label]) for label in labels],power)[labels.index(candidate['rung'])]
    price=bid+tick
    q_fill=phi*q/(phi*q+1-q)
    ev_filled=q_fill-price
    ev_intended=phi*q*(1-price)-(1-q)*price
    if ev_intended<.015 or ev_filled<.010: return dict(base,reason='fill-selection EV gate')
    gate=decision_gate(q=q_fill,price=price,entry_fee=0,exit_cost=0,cluster_sd=candidate.get('cluster_sd'))
    if not gate['allowed']: return dict(base,reason='clustered uncertainty gate')
    if candidate.get('gross_exposure') is None:
        return dict(base,reason='portfolio exposure unavailable')
    variants=[]
    for variant in ('A','B'):
        if variant=='A':
            sized_bid=next((float(p) for p,s in sorted(candidate['bids'],reverse=True) if s>=candidate['rewards_min_size']),None)
            sized_ask=next((float(p) for p,s in sorted(candidate['asks']) if s>=candidate['rewards_min_size']),None)
            eligible=sized_bid is not None and sized_ask is not None and .10<=(sized_bid+sized_ask)/2<=.45
        else:
            eligible=candidate['rung']==max(pmf.raw,key=pmf.raw.get) and local.date()<day and spread>=.03-1e-9
        size=admissible_clip(q=q_fill,c=price,variance=candidate['cluster_sd']**2,bankroll=candidate['bankroll'],date_positions=candidate['date_positions'],event_exposure=candidate['event_exposure'],event_cap=candidate['event_cap'],reward=variant=='A',city_day_occupied=candidate.get('city_day_occupied',False)) if eligible else 0
        if size and candidate['gross_exposure']+size*price>.30*candidate['bankroll']: size=0
        if size: variants.append({'variant':variant,'size':size,'price':price,'side':'BUY','post_only':True,'ttl_seconds':121,'exit_policy':'hold_to_resolution_with_vetoes','q_final':q,'q_fill':q_fill,'ev_intended':ev_intended,'ev_filled':ev_filled})
    mid=(bid+ask)/2
    return dict(base,status='shadow_intent' if variants else 'skipped',variants=variants,
                cancel_previous=abs(mid-candidate.get('previous_mid',mid))>=tick-1e-9,
                queue_ahead=queue,local_hour=local.hour,running_max=candidate.get('running_max'),pool_rate=candidate.get('pool_rate',0),best_ask_size=candidate['best_ask_size'])


def log_shadow_tick(root, *, candidate, decision, now):
    """One create-only record per five-minute tick/event/rung; both variants separate."""
    import hashlib
    import json
    from pathlib import Path
    if now.tzinfo is None: raise ValueError('aware timestamp required')
    slot=int(now.timestamp())//300*300
    identity=hashlib.sha256((candidate['event_id']+'|'+candidate['rung']).encode()).hexdigest()
    path=Path(root)/str(slot)/(identity+'.json'); path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('x') as f:
        json.dump({'slot':slot,'logged_at':now.isoformat(),'candidate':candidate,'decision':decision},f,allow_nan=False,default=str)
    return path
