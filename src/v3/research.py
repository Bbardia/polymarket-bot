"""Offline, date-clustered research and conservative shadow policy. No order client."""
from __future__ import annotations
import math
from collections import defaultdict
from .scoring import cluster_bootstrap_mean


def phi_report(rows, *, denominator_complete=False):
    """Quote-intention panel required: fills alone cannot identify conditional fill rates."""
    if not denominator_complete:
        return {'status':'identifiability_blocked','quotes':0,'reason':'complete resting quote denominator unavailable; fills are not intentions'}
    ids = [r['quote_id'] for r in rows]
    if len(set(ids)) != len(ids):
        raise ValueError('duplicate quote intentions')
    buckets = defaultdict(list)
    for row in rows:
        if type(row['won']) is not bool or type(row['filled']) is not bool:
            raise ValueError('settled outcome and observed fill required')
        queue = float(row['queue_ahead'])
        if not math.isfinite(queue) or queue < 0:
            raise ValueError('invalid queue')
        buckets['0-5' if queue<=5 else '5-20' if queue<=20 else '20-50' if queue<=50 else '>50'].append(row)
    reports = {}
    for bucket, panel in buckets.items():
        report = {'quotes':len(panel),'clusters':len({r['target_date'] for r in panel})}
        for outcome, name in ((True,'phi'),(False,'loser_fill')):
            grouped = defaultdict(list)
            for r in panel:
                if r['won'] is outcome: grouped[r['target_date']].append(float(r['filled']))
            if grouped:
                ci=cluster_bootstrap_mean(grouped)
                report[name]={'mean':ci.mean,'lower':ci.lower,'upper':ci.upper,'clusters':ci.clusters,'quotes':ci.observations,'filled':sum(r['filled'] for r in panel if r['won'] is outcome)}
        reports[bucket]=report
    eligible=reports.get('0-5',{})
    phi=eligible.get('phi',{})
    loser=eligible.get('loser_fill',{})
    enough=eligible.get('clusters',0)>=60 and eligible.get('quotes',0)>=330 and phi.get('filled',0)>=151 and phi.get('clusters',0)>=60 and loser.get('clusters',0)>=60
    # The 0.816 break-even is conditional on loser fills being one; refuse transfer otherwise.
    passed=enough and phi['lower']>.816 and loser['mean']==1
    return {'status':'pass' if passed else 'fail' if enough else 'insufficient_data','quotes':len(rows),'strata':reports,'denominator_complete':True}


def power_pmf(q, power):
    if not q or any(not math.isfinite(x) or x<0 for x in q) or abs(sum(q)-1)>1e-6:
        raise ValueError('invalid PMF')
    if not 1<=power<=1.35: raise ValueError('power outside preregistered range')
    values=[x**power for x in q]; norm=sum(values)
    return [x/norm for x in values]


def extremize_fit(rows, *, lead, as_of, window=60):
    if any(r['target_date']>=as_of for r in rows):
        raise ValueError('training labels must predate decision date')
    if window<40: raise ValueError('rolling window must cover at least 40 dates')
    rows=[r for r in rows if r['lead']==lead]
    dates=sorted({r['target_date'] for r in rows})[-window:]
    rows=[r for r in rows if r['target_date'] in dates]
    if len(dates)<40: return {'status':'insufficient_data','power':1.,'dates':len(dates)}
    grid=[1+i/100 for i in range(36)]
    def loss(r,p): return -math.log(max(power_pmf(r['q'],p)[r['winner']],1e-15))
    def fit(panel): return min(grid,key=lambda p:sum(loss(r,p) for r in panel))
    gains={}
    for day in dates:
        p=fit([r for r in rows if r['target_date']!=day])
        gains[day]=[loss(r,1)-loss(r,p) for r in rows if r['target_date']==day]
    ci=cluster_bootstrap_mean(gains)
    passed=ci.lower>0
    return {'status':'pass' if passed else 'fail','power':fit(rows) if passed else 1.,'dates':len(dates),'skill_ci':vars(ci),'lead':lead,'as_of':as_of,'method':'leave-one-date-out; all training labels before as_of'}


def decision_gate(*,q,price,entry_fee,exit_cost,cluster_sd,z=1.96):
    if cluster_sd is None: return {'allowed':False,'reason':'insufficient_data: clustered uncertainty missing'}
    values=(q,price,entry_fee,exit_cost,cluster_sd,z)
    if not all(math.isfinite(x) for x in values) or not 0<=q<=1 or not 0<price<1 or min(values[2:])<0:
        raise ValueError('invalid decision inputs')
    cost=price+entry_fee+exit_cost
    required=z*cluster_sd
    return {'allowed':q-cost>required,'all_in_cost':cost,'uncertainty':required,'edge':q-cost,'base_edge':0}


def admissible_clip(*,q,c,variance,bankroll,date_positions,event_exposure,event_cap,reward=False,city_day_occupied=False):
    if not all(math.isfinite(x) for x in (q,c,variance,bankroll,event_exposure,event_cap)) or not 0<=q<=1 or not 0<c<1 or min(variance,bankroll,event_exposure,event_cap,date_positions)<0:
        raise ValueError('invalid sizing inputs')
    clip=20 if reward else 5
    if date_positions>=3 or city_day_occupied or q<=c: return 0
    f=(q-c)/(1-c)*max(0,1-variance/(q-c)**2)
    design=1+date_positions*.30
    return clip if clip*c<=f*bankroll/design and event_exposure+clip*c<=event_cap else 0


def forecast_veto(mu,lead,city,*,city_offsets=None):
    if lead not in (0,1,2) or not math.isfinite(mu): raise ValueError('invalid forecast')
    offsets=city_offsets or {}
    return {'mu':mu+(.58,.64,.66)[lead]+offsets.get(city,0), 'sigma':max(1.25,(1.38,1.40,1.58)[lead]),'df':7,'market_pool_weight':0,'usage':'veto_telemetry_only'}


def exit_intent(*,q,bid,friction,dead,resolver_certain):
    if bid is None or bid<=0: return 'hold'
    if resolver_certain: return 'resolver_certain_close'
    if dead: return 'dead_rung_close'
    if q<bid-friction: return 'maker_only_edge_flip'
    return 'hold'


def running_max_telemetry(rows, *, station, day, zone, unit, now, upper, bid):
    from .station_labels import daily_label
    label=daily_label([r for r in rows if r['observed_at']<=now and r.get('available_at') is not None and r['available_at']<=now],station=station,day=day,zone=zone,unit=unit,acquired_at=now)
    dead=label['label'] is not None and upper is not None and float(label['label'])>upper
    return {'poll_due':18<=now.minute<=26 or 48<=now.minute<=56,'running_max':label['label'],'dead':dead,'dead_unfillable':dead and (bid is None or bid<=0),'action':'veto' if dead else 'observe'}


def fit_city_offsets(rows, *, as_of):
    """Empirical-Bayes residual intercepts from station/grid panels, never trades."""
    if any(r['target_date']>=as_of or r.get('truth_source')!='station' or r.get('selected_from_trades') is not False for r in rows):
        raise ValueError('requires earlier unselected station panel')
    grouped=defaultdict(list)
    for r in rows:
        grouped[r['city']].append(r['observation']-r['forecast']-(.58,.64,.66)[r['lead']])
    eligible={city:values for city,values in grouped.items() if len(values)>=20}
    if len(eligible)<3: return {'status':'insufficient_data','city_offsets':{}}
    means={city:sum(v)/len(v) for city,v in eligible.items()}
    variances={city:sum((x-means[city])**2 for x in v)/(len(v)-1)/len(v) for city,v in eligible.items()}
    tau=max(0,sum(m*m for m in means.values())/len(means)-sum(variances.values())/len(variances))
    return {'status':'fit_for_veto_only','city_offsets':{city:mean*tau/(tau+variances[city]) if tau+variances[city]>0 else 0 for city,mean in means.items()},'as_of':as_of}
