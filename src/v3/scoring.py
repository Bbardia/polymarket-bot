"""Continuous-forecast evaluation: CRPS, PIT, coverage, clustered CI (item 8).

Formulas verified against standard references rather than copied from the
review:

* Gaussian CRPS (Gneiting & Raftery 2007):
  ``CRPS(N(mu, s), y) = s * [ z*(2*Phi(z) - 1) + 2*phi(z) - 1/sqrt(pi) ]``
  with ``z = (y - mu)/s``.
* PIT value ``Phi((y - mu)/s)``; uniform under calibration.
* Central interval coverage at level ``alpha``: fraction of ``|z| <= z_alpha``.
* Cluster bootstrap: resample clusters (weather days) with replacement.

No decision code imports this module; it is evaluation only.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from statistics import NormalDist
from typing import Iterable, Mapping, Sequence

_STD_NORMAL = NormalDist()


def gaussian_crps(mu: float, sigma: float, observation: float) -> float:
    if not all(math.isfinite(x) for x in (mu, sigma, observation)) or sigma <= 0:
        raise ValueError("sigma must be positive")
    z = (observation - mu) / sigma
    return sigma * (z * (2.0 * _STD_NORMAL.cdf(z) - 1.0) + 2.0 * _STD_NORMAL.pdf(z) - 1.0 / math.sqrt(math.pi))


def pit_value(mu: float, sigma: float, observation: float) -> float:
    if not all(math.isfinite(x) for x in (mu, sigma, observation)) or sigma <= 0:
        raise ValueError("sigma must be positive")
    return _STD_NORMAL.cdf((observation - mu) / sigma)


def interval_coverage(pits: Sequence[float], level: float) -> float:
    if not pits:
        raise ValueError("no PIT values")
    lower, upper = (1.0 - level) / 2.0, 1.0 - (1.0 - level) / 2.0
    return sum(1 for p in pits if lower <= p <= upper) / len(pits)


def pit_ks_statistic(pits: Sequence[float]) -> float:
    """Kolmogorov-Smirnov distance from the uniform distribution."""
    if not pits:
        raise ValueError("no PIT values")
    ordered = sorted(pits)
    n = len(ordered)
    d = 0.0
    for index, value in enumerate(ordered, start=1):
        d = max(d, abs(index / n - value), abs(value - (index - 1) / n))
    return d


@dataclass(frozen=True)
class ClusteredMean:
    mean: float
    lower: float
    upper: float
    clusters: int
    observations: int


def cluster_bootstrap_mean(
    values_by_cluster: Mapping[str, Sequence[float]],
    *,
    level: float = 0.95,
    replicates: int = 2000,
    seed: int = 7,
) -> ClusteredMean:
    """Bootstrap the mean over observations by resampling clusters (weather days)."""
    if not 0 < level < 1 or not 100 <= replicates <= 10000:
        raise ValueError("invalid bootstrap configuration")
    clusters = [tuple(v) for v in values_by_cluster.values() if v]
    if any(not math.isfinite(x) for v in clusters for x in v):
        raise ValueError("nonfinite bootstrap observation")
    if not clusters:
        raise ValueError("no clusters")
    total = [x for cluster in clusters for x in cluster]
    point = sum(total) / len(total)
    rng = random.Random(seed)
    means = []
    for _ in range(replicates):
        sample = [clusters[rng.randrange(len(clusters))] for _ in clusters]
        flat = [x for cluster in sample for x in cluster]
        means.append(sum(flat) / len(flat))
    means.sort()
    lo_index = int((1.0 - level) / 2.0 * (replicates - 1))
    hi_index = int((1.0 - (1.0 - level) / 2.0) * (replicates - 1))
    return ClusteredMean(point, means[lo_index], means[hi_index], len(clusters), len(total))


def crps_skill_by_cluster(
    rows: Iterable[tuple[str, float, float, float, float, float]],
) -> Mapping[str, list[float]]:
    """rows: (cluster, model_mu, model_sigma, market_mu, market_sigma, observation).

    Returns per-cluster lists of ``CRPS_market - CRPS_model`` (positive means the
    model beat the market on that observation).
    """
    out: dict[str, list[float]] = {}
    for cluster, m_mu, m_sigma, k_mu, k_sigma, y in rows:
        diff = gaussian_crps(k_mu, k_sigma, y) - gaussian_crps(m_mu, m_sigma, y)
        out.setdefault(cluster, []).append(diff)
    return out


def student_t_crps(mu: float, sigma: float, observation: float, df: float = 7) -> float:
    """Closed-form CRPS for a location/scale Student distribution (df > 1)."""
    from scipy.stats import t
    from scipy.special import beta
    if not all(math.isfinite(x) for x in (mu,sigma,observation,df)) or sigma<=0 or df<=1:
        raise ValueError('invalid Student parameters')
    z=(observation-mu)/sigma
    constant=2*math.sqrt(df)/(df-1)*beta(.5,df-.5)/beta(.5,df/2)**2
    return float(sigma*(z*(2*t.cdf(z,df)-1)+2*t.pdf(z,df)*(df+z*z)/(df-1)-constant))


def discrete_crps(values, probabilities, observation):
    if len(values)!=len(probabilities) or not values or abs(sum(probabilities)-1)>1e-6 or any(p<0 for p in probabilities):
        raise ValueError('invalid baseline distribution')
    return sum(p*abs(x-observation) for x,p in zip(values,probabilities))-.5*sum(p*q*abs(x-y) for x,p in zip(values,probabilities) for y,q in zip(values,probabilities))


def brier_decomposition(probabilities, outcomes):
    if not probabilities or len(probabilities)!=len(outcomes) or any(y not in (0,1) for y in outcomes) or any(not 0<=p<=1 for p in probabilities):
        raise ValueError('invalid Brier panel')
    # Exact decomposition by identical forecasts; no hidden binning approximation.
    groups={}
    for p,y in zip(probabilities,outcomes): groups.setdefault(p,[]).append(y)
    n=len(outcomes); base=sum(outcomes)/n
    reliability=sum(len(ys)/n*(p-sum(ys)/len(ys))**2 for p,ys in groups.items())
    resolution=sum(len(ys)/n*(sum(ys)/len(ys)-base)**2 for ys in groups.values())
    uncertainty=base*(1-base)
    return {'brier':sum((p-y)**2 for p,y in zip(probabilities,outcomes))/n,'reliability':reliability,'resolution':resolution,'uncertainty':uncertainty}


def evaluate_forecasts(rows):
    """Point-in-time panel, pooled diagnostics; promotion remains per-unit and OOS."""
    from scipy.stats import t, kstest
    if not rows: return {'status':'insufficient_data','observations':0,'clusters':0}
    scores=[]; pits=[]; gains={}
    for r in rows:
        from datetime import datetime, date
        stamps={k:datetime.fromisoformat(r[k].replace('Z','+00:00')) for k in ('forecast_available_at','decision_at','truth_available_at','market_available_at')}
        if any(t.tzinfo is None for t in stamps.values()): raise ValueError('aware evaluation timestamps required')
        if not stamps['forecast_available_at']<=stamps['decision_at']<stamps['truth_available_at'] or stamps['market_available_at']>stamps['decision_at'] or date.fromisoformat(r['climatology_last_date'])>=date.fromisoformat(r['target_date']):
            raise ValueError('look-ahead in evaluation panel')
        y=r['observation']; mu=r['mu']; sigma=r['sigma']
        student=r.get('distribution','gaussian')=='student_t'
        score=student_t_crps(mu,sigma,y) if student else gaussian_crps(mu,sigma,y)
        pit=float(t.cdf((y-mu)/sigma,7)) if student else pit_value(mu,sigma,y)
        market=discrete_crps(r['market_values'],r['market_q'],y)
        climate=discrete_crps(r['climatology_values'],r['climatology_q'],y)
        scores.append({'model':score,'market':market,'climatology':climate,'point':abs(mu-y)})
        pits.append(pit); gains.setdefault(r['target_date'],[]).append(market-score)
    ci=cluster_bootstrap_mean(gains)
    # Encompassing weight requires a separate out-of-sample fit; no pass from CRPS alone.
    report = {'status':'insufficient_data' if ci.clusters<400 else 'evidence_blocked',
            'reason':'requires per-unit clustered OOS encompassing regression and PIT validation',
            'observations':len(rows),'clusters':ci.clusters,'crps':{k:sum(s[k] for s in scores)/len(scores) for k in scores[0]},
            'skill_ci':vars(ci),'pit_ks':float(kstest(pits,'uniform').statistic),'pit_ks_pvalue':float(kstest(pits,'uniform').pvalue),
            'coverage':{str(level):interval_coverage(pits,level) for level in (.9,.95,.99)},
            'pit_histogram':[sum(i/10<=p<(i+1)/10 or i==9 and p==1 for p in pits) for i in range(10)]}
    binary=[r for r in rows if all(k in r for k in ('p_model','q_market','outcome'))]
    if len(binary)==len(rows):
        probabilities=[r['p_model'] for r in rows]; outcomes=[r['outcome'] for r in rows]
        report['model_brier']=brier_decomposition(probabilities,outcomes)
        report['market_brier']=brier_decomposition([r['q_market'] for r in rows],outcomes)
        report['spiegelhalter_z_descriptive_only']=spiegelhalter_z(probabilities,outcomes) if ci.clusters>=20 else None
        report['encompassing']=encompassing_oos(rows)
    else:
        report['tier2_status']='insufficient_data: complete unselected binary panel missing'
    return report


def encompassing_oos(rows, *, min_training_dates=20, bootstrap_replicates=200):
    """Walk-forward logit pool weights; date-clustered OOS score improvement.

    Weights are fitted on earlier dates only. Positive coefficients alone never
    promote: held-out score improvement must also exclude zero. This diagnostic
    is bounded to 200 bootstrap replicates on the Pi.
    """
    import numpy as np
    from scipy.optimize import minimize
    from scipy.special import expit, logit
    dates=sorted({r['target_date'] for r in rows})
    if len(dates)<=min_training_dates:
        return {'status':'insufficient_data','oos_clusters':0}
    def fit(panel):
        x=np.array([[1,logit(min(.999999,max(.000001,r['q_market']))),logit(min(.999999,max(.000001,r['p_model'])))] for r in panel])
        y=np.array([r['outcome'] for r in panel])
        def objective(b):
            z=x@b
            return np.mean(np.logaddexp(0,z)-y*z)+1e-6*np.sum(b*b)
        result=minimize(objective,np.array([0.,1.,0.]),method='BFGS')
        if not result.success: return None
        return result.x
    gains={}; weights={}
    for day in dates[min_training_dates:]:
        train=[r for r in rows if r['target_date']<day]
        b=fit(train)
        if b is None: continue
        test=[r for r in rows if r['target_date']==day]
        gains[day]=[]; weights[day]=[float(b[2])]
        for r in test:
            q=min(.999999,max(.000001,r['q_market'])); p=min(.999999,max(.000001,r['p_model'])); y=r['outcome']
            pred=float(expit(b[0]+b[1]*logit(q)+b[2]*logit(p)))
            pred=min(.999999,max(.000001,pred))
            gains[day].append((-y*math.log(q)-(1-y)*math.log(1-q))-(-y*math.log(pred)-(1-y)*math.log(1-pred)))
    if not gains: return {'status':'insufficient_data','oos_clusters':0}
    ci=cluster_bootstrap_mean(gains,replicates=bootstrap_replicates)
    coefficient=cluster_bootstrap_mean(weights,replicates=bootstrap_replicates)
    return {'status':'pass' if ci.clusters>=400 and ci.lower>0 and coefficient.lower>0 else 'insufficient_data' if ci.clusters<400 else 'fail',
            'oos_clusters':ci.clusters,'logloss_skill':vars(ci),'forecast_weight':vars(coefficient),
            'coefficient_ci_interpretation':'variation of rolling fitted weights by held-out date, not an independent coefficient sampling CI'}


def spiegelhalter_z(probabilities, outcomes):
    numerator=sum((y-p)**2-p*(1-p) for p,y in zip(probabilities,outcomes))
    variance=sum((1-2*p)**2*p*(1-p) for p in probabilities)
    return None if variance<=0 else numerator/math.sqrt(variance)
