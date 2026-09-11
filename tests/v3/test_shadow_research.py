import json
import subprocess
import sys

import pytest
from src.v3.research import phi_report, extremize_fit, admissible_clip, forecast_veto, exit_intent, decision_gate
from src.v3.scoring import student_t_crps, evaluate_forecasts


def test_phi_never_invents_denominator():
    assert phi_report([], denominator_complete=False)['status']=='identifiability_blocked'
    assert phi_report([], denominator_complete=True)['status']=='insufficient_data'


def test_phi_cluster_requirement_and_selection():
    rows=[{'quote_id':str(i),'target_date':str(i//10),'queue_ahead':0,'won':True,'filled':True} for i in range(330)]
    assert phi_report(rows,denominator_complete=True)['status']=='insufficient_data'
    with pytest.raises(ValueError): phi_report(rows+rows,denominator_complete=True)


def test_sizing_skip_and_variance():
    assert admissible_clip(q=.52,c=.5,variance=0,bankroll=50,date_positions=0,event_exposure=0,event_cap=10)==0
    assert admissible_clip(q=.7,c=.4,variance=0,bankroll=50,date_positions=0,event_exposure=0,event_cap=10)==5
    assert admissible_clip(q=.7,c=.4,variance=.1,bankroll=50,date_positions=0,event_exposure=0,event_cap=10)==0
    assert admissible_clip(q=.9,c=.1,variance=0,bankroll=50,date_positions=3,event_exposure=0,event_cap=10)==0


def test_forecast_not_fair_value_and_hold():
    assert forecast_veto(20,1,'unknown')['market_pool_weight']==0
    assert forecast_veto(20,2,'unknown')['sigma']>=1.25
    assert exit_intent(q=.6,bid=.5,friction=.01,dead=False,resolver_certain=False)=='hold'
    assert exit_intent(q=.4,bid=.5,friction=.01,dead=False,resolver_certain=False)=='maker_only_edge_flip'
    assert not decision_gate(q=.52,price=.5,entry_fee=0,exit_cost=0,cluster_sd=None)['allowed']


def test_extremization_no_future_or_short_history():
    assert extremize_fit([],lead=1,as_of='2026-09-11')['power']==1
    with pytest.raises(ValueError): extremize_fit([{'target_date':'2026-09-12','lead':1,'q':[.5,.5],'winner':0}],lead=1,as_of='2026-09-11')


def test_student_crps_and_eval_gate():
    assert student_t_crps(0,1,0)>0
    assert student_t_crps(0,1,2)>student_t_crps(0,1,0)
    assert evaluate_forecasts([])['status']=='insufficient_data'


def test_brier_exact_decomposition_and_crps_oracle():
    from scipy.integrate import quad
    from scipy.stats import t
    from src.v3.scoring import brier_decomposition
    result=brier_decomposition([.2,.2,.7,.7],[0,1,1,1])
    assert result['brier']==pytest.approx(result['reliability']-result['resolution']+result['uncertainty'])
    numeric=quad(lambda x:t.cdf(x,7)**2,-float('inf'),.5)[0]+quad(lambda x:(1-t.cdf(x,7))**2,.5,float('inf'))[0]
    assert student_t_crps(0,1,.5)==pytest.approx(numeric,abs=1e-7)


def test_extremization_folds_and_no_signal_fallback():
    from datetime import date,timedelta
    rows=[{'target_date':str(date(2026,1,1)+timedelta(days=i)),'lead':1,'q':[.5,.5],'winner':i%2} for i in range(40)]
    result=extremize_fit(rows,lead=1,as_of='2026-09-11')
    assert result['dates']==40 and result['power']==1 and result['status']=='fail'


def test_no_lookahead_forecasts_and_runningmax():
    from datetime import date, datetime, timezone
    from src.v3.research import running_max_telemetry
    with pytest.raises(ValueError):
        evaluate_forecasts([{'forecast_available_at':'2026-09-12','market_available_at':'2026-09-09','decision_at':'2026-09-10','truth_available_at':'2026-09-11','climatology_last_date':'2026-09-08','target_date':'2026-09-10'}])
    now=datetime(2026,9,10,12,50,tzinfo=timezone.utc)
    row={'observed_at':now,'report_type':'METAR','source':'aviationweather-metar','raw':'KJFK 101250Z 00000KT 10SM CLR 30/20'}
    telemetry=running_max_telemetry([row],station='KJFK',day=date(2026,9,10),zone='America/New_York',unit='C',now=now,upper=29,bid=None)
    assert telemetry['running_max'] is None
    row['available_at']=now
    telemetry=running_max_telemetry([row],station='KJFK',day=date(2026,9,10),zone='America/New_York',unit='C',now=now,upper=29,bid=None)
    assert telemetry['dead_unfillable'] and telemetry['poll_due']


def test_research_replay_cli_accepts_empty_report_shape(tmp_path):
    source = tmp_path / 'replay-report.json'
    output = tmp_path / 'replayed.json'
    source.write_text(json.dumps({'quotes': 0, 'fills': []}))
    result = subprocess.run(
        [sys.executable, 'scripts/remediation_research.py', 'replay',
         '--input', str(source), '--out', str(output)],
        check=True, capture_output=True, text=True,
    )
    assert json.loads(result.stdout)['quotes'] == 0
