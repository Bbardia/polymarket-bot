from decimal import Decimal as D
from datetime import datetime, timezone
from src.v3.simulation import TapeReplay
from src.v3.maker_shadow import t1_shadow_decision


def test_tape_cancel_ahead_expiry_and_ghost():
    events=[{'type':'quote','at':0,'quote_id':'q','token_id':'t','price':.3,'size':5,'queue_ahead':10,'expires_at':200},
            {'type':'cancel_ahead','at':1,'quote_id':'q','size':5},
            {'type':'maker_fill','at':2,'token_id':'t','maker_side':'BUY','price':.3,'size':8},
            {'type':'mark','at':302,'token_id':'t','bid':.32}]
    result=TapeReplay(ghost_probability=0).run(events)
    assert result['filled_size']==3
    assert abs(result['fills'][0]['markouts']['300']-.02)<1e-8
    assert result['maker_fee']==0 and result['rewards']==0
    assert TapeReplay(ghost_probability=1).run(events)['filled_size']==0
    events[2]['at']=201
    assert TapeReplay().run(events)['filled_size']==0


def test_shadow_disabled_without_phi():
    result=t1_shadow_decision({},pmf=None,phi_evidence=None,now=datetime.now(timezone.utc))
    assert result['status']=='disabled' and result['execution_status']=='not_submitted'


def test_separate_variants_cancel_blackout_and_immutable_log(tmp_path):
    from src.v3.ladder_pmf import RungQuote,build_ladder_pmf
    from src.v3.maker_shadow import log_shadow_tick
    import pytest
    pmf=build_ladder_pmf([RungQuote('modal',D('.28'),D('.32'))]+[RungQuote(str(i),D('.01'),D('.02')) for i in range(10)])
    candidate={'rung':'modal','event_id':'event','timezone':'UTC','target_date':'2026-09-11','best_bid':.28,'best_ask':.32,'tick_size':.01,'queue_ahead':0,'resolver_verified':True,'best_ask_size':20,'rewards_min_size':20,'price_age_seconds':0,'cluster_sd':.001,'bankroll':50,'date_positions':0,'event_exposure':0,'gross_exposure':0,'event_cap':10,'bids':[[.28,20]],'asks':[[.32,20]]}
    evidence={'status':'pass','denominator_complete':True,'strata':{'0-5':{'clusters':60,'quotes':330,'phi':{'mean':.9,'lower':.85},'loser_fill':{'mean':1}}}}
    now=datetime(2026,9,10,13,tzinfo=timezone.utc)
    decision=t1_shadow_decision(candidate,pmf=pmf,phi_evidence=evidence,now=now)
    assert decision['status']=='shadow_intent'
    assert [v['variant'] for v in decision['variants']]==['A','B']
    assert [v['size'] for v in decision['variants']]==[20,5]
    assert decision['execution_status']=='not_submitted'
    log_shadow_tick(tmp_path,candidate=candidate,decision=decision,now=now)
    with pytest.raises(FileExistsError): log_shadow_tick(tmp_path,candidate=candidate,decision=decision,now=now)
    stopped=t1_shadow_decision(candidate,pmf=pmf,phi_evidence=evidence,now=datetime(2026,9,11,10,tzinfo=timezone.utc))
    assert stopped['status']=='cancel_intent'
