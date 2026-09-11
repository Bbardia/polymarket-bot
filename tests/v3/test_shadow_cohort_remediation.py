import json
from decimal import Decimal as D
from src.v3.shadow_settlement import load_cohort, reconcile


def test_complete_set_preserves_both_sides_and_total_cost(tmp_path):
    trade={'paper_executed':True,'strategy':'complete_set','condition_id':'c','candidate_id':'q','opportunity':{'shares':'5','gross_cost':'4.8','fees':'.1'}}
    (tmp_path/'paper_trades.jsonl').write_text(json.dumps(trade)+'\n')
    panel=load_cohort(tmp_path)
    assert len(panel)==2
    assert sum(o.all_in_cost for o in panel.values())==D('4.9')
    report=reconcile(panel,getter=lambda _: {'tokens':[{'outcome':'Yes','winner':True},{'outcome':'No','winner':False}]})
    assert report['cohort']['realized_pnl_known']=='0.10'


def test_repeated_condition_is_not_silently_overwritten(tmp_path):
    rows=[{'paper_executed':True,'strategy':'weather','condition_id':'c','candidate_id':str(i),'shares':'5','all_in_cost':'2','side':'YES'} for i in range(2)]
    (tmp_path/'paper_trades.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in rows))
    assert len(load_cohort(tmp_path))==2
