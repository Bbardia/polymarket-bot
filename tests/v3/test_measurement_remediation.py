from datetime import date, datetime, timezone
from decimal import Decimal as D
import pytest
from src.v3.ladder_pmf import RungQuote, build_ladder_pmf
from src.v3.station_labels import daily_label, local_day_window, temperature_c, validate_manifest, parse_aviation
from src.v3.gates import evaluate_gate


def test_invalid_partition_and_book():
    with pytest.raises(ValueError):
        build_ladder_pmf([RungQuote('a', D('.2'), D('.1'))]*11)
    with pytest.raises(ValueError):
        build_ladder_pmf([RungQuote(str(i), D('.1'), D('.2')) for i in range(9)], total_rungs=11)


def test_full_ladder_uncensored():
    rungs = [RungQuote(str(i), D('.001') if i==0 else D('.1'), None) for i in range(9)]
    rungs += [RungQuote(str(i), None, None) for i in range(9,11)]
    pmf = build_ladder_pmf(rungs)
    assert pmf.complete and abs(sum(pmf.q.values())-1)<D('1e-20')
    assert '0' in pmf.q and len(pmf.missing_rungs)==2


def test_dst_tgroup_and_manifest():
    a,b=local_day_window(date(2026,3,8),'America/New_York')
    assert (b-a).total_seconds()==23*3600
    assert temperature_c('KJFK 101200Z 00000KT 10SM CLR 20/10 RMK T02061000',use_tenths=True)==D('20.6')
    with pytest.raises(ValueError): validate_manifest([{'event_id':'x'}])
    rows=parse_aviation([{'icaoId':'KJFK','obsTime':1789041600,'metarType':'SPECI','rawOb':'SPECI KJFK 101200Z 00000KT 10SM CLR 20/10'}], 'KJFK')
    assert len(rows)==1 and rows[0]['report_type']=='SPECI'


@pytest.mark.parametrize('claim,field,value',[
 ('intraday_snipe','price_age_seconds',3590),
 ('flip_side','settled_only',True),
 ('cheap_longshots','argmax_selected',True),
 ('dead_rung_capture','crossable_quotes',0),
])
def test_refuted_claim_rules(claim,field,value):
    gate={'lane':claim,'min_clusters':20,'preregistered_cell':'fixed','threshold':{'metric':'skill','lower_bound_must_exceed':0}}
    evidence={'clusters':60,'cell':'fixed','skill':1,'skill_ci_lower':.5,'decision_interval_seconds':300,field:value}
    assert evaluate_gate(gate,evidence).verdict=='fail'


def test_station_daily_completeness_and_foreign_station():
    from datetime import timedelta
    start,end=local_day_window(date(2026,9,9),'America/New_York')
    rows=[{'observed_at':start+timedelta(hours=i),'raw':'KJFK 091200Z 00000KT 10SM CLR 20/10 RMK T02061000','report_type':'METAR','source':'iem-asos-report-types-3-4'} for i in range(24)]
    label=daily_label(rows,station='KJFK',day=date(2026,9,9),zone='America/New_York',unit='F',acquired_at=end)
    assert label['status']=='label_available' and label['label']=='69'
    label=daily_label(rows[:5],station='KJFK',day=date(2026,9,9),zone='America/New_York',unit='F',acquired_at=end)
    assert label['status']=='insufficient_data'
    rows[0]['raw']=rows[0]['raw'].replace('KJFK','KORD')
    with pytest.raises(ValueError): daily_label(rows,station='KJFK',day=date(2026,9,9),zone='America/New_York',unit='F',acquired_at=end)


def test_cache_integrity_and_allowlist(tmp_path):
    import hashlib,json
    from src.v3.public_cache import PublicCache
    cache=PublicCache(tmp_path,offline=True)
    with pytest.raises(ValueError): cache.get_text('https://evil.example/data')
    url='https://aviationweather.gov/api/data/metar?ids=KJFK&format=json'
    key=hashlib.sha256(url.encode()).hexdigest()
    (tmp_path/(key+'.raw')).write_text('[]')
    (tmp_path/(key+'.json')).write_text(json.dumps({'url':url,'sha256':'bad'}))
    with pytest.raises(ValueError): cache.get_text(url)
