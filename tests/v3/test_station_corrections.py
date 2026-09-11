from src.v3.paper_weather import CITY_STATIONS, CITY_COORDS, CITY_TIMEZONES, AVOID_CITIES


def test_review_station_corrections():
    assert CITY_STATIONS['chicago']=='KORD'
    assert CITY_STATIONS['dallas']=='KDAL'
    assert CITY_STATIONS['paris']=='LFPB'
    assert CITY_STATIONS['moscow']=='UUWW'
    assert CITY_STATIONS['panama city']=='MPMG'
    assert CITY_STATIONS['taipei']=='RCSS'
    assert CITY_STATIONS['manila']=='RPLL'
    assert CITY_COORDS['istanbul']==(41.262,28.74)
    assert CITY_COORDS['qingdao']==(36.362,120.087)
    assert CITY_TIMEZONES['manila']=='Asia/Manila'
    assert 'hong kong' in AVOID_CITIES
