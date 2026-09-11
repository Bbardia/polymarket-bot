#!/usr/bin/env python3
"""Bounded public acquisition and offline research. Outputs are create-only."""
import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from src.v3.public_cache import PublicCache
from src.v3.research import phi_report, extremize_fit
from src.v3.scoring import evaluate_forecasts
from src.v3.gates import load_registry, evaluate_gate


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('command',choices=['acquire-tape','phi','evaluate','extremize','gates','replay','calibrate'])
    p.add_argument('--input',type=Path)
    p.add_argument('--out',type=Path,required=True)
    p.add_argument('--cache-dir',type=Path)
    p.add_argument('--denominator-complete',action='store_true',help='input is a complete quote-intention panel, including unfilled quotes')
    p.add_argument('--lead',type=int,default=1)
    p.add_argument('--as-of')
    a=p.parse_args()
    payload=json.loads(a.input.read_text()) if a.input else []
    # Research commands consume row lists.  Accept the durable replay report
    # shape as an empty event panel rather than failing with a string-index
    # error; never treat summary metrics as replay events.
    if a.command == 'replay' and isinstance(payload, dict):
        rows = payload.get('events', [])
        if not isinstance(rows, list):
            raise ValueError('replay input events must be a list')
    else:
        rows = payload
    if a.command=='acquire-tape':
        if not a.cache_dir: p.error('--cache-dir required')
        cache=PublicCache(a.cache_dir,max_requests=1,max_bytes=2_000_000)
        url='https://data-api.polymarket.com/trades?limit=100&offset=0&takerOnly=false'
        try:
            tape=cache.get_json(url)
            if not isinstance(tape,list): raise ValueError('unexpected tape schema')
            report={'status':'identifiability_blocked','raw_records':len(tape),'source_url':url,
                    'reason':'public transaction rows do not supply complete resting intentions, queue or unfilled denominator',
                    'calibration_status':'insufficient_data','maker_parameters_usable':False}
        except Exception as e:
            report={'status':'acquisition_failed','raw_records':0,'source_url':url,'error':str(e),'calibration_status':'insufficient_data'}
    elif a.command=='calibrate':
        from src.v3.simulation import calibration_report
        report=calibration_report(rows)
    elif a.command=='replay':
        from src.v3.simulation import TapeReplay
        report=TapeReplay().run(rows)
    elif a.command=='phi': report=phi_report(rows,denominator_complete=a.denominator_complete)
    elif a.command=='evaluate': report=evaluate_forecasts(rows)
    elif a.command=='extremize':
        if not a.as_of: p.error('--as-of required')
        report=extremize_fit(rows,lead=a.lead,as_of=a.as_of)
    else:
        registry=load_registry(Path(__file__).resolve().parents[1]/'config/gates/gate_registry.json')
        report={'gates':[vars(evaluate_gate(g,rows.get(g['lane']) if isinstance(rows,dict) else None)) for g in registry['gates']]}
    a.out.parent.mkdir(parents=True,exist_ok=True)
    with a.out.open('x') as f: json.dump(report,f,indent=2,allow_nan=False)
    print(json.dumps(report,indent=2,allow_nan=False))

if __name__=='__main__': main()
