#!/usr/bin/env python3
"""Nightly public-data label collector. Input: JSON list of per-market rules and zones."""
import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from src.v3.public_cache import PublicCache
from src.v3.station_labels import collect_label, validate_manifest


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest',type=Path,required=True)
    p.add_argument('--cache-dir',type=Path,required=True)
    p.add_argument('--out',type=Path,required=True)
    p.add_argument('--max-events',type=int,default=100)
    p.add_argument('--offline',action='store_true')
    a=p.parse_args()
    events=json.loads(a.manifest.read_text())
    if not isinstance(events,list) or len(events)>a.max_events:
        p.error('manifest exceeds event budget or is not a list')
    validate_manifest(events, max_events=a.max_events)
    cache=PublicCache(a.cache_dir,max_requests=a.max_events,offline=a.offline)
    rows=[]
    for event in events:
        try: rows.append(collect_label(event,cache))
        except Exception as e:
            rows.append({'event_id':event.get('event_id'),'status':'acquisition_or_validation_error','error':str(e)})
    scored=[r['winner_match'] for r in rows if r.get('winner_match') is not None]
    report={'generated_at':datetime.now(timezone.utc).isoformat(),'events':len(rows),
        'labels_available':sum(r['status']=='label_available' for r in rows),
        'winner_validated':len(scored),'winner_agreement':sum(scored)/len(scored) if scored else None,
        'gate_status':'pass' if len(scored)>=500 and sum(scored)/len(scored)>=.995 else 'insufficient_data' if len(scored)<500 else 'fail',
        'rows':rows}
    a.out.parent.mkdir(parents=True,exist_ok=True)
    with a.out.open('x') as f: json.dump(report,f,indent=2)
    print(json.dumps({k:v for k,v in report.items() if k!='rows'},indent=2))

if __name__=='__main__':main()
