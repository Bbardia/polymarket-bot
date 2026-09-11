#!/usr/bin/env python3
"""Offline immutable five-minute shadow tick. No submission or account API."""
import argparse
import json
import sys
from pathlib import Path
from decimal import Decimal
from datetime import datetime
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from src.v3.ladder_pmf import RungQuote, build_ladder_pmf
from src.v3.maker_shadow import t1_shadow_decision, log_shadow_tick


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',type=Path,required=True)
    p.add_argument('--out-dir',type=Path,required=True)
    a=p.parse_args(); panel=json.loads(a.input.read_text())
    if len(panel)>1000: p.error('1000 snapshot maximum')
    counts={}
    for row in panel:
        now=datetime.fromisoformat(row['decision_at'])
        if now.tzinfo is None: raise ValueError('aware decision timestamp required')
        pmf=build_ladder_pmf([RungQuote(r['label'],Decimal(str(r['bid'])) if r.get('bid') is not None else None,Decimal(str(r['ask'])) if r.get('ask') is not None else None) for r in row['rungs']])
        decision=t1_shadow_decision(row['candidate'],pmf=pmf,phi_evidence=row.get('phi_evidence'),now=now,extremization=row.get('extremization'))
        log_shadow_tick(a.out_dir,candidate=row['candidate'],decision=decision,now=now)
        counts[decision['status']]=counts.get(decision['status'],0)+1
    print(json.dumps(counts))
if __name__=='__main__': main()
