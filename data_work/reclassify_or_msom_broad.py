#!/usr/bin/env python3
import csv,re
from collections import Counter


def classify(title, abstract, dept_signal, journal):
    t=((title or '')+' '+(abstract or '')+' '+(dept_signal or '')).lower()

    # strong non-OM/OR signals
    non_om=[
        r'accepted by[^.]{0,120}(finance|accounting|marketing|information systems|behavioral economics)',
        r'\basset pricing\b', r'\bportfolio\b', r'\bderivative\w*\b',
        r'\baccounting\b', r'\baccrual\b',
        r'\bconsumer\s+behavior\b', r'\bbrand\b', r'\binformation systems\b'
    ]

    # broad OM/OR signals (intentionally inclusive)
    om_or=[
        r'\boperations?\b', r'\boperations management\b', r'\boperations research\b',
        r'\boptimization\b', r'\boptimal\b', r'\bstochastic\b', r'\bdynamic program\w*\b',
        r'\bmarkov\b', r'\bnetwork\b', r'\binteger program\w*\b', r'\bsubmodular\b',
        r'\bqueue\w*\b', r'\bscheduling\b', r'\binventory\b', r'\blogistics\b',
        r'\bsupply chain\b', r'\brevenue management\b', r'\bpricing\b', r'\bauction\b',
        r'\bhealthcare\b', r'\bhospital\b', r'\bmedical\b', r'\bclinical\b', r'\bpatient\b',
        r'\bservice operations\b', r'\bmanufacturing\b', r'\bcapacity\b', r'\bmatching\b'
    ]

    non_hit=any(re.search(p,t,re.I) for p in non_om)
    om_hit=any(re.search(p,t,re.I) for p in om_or)

    if journal.lower().startswith('manufacturing') or journal.lower().startswith('msom'):
        # User rule: treat MSOM papers as OM/OR under a broad definition.
        return 'OM_OR','1','auto_broad_msom'

    # OR journal: broad include, but allow strong non-OM exclusion
    if om_hit:
        return 'OM_OR','1','auto_broad_or'
    if non_hit:
        return 'Other','0','auto_non_om_strong'
    # default broad include for OR unless explicit non-OM
    return 'OM_OR','1','auto_default_or'


def remap_dept(bucket, old_dept):
    if bucket=='OM_OR':
        if old_dept and old_dept!='General/Unclear':
            return old_dept
        return 'Operations/OR'
    return old_dept if old_dept else 'Other Discipline'


def run(csv_path):
    rows=list(csv.DictReader(open(csv_path,encoding='utf-8')))
    changed=0
    for r in rows:
        bucket,keep,tag=classify(r.get('title',''), r.get('abstract_excerpt',''), r.get('dept_signals',''), r.get('journal',''))
        if r.get('discipline_bucket')!=bucket:
            changed+=1
        r['discipline_bucket']=bucket
        r['keep_om']=keep
        r['exclude_reason']='' if bucket=='OM_OR' else 'non_om_signal'
        r['ms_department']=remap_dept(bucket, r.get('ms_department',''))
        sig=(r.get('dept_signals') or '')
        if tag not in sig:
            r['dept_signals']=(sig+' | '+tag).strip(' |')

    with open(csv_path,'w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=rows[0].keys())
        w.writeheader(); w.writerows(rows)

    c=Counter(r['discipline_bucket'] for r in rows)
    total=len(rows)
    om=c.get('OM_OR',0)
    print('file',csv_path)
    print('total',total,'om_or',om,'other',c.get('Other',0),'om_or_pct',round(100.0*om/total,2))
    print('changed',changed)


if __name__=='__main__':
    import sys
    if len(sys.argv)<2:
        raise SystemExit('Usage: reclassify_or_msom_broad.py <csv1> [csv2 ...]')
    for p in sys.argv[1:]:
        run(p)
