#!/usr/bin/env python3
import csv
import json
import re
import time
import urllib.parse
import urllib.request

CSV_PATH='/Users/evgenykagan/Library/CloudStorage/Dropbox/Website/evgenykagan.github.io/data_work/ms_om_paper_review.csv'

OM_DEPTS={'Operations Management','Optimization/Stochastic Models','Healthcare Operations','Revenue Management'}

RULES={
 'Operations Management':[
   (r'\boperations?\b',3),(r'\boperations\s+management\b',5),(r'\bsupply\s+chain\b',4),(r'\binventory\b',4),
   (r'\blogistics\b',4),(r'\bscheduling\b',3),(r'\bqueue\w*\b',3),(r'\bservice\s+operations\b',5),
   (r'\bprocurement\b',3),(r'\brevenue\s+management\b',4),(r'\bcapacity\b',2)
 ],
 'Optimization/Stochastic Models':[
   (r'\boptimization\b',5),(r'\boptimal\b',4),(r'\bstochastic\b',5),(r'\bdynamic\s+program\w*\b',5),
   (r'\bmarkov\b',4),(r'\bnetwork\s+flow\b',4),(r'\brobust\s+optimization\b',5),(r'\bapproximation\b',3),
   (r'\binteger\s+program\w*\b',4),(r'\bsubmodular\b',3),(r'\boperations\s+research\b',5)
 ],
 'Healthcare Operations':[
   (r'\bhealthcare\b',5),(r'\bhospital\b',5),(r'\bmedical\b',4),(r'\bclinical\b',4),(r'\bpatient\b',3),
   (r'\breadmission\w*\b',5)
 ],
 'Revenue Management':[
   (r'\brevenue\s+management\b',6),(r'\bdynamic\s+pricing\b',4),(r'\bdemand\s+management\b',4)
 ],
 'Finance':[(r'\basset\s+pricing\b',6),(r'\bportfolio\b',5),(r'\bequity\b',4),(r'\bderivative\w*\b',4),(r'\bloan\b',4),(r'\bbank\b',4),(r'\bcredit\b',4),(r'\bbond\b',4)],
 'Accounting':[(r'\baccounting\b',6),(r'\baudit\w*\b',5),(r'\btax\b',5),(r'\bearnings\b',4),(r'\baccrual\b',5)],
 'Information Systems':[(r'\binformation\s+systems?\b',6),(r'\balgorithm\b',4),(r'\bmachine\s+learning\b',5),(r'\bartificial\s+intelligence\b',5),(r'\bdigital\b',4),(r'\bplatform\b',3)],
 'Marketing':[(r'\bmarketing\b',6),(r'\bbrand\b',5),(r'\badvertis\w*\b',5),(r'\bconsumer\b',4),(r'\bcustomers?\b',3)],
 'Strategy/Organizations':[(r'\bstrategy\b',5),(r'\borganiz\w*\b',4),(r'\bleadership\b',4),(r'\bgovernance\b',4)],
 'Economics/Policy':[(r'\beconom\w*\b',4),(r'\bpolicy\b',3),(r'\bregulation\b',3)]
}


def fetch_crossref_abstract(doi):
    url='https://api.crossref.org/works/'+urllib.parse.quote(doi, safe='')
    req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'})
    with urllib.request.urlopen(req,timeout=30) as r:
        j=json.loads(r.read().decode('utf-8','ignore'))
    abs_html=j.get('message',{}).get('abstract','') or ''
    if not abs_html:
        return ''
    txt=re.sub(r'<[^>]+>',' ',abs_html)
    txt=re.sub(r'\s+',' ',txt).strip()
    return txt


def classify(text):
    scores={}
    signals=[]
    for dept,pats in RULES.items():
        s=0
        for pat,w in pats:
            if re.search(pat,text,re.I):
                s+=w
                signals.append(f'{dept}:{pat}:{w}')
        if s:
            scores[dept]=s
    if not scores:
        return 'General/Unclear',0.0,''
    ranked=sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    best,bv=ranked[0]
    sv=ranked[1][1] if len(ranked)>1 else 0
    conf=bv/(bv+sv+1e-6)
    return best,conf,' | '.join(signals[:12])


def main(limit=500,sleep_s=0.08):
    with open(CSV_PATH,'r',encoding='utf-8',newline='') as f:
        rows=list(csv.DictReader(f))
    fields=list(rows[0].keys())
    if 'abstract_excerpt' not in fields:
        fields.append('abstract_excerpt')

    target=[r for r in rows if r.get('ms_department')=='General/Unclear' and (r.get('doi') or '').strip()]
    done=0
    changed=0
    for r in target:
        if done>=limit:
            break
        doi=r['doi'].strip()
        try:
            abs_txt=fetch_crossref_abstract(doi)
        except Exception:
            done+=1
            continue
        merged=(r.get('title','')+' '+abs_txt).lower()
        dept,conf,sig=classify(merged)
        if abs_txt:
            r['abstract_excerpt']=abs_txt[:600]
        if dept!='General/Unclear':
            if r.get('ms_department')!=dept:
                changed+=1
            r['ms_department']=dept
            r['dept_confidence']=f'{conf:.2f}'
            base=(r.get('dept_signals') or '')
            r['dept_signals']=(base+' | abs_reclass:'+sig).strip(' |')
            if r.get('keep_om') in ('1','0'):
                pass
            else:
                r['discipline_bucket']='OM_OR' if dept in OM_DEPTS else 'Other'
        done+=1
        if done%100==0:
            print('processed',done,'changed',changed)
        time.sleep(sleep_s)

    with open(CSV_PATH,'w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader(); w.writerows(rows)
    print('done processed',done,'changed',changed)

if __name__=='__main__':
    import sys
    lim=int(sys.argv[1]) if len(sys.argv)>1 else 500
    main(lim)
