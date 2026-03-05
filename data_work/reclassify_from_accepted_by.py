#!/usr/bin/env python3
import csv, json, re, urllib.parse, urllib.request, time
CSV='/Users/evgenykagan/Library/CloudStorage/Dropbox/Website/evgenykagan.github.io/data_work/ms_om_paper_review.csv'

MAP={
 'finance':'Finance',
 'accounting':'Accounting',
 'information systems':'Information Systems',
 'is':'Information Systems',
 'marketing':'Marketing',
 'strategy':'Strategy/Organizations',
 'organizations':'Strategy/Organizations',
 'economics':'Economics/Policy',
 'operations management':'Operations Management',
 'operations':'Operations Management',
 'stochastic models':'Optimization/Stochastic Models',
 'optimization':'Optimization/Stochastic Models',
 'healthcare':'Healthcare Operations',
 'revenue management':'Revenue Management',
 'operations research':'Optimization/Stochastic Models'
}
OM={'Operations Management','Optimization/Stochastic Models','Healthcare Operations','Revenue Management'}


def fetch_abstract(doi):
    url='https://api.crossref.org/works/'+urllib.parse.quote(doi,safe='')
    req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'})
    with urllib.request.urlopen(req,timeout=20) as r:
        j=json.loads(r.read().decode('utf-8','ignore'))
    a=j.get('message',{}).get('abstract','') or ''
    if not a:
        return ''
    t=re.sub(r'<[^>]+>',' ',a)
    t=re.sub(r'\s+',' ',t).strip()
    return t


def parse_dept(txt):
    t=txt.lower()
    # pattern: "This paper was accepted by Lukas Schmid, finance."
    m=re.search(r'accepted by[^.]{0,200}?[,;:]\s*([^.;]{2,80})',t,re.I)
    if not m:
        return None,None
    raw=m.group(1).strip(' .;:')
    raw=raw.replace('&',' and ')
    # use last phrase chunk if includes names/extra words
    parts=[p.strip() for p in re.split(r'[,/]| and ',raw) if p.strip()]
    cand=parts[-1] if parts else raw
    cand=cand.strip()
    for k,v in MAP.items():
        if k in cand:
            return v,raw
    # if exact last word matches
    last=cand.split()[-1] if cand.split() else cand
    for k,v in MAP.items():
        if k==last:
            return v,raw
    return None,raw


def main(limit=600):
    rows=list(csv.DictReader(open(CSV,encoding='utf-8')))
    fields=list(rows[0].keys())
    if 'abstract_excerpt' not in fields: fields.append('abstract_excerpt')
    changed=0; tried=0; hits=0
    for r in rows:
        doi=(r.get('doi') or '').strip()
        if not doi: continue
        if tried>=limit: break
        # prioritize unclear or low confidence
        conf=float(r.get('dept_confidence') or 0)
        if r.get('ms_department')!='General/Unclear' and conf>=0.7:
            continue
        tried+=1
        try:
            ab=fetch_abstract(doi)
        except Exception:
            continue
        if not ab:
            continue
        dept,raw=parse_dept(ab)
        if dept:
            hits+=1
            prev=r.get('ms_department','')
            if prev!=dept:
                changed+=1
            r['ms_department']=dept
            r['dept_confidence']='0.95'
            r['dept_signals']=(r.get('dept_signals','')+' | accepted_by:'+raw).strip(' |')
            r['discipline_bucket']='OM_OR' if dept in OM else 'Other'
            r['abstract_excerpt']=ab[:600]
        time.sleep(0.05)

    with open(CSV,'w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader(); w.writerows(rows)
    print('tried',tried,'accepted_by_hits',hits,'changed',changed)

if __name__=='__main__':
    import sys
    lim=int(sys.argv[1]) if len(sys.argv)>1 else 600
    main(lim)
