#!/usr/bin/env python3
import csv, json, re
from collections import defaultdict

BASE='/Users/evgenykagan/Library/CloudStorage/Dropbox/Website/evgenykagan.github.io'
MS=f'{BASE}/data_work/ms_om_paper_review.csv'
RANK=f'{BASE}/_data/rankings_data.json'
OUT=f'{BASE}/data_work/ms_school_year_metrics.csv'


def norm(s):
    s=(s or '').lower().strip()
    s=re.sub(r'\([^)]*\)',' ',s)
    s=re.sub(r'[^a-z0-9 ]',' ',s)
    s=re.sub(r'\s+',' ',s).strip()
    return s


def aliases_for_school(s):
    name=s['name']; full=s.get('full_name','')
    vals={norm(name),norm(full),norm(re.sub(r'\([^)]*\)','',name)),norm(re.sub(r'\([^)]*\)','',full))}
    # university-level aliases
    if 'Wharton' in name:
        vals.update(map(norm,['university of pennsylvania','upenn','wharton']))
    if 'Michigan (Ross)'==name:
        vals.update(map(norm,['university of michigan','ross school of business']))
    if 'Chicago (Booth)'==name:
        vals.update(map(norm,['university of chicago','booth school of business']))
    if 'UCLA' in name:
        vals.update(map(norm,['ucla','university of california los angeles']))
    if 'UC Berkeley' in name:
        vals.update(map(norm,['university of california berkeley','uc berkeley']))
    if 'UC San Diego' in name:
        vals.update(map(norm,['university of california san diego','ucsd']))
    if 'UT Dallas'==name:
        vals.update(map(norm,['university of texas at dallas','ut dallas','jindal school']))
    if 'UBC' in name:
        vals.update(map(norm,['university of british columbia','ubc']))
    if 'UIUC' in name:
        vals.update(map(norm,['university of illinois','university of illinois urbana champaign','gies']))
    return [v for v in vals if v]


def map_school(aff_text, alias_map):
    t=norm(aff_text)
    best=None; bl=0
    for school, aliases in alias_map.items():
        for a in aliases:
            if a and a in t and len(a)>bl:
                best=school; bl=len(a)
    return best


def parse_aff(raw):
    try:
        obj=json.loads(raw) if raw else []
        return obj if isinstance(obj,list) else []
    except Exception:
        return []


def main():
    schools=json.load(open(RANK,encoding='utf-8'))['schools']
    school_names=[s['name'] for s in schools]
    alias_map={s['name']:aliases_for_school(s) for s in schools}

    # (school, year) -> metrics
    M=defaultdict(lambda: {
        'std_papers':0.0,
        'personal_papers':0.0,
        'std_citations':0.0,
        'personal_citation_weighted':0.0,
    })

    rows=list(csv.DictReader(open(MS,encoding='utf-8')))
    for r in rows:
        if r.get('discipline_bucket')!='OM_OR':
            continue
        try:
            year=int(r.get('year') or 0)
        except Exception:
            continue
        if not (2016 <= year <= 2026):
            continue
        try:
            cites=float(r.get('citation_count') or 0)
        except Exception:
            cites=0.0

        affs=parse_aff(r.get('author_affiliations_at_publication',''))
        # author-level mapped schools for personal metrics
        author_schools=[]
        seen_auth=set()
        for a in affs:
            nm=(a.get('author') or '').strip()
            if not nm or nm in seen_auth:
                continue
            seen_auth.add(nm)
            aff=' ; '.join(x for x in (a.get('affiliations') or []) if isinstance(x,str))
            s=map_school(aff,alias_map)
            if s:
                author_schools.append(s)

        # standard school list per paper-school from precomputed map if present, else unique author schools
        std_schools=[]
        try:
            arr=json.loads(r.get('school_credit_map') or '[]')
            if isinstance(arr,list):
                std_schools=[x for x in arr if x in school_names]
        except Exception:
            std_schools=[]
        if not std_schools:
            std_schools=sorted(set(author_schools))

        for s in std_schools:
            rec=M[(s,year)]
            rec['std_papers'] += 1.0
            rec['std_citations'] += cites

        for s in author_schools:
            rec=M[(s,year)]
            rec['personal_papers'] += 1.0
            rec['personal_citation_weighted'] += cites

    out=[]
    for s in school_names:
        for y in range(2016,2027):
            rec=M[(s,y)]
            out.append({
                'rankings_school':s,
                'year':str(y),
                'ms_std_papers':f"{rec['std_papers']:.4f}",
                'ms_personal_papers':f"{rec['personal_papers']:.4f}",
                'ms_std_citations':f"{rec['std_citations']:.4f}",
                'ms_personal_citation_weighted':f"{rec['personal_citation_weighted']:.4f}",
            })

    with open(OUT,'w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=['rankings_school','year','ms_std_papers','ms_personal_papers','ms_std_citations','ms_personal_citation_weighted'])
        w.writeheader(); w.writerows(out)

    print('rows_out',len(out))
    print('out',OUT)

if __name__=='__main__':
    main()
