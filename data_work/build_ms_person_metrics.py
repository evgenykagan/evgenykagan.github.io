#!/usr/bin/env python3
import csv
import json
import re
from collections import defaultdict

BASE='/Users/evgenykagan/Library/CloudStorage/Dropbox/Website/evgenykagan.github.io'
MS_CSV=f'{BASE}/data_work/ms_om_paper_review.csv'
RANKINGS=f'{BASE}/_data/rankings_data.json'
OUT=f'{BASE}/data_work/ms_person_metrics.csv'


def norm(s: str) -> str:
    s=(s or '').lower().strip()
    s=re.sub(r'\([^)]*\)',' ',s)
    s=re.sub(r'[^a-z0-9 ]',' ',s)
    s=re.sub(r'\s+',' ',s).strip()
    return s


def school_aliases(school):
    name=school['name']
    full=school.get('full_name','')
    out=set([norm(name), norm(full)])
    # stripped versions
    out.add(norm(re.sub(r'\([^)]*\)','',name)))
    out.add(norm(re.sub(r'\([^)]*\)','',full)))
    # common business-school strings
    if 'MIT'==name:
        out.update(map(norm,['mit','massachusetts institute of technology','sloan school of management']))
    if 'Michigan (Ross)'==name:
        out.update(map(norm,['university of michigan','ross school of business']))
    if 'Chicago (Booth)'==name:
        out.update(map(norm,['university of chicago','booth school of business']))
    if 'Wharton (UPenn)'==name:
        out.update(map(norm,['wharton school','university of pennsylvania']))
    if 'UIUC (Gies)'==name:
        out.update(map(norm,['university of illinois','gies college of business']))
    if 'UW (Foster)'==name:
        out.update(map(norm,['university of washington','foster school of business']))
    if 'UCLA (Anderson)'==name:
        out.update(map(norm,['ucla','anderson school of management']))
    if 'USC (Marshall)'==name:
        out.update(map(norm,['university of southern california','marshall school of business']))
    if 'UNC (Kenan-Flagler)'==name:
        out.update(map(norm,['university of north carolina','kenan flagler']))
    if 'UT Dallas'==name:
        out.update(map(norm,['university of texas at dallas','jindal school of management']))
    if 'Virginia (Darden)'==name:
        out.update(map(norm,['university of virginia','darden school of business']))
    if 'UBC (Sauder)'==name:
        out.update(map(norm,['university of british columbia','sauder school of business']))
    return [x for x in out if x]


def map_aff_to_school(aff_text, alias_map):
    t=norm(aff_text)
    best=None
    best_len=0
    for school, aliases in alias_map.items():
        for a in aliases:
            if a and a in t and len(a)>best_len:
                best=school
                best_len=len(a)
    return best or 'Unknown'


def parse_author_affils(raw):
    try:
        obj=json.loads(raw) if raw else []
        if isinstance(obj,list):
            return obj
    except Exception:
        pass
    return []


def main():
    rankings=json.load(open(RANKINGS,encoding='utf-8'))
    alias_map={s['name']:school_aliases(s) for s in rankings['schools']}

    person=defaultdict(lambda: {
        'papers_full':0.0,
        'papers_frac_authors':0.0,
        'papers_frac_authors_year_norm':0.0,
        'citation_sum':0.0,
        'citation_frac_authors':0.0,
        'first_year':9999,
        'last_year':0,
        'paper_keys':set(),
        'unknown_school_hits':0,
    })

    with open(MS_CSV,encoding='utf-8',newline='') as f:
        rows=list(csv.DictReader(f))

    for r in rows:
        if (r.get('discipline_bucket') or '') != 'OM_OR':
            continue

        pkey=r.get('paper_key','').strip()
        if not pkey:
            continue
        try:
            year=int(r.get('year') or 0)
        except Exception:
            year=0
        try:
            cites=float(r.get('citation_count') or 0)
        except Exception:
            cites=0.0

        affs=parse_author_affils(r.get('author_affiliations_at_publication',''))
        if not affs:
            continue

        # unique authors on paper
        authors=[]
        seen=set()
        for a in affs:
            nm=(a.get('author') or '').strip()
            if not nm or nm in seen:
                continue
            seen.add(nm)
            authors.append(a)
        n_auth=max(1,len(authors))

        year_norm=1.0
        if year and 2016 <= year <= 2026:
            year_norm=1.0

        for a in authors:
            nm=(a.get('author') or '').strip()
            aff_list=a.get('affiliations') or []
            aff_text=' ; '.join(x for x in aff_list if isinstance(x,str))
            school=map_aff_to_school(aff_text, alias_map)
            key=(nm,school)
            rec=person[key]

            # de-dup at person-paper level
            if pkey in rec['paper_keys']:
                continue
            rec['paper_keys'].add(pkey)

            rec['papers_full'] += 1.0
            rec['papers_frac_authors'] += 1.0 / n_auth
            rec['papers_frac_authors_year_norm'] += (1.0 / n_auth) / year_norm
            rec['citation_sum'] += cites
            rec['citation_frac_authors'] += cites / n_auth
            if year:
                rec['first_year']=min(rec['first_year'],year)
                rec['last_year']=max(rec['last_year'],year)
            if school=='Unknown':
                rec['unknown_school_hits'] += 1

    out_rows=[]
    for (name,school),v in person.items():
        out_rows.append({
            'person_name':name,
            'school':school,
            'paper_count_full':f"{v['papers_full']:.0f}",
            'paper_count_frac_authors':f"{v['papers_frac_authors']:.4f}",
            'citation_sum_openalex':f"{v['citation_sum']:.1f}",
            'citation_frac_authors_openalex':f"{v['citation_frac_authors']:.4f}",
            'first_year': '' if v['first_year']==9999 else str(v['first_year']),
            'last_year': '' if v['last_year']==0 else str(v['last_year']),
            'unique_om_or_papers': str(len(v['paper_keys'])),
            'unknown_school_hits': str(v['unknown_school_hits']),
        })

    out_rows.sort(key=lambda r: (
        -float(r['paper_count_frac_authors']),
        -float(r['citation_frac_authors_openalex']),
        r['person_name']
    ))

    fields=[
        'person_name','school','paper_count_full','paper_count_frac_authors',
        'citation_sum_openalex','citation_frac_authors_openalex',
        'first_year','last_year','unique_om_or_papers','unknown_school_hits'
    ]

    with open(OUT,'w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader()
        w.writerows(out_rows)

    print('rows_in',len(rows))
    print('person_school_rows',len(out_rows))
    print('out',OUT)

if __name__=='__main__':
    main()
