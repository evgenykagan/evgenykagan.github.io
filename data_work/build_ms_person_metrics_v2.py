#!/usr/bin/env python3
import csv
import json
import re
from collections import defaultdict

BASE='/Users/evgenykagan/Library/CloudStorage/Dropbox/Website/evgenykagan.github.io'
MS_CSV=f'{BASE}/data_work/ms_om_paper_review.csv'
RANKINGS=f'{BASE}/_data/rankings_data.json'
OUT=f'{BASE}/data_work/ms_person_metrics_v2.csv'


def norm(s: str) -> str:
    s=(s or '').lower().strip()
    s=re.sub(r'\([^)]*\)',' ',s)
    s=re.sub(r'[^a-z0-9 ]',' ',s)
    s=re.sub(r'\s+',' ',s).strip()
    return s


def title_case_clean(s: str) -> str:
    s=re.sub(r'\s+',' ',(s or '').strip(' ,;'))
    return s


def school_aliases(school):
    name=school['name']
    full=school.get('full_name','')
    out=set([norm(name), norm(full)])
    out.add(norm(re.sub(r'\([^)]*\)','',name)))
    out.add(norm(re.sub(r'\([^)]*\)','',full)))
    if 'MIT'==name:
        out.update(map(norm,['mit','massachusetts institute of technology','sloan school of management']))
    if 'Michigan (Ross)'==name:
        out.update(map(norm,['university of michigan','ross school of business']))
    if 'Chicago (Booth)'==name:
        out.update(map(norm,['university of chicago','booth school of business']))
    if 'Wharton (UPenn)'==name:
        out.update(map(norm,['wharton school','university of pennsylvania']))
    if 'UIUC (Gies)'==name:
        out.update(map(norm,['university of illinois','gies college of business','university of illinois urbana champaign']))
    if 'UW (Foster)'==name:
        out.update(map(norm,['university of washington','foster school of business']))
    if 'UCLA (Anderson)'==name:
        out.update(map(norm,['ucla','anderson school of management','university of california los angeles']))
    if 'USC (Marshall)'==name:
        out.update(map(norm,['university of southern california','marshall school of business']))
    if 'UNC (Kenan-Flagler)'==name:
        out.update(map(norm,['university of north carolina','kenan flagler']))
    if 'UT Dallas'==name:
        out.update(map(norm,['university of texas at dallas','jindal school']))
    if 'Virginia (Darden)'==name:
        out.update(map(norm,['university of virginia','darden school of business']))
    if 'UBC (Sauder)'==name:
        out.update(map(norm,['university of british columbia','sauder school of business']))
    if 'Boston U (Questrom)'==name:
        out.update(map(norm,['boston university','questrom school of business']))
    if 'Carnegie Mellon (Tepper)'==name:
        out.update(map(norm,['carnegie mellon university','tepper school of business']))
    return [x for x in out if x]


def parse_author_affils(raw):
    try:
        obj=json.loads(raw) if raw else []
        if isinstance(obj,list):
            return obj
    except Exception:
        pass
    return []


def extract_institution(aff_text: str) -> str:
    # Attempt to recover institution from affiliation string, not limited to rankings list.
    if not aff_text:
        return 'Unknown'
    s=aff_text.strip(' ;')
    parts=[p.strip() for p in re.split(r';',s) if p.strip()]
    # Use first affiliation entry if multiple.
    s=parts[0] if parts else s
    segs=[x.strip() for x in s.split(',') if x.strip()]
    if not segs:
        return 'Unknown'

    edu_kw=('university','college','school','institute','business','polytechnic','insead','hbs','mit','center','centre')
    corp_kw=('google','amazon','microsoft','meta','uber','airbnb','tencent','alibaba')
    generic_unit_kw=('department','faculty','division','area','institute for','center for','centre for')

    # Pick best segment with strong institution keywords, preferring university/business-school entities.
    cand=None
    cand_score=-1
    for seg in segs:
        l=seg.lower()
        score=0
        if any(k in l for k in ('university','business school','school of management','college of business','school of business','mit','insead')):
            score += 4
        if any(k in l for k in edu_kw):
            score += 2
        if any(k in l for k in corp_kw):
            score += 2
        if any(k in l for k in generic_unit_kw):
            score -= 2
        if score > cand_score:
            cand_score=score
            cand=seg
    if cand is None:
        cand=segs[0]

    # If first segment is department-like, combine with next segment.
    dept_like=any(x in cand.lower() for x in ['department','faculty','area','division','institute for','center for','centre for'])
    if dept_like and len(segs)>=2:
        # Prefer the first follow-up segment that looks like an institution
        nxt=None
        for s in segs[1:]:
            sl=s.lower()
            if any(k in sl for k in ('university','school','college','business','mit','insead')):
                nxt=s
                break
        if nxt:
            cand=nxt
        else:
            cand=f"{cand}, {segs[1]}"

    # Normalize common wrappers
    cand=re.sub(r'\b(the|and)\b',' ',cand,flags=re.I)
    cand=title_case_clean(cand)
    return cand if cand else 'Unknown'


def map_to_rankings_school(aff_text, alias_map):
    t=norm(aff_text)
    best=None
    best_len=0
    for school, aliases in alias_map.items():
        for a in aliases:
            if a and a in t and len(a)>best_len:
                best=school
                best_len=len(a)
    return best or 'Unmapped'


def main():
    rankings=json.load(open(RANKINGS,encoding='utf-8'))
    alias_map={s['name']:school_aliases(s) for s in rankings['schools']}

    person=defaultdict(lambda: {
        'papers_full':0.0,
        'papers_frac_authors':0.0,
        'citation_sum':0.0,
        'citation_frac_authors':0.0,
        'first_year':9999,
        'last_year':0,
        'paper_keys':set(),
    })

    with open(MS_CSV,encoding='utf-8',newline='') as f:
        rows=list(csv.DictReader(f))

    for r in rows:
        if (r.get('discipline_bucket') or '') != 'OM_OR':
            continue
        pkey=(r.get('paper_key') or '').strip()
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

        seen=set(); authors=[]
        for a in affs:
            nm=(a.get('author') or '').strip()
            if not nm or nm in seen:
                continue
            seen.add(nm)
            authors.append(a)
        n_auth=max(1,len(authors))

        for a in authors:
            nm=(a.get('author') or '').strip()
            aff_list=a.get('affiliations') or []
            aff_text=' ; '.join(x for x in aff_list if isinstance(x,str))
            inst=extract_institution(aff_text)
            mapped=map_to_rankings_school(aff_text, alias_map)
            key=(nm,inst,mapped)
            rec=person[key]
            if pkey in rec['paper_keys']:
                continue
            rec['paper_keys'].add(pkey)
            rec['papers_full'] += 1.0
            rec['papers_frac_authors'] += 1.0 / n_auth
            rec['citation_sum'] += cites
            rec['citation_frac_authors'] += cites / n_auth
            if year:
                rec['first_year']=min(rec['first_year'],year)
                rec['last_year']=max(rec['last_year'],year)

    out=[]
    for (name,inst,mapped),v in person.items():
        out.append({
            'person_name':name,
            'affiliation_institution':inst,
            'rankings_school_match':mapped,
            'paper_count_full':f"{v['papers_full']:.0f}",
            'paper_count_frac_authors':f"{v['papers_frac_authors']:.4f}",
            'citation_sum_openalex':f"{v['citation_sum']:.1f}",
            'citation_frac_authors_openalex':f"{v['citation_frac_authors']:.4f}",
            'first_year':'' if v['first_year']==9999 else str(v['first_year']),
            'last_year':'' if v['last_year']==0 else str(v['last_year']),
            'unique_om_or_papers':str(len(v['paper_keys']))
        })

    out.sort(key=lambda r:(-float(r['paper_count_frac_authors']),-float(r['citation_frac_authors_openalex']),r['person_name']))

    fields=['person_name','affiliation_institution','rankings_school_match','paper_count_full','paper_count_frac_authors','citation_sum_openalex','citation_frac_authors_openalex','first_year','last_year','unique_om_or_papers']
    with open(OUT,'w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader(); w.writerows(out)

    unmapped=sum(1 for r in out if r['rankings_school_match']=='Unmapped')
    print('rows_out',len(out))
    print('unmapped_rows',unmapped)
    print('mapped_rows',len(out)-unmapped)
    print('out',OUT)

if __name__=='__main__':
    main()
