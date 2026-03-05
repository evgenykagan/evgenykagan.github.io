#!/usr/bin/env python3
import csv, json, re
from collections import defaultdict

BASE='/Users/evgenykagan/Library/CloudStorage/Dropbox/Website/evgenykagan.github.io'
MS=f'{BASE}/data_work/ms_om_paper_review.csv'
RANK=f'{BASE}/_data/rankings_data.json'
OUT=f'{BASE}/data_work/ms_person_metrics_rankings_only.csv'


def norm(s):
    s=(s or '').lower().strip()
    s=re.sub(r'\([^)]*\)',' ',s)
    s=re.sub(r'[^a-z0-9 ]',' ',s)
    s=re.sub(r'\s+',' ',s).strip()
    return s


def university_key(full_name):
    # Keep university identity, drop school/college suffixes and parenthetical school names.
    x=re.sub(r'\([^)]*\)','',full_name or '').strip()
    x=re.sub(r'\s*[-–]\s*.*$','',x).strip()
    return x


def aliases_for_school(s):
    name=s['name']; full=s.get('full_name','')
    uni=university_key(full)
    vals={norm(name), norm(full), norm(uni), norm(re.sub(r'\([^)]*\)','',name)), norm(re.sub(r'\([^)]*\)','',full))}
    # Robust university-level aliases
    vals.add(norm(uni.replace('University of ','').replace('The ','')))
    if 'UIUC' in name or 'Illinois' in full:
        vals.update(map(norm,['university of illinois','university of illinois urbana champaign']))
    if 'Wharton' in name:
        vals.update(map(norm,['university of pennsylvania','upenn','wharton']))
    if 'Michigan (Ross)'==name:
        vals.update(map(norm,['university of michigan','michigan']))
    if 'Chicago (Booth)'==name:
        vals.update(map(norm,['university of chicago','chicago']))
    if 'UCLA' in name:
        vals.update(map(norm,['university of california los angeles','ucla']))
    if 'UC Berkeley' in name:
        vals.update(map(norm,['university of california berkeley','uc berkeley']))
    if 'UC San Diego' in name:
        vals.update(map(norm,['university of california san diego','ucsd']))
    if 'UT Dallas'==name:
        vals.update(map(norm,['university of texas at dallas','ut dallas']))
    if 'UBC' in name:
        vals.update(map(norm,['university of british columbia','ubc']))
    if 'UW (Foster)'==name:
        vals.update(map(norm,['university of washington','washington']))
    return [v for v in vals if v]


def parse_aff(raw):
    try:
        v=json.loads(raw) if raw else []
        return v if isinstance(v,list) else []
    except Exception:
        return []


def map_school(aff_text, alias_map):
    t=norm(aff_text)
    best=None; bl=0
    for school, aliases in alias_map.items():
        for a in aliases:
            if a and a in t and len(a)>bl:
                best=school; bl=len(a)
    return best


def main():
    schools=json.load(open(RANK,encoding='utf-8'))['schools']
    alias_map={s['name']:aliases_for_school(s) for s in schools}
    uni_map={s['name']:university_key(s.get('full_name','')) for s in schools}

    out=defaultdict(lambda: {'full':0.0,'frac':0.0,'cites':0.0,'cites_frac':0.0,'first':9999,'last':0,'keys':set()})

    rows=list(csv.DictReader(open(MS,encoding='utf-8')))
    for r in rows:
        if r.get('discipline_bucket')!='OM_OR':
            continue
        pkey=(r.get('paper_key') or '').strip()
        if not pkey:
            continue
        try: y=int(r.get('year') or 0)
        except: y=0
        try: c=float(r.get('citation_count') or 0)
        except: c=0.0
        affs=parse_aff(r.get('author_affiliations_at_publication',''))
        seen=set(); auth=[]
        for a in affs:
            nm=(a.get('author') or '').strip()
            if not nm or nm in seen: continue
            seen.add(nm); auth.append(a)
        n=max(1,len(auth))
        for a in auth:
            nm=(a.get('author') or '').strip()
            aff=' ; '.join(x for x in (a.get('affiliations') or []) if isinstance(x,str))
            school=map_school(aff,alias_map)
            if not school:
                continue
            uni=uni_map[school]
            key=(nm,school,uni)
            rec=out[key]
            if pkey in rec['keys']:
                continue
            rec['keys'].add(pkey)
            rec['full']+=1.0
            rec['frac']+=1.0/n
            rec['cites']+=c
            rec['cites_frac']+=c/n
            if y:
                rec['first']=min(rec['first'],y)
                rec['last']=max(rec['last'],y)

    rows_out=[]
    for (nm,school,uni),v in out.items():
        rows_out.append({
            'person_name':nm,
            'rankings_school':school,
            'university_key':uni,
            'paper_count_full':f"{v['full']:.0f}",
            'paper_count_frac_authors':f"{v['frac']:.4f}",
            'citation_sum_openalex':f"{v['cites']:.1f}",
            'citation_frac_authors_openalex':f"{v['cites_frac']:.4f}",
            'first_year':'' if v['first']==9999 else str(v['first']),
            'last_year':'' if v['last']==0 else str(v['last']),
            'unique_om_or_papers':str(len(v['keys']))
        })

    rows_out.sort(key=lambda r:(-float(r['paper_count_frac_authors']),-float(r['citation_frac_authors_openalex']),r['person_name']))
    fields=['person_name','rankings_school','university_key','paper_count_full','paper_count_frac_authors','citation_sum_openalex','citation_frac_authors_openalex','first_year','last_year','unique_om_or_papers']
    with open(OUT,'w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader(); w.writerows(rows_out)

    print('rows_out',len(rows_out))
    print('out',OUT)

if __name__=='__main__':
    main()
