#!/usr/bin/env python3
import csv
import json
import re
import urllib.parse
import urllib.request
from collections import defaultdict

CSV_FIELDS = [
    'paper_key','year','title','journal','status','published_or_forthcoming','doi','authors',
    'author_affiliations_at_publication','school_credit_map','keep_om','exclude_reason',
    'citation_count','citation_source','citation_url','citation_last_checked_date',
    'google_scholar_citations','google_scholar_url','citations_last_checked_date',
    'notes','ms_department','discipline_bucket','dept_confidence','dept_signals','abstract_excerpt'
]


def fetch_json(url, timeout=60):
    req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0 (mailto:evgeny@example.com)','Accept':'application/json'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8','ignore'))


def get_year(item):
    for k in ('published-print','published-online','issued'):
        p=item.get(k,{}).get('date-parts',[])
        if p and p[0]:
            return int(p[0][0])
    return None


def pkey(item):
    doi=(item.get('DOI') or '').strip().lower()
    if doi:
        return 'doi:'+doi
    t=(item.get('title') or [''])[0].lower().strip()
    t=re.sub(r'\s+',' ',re.sub(r'[^a-z0-9 ]','',t))
    y=get_year(item) or 0
    return f'title:{t}|year:{y}'


def pub_status(item):
    return 'published' if (item.get('volume') or item.get('issue') or item.get('page')) else 'forthcoming'


def parse_authors(item):
    names=[]
    arr=[]
    for a in item.get('author',[]):
        nm=((a.get('given') or '')+' '+(a.get('family') or '')).strip()
        if not nm:
            continue
        names.append(nm)
        aff=[x.get('name','').strip() for x in a.get('affiliation',[]) if x.get('name')]
        arr.append({'author':nm,'affiliations':aff})
    return '; '.join(names), json.dumps(arr, ensure_ascii=True)


def parse_abstract(item):
    abs_html=item.get('abstract') or ''
    if not abs_html:
        return ''
    t=re.sub(r'<[^>]+>',' ',abs_html)
    t=re.sub(r'\s+',' ',t).strip()
    return t[:700]


def infer_department(text, journal_name):
    t=(text or '').lower()
    if 'manufacturing service operations management' in journal_name.lower() or journal_name.lower()=='msom':
        return 'Operations Management', 'OM_OR', '0.99', 'journal=MSOM'
    if 'operations research' in journal_name.lower():
        if any(x in t for x in ['healthcare','hospital','medical','clinical','patient','readmission']):
            return 'Healthcare Operations', 'OM_OR', '0.90', 'kw=healthcare'
        if any(x in t for x in ['stochastic','optimization','markov','dynamic program','integer program','network flow']):
            return 'Optimization/Stochastic Models', 'OM_OR', '0.92', 'kw=or_math'
        return 'Operations Research', 'OM_OR', '0.88', 'journal=OR'
    return 'General/Unclear', 'OM_OR', '0.60', 'fallback'


def iter_year(issn, year):
    off=0
    while True:
        flt=f'issn:{issn},from-pub-date:{year}-01-01,until-pub-date:{year}-12-31,type:journal-article'
        q={'filter':flt,'rows':1000,'offset':off,'select':'DOI,title,container-title,published-print,published-online,issued,author,volume,issue,page,abstract'}
        u='https://api.crossref.org/works?'+urllib.parse.urlencode(q)
        msg=fetch_json(u).get('message',{})
        items=msg.get('items',[])
        if not items:
            break
        for it in items:
            yield it
        off += len(items)
        if len(items) < 1000:
            break


def build(journal_name, issn, out_path):
    out={}
    added_by_year=defaultdict(int)
    for y in range(2016, 2027):
        for it in iter_year(issn, y):
            ct=[x.lower() for x in it.get('container-title',[])]
            # keep only target journal title hits
            if journal_name.lower() == 'operations research':
                if 'operations research' not in ct:
                    continue
            elif journal_name.lower() in ('msom','manufacturing service operations management'):
                ok=any('manufacturing' in c and 'service' in c and 'operations management' in c for c in ct)
                if not ok:
                    continue
            yr=get_year(it)
            if not yr or yr < 2016 or yr > 2026:
                continue
            k=pkey(it)
            if k in out:
                continue
            doi=(it.get('DOI') or '').strip()
            title=(it.get('title') or [''])[0].strip()
            authors,aff=parse_authors(it)
            abs_txt=parse_abstract(it)
            dept,bucket,conf,sig=infer_department((title+' '+abs_txt), journal_name)
            out[k]={
                'paper_key':k,
                'year':str(yr),
                'title':title,
                'journal':'Operations Research' if journal_name.lower()=='operations research' else 'Manufacturing & Service Operations Management',
                'status':'',
                'published_or_forthcoming':pub_status(it),
                'doi':doi,
                'authors':authors,
                'author_affiliations_at_publication':aff,
                'school_credit_map':'',
                'keep_om':'1',
                'exclude_reason':'',
                'citation_count':'',
                'citation_source':'',
                'citation_url':'',
                'citation_last_checked_date':'',
                'google_scholar_citations':'',
                'google_scholar_url':'',
                'citations_last_checked_date':'',
                'notes':'source=crossref',
                'ms_department':dept,
                'discipline_bucket':bucket,
                'dept_confidence':conf,
                'dept_signals':sig,
                'abstract_excerpt':abs_txt
            }
            added_by_year[yr]+=1

    rows=sorted(out.values(), key=lambda r:(int(r['year']), r['title']))
    with open(out_path,'w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=CSV_FIELDS)
        w.writeheader(); w.writerows(rows)

    print('journal',journal_name)
    print('rows',len(rows))
    for y in sorted(added_by_year):
        print('year',y,'rows',added_by_year[y])
    print('out',out_path)


if __name__=='__main__':
    import sys
    if len(sys.argv) != 4:
        raise SystemExit('Usage: build_journal_review_csv.py <journal_name> <issn> <out_csv>')
    build(sys.argv[1], sys.argv[2], sys.argv[3])
