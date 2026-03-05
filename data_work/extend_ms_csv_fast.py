#!/usr/bin/env python3
import csv
import datetime as dt
import json
import re
import urllib.parse
import urllib.request

CSV_PATH = '/Users/evgenykagan/Library/CloudStorage/Dropbox/Website/evgenykagan.github.io/data_work/ms_om_paper_review.csv'
TODAY = dt.date(2026,2,19).isoformat()

FIELDS=[
    'paper_key','year','title','journal','status','published_or_forthcoming','doi','authors',
    'author_affiliations_at_publication','school_credit_map','keep_om','exclude_reason',
    'google_scholar_citations','google_scholar_url','citations_last_checked_date','notes'
]


def fetch_json(url):
    req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'})
    with urllib.request.urlopen(req,timeout=30) as r:
        return json.loads(r.read().decode('utf-8','ignore'))


def get_year(it):
    for k in ('published-print','published-online','issued'):
        p=it.get(k,{}).get('date-parts',[])
        if p and p[0]:
            return int(p[0][0])
    return None


def paper_key(it):
    doi=(it.get('DOI') or '').strip().lower()
    if doi:
        return 'doi:'+doi
    t=(it.get('title') or [''])[0].lower().strip()
    t=re.sub(r'\s+',' ',re.sub(r'[^a-z0-9 ]','',t))
    y=get_year(it) or 0
    return f'title:{t}|year:{y}'


def pub_status(it):
    return 'published' if (it.get('volume') or it.get('issue') or it.get('page')) else 'forthcoming'


def auth(it):
    names=[]; arr=[]
    for a in it.get('author',[]):
        n=((a.get('given') or '')+' '+(a.get('family') or '')).strip()
        if not n: continue
        names.append(n)
        aff=[x.get('name','').strip() for x in a.get('affiliation',[]) if x.get('name')]
        arr.append({'author':n,'affiliations':aff})
    return '; '.join(names), json.dumps(arr,ensure_ascii=True)


def read_rows():
    out={}
    with open(CSV_PATH,'r',encoding='utf-8',newline='') as f:
        for r in csv.DictReader(f):
            out[r['paper_key']]=r
    return out


def write_rows(rows):
    with open(CSV_PATH,'w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=FIELDS)
        w.writeheader(); w.writerows(sorted(rows.values(),key=lambda r:(r.get('year',''),r.get('title',''))))


def it_crossref():
    cursor='*'
    while True:
        u=(
            'https://api.crossref.org/journals/0025-1909/works?'
            'filter=from-pub-date:2016-01-01,until-pub-date:2026-12-31,type:journal-article'
            f'&rows=250&cursor={urllib.parse.quote(cursor,safe="")}'
            '&select=DOI,title,container-title,published-print,published-online,issued,author,volume,issue,page'
        )
        data=fetch_json(u)
        msg=data.get('message',{})
        items=msg.get('items',[])
        if not items: break
        for it in items:
            ct=[x.lower() for x in it.get('container-title',[])]
            if 'management science' not in ct: continue
            y=get_year(it)
            if not y or y<2016 or y>2026: continue
            yield it
        nc=msg.get('next-cursor')
        if not nc or nc==cursor: break
        cursor=nc


def main():
    rows=read_rows()
    before=len(rows)
    for it in it_crossref():
        k=paper_key(it)
        if k in rows: continue
        doi=(it.get('DOI') or '').strip()
        title=(it.get('title') or [''])[0].strip()
        year=get_year(it) or ''
        an,af=auth(it)
        q=urllib.parse.quote_plus(doi if doi else title)
        rows[k]={
            'paper_key':k,'year':str(year),'title':title,'journal':'Management Science','status':'',
            'published_or_forthcoming':pub_status(it),'doi':doi,'authors':an,
            'author_affiliations_at_publication':af,'school_credit_map':'','keep_om':'','exclude_reason':'',
            'google_scholar_citations':'','google_scholar_url':f'https://scholar.google.com/scholar?q={q}',
            'citations_last_checked_date':'','notes':'source=crossref;needs_citation_lookup'
        }
    write_rows(rows)
    print('before',before,'after',len(rows),'added',len(rows)-before)

if __name__=='__main__':
    main()
