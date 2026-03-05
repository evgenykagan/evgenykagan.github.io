#!/usr/bin/env python3
import csv, json, re, urllib.parse, urllib.request

CSV_PATH='/Users/evgenykagan/Library/CloudStorage/Dropbox/Website/evgenykagan.github.io/data_work/ms_om_paper_review.csv'
FIELDS=['paper_key','year','title','journal','status','published_or_forthcoming','doi','authors','author_affiliations_at_publication','school_credit_map','keep_om','exclude_reason','google_scholar_citations','google_scholar_url','citations_last_checked_date','notes','ms_department','discipline_bucket']


def jget(url):
    req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'})
    with urllib.request.urlopen(req,timeout=60) as r:
        return json.loads(r.read().decode('utf-8','ignore'))


def pkey(it):
    doi=(it.get('DOI') or '').strip().lower()
    if doi: return 'doi:'+doi
    t=(it.get('title') or [''])[0].lower().strip()
    t=re.sub(r'\s+',' ',re.sub(r'[^a-z0-9 ]','',t))
    y=year(it) or 0
    return f'title:{t}|year:{y}'


def year(it):
    for k in ('published-print','published-online','issued'):
        p=it.get(k,{}).get('date-parts',[])
        if p and p[0]: return int(p[0][0])
    return None


def pauth(it):
    names=[]; arr=[]
    for a in it.get('author',[]):
        n=((a.get('given') or '')+' '+(a.get('family') or '')).strip()
        if not n: continue
        names.append(n)
        aff=[x.get('name','').strip() for x in a.get('affiliation',[]) if x.get('name')]
        arr.append({'author':n,'affiliations':aff})
    return '; '.join(names), json.dumps(arr,ensure_ascii=True)


def pubstatus(it):
    return 'published' if (it.get('volume') or it.get('issue') or it.get('page')) else 'forthcoming'


def read_rows():
    rows={}
    with open(CSV_PATH,'r',encoding='utf-8',newline='') as f:
        for r in csv.DictReader(f):
            rows[r['paper_key']]=r
    return rows


def write_rows(rows):
    with open(CSV_PATH,'w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=FIELDS)
        w.writeheader(); w.writerows(sorted(rows.values(),key=lambda r:(r.get('year',''),r.get('title',''))))


def iter_year(y):
    off=0
    while True:
        flt=f'issn:0025-1909,from-pub-date:{y}-01-01,until-pub-date:{y}-12-31,type:journal-article'
        q={'filter':flt,'rows':1000,'offset':off}
        u='https://api.crossref.org/works?'+urllib.parse.urlencode(q)
        msg=jget(u).get('message',{})
        items=msg.get('items',[])
        if not items: break
        for it in items:
            ct=[x.lower() for x in it.get('container-title',[])]
            if 'management science' not in ct: continue
            yield it
        off += len(items)
        if len(items) < 1000: break


def main():
    rows=read_rows(); before=len(rows)
    added=0
    for y in range(2016,2027):
        yc=0
        for it in iter_year(y):
            k=pkey(it)
            if k in rows: continue
            doi=(it.get('DOI') or '').strip()
            title=(it.get('title') or [''])[0].strip()
            an,af=pauth(it)
            q=urllib.parse.quote_plus(doi if doi else title)
            rows[k]={
                'paper_key':k,'year':str(year(it) or y),'title':title,'journal':'Management Science','status':'',
                'published_or_forthcoming':pubstatus(it),'doi':doi,'authors':an,
                'author_affiliations_at_publication':af,'school_credit_map':'','keep_om':'','exclude_reason':'',
                'google_scholar_citations':'','google_scholar_url':f'https://scholar.google.com/scholar?q={q}',
                'citations_last_checked_date':'','notes':'source=crossref;needs_citation_lookup',
                'ms_department':'','discipline_bucket':''
            }
            added += 1; yc += 1
        print('year',y,'added',yc)
    write_rows(rows)
    print('before',before,'after',len(rows),'added_total',added)

if __name__=='__main__':
    main()
