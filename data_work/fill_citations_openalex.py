#!/usr/bin/env python3
import csv
import json
import time
import urllib.parse
import urllib.request

CSV_PATH='/Users/evgenykagan/Library/CloudStorage/Dropbox/Website/evgenykagan.github.io/data_work/ms_om_paper_review.csv'


def fetch_json(url, timeout=20):
    req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0 (mailto:evgeny@example.com)','Accept':'application/json'})
    with urllib.request.urlopen(req,timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8','ignore'))


def openalex_for_doi(doi):
    q=urllib.parse.quote('https://doi.org/'+doi, safe='')
    url=f'https://api.openalex.org/works/{q}'
    data=fetch_json(url)
    return data.get('cited_by_count'), url


def main(limit=1000, sleep_s=0.08):
    with open(CSV_PATH,'r',encoding='utf-8',newline='') as f:
        rows=list(csv.DictReader(f))
    fields=list(rows[0].keys())
    for c in ['citation_count','citation_source','citation_url','citation_last_checked_date']:
        if c not in fields:
            fields.append(c)

    done=0; updated=0; errors=0
    for r in rows:
        if done>=limit:
            break
        doi=(r.get('doi') or '').strip().lower()
        if not doi:
            continue
        if (r.get('citation_count') or '').strip():
            continue
        done += 1
        try:
            c,u=openalex_for_doi(doi)
            if c is not None:
                r['citation_count']=str(c)
                r['citation_source']='OpenAlex'
                r['citation_url']=u
                r['citation_last_checked_date']='2026-02-19'
                updated += 1
        except Exception:
            errors += 1
        if done % 100 == 0:
            print('processed',done,'updated',updated,'errors',errors)
        time.sleep(sleep_s)

    with open(CSV_PATH,'w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader(); w.writerows(rows)
    print('done processed',done,'updated',updated,'errors',errors)

if __name__=='__main__':
    import sys
    lim=int(sys.argv[1]) if len(sys.argv)>1 else 1000
    main(lim)
