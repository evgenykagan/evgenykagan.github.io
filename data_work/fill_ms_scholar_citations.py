#!/usr/bin/env python3
import csv
import datetime as dt
import re
import time
import urllib.request

CSV_PATH='/Users/evgenykagan/Library/CloudStorage/Dropbox/Website/evgenykagan.github.io/data_work/ms_om_paper_review.csv'
TODAY=dt.date(2026,2,19).isoformat()


def fetch(url,timeout=20):
    req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0','Accept':'text/html'})
    with urllib.request.urlopen(req,timeout=timeout) as r:
        return r.read().decode('utf-8','ignore')


def cited_by(url):
    try:
        h=fetch(url)
    except Exception:
        return None
    m=re.search(r'>Cited by\s*(\d+)<',h,re.I)
    if m:
        return int(m.group(1))
    m=re.search(r'Cited by\s*(\d+)',h,re.I)
    if m:
        return int(m.group(1))
    return None


def main(limit=50,sleep_s=2.5):
    with open(CSV_PATH,'r',encoding='utf-8',newline='') as f:
        rows=list(csv.DictReader(f))
    n=0
    for r in rows:
        if n>=limit: break
        if (r.get('google_scholar_citations') or '').strip():
            continue
        u=(r.get('google_scholar_url') or '').strip()
        if not u:
            continue
        c=cited_by(u)
        r['google_scholar_citations']='' if c is None else str(c)
        r['citations_last_checked_date']=TODAY
        n+=1
        if n%10==0:
            print('updated',n)
        time.sleep(sleep_s)

    with open(CSV_PATH,'w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=rows[0].keys())
        w.writeheader(); w.writerows(rows)
    print('done updated',n)

if __name__=='__main__':
    import sys
    limit=int(sys.argv[1]) if len(sys.argv)>1 else 50
    main(limit)
