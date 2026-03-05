#!/usr/bin/env python3
import csv, json, time, urllib.parse, urllib.request

CSV_PATH='/Users/evgenykagan/Library/CloudStorage/Dropbox/Website/evgenykagan.github.io/data_work/ms_om_paper_review.csv'


def hit(doi, timeout=8):
    q=urllib.parse.quote('https://doi.org/'+doi, safe='')
    u=f'https://api.openalex.org/works/{q}'
    req=urllib.request.Request(u, headers={'User-Agent':'Mozilla/5.0 (mailto:evgeny@example.com)','Accept':'application/json'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        j=json.loads(r.read().decode('utf-8','ignore'))
    return j.get('cited_by_count'), u


def main(limit=1000):
    rows=list(csv.DictReader(open(CSV_PATH,encoding='utf-8')))
    fields=list(rows[0].keys())
    for c in ['citation_count','citation_source','citation_url','citation_last_checked_date']:
        if c not in fields: fields.append(c)

    todo=[r for r in rows if (r.get('doi') or '').strip() and not (r.get('citation_count') or '').strip()]
    todo=todo[:limit]
    done=0; upd=0; err=0

    for r in todo:
        doi=r['doi'].strip().lower()
        done += 1
        ok=False
        for attempt in (1,2,3):
            try:
                c,u=hit(doi, timeout=8)
                if c is not None:
                    r['citation_count']=str(c)
                    r['citation_source']='OpenAlex'
                    r['citation_url']=u
                    r['citation_last_checked_date']='2026-02-19'
                    upd += 1
                ok=True
                break
            except Exception:
                time.sleep(0.2*attempt)
        if not ok:
            err += 1
        if done % 50 == 0:
            print('processed',done,'updated',upd,'errors',err)

    # write back merged by paper_key
    by={r['paper_key']:r for r in rows}
    for r in todo: by[r['paper_key']]=r
    out=[by[k] for k in by]
    with open(CSV_PATH,'w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader(); w.writerows(out)
    print('done processed',done,'updated',upd,'errors',err)

if __name__=='__main__':
    import sys
    lim=int(sys.argv[1]) if len(sys.argv)>1 else 1000
    main(lim)
