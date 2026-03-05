#!/usr/bin/env python3
import csv
import math
from collections import defaultdict

BASE='/Users/evgenykagan/Library/CloudStorage/Dropbox/Website/evgenykagan.github.io/data_work'
INP=f'{BASE}/ms_person_metrics_rankings_only.csv'
OUT_PERSON_P=f'{BASE}/ms_person_rankings_papers.csv'
OUT_PERSON_C=f'{BASE}/ms_person_rankings_citations.csv'
OUT_PERSON_B=f'{BASE}/ms_person_rankings_blended.csv'
OUT_SCHOOL=f'{BASE}/ms_school_from_person_aggregates.csv'
OUT_SCHOOL_CMP=f'{BASE}/ms_school_metric_comparison.csv'


def f(x):
    try:
        return float(x)
    except Exception:
        return 0.0


def minmax(vals):
    if not vals:
        return (0.0, 1.0)
    mn=min(vals); mx=max(vals)
    if mx==mn:
        return (mn, mn+1.0)
    return (mn, mx)


def write_csv(path, fields, rows):
    with open(path,'w',encoding='utf-8',newline='') as fh:
        w=csv.DictWriter(fh,fieldnames=fields)
        w.writeheader(); w.writerows(rows)


def main():
    rows=list(csv.DictReader(open(INP,encoding='utf-8')))

    # Person-level rankings
    for r in rows:
        r['paper_count_frac_authors_num']=f(r.get('paper_count_frac_authors'))
        r['citation_frac_authors_openalex_num']=f(r.get('citation_frac_authors_openalex'))

    paper_vals=[r['paper_count_frac_authors_num'] for r in rows]
    cite_vals=[r['citation_frac_authors_openalex_num'] for r in rows]
    pmin,pmax=minmax(paper_vals)
    cmin,cmax=minmax(cite_vals)

    for r in rows:
        p=(r['paper_count_frac_authors_num']-pmin)/(pmax-pmin)
        c=(r['citation_frac_authors_openalex_num']-cmin)/(cmax-cmin)
        r['score_papers_norm']=p
        r['score_citations_norm']=c
        r['score_blended_50_50']=0.5*p+0.5*c

    # papers ranking
    papers_sorted=sorted(rows,key=lambda r:(-r['paper_count_frac_authors_num'],-r['citation_frac_authors_openalex_num'],r['person_name']))
    out=[]
    rank=0; prev=None
    for i,r in enumerate(papers_sorted,1):
        key=(r['paper_count_frac_authors_num'],r['citation_frac_authors_openalex_num'])
        if key!=prev: rank=i
        prev=key
        out.append({
            'rank':rank,
            'person_name':r['person_name'],
            'rankings_school':r['rankings_school'],
            'paper_count_full':r['paper_count_full'],
            'paper_count_frac_authors':f"{r['paper_count_frac_authors_num']:.4f}",
            'citation_frac_authors_openalex':f"{r['citation_frac_authors_openalex_num']:.4f}",
            'unique_om_or_papers':r['unique_om_or_papers'],
            'first_year':r['first_year'],
            'last_year':r['last_year'],
        })
    write_csv(OUT_PERSON_P,list(out[0].keys()) if out else ['rank','person_name'],out)

    # citation ranking
    cites_sorted=sorted(rows,key=lambda r:(-r['citation_frac_authors_openalex_num'],-r['paper_count_frac_authors_num'],r['person_name']))
    out=[]
    rank=0; prev=None
    for i,r in enumerate(cites_sorted,1):
        key=(r['citation_frac_authors_openalex_num'],r['paper_count_frac_authors_num'])
        if key!=prev: rank=i
        prev=key
        out.append({
            'rank':rank,
            'person_name':r['person_name'],
            'rankings_school':r['rankings_school'],
            'citation_sum_openalex':r['citation_sum_openalex'],
            'citation_frac_authors_openalex':f"{r['citation_frac_authors_openalex_num']:.4f}",
            'paper_count_frac_authors':f"{r['paper_count_frac_authors_num']:.4f}",
            'unique_om_or_papers':r['unique_om_or_papers'],
            'first_year':r['first_year'],
            'last_year':r['last_year'],
        })
    write_csv(OUT_PERSON_C,list(out[0].keys()) if out else ['rank','person_name'],out)

    # blended ranking 50/50 normalized
    blend_sorted=sorted(rows,key=lambda r:(-r['score_blended_50_50'],-r['paper_count_frac_authors_num'],-r['citation_frac_authors_openalex_num'],r['person_name']))
    out=[]
    rank=0; prev=None
    for i,r in enumerate(blend_sorted,1):
        key=(round(r['score_blended_50_50'],10),r['paper_count_frac_authors_num'],r['citation_frac_authors_openalex_num'])
        if key!=prev: rank=i
        prev=key
        out.append({
            'rank':rank,
            'person_name':r['person_name'],
            'rankings_school':r['rankings_school'],
            'score_blended_50_50':f"{r['score_blended_50_50']:.6f}",
            'score_papers_norm':f"{r['score_papers_norm']:.6f}",
            'score_citations_norm':f"{r['score_citations_norm']:.6f}",
            'paper_count_frac_authors':f"{r['paper_count_frac_authors_num']:.4f}",
            'citation_frac_authors_openalex':f"{r['citation_frac_authors_openalex_num']:.4f}",
            'unique_om_or_papers':r['unique_om_or_papers'],
        })
    write_csv(OUT_PERSON_B,list(out[0].keys()) if out else ['rank','person_name'],out)

    # School-level aggregations from person data
    school=defaultdict(lambda: {
        'people':0,
        'paper_full_sum':0.0,
        'paper_frac_sum':0.0,
        'citation_sum':0.0,
        'citation_frac_sum':0.0,
        'person_paper_frac_vals':[],
        'person_citation_frac_vals':[],
    })

    for r in rows:
        s=r['rankings_school']
        p=f(r['paper_count_frac_authors'])
        c=f(r['citation_frac_authors_openalex'])
        school[s]['people']+=1
        school[s]['paper_full_sum']+=f(r['paper_count_full'])
        school[s]['paper_frac_sum']+=p
        school[s]['citation_sum']+=f(r['citation_sum_openalex'])
        school[s]['citation_frac_sum']+=c
        school[s]['person_paper_frac_vals'].append(p)
        school[s]['person_citation_frac_vals'].append(c)

    sch_rows=[]
    for s,v in school.items():
        pp=sorted(v['person_paper_frac_vals'],reverse=True)
        cc=sorted(v['person_citation_frac_vals'],reverse=True)
        ptotal=max(v['paper_frac_sum'],1e-9)
        ctotal=max(v['citation_frac_sum'],1e-9)
        top1_p=(pp[0]/ptotal) if pp else 0.0
        top3_p=(sum(pp[:3])/ptotal) if pp else 0.0
        top1_c=(cc[0]/ctotal) if cc else 0.0
        top3_c=(sum(cc[:3])/ctotal) if cc else 0.0
        sch_rows.append({
            'rankings_school':s,
            'people_count':v['people'],
            'paper_full_sum':f"{v['paper_full_sum']:.0f}",
            'paper_frac_sum':f"{v['paper_frac_sum']:.4f}",
            'paper_frac_avg_per_person':f"{(v['paper_frac_sum']/v['people']):.4f}" if v['people'] else '0.0000',
            'citation_sum_openalex':f"{v['citation_sum']:.1f}",
            'citation_frac_sum_openalex':f"{v['citation_frac_sum']:.4f}",
            'citation_frac_avg_per_person':f"{(v['citation_frac_sum']/v['people']):.4f}" if v['people'] else '0.0000',
            'paper_concentration_top1_share':f"{top1_p:.4f}",
            'paper_concentration_top3_share':f"{top3_p:.4f}",
            'citation_concentration_top1_share':f"{top1_c:.4f}",
            'citation_concentration_top3_share':f"{top3_c:.4f}",
        })

    sch_rows.sort(key=lambda r:(-f(r['paper_frac_sum']),-f(r['citation_frac_sum_openalex']),r['rankings_school']))
    write_csv(OUT_SCHOOL,list(sch_rows[0].keys()) if sch_rows else ['rankings_school'],sch_rows)

    # Comparison table with derived rank positions
    def rank_map(rows, key):
        s=sorted(rows,key=lambda r:(-f(r[key]),r['rankings_school']))
        out={}
        rk=0; prev=None
        for i,r in enumerate(s,1):
            val=f(r[key])
            if val!=prev: rk=i
            prev=val
            out[r['rankings_school']]=rk
        return out

    rank_p=rank_map(sch_rows,'paper_frac_sum')
    rank_c=rank_map(sch_rows,'citation_frac_sum_openalex')
    cmp=[]
    for r in sch_rows:
        cmp.append({
            'rankings_school':r['rankings_school'],
            'rank_by_paper_frac_sum':rank_p[r['rankings_school']],
            'rank_by_citation_frac_sum':rank_c[r['rankings_school']],
            'paper_frac_sum':r['paper_frac_sum'],
            'citation_frac_sum_openalex':r['citation_frac_sum_openalex'],
            'people_count':r['people_count'],
            'paper_concentration_top3_share':r['paper_concentration_top3_share'],
            'citation_concentration_top3_share':r['citation_concentration_top3_share'],
        })
    cmp.sort(key=lambda r:(int(r['rank_by_paper_frac_sum']),r['rankings_school']))
    write_csv(OUT_SCHOOL_CMP,list(cmp[0].keys()) if cmp else ['rankings_school'],cmp)

    print('generated:')
    for p in [OUT_PERSON_P,OUT_PERSON_C,OUT_PERSON_B,OUT_SCHOOL,OUT_SCHOOL_CMP]:
        print(p)

if __name__=='__main__':
    main()
