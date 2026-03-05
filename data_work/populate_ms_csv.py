#!/usr/bin/env python3
import csv
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from typing import Dict, Iterable, List, Optional, Tuple

CROSSREF_URL = "https://api.crossref.org/journals/0025-1909/works"
TODAY = dt.date(2026, 2, 19)

CSV_FIELDS = [
    "paper_key",
    "year",
    "title",
    "journal",
    "status",
    "published_or_forthcoming",
    "doi",
    "authors",
    "author_affiliations_at_publication",
    "school_credit_map",
    "keep_om",
    "exclude_reason",
    "google_scholar_citations",
    "google_scholar_url",
    "citations_last_checked_date",
    "notes",
]


def fetch_json(url: str, timeout: int = 30) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; MS-OM-collector/1.0)",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", errors="ignore"))


def fetch_text(url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def norm_title(title: str) -> str:
    s = re.sub(r"\s+", " ", title.strip().lower())
    return re.sub(r"[^a-z0-9 ]", "", s)


def to_date(parts: Optional[List[int]]) -> Optional[dt.date]:
    if not parts:
        return None
    y = parts[0]
    m = parts[1] if len(parts) >= 2 else 1
    d = parts[2] if len(parts) >= 3 else 1
    try:
        return dt.date(y, m, d)
    except Exception:
        return None


def extract_year(item: dict) -> Optional[int]:
    for key in ("published-print", "published-online", "issued"):
        p = item.get(key, {}).get("date-parts", [])
        if p and p[0]:
            return int(p[0][0])
    return None


def extract_date(item: dict) -> Optional[dt.date]:
    for key in ("published-print", "published-online", "issued"):
        p = item.get(key, {}).get("date-parts", [])
        if p and p[0]:
            d = to_date(p[0])
            if d:
                return d
    return None


def authors_and_affils(item: dict) -> Tuple[str, str]:
    author_names = []
    rows = []
    for a in item.get("author", []):
        given = (a.get("given") or "").strip()
        family = (a.get("family") or "").strip()
        full = (given + " " + family).strip()
        if not full:
            continue
        author_names.append(full)
        aff = [x.get("name", "").strip() for x in a.get("affiliation", []) if x.get("name")]
        rows.append({"author": full, "affiliations": aff})
    return "; ".join(author_names), json.dumps(rows, ensure_ascii=True)


def paper_key(item: dict) -> str:
    doi = (item.get("DOI") or "").strip().lower()
    if doi:
        return f"doi:{doi}"
    title = (item.get("title") or [""])[0]
    year = extract_year(item) or 0
    return f"title:{norm_title(title)}|year:{year}"


def infer_pub_status(item: dict) -> str:
    # Heuristic: if no volume/issue/page yet, treat as forthcoming
    has_vol = bool(item.get("volume"))
    has_issue = bool(item.get("issue"))
    has_page = bool(item.get("page"))
    return "published" if (has_vol or has_issue or has_page) else "forthcoming"


def scholar_cited_by(query: str) -> Tuple[Optional[int], str]:
    q = urllib.parse.quote_plus(query)
    url = f"https://scholar.google.com/scholar?q={q}"
    try:
        html = fetch_text(url, timeout=20)
    except Exception:
        return None, url

    m = re.search(r">Cited by\s*(\d+)<", html, flags=re.IGNORECASE)
    if m:
        try:
            return int(m.group(1)), url
        except Exception:
            return None, url

    # fallback for alternate rendering
    m2 = re.search(r"Cited by\s*(\d+)", html, flags=re.IGNORECASE)
    if m2:
        try:
            return int(m2.group(1)), url
        except Exception:
            return None, url
    return None, url


def read_existing(csv_path: str) -> Dict[str, dict]:
    rows: Dict[str, dict] = {}
    if not os.path.exists(csv_path):
        return rows
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            k = row.get("paper_key", "")
            if k:
                rows[k] = row
    return rows


def write_rows(csv_path: str, rows: Iterable[dict]) -> None:
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for row in rows:
            out = {k: row.get(k, "") for k in CSV_FIELDS}
            w.writerow(out)


def crossref_iter(from_date: str = "2016-01-01", until_date: str = "2026-12-31"):
    cursor = "*"
    while True:
        url = (
            f"{CROSSREF_URL}?filter=from-pub-date:{from_date},until-pub-date:{until_date},type:journal-article"
            f"&rows=200&cursor={urllib.parse.quote(cursor, safe='')}"
            f"&select=DOI,title,container-title,published-print,published-online,issued,author,volume,issue,page"
        )
        data = fetch_json(url)
        msg = data.get("message", {})
        items = msg.get("items", [])
        if not items:
            break
        for it in items:
            # restrict strictly to Management Science title hits
            titles = [t.lower() for t in it.get("container-title", [])]
            if not any("management science" == t for t in titles):
                continue
            year = extract_year(it)
            if year is None or year < 2016:
                continue
            yield it

        next_cursor = msg.get("next-cursor")
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: populate_ms_csv.py <csv_path> [max_new_rows]")
        return 2

    csv_path = sys.argv[1]
    max_new = int(sys.argv[2]) if len(sys.argv) > 2 else 50

    existing = read_existing(csv_path)
    out = dict(existing)

    new_count = 0
    checked = 0

    for item in crossref_iter():
        checked += 1
        k = paper_key(item)
        if k in out:
            continue

        year = extract_year(item)
        d = extract_date(item)
        title = (item.get("title") or [""])[0].strip()
        doi = (item.get("DOI") or "").strip()
        authors, affils = authors_and_affils(item)
        pub_status = infer_pub_status(item)

        # Google Scholar lookup query prefers DOI.
        q = doi if doi else title
        cites, gs_url = scholar_cited_by(q)
        time.sleep(2.0)

        out[k] = {
            "paper_key": k,
            "year": str(year or ""),
            "title": title,
            "journal": "Management Science",
            "status": "",
            "published_or_forthcoming": pub_status,
            "doi": doi,
            "authors": authors,
            "author_affiliations_at_publication": affils,
            "school_credit_map": "",
            "keep_om": "",
            "exclude_reason": "",
            "google_scholar_citations": "" if cites is None else str(cites),
            "google_scholar_url": gs_url,
            "citations_last_checked_date": TODAY.isoformat(),
            "notes": "source=crossref+google_scholar",
        }

        new_count += 1
        if new_count % 10 == 0:
            print(f"Added {new_count} new rows (checked {checked} records)")
            write_rows(csv_path, sorted(out.values(), key=lambda r: (r.get("year", ""), r.get("title", ""))))

        if new_count >= max_new:
            break

    write_rows(csv_path, sorted(out.values(), key=lambda r: (r.get("year", ""), r.get("title", ""))))
    print(f"Done. checked={checked} new_rows={new_count} total_rows={len(out)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
