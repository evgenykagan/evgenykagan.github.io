#!/usr/bin/env python3
import csv
import json
from pathlib import Path

BASE = Path("data_work")
FILES = [
    BASE / "ms_om_paper_review.csv",
    BASE / "or_om_paper_review.csv",
    BASE / "msom_om_paper_review.csv",
]
OUT_ALL = BASE / "all_om_or_paper_review.csv"

# Broad but practical phrase list for identifying business-school affiliations.
BUSINESS_MARKERS = [
    "school of business",
    "business school",
    "school of management",
    "college of business",
    "faculty of management",
    "faculty of business",
    "management school",
    "graduate school of business",
    "graduate school of management",
    "department of business administration",
    "department of operations management",
    "department of decision sciences",
    "operations management area",
]


def parse_aff(raw):
    try:
        arr = json.loads(raw) if raw else []
        return arr if isinstance(arr, list) else []
    except Exception:
        return []


def has_business_affiliation(raw_aff):
    affs = parse_aff(raw_aff)
    for rec in affs:
        for aff in (rec.get("affiliations") or []):
            t = (aff or "").lower()
            if any(m in t for m in BUSINESS_MARKERS):
                return "yes"
    return "no"


def process_one(path: Path):
    rows = list(csv.DictReader(path.open(encoding="utf-8", newline="")))
    fieldnames = list(rows[0].keys()) if rows else []
    if "business_school_affiliation" not in fieldnames:
        fieldnames.append("business_school_affiliation")

    for r in rows:
        r["business_school_affiliation"] = has_business_affiliation(
            r.get("author_affiliations_at_publication", "")
        )

    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    yes_n = sum(1 for r in rows if r.get("business_school_affiliation") == "yes")
    return len(rows), yes_n


def build_combined():
    all_rows = []
    field_union = []
    for p in FILES:
        rows = list(csv.DictReader(p.open(encoding="utf-8", newline="")))
        for r in rows:
            all_rows.append(r)
            for k in r.keys():
                if k not in field_union:
                    field_union.append(k)

    # Keep key identifiers near the front.
    preferred = [
        "paper_key",
        "year",
        "journal",
        "title",
        "doi",
        "authors",
        "business_school_affiliation",
    ]
    fieldnames = []
    for k in preferred:
        if k in field_union:
            fieldnames.append(k)
    for k in field_union:
        if k not in fieldnames:
            fieldnames.append(k)

    with OUT_ALL.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)

    yes_n = sum(1 for r in all_rows if r.get("business_school_affiliation") == "yes")
    return len(all_rows), yes_n


def main():
    for p in FILES:
        total, yes_n = process_one(p)
        print(f"{p.name}: rows={total}, business_yes={yes_n}")
    t, y = build_combined()
    print(f"{OUT_ALL.name}: rows={t}, business_yes={y}")


if __name__ == "__main__":
    main()

