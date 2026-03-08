#!/usr/bin/env python3
import csv
import json
import re
from collections import defaultdict

BASE = "."
RANK = f"{BASE}/_data/rankings_data.json"
OUT = f"{BASE}/data_work/school_year_metrics_combined.csv"

JOURNALS = {
    "ms": f"{BASE}/data_work/ms_om_paper_review.csv",
    "or": f"{BASE}/data_work/or_om_paper_review.csv",
    "msom": f"{BASE}/data_work/msom_om_paper_review.csv",
}

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
]

# Broad OM/OR author-affiliation signals used for global cleanup.
OM_AUTHOR_MARKERS = [
    "operations management",
    "operations research",
    "supply chain",
    "decision sciences",
    "management science",
    "operations and technology",
    "technology and operations",
    "industrial engineering",
    "logistics",
    "service operations",
    "healthcare operations",
]

# Common non-OM business disciplines to exclude when explicitly stated
# (unless an OM marker is also present in the same affiliation text).
NON_OM_AUTHOR_MARKERS = [
    "finance",
    "accounting",
    "marketing",
    "information systems",
    "strategy",
    "organizational",
    "economics",
    "real estate",
    "entrepreneur",
    "public policy",
]

# Manual author exclusions for known non-OM faculty in specific schools.
# Keys and names are matched case-insensitively after light normalization.
AUTHOR_EXCLUSIONS = {
    "Georgetown (McDonough)": {
        "harvey iglarsh",
        "jonathan stroud",
    },
    "Miami (Herbert)": {
        "ganggang xu",
    },
    "UConn": {
        "david bergman",
        "wei chen",
    },
    "Boston U (Questrom)": {
        "steven kou",
        "rena m conti",
        "nachiketa sahoo",
        "dirk hackbarth",
        "gordon burtch",
        "andrei hagiu",
        "james b rebitzer",
        "keith marzilli ericson",
        "a max reppen",
    },
    "WashU (Olin)": {
        "lamar pierce",
        "tat y chan",
        "cynthia cryder",
        "sinan erzurumlu",
    },
    "Arizona State (Carey)": {
        "suparerk lekwijit",
    },
    "Carnegie Mellon (Tepper)": {
        "holly wiberg",
        "michael d smith",
    },
    "Harvard": {
        "michael i norton",
    },
    "Indiana (Kelley)": {
        "matthew josefy",
        "vivek astvansh",
        "jorge mejia",
    },
    "Minnesota (Carlson)": {
        "russell j funk",
    },
    "Northwestern (Kellogg)": {
        "dean karlan",
    },
    "Stanford": {
        "susan athey",
        "stefan wager",
    },
    "UNC (Kenan-Flagler)": {
        "eva labro",
        "norris i bruce",
        "stephen glaeser",
    },
    "UT Dallas": {
        "srinivasan raghunathan",
    },
    "MIT": {
        "andrew w lo",
        "manish raghavan",
        "thomas l magnanti",
    },
    "Notre Dame (Mendoza)": {
        "john r busenbark",
    },
    "UPenn (Wharton)": {
        "gen li",
        "shane t jensen",
        "ying jin",
        "yuxin chen",
        "yuting wei",
        "zhimei ren",
    },
    "UCLA (Anderson)": {
        "hengchen dai",
    },
    "Chicago (Booth)": {
        "erika l kirgios",
    },
}

# Manual keep overrides for authors the paper-side affiliation text can
# misclassify (for example, because it only says "information systems" even
# though the person should count under the user's OM/OR definition).
AUTHOR_KEEPS = {
    "UT Dallas": {
        "mehmet ayvaci",
        "yining wang",
        "zalp zer",
    },
    "Johns Hopkins (Carey)": {
        "ruxian wang",
        "tinglong dai",
    },
    "Colorado (Leeds)": {
        "sentao miao",
    },
    "Dartmouth (Tuck)": {
        "jiwen ge",
    },
    "NYU (Stern)": {
        "jiawei zhang",
        "ilan lobel",
    },
    "Duke (Fuqua)": {
        "ali makhdoumi",
        "peng sun",
        "n bora keskin",
        "jing sheng song",
        "sa a peke",
    },
    "McGill (Desautels)": {
        "maxime c cohen",
        "derek d wang",
    },
    "Northwestern (Kellogg)": {
        "achal bassamboo",
        "antonio moreno",
        "ioannis stamatopoulos",
    },
    "UPenn (Wharton)": {
        "hamsa bastani",
    },
    "Columbia": {
        "santiago r balseiro",
        "omar besbes",
        "will ma",
    },
    "Chicago (Booth)": {
        "ozan candogan",
        "john r birge",
        "rad niazadeh",
    },
    "Toronto (Rotman)": {
        "ningyuan chen",
        "opher baron",
        "philipp af che",
    },
    "Stanford": {
        "kostas bimpikis",
        "yonatan gur",
    },
    "UBC (Sauder)": {
        "woonghee tim huh",
    },
    "UW (Foster)": {
        "michael r wagner",
        "masha shunko",
        "andrew f siegel",
        "soraya fatehi",
    },
    "MIT": {
        "negin golrezaei",
    },
    "USC (Marshall)": {
        "hamid nazerzadeh",
    },
    "UCLA (Anderson)": {
        "auyon siddiq",
    },
    "Cornell": {
        "ruihao zhu",
    },
    "Florida (Warrington)": {
        "yining wang",
        "xiajun amy pan",
    },
    "Emory (Goizueta)": {
        "tian heong chan",
        "eve rosenzweig",
        "kc diwas",
    },
}


def norm(s):
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def aliases_for_school(school):
    name = school["name"]
    full = school.get("full_name", "")
    # Keep aliases school-unit specific. Avoid university-only names.
    vals = set()

    # Full school label (often includes both university + school unit).
    vals.add(norm(full))

    # Unit names in parentheses, e.g., "(Ross School of Business)".
    generic_units = {
        "school of management",
        "school of business",
        "graduate school of business",
        "graduate school of management",
        "business administration",
        "college of business",
        "business school",
        "faculty of management",
        "faculty of business",
        "department of business administration",
    }

    for src in (name, full):
        for part in re.findall(r"\(([^)]*)\)", src or ""):
            p = norm(part)
            if p and p not in generic_units:
                # Skip single-word aliases (e.g. "smith", "ross") from the
                # short school name — they are common surnames and cause false
                # matches.  Multi-word unit names from full_name are safe.
                if " " not in p and src == name:
                    continue
                vals.add(p)

    # University + unit combined aliases, helpful for generic units like
    # "School of Management" where university context is needed.
    uni = norm(re.sub(r"\([^)]*\)", "", full))
    for part in re.findall(r"\(([^)]*)\)", full or ""):
        p = norm(part)
        if uni and p:
            vals.add(norm(f"{uni} {p}"))
            vals.add(norm(f"{p} {uni}"))

    if "Wharton" in name:
        vals.update(map(norm, ["wharton school", "the wharton school"]))
        vals.update(map(norm, ["wharton school university of pennsylvania", "the wharton school university of pennsylvania"]))
    if name == "Michigan (Ross)":
        vals.update(
            map(
                norm,
                [
                    "ross school of business",
                    "stephen m ross school of business",
                    "ross business school",
                    "school of business administration university of michigan",
                    "university of michigan school of business administration",
                ],
            )
        )
    if name == "Chicago (Booth)":
        vals.update(
            map(
                norm,
                [
                    "booth school of business",
                    "graduate school of business university of chicago",
                    "university of chicago graduate school of business",
                    "graduate school of business the university of chicago",
                ],
            )
        )
    if "UCLA" in name:
        vals.update(
            map(
                norm,
                [
                    "anderson school of management",
                    "ucla anderson school of management",
                    "ucla anderson",
                    "anderson graduate school of management",
                ],
            )
        )
    if "UC Berkeley" in name:
        vals.update(map(norm, ["haas school of business"]))
    if "UC San Diego" in name:
        vals.update(map(norm, ["rady school of management"]))
    if name == "UT Dallas":
        vals.update(map(norm, ["jindal school", "naveen jindal school of management"]))
    if "UBC" in name:
        vals.update(map(norm, ["sauder school of business"]))
    if "UIUC" in name:
        vals.update(
            map(
                norm,
                [
                    "gies",
                    "gies college of business",
                    "department of business administration university of illinois",
                    "university of illinois department of business administration",
                    "department of business administration university of illinois urbana champaign",
                ],
            )
        )
    if name == "Purdue (Daniels)":
        vals.update(
            map(
                norm,
                [
                    "daniels school of business",
                    "m e daniels jr school of business",
                    "m e daniels school of business",
                    "mitchell e daniels jr school of business",
                    "mitchell e daniels school of business",
                    "krannert school of management",
                    "krannert graduate school of management",
                    "krannert school of management purdue university",
                    "purdue university krannert school of management",
                ],
            )
        )
    if name == "Cornell":
        vals.update(
            map(
                norm,
                [
                    "sc johnson school of management",
                    "samuel curtis johnson graduate school of management",
                    "johnson graduate school of management",
                    "sc johnson graduate school of management",
                    "cjohnson graduate school of management",
                    "samuel curtis johnson college of business",
                    "sc johnson college of business",
                    "cornell sc johnson college of business",
                    "cornell tech johnson college of business",
                ],
            )
        )
    if "Carnegie Mellon" in name:
        vals.update(
            map(
                norm,
                [
                    "tepper school of business",
                    "heinz college",
                    "tepper school of business carnegie mellon university",
                ],
            )
        )
    if name in {"MIT", "MIT (Sloan)"}:
        vals.update(map(norm, ["mit sloan", "sloan school of management"]))
    if name == "Columbia":
        vals.update(
            map(
                norm,
                [
                    "graduate school of business columbia university",
                    "columbia university graduate school of business",
                    "columbia university business school",
                    "columbia business school",
                    "graduate school of business and data science institute columbia university",
                    "graduate school of business decision risk and operations division columbia university",
                ],
            )
        )
    if name == "Yale":
        vals.update(
            map(
                norm,
                [
                    "yale school of management",
                    "school of management yale university",
                    "school of management and cowles foundation yale university",
                ],
            )
        )
    if name == "Dartmouth (Tuck)":
        vals.update(
            map(
                norm,
                [
                    "amos tuck school of business administration",
                    "tuck school of business administration",
                ],
            )
        )
    if name == "WashU (Olin)":
        # Keep Washington University-specific aliases only; plain "Olin School
        # of Business" can incorrectly match Babson Olin.
        vals.update(
            map(
                norm,
                [
                    "john m olin school of business",
                    "olin business school washington university",
                    "olin business school washington university in st louis",
                ],
            )
        )
    if name == "SMU (Cox)":
        vals.update(map(norm, ["cox business school"]))
    if name == "Texas A&M (Mays)":
        vals.update(
            map(
                norm,
                [
                    "mayes business school",
                    "mays school of business",
                    "department of business administration texas a m university",
                    "department of information and operations management texas a m university",
                ],
            )
        )
    if name == "Texas (McCombs)":
        vals.update(
            map(
                norm,
                [
                    "mccombs school of business",
                    "the university of texas at austin mccombs school of business",
                    "university of texas at austin mccombs school of business",
                    "department of information risk and operations management mccombs school of business",
                    "department of information risk and operations management university of texas at austin",
                    "information risk and operations management mccombs school of business",
                ],
            )
        )
    if name == "McGill (Desautels)":
        vals.update(map(norm, ["desautels school of management", "desaultels faculty of management"]))
    if name == "NYU (Stern)":
        vals.update(map(norm, ["stern business school"]))
    if name == "USC (Marshall)":
        vals.update(map(norm, ["marshall business school"]))
    if name == "Georgetown (McDonough)":
        vals.update(
            map(
                norm,
                [
                    "mcdonough school of business",
                    "georgetown university mcdonough school of business",
                    "georgetown university mcdonough school of business washington district of columbia",
                    "georgetown mcdonough school of business",
                ],
            )
        )
    if name == "Miami (Herbert)":
        vals.update(
            map(
                norm,
                [
                    "miami herbert business school",
                    "university of miami miami herbert business school",
                    "herbert business school",
                    "school of business administration university of miami",
                    "miami herbert",
                ],
            )
        )
    if name == "UConn":
        vals.update(
            map(
                norm,
                [
                    "school of business university of connecticut",
                    "university of connecticut school of business",
                    "school of business uconn",
                ],
            )
        )
    if name == "South Carolina (Moore)":
        vals.update(
            map(
                norm,
                [
                    "darla moore school of business",
                    "moore school of business",
                    "university of south carolina darla moore school of business",
                    "department of management science darla moore school of business",
                ],
            )
        )
    if name == "Utah (Eccles)":
        vals.update(
            map(
                norm,
                [
                    "david eccles school of business",
                    "university of utah david eccles school of business",
                    "eccles school of business",
                ],
            )
        )
    if name == "Temple (Fox)":
        vals.update(
            map(
                norm,
                [
                    "fox school of business",
                    "fox school of business and management",
                    "temple university fox school of business",
                    "department of statistics operations and data science fox school of business",
                ],
            )
        )
    if name == "Baruch (Zicklin)":
        vals.update(
            map(
                norm,
                [
                    "zicklin school of business",
                    "baruch college zicklin school of business",
                    "loomba department of management zicklin school of business",
                ],
            )
        )
    if name == "UIC":
        vals.update(
            map(
                norm,
                [
                    "college of business administration university of illinois chicago",
                    "university of illinois chicago college of business administration",
                    "college of business administration university of illinois at chicago",
                    "university of illinois at chicago college of business administration",
                    "university of illinois chicago",
                ],
            )
        )
    if name == "UC Irvine (Merage)":
        vals.update(
            map(
                norm,
                [
                    "paul merage school of business",
                    "the paul merage school of business",
                    "university of california irvine paul merage school of business",
                    "university of california at irvine paul merage school of business",
                ],
            )
        )
    if name == "Pittsburgh (Katz)":
        vals.update(
            map(
                norm,
                [
                    "joseph m katz graduate school of business",
                    "katz graduate school of business",
                    "university of pittsburgh katz graduate school of business",
                ],
            )
        )
    if name == "Rice (Jones)":
        vals.update(
            map(
                norm,
                [
                    "jones graduate school of business",
                    "jesse h jones graduate school of business",
                    "rice university jones graduate school of business",
                ],
            )
        )
    if name == "Rochester (Simon)":
        vals.update(
            map(
                norm,
                [
                    "simon business school",
                    "simon school of business",
                    "university of rochester simon business school",
                ],
            )
        )
    if name == "Santa Clara (Leavey)":
        vals.update(
            map(
                norm,
                [
                    "leavey school of business",
                    "santa clara university leavey school of business",
                ],
            )
        )
    if name == "Tennessee (Haslam)":
        vals.update(
            map(
                norm,
                [
                    "haslam college of business",
                    "university of tennessee haslam college of business",
                    "department of business analytics and statistics university of tennessee",
                ],
            )
        )
    if name == "Houston (Bauer)":
        vals.update(
            map(
                norm,
                [
                    "bauer college of business",
                    "c t bauer college of business",
                    "university of houston bauer college of business",
                ],
            )
        )
    if name == "Rutgers":
        vals.update(
            map(
                norm,
                [
                    "rutgers business school",
                    "rutgers business school newark and new brunswick",
                ],
            )
        )
    if name == "Arizona (Eller)":
        vals.update(
            map(
                norm,
                [
                    "eller college of management",
                    "university of arizona eller college of management",
                ],
            )
        )
    if name == "Georgia (Terry)":
        vals.update(
            map(
                norm,
                [
                    "terry college of business",
                    "university of georgia terry college of business",
                ],
            )
        )
    if name == "Nebraska (Lincoln)":
        vals.update(
            map(
                norm,
                [
                    "university of nebraska lincoln college of business",
                    "college of business university of nebraska lincoln",
                    "college of business administration university of nebraska lincoln",
                ],
            )
        )
    if name == "Queens (Smith)":
        vals.update(
            map(
                norm,
                [
                    "smith school of business",
                    "queen s university smith school of business",
                    "queen's university smith school of business",
                ],
            )
        )
    if name == "Tulane (Freeman)":
        vals.update(
            map(
                norm,
                [
                    "freeman school of business",
                    "a b freeman school of business",
                    "tulane university freeman school of business",
                ],
            )
        )
    if name == "George Washington":
        vals.update(
            map(
                norm,
                [
                    "school of business george washington university",
                    "george washington university school of business",
                    "department of decision sciences school of business george washington university",
                ],
            )
        )
    if name == "Alberta":
        vals.update(
            map(
                norm,
                [
                    "alberta school of business",
                    "university of alberta school of business",
                ],
            )
        )
    if name == "BYU (Marriott)":
        vals.update(
            map(
                norm,
                [
                    "marriott school of business",
                    "marriott school of management",
                    "brigham young university marriott school of business",
                ],
            )
        )
    if name == "Buffalo (SUNY)":
        vals.update(
            map(
                norm,
                [
                    "university at buffalo school of management",
                    "state university of new york at buffalo school of management",
                    "school of management university at buffalo",
                ],
            )
        )
    if name == "Syracuse (Whitman)":
        vals.update(
            map(
                norm,
                [
                    "whitman school of management",
                    "syracuse university whitman school of management",
                    "martin j whitman school of management",
                ],
            )
        )
    if name == "Stanford":
        vals.update(map(norm, ["graduate school of business at stanford university"]))
    if name == "Toronto (Rotman)":
        vals.update(map(norm, ["rotman school of business"]))
    if name == "Emory (Goizueta)":
        vals.update(map(norm, ["goizeuta business school"]))
    return [v for v in vals if v]


def map_school(aff_text, alias_map):
    t = norm(aff_text)
    t_padded = f" {t} "
    best = None
    best_len = 0
    for school, aliases in alias_map.items():
        for a in aliases:
            if not a:
                continue
            a_padded = f" {a} "
            if a_padded in t_padded and len(a) > best_len:
                best = school
                best_len = len(a)
    return best


def is_business_affiliation_for_school(aff_text, school, school_aliases):
    t = norm(aff_text)
    t_padded = f" {t} "
    if any(m in t for m in BUSINESS_MARKERS):
        return True
    # School-specific aliases (e.g., "Wharton School") that may not include
    # generic words like business/management.
    for a in school_aliases or []:
        if not a:
            continue
        a_padded = f" {a} "
        if a_padded in t_padded and any(k in a for k in ["school", "college", "faculty", "gies", "wharton", "sloan", "ross", "booth", "haas", "kellogg", "darden", "fuqua", "jindal", "sauder", "tepper", "questrom", "carey", "fisher", "tippie", "mendoza", "goizueta", "anderson", "marshall", "warrington", "pamplin", "cox", "tuck", "broad", "smeal", "rady", "rotman", "kenan", "foster", "leeds", "heinz"]):
            return True
    return False


def parse_aff(raw):
    try:
        arr = json.loads(raw) if raw else []
        return arr if isinstance(arr, list) else []
    except Exception:
        return []


def parse_school_credit(raw, valid_schools):
    try:
        arr = json.loads(raw) if raw else []
        if isinstance(arr, list):
            return [x for x in arr if x in valid_schools]
    except Exception:
        return []
    return []


def norm_person(name):
    return norm(name)


def is_om_author_affiliation(aff_text):
    t = norm(aff_text)
    has_om = any(m in t for m in OM_AUTHOR_MARKERS)
    has_non_om = any(m in t for m in NON_OM_AUTHOR_MARKERS)
    if has_om:
        return True
    if has_non_om:
        return False
    # If no explicit signal is present, keep to avoid false negatives for
    # school-only affiliation strings.
    return True


def is_om_or(row):
    b = (row.get("discipline_bucket") or "").strip()
    keep = (row.get("keep_om") or "").strip()
    return b == "OM_OR" or keep == "1"


def main():
    schools = json.load(open(RANK, encoding="utf-8"))["schools"]
    school_names = [s["name"] for s in schools]
    valid = set(school_names)
    alias_map = {s["name"]: aliases_for_school(s) for s in schools}

    metrics = defaultdict(
        lambda: {
            "ms_std_papers": 0.0,
            "ms_personal_papers": 0.0,
            "ms_std_citations": 0.0,
            "ms_personal_citation_weighted": 0.0,
            "or_std_papers": 0.0,
            "or_personal_papers": 0.0,
            "or_std_citations": 0.0,
            "or_personal_citation_weighted": 0.0,
            "msom_std_papers": 0.0,
            "msom_personal_papers": 0.0,
            "msom_std_citations": 0.0,
            "msom_personal_citation_weighted": 0.0,
        }
    )

    for jkey, path in JOURNALS.items():
        rows = list(csv.DictReader(open(path, encoding="utf-8")))
        for r in rows:
            if not is_om_or(r):
                continue
            try:
                y = int(r.get("year") or 0)
            except Exception:
                continue
            if not (2016 <= y <= 2026):
                continue
            try:
                cites = float(r.get("citation_count") or 0)
            except Exception:
                cites = 0.0

            affs = parse_aff(r.get("author_affiliations_at_publication", ""))
            author_schools = []
            seen_auth = set()
            for a in affs:
                nm = (a.get("author") or "").strip()
                if not nm or nm in seen_auth:
                    continue
                seen_auth.add(nm)
                aff = " ; ".join(x for x in (a.get("affiliations") or []) if isinstance(x, str))
                s = map_school(aff, alias_map)
                if s and is_business_affiliation_for_school(aff, s, alias_map.get(s, [])):
                    if norm_person(nm) in AUTHOR_KEEPS.get(s, set()):
                        author_schools.append(s)
                        continue
                    if not is_om_author_affiliation(aff):
                        continue
                    if norm_person(nm) in AUTHOR_EXCLUSIONS.get(s, set()):
                        continue
                    author_schools.append(s)

            # Use affiliation-derived mapping for school-level credit to keep
            # school-level and personal metrics on the same attribution basis.
            std_schools = sorted(set(author_schools))

            std_p = f"{jkey}_std_papers"
            per_p = f"{jkey}_personal_papers"
            std_c = f"{jkey}_std_citations"
            per_c = f"{jkey}_personal_citation_weighted"

            for s in std_schools:
                rec = metrics[(s, y)]
                rec[std_p] += 1.0
                rec[std_c] += cites

            for s in author_schools:
                rec = metrics[(s, y)]
                rec[per_p] += 1.0
                rec[per_c] += cites

    fields = [
        "rankings_school",
        "year",
        "ms_std_papers",
        "ms_personal_papers",
        "ms_std_citations",
        "ms_personal_citation_weighted",
        "or_std_papers",
        "or_personal_papers",
        "or_std_citations",
        "or_personal_citation_weighted",
        "msom_std_papers",
        "msom_personal_papers",
        "msom_std_citations",
        "msom_personal_citation_weighted",
    ]

    out = []
    for s in school_names:
        for y in range(2016, 2027):
            rec = metrics[(s, y)]
            row = {"rankings_school": s, "year": str(y)}
            for f in fields[2:]:
                row[f] = f"{rec[f]:.4f}"
            out.append(row)

    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(out)

    print("rows_out", len(out))
    print("out", OUT)


if __name__ == "__main__":
    main()
