# Management Science OM Filtering Pipeline

Scope:
- Journal: Management Science
- Window: 2016-01-01 to 2026-02-19
- Include: Published + forthcoming
- No double counting (forthcoming->published dedupe; school-level dedupe per paper)

Primary file:
- data_work/ms_om_paper_review.csv

Columns:
- paper_key: stable dedupe key
- year, title, journal, status, published_or_forthcoming, doi
- authors
- author_affiliations_at_publication
- school_credit_map
- keep_om
- exclude_reason
- google_scholar_citations
- google_scholar_url
- citations_last_checked_date
- notes
