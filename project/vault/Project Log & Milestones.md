---
FileType: 📓 Project Log
Project: rosmerta
ProjectCode: ROSM001
Status: 🟢 Active
Created: 2026-06-15
SourceArchive: "Note Processing/New Notes/Project Rosmerta Neo.md (former vault location — archival)"
tags:
  - rosmerta
  - project
  - log
---

# Project Log & Milestones

Digest of the dated dev-log in the parent-vault hub note **Project Rosmerta Neo**
(`ProjectCode: ROSM001`, `FileType: 💼 project`). That note is the authoritative
project journal; this is a repo-side capture. See [[Parent Vault Intel]].

## Milestones referenced (from the 2026-03-16 entry)

The log frames work as numbered milestones, recommended order:

1. **Incremental fetch + dedup (M2) — highest leverage, do first.** Before
   fetching, query the DB for the latest `timestamp` (price) / latest `period_end`
   (fundamentals) and only request data forward of that. IB: set `start_date` to
   `max(timestamp) + 1h`. EDGAR: skip any `(stock_id, period_end, period_type)`
   already present. The `fetch_log` table was designed for exactly this — ensure
   it's written and read. Also fixes the PFE duplicate (dedupe ticker list with a
   `set()`).
2. **Dividend retrieval (M1).** `dividends` table already exists. IB via
   `reqFundamentalData(reportType='ReportSnapshot')` is the quick path; EDGAR is
   the alternative. Add a `fetch dividends` command, upsert keyed on
   `(stock_id, ex_date, amount)`. *(Status: now implemented in the repo —
   `price_retrival/dividends_api.py` with EDGAR + IB fallback.)*
3. **Missing-fundamentals detection (M4).** Diagnostic that flags quarter gaps per
   stock (e.g. "AMGN missing Q2/Q3 2022") plus an IB fallback for gaps. Proposed as
   `stock check fundamentals` (report) + `stock fetch fundamentals TICKER --source ib`.
4. **Basic valuation model (M5).** Composite, transparent score (no ML yet):
   trailing P/E vs sector median, revenue growth QoQ/YoY, EPS trend, price vs
   200-day MA. Normalised percentile ranks within the universe → one sortable
   "attractiveness" score per stock.

## Dated log entries

| Date | Entry |
|---|---|
| **2026-03-16** 22:10 | DB at 50 tickers × 10 years ≈ **127 MB**. Laid out the milestone order above. |
| **2026-03-17** 11:41 | Low DB size means room to scale — target **500–1000 tickers**. |
| **2026-03-17** 22:27 | Need to fix the database connection. |
| **2026-03-19** 18:13 | Missing-data checker works; **lots of EDGAR gaps**. Implications: better XBRL item mapping, a better SQL script to analyse gaps, and logic for **Q4-only-in-annual** reporting. Interim: build technical-only analysis tools. |
| **2026-03-25** 21:25 | → `Dev Database Setup`. |
| **2026-04-24** 14:50 | Wants **goals, not just features**, for direction: 52-week avg/high/low, avg weekly/monthly movement, a Plotly dashboard to see them, a fundamental-to-price metric, dividend stats, YTD price range. |
| **2026-04-24** 21:40 | **Duplicates in `stocks` table** (caused by watchlist seeders). Fix plan: export tickers → export watchlists → drop watchlists → remove dupes → re-add watchlists. |
| **2026-06-10** 13:23 | `#review`. Rosmerta needs a way to **retrieve current positions** for daily performance calcs. *(Status: `portfolio` command + IB positions now exist.)* |

## Carried-over open threads

- Better EDGAR/XBRL **tag mapping** and a **gap-analysis SQL** script (from 03-19).
- **Q4-only-in-annual** fundamentals handling.
- A repeatable **dedup / watchlist-rebuild** procedure (from 04-24).
- A goals-oriented **stats + dashboard** layer (from 04-24).

Related: [[Roadmap & Versioning]] · [[Architecture Vision]]
