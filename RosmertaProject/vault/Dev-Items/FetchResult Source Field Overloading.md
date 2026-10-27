---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Backlog
Priority: 🟢
Bucket: tech-debt
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
`bulk_fetch_dividends` stuffs the dividend **source** (`edgar` / `ib` / `ib_fallback`) into `FetchResult.error` so the existing results table prints it in the Notes column — a self-documented hack in `bulk_fetch.py`. It conflates "which source succeeded" with "what went wrong", so a successful fetch carries a non-None `error`. Add a proper field instead.

# Development Plan
- [ ] Add an optional `source: str | None = None` (or `note`) field to the `FetchResult` dataclass.
- [ ] Populate `source` in `bulk_fetch_dividends`; stop overloading `error`.
- [ ] Update the Rich results renderer in `main.py` to show `source` for dividends and keep `error` for failures.

# Dependencies
##### Depends on
- Nothing.
##### Requires
- Nothing.
# Links
##### Relevant Docs
- `price_retrival/bulk_fetch.py` (`FetchResult`, `bulk_fetch_dividends`), `main.py` (results table renderer).
##### Prerequisite for
- Nothing (cleanup; makes success/error state unambiguous).
