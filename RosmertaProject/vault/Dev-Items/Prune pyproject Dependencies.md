---
FileType: 🚥 Dev Item
Type: Tech-Debt
Status: Backlog
Priority: 🟡
Bucket: tech-debt
Cycle: "2"
DependsOn: 
StartDate: 
EndDate: 
tags: 
---
# Summary
`pyproject.toml` declares several dependencies that are **standard library** on Python 3.13 and pull dead — sometimes install-breaking — PyPI backports: `argparse`, `dataclasses` (the `dataclasses` backport is for <3.7 and misbehaves on modern Python), and `pathlib` (the PyPI `pathlib` backport is known to break builds). `dotenv>=0.9.9` is also suspect — the package that provides `import dotenv` / `load_dotenv` is **`python-dotenv`**, not `dotenv`. Plus some heavy deps look unused (`marimo`, `seaborn`, `dash-ag-grid`?), and the metadata is placeholder (`description = "Add your description here"`, `version = "0.1.0"` vs metadata.json's 1.0.0.0).

# Development Plan
- [ ] Remove `argparse`, `dataclasses`, `pathlib` (all stdlib on 3.13).
- [ ] Replace `dotenv` with `python-dotenv` (verify `db_config.load_dotenv` still resolves).
- [ ] Audit actually-imported packages (`grep -r import`) vs declared; drop unused (`marimo`/`seaborn`/`dash-ag-grid` unless used by dashboards).
- [ ] Fix `description` + align `version`.
- [ ] `uv sync` from clean, run `uv run pytest` + `uv run main.py --help` to confirm nothing broke.

# Dependencies
##### Depends on
- Nothing.
##### Requires
- Clean `uv sync` to validate.
# Links
##### Relevant Docs
- `pyproject.toml`, `db_config.py` (`load_dotenv`), `dashboards/` (dash-ag-grid usage check).
##### Prerequisite for
- Reliable installs (ties into [[Install process]]); trustworthy dependency surface.
