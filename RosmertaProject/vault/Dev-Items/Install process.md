---
tags:
DevStatus: 
DependsOn: 
DevBucket: tech-debt
ReviewCount:
FileType: 🚥 Dev Item
StartDate:
EndDate: 
Priority: 🟢
Dev Status:
Cycle: "1"
ProjectCodes:
Requirements:
Completed: false
---

# Summary (Tech Debt)
The README advertises a `rosmerta ...` entry point (via `pip install -e .`), but `pyproject.toml` has **no `[project.scripts]`**, so in practice everything runs as `uv run main.py <command>`. Either wire up the console script or fix the docs so the install story is honest.

# Development Plan
- [ ] Decide: add `[project.scripts] rosmerta = "main:<group>"` (needs the click group importable), or document `uv run main.py` and drop the entry-point claim.
- [ ] If adding the script: verify `uv run rosmerta --help` after `uv sync`.
- [ ] Align README + CLAUDE.md (both mention the entry point).

# Dependencies
##### Depends on
- Nothing.
# Links
##### Relevant Docs
- `pyproject.toml`, `main.py` (click group `rosmerta`), README.md.
