---
StartDate: 2026-11-25
EndDate: 2026-12-08
Version:
Priority: 🟡
Status: ⚪ Planned
Dev Status:
DevStatus: Planned
Cycle: "4"
ReviewCount: "0"
---

# Cycle Objectives

Make data refresh unattended, so prod can't silently go stale again (it sat four months old until [[Cycle 3]]).

1. **Run IB Gateway headless**: no manual login, so jobs that need IB can run on a timer.
2. **Schedule the refreshes** on top of that, with a staleness check that flags a missed run.

# Cycle Metadata

- **Window:** 2026-11-25 → 2026-12-08 (two weeks, provisional)
- **Version:** not yet decided
- **Predecessor:** [[Cycle 3]]
- Started 2026-11-18 as a placeholder for items moved out of Cycle 3. Fill in the rest at Cycle 3 close.

# Dev Items in Cycle

## Unattended refresh:
- [ ] [[Headless IB]] 🟡: IBC auto-login and restart for the Gateway; prerequisite for the scheduler.
- [ ] [[Scheduled data refresh]] 🟡: launchd jobs for prices/trades (then fundamentals/futures), plus a last-bar-age check.
