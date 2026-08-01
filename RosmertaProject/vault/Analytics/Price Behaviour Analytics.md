---
FileType: 📊 Analytics
tags: [analytics, reports, skills, price-behaviour]
Completed: false
---

# Price Behaviour Analytics

A small suite that studies how the book's stocks move over **a week** and **two
weeks**, plus a **backtest** of a dip-entry timing rule. Everything is generated on
demand from the live Postgres by one Claude Code skill and rendered as a shareable
dashboard (Artifact).

> Basis: **close-to-close** returns — weekly = Friday→Friday, biweekly = every-other
> -Friday. This is the *tradeable* move (weekend gap included). Equal-weight across
> the ~99 tickers in `price_hourly` — it is **not** an index. Window 2020–2026 skews
> bullish. All figures gross of costs. **Long-only** framing throughout.

## The dashboards (live Artifacts)

| Report | What it shows | Open |
|--------|---------------|------|
| **Weekly — Behaviour of the Book** | Fri→Fri return distribution & fat tails, movement-type taxonomy, intra-week path, momentum vs volatility, seasonality | https://claude.ai/code/artifact/67e19716-4ecd-4ed3-b186-05d785ba154f |
| **Biweekly / Fortnightly** | Same battery over two-week blocks; adds week-1 vs week-2 strength and a 10-slot high/low grid | https://claude.ai/code/artifact/13c91f0d-dbc8-4a3d-936d-1770fe3b95b5 |
| **Backtest — red-Wednesday dip entry** | "Buy a red Wednesday in an uptrend, hold ~10 days": equity curve vs buy-and-hold, execution edge, stops, costs | https://claude.ai/code/artifact/f8becc77-4555-4405-b256-638d31c83c0d |

Artifacts are **private** to the account until shared from the page's share menu.
Browse all of yours at **claude.ai/code/artifacts**. Links stay stable when a report
is refreshed to the same artifact; a fresh run otherwise mints a new URL.

## Headline findings (current data)

- **Direction is ~random, size is not.** Week-to-week return autocorrelation ≈ 0 and
  streaks extend at a coin-flip rate — but the **range** clusters strongly
  (autocorrelation +0.5). You can forecast turbulence, not direction. σ scales ≈ √2
  from weekly (5.4%) to biweekly (7.7%) — the random-walk signature.
- **Returns are fat-tailed** (excess kurtosis ≈ +9) and mildly right-skewed. ~1 week
  in 8 (1 fortnight in 5) moves more than ±5%.
- **Intra-period rhythm:** the week/fortnight tends to bottom early and top late;
  Wednesday is the strongest up-day; most of a fortnight's gain lands in **week 1**.
- **Dip-entry rule** (see the backtest): a **real but modest** execution edge —
  +0.36%/trade vs a random-day entry (p<1e-4), 55.7% win — worth it as an *overlay*
  on names the models already like, **not** a standalone system (it trails
  buy-and-hold, Sharpe 0.94 vs 1.14). Tight stops hurt it; drop the "green Monday"
  precondition — the red Wednesday is the whole signal.

## Regenerate / customise

The reports are produced by the **`weekly-price-report`** Claude Code skill (three
modes: weekly, biweekly, backtest), all parameterisable by watchlist and date range.
See **[[Running the reports]]** for the exact commands and options.

Related: [[Docs for using indicators]] · [[Lookforawd gains]]
