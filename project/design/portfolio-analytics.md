# Design: Portfolio Analytics

_Status: **design, ready for review** · first draft 2026-08-01 · revised 2026-11-18 against prod data · owner: bg_

Turns executed trades (`fetch trades` → `transaction`/`trade`) into a **position and performance
picture**: what's held, what each closed trade earned, and how the book is doing. Companion dev
item: `[[Trades and Portfolio Analytics]]` (Cycle 3, target V1.0.0.2).

**What changed since the August draft.** Trades are now current and correctly timed: 39 STK
trades to 2026-11-04, stored as UTC (`[[Flex Trade Timezone Fix]]`). Running the draft's FIFO
algorithm over them found **an issue the draft missed**: the book has *opening positions* that
predate the trade data (Decision 3). Prices now live in `price_bars` (bar-size aware), not
`price_hourly`. Dividends are no longer parked: per-share history now comes from IB (Decision 6),
and portfolio work depends on it, since dividends are a real part of the return on this book.

## Goals

- Derive **positions** (open holdings and closed round-trips) from raw `trade` rows.
- **Realized P&L** per closed lot and in aggregate; **unrealized P&L** on open holdings.
- **Holding-period** and per-trade outcome stats (win rate, avg gain/loss, duration): the same
  raw material the forward-gain / weekly-behaviour work uses, but keyed to real entries.
- **Dividend income** per holding and **total return** (price + dividends), so dividend payers
  (KO, PFE, ABT, BMY, GILD, MDT in this book) aren't understated.
- A read-only analysis surface matching `data_models` (`Asset`/`Market`), plus CLI and Dash.

## Non-goals (first cut)

- **Shorting.** The trader is **long-only** (buy then sell, never short). Matching assumes it.
- **Multi-currency.** Ingest skips CASH/forex; every STK trade so far is USD. Single-currency USD book.
- **Tax-lot reporting / wash sales.** FIFO gives tax-lot-like semantics, but this is an analysis tool.
- **Dividend reinvestment / DRIP lots.** Dividends are cash income, not new lots.

## What the real data looks like (probe, 2026-11-18)

Read-only run of the FIFO algorithm below over prod `trade`:

| | |
|---|---|
| Trades / symbols | 39 / 12 (22 BUY, 17 SELL), 2026-03-27 → 2026-11-04 |
| Closed FIFO slices | 20 (19 wins, 1 loss); hold 0–96 days, median 49 |
| Multi-slice symbols | 5 (partial exits and multi-lot buys are normal here, not edge cases) |
| Same-second split fills | yes (ABT ×2, GILD ×2, MDT ×2) |
| **Sells with no prior buy** | **4: KO 7 (03-27), PFE 5 (04-15), ADSK 4 (04-27), DOMO 20 (09-18)** |
| Open after matching | SMR 2 @ 8.515, OKLO 32 @ 42.80, BNTX 10 @ 101.77 |
| Price for held symbols | SMR, BNTX have `1 hour` bars to 2026-11-12; **OKLO has none** (not in `core`) |
| Flex fields available | `tradeID, symbol, conid, buySell, quantity, tradePrice, dateTime, orderTime, listingExchange`. **No commission, no currency.** |

The four orphan sells are shares bought **before 2026**: the Flex query's period is *YearToDate*,
so their buys were never downloaded. Without them, realized P&L (gross 697.41 today) is incomplete
and "open holdings" is only the part of the book bought this year.

## Source of truth

**`trade` is the source of truth; positions are *derived*** on each run. That keeps analysis fresh
after re-ingest, avoids a sync problem, and is testable against the FakeDB. It also keeps the
project rule *"fetchers persist; `data_models`/`indicators` only read."*

Order trades by `(trade_datetime, trade_id)`. `trade_datetime` is UTC, so holding periods are exact.
`stock_quantity` is stored positive with direction in `trade_type`.

## Decision 1: Cost basis → **FIFO** (recommended)

| | FIFO (per lot) | Average cost (per symbol) |
|---|---|---|
| Realized P&L on a sell | proceeds − cost of the **oldest** shares | proceeds − running avg × qty |
| Holding periods | **exact per lot** | not well defined |
| Matches IB reporting | yes | no |
| Complexity | lot queue, partial splits | one running (qty, avg) |

FIFO gives exact per-round-trip holding periods and outcomes, which is the point for this book: it
feeds the dip-entry and forward-gain work. Average cost discards the entry timing. With 5 of 12
symbols already multi-slice, the difference shows up immediately.

## Decision 2: Derive in memory (recommended), persist optionally

A read-only `Portfolio` class computes lots from `trade` each run. Writing to `position` is an
**optional materialization** (`build positions`, truncate-and-rebuild) for the dashboard, never the
source of truth. At 39 trades, deriving takes milliseconds; materialize only if the dashboard needs it.

The existing `position` table can't hold a lot slice anyway: it pairs one open trade with one close
trade and has **no quantity**, so a 20-share buy closed by two 10-share sells doesn't fit. If it's
ever materialized, it needs a migration first (see *Persistence*).

## Decision 3: Opening positions (**new; needs your call**)

Positions bought before the trade data starts have no `BUY` row. Options:

| Option | How | Pros | Cons |
|---|---|---|---|
| **A. Widen the Flex history** | Set the query period to the longest IB allows and re-ingest (idempotent on `ib_trade_id`) | Real fills, exact cost and dates; no new concepts | IB caps a Flex period (I believe 365 days, to check in the portal), so trades from before then are still missing; needs a portal change |
| **B. Opening-balance lots** | Seed one synthetic lot per symbol held at a cut-off date (qty, cost basis, as-of date) from IB's *Open Positions* Flex section or a statement | Complete book from a known date, whatever IB's history limit | Synthetic entries: holding periods for those lots start at the cut-off, not the real buy |
| **C. Ignore and flag** | Treat orphan sells as data gaps: log them and exclude them from realized P&L | Zero work | Realized P&L and win rate silently incomplete; DOMO's 20-share sale vanishes |

**Recommendation: A, then B for whatever's left.** Widen the query first: if it covers every orphan
sell, no synthetic lots are needed. For anything older, add opening-balance lots from IB's
*Open Positions* (it reports quantity and cost basis per holding). They'd live in a small
`opening_lot` table (`stock_id, quantity, cost_price, as_of, source`) that the FIFO engine reads as
BUYs dated `as_of` and flags as `synthetic` in its output. **C stays as the runtime safety net:**
any sell that still can't be matched is logged and reported, never invented.

## Decision 4: Fees → gross first, net later (recommended)

The Flex query has **no commission field**, so net P&L needs:
1. adding `ibCommission` (+ `ibCommissionCurrency`) to the Trades section of query `1571459` in
   the IB portal (your action);
2. a `trade.commission NUMERIC(10,4)` migration, and ingesting it;
3. a re-ingest. New fields only arrive on new rows, so backfill the existing 39 by `ib_trade_id`.

Recommendation: ship **gross** P&L in phase 1, labelled "gross" everywhere it's shown. Make the
portal change now so commissions start accumulating, and net them in phase 3.

## Decision 5: Price for unrealized P&L

- **Price source:** latest `close` from `price_bars` with `bar_size='1 hour'` via
  `ohlcv_store.read_bars` (not the legacy `price_hourly` view). Report the bar's timestamp next to
  each value so staleness is visible. As-of evaluation uses the last bar at or before `as_of`.
- **Held symbols without prices** (OKLO today): unrealized = NaN with a "no price" flag, never 0.
  Fix the gap with a `holdings` watchlist that the portfolio layer can *report* (symbols held but
  missing bars). The refresh workflows, and later `[[Scheduled data refresh]]`, fetch it alongside
  `core`. The analysis layer itself never fetches.

## Decision 6: Dividends → entitled from IB history first, received from Flex later

Two different things, from two sources:

- **Entitled (per-share history × shares held).** `dividends` rows with `source='ib_adjusted'`,
  derived from IB's daily adjusted vs unadjusted closes (`price_retrival/dividends_api.py`):
  ex-date and cash amount per share, no pay date. For each ex-date, shares held = open FIFO lot
  quantity at the *close of the day before* the ex-date (bought before ex-date → entitled; sold
  before ex-date → not). Entitled income = Σ(shares × amount), attributed to the lots open at
  the time, so a closed round-trip carries the dividends it collected.
- **Received (cash).** IB Flex **Cash Transactions** (dividends, withholding tax, payments in
  lieu) give the actual cash, pay date and the 15% US withholding. Needs that section added to
  the Flex query in the IB portal (your action) plus an ingest; until then income is **gross of
  withholding** and labelled so.

Recommendation: ship entitled dividends with the engine (phase 2) and reconcile against Flex cash
once it's ingested (phase 3). Orphan sells (Decision 3) mean early-2026 holdings are incomplete,
so dividends on those positions are understated until the opening positions are fixed.

## The FIFO matching algorithm

```
lots   = defaultdict(deque)   # stock_id -> open lots (qty, price, trade_id, dt, synthetic)
closed = []                   # closed lot slices
unmatched = []                # sells (or parts) with no lot to match: data gaps
for t in opening_lots_as_buys + trades, ordered by (dt, trade_id):
    if t.side == 'BUY':
        lots[t.stock_id].append(Lot(t.qty, t.price, t.id, t.dt, t.synthetic))
    else:  # SELL
        remaining = t.qty
        while remaining > 0 and lots[t.stock_id]:
            lot = lots[t.stock_id][0]
            m = min(remaining, lot.qty)
            closed.append(ClosedSlice(t.stock_id, m, lot.price, t.price, lot.dt, t.dt,
                                      realized=m * (t.price - lot.price),
                                      open_trade=lot.trade_id, close_trade=t.id,
                                      synthetic_open=lot.synthetic))
            lot.qty -= m; remaining -= m
            if lot.qty == 0: lots[t.stock_id].popleft()
        if remaining > 0:
            unmatched.append((t.id, t.stock_id, remaining))   # long-only: a gap, not a short
```

- **Holding period** of a slice = `close_dt − open_dt` (both UTC).
- **Open holding** per symbol = Σ remaining lot qty; **avg open cost** = Σ(qty·price) / Σqty.
- Split fills at the same second become separate lots and match in `trade_id` order: correct
  and deterministic.

## P&L definitions

- **Realized (slice)** = `qty × (sell − buy)`. Aggregate by symbol, by month, or total.
- **Unrealized (open lot)** = `qty × (last_close − buy)`; NaN when there's no price.
- **Return %** (slice) = `sell / buy − 1`; the capital-weighted aggregate is Σrealized / Σ(qty·buy).
- **Dividend income** (lot) = Σ over ex-dates while the lot was open of `qty × amount`
  (Decision 6); gross of withholding until Flex cash is ingested.
- **Total P&L** = realized + unrealized + dividend income. All **gross** until Decision 4 lands.
- **Total return %** (slice) = `(sell − buy + dividends per share) / buy`.
- Every result carries `synthetic_open` and an `unmatched` list, so incomplete numbers are visible.

## `Portfolio` class (mirrors `data_models.Asset`/`Market`)

```python
class Portfolio:
    """Read-only view of the traded book, derived from `trade` (+ `opening_lot`).

    Lazy-cached like Asset/Market (self._*_cache); reads the DB, never writes.
    """
    def __init__(self, conn, as_of=None, bar_size='1 hour'): ...

    def get_lots(self, conn)              # -> (open_lots_df, closed_slices_df), FIFO
    def get_unmatched(self, conn)         # -> sells with no lot (data gaps)
    def get_holdings(self, conn)          # per symbol: qty, avg_cost, last_close, last_bar_ts,
                                          #             mkt_value, unrealized_pl, price_missing
    def get_closed_positions(self, conn)  # closed slices + holding period + realized + return %
    def realized_pl(self, conn, by='symbol')   # 'symbol' | 'month' | 'total'
    def unrealized_pl(self, conn)
    def get_performance_stats(self, conn) # count, win rate, avg/median gain & loss,
                                          # avg/median hold, payoff ratio, best/worst
```

`as_of` replays trades and prices up to a timestamp, so the book can be evaluated at past dates.

## Persistence (optional; phase 4)

Only if the dashboard needs precomputed rows: `build positions` truncates and repopulates `position`
from the FIFO result, after this migration:

```sql
ALTER TABLE position
  ADD COLUMN IF NOT EXISTS quantity       INT,
  ADD COLUMN IF NOT EXISTS open_price     NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS close_price    NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS synthetic_open BOOLEAN DEFAULT FALSE;
-- asset_status: 'holding' (open) / 'closed hold' (closed long); realized_pl exists.
```

Any schema added here (`opening_lot`, `trade.commission`, `position` columns) also goes into the
fresh-create bundle under `[[Reconcile DB schema - init migrations and scripts]]`.

## Surfacing

- **CLI:** keep `portfolio` (live IB positions) and add `portfolio --db` (or a `book` command):
  holdings, realized/unrealized, performance summary, and an **unmatched/synthetic warning
  line**. Rich tables, lazy imports, like the `fetch`/`bulk` commands.
- **Reconciliation:** when the Gateway is up, compare derived open holdings with live IB
  positions and print any differences. That's the cheapest end-to-end check that the book is complete.
- **Dashboard:** `dashboards/views/portfolio.py` (`@register_view`). Holdings table,
  realized-by-month bars, holding-period histogram, entry/exit markers on the asset price chart.

## Testing (FakeDB, `tests/test_portfolio.py`)

- FIFO across a partial exit: buy 20 @10, buy 4 @12, sell 10 @15 → realized 50 on the 10 oldest; 14 remain.
- Multi-lot buys then full liquidation; holding periods in UTC.
- Same-second split fills match in `trade_id` order.
- Orphan sell → `unmatched`, excluded from realized, nothing invented.
- Opening-balance lot matches a later sell and is flagged `synthetic_open`.
- Unrealized uses the latest `1 hour` close at or before `as_of`; a missing price gives NaN + flag.
- `get_performance_stats` on a known set (win rate, payoff, median hold).
- Regression fixture built from the real probe shape (12 symbols, 4 orphans, 3 open).

## Decisions to confirm

1. **FIFO** cost basis. Recommended.
2. **Derive in memory**, materialize `position` only if needed. Recommended.
3. **Opening positions:** widen the Flex period first, then opening-balance lots for the rest.
   *Needs you:* change the query period in the IB portal, and later export Open Positions if gaps remain.
4. **Fees:** gross first. *Needs you:* add `ibCommission` to the Flex query now.
5. **Held-but-unpriced symbols:** a `holdings` watchlist fetched by the refresh workflows. Recommended.
6. **Dividends:** entitled income from IB-derived history in phase 2; Flex cash (and withholding)
   in phase 3. *Needs you:* add Cash Transactions to the Flex query.

## Phasing

1. **Engine:** `Portfolio` + FIFO + realized P&L + open holdings + unmatched reporting, with tests.
   Re-ingest with the widened Flex period first, if Decision 3A goes ahead.
2. **Prices, dividends and stats:** unrealized from `price_bars`, entitled dividend income and
   total return (Decision 6), `get_performance_stats`, `holdings` watchlist; CLI `portfolio --db`
   with IB reconciliation. Needs `bulk dividends --list core` (and `holdings`) loaded first.
3. **Completeness:** `opening_lot` for any remaining orphans; commissions → net P&L; Flex Cash
   Transactions → dividends received and withholding.
4. **Dash view**; optional `build positions` materialization.

## Links

- Schema: `db/add_positions.sql` (`transaction`/`trade`/`position`/`transfer`),
  `db/db_scripts/migrations/add_trade_ib_id.sql`.
- Ingest: `price_retrival/trades_api.py` (Flex query `1571459`). Prices: `price_retrival/ohlcv_store.py`
  (`read_bars`, `price_bars`).
- Patterns: `data_models.py` (`Asset`/`Market`), `tests/conftest.py` (FakeDB).
- Dev items: `[[Trades and Portfolio Analytics]]`, `[[Flex Trade Timezone Fix]]` (done),
  `[[Scheduled data refresh]]`, `[[Dividends source and fetcher fixes]]`.
