# Design: Portfolio Analytics

_Status: draft · 2026-08-01 · owner: bg_

Turns the executed trades now landing in the DB (via `fetch trades` → `transaction`/`trade`)
into a **position & performance picture**: what's held, what each closed trade earned, and how
the book is doing. This is the payoff of the IB Flex work. Companion dev item:
`[[Trades and Portfolio Analytics]]`.

## Goals

- Derive **positions** (open holdings and closed round-trips) from raw `trade` rows.
- **Realized P&L** per closed lot and in aggregate; **unrealized P&L** on open holdings.
- **Holding-period** and per-trade outcome stats (win rate, avg gain/loss, duration) — the same
  raw material the `days_offset_gain` / behaviour analysis wants, but keyed to real entries.
- A read-only analysis surface that matches the existing `data_models` layer (Dash view + CLI).

## Non-goals (for the first cut)

- **Shorting.** The trader is **long-only** (buy then sell, never short) — matching assumes it.
- **Multi-currency.** Ingest already skips CASH/forex and defaults trades to USD; treat the book
  as single-currency USD. Revisit only if non-USD equity trades appear.
- **Tax-lot reporting / wash sales.** Out of scope; FIFO gives tax-lot-ish semantics for free but
  this is an analysis tool, not a tax product.

## Source of truth

**`trade` is the source of truth; positions are *derived*.** Each IB execution is one `trade` row
(`trade_type` BUY/SELL, `stock_quantity` stored **positive**, direction in `trade_type`;
`stock_price`, `trade_datetime`, `ib_trade_id`). Deriving positions on the fly keeps analysis
always-fresh, avoids a sync problem after re-ingest, and is trivially testable against the FakeDB —
consistent with the project rule *"fetchers persist; `data_models`/`indicators` only read."*

Order trades by `(trade_datetime, trade_id)`. Note `trade_datetime` currently carries a timezone
offset bug (`[[Flex Trade Timezone Fix]]`) — ordering within a symbol is unaffected, but absolute
holding-period durations inherit it until that's fixed.

### Schema tension (the key decision)

The existing `position` table pairs **one** `trade_open_id` with **one** `trade_close_id` and stores
`realized_pl`, but has **no quantity column**. A single buy of 20 closed by two sells of 10 can't be
represented as one position row, and a sell that partially closes a buy has nowhere to record the
matched quantity. So the schema as-is only works if every sell exactly matches a prior whole buy —
which your real fills violate.

**Resolution:** model positions as **FIFO lots** and, *if* we persist them, extend `position` with a
`quantity` (and `open_price`/`close_price`) column so a lot-slice is representable. See *Persistence*.

## Decision 1 — Cost basis: FIFO (recommended) vs average cost

| | FIFO (per-lot) | Average cost (per-symbol) |
|---|---|---|
| Realized P&L on a sell | sell proceeds − cost of the **oldest** shares consumed | sell proceeds − (running avg cost × qty) |
| Holding periods | **exact per lot** (each buy→sell pair) | not well-defined (avg only) |
| Matches IB reporting | yes | no |
| Complexity | lot queue, partial splits | one running (qty, avg) per symbol |

**Recommend FIFO.** It yields exact per-round-trip holding periods and outcomes — which is the whole
point for this book (it feeds the dip-entry / offset-growth behaviour work) — and matches how IB
reports. Average cost is simpler but throws away the per-entry timing we care about.

## Decision 2 — Derive in memory (recommended) vs persist to `position`

**Recommend derive-primary.** A read-only `Portfolio` class computes lots from `trade` each run.
Persisting to the `position` table becomes an **optional materialization** (a `build positions`
command) for the dashboard, rebuilt idempotently from scratch — never the source of truth. This
sidesteps staleness after re-ingest and keeps the analysis layer pure.

## The FIFO matching algorithm

```
lots      = defaultdict(deque)   # stock_id -> queue of open lots (qty, price, trade_id, dt)
closed    = []                   # closed lot-slices
for t in trades ordered by (trade_datetime, trade_id):
    if t.trade_type == 'BUY':
        lots[t.stock_id].append(Lot(qty=t.qty, price=t.price, trade_id=t.id, dt=t.dt))
    elif t.trade_type == 'SELL':
        remaining = t.qty
        while remaining > 0 and lots[t.stock_id]:
            lot     = lots[t.stock_id][0]
            matched = min(remaining, lot.qty)
            realized = matched * (t.price - lot.price)          # long-only
            closed.append(ClosedLot(stock_id, matched, lot.price, t.price,
                                    lot.dt, t.dt, realized, lot.trade_id, t.id))
            lot.qty   -= matched
            remaining -= matched
            if lot.qty == 0:
                lots[t.stock_id].popleft()
        if remaining > 0:
            log.warning("sold more than held for stock %s (data gap?)", t.stock_id)
# open holdings = whatever remains in the lot queues
```

- **Holding period** of a closed slice = `sell.dt − buy.dt`.
- **Open holding** per symbol = sum of remaining lot quantities; **average open cost** =
  Σ(qty·price)/Σqty over remaining lots.
- Long-only makes this clean: sells only ever reduce lots; a `remaining > 0` sell is a data
  problem, not a short — log it rather than inventing a negative lot.

## P&L definitions

- **Realized P&L (per closed slice)** = `matched_qty × (sell_price − buy_price)`. Aggregate by
  symbol or overall by summing slices.
- **Unrealized P&L (per open lot)** = `open_qty × (current_price − buy_price)`, where
  `current_price` = latest `price_hourly.close` for that `stock_id`. If no price bars exist for a
  held symbol, mark unrealized as `None`/NaN and surface "price missing" rather than zero.
- **Total P&L** = realized + unrealized. **Total return** additionally folds in dividends received
  (`dividends` / `transfer` dividend rows) — *phase 2*, see Scope.
- **Fees:** IB Flex `Trade` rows carry `ibCommission`; the ingest does **not** store it yet, so P&L
  is currently **gross**. Netting fees requires adding a `commission` column to `trade` and
  ingesting it — tracked as a follow-up, noted here so numbers aren't mistaken for net.

## `Portfolio` class API (mirrors `data_models.Asset`/`Market`)

```python
class Portfolio:
    """Read-only view of the traded book, derived from `trade` rows.

    Lazy-cached like Asset/Market (self._*_cache); reads DB, never writes.
    """
    def __init__(self, conn, as_of=None, currency='USD'): ...

    def get_lots(self, conn):          # -> (open_lots_df, closed_lots_df)  [FIFO]
    def get_holdings(self, conn):      # -> per-symbol: qty, avg_cost, mkt_value, unrealized_pl
    def get_closed_positions(self, conn):  # closed round-trip slices + holding period + realized
    def realized_pl(self, conn, by='symbol'|'total')
    def unrealized_pl(self, conn)      # needs latest price_hourly.close
    def get_performance_stats(self, conn)  # win rate, avg gain/loss, avg hold, count, hit ratio
```

`as_of` (optional) lets P&L/holdings be evaluated at a past timestamp (replays trades up to it and
prices as-of), so the book can be back-tested — parallels `Asset(start, end)`.

## Edge cases

- **Held symbol with no price data** → unrealized = NaN, flagged; don't silently zero it.
- **Sell > held** → log, cap realized at available lots, continue (long-only data gap).
- **Same-timestamp fills** (e.g. an order split into two executions at `20260529;095303`) → stable
  order via secondary `trade_id` sort; both become separate lots, which is correct.
- **Fractional/whole shares** → `stock_quantity` is INT today; keep quantities numeric in the class
  so a later move to fractional shares doesn't need a rewrite.

## Persistence (optional materialization)

If/when the dashboard wants pre-computed rows, add a `build positions` command that truncates and
repopulates `position` from the FIFO result. This needs a migration, because a lot-slice carries a
quantity the current table can't hold:

```sql
ALTER TABLE position
  ADD COLUMN IF NOT EXISTS quantity    INT,
  ADD COLUMN IF NOT EXISTS open_price  NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS close_price NUMERIC(12,4);
-- asset_status: 'holding' (open) / 'closed hold' (closed long); realized_pl already exists.
```

Until then, `position` stays empty and everything is derived.

## Surfacing

- **CLI:** expand the existing `portfolio` command (currently live IB positions only) with a
  DB-backed mode — holdings, realized/unrealized, and a performance summary via Rich tables,
  matching the `fetch`/`bulk` output style.
- **Dashboard:** a new `dashboards/views/portfolio.py` view (self-registers via `@register_view`)
  — holdings table, realized-vs-unrealized, equity/holding-period charts.

## Testing

Deterministic `trade` fixtures against the FakeDB in `tests/`:

- FIFO realized P&L across a partial exit (buy 20 @10, buy 4 @12, sell 10 @15 → realized on 10
  oldest shares; 14 remain).
- Multiple buys then a full liquidation; holding-period correctness.
- Sell-more-than-held is logged and capped.
- Unrealized uses latest `price_hourly.close`; missing price → NaN.
- `get_performance_stats` win-rate/avg on a known set.

## Open questions to confirm before building

1. **FIFO vs average cost** — recommend FIFO (holding periods). Confirm.
2. **Derive vs persist** — recommend derive-primary + optional materialize. Confirm.
3. **Fees now or later?** — add `commission` to ingest + `trade` so P&L is net, or ship gross first?
4. **Dividends in total return** — phase 2, or in from the start (data already in `dividends`)?
5. **Timezone** — holding-period accuracy depends on `[[Flex Trade Timezone Fix]]`; fix first or
   accept the offset for now?

## Suggested phasing

1. `Portfolio` class + FIFO matching + realized P&L + open holdings, with tests (no prices).
2. Unrealized P&L from `price_hourly`; `get_performance_stats`.
3. CLI `portfolio` DB mode; then the Dash view.
4. (Optional) `build positions` materialization + migration; fees; dividends → total return.

## Links

- Schema: `db/add_positions.sql` (`transaction`/`trade`/`position`/`transfer`).
- Ingest: `price_retrival/trades_api.py`. Prices: `price_hourly`.
- Patterns to mirror: `data_models.py` (`Asset`/`Market`), `tests/conftest.py` (FakeDB).
- Dev items: `[[Trades and Portfolio Analytics]]`, `[[Flex Trade Timezone Fix]]`.
