"""Download futures data for one or more roots and (re)build their continuous series.

For each root (``ES`` by default) this runs the full futures pipeline end to end:

1. **Register the term structure** — enumerate the root's contracts from IB
   (``reqContractDetails``) and register the root + every contract
   (``IBStockDataFetcher.register_futures_term_structure``). Enumerating with
   ``include_expired`` also pulls recently-expired contracts, so the continuous
   series can span past rolls.
2. **Fetch the contracts we need** — the current front month plus
   ``HISTORY_CONTRACTS`` prior (mostly expired) contracts, so the continuous
   series has history. Each contract is fetched **incrementally**: from the day
   of its last stored bar (or ``LOOKBACK_DAYS`` before its expiry if it has none)
   through tomorrow (bounded at the contract's expiry for expired ones). Bars are
   UTC and upserted ``ON CONFLICT``, so re-runs are idempotent.
3. **Build the continuous series** — volume roll + ratio back-adjustment into a
   ``CONTFUT`` child (``<root>.c1``) via
   ``price_retrival.continuous.build_and_store_continuous``.

Only the front + prior contracts are *fetched* (the continuous series anchors on
the front and needs history behind it); later-dated contracts are still
registered but left empty until they become the front.

Result
------
``main`` returns ``{root: summary}`` where each summary has the registered count,
per-contract fetch results, and the continuous-series build result, e.g.::

    out = main()                       # refresh ES, rebuild ES.c1
    out = main(roots=('ES', 'NQ'))     # several roots (same exchange)
"""

import time
import logging
from datetime import datetime, timedelta, timezone

import psycopg2 as db

from db_config import get_db_config
from price_retrival.ib_api import IBStockDataFetcher
from price_retrival import futures
from price_retrival.continuous import build_and_store_continuous

logger = logging.getLogger(__name__)


# --- Refresh parameters (tune here) ------------------------------------------
ROOTS = ('ES',)             # futures roots to refresh
EXCHANGE = 'CME'            # real exchange (never SMART for futures)
CURRENCY = 'USD'
HISTORY_CONTRACTS = 4       # prior contracts to fetch behind the front (ES: 4 = ~1yr)
LOOKBACK_DAYS = 220         # backfill window per contract with no stored bars yet
INCLUDE_EXPIRED = True      # enumerate + fetch recently-expired contracts for history
IB_HOST = '127.0.0.1'
IB_PORT = 4001
DELAY = 2                   # seconds between contract fetches, to respect IB pacing


def _tomorrow(now=None):
    """Tomorrow's date (UTC) — overshoot the end so the latest session is caught."""
    now = now or datetime.now(timezone.utc)
    return (now + timedelta(days=1)).date()


def _last_bars(conn, stock_ids):
    """``{stock_id: last hourly timestamp}`` for the given contracts (None if empty)."""
    if not stock_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """SELECT stock_id, MAX(timestamp) FROM price_hourly
               WHERE stock_id = ANY(%s) GROUP BY stock_id""",
            (list(stock_ids),),
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def _fetch_window(expiry, last_ts, tomorrow, lookback_days):
    """(start, end) 'YYYY-MM-DD' strings to fetch for a contract, or None if current.

    End is tomorrow, bounded a few days past the contract's expiry (expired
    contracts have no bars after that). Start is the day of the last stored bar
    (incremental) or ``lookback_days`` before expiry for a fresh contract.
    """
    end = min(tomorrow, expiry + timedelta(days=3))
    start = last_ts.date() if last_ts is not None else (expiry - timedelta(days=lookback_days))
    if start >= end:
        return None
    return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')


def _refresh_root(fetcher, conn, root, exchange, currency, tomorrow):
    """Register, fetch the front + priors, and rebuild the continuous series for one root."""
    root = root.upper()
    print(f"\n=== {root} ===")

    # 1. Register the term structure (incl. recently-expired for history).
    registered = fetcher.register_futures_term_structure(
        root, exchange, currency=currency, include_expired=INCLUDE_EXPIRED,
    )
    if registered is None:
        print(f"  [{root}] could not register term structure (IB/DB error)")
        return {'registered': 0, 'fetched': [], 'continuous': None}
    print(f"  registered {len(registered)} contracts")

    # 2. Select the front month + HISTORY_CONTRACTS priors (by expiry).
    front = futures.get_front_contract(conn, root)
    if front is None:
        print(f"  [{root}] no active front contract — skipping fetch/build")
        return {'registered': len(registered), 'fetched': [], 'continuous': None}
    front_expiry = futures._as_date(front[2])

    children = futures.get_child_contracts(conn, root)            # FUT, expiry asc
    selectable = [c for c in children if futures._as_date(c[2]) is not None
                  and futures._as_date(c[2]) <= front_expiry]
    selected = selectable[-(HISTORY_CONTRACTS + 1):]             # front + N priors
    last_bars = _last_bars(conn, [c[0] for c in selected])

    # 3. Fetch each selected contract incrementally.
    fetched = []
    for i, (sid, sym, expiry, _mult, _conid) in enumerate(selected, start=1):
        exp = futures._as_date(expiry)
        window = _fetch_window(exp, last_bars.get(sid), tomorrow, LOOKBACK_DAYS)
        if window is None:
            print(f"  [{i}/{len(selected)}] {sym}: up to date")
            fetched.append((sym, 'current', 0))
            continue
        start, end = window
        print(f"  [{i}/{len(selected)}] {sym}: fetching {start} -> {end}")
        df = fetcher.get_futures_hourly_bars(
            root, exp.strftime('%Y%m%d'), exchange, currency=currency,
            start_date=start, end_date=end, include_expired=INCLUDE_EXPIRED,
        )
        n = 0 if df is None else len(df)
        fetched.append((sym, 'ok' if n else 'empty', n))
        print(f"      {n} bars")
        if i < len(selected):
            time.sleep(DELAY)

    # 4. Rebuild the continuous series from whatever contracts now have candles.
    cont = build_and_store_continuous(conn, root)
    if cont is None:
        print(f"  [{root}] continuous: nothing to build")
    else:
        rolls = ' , '.join(f"{r['from']}->{r['to']}@{r['roll_ts'].date()}(x{r['ratio']:.4f})"
                           for r in cont['rolls']) or 'single contract'
        print(f"  continuous {cont['symbol']}: {cont['bars']:,} bars; rolls: {rolls}")

    return {'registered': len(registered), 'fetched': fetched, 'continuous': cont}


def main(roots=ROOTS, exchange=EXCHANGE, currency=CURRENCY):
    """Refresh futures data and rebuild the continuous series for each root."""
    db_config = get_db_config()
    tomorrow = _tomorrow()

    out = {}
    with IBStockDataFetcher(host=IB_HOST, port=IB_PORT, db_config=db_config) as fetcher:
        conn = fetcher.db_conn
        for root in roots:
            out[root] = _refresh_root(fetcher, conn, root, exchange, currency, tomorrow)

    # Summary
    print("\n=== summary ===")
    for root, s in out.items():
        cont = s['continuous']
        cont_str = f"{cont['symbol']} {cont['bars']:,} bars" if cont else "no continuous"
        bars = sum(n for _sym, _st, n in s['fetched'])
        print(f"  {root}: {s['registered']} registered, {bars:,} bars fetched, {cont_str}")
    return out


if __name__ == '__main__':
    main()
