"""
Futures continuous series — volume roll + ratio back-adjustment
===============================================================
Splices a root's overlapping contracts into one gap-free series (board item
"Futures contract aggregation and continuous series").

Two sub-problems, both solved here:

  * **Roll schedule (when to switch):** *volume roll* — switch from the front
    contract to the next on the first day the next contract's daily volume
    exceeds the front's (falling back to the front's expiry if that never
    happens in the data we have). We already store per-bar TRADES volume, so no
    extra data is needed.
  * **Price adjustment (how to splice):** *ratio / proportional back-adjustment*
    — the newest (active/front) contract is the unadjusted anchor; each earlier
    contract's OHLC is scaled by the cumulative price ratio at the roll points,
    so **percentage returns are continuous** across every join. Volume is left
    unadjusted (it's the active contract's real volume).

The pure builder (``build_continuous``) works on in-memory candle lists and is
unit-tested without a DB/IB. ``build_and_store_continuous`` is the thin DB driver
that reads raw contracts, builds the series, and persists it as a ``CONTFUT``
child row. **Raw per-contract candles stay the source of truth**; the continuous
series is a derived, rebuildable artefact.
"""

from datetime import datetime, timezone

from price_retrival import ohlcv_store


def _daily_volume(candles):
    """Map each bar's date → that day's total volume."""
    agg = {}
    for ts, _o, _h, _l, _c, v in candles:
        d = ts.date()
        agg[d] = agg.get(d, 0) + v
    return agg


def _find_roll_ts(front, nxt):
    """Timestamp at which the active contract switches from ``front`` to ``nxt``.

    Volume roll: the first bar of ``nxt`` on/after the first *overlap day* where
    ``nxt``'s daily volume exceeds ``front``'s. Fallback: the first ``nxt`` bar
    on/after ``front``'s expiry; ultimately ``nxt``'s first bar.
    """
    fv, nv = _daily_volume(front['candles']), _daily_volume(nxt['candles'])
    for d in sorted(set(fv) & set(nv)):
        if nv[d] > fv[d]:
            for ts, *_ in nxt['candles']:
                if ts.date() >= d:
                    return ts
    expiry = front.get('expiry')
    if expiry is not None:
        for ts, *_ in nxt['candles']:
            if ts.date() >= expiry:
                return ts
    return nxt['candles'][0][0]


def _roll_ratio(front, nxt, roll_ts):
    """Proportional adjustment ratio at a roll = ``nxt_close / front_close`` at the
    last timestamp present in **both** contracts before ``roll_ts`` (1.0 if none)."""
    fc = {ts: c for ts, _o, _h, _l, c, _v in front['candles'] if ts < roll_ts}
    nc = {ts: c for ts, _o, _h, _l, c, _v in nxt['candles'] if ts < roll_ts}
    common = sorted(set(fc) & set(nc))
    if common and fc[common[-1]]:
        return nc[common[-1]] / fc[common[-1]]
    return 1.0


def build_continuous(contracts):
    """Build a continuous series from a root's contracts (pure).

    ``contracts``: an iterable of dicts ``{symbol, expiry (date), candles}`` where
    ``candles`` is ``[(ts, o, h, l, c, v), ...]`` (ts tz-aware, ascending).
    Contracts are processed in **expiry order**; the last (newest) is the
    unadjusted anchor. Returns::

        {'candles': [(ts, o, h, l, c, v), ...],   # back-adjusted, time-ordered
         'rolls':   [{'from','to','roll_ts','ratio'}, ...],
         'factors': {symbol: cumulative_factor, ...}}

    An empty input → empty series; a single contract → its candles unchanged.
    """
    contracts = sorted((c for c in contracts if c['candles']), key=lambda c: c['expiry'])
    if not contracts:
        return {'candles': [], 'rolls': [], 'factors': {}}
    if len(contracts) == 1:
        c = contracts[0]
        return {'candles': list(c['candles']), 'rolls': [], 'factors': {c['symbol']: 1.0}}

    n = len(contracts)
    rolls = []
    for i in range(n - 1):
        front, nxt = contracts[i], contracts[i + 1]
        roll_ts = _find_roll_ts(front, nxt)
        rolls.append({'from': front['symbol'], 'to': nxt['symbol'],
                      'roll_ts': roll_ts, 'ratio': _roll_ratio(front, nxt, roll_ts)})

    # Cumulative back-adjust factors: anchor (newest) = 1.0; older = product of
    # the ratios of every roll at or after it.
    factors = [1.0] * n
    for i in range(n - 2, -1, -1):
        factors[i] = factors[i + 1] * rolls[i]['ratio']

    # Assemble each contract's active slice [roll_in, roll_out), scaled by factor.
    out = {}
    for i, c in enumerate(contracts):
        lo = rolls[i - 1]['roll_ts'] if i > 0 else None
        hi = rolls[i]['roll_ts'] if i < n - 1 else None
        f = factors[i]
        for ts, o, h, l, cl, v in c['candles']:
            if lo is not None and ts < lo:
                continue
            if hi is not None and ts >= hi:
                continue
            out[ts] = (ts, o * f, h * f, l * f, cl * f, v)

    candles = [out[ts] for ts in sorted(out)]
    return {'candles': candles, 'rolls': rolls,
            'factors': {c['symbol']: factors[i] for i, c in enumerate(contracts)}}


def build_and_store_continuous(conn, root, series_symbol=None, as_of=None,
                               bar_size=ohlcv_store._HOURLY):
    """Read a root's raw contracts, build the continuous series, and persist it
    as a ``CONTFUT`` child row (default symbol ``<root>.c1``).

    Includes only contracts that have candles and whose expiry is on/before the
    current front month (``futures.get_front_contract``) — future, not-yet-active
    contracts are excluded. ``bar_size`` selects the timeframe to splice: raw
    contracts are read at that size from ``price_bars`` and the result is stored
    back at the same size (so a root can carry an hourly *and* a 1-min continuous
    series side by side). Returns a summary dict (``symbol, stock_id, bars,
    rolls, factors, bar_size``) or ``None`` if there is nothing to build.
    """
    from price_retrival import futures

    front = futures.get_front_contract(conn, root, as_of=as_of)
    if front is None:
        return None
    front_expiry = futures._as_date(front[2])

    contracts = []
    for sid, sym, expiry, _mult, _conid in futures.get_child_contracts(conn, root):
        ed = futures._as_date(expiry)
        if ed is None or (front_expiry is not None and ed > front_expiry):
            continue
        rows = ohlcv_store.read_bars(conn, sid, bar_size=bar_size)
        if not rows:
            continue
        candles = [(r[1], float(r[2]), float(r[3]), float(r[4]), float(r[5]), int(r[6]))
                   for r in rows]
        contracts.append({'symbol': sym, 'stock_id': sid, 'expiry': ed, 'candles': candles})

    if not contracts:
        return None

    result = build_continuous(contracts)
    symbol = series_symbol or f"{root}.c1"
    cont_id = futures.register_continuous(conn, root, symbol)
    n = ohlcv_store.upsert_bars(conn, cont_id, result['candles'], bar_size=bar_size)
    return {'symbol': symbol, 'stock_id': cont_id, 'bars': n,
            'rolls': result['rolls'], 'factors': result['factors'],
            'bar_size': bar_size, 'contracts': [c['symbol'] for c in contracts]}
