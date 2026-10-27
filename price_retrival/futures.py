"""
Futures registry & root -> contract navigation
==============================================
The model-side helpers for the "contracts-as-stocks" futures schema
(migration ``db/db_scripts/migrations/add_futures_contract_support.sql``):

  * a root (e.g. ``ES``) is a ``stocks`` row with ``sec_type='FUTROOT'``;
  * each contract (e.g. ``ESZ26``) is a ``stocks`` row with ``sec_type='FUT'``
    and ``parent_stock_id`` pointing at its root;
  * a contract's candles are ordinary ``price_hourly`` rows keyed by its
    ``stock_id`` — write/read them via ``price_retrival.ohlcv_store``.

Scope (board item "Futures OHLCV storage and retrieval"): idempotent
registration of a root + its contracts, and the only model function needed
now — **root -> children navigation**. Front-month selection, roll, and
cross-contract aggregation are deferred (they need overlapping-contract
aggregation logic first).

Vendor-agnostic by design: store the **normalised** symbol (``ESZ26``) as the
canonical ``stocks.symbol``; IB's ``localSymbol`` (``ESZ6``) is never the key —
its ``conId`` maps into ``ib_con_id`` at ingestion (that mapping is the
deferred IB-retrieval slice). Every write here is idempotent via
``ON CONFLICT (symbol)`` so re-registering a contract updates its metadata
rather than duplicating it.

Phase B adds **term-structure registration**: a pure, IB-free mapping from an IB
futures contract to the canonical registration fields (``normalise_ib_future``),
and a batch register of a root + all its live contracts (``register_term_structure``).
The IB enumeration itself (``reqContractDetails``) lives in ``price_retrival.ib_api``;
everything here stays vendor-agnostic and unit-testable without a gateway.
"""

from datetime import date, datetime


def _as_date(value):
    """Coerce a stored expiry (``date`` / ``datetime`` / ``'YYYY-MM-DD'``) to a ``date``."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value)[:10], '%Y-%m-%d').date()


# CME / IB futures month codes (Jan..Dec). Vendor-neutral: these are the
# standard delivery-month letters, the same set IB encodes into ``localSymbol``.
_MONTH_CODES = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
                7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'}


def normalise_future_symbol(root, last_trade_date):
    """Build the canonical contract symbol (e.g. ``'ESZ26'``) for a root + expiry.

    ``last_trade_date`` is IB's ``lastTradeDateOrContractMonth`` — accepted as a
    ``'YYYYMMDD'`` or ``'YYYYMM'`` string, or a ``date`` / ``datetime``. The
    symbol is ``root + <month code> + <2-digit year>``.

    We deliberately construct this from the **full expiry** rather than reuse
    IB's ``localSymbol`` (``ESZ6``): its single-digit year is ambiguous across
    decades, whereas the contract month gives an unambiguous year. This is the
    "DB stores the normalised id, vendor ids map in" rule (the IB ``conId`` goes
    to ``ib_con_id``, never the symbol).
    """
    if hasattr(last_trade_date, 'year'):          # date / datetime
        year, month = last_trade_date.year, last_trade_date.month
    else:
        s = str(last_trade_date).strip()
        if len(s) not in (6, 8) or not s.isdigit():
            raise ValueError(
                f"last_trade_date must be YYYYMM / YYYYMMDD or a date, got {last_trade_date!r}"
            )
        year, month = int(s[:4]), int(s[4:6])
    if month not in _MONTH_CODES:
        raise ValueError(f"invalid contract month {month} in {last_trade_date!r}")
    return f"{root}{_MONTH_CODES[month]}{year % 100:02d}"


def _expiry_to_date(last_trade_date):
    """IB ``lastTradeDateOrContractMonth`` → a ``date``.

    ``'YYYYMMDD'`` → that date; a ``date``/``datetime`` passes through; a bare
    ``'YYYYMM'`` (no day) → ``None`` (we don't invent a day).
    """
    if hasattr(last_trade_date, 'year'):
        return last_trade_date.date() if isinstance(last_trade_date, datetime) else last_trade_date
    s = str(last_trade_date).strip()
    if len(s) >= 8 and s[:8].isdigit():
        return datetime.strptime(s[:8], '%Y%m%d').date()
    return None


def normalise_ib_future(contract):
    """Map an IB futures contract to the canonical registration fields.

    Pure and IB-free: ``contract`` may be an ``ib_insync`` ``Contract`` (any object
    exposing the attributes) **or** a plain ``dict`` with the same keys — so it is
    unit-testable from captured contract fixtures without a gateway. Reads
    ``symbol`` (the root), ``lastTradeDateOrContractMonth``, ``multiplier``,
    ``conId``, ``exchange``, ``currency`` and returns::

        {root, symbol, expiry, multiplier, ib_con_id, exchange, currency}

    where ``symbol`` is the **constructed** normalised symbol (``ESZ26``) and the
    IB ``conId`` lands in ``ib_con_id`` — never in the symbol (see
    ``normalise_future_symbol`` for why ``localSymbol`` is unsafe as a key).
    """
    get = contract.get if isinstance(contract, dict) else (lambda k: getattr(contract, k, None))
    root = get('symbol')
    last_trade = get('lastTradeDateOrContractMonth')
    if not root or not last_trade:
        raise ValueError(f"IB future missing symbol/lastTradeDateOrContractMonth: {contract!r}")
    multiplier = get('multiplier')
    return {
        'root': root,
        'symbol': normalise_future_symbol(root, last_trade),
        'expiry': _expiry_to_date(last_trade),
        'multiplier': float(multiplier) if multiplier not in (None, '') else None,
        'ib_con_id': get('conId') or None,
        'exchange': get('exchange'),
        'currency': get('currency') or 'USD',
    }


def resolve_stock_id(conn, symbol):
    """Return the ``stock_id`` for a symbol (root, contract, or equity), or None."""
    with conn.cursor() as cur:
        cur.execute("SELECT stock_id FROM stocks WHERE symbol = %s", (symbol,))
        row = cur.fetchone()
        return row[0] if row else None


def get_or_create_root(conn, symbol, name=None, exchange='CME', currency='USD'):
    """Idempotently register a futures **root** (``sec_type='FUTROOT'``).

    The root is an organizing parent only — not itself fetchable yet. Returns
    its ``stock_id``.
    """
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO stocks (symbol, name, exchange, currency, sec_type)
               VALUES (%s, %s, %s, %s, 'FUTROOT')
               ON CONFLICT (symbol) DO UPDATE SET sec_type = 'FUTROOT'
               RETURNING stock_id""",
            (symbol, name or symbol, exchange, currency),
        )
        stock_id = cur.fetchone()[0]
    conn.commit()
    return stock_id


def register_contract(conn, root_symbol, symbol, expiry, multiplier,
                      exchange='CME', currency='USD', ib_con_id=None, name=None):
    """Idempotently register a **contract** (``sec_type='FUT'``) under a root.

    Creates the root on demand if it doesn't exist. ``symbol`` must be the
    normalised contract symbol (``ESZ26``), ``expiry`` a date / ``'YYYY-MM-DD'``
    string (IB ``lastTradeDateOrContractMonth``), ``multiplier`` the contract
    multiplier. On re-registration the contract's metadata is updated in place;
    an existing ``ib_con_id`` is preserved when a new one isn't supplied.
    Returns the contract's ``stock_id``.
    """
    root_id = resolve_stock_id(conn, root_symbol)
    if root_id is None:
        root_id = get_or_create_root(conn, root_symbol, exchange=exchange, currency=currency)
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO stocks
                   (symbol, name, exchange, currency, sec_type,
                    parent_stock_id, expiry, multiplier, ib_con_id)
               VALUES (%s, %s, %s, %s, 'FUT', %s, %s, %s, %s)
               ON CONFLICT (symbol) DO UPDATE SET
                   parent_stock_id = EXCLUDED.parent_stock_id,
                   expiry          = EXCLUDED.expiry,
                   multiplier      = EXCLUDED.multiplier,
                   ib_con_id       = COALESCE(EXCLUDED.ib_con_id, stocks.ib_con_id),
                   sec_type        = 'FUT'
               RETURNING stock_id""",
            (symbol, name or symbol, exchange, currency,
             root_id, expiry, multiplier, ib_con_id),
        )
        contract_id = cur.fetchone()[0]
    conn.commit()
    return contract_id


def register_term_structure(conn, root_symbol, contracts, name=None):
    """Register a root + a batch of its contracts in one pass (Phase B).

    ``contracts`` is an iterable of normalised dicts as produced by
    ``normalise_ib_future`` (keys: ``symbol, expiry, multiplier, ib_con_id,
    exchange, currency``). The root is created/ensured first (taking exchange /
    currency from the first contract when available), then every contract is
    registered idempotently via ``register_contract``. Rows are processed in
    expiry order (undated last) for deterministic writes. Returns a list of
    ``(symbol, stock_id)`` in that order; an empty ``contracts`` is a no-op.
    """
    contracts = list(contracts)
    if not contracts:
        return []
    first = contracts[0]
    root_id = get_or_create_root(
        conn, root_symbol, name=name,
        exchange=first.get('exchange') or 'CME',
        currency=first.get('currency') or 'USD',
    )
    ordered = sorted(contracts, key=lambda c: (c.get('expiry') is None, c.get('expiry')))
    results = []
    for c in ordered:
        cid = register_contract(
            conn, root_symbol, c['symbol'], c.get('expiry'), c.get('multiplier'),
            exchange=c.get('exchange') or 'CME', currency=c.get('currency') or 'USD',
            ib_con_id=c.get('ib_con_id'),
        )
        results.append((c['symbol'], cid))
    return results


def get_child_contracts(conn, root_symbol, active_only=True):
    """List a root's contracts (root -> children navigation), ordered by expiry.

    Returns rows as ``(stock_id, symbol, expiry, multiplier, ib_con_id)``,
    expiry-ordered. ``get_front_contract`` builds on this for front-month
    selection; roll / cross-contract aggregation remain deferred. Returns ``[]``
    if the root is unknown or has no contracts.
    """
    root_id = resolve_stock_id(conn, root_symbol)
    if root_id is None:
        return []
    sql = ("SELECT stock_id, symbol, expiry, multiplier, ib_con_id "
           "FROM stocks WHERE parent_stock_id = %s")
    if active_only:
        sql += " AND is_active = TRUE"
    sql += " ORDER BY expiry ASC"
    with conn.cursor() as cur:
        cur.execute(sql, (root_id,))
        return cur.fetchall()


def get_front_contract(conn, root_symbol, as_of=None, active_only=True):
    """Return a root's **front-month** contract: the child with the nearest
    expiry on/after ``as_of`` (Phase C — front-month selection only).

    ``as_of`` defaults to today's UTC date; a ``date`` / ``datetime`` / ``'YYYY-MM-DD'``
    is accepted. Returns the row ``(stock_id, symbol, expiry, multiplier,
    ib_con_id)``, or ``None`` if the root is unknown or every contract has already
    expired (caller should enumerate/register a fresher chain). Contracts with a
    missing expiry are skipped. Roll convention and continuous series are
    deliberately out of scope — this just picks the active contract to fetch.
    """
    as_of = _as_date(as_of) if as_of is not None else datetime.utcnow().date()
    for row in get_child_contracts(conn, root_symbol, active_only=active_only):
        expiry = _as_date(row[2])
        if expiry is not None and expiry >= as_of:
            return row          # children are expiry-ascending → first match is nearest
    return None
