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
"""


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


def get_child_contracts(conn, root_symbol, active_only=True):
    """List a root's contracts (root -> children navigation), ordered by expiry.

    Returns rows as ``(stock_id, symbol, expiry, multiplier, ib_con_id)``. This
    is the only model function needed now; front-month selection and roll are
    deferred. Returns ``[]`` if the root is unknown or has no contracts.
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
