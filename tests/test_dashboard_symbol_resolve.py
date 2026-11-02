"""The deep-dive view must resolve a typed ticker to the stored symbol without
mangling mixed-case symbols (the continuous-futures series ``ES.c1``).

Network-free: a tiny fake cursor replicates the resolve query's semantics
(case-insensitive match, exact match preferred). Guards the casing fix — a
blind ``.upper()`` used to turn ``es.c1`` into an unknown ``ES.C1``.
"""

from dashboards.views.asset_deep_dive import _resolve_symbol


class _Cur:
    def __init__(self, symbols):
        self._symbols = symbols
        self._res = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params):
        raw = params[0]
        matches = [s for s in self._symbols if s.upper() == raw.upper()]
        matches.sort(key=lambda s: s == raw, reverse=True)  # ORDER BY (symbol = raw) DESC
        self._res = (matches[0],) if matches else None

    def fetchone(self):
        return self._res


class _Conn:
    def __init__(self, symbols):
        self._symbols = symbols

    def cursor(self):
        return _Cur(self._symbols)


def test_resolve_symbol_case_insensitive_prefers_exact():
    conn = _Conn(["PFE", "ES.c1", "ESZ26"])
    assert _resolve_symbol(conn, "pfe") == "PFE"       # equity, any case
    assert _resolve_symbol(conn, "ES.c1") == "ES.c1"   # exact mixed-case
    assert _resolve_symbol(conn, "es.c1") == "ES.c1"   # case-folded → stored mixed-case
    assert _resolve_symbol(conn, "ESZ26") == "ESZ26"
    assert _resolve_symbol(conn, "nope") is None


def test_resolve_symbol_exact_wins_over_casefold():
    # Pathological: two symbols differing only by case — the exact hit must win.
    conn = _Conn(["ES.C1", "ES.c1"])
    assert _resolve_symbol(conn, "ES.c1") == "ES.c1"
