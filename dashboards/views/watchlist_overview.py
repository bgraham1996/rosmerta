"""Watchlist overview: scrollable AG Grid table per watchlist.

Columns: ticker, latest quarterly net income, period end, 52-week ribbon,
latest close. The ribbon is a JS cell renderer drawing an inline SVG with
a neutral track from 52w low to 52w high and a marker at the latest close.
"""
from __future__ import annotations

import psycopg2
import dash_ag_grid as dag
from dash import Dash, Input, Output, dcc, html

from dashboards.base import View
from dashboards.registry import register_view
from dashboards.theme import (
    ACCENT,
    BORDER,
    SURFACE_ALT,
    TEXT,
    TEXT_DIM,
    TEXT_MUTED,
)
from db_config import get_db_config


# ── Watchlist names: fetched once at module import ───────────────────────
def _fetch_watchlist_names() -> list[str]:
    conn = psycopg2.connect(**get_db_config())
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT list_name FROM watchlist_members ORDER BY list_name"
            )
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


_WATCHLIST_NAMES = _fetch_watchlist_names()


# ── Overview query ───────────────────────────────────────────────────────
_OVERVIEW_SQL = """
WITH wl AS (
    SELECT s.stock_id, s.symbol, s.name
    FROM watchlist_members wm
    JOIN stocks s ON s.stock_id = wm.stock_id
    WHERE wm.list_name = %s AND s.is_active = TRUE
),
price_window AS (
    SELECT stock_id,
           MIN(low)  AS week52_low,
           MAX(high) AS week52_high
    FROM price_hourly
    WHERE stock_id IN (SELECT stock_id FROM wl)
      AND timestamp >= NOW() - INTERVAL '52 weeks'
    GROUP BY stock_id
),
latest_price AS (
    SELECT DISTINCT ON (stock_id)
           stock_id, close AS latest_close
    FROM price_hourly
    WHERE stock_id IN (SELECT stock_id FROM wl)
    ORDER BY stock_id, timestamp DESC
),
latest_ni AS (
    SELECT DISTINCT ON (stock_id)
           stock_id, net_income, period_end
    FROM fundamentals
    WHERE stock_id IN (SELECT stock_id FROM wl)
      AND period_type != 'FY'
      AND net_income IS NOT NULL
    ORDER BY stock_id, period_end DESC
)
SELECT wl.symbol,
       wl.name,
       latest_ni.net_income,
       latest_ni.period_end,
       price_window.week52_low,
       price_window.week52_high,
       latest_price.latest_close
FROM wl
LEFT JOIN price_window ON price_window.stock_id = wl.stock_id
LEFT JOIN latest_price ON latest_price.stock_id = wl.stock_id
LEFT JOIN latest_ni    ON latest_ni.stock_id    = wl.stock_id
ORDER BY wl.symbol;
"""


def _fetch_overview(list_name: str) -> list[dict]:
    """Run the overview query and return a list of row dicts for AG Grid."""
    conn = psycopg2.connect(**get_db_config())
    try:
        with conn.cursor() as cur:
            cur.execute(_OVERVIEW_SQL, (list_name,))
            rows = cur.fetchall()
    finally:
        conn.close()

    out = []
    for symbol, name, net_income, period_end, w52_low, w52_high, latest_close in rows:
        out.append({
            "symbol": symbol,
            "name": name,
            "net_income": float(net_income) if net_income is not None else None,
            "period_end": period_end.isoformat() if period_end is not None else None,
            "week52_low": float(w52_low) if w52_low is not None else None,
            "week52_high": float(w52_high) if w52_high is not None else None,
            "latest_close": float(latest_close) if latest_close is not None else None,
        })
    return out


# ── JS formatters and cell renderer (strings; evaluated by AG Grid) ──────
# Net income → "$2.4B" / "$345M" style.
_NET_INCOME_FORMATTER = """
function(params) {
    if (params.value === null || params.value === undefined) return '—';
    const v = params.value;
    const abs = Math.abs(v);
    const sign = v < 0 ? '-' : '';
    if (abs >= 1e9) return sign + '$' + (abs / 1e9).toFixed(2) + 'B';
    if (abs >= 1e6) return sign + '$' + (abs / 1e6).toFixed(1) + 'M';
    if (abs >= 1e3) return sign + '$' + (abs / 1e3).toFixed(1) + 'K';
    return sign + '$' + abs.toFixed(0);
}
"""

_PRICE_FORMATTER = """
function(params) {
    if (params.value === null || params.value === undefined) return '—';
    return params.value.toFixed(2);
}
"""

_DATE_FORMATTER = """
function(params) {
    return params.value ? params.value : '—';
}
"""

# Ribbon cell renderer. Reads low/high/current off params.data, draws an
# inline SVG: neutral track + accent tick at current price, with low/high
# values flanking. Returns the SVG as innerHTML on a wrapper div.
_RIBBON_RENDERER = f"""
class RibbonRenderer {{
    init(params) {{
        const low  = params.data.week52_low;
        const high = params.data.week52_high;
        const cur  = params.data.latest_close;

        this.eGui = document.createElement('div');
        this.eGui.style.width = '100%';
        this.eGui.style.height = '100%';
        this.eGui.style.display = 'flex';
        this.eGui.style.alignItems = 'center';

        if (low === null || high === null || cur === null || high <= low) {{
            this.eGui.innerHTML = '<span style="color:{TEXT_DIM};font-size:11px;">—</span>';
            return;
        }}

        const pct = Math.max(0, Math.min(1, (cur - low) / (high - low)));

        const W = 240;          // SVG drawable width (px)
        const H = 28;
        const padX = 36;        // room for low/high labels
        const trackY = H / 2;
        const trackX0 = padX;
        const trackX1 = W - padX;
        const trackLen = trackX1 - trackX0;
        const markerX = trackX0 + pct * trackLen;

        const fmt = (v) => v.toFixed(2);

        const svg = `
            <svg width="100%" height="${{H}}" viewBox="0 0 ${{W}} ${{H}}"
                 preserveAspectRatio="none"
                 xmlns="http://www.w3.org/2000/svg">
              <text x="${{trackX0 - 4}}" y="${{trackY + 4}}"
                    text-anchor="end" font-size="10"
                    fill="{TEXT_MUTED}" font-family="-apple-system, sans-serif">
                ${{fmt(low)}}
              </text>
              <text x="${{trackX1 + 4}}" y="${{trackY + 4}}"
                    text-anchor="start" font-size="10"
                    fill="{TEXT_MUTED}" font-family="-apple-system, sans-serif">
                ${{fmt(high)}}
              </text>
              <line x1="${{trackX0}}" y1="${{trackY}}"
                    x2="${{trackX1}}" y2="${{trackY}}"
                    stroke="{BORDER}" stroke-width="3" stroke-linecap="round" />
              <circle cx="${{markerX}}" cy="${{trackY}}" r="5"
                      fill="{ACCENT}" stroke="{SURFACE_ALT}" stroke-width="1.5">
                <title>${{fmt(cur)}}</title>
              </circle>
            </svg>
        `;
        this.eGui.innerHTML = svg;
    }}
    getGui() {{ return this.eGui; }}
    refresh() {{ return false; }}
}}
"""


# ── Column definitions ───────────────────────────────────────────────────
def _column_defs() -> list[dict]:
    return [
        {
            "field": "symbol",
            "headerName": "Ticker",
            "pinned": "left",
            "width": 100,
            "sortable": True,
            "filter": True,
        },
        {
            "field": "name",
            "headerName": "Name",
            "width": 220,
            "sortable": True,
            "filter": True,
        },
        {
            "field": "net_income",
            "headerName": "Net income (latest Q)",
            "width": 170,
            "sortable": True,
            "type": "numericColumn",
            "valueFormatter": {"function": _NET_INCOME_FORMATTER},
        },
        {
            "field": "period_end",
            "headerName": "Period end",
            "width": 130,
            "sortable": True,
            "valueFormatter": {"function": _DATE_FORMATTER},
        },
        {
            "headerName": "52-week range",
            "width": 280,
            "sortable": False,
            "filter": False,
            "cellRenderer": _RIBBON_RENDERER,
            "autoHeight": False,
        },
        {
            "field": "latest_close",
            "headerName": "Last",
            "width": 100,
            "sortable": True,
            "type": "numericColumn",
            "valueFormatter": {"function": _PRICE_FORMATTER},
        },
    ]


@register_view("watchlist_overview")
class WatchlistOverviewView(View):
    label = "Watchlist Overview"

    def layout(self) -> html.Div:
        options = [{"label": n, "value": n} for n in _WATCHLIST_NAMES]
        default_value = _WATCHLIST_NAMES[0] if _WATCHLIST_NAMES else None

        return html.Div([
            # ── Controls row ─────────────────────────────────────────
            html.Div([
                html.Div([
                    html.Label("Watchlist"),
                    dcc.Dropdown(
                        id=self.cid("watchlist-selector"),
                        options=options,
                        value=default_value,
                        clearable=False,
                        style={"width": "260px"},
                    ),
                ], className="control-group"),
            ], className="controls-row"),

            # ── Status line ──────────────────────────────────────────
            html.Div(
                id=self.cid("status"),
                style={"marginBottom": "0.75rem", "color": TEXT_MUTED},
            ),

            # ── Grid panel ───────────────────────────────────────────
            html.Div([
                dag.AgGrid(
                    id=self.cid("grid"),
                    columnDefs=_column_defs(),
                    rowData=[],
                    defaultColDef={
                        "resizable": True,
                        "sortable": True,
                    },
                    dashGridOptions={
                        "rowHeight": 34,
                        "headerHeight": 36,
                        "domLayout": "normal",
                        "animateRows": False,
                        "suppressCellFocus": True,
                    },
                    style={"height": "70vh", "width": "100%"},
                    className="ag-theme-alpine-dark",
                ),
            ], className="panel"),
        ])

    def register_callbacks(self, app: Dash) -> None:
        @app.callback(
            Output(self.cid("grid"), "rowData"),
            Output(self.cid("status"), "children"),
            Input(self.cid("watchlist-selector"), "value"),
        )
        def _load(list_name):
            if not list_name:
                return [], "No watchlist selected."

            try:
                rows = _fetch_overview(list_name)
            except Exception as exc:
                return [], f"Error loading {list_name}: {exc}"

            return rows, f"{list_name}: {len(rows)} tickers."
