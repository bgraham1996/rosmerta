"""Asset deep-dive: single-ticker price chart with overlays + RSI."""
from __future__ import annotations

from datetime import date, timedelta

from dash import Dash, Input, Output, State, dcc, html

from dashboards.base import View
from dashboards.figures.price import price_with_indicators
from dashboards.registry import register_view


@register_view("asset_deep_dive")
class AssetDeepDiveView(View):
    label = "Asset Deep-Dive"

    DEFAULT_LOOKBACK_DAYS = 365

    def layout(self) -> html.Div:
        today = date.today()
        start_default = today - timedelta(days=self.DEFAULT_LOOKBACK_DAYS)

        return html.Div([
            # ── Controls row ─────────────────────────────────────────
            html.Div([
                html.Div([
                    html.Label("Ticker"),
                    dcc.Input(
                        id=self.cid("ticker"),
                        type="text",
                        value="PFE",
                        debounce=True,
                        style={"width": "120px"},
                    ),
                ], className="control-group"),

                html.Div([
                    html.Label("Date range"),
                    dcc.DatePickerRange(
                        id=self.cid("date-range"),
                        start_date=start_default,
                        end_date=today,
                        display_format="YYYY-MM-DD",
                    ),
                ], className="control-group"),

                html.Button(
                    "Load",
                    id=self.cid("load-button"),
                    n_clicks=0,
                    className="control-button",
                ),
            ], className="controls-row"),

            # ── Status line ──────────────────────────────────────────
            html.Div(
                id=self.cid("status"),
                style={"marginBottom": "0.75rem", "color": "#9098a8"},
            ),

            # ── Chart panel: title strip + loading-wrapped graph ────
            html.Div([
                html.Div([
                    html.H2(id=self.cid("chart-title"), children=""),
                ], className="chart-header"),

                dcc.Loading(
                    dcc.Graph(
                        id=self.cid("chart"),
                        config={"displayModeBar": False},
                    ),
                    type="default",
                    color="#7aa2f7",
                ),
            ], className="panel chart-panel"),
        ])

    def register_callbacks(self, app: Dash) -> None:
        @app.callback(
            Output(self.cid("chart"), "figure"),
            Output(self.cid("status"), "children"),
            Output(self.cid("chart-title"), "children"),
            Input(self.cid("load-button"), "n_clicks"),
            State(self.cid("ticker"), "value"),
            State(self.cid("date-range"), "start_date"),
            State(self.cid("date-range"), "end_date"),
            prevent_initial_call=False,
        )
        def _load(n_clicks, ticker, start_date, end_date):
            import psycopg2

            from data_models import Asset
            from db_config import get_db_config
            from indicators import Indicator

            if not ticker:
                return _empty_figure(), "Enter a ticker.", ""

            ticker = ticker.strip().upper()

            # Asset expects 'YYYY-MM-DD HH:MM:SS' strings.
            start_str = f"{start_date} 00:00:00"
            end_str = f"{end_date} 23:59:59"

            try:
                conn = psycopg2.connect(**get_db_config())
                try:
                    asset = Asset(conn, ticker, start_str, end_str)
                    prices_df = asset.get_prices(conn)

                    sma_20_ind = Indicator("sma", window=20)
                    sma_50_ind = Indicator("sma", window=50)
                    rsi_14_ind = Indicator("rsi", window=14)

                    asset.add_indicator(sma_20_ind, conn, source="close")
                    asset.add_indicator(sma_50_ind, conn, source="close")
                    asset.add_indicator(rsi_14_ind, conn, source="close")
                finally:
                    conn.close()
            except Exception as exc:
                return _empty_figure(), f"Error loading {ticker}: {exc}", ""

            if prices_df is None or prices_df.empty:
                return _empty_figure(), f"No price data for {ticker} in selected range.", ""

            # Set timestamp as index for the figure factory.
            prices_indexed = prices_df.set_index("timestamp")
            close = prices_indexed["close"]

            # Indicator results were computed against the un-indexed close series,
            # so they share the same positional alignment — re-index them to match.
            sma_20 = sma_20_ind._result.copy()
            sma_50 = sma_50_ind._result.copy()
            rsi_14 = rsi_14_ind._result.copy()
            sma_20.index = close.index
            sma_50.index = close.index
            rsi_14.index = close.index

            fig = price_with_indicators(
                ticker=ticker,
                prices=close,
                overlays={
                    "SMA(20)": sma_20,
                    "SMA(50)": sma_50,
                },
                rsi=rsi_14,
                rsi_window=14,
            )

            status = (
                f"Loaded {ticker}: {len(close)} bars from "
                f"{close.index[0]} to {close.index[-1]}."
            )
            return fig, status, ticker


def _empty_figure():
    import plotly.graph_objects as go
    from dashboards.theme import plotly_template

    fig = go.Figure()
    fig.update_layout(template=plotly_template(), height=620)
    return fig
