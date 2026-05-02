"""Figure factories for price-related charts."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from dashboards.theme import (
    ACCENT,
    NEGATIVE,
    POSITIVE,
    SERIES_PALETTE,
    TEXT_DIM,
    TEXT_MUTED,
    plotly_template,
)


def price_with_indicators(
    ticker: str,
    prices: pd.Series,
    overlays: dict[str, pd.Series] | None = None,
    rsi: pd.Series | None = None,
    rsi_window: int = 14,
) -> go.Figure:
    """Build a two-row figure: price + overlays on top, RSI below.

    Parameters
    ----------
    ticker : str
        Used in the figure title.
    prices : pd.Series
        Close prices, indexed by date.
    overlays : dict[str, pd.Series] | None
        Series to plot on the price axis (e.g. {"SMA(20)": series, "SMA(50)": series}).
        Keys are used as legend labels.
    rsi : pd.Series | None
        RSI series, indexed by date. If None, the RSI subplot is omitted.
    rsi_window : int
        Used only for the RSI axis title.
    """
    has_rsi = rsi is not None

    if has_rsi:
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.04,
            row_heights=[0.72, 0.28],
        )
    else:
        fig = make_subplots(rows=1, cols=1)

    # ── Price line ────────────────────────────────────────────────────
    fig.add_trace(
        go.Scatter(
            x=prices.index,
            y=prices.values,
            name=ticker,
            line={"color": ACCENT, "width": 1.8},
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:.2f}<extra></extra>",
        ),
        row=1,
        col=1,
    )

    # ── Overlays (SMAs etc.) ──────────────────────────────────────────
    if overlays:
        # Skip the first palette colour — it's used for the main price line.
        palette = SERIES_PALETTE[1:] + SERIES_PALETTE[:1]
        for i, (label, series) in enumerate(overlays.items()):
            fig.add_trace(
                go.Scatter(
                    x=series.index,
                    y=series.values,
                    name=label,
                    line={"color": palette[i % len(palette)], "width": 1.2},
                    hovertemplate=f"{label}<br>%{{x|%Y-%m-%d}}<br>%{{y:.2f}}<extra></extra>",
                ),
                row=1,
                col=1,
            )

    # ── RSI subplot ───────────────────────────────────────────────────
    if has_rsi:
        fig.add_trace(
            go.Scatter(
                x=rsi.index,
                y=rsi.values,
                name=f"RSI({rsi_window})",
                line={"color": SERIES_PALETTE[2], "width": 1.2},
                hovertemplate="RSI<br>%{x|%Y-%m-%d}<br>%{y:.1f}<extra></extra>",
                showlegend=False,
            ),
            row=2,
            col=1,
        )
        
        # Reference lines: 70 (overbought), 30 (oversold), 50 (midline)
        # Reference lines: 70 (overbought), 30 (oversold), 50 (midline)
        fig.add_hline(
            y=70, line={"color": NEGATIVE, "width": 1.2, "dash": "dash"},
            row=2, col=1,
            annotation={"text": "70", "font": {"color": NEGATIVE, "size": 10}},
            annotation_position="right",
        )
        fig.add_hline(
            y=30, line={"color": POSITIVE, "width": 1.2, "dash": "dash"},
            row=2, col=1,
            annotation={"text": "30", "font": {"color": NEGATIVE, "size": 10}},
            annotation_position="right",
        )
        fig.add_hline(
            y=50, line={"color": TEXT_MUTED, "width": 0.8, "dash": "dot"},
            row=2, col=1,
        )

     
        fig.update_yaxes(
            title_text="RSI",
            range=[0, 100],
            tickvals=[30, 50, 70],
            row=2,
            col=1,
        )

    # ── Layout ────────────────────────────────────────────────────────
    fig.update_layout(
        template=plotly_template(),
        height=620 if has_rsi else 500,
        hovermode="x unified",
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1,
        },
        margin={"l": 60, "r": 30, "t": 20, "b": 50},
    )

    fig.update_yaxes(title_text="Price", row=1, col=1)
    if has_rsi:
        fig.update_xaxes(title_text="", row=1, col=1)
        fig.update_xaxes(title_text="Date", row=2, col=1)
    else:
        fig.update_xaxes(title_text="Date", row=1, col=1)

    # Hide non-trading periods (weekends + overnight gaps) so the chart
    # doesn't draw long diagonal lines across dead time.
    rangebreaks = [
        {"bounds": ["sat", "mon"]},          # weekends
        {"bounds": [21, 13.5], "pattern": "hour"},  # outside ~13:30–21:00 UTC (covers US session)
    ]
    fig.update_xaxes(rangebreaks=rangebreaks, row=1, col=1)
    if has_rsi:
        fig.update_xaxes(rangebreaks=rangebreaks, row=2, col=1)

    return fig
