"""Design tokens for the Rosmerta dashboard.

Single source of truth for colours, spacing, and typography. Both Dash
component styles and Plotly figure templates pull from here so the chart
and the chrome stay coherent.
"""
from __future__ import annotations


# ── Surfaces ──────────────────────────────────────────────────────────────
# Layered dark backgrounds. `bg` is the page; `surface` is cards/panels;
# `surface_alt` is for subtle contrast (table rows, hover states).
BG = "#0f1115"
SURFACE = "#171a21"
SURFACE_ALT = "#1d2129"
BORDER = "#262b36"

# ── Text ──────────────────────────────────────────────────────────────────
TEXT = "#e4e6eb"           # primary
TEXT_MUTED = "#9098a8"     # secondary / labels
TEXT_DIM = "#5d6577"       # tertiary / axis ticks, footnotes

# ── Accents ───────────────────────────────────────────────────────────────
# Muted, not neon. These are the workhorses for chart series.
ACCENT = "#7aa2f7"         # primary blue — main price line
ACCENT_ALT = "#bb9af7"     # purple — secondary series
POSITIVE = "#9ece6a"       # green — gains, bullish
NEGATIVE = "#f7768e"       # red — losses, bearish
WARN = "#e0af68"           # amber — alerts, neutral signals

# Indicator series palette — distinct but harmonious. Cycle through these
# when overlaying multiple indicators on one axis.
SERIES_PALETTE = [
    "#7aa2f7",  # blue
    "#bb9af7",  # purple
    "#e0af68",  # amber
    "#7dcfff",  # cyan
    "#9ece6a",  # green
    "#f7768e",  # red
]

# ── Typography ────────────────────────────────────────────────────────────
# System font stack — no web fonts to load, native rendering on each OS.
FONT_FAMILY = (
    '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, '
    '"Helvetica Neue", Arial, sans-serif'
)
FONT_SIZE_BASE = 14
FONT_SIZE_SMALL = 12
FONT_SIZE_HEADING = 18

# ── Spacing ───────────────────────────────────────────────────────────────
SPACE_XS = "0.25rem"
SPACE_SM = "0.5rem"
SPACE_MD = "1rem"
SPACE_LG = "1.5rem"
SPACE_XL = "2rem"

RADIUS = "6px"


# ── Plotly figure template ────────────────────────────────────────────────
def plotly_template() -> dict:
    """Return a Plotly layout template matching the dashboard theme.

    Apply per-figure via `fig.update_layout(template=plotly_template())`
    or set as default once at app startup.
    """
    return {
        "layout": {
            "paper_bgcolor": SURFACE,
            "plot_bgcolor": SURFACE,
            "font": {
                "family": FONT_FAMILY,
                "size": FONT_SIZE_BASE,
                "color": TEXT,
            },
            "colorway": SERIES_PALETTE,
            "xaxis": {
                "gridcolor": BORDER,
                "linecolor": BORDER,
                "zerolinecolor": BORDER,
                "tickcolor": TEXT_DIM,
                "tickfont": {"color": TEXT_MUTED, "size": FONT_SIZE_SMALL},
                "title": {"font": {"color": TEXT_MUTED}},
            },
            "yaxis": {
                "gridcolor": BORDER,
                "linecolor": BORDER,
                "zerolinecolor": BORDER,
                "tickcolor": TEXT_DIM,
                "tickfont": {"color": TEXT_MUTED, "size": FONT_SIZE_SMALL},
                "title": {"font": {"color": TEXT_MUTED}},
            },
            "legend": {
                "bgcolor": "rgba(0,0,0,0)",
                "bordercolor": BORDER,
                "borderwidth": 0,
                "font": {"color": TEXT_MUTED, "size": FONT_SIZE_SMALL},
            },
            "hoverlabel": {
                "bgcolor": SURFACE_ALT,
                "bordercolor": BORDER,
                "font": {"family": FONT_FAMILY, "color": TEXT},
            },
            "margin": {"l": 60, "r": 30, "t": 40, "b": 50},
        }
    }
