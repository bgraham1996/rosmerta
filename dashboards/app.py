"""Rosmerta dashboard — Dash app factory and runner."""
from __future__ import annotations

from dash import Dash, Input, Output, dcc, html

from dashboards.registry import get_registered_views

def _build_shell() -> html.Div:
    """Top-level layout: persistent header + active-view container."""
    views = get_registered_views()
    options = [{"label": cls.label, "value": key} for key, cls in views.items()]
    default_value = next(iter(views), None)

    return html.Div([
        html.Div([
            html.H1("ROSMERTA"),
            html.Label("View"),
            dcc.Dropdown(
                id="view-selector",
                options=options,
                value=default_value,
                clearable=False,
                style={"width": "240px"},
            ),
        ], id="global-header", style={"padding": "0.75rem 1.5rem"}),

        html.Div(id="active-view-container", style={"padding": "1.5rem"}),
    ])

def _register_global_callbacks(app: Dash) -> None:
    """Wire the view-selector to the active-view container."""
    views = get_registered_views()

    @app.callback(
        Output("active-view-container", "children"),
        Input("view-selector", "value"),
    )
    def _switch_view(view_key: str):
        if view_key is None or view_key not in views:
            return html.Div("No view selected.")
        return views[view_key]().layout()


def create_app() -> Dash:
    """Build and return the Dash app, with all views registered."""
    import dashboards.views  # noqa: F401  -- triggers view registration

    app = Dash(__name__, title="Rosmerta")
    app.layout = _build_shell()

    _register_global_callbacks(app)

    for cls in get_registered_views().values():
        cls().register_callbacks(app)

    return app


def run(host: str = "127.0.0.1", port: int = 8050, debug: bool = False) -> None:
    """Launch the dashboard server."""
    create_app().run(host=host, port=port, debug=debug)
