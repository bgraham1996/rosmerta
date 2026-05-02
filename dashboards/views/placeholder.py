"""Placeholder view — proves the registry and view-switching mechanism work."""
from __future__ import annotations

from dash import Dash, html

from dashboards.base import View
from dashboards.registry import register_view


@register_view("placeholder")
class PlaceholderView(View):
    label = "Placeholder"

    def layout(self) -> html.Div:
        return html.Div([
            html.H2("Placeholder View"),
            html.P("If you can read this, the view registry and switcher are working."),
            html.P(id=self.cid("status"), children="Click the button to test callbacks."),
            html.Button("Test callback", id=self.cid("test-button"), n_clicks=0),
        ])

    def register_callbacks(self, app: Dash) -> None:
        from dash import Input, Output

        @app.callback(
            Output(self.cid("status"), "children"),
            Input(self.cid("test-button"), "n_clicks"),
            prevent_initial_call=True,
        )
        def _on_click(n_clicks: int) -> str:
            return f"Callback fired. Click count: {n_clicks}"
