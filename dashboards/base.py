"""Base class for dashboard views."""
from __future__ import annotations

from typing import ClassVar

from dash import Dash, html


class View:
    """Base class for a dashboard view.

    Subclasses should:
      - Set `label` (the human-readable name shown in the view selector)
      - Implement `layout()` returning the view's Dash components
      - Implement `register_callbacks(app)` to wire up interactivity

    The `key` attribute is set automatically by @register_view and should
    be used as a prefix for all component IDs owned by the view, e.g.
    f"{self.key}--ticker-input".
    """

    key: ClassVar[str] = ""  # set by @register_view
    label: ClassVar[str] = ""

    def layout(self) -> html.Div:
        raise NotImplementedError

    def register_callbacks(self, app: Dash) -> None:
        raise NotImplementedError

    def cid(self, name: str) -> str:
        """Helper: return a namespaced component ID."""
        return f"{self.key}--{name}"
