"""View registry for the Rosmerta dashboard.

Views self-register via the @register_view decorator. The dashboard
discovers available views at startup by importing dashboards.views,
which triggers registration as a side effect of module loading.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dashboards.base import View

_VIEWS: dict[str, type[View]] = {}


def register_view(key: str):
    """Decorator: register a View subclass under the given key.

    The key is used as the value in the view-selector dropdown and
    as the prefix for component IDs owned by this view.
    """
    def decorator(cls: type[View]) -> type[View]:
        if key in _VIEWS:
            raise ValueError(f"View key {key!r} already registered to {_VIEWS[key].__name__}")
        cls.key = key
        _VIEWS[key] = cls
        return cls
    return decorator


def get_registered_views() -> dict[str, type[View]]:
    """Return all registered view classes, keyed by registration key."""
    return dict(_VIEWS)
