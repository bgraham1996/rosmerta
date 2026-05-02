"""Importing this package registers all views with the registry."""
from dashboards.views import placeholder  # noqa: F401

# When adding a new view, add an import line here.
from dashboards.views import asset_deep_dive   # noqa: F401
from dashboards.views import watchlist_overview   # noqa: F401
