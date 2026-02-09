"""Token management wrapper around lark-oapi.

The lark-oapi SDK handles tenant access token lifecycle internally
(automatic fetch, caching, and refresh). This module provides a thin
wrapper that builds and exposes the underlying ``lark.Client`` instance
configured from application settings.
"""

from __future__ import annotations

import lark_oapi as lark

from lark_sync.config import settings


def build_lark_client(
    *,
    app_id: str | None = None,
    app_secret: str | None = None,
    domain: str | None = None,
    log_level: lark.LogLevel = lark.LogLevel.WARNING,
) -> lark.Client:
    """Build a configured ``lark.Client`` using application credentials.

    Parameters default to the values in ``settings`` so callers can
    simply call ``build_lark_client()`` with no arguments during normal
    operation.  Explicit overrides are accepted for testing.

    Args:
        app_id: Lark application ID. Falls back to ``settings.app_id``.
        app_secret: Lark application secret. Falls back to ``settings.app_secret``.
        domain: Lark API domain. Falls back to ``settings.domain``.
        log_level: SDK log verbosity. Defaults to WARNING.

    Returns:
        A fully configured ``lark.Client`` ready for API calls.

    Raises:
        ValueError: If app_id or app_secret are empty after resolving defaults.
    """
    resolved_app_id = app_id or settings.app_id
    resolved_app_secret = app_secret or settings.app_secret
    resolved_domain = domain or settings.domain

    if not resolved_app_id:
        raise ValueError(
            "Lark app_id is required. Set LARK_APP_ID env var or pass app_id explicitly."
        )
    if not resolved_app_secret:
        raise ValueError(
            "Lark app_secret is required. Set LARK_APP_SECRET env var or pass app_secret explicitly."
        )

    client = (
        lark.Client.builder()
        .app_id(resolved_app_id)
        .app_secret(resolved_app_secret)
        .domain(resolved_domain)
        .log_level(log_level)
        .build()
    )

    return client
