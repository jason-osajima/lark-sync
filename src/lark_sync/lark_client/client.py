"""Composed Lark API client that exposes all sub-clients.

``LarkClient`` is the single entry point for all Lark API operations.
It builds the underlying ``lark.Client`` via ``auth.build_lark_client``
and exposes domain-specific sub-clients as properties.
"""

from __future__ import annotations

import lark_oapi as lark

from lark_sync.lark_client.auth import build_lark_client
from lark_sync.lark_client.blocks import BlocksClient
from lark_sync.lark_client.documents import DocumentsClient
from lark_sync.lark_client.drive import DriveClient
from lark_sync.lark_client.search import SearchClient
from lark_sync.lark_client.wiki import WikiClient


class LarkClient:
    """Unified Lark API client composing all domain sub-clients.

    Instantiate with no arguments to use settings from environment
    variables, or pass explicit credentials for testing.

    Usage::

        client = LarkClient()
        doc = client.documents.get("some_doc_id")
        blocks = client.blocks.list_all_blocks("some_doc_id")
        spaces = client.wiki.list_all_spaces()

    Args:
        app_id: Optional override for ``LARK_APP_ID``.
        app_secret: Optional override for ``LARK_APP_SECRET``.
        domain: Optional override for ``LARK_DOMAIN``.
    """

    def __init__(
        self,
        *,
        app_id: str | None = None,
        app_secret: str | None = None,
        domain: str | None = None,
    ) -> None:
        self._raw_client: lark.Client = build_lark_client(
            app_id=app_id,
            app_secret=app_secret,
            domain=domain,
        )

        self._documents: DocumentsClient | None = None
        self._blocks: BlocksClient | None = None
        self._wiki: WikiClient | None = None
        self._search: SearchClient | None = None
        self._drive: DriveClient | None = None

    # ------------------------------------------------------------------
    # Sub-client accessors (lazy-initialized)
    # ------------------------------------------------------------------

    @property
    def documents(self) -> DocumentsClient:
        """Document-level CRUD operations."""
        if self._documents is None:
            self._documents = DocumentsClient(self._raw_client)
        return self._documents

    @property
    def blocks(self) -> BlocksClient:
        """Block-level operations with rate limiting support."""
        if self._blocks is None:
            self._blocks = BlocksClient(self._raw_client)
        return self._blocks

    @property
    def wiki(self) -> WikiClient:
        """Wiki space and node operations."""
        if self._wiki is None:
            self._wiki = WikiClient(self._raw_client)
        return self._wiki

    @property
    def search(self) -> SearchClient:
        """Document and wiki search."""
        if self._search is None:
            self._search = SearchClient(self._raw_client)
        return self._search

    @property
    def drive(self) -> DriveClient:
        """Drive/folder operations."""
        if self._drive is None:
            self._drive = DriveClient(self._raw_client)
        return self._drive

    @property
    def raw(self) -> lark.Client:
        """Access the underlying ``lark.Client`` for advanced use cases."""
        return self._raw_client
