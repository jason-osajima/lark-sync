"""Block-level operations against the Lark Docx API.

Wraps ``/open-apis/docx/v1/documents/{id}/blocks`` endpoints: list, get,
create children, update, and batch delete.

Includes an asyncio-based rate limiter that enforces a maximum of
3 write operations per second per document, matching Lark's documented
concurrency limits.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import lark_oapi as lark
from lark_oapi.api.docx.v1 import (
    BatchDeleteDocumentBlockChildrenRequest,
    BatchDeleteDocumentBlockChildrenRequestBody,
    Block,
    CreateDocumentBlockChildrenRequest,
    CreateDocumentBlockChildrenRequestBody,
    GetDocumentBlockRequest,
    GetDocumentBlockResponse,
    ListDocumentBlockRequest,
    ListDocumentBlockResponse,
    PatchDocumentBlockRequest,
)


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

_MAX_OPS_PER_SECOND = 3


@dataclass
class _DocumentRateLimiter:
    """Token-bucket rate limiter scoped to a single document.

    Ensures at most ``_MAX_OPS_PER_SECOND`` write operations are
    dispatched per second per document.
    """

    semaphore: asyncio.Semaphore = field(
        default_factory=lambda: asyncio.Semaphore(_MAX_OPS_PER_SECOND)
    )
    timestamps: list[float] = field(default_factory=list)

    async def acquire(self) -> None:
        """Wait until a write slot is available."""
        await self.semaphore.acquire()
        now = time.monotonic()
        # Prune timestamps older than 1 second
        self.timestamps = [t for t in self.timestamps if now - t < 1.0]
        if len(self.timestamps) >= _MAX_OPS_PER_SECOND:
            # Wait until the oldest timestamp is at least 1s old
            sleep_for = 1.0 - (now - self.timestamps[0])
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
        self.timestamps.append(time.monotonic())

    def release(self) -> None:
        self.semaphore.release()


class WriteRateLimiter:
    """Per-document asyncio rate limiter for Lark write operations.

    Usage::

        limiter = WriteRateLimiter()
        async with limiter(document_id):
            # perform write operation
            ...
    """

    def __init__(self) -> None:
        self._limiters: dict[str, _DocumentRateLimiter] = defaultdict(
            _DocumentRateLimiter
        )

    def __call__(self, document_id: str) -> _RateLimitContext:
        return _RateLimitContext(self._limiters[document_id])


class _RateLimitContext:
    """Async context manager returned by ``WriteRateLimiter.__call__``."""

    def __init__(self, limiter: _DocumentRateLimiter) -> None:
        self._limiter = limiter

    async def __aenter__(self) -> None:
        await self._limiter.acquire()

    async def __aexit__(self, *exc: object) -> None:
        self._limiter.release()


# ---------------------------------------------------------------------------
# Blocks client
# ---------------------------------------------------------------------------


class BlocksClient:
    """Client for block-level operations on Lark documents.

    Args:
        client: A configured ``lark.Client`` instance.
    """

    def __init__(self, client: lark.Client) -> None:
        self._client = client
        self.rate_limiter = WriteRateLimiter()

    # ------------------------------------------------------------------
    # List blocks (paginated)
    # ------------------------------------------------------------------

    def list_blocks(
        self,
        document_id: str,
        *,
        page_size: int = 500,
        page_token: str | None = None,
    ) -> tuple[list[Block], str | None]:
        """List blocks in a document, returning one page.

        Args:
            document_id: Target document.
            page_size: Number of blocks per page (max 500).
            page_token: Pagination cursor from a previous call.

        Returns:
            A tuple of ``(blocks, next_page_token)``.  ``next_page_token``
            is ``None`` when there are no more pages.

        Raises:
            RuntimeError: If the API call fails.
        """
        builder = (
            ListDocumentBlockRequest.builder()
            .document_id(document_id)
            .page_size(page_size)
        )
        if page_token:
            builder = builder.page_token(page_token)

        request = builder.build()
        response: ListDocumentBlockResponse = (
            self._client.docx.v1.document_block.list(request)
        )
        self._check_response(response, f"list blocks for {document_id}")

        items: list[Block] = response.data.items or []
        next_token: str | None = response.data.page_token or None
        # The SDK may set has_more; prefer checking the token directly.
        if not response.data.has_more:
            next_token = None

        return items, next_token

    def list_all_blocks(self, document_id: str) -> list[Block]:
        """Convenience: iterate all pages and return every block.

        Args:
            document_id: Target document.

        Returns:
            A flat list of all ``Block`` objects in the document.
        """
        all_blocks: list[Block] = []
        page_token: str | None = None
        while True:
            blocks, page_token = self.list_blocks(
                document_id, page_token=page_token
            )
            all_blocks.extend(blocks)
            if page_token is None:
                break
        return all_blocks

    # ------------------------------------------------------------------
    # Get single block
    # ------------------------------------------------------------------

    def get_block(self, document_id: str, block_id: str) -> Block:
        """Get a single block by ID.

        Args:
            document_id: The document containing the block.
            block_id: The block to retrieve.

        Returns:
            The ``Block`` object.

        Raises:
            RuntimeError: If the API call fails.
        """
        request = (
            GetDocumentBlockRequest.builder()
            .document_id(document_id)
            .block_id(block_id)
            .build()
        )

        response: GetDocumentBlockResponse = (
            self._client.docx.v1.document_block.get(request)
        )
        self._check_response(response, f"get block {block_id} in {document_id}")

        return response.data.block

    # ------------------------------------------------------------------
    # Create children
    # ------------------------------------------------------------------

    def create_children(
        self,
        document_id: str,
        block_id: str,
        children: list[Block],
        *,
        index: int | None = None,
        document_revision_id: int | None = None,
    ) -> list[Block]:
        """Insert child blocks under a parent block.

        Args:
            document_id: Target document.
            block_id: Parent block to insert under.
            children: List of ``Block`` objects to insert.
            index: Optional insertion index among existing children.
            document_revision_id: Optional revision for optimistic locking.

        Returns:
            The list of created ``Block`` objects as returned by the API.

        Raises:
            RuntimeError: If the API call fails.
        """
        body_builder = (
            CreateDocumentBlockChildrenRequestBody.builder()
            .children(children)
        )
        if index is not None:
            body_builder = body_builder.index(index)

        req_builder = (
            CreateDocumentBlockChildrenRequest.builder()
            .document_id(document_id)
            .block_id(block_id)
            .request_body(body_builder.build())
        )
        if document_revision_id is not None:
            req_builder = req_builder.document_revision_id(document_revision_id)

        request = req_builder.build()
        response = self._client.docx.v1.document_block_children.create(request)
        self._check_response(
            response, f"create children under {block_id} in {document_id}"
        )

        return response.data.children or []

    # ------------------------------------------------------------------
    # Update (patch) block
    # ------------------------------------------------------------------

    def update_block(
        self,
        document_id: str,
        block_id: str,
        update_body: Any,
        *,
        document_revision_id: int | None = None,
    ) -> None:
        """Update (patch) a single block.

        Args:
            document_id: Target document.
            block_id: Block to update.
            update_body: The request body for the patch operation.
                Build with ``UpdateBlockRequest`` / appropriate body builder.
            document_revision_id: Optional revision for optimistic locking.

        Raises:
            RuntimeError: If the API call fails.
        """
        req_builder = (
            PatchDocumentBlockRequest.builder()
            .document_id(document_id)
            .block_id(block_id)
            .request_body(update_body)
        )
        if document_revision_id is not None:
            req_builder = req_builder.document_revision_id(document_revision_id)

        request = req_builder.build()
        response = self._client.docx.v1.document_block.patch(request)
        self._check_response(
            response, f"update block {block_id} in {document_id}"
        )

    # ------------------------------------------------------------------
    # Batch delete children
    # ------------------------------------------------------------------

    def batch_delete(
        self,
        document_id: str,
        block_id: str,
        start_index: int,
        end_index: int,
        *,
        document_revision_id: int | None = None,
    ) -> None:
        """Delete a contiguous range of child blocks.

        Args:
            document_id: Target document.
            block_id: Parent block whose children are being deleted.
            start_index: Start index (inclusive).
            end_index: End index (exclusive).
            document_revision_id: Optional revision for optimistic locking.

        Raises:
            RuntimeError: If the API call fails.
        """
        body = (
            BatchDeleteDocumentBlockChildrenRequestBody.builder()
            .start_index(start_index)
            .end_index(end_index)
            .build()
        )

        req_builder = (
            BatchDeleteDocumentBlockChildrenRequest.builder()
            .document_id(document_id)
            .block_id(block_id)
            .request_body(body)
        )
        if document_revision_id is not None:
            req_builder = req_builder.document_revision_id(document_revision_id)

        request = req_builder.build()
        response = self._client.docx.v1.document_block_children.batch_delete(request)
        self._check_response(
            response,
            f"batch delete children [{start_index}:{end_index}] "
            f"under {block_id} in {document_id}",
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _check_response(response: Any, operation: str) -> None:
        """Raise ``RuntimeError`` if the Lark API response indicates failure."""
        if not response.success():
            raise RuntimeError(
                f"Lark API error during '{operation}': "
                f"code={response.code}, msg={response.msg}, "
                f"log_id={response.get_log_id()}"
            )
