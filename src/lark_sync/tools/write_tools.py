"""MCP tools for writing and syncing to Lark documents."""

from __future__ import annotations

import asyncio
from typing import Any

from mcp.server.fastmcp import FastMCP

from lark_sync.converter import LarkToMarkdownConverter, MarkdownToLarkConverter
from lark_sync.lark_client import LarkClient
from lark_sync.sync.engine import SyncEngine
from lark_sync.sync.state import SyncStateManager


def register_write_tools(
    mcp: FastMCP,
    client: LarkClient,
    engine: SyncEngine,
) -> None:
    """Register write and sync-to-lark tools with the MCP server."""

    md_to_lark = MarkdownToLarkConverter()

    @mcp.tool()
    def write_document(
        content: str,
        document_id: str | None = None,
        title: str | None = None,
        folder_token: str | None = None,
    ) -> dict[str, str]:
        """Create or update a Lark cloud document from Markdown content.

        If document_id is provided, updates the existing document (replaces all content).
        If not provided, creates a new document.

        Args:
            content: Markdown content to write to the document.
            document_id: ID of existing document to update. If None, creates new.
            title: Title for a new document. Ignored if updating.
            folder_token: Lark Drive folder token for new document placement.
        """
        blocks = md_to_lark.convert(content)

        if document_id:
            # Clear existing content and recreate
            existing = client.blocks.list_all_blocks(document_id)
            child_ids = [
                _get_block_id(b)
                for b in existing
                if _get_block_id(b) != document_id
                and _get_parent_id(b) == document_id
            ]
            if child_ids:
                client.blocks.batch_delete(document_id, document_id, 0, len(child_ids))

            if blocks:
                client.blocks.create_children(document_id, document_id, blocks)

            doc = client.documents.get(document_id)
            return {
                "document_id": doc.document_id,
                "title": doc.title,
                "action": "updated",
            }
        else:
            doc_title = title or "Untitled Document"
            doc = client.documents.create(doc_title, folder_token)
            if blocks:
                client.blocks.create_children(doc.document_id, doc.document_id, blocks)

            return {
                "document_id": doc.document_id,
                "title": doc.title,
                "action": "created",
            }

    @mcp.tool()
    def sync_to_lark(
        local_path: str,
        document_id: str | None = None,
        folder_token: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """Push a local Markdown file to a Lark cloud document.

        If the file has been synced before, uses the existing mapping.
        Detects conflicts (both sides changed) and warns unless force=True.

        Args:
            local_path: Path to the local .md file.
            document_id: Target Lark document ID. Optional.
            folder_token: Lark Drive folder for new documents.
            force: If True, overwrite remote even if conflicts detected.
        """
        result = asyncio.run(
            engine.sync_to_lark(
                local_path=local_path,
                document_id=document_id,
                folder_token=folder_token,
                force=force,
            )
        )
        return result.model_dump()


def _get_block_id(block: Any) -> str:
    """Extract block_id from a block object or dict."""
    if isinstance(block, dict):
        return block.get("block_id", "")
    return getattr(block, "block_id", "")


def _get_parent_id(block: Any) -> str:
    """Extract parent_id from a block object or dict."""
    if isinstance(block, dict):
        return block.get("parent_id", "")
    return getattr(block, "parent_id", "")
