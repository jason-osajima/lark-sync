"""MCP tools for syncing from Lark and checking sync status."""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from lark_sync.sync.engine import SyncEngine


def register_sync_tools(mcp: FastMCP, engine: SyncEngine) -> None:
    """Register sync-from-lark and status tools with the MCP server."""

    @mcp.tool()
    def sync_from_lark(
        document_id: str,
        local_path: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """Pull a Lark cloud document to a local Markdown file.

        If local_path is not specified, derives filename from document title.
        Detects conflicts and warns unless force=True.

        Args:
            document_id: Lark document ID or URL to pull.
            local_path: Local file path to save. If None, auto-generated.
            force: If True, overwrite local even if conflicts detected.
        """
        result = engine.sync_from_lark(
            document_id=document_id,
            local_path=local_path,
            force=force,
        )
        return result.model_dump()

    @mcp.tool()
    def get_sync_status(local_path: str | None = None) -> list[dict[str, Any]]:
        """Check sync status between local files and Lark documents.

        If local_path provided, checks status for that file only.
        If not provided, checks all tracked sync mappings.

        Args:
            local_path: Optional path to check specific file status.
        """
        entries = engine.get_sync_status(local_path=local_path)
        return [
            {
                "local_path": e.local_path,
                "document_id": e.document_id,
                "document_url": e.document_url,
                "status": e.status.value,
                "last_synced": e.last_synced.isoformat() if e.last_synced else None,
            }
            for e in entries
        ]
