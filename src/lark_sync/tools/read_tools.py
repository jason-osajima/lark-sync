"""MCP tools for reading Lark documents."""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from lark_sync.converter import LarkToMarkdownConverter
from lark_sync.lark_client import LarkClient


def register_read_tools(mcp: FastMCP, client: LarkClient) -> None:
    """Register read-only tools with the MCP server."""

    converter = LarkToMarkdownConverter()

    @mcp.tool()
    def read_document(document_id: str) -> str:
        """Read a Lark cloud document and return its content as Markdown.

        Args:
            document_id: The Lark document ID (e.g. 'doxcnXYZ123').
        """
        blocks = client.blocks.list_all_blocks(document_id)
        block_dicts = [_block_to_dict(b) for b in blocks]
        return converter.convert(block_dicts)

    @mcp.tool()
    def list_documents(
        folder_token: str | None = None,
        wiki_space_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """List documents in a Lark Drive folder or Wiki space.

        Provide folder_token for Drive folders, or wiki_space_id for Wiki spaces.
        If neither is provided, lists wiki spaces available.

        Args:
            folder_token: Lark Drive folder token.
            wiki_space_id: Wiki space ID to list nodes from.
        """
        if wiki_space_id:
            nodes = client.wiki.list_all_nodes(wiki_space_id)
            return [
                {
                    "node_token": n.node_token,
                    "obj_token": n.obj_token,
                    "title": n.title,
                    "obj_type": n.obj_type,
                    "has_child": n.has_child,
                }
                for n in nodes
            ]
        if folder_token:
            files = client.drive.list_all_files(folder_token)
            return [
                {
                    "token": f.token,
                    "name": f.name,
                    "type": f.type,
                    "url": f.url,
                }
                for f in files
            ]
        # Default: list wiki spaces
        spaces = client.wiki.list_all_spaces()
        return [
            {
                "space_id": s.space_id,
                "name": s.name,
                "description": s.description,
            }
            for s in spaces
        ]

    @mcp.tool()
    def search_documents(query: str, count: int = 20) -> list[dict[str, str]]:
        """Search Lark documents by keyword.

        Args:
            query: Search query string.
            count: Maximum number of results (default 20, max 50).
        """
        results = client.search.search_all(query, max_results=min(count, 50))
        return [
            {
                "doc_id": r.doc_id,
                "title": r.title,
                "url": r.url,
                "doc_type": r.doc_type,
            }
            for r in results
        ]


def _block_to_dict(block: Any) -> dict[str, Any]:
    """Recursively convert a lark_oapi Block object to a plain dict."""
    if isinstance(block, dict):
        return {k: _convert_value(v) for k, v in block.items()}
    if hasattr(block, "__dict__"):
        d: dict[str, Any] = {}
        for key, value in block.__dict__.items():
            if key.startswith("_"):
                continue
            d[key] = _convert_value(value)
        return d
    return {"raw": str(block)}


def _convert_value(value: Any) -> Any:
    """Recursively convert SDK objects, lists, and dicts to plain types."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_convert_value(item) for item in value]
    if isinstance(value, dict):
        return {k: _convert_value(v) for k, v in value.items()}
    if hasattr(value, "__dict__"):
        return _block_to_dict(value)
    return value
