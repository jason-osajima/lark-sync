"""MCP server for bi-directional Lark <-> Markdown sync.

This is the main entry point. It creates a FastMCP server, initializes
the Lark client and sync engine, and registers all tools.

Run with:
    uv run lark-sync
    # or
    python -m lark_sync
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from lark_sync.config import settings
from lark_sync.converter import LarkToMarkdownConverter, MarkdownToLarkConverter
from lark_sync.lark_client import LarkClient
from lark_sync.sync.engine import SyncEngine
from lark_sync.sync.state import SyncStateManager
from lark_sync.tools.read_tools import register_read_tools
from lark_sync.tools.sync_tools import register_sync_tools
from lark_sync.tools.write_tools import register_write_tools

mcp = FastMCP(
    "lark-sync",
    instructions=(
        "Lark-Sync MCP server for bi-directional sync between "
        "Markdown files and Lark cloud documents. Use these tools "
        "to read, write, search, and sync Lark documents as Markdown."
    ),
)


def _initialize() -> None:
    """Initialize all components and register tools."""
    settings.validate()

    client = LarkClient()
    state_manager = SyncStateManager(settings.sync_state_file)
    lark_to_md = LarkToMarkdownConverter()
    md_to_lark = MarkdownToLarkConverter()

    engine = SyncEngine(
        lark_client=client,
        state_manager=state_manager,
        lark_to_md_converter=lark_to_md,
        md_to_lark_converter=md_to_lark,
    )

    register_read_tools(mcp, client)
    register_write_tools(mcp, client, engine)
    register_sync_tools(mcp, engine)


def main() -> None:
    """Entry point for the MCP server."""
    _initialize()
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
