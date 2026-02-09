"""MCP tools for syncing from Lark and checking sync status."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

from lark_sync.sync.engine import SyncEngine
from lark_sync.sync.state import SyncState, SyncStateManager


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

    @mcp.tool()
    def init_project_sync(project_path: str) -> dict[str, Any]:
        """Initialize a project directory for Lark sync.

        Creates a .lark-sync.json file at the Git repository root for
        the given path.  If the global state already has mappings for
        files in this repo, they are migrated into the project-local
        state (converted to relative POSIX paths).

        Args:
            project_path: Path to a directory inside the Git repository.
        """
        git_root = engine._find_git_root(project_path)
        if git_root is None:
            return {
                "success": False,
                "message": f"No Git repository found for: {project_path}",
            }

        state_file = git_root / engine.PROJECT_STATE_FILENAME
        if state_file.exists():
            return {
                "success": False,
                "message": f"Project already initialized: {state_file}",
            }

        # Create empty project state.
        project_mgr = SyncStateManager(str(state_file), project_root=git_root)
        project_state = SyncState()

        # Migrate matching mappings from global state.
        global_state = engine._state.load()
        migrated = 0
        for mapping in global_state.mappings:
            # Check if this mapping's file lives inside the project.
            mp = Path(mapping.local_path)
            try:
                rel = mp.relative_to(git_root).as_posix()
            except ValueError:
                continue

            # Clone mapping with relative path.
            cloned = mapping.model_copy(update={"local_path": rel})
            project_state.mappings.append(cloned)
            migrated += 1

        project_mgr.save(project_state)

        # Cache the new project state manager in the engine.
        engine._project_states[git_root] = project_mgr

        return {
            "success": True,
            "state_file": str(state_file),
            "migrated_mappings": migrated,
            "message": (
                f"Initialized {state_file} with {migrated} migrated mapping(s)."
            ),
        }
