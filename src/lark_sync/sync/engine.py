"""Sync engine orchestrator for bidirectional Lark <-> Markdown sync.

Coordinates the entire sync workflow: reading local files, fetching
remote blocks, converting between formats, detecting conflicts, and
updating the sync state.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from lark_sync.sync.conflict import ConflictDetector, ConflictType
from lark_sync.sync.differ import SyncDiffer
from lark_sync.sync.state import (
    SyncDirection,
    SyncMapping,
    SyncStateManager,
    compute_file_hash,
)
from lark_oapi.api.docx.v1 import (
    BatchUpdateDocumentBlockRequest,
    BatchUpdateDocumentBlockRequestBody,
    InsertTableRowRequest,
    PatchDocumentBlockRequest,
    TextElement,
    TextRun,
    TextElementStyle,
    UpdateBlockRequest,
    UpdateTablePropertyRequest,
    UpdateTextElementsRequest,
)

from lark_sync.converter.block_types import BlockType
from lark_sync.converter.text_elements import parse_inline_markdown
from lark_sync.tools.read_tools import _block_to_dict

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Result / Status models
# ------------------------------------------------------------------


class SyncStatusLabel(StrEnum):
    """Human-readable label for a mapping's sync status."""

    IN_SYNC = "in_sync"
    LOCAL_AHEAD = "local_ahead"
    REMOTE_AHEAD = "remote_ahead"
    CONFLICT = "conflict"
    UNLINKED = "unlinked"


class SyncResult(BaseModel):
    """Outcome of a single sync operation."""

    success: bool
    message: str
    document_id: str = ""
    document_url: str = ""
    local_path: str = ""
    conflict: ConflictType = ConflictType.NONE
    diff_summary: str = ""


class SyncStatusEntry(BaseModel):
    """Status snapshot for one tracked mapping."""

    local_path: str
    document_id: str
    document_url: str = ""
    status: SyncStatusLabel
    last_synced: datetime | None = None


# ------------------------------------------------------------------
# Engine
# ------------------------------------------------------------------


class SyncEngine:
    """Orchestrates bidirectional sync between local Markdown files and
    Lark cloud documents.

    Automatically detects project-local ``.lark-sync.json`` files at the
    Git repository root for any synced file.  Falls back to the global
    state file when no project-local state exists.

    Args:
        lark_client: A ``LarkClient`` instance for API calls.
        state_manager: Global state manager (fallback).
        lark_to_md_converter: Converts Lark block trees to Markdown.
        md_to_lark_converter: Converts Markdown text to Lark blocks.
    """

    PROJECT_STATE_FILENAME = ".lark-sync.json"

    def __init__(
        self,
        lark_client: Any,
        state_manager: SyncStateManager,
        lark_to_md_converter: Any,
        md_to_lark_converter: Any,
    ) -> None:
        self._client = lark_client
        self._state = state_manager
        self._to_md = lark_to_md_converter
        self._to_lark = md_to_lark_converter
        self._conflict_detector = ConflictDetector()
        self._differ = SyncDiffer()
        self._project_states: dict[Path, SyncStateManager] = {}

    # ------------------------------------------------------------------
    # Project-local state detection
    # ------------------------------------------------------------------

    @staticmethod
    def _find_git_root(file_path: str) -> Path | None:
        """Walk up from *file_path* to find the Git repository root."""
        current = Path(file_path).resolve()
        if current.is_file():
            current = current.parent
        for parent in [current, *current.parents]:
            if (parent / ".git").exists():
                return parent
        return None

    def _get_state_manager(self, local_path: str) -> SyncStateManager:
        """Return the appropriate state manager for *local_path*.

        If the file lives inside a Git repo that has a
        ``.lark-sync.json``, a project-local ``SyncStateManager`` is
        returned (cached per project root).  Otherwise the global
        state manager is used.
        """
        git_root = self._find_git_root(local_path)
        if git_root is None:
            return self._state

        project_state_file = git_root / self.PROJECT_STATE_FILENAME
        if not project_state_file.exists():
            return self._state

        if git_root not in self._project_states:
            self._project_states[git_root] = SyncStateManager(
                str(project_state_file), project_root=git_root
            )
        return self._project_states[git_root]

    # ------------------------------------------------------------------
    # Push: local Markdown -> Lark
    # ------------------------------------------------------------------

    def sync_to_lark(
        self,
        local_path: str,
        document_id: str | None = None,
        folder_token: str | None = None,
        wiki_space_id: str | None = None,
        force: bool = False,
    ) -> SyncResult:
        """Push a local Markdown file to a Lark document.

        If ``document_id`` is provided the existing document is updated
        (all child blocks are cleared and recreated).  Otherwise a new
        document is created in the specified ``folder_token``.
        """
        path = Path(local_path)
        if not path.exists():
            return SyncResult(
                success=False,
                message=f"Local file not found: {local_path}",
                local_path=local_path,
            )

        markdown_content = path.read_text(encoding="utf-8")
        state_mgr = self._get_state_manager(local_path)
        mapping = state_mgr.get_mapping(local_path)

        if document_id is None and mapping is not None:
            document_id = mapping.lark_document_id

        # Conflict detection
        if not force and mapping is not None and document_id is not None:
            try:
                doc_info = self._client.documents.get(document_id)
                current_revision: int = doc_info.revision_id
                conflict = self._conflict_detector.detect(mapping, current_revision)
                if conflict == ConflictType.BOTH_CHANGED:
                    return SyncResult(
                        success=False,
                        message=(
                            "Conflict: both local and remote have changed since "
                            "the last sync. Use force=True to overwrite."
                        ),
                        document_id=document_id,
                        local_path=local_path,
                        conflict=conflict,
                    )
            except Exception as exc:
                logger.warning("Failed conflict check: %s", exc)

        # Convert Markdown -> Lark blocks
        lark_blocks = self._to_lark.convert(markdown_content)

        # Create or update the remote document
        document_url = ""

        if document_id is not None:
            self._clear_document_blocks(document_id)
            if lark_blocks:
                self._create_blocks_with_nesting(
                    document_id, document_id, lark_blocks
                )
            doc_info = self._client.documents.get(document_id)
            new_revision: int = doc_info.revision_id
        else:
            title = path.stem.replace("-", " ").replace("_", " ").title()
            doc_response = self._client.documents.create(title, folder_token)
            document_id = doc_response.document_id
            if lark_blocks:
                self._create_blocks_with_nesting(
                    document_id, document_id, lark_blocks
                )
            doc_info = self._client.documents.get(document_id)
            new_revision = doc_info.revision_id
            document_url = ""

        # Update sync state
        now = datetime.now(timezone.utc)
        current_hash = compute_file_hash(local_path)

        if mapping is not None:
            state_mgr.update_mapping(
                local_path,
                lark_document_id=document_id,
                lark_document_url=document_url or mapping.lark_document_url,
                lark_wiki_space_id=wiki_space_id or mapping.lark_wiki_space_id,
                last_synced_at=now,
                local_hash_at_sync=current_hash,
                remote_revision_at_sync=new_revision,
            )
        else:
            new_mapping = SyncMapping(
                local_path=local_path,
                lark_document_id=document_id,
                lark_document_url=document_url,
                lark_wiki_space_id=wiki_space_id,
                last_synced_at=now,
                local_hash_at_sync=current_hash,
                remote_revision_at_sync=new_revision,
                sync_direction=SyncDirection.TO_LARK,
            )
            state_mgr.add_mapping(new_mapping)

        logger.info(
            "Synced local file %s -> Lark document %s (rev %d)",
            local_path, document_id, new_revision,
        )

        return SyncResult(
            success=True,
            message=f"Successfully synced to Lark document {document_id}",
            document_id=document_id,
            document_url=document_url,
            local_path=local_path,
        )

    # ------------------------------------------------------------------
    # Pull: Lark -> local Markdown
    # ------------------------------------------------------------------

    def _find_mapping_by_doc_id(
        self, document_id: str
    ) -> tuple[SyncMapping | None, SyncStateManager]:
        """Search all known state managers for a mapping by document ID.

        Returns the mapping and the state manager that owns it.  The global
        state is checked first, then any cached project-local states.
        """
        mapping = self._state.get_mapping_by_doc_id(document_id)
        if mapping is not None:
            return mapping, self._state
        for mgr in self._project_states.values():
            mapping = mgr.get_mapping_by_doc_id(document_id)
            if mapping is not None:
                return mapping, mgr
        return None, self._state

    def sync_from_lark(
        self,
        document_id: str,
        local_path: str | None = None,
        force: bool = False,
    ) -> SyncResult:
        """Pull a Lark document and write it as a local Markdown file."""
        mapping, state_mgr = self._find_mapping_by_doc_id(document_id)

        if local_path is None and mapping is not None:
            local_path = mapping.local_path
            # If the stored path is relative and we have a project root, resolve it.
            if state_mgr.project_root and not Path(local_path).is_absolute():
                local_path = state_mgr.resolve_path(local_path)

        # Fetch remote document metadata
        try:
            doc_info = self._client.documents.get(document_id)
        except Exception as exc:
            return SyncResult(
                success=False,
                message=f"Failed to fetch document {document_id}: {exc}",
                document_id=document_id,
                local_path=local_path or "",
            )

        current_revision: int = doc_info.revision_id
        document_title: str = doc_info.title
        document_url: str = ""

        if local_path is None:
            safe_name = (
                document_title.lower()
                .replace(" ", "-")
                .replace("/", "-")
            )
            local_path = f"{safe_name}.md"

        # Conflict detection
        if not force and mapping is not None:
            conflict = self._conflict_detector.detect(mapping, current_revision)
            if conflict == ConflictType.BOTH_CHANGED:
                return SyncResult(
                    success=False,
                    message=(
                        "Conflict: both local and remote have changed since "
                        "the last sync. Use force=True to overwrite."
                    ),
                    document_id=document_id,
                    local_path=local_path,
                    conflict=conflict,
                )

        # Fetch blocks and convert to Markdown
        try:
            raw_blocks = self._client.blocks.list_all_blocks(document_id)
            blocks = [_block_to_dict(b) for b in raw_blocks]
        except Exception as exc:
            return SyncResult(
                success=False,
                message=f"Failed to fetch blocks for {document_id}: {exc}",
                document_id=document_id,
                local_path=local_path,
            )

        markdown_content = self._to_md.convert(blocks)

        # Compute diff summary if we have previous local content.
        diff_summary = ""
        path = Path(local_path)
        if path.exists():
            old_content = path.read_text(encoding="utf-8")
            diff_summary = self._differ.compute_diff(old_content, markdown_content)

        # Write to local file
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(markdown_content, encoding="utf-8")

        # Update sync state — use the state manager that owns the mapping,
        # or resolve one from the local_path for new mappings.
        now = datetime.now(timezone.utc)
        current_hash = compute_file_hash(local_path)

        if mapping is not None:
            state_mgr.update_mapping(
                mapping.local_path,
                lark_document_url=document_url or mapping.lark_document_url,
                last_synced_at=now,
                local_hash_at_sync=current_hash,
                remote_revision_at_sync=current_revision,
            )
        else:
            # For new mappings, pick the right state manager from the local path.
            state_mgr = self._get_state_manager(local_path)
            new_mapping = SyncMapping(
                local_path=local_path,
                lark_document_id=document_id,
                lark_document_url=document_url,
                last_synced_at=now,
                local_hash_at_sync=current_hash,
                remote_revision_at_sync=current_revision,
                sync_direction=SyncDirection.FROM_LARK,
            )
            state_mgr.add_mapping(new_mapping)

        logger.info(
            "Synced Lark document %s -> local file %s (rev %d)",
            document_id, local_path, current_revision,
        )

        return SyncResult(
            success=True,
            message=f"Successfully synced from Lark document {document_id}",
            document_id=document_id,
            document_url=document_url,
            local_path=local_path,
            diff_summary=diff_summary,
        )

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def get_sync_status(
        self, local_path: str | None = None
    ) -> list[SyncStatusEntry]:
        """Return the sync status of all tracked mappings.

        When *local_path* is given, the appropriate project-local or global
        state manager is consulted.  Otherwise, all known state managers
        (global + cached project states) are queried.
        """
        if local_path is not None:
            state_mgr = self._get_state_manager(local_path)
            state = state_mgr.load()
            mappings = [
                m for m in state.mappings
                if m.local_path == local_path
                or m.local_path == state_mgr._normalize_path(local_path)
            ]
        else:
            # Gather mappings from all known state managers.
            mappings: list[SyncMapping] = []
            seen_doc_ids: set[str] = set()
            for mgr in [self._state, *self._project_states.values()]:
                state = mgr.load()
                for m in state.mappings:
                    if m.lark_document_id not in seen_doc_ids:
                        mappings.append(m)
                        seen_doc_ids.add(m.lark_document_id)

        entries: list[SyncStatusEntry] = []

        for mapping in mappings:
            status = self._compute_status(mapping)
            entries.append(
                SyncStatusEntry(
                    local_path=mapping.local_path,
                    document_id=mapping.lark_document_id,
                    document_url=mapping.lark_document_url,
                    status=status,
                    last_synced=mapping.last_synced_at,
                )
            )

        return entries

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _compute_status(self, mapping: SyncMapping) -> SyncStatusLabel:
        """Determine the current sync status label for a mapping."""
        try:
            doc_info = self._client.documents.get(mapping.lark_document_id)
            current_revision: int = doc_info.revision_id
        except Exception:
            return SyncStatusLabel.UNLINKED

        conflict = self._conflict_detector.detect(mapping, current_revision)

        status_map: dict[ConflictType, SyncStatusLabel] = {
            ConflictType.NONE: SyncStatusLabel.IN_SYNC,
            ConflictType.LOCAL_ONLY: SyncStatusLabel.LOCAL_AHEAD,
            ConflictType.REMOTE_ONLY: SyncStatusLabel.REMOTE_AHEAD,
            ConflictType.BOTH_CHANGED: SyncStatusLabel.CONFLICT,
        }

        return status_map.get(conflict, SyncStatusLabel.UNLINKED)

    def _create_blocks_with_nesting(
        self,
        document_id: str,
        parent_block_id: str,
        blocks: list[dict[str, Any]],
    ) -> None:
        """Create blocks under a parent, handling nested children.

        The Lark API rejects ``children`` in the create payload.  Container
        blocks are handled specially:

        - **TABLE**: created empty (API auto-generates cells with TEXT
          children), then each cell's TEXT block is updated via batch update.
        - **QUOTE_CONTAINER**: created empty, then child blocks are added
          via ``create_children`` on the container.

        Flat (non-container) blocks are batched together for efficiency.
        """
        flat_batch: list[dict[str, Any]] = []

        for block in blocks:
            children = block.get("children")
            if not children:
                flat_batch.append(block)
                continue

            # Flush any pending flat batch before handling a container.
            if flat_batch:
                self._client.blocks.create_children(
                    document_id, parent_block_id, flat_batch
                )
                flat_batch = []

            bt = BlockType.from_value(block.get("block_type", 0))

            if bt == BlockType.TABLE:
                self._create_table_block(document_id, parent_block_id, block)
            else:
                # Generic container: create without children, then add children.
                container = {k: v for k, v in block.items() if k != "children"}
                created = self._client.blocks.create_children(
                    document_id, parent_block_id, [container]
                )
                if created:
                    created_id = getattr(created[0], "block_id", None)
                    if created_id:
                        self._create_blocks_with_nesting(
                            document_id, created_id, children
                        )

        # Flush any remaining flat batch.
        if flat_batch:
            self._client.blocks.create_children(
                document_id, parent_block_id, flat_batch
            )

    # Maximum rows the Lark API allows in a single table creation call.
    _MAX_TABLE_ROWS = 9
    # Approximate page content width in pixels for full-width tables.
    _TABLE_PAGE_WIDTH = 686

    def _create_table_block(
        self,
        document_id: str,
        parent_block_id: str,
        table_block: dict[str, Any],
    ) -> None:
        """Create a TABLE block and populate its cells.

        The Lark API auto-creates TABLE_CELL blocks (each with an empty TEXT
        child) when a TABLE is created.  We then batch-update the TEXT blocks
        to set the actual cell content.

        Tables exceeding 9 rows are created with 9 rows initially, then
        additional rows are appended via ``insert_table_row``.
        """
        children = table_block.get("children") or []
        table_body = table_block.get("table") or {}
        prop = table_body.get("property") or {}
        total_rows: int = prop.get("row_size", 0)
        col_count: int = prop.get("column_size", 0)

        if total_rows == 0 or col_count == 0:
            return

        # 1. Create the TABLE block (capped at 9 rows).
        initial_rows = min(total_rows, self._MAX_TABLE_ROWS)
        create_block = {
            "block_type": table_block["block_type"],
            "table": {
                "property": {
                    "row_size": initial_rows,
                    "column_size": col_count,
                },
            },
        }
        created = self._client.blocks.create_children(
            document_id, parent_block_id, [create_block]
        )
        if not created:
            return

        table_obj = created[0]
        table_id = getattr(table_obj, "block_id", None)
        if not table_id:
            return

        # 2. Insert additional rows if the table exceeds 9 rows.
        for row_idx in range(initial_rows, total_rows):
            insert_row = (
                InsertTableRowRequest.builder().row_index(row_idx).build()
            )
            update_body = (
                UpdateBlockRequest.builder()
                .block_id(table_id)
                .insert_table_row(insert_row)
                .build()
            )
            request = (
                PatchDocumentBlockRequest.builder()
                .document_id(document_id)
                .block_id(table_id)
                .request_body(update_body)
                .build()
            )
            response = self._client.raw.docx.v1.document_block.patch(request)
            if not response.success():
                logger.warning(
                    "Failed to insert table row %d: code=%s, msg=%s",
                    row_idx,
                    response.code,
                    response.msg,
                )

        # 3. Set column widths to fill the page (~686px).
        col_width = self._TABLE_PAGE_WIDTH // col_count
        for col_idx in range(col_count):
            update_table = (
                UpdateTablePropertyRequest.builder()
                .column_index(col_idx)
                .column_width(col_width)
                .build()
            )
            update_body = (
                UpdateBlockRequest.builder()
                .block_id(table_id)
                .update_table_property(update_table)
                .build()
            )
            request = (
                PatchDocumentBlockRequest.builder()
                .document_id(document_id)
                .block_id(table_id)
                .request_body(update_body)
                .build()
            )
            self._client.raw.docx.v1.document_block.patch(request)

        # 4. Refresh the table block to get all cell IDs (including new rows).
        table_obj = self._client.blocks.get_block(document_id, table_id)
        cell_ids: list[str] = getattr(table_obj, "children", None) or []

        if not cell_ids:
            return

        # 4. Get the TEXT child block ID for each cell.
        cell_text_ids: list[str] = []
        for cid in cell_ids:
            cell = self._client.blocks.get_block(document_id, cid)
            cell_children = getattr(cell, "children", None) or []
            cell_text_ids.append(cell_children[0] if cell_children else "")

        # 5. Build batch update requests to populate cell content.
        updates: list[Any] = []
        for i, text_block_id in enumerate(cell_text_ids):
            if not text_block_id:
                continue

            cell_text = self._extract_cell_text(children, i)
            if not cell_text:
                continue

            style = TextElementStyle.builder().build()
            text_run = (
                TextRun.builder()
                .content(cell_text)
                .text_element_style(style)
                .build()
            )
            element = TextElement.builder().text_run(text_run).build()
            update_elements = (
                UpdateTextElementsRequest.builder().elements([element]).build()
            )
            update = (
                UpdateBlockRequest.builder()
                .block_id(text_block_id)
                .update_text_elements(update_elements)
                .build()
            )
            updates.append(update)

        # 6. Execute batch update.
        if updates:
            body = (
                BatchUpdateDocumentBlockRequestBody.builder()
                .requests(updates)
                .build()
            )
            request = (
                BatchUpdateDocumentBlockRequest.builder()
                .document_id(document_id)
                .request_body(body)
                .build()
            )
            response = self._client.raw.docx.v1.document_block.batch_update(
                request
            )
            if not response.success():
                logger.warning(
                    "Failed to batch-update table cells: code=%s, msg=%s",
                    response.code,
                    response.msg,
                )

    @staticmethod
    def _extract_cell_text(
        children: list[dict[str, Any]], index: int
    ) -> str:
        """Extract plain text content from a converter TABLE_CELL child."""
        if index >= len(children):
            return ""
        cell_dict = children[index]
        cell_children = cell_dict.get("children") or []
        if not cell_children:
            return ""
        text_dict = cell_children[0]
        text_body = text_dict.get("text") or {}
        elements = text_body.get("elements") or []
        parts = []
        for el in elements:
            tr = el.get("text_run") or {}
            parts.append(tr.get("content", ""))
        return "".join(parts)

    def _clear_document_blocks(self, document_id: str) -> None:
        """Remove all child blocks from a document's root page block."""
        raw_blocks = self._client.blocks.list_all_blocks(document_id)

        child_count = 0
        for b in raw_blocks:
            bid = getattr(b, "block_id", None) or (b.get("block_id") if isinstance(b, dict) else None)
            pid = getattr(b, "parent_id", None) or (b.get("parent_id") if isinstance(b, dict) else None)
            if bid != document_id and pid == document_id:
                child_count += 1

        if child_count > 0:
            self._client.blocks.batch_delete(
                document_id, document_id, 0, child_count
            )
