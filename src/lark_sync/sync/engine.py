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

    Args:
        lark_client: A ``LarkClient`` instance for API calls.
        state_manager: Manages sync state persistence.
        lark_to_md_converter: Converts Lark block trees to Markdown.
        md_to_lark_converter: Converts Markdown text to Lark blocks.
    """

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
        mapping = self._state.get_mapping(local_path)

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
                self._client.blocks.create_children(
                    document_id, document_id, lark_blocks
                )
            doc_info = self._client.documents.get(document_id)
            new_revision: int = doc_info.revision_id
        else:
            title = path.stem.replace("-", " ").replace("_", " ").title()
            doc_response = self._client.documents.create(title, folder_token)
            document_id = doc_response.document_id
            if lark_blocks:
                self._client.blocks.create_children(
                    document_id, document_id, lark_blocks
                )
            doc_info = self._client.documents.get(document_id)
            new_revision = doc_info.revision_id
            document_url = ""

        # Update sync state
        now = datetime.now(timezone.utc)
        current_hash = compute_file_hash(local_path)

        if mapping is not None:
            self._state.update_mapping(
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
            self._state.add_mapping(new_mapping)

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

    def sync_from_lark(
        self,
        document_id: str,
        local_path: str | None = None,
        force: bool = False,
    ) -> SyncResult:
        """Pull a Lark document and write it as a local Markdown file."""
        mapping = self._state.get_mapping_by_doc_id(document_id)

        if local_path is None and mapping is not None:
            local_path = mapping.local_path

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

        # Update sync state
        now = datetime.now(timezone.utc)
        current_hash = compute_file_hash(local_path)

        if mapping is not None:
            self._state.update_mapping(
                mapping.local_path,
                lark_document_url=document_url or mapping.lark_document_url,
                last_synced_at=now,
                local_hash_at_sync=current_hash,
                remote_revision_at_sync=current_revision,
            )
        else:
            new_mapping = SyncMapping(
                local_path=local_path,
                lark_document_id=document_id,
                lark_document_url=document_url,
                last_synced_at=now,
                local_hash_at_sync=current_hash,
                remote_revision_at_sync=current_revision,
                sync_direction=SyncDirection.FROM_LARK,
            )
            self._state.add_mapping(new_mapping)

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
        """Return the sync status of all tracked mappings."""
        state = self._state.load()
        mappings = state.mappings

        if local_path is not None:
            mappings = [m for m in mappings if m.local_path == local_path]

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
