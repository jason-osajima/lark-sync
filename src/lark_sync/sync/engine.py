"""Sync engine orchestrator for bidirectional Lark <-> Markdown sync.

Coordinates the entire sync workflow: reading local files, fetching
remote blocks, converting between formats, detecting conflicts, and
updating the sync state.

The engine depends on three collaborators injected at construction time:

- **lark_client** -- a ``LarkClient`` instance for API calls.
- **lark_to_md_converter** -- converts Lark blocks to Markdown text.
- **md_to_lark_converter** -- converts Markdown text to Lark blocks.

These are referenced via structural typing (duck typing with type
hints) so that the engine module does not create hard import-time
dependencies on modules that may not be built yet.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import BaseModel, Field

from lark_sync.sync.conflict import ConflictDetector, ConflictType
from lark_sync.sync.differ import SyncDiffer
from lark_sync.sync.state import (
    SyncDirection,
    SyncMapping,
    SyncStateManager,
    compute_content_hash,
    compute_file_hash,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Protocols for collaborators (structural typing)
# ------------------------------------------------------------------


class DocumentsAPI(Protocol):
    """Structural interface for the Lark documents sub-client."""

    async def create_document(
        self, title: str, folder_token: str | None = None
    ) -> Any: ...

    async def get_document(self, document_id: str) -> Any: ...


class BlocksAPI(Protocol):
    """Structural interface for the Lark blocks sub-client."""

    async def list_blocks(self, document_id: str) -> list[dict[str, Any]]: ...

    async def create_children(
        self,
        document_id: str,
        parent_block_id: str,
        blocks: list[dict[str, Any]],
    ) -> list[Any]: ...

    async def batch_delete(
        self, document_id: str, block_ids: list[str]
    ) -> bool: ...


class LarkClientProtocol(Protocol):
    """Structural interface for the Lark API client."""

    @property
    def documents(self) -> DocumentsAPI: ...

    @property
    def blocks(self) -> BlocksAPI: ...


class LarkToMdConverterProtocol(Protocol):
    """Structural interface for the Lark-blocks-to-Markdown converter."""

    def convert(self, blocks: list[dict[str, Any]]) -> str: ...


class MdToLarkConverterProtocol(Protocol):
    """Structural interface for the Markdown-to-Lark-blocks converter."""

    def convert(self, markdown_text: str) -> list[dict[str, Any]]: ...


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
        lark_client: Client for interacting with the Lark Open API.
        state_manager: Manages sync state persistence.
        lark_to_md_converter: Converts Lark block trees to Markdown.
        md_to_lark_converter: Converts Markdown text to Lark blocks.
    """

    def __init__(
        self,
        lark_client: LarkClientProtocol,
        state_manager: SyncStateManager,
        lark_to_md_converter: LarkToMdConverterProtocol,
        md_to_lark_converter: MdToLarkConverterProtocol,
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

    async def sync_to_lark(
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

        Args:
            local_path: Path to the local Markdown file.
            document_id: Optional existing Lark document to update.
            folder_token: Folder token for new document creation.
            wiki_space_id: Optional wiki space ID for the mapping.
            force: Skip conflict detection when ``True``.

        Returns:
            A ``SyncResult`` describing the outcome.
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

        # Resolve document_id from existing mapping if not explicitly given.
        if document_id is None and mapping is not None:
            document_id = mapping.lark_document_id

        # ----------------------------------------------------------
        # Conflict detection
        # ----------------------------------------------------------
        if not force and mapping is not None and document_id is not None:
            try:
                doc_info = await self._client.documents.get_document(document_id)
                current_revision: int = getattr(doc_info, "revision", 0)
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
                logger.warning(
                    "Failed to fetch document for conflict check: %s", exc
                )

        # ----------------------------------------------------------
        # Convert Markdown -> Lark blocks
        # ----------------------------------------------------------
        lark_blocks = self._to_lark.convert(markdown_content)

        # ----------------------------------------------------------
        # Create or update the remote document
        # ----------------------------------------------------------
        document_url = ""

        if document_id is not None:
            # Update existing document: clear children, then recreate.
            await self._clear_document_blocks(document_id)
            if lark_blocks:
                await self._client.blocks.create_children(
                    document_id, document_id, lark_blocks
                )
            # Fetch the updated document to get the new revision.
            doc_info = await self._client.documents.get_document(document_id)
            new_revision: int = getattr(doc_info, "revision", 0)
        else:
            # Create a new document.
            title = path.stem.replace("-", " ").replace("_", " ").title()
            doc_response = await self._client.documents.create_document(
                title, folder_token
            )
            document_id = doc_response.document_id
            if lark_blocks:
                await self._client.blocks.create_children(
                    document_id, document_id, lark_blocks
                )
            doc_info = await self._client.documents.get_document(document_id)
            new_revision = getattr(doc_info, "revision", 0)
            document_url = getattr(doc_info, "url", "")

        # ----------------------------------------------------------
        # Update sync state
        # ----------------------------------------------------------
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
            local_path,
            document_id,
            new_revision,
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

    async def sync_from_lark(
        self,
        document_id: str,
        local_path: str | None = None,
        force: bool = False,
    ) -> SyncResult:
        """Pull a Lark document and write it as a local Markdown file.

        If ``local_path`` is not specified, the path is inferred from
        an existing mapping or derived from the document title.

        Args:
            document_id: The Lark document identifier.
            local_path: Optional explicit output path.
            force: Skip conflict detection when ``True``.

        Returns:
            A ``SyncResult`` describing the outcome.
        """
        mapping = self._state.get_mapping_by_doc_id(document_id)

        # Resolve local_path from mapping if not given.
        if local_path is None and mapping is not None:
            local_path = mapping.local_path

        # ----------------------------------------------------------
        # Fetch remote document metadata and blocks
        # ----------------------------------------------------------
        try:
            doc_info = await self._client.documents.get_document(document_id)
        except Exception as exc:
            return SyncResult(
                success=False,
                message=f"Failed to fetch document {document_id}: {exc}",
                document_id=document_id,
                local_path=local_path or "",
            )

        current_revision: int = getattr(doc_info, "revision", 0)
        document_title: str = getattr(doc_info, "title", "untitled")
        document_url: str = getattr(doc_info, "url", "")

        # Derive local_path from title if still not resolved.
        if local_path is None:
            safe_name = (
                document_title.lower()
                .replace(" ", "-")
                .replace("/", "-")
            )
            local_path = f"{safe_name}.md"

        # ----------------------------------------------------------
        # Conflict detection
        # ----------------------------------------------------------
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

        # ----------------------------------------------------------
        # Fetch blocks and convert to Markdown
        # ----------------------------------------------------------
        try:
            blocks = await self._client.blocks.list_blocks(document_id)
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

        # ----------------------------------------------------------
        # Write to local file
        # ----------------------------------------------------------
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(markdown_content, encoding="utf-8")

        # ----------------------------------------------------------
        # Update sync state
        # ----------------------------------------------------------
        now = datetime.now(timezone.utc)
        current_hash = compute_file_hash(local_path)

        if mapping is not None:
            self._state.update_mapping(
                mapping.local_path,
                local_path=local_path,
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
            document_id,
            local_path,
            current_revision,
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

    async def get_sync_status(
        self, local_path: str | None = None
    ) -> list[SyncStatusEntry]:
        """Return the sync status of all tracked mappings, or a single
        mapping identified by ``local_path``.

        Args:
            local_path: If provided, restrict the status report to this
                single mapping.

        Returns:
            A list of ``SyncStatusEntry`` objects.
        """
        state = self._state.load()
        mappings = state.mappings

        if local_path is not None:
            mappings = [m for m in mappings if m.local_path == local_path]

        entries: list[SyncStatusEntry] = []

        for mapping in mappings:
            status = await self._compute_status(mapping)
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

    async def _compute_status(self, mapping: SyncMapping) -> SyncStatusLabel:
        """Determine the current sync status label for a mapping.

        This makes a lightweight API call to fetch the current revision.
        If the call fails the status is reported as ``UNLINKED``.
        """
        try:
            doc_info = await self._client.documents.get_document(
                mapping.lark_document_id
            )
            current_revision: int = getattr(doc_info, "revision", 0)
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

    async def _clear_document_blocks(self, document_id: str) -> None:
        """Remove all child blocks from a document's root page block.

        The Lark API uses the document_id as the page-level block ID,
        and all top-level content blocks are its children.
        """
        blocks = await self._client.blocks.list_blocks(document_id)

        # Collect IDs of all child blocks (skip the page block itself).
        child_ids = [
            block["block_id"]
            for block in blocks
            if block.get("block_id") != document_id
            and block.get("parent_id") == document_id
        ]

        if child_ids:
            await self._client.blocks.batch_delete(document_id, child_ids)
