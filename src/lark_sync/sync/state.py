"""Sync state persistence using JSON-backed Pydantic models.

Tracks the bidirectional mapping between local Markdown files and Lark
cloud documents so the sync engine can detect changes and conflicts.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field


class SyncDirection(StrEnum):
    """Direction of sync for a given mapping."""

    BIDIRECTIONAL = "bidirectional"
    TO_LARK = "to_lark"
    FROM_LARK = "from_lark"


class SyncMapping(BaseModel):
    """A single mapping between a local file and a Lark document."""

    local_path: str
    lark_document_id: str
    lark_document_url: str = ""
    lark_wiki_space_id: str | None = None
    lark_wiki_node_token: str | None = None
    last_synced_at: datetime | None = None
    local_hash_at_sync: str = ""
    remote_revision_at_sync: int = 0
    sync_direction: SyncDirection = SyncDirection.BIDIRECTIONAL


class SyncState(BaseModel):
    """Root model for the persisted sync state file."""

    version: int = 1
    mappings: list[SyncMapping] = Field(default_factory=list)


def compute_file_hash(file_path: str) -> str:
    """Compute a SHA-256 hash of a file's content after normalizing line endings.

    Line endings are normalized to ``\\n`` before hashing so that the
    same logical content produces the same hash across platforms.

    Args:
        file_path: Absolute or relative path to the file.

    Returns:
        Hex-encoded SHA-256 digest string.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    content = Path(file_path).read_text(encoding="utf-8")
    normalized = content.replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def compute_content_hash(content: str) -> str:
    """Compute a SHA-256 hash of a string after normalizing line endings.

    Args:
        content: The text content to hash.

    Returns:
        Hex-encoded SHA-256 digest string.
    """
    normalized = content.replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class SyncStateManager:
    """Manages reading, writing, and querying the JSON sync state file.

    The state file lives alongside the synced documents and records
    which local files map to which Lark documents, along with the
    hashes and revisions that were current at the last sync.

    When ``project_root`` is provided, paths are stored as POSIX-style
    relative paths (e.g. ``docs/prd.md``) and resolved against
    ``project_root`` for file operations.

    Args:
        state_file: Path to the JSON state file.
        project_root: Optional project root for relative path support.
    """

    def __init__(
        self, state_file: str, project_root: Path | None = None
    ) -> None:
        self._state_file = Path(state_file)
        self._project_root = project_root
        self._state: SyncState | None = None

    @property
    def project_root(self) -> Path | None:
        return self._project_root

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def load(self) -> SyncState:
        """Load sync state from disk, returning an empty state if the file
        does not exist or is empty.

        Returns:
            The deserialized ``SyncState``.
        """
        if self._state_file.exists() and self._state_file.stat().st_size > 0:
            raw = self._state_file.read_text(encoding="utf-8")
            self._state = SyncState.model_validate_json(raw)
        else:
            self._state = SyncState()
        return self._state

    def save(self, state: SyncState) -> None:
        """Persist the given sync state to disk as pretty-printed JSON.

        Parent directories are created automatically if they do not exist.

        Args:
            state: The ``SyncState`` to write.
        """
        self._state = state
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        self._state_file.write_text(
            state.model_dump_json(indent=2) + "\n",
            encoding="utf-8",
        )

    # ------------------------------------------------------------------
    # Internal helper to ensure state is loaded
    # ------------------------------------------------------------------

    def _ensure_loaded(self) -> SyncState:
        """Return the in-memory state, loading from disk if necessary."""
        if self._state is None:
            self.load()
        assert self._state is not None  # noqa: S101
        return self._state

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def get_mapping(self, local_path: str) -> SyncMapping | None:
        """Look up a mapping by local file path.

        When the state manager is in project-local mode, incoming
        absolute paths are converted to relative for lookup, and
        incoming relative paths are matched as-is.

        Args:
            local_path: The path used as the mapping key.

        Returns:
            The matching ``SyncMapping`` or ``None``.
        """
        state = self._ensure_loaded()
        lookup = self._normalize_path(local_path)
        for mapping in state.mappings:
            if mapping.local_path == lookup:
                return mapping
        return None

    def get_mapping_by_doc_id(self, document_id: str) -> SyncMapping | None:
        """Look up a mapping by Lark document ID.

        Args:
            document_id: The Lark document identifier.

        Returns:
            The matching ``SyncMapping`` or ``None``.
        """
        state = self._ensure_loaded()
        for mapping in state.mappings:
            if mapping.lark_document_id == document_id:
                return mapping
        return None

    # ------------------------------------------------------------------
    # Mutation helpers
    # ------------------------------------------------------------------

    def add_mapping(self, mapping: SyncMapping) -> None:
        """Add a new mapping and persist to disk.

        If a mapping with the same ``local_path`` already exists it will
        be replaced.  In project-local mode, the mapping's local_path
        is converted to a relative POSIX path before storage.

        Args:
            mapping: The ``SyncMapping`` to add.
        """
        state = self._ensure_loaded()
        normalized = self._normalize_path(mapping.local_path)
        # Remove any existing mapping for the same path.
        state.mappings = [
            m for m in state.mappings if m.local_path != normalized
        ]
        mapping.local_path = normalized
        state.mappings.append(mapping)
        self.save(state)

    def update_mapping(self, local_path: str, **updates: object) -> None:
        """Update fields on an existing mapping and persist.

        Args:
            local_path: The path identifying the mapping to update.
            **updates: Keyword arguments corresponding to ``SyncMapping``
                field names and their new values.

        Raises:
            KeyError: If no mapping exists for the given path.
        """
        state = self._ensure_loaded()
        lookup = self._normalize_path(local_path)
        for mapping in state.mappings:
            if mapping.local_path == lookup:
                for key, value in updates.items():
                    setattr(mapping, key, value)
                self.save(state)
                return
        raise KeyError(f"No mapping found for local path: {lookup}")

    def remove_mapping(self, local_path: str) -> None:
        """Remove a mapping by local path and persist.

        This is a no-op if no mapping exists for the given path.

        Args:
            local_path: The path identifying the mapping to remove.
        """
        state = self._ensure_loaded()
        lookup = self._normalize_path(local_path)
        state.mappings = [
            m for m in state.mappings if m.local_path != lookup
        ]
        self.save(state)

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def _normalize_path(self, local_path: str) -> str:
        """Normalize a path for storage/lookup.

        In project-local mode, absolute paths are converted to relative
        POSIX paths.  Otherwise the path is returned as-is.
        """
        if self._project_root is None:
            return local_path
        p = Path(local_path)
        if p.is_absolute():
            try:
                return p.relative_to(self._project_root).as_posix()
            except ValueError:
                # Path is outside project — store as-is.
                return local_path
        return p.as_posix()

    def resolve_path(self, local_path: str) -> str:
        """Resolve a stored path to an absolute path for file operations.

        In project-local mode, relative paths are resolved against
        ``project_root``.  Absolute paths are returned as-is.
        """
        if self._project_root is None:
            return local_path
        p = Path(local_path)
        if not p.is_absolute():
            return str(self._project_root / p)
        return local_path
