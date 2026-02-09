"""Conflict detection for bidirectional sync.

Compares both local file state and remote Lark document state to
classify the type of change (if any) that has occurred since the last
successful sync.
"""

from __future__ import annotations

from enum import StrEnum

from lark_sync.sync.differ import SyncDiffer
from lark_sync.sync.state import SyncMapping


class ConflictType(StrEnum):
    """Classification of changes detected between sync points."""

    NONE = "none"
    LOCAL_ONLY = "local_only"
    REMOTE_ONLY = "remote_only"
    BOTH_CHANGED = "both_changed"


class ConflictDetector:
    """Detects and classifies conflicts between local and remote state.

    Uses :class:`SyncDiffer` internally to check for local file hash
    changes and remote revision changes.
    """

    def __init__(self) -> None:
        self._differ = SyncDiffer()

    def detect(self, mapping: SyncMapping, current_revision: int) -> ConflictType:
        """Classify the change state for a given mapping.

        Args:
            mapping: The sync mapping to evaluate.
            current_revision: The latest revision number from the Lark
                API for the mapped document.

        Returns:
            A ``ConflictType`` indicating what has changed:

            - ``NONE`` -- neither side changed.
            - ``LOCAL_ONLY`` -- only the local file changed.
            - ``REMOTE_ONLY`` -- only the remote document changed.
            - ``BOTH_CHANGED`` -- both sides changed (true conflict).
        """
        local_changed = self._differ.has_local_changes(mapping)
        remote_changed = self._differ.has_remote_changes(mapping, current_revision)

        if local_changed and remote_changed:
            return ConflictType.BOTH_CHANGED
        if local_changed:
            return ConflictType.LOCAL_ONLY
        if remote_changed:
            return ConflictType.REMOTE_ONLY
        return ConflictType.NONE
