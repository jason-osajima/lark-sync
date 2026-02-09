"""Diffing utilities for comparing local and remote document states.

Provides hash-based change detection for local files, revision-based
change detection for remote Lark documents, and unified diff generation
for displaying what changed.
"""

from __future__ import annotations

import difflib
from pathlib import Path

from lark_sync.sync.state import SyncMapping, compute_file_hash


class SyncDiffer:
    """Stateless helper for detecting and displaying changes between
    local Markdown files and their remote Lark counterparts.
    """

    @staticmethod
    def has_local_changes(mapping: SyncMapping) -> bool:
        """Determine whether the local file has changed since the last sync.

        Compares the current SHA-256 hash of the file (with normalized
        line endings) against the hash stored in the mapping at the time
        of the last successful sync.

        Args:
            mapping: The sync mapping to check.

        Returns:
            ``True`` if the file has been modified (or the hash was never
            recorded), ``False`` otherwise.  Also returns ``False`` if
            the file no longer exists on disk.
        """
        path = Path(mapping.local_path)
        if not path.exists():
            # File was deleted -- treated as a change would be misleading;
            # higher-level logic should handle missing files separately.
            return False

        if not mapping.local_hash_at_sync:
            # No previous hash recorded -- consider it changed.
            return True

        current_hash = compute_file_hash(mapping.local_path)
        return current_hash != mapping.local_hash_at_sync

    @staticmethod
    def has_remote_changes(mapping: SyncMapping, current_revision: int) -> bool:
        """Determine whether the remote Lark document has changed since
        the last sync.

        Args:
            mapping: The sync mapping to check.
            current_revision: The latest revision number reported by the
                Lark API.

        Returns:
            ``True`` if the remote revision is greater than the stored
            revision (or no revision was ever recorded), ``False``
            otherwise.
        """
        if mapping.remote_revision_at_sync == 0:
            # No previous revision recorded -- consider it changed.
            return True

        return current_revision > mapping.remote_revision_at_sync

    @staticmethod
    def compute_diff(local_content: str, remote_content: str) -> str:
        """Generate a unified diff between local and remote content.

        Both inputs are normalized to ``\\n`` line endings before
        comparison to avoid spurious line-ending differences.

        Args:
            local_content: The content of the local Markdown file.
            remote_content: The Markdown content converted from the
                remote Lark document.

        Returns:
            A unified diff string.  Empty string if the contents are
            identical after normalization.
        """
        local_lines = local_content.replace("\r\n", "\n").replace("\r", "\n").splitlines(keepends=True)
        remote_lines = remote_content.replace("\r\n", "\n").replace("\r", "\n").splitlines(keepends=True)

        diff = difflib.unified_diff(
            remote_lines,
            local_lines,
            fromfile="remote (Lark)",
            tofile="local (Markdown)",
            lineterm="",
        )
        return "".join(diff)
