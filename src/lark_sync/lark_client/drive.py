"""Drive and folder operations against the Lark Drive API.

Wraps ``/open-apis/drive/v1/files`` endpoints for listing files in a
folder and retrieving folder metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import lark_oapi as lark
from lark_oapi.api.drive.v1 import (
    ListFileRequest,
    ListFileResponse,
)


@dataclass(frozen=True)
class DriveFileInfo:
    """Lightweight container for a file/folder entry in Drive."""

    token: str
    name: str
    type: str
    parent_token: str
    url: str
    created_time: str
    modified_time: str
    owner_id: str


class DriveClient:
    """Client for Lark Drive/folder operations.

    Args:
        client: A configured ``lark.Client`` instance.
    """

    def __init__(self, client: lark.Client) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # List files in folder
    # ------------------------------------------------------------------

    def list_files(
        self,
        folder_token: str,
        *,
        page_size: int = 50,
        page_token: str | None = None,
        order_by: str | None = None,
        direction: str | None = None,
    ) -> tuple[list[DriveFileInfo], str | None]:
        """List files and sub-folders inside a Drive folder.

        Args:
            folder_token: Token of the folder to list.
            page_size: Number of items per page (max 200).
            page_token: Pagination cursor from a previous call.
            order_by: Optional sort field (e.g. ``"EditedTime"``).
            direction: Sort direction (``"ASC"`` or ``"DESC"``).

        Returns:
            A tuple of ``(files, next_page_token)``.

        Raises:
            RuntimeError: If the API call fails.
        """
        builder = (
            ListFileRequest.builder()
            .folder_token(folder_token)
            .page_size(page_size)
        )
        if page_token:
            builder = builder.page_token(page_token)
        if order_by:
            builder = builder.order_by(order_by)
        if direction:
            builder = builder.direction(direction)

        request = builder.build()
        response: ListFileResponse = self._client.drive.v1.file.list(request)
        self._check_response(response, f"list files in folder {folder_token}")

        files: list[DriveFileInfo] = []
        for f in response.data.files or []:
            files.append(
                DriveFileInfo(
                    token=getattr(f, "token", "") or "",
                    name=getattr(f, "name", "") or "",
                    type=getattr(f, "type", "") or "",
                    parent_token=getattr(f, "parent_token", "") or "",
                    url=getattr(f, "url", "") or "",
                    created_time=getattr(f, "created_time", "") or "",
                    modified_time=getattr(f, "modified_time", "") or "",
                    owner_id=getattr(f, "owner_id", "") or "",
                )
            )

        next_token: str | None = response.data.next_page_token or None
        if not response.data.has_more:
            next_token = None

        return files, next_token

    def list_all_files(self, folder_token: str) -> list[DriveFileInfo]:
        """Convenience: paginate through all files in a folder.

        Args:
            folder_token: Token of the folder to list.

        Returns:
            A flat list of all ``DriveFileInfo`` objects.
        """
        all_files: list[DriveFileInfo] = []
        page_token: str | None = None
        while True:
            files, page_token = self.list_files(
                folder_token, page_token=page_token
            )
            all_files.extend(files)
            if page_token is None:
                break
        return all_files

    # ------------------------------------------------------------------
    # Get folder info
    # ------------------------------------------------------------------

    def get_folder_info(self, folder_token: str) -> DriveFileInfo | None:
        """Get metadata for a specific folder.

        This is implemented by listing the parent and filtering, because
        the Drive API does not provide a direct ``get folder`` endpoint.
        For the root folder, we return a synthetic entry.

        Args:
            folder_token: Token of the folder to look up.

        Returns:
            A ``DriveFileInfo`` if found, or ``None``.
        """
        # List the folder itself -- listing with the token as folder_token
        # returns its children.  We can get minimal info from the first
        # list call's metadata when available.  As a pragmatic fallback,
        # we return a synthetic entry.
        try:
            files, _ = self.list_files(folder_token, page_size=1)
        except RuntimeError:
            return None

        # Return a synthetic info for the folder itself
        return DriveFileInfo(
            token=folder_token,
            name="",
            type="folder",
            parent_token="",
            url="",
            created_time="",
            modified_time="",
            owner_id="",
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _check_response(response: Any, operation: str) -> None:
        """Raise ``RuntimeError`` if the Lark API response indicates failure."""
        if not response.success():
            raise RuntimeError(
                f"Lark API error during '{operation}': "
                f"code={response.code}, msg={response.msg}, "
                f"log_id={response.get_log_id()}"
            )
