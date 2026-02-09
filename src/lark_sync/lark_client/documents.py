"""Document-level CRUD operations against the Lark Docx API.

Wraps ``/open-apis/docx/v1/documents`` endpoints: create, get metadata,
and get raw text content.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import lark_oapi as lark
from lark_oapi.api.docx.v1 import (
    CreateDocumentRequest,
    CreateDocumentRequestBody,
    CreateDocumentResponse,
    GetDocumentRequest,
    GetDocumentResponse,
    RawContentDocumentRequest,
    RawContentDocumentResponse,
)


@dataclass(frozen=True)
class DocumentInfo:
    """Lightweight container for document metadata."""

    document_id: str
    title: str
    revision_id: int


class DocumentsClient:
    """Client for Lark document CRUD operations.

    Args:
        client: A configured ``lark.Client`` instance.
    """

    def __init__(self, client: lark.Client) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Create
    # ------------------------------------------------------------------

    def create(
        self,
        title: str,
        folder_token: str | None = None,
    ) -> DocumentInfo:
        """Create a new document.

        Args:
            title: Document title.
            folder_token: Optional folder to create the document in.

        Returns:
            A ``DocumentInfo`` with the new document's id, title, and revision.

        Raises:
            RuntimeError: If the API call fails.
        """
        body_builder = CreateDocumentRequestBody.builder().title(title)
        if folder_token:
            body_builder = body_builder.folder_token(folder_token)

        request: CreateDocumentRequest = (
            CreateDocumentRequest.builder()
            .request_body(body_builder.build())
            .build()
        )

        response: CreateDocumentResponse = self._client.docx.v1.document.create(request)
        self._check_response(response, "create document")

        doc = response.data.document
        return DocumentInfo(
            document_id=doc.document_id,
            title=doc.title or title,
            revision_id=doc.revision_id or 1,
        )

    # ------------------------------------------------------------------
    # Get metadata
    # ------------------------------------------------------------------

    def get(self, document_id: str) -> DocumentInfo:
        """Retrieve document metadata (title, revision).

        Args:
            document_id: The document to look up.

        Returns:
            A ``DocumentInfo`` with the document's metadata.

        Raises:
            RuntimeError: If the API call fails.
        """
        request: GetDocumentRequest = (
            GetDocumentRequest.builder()
            .document_id(document_id)
            .build()
        )

        response: GetDocumentResponse = self._client.docx.v1.document.get(request)
        self._check_response(response, f"get document {document_id}")

        doc = response.data.document
        return DocumentInfo(
            document_id=doc.document_id,
            title=doc.title or "",
            revision_id=doc.revision_id or 0,
        )

    # ------------------------------------------------------------------
    # Raw content
    # ------------------------------------------------------------------

    def get_raw_content(self, document_id: str) -> str:
        """Get the plain-text content of a document.

        Args:
            document_id: The document to read.

        Returns:
            The raw text content of the document.

        Raises:
            RuntimeError: If the API call fails.
        """
        request: RawContentDocumentRequest = (
            RawContentDocumentRequest.builder()
            .document_id(document_id)
            .build()
        )

        response: RawContentDocumentResponse = (
            self._client.docx.v1.document.raw_content(request)
        )
        self._check_response(response, f"get raw content for {document_id}")

        return response.data.content or ""

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
