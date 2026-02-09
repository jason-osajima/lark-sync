"""Document search operations against the Lark Search API.

Wraps ``/open-apis/search/v2/doc_wiki`` for full-text search across
documents and wiki pages.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import lark_oapi as lark
from lark_oapi.api.search.v2 import (
    DocFilter,
    SearchDocWikiRequest,
    SearchDocWikiRequestBody,
    SearchDocWikiResponse,
    WikiFilter,
)


@dataclass(frozen=True)
class SearchResult:
    """A single search hit."""

    doc_id: str
    title: str
    url: str
    doc_type: str
    owner_id: str


class SearchClient:
    """Client for Lark document search operations.

    Args:
        client: A configured ``lark.Client`` instance.
    """

    def __init__(self, client: lark.Client) -> None:
        self._client = client

    def search(
        self,
        query: str,
        *,
        page_size: int = 20,
        page_token: str | None = None,
        doc_types: list[str] | None = None,
        wiki_space_ids: list[str] | None = None,
    ) -> tuple[list[SearchResult], str | None, bool]:
        """Search for documents and wiki pages.

        Args:
            query: The search query string.
            page_size: Number of results per page (max 50).
            page_token: Pagination cursor from a previous call.
            doc_types: Optional filter for document types
                (e.g. ``["docx", "doc", "sheet"]``).
            wiki_space_ids: Optional filter for specific wiki space IDs.

        Returns:
            A tuple of ``(results, next_page_token, has_more)``.

        Raises:
            RuntimeError: If the API call fails.
        """
        body_builder = (
            SearchDocWikiRequestBody.builder()
            .query(query)
            .page_size(page_size)
        )
        if page_token:
            body_builder = body_builder.page_token(page_token)
        if doc_types:
            doc_filter = DocFilter.builder().build()
            doc_filter.search_obj_type = doc_types
            body_builder = body_builder.doc_filter(doc_filter)
        if wiki_space_ids:
            wiki_filter = (
                WikiFilter.builder().build()
            )
            wiki_filter.space_id = wiki_space_ids
            body_builder = body_builder.wiki_filter(wiki_filter)

        request: SearchDocWikiRequest = (
            SearchDocWikiRequest.builder()
            .request_body(body_builder.build())
            .build()
        )

        response: SearchDocWikiResponse = (
            self._client.search.v2.doc_wiki.search(request)
        )
        self._check_response(response, f"search for '{query}'")

        results: list[SearchResult] = []
        for item in response.data.items or []:
            results.append(
                SearchResult(
                    doc_id=getattr(item, "doc_id", "") or "",
                    title=getattr(item, "title", "") or "",
                    url=getattr(item, "url", "") or "",
                    doc_type=getattr(item, "doc_type", "") or "",
                    owner_id=getattr(item, "owner_id", "") or "",
                )
            )

        next_token: str | None = response.data.page_token or None
        has_more: bool = bool(response.data.has_more)
        if not has_more:
            next_token = None

        return results, next_token, has_more

    def search_all(
        self,
        query: str,
        *,
        doc_types: list[str] | None = None,
        wiki_space_ids: list[str] | None = None,
        max_results: int = 200,
    ) -> list[SearchResult]:
        """Convenience: paginate through search results up to a limit.

        Args:
            query: The search query string.
            doc_types: Optional document type filter.
            wiki_space_ids: Optional wiki space filter.
            max_results: Maximum number of results to return.

        Returns:
            A flat list of ``SearchResult`` objects.
        """
        all_results: list[SearchResult] = []
        page_token: str | None = None

        while len(all_results) < max_results:
            results, page_token, has_more = self.search(
                query,
                doc_types=doc_types,
                wiki_space_ids=wiki_space_ids,
                page_token=page_token,
            )
            all_results.extend(results)
            if page_token is None or not has_more:
                break

        return all_results[:max_results]

    @staticmethod
    def _check_response(response: Any, operation: str) -> None:
        """Raise ``RuntimeError`` if the Lark API response indicates failure."""
        if not response.success():
            raise RuntimeError(
                f"Lark API error during '{operation}': "
                f"code={response.code}, msg={response.msg}, "
                f"log_id={response.get_log_id()}"
            )
