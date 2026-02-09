"""Wiki space and node operations against the Lark Wiki API.

Wraps ``/open-apis/wiki/v2/spaces`` endpoints: list spaces, list nodes,
get node, and create node.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import lark_oapi as lark
from lark_oapi.api.wiki.v2 import (
    CreateSpaceNodeRequest,
    GetNodeSpaceRequest,
    GetNodeSpaceResponse,
    ListSpaceNodeRequest,
    ListSpaceNodeResponse,
    ListSpaceRequest,
    ListSpaceResponse,
    Node,
    Space,
)


@dataclass(frozen=True)
class WikiSpaceInfo:
    """Lightweight container for wiki space metadata."""

    space_id: str
    name: str
    description: str


@dataclass(frozen=True)
class WikiNodeInfo:
    """Lightweight container for wiki node metadata."""

    node_token: str
    space_id: str
    obj_token: str
    obj_type: str
    parent_node_token: str
    title: str
    has_child: bool


class WikiClient:
    """Client for Lark wiki space and node operations.

    Args:
        client: A configured ``lark.Client`` instance.
    """

    def __init__(self, client: lark.Client) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # List spaces
    # ------------------------------------------------------------------

    def list_spaces(
        self,
        *,
        page_size: int = 50,
        page_token: str | None = None,
    ) -> tuple[list[WikiSpaceInfo], str | None]:
        """List wiki spaces accessible to the app, returning one page.

        Args:
            page_size: Number of spaces per page.
            page_token: Pagination cursor from a previous call.

        Returns:
            A tuple of ``(spaces, next_page_token)``.

        Raises:
            RuntimeError: If the API call fails.
        """
        builder = ListSpaceRequest.builder().page_size(page_size)
        if page_token:
            builder = builder.page_token(page_token)

        request = builder.build()
        response: ListSpaceResponse = self._client.wiki.v2.space.list(request)
        self._check_response(response, "list wiki spaces")

        spaces: list[WikiSpaceInfo] = []
        for s in response.data.items or []:
            spaces.append(
                WikiSpaceInfo(
                    space_id=s.space_id or "",
                    name=s.name or "",
                    description=s.description or "",
                )
            )

        next_token: str | None = response.data.page_token or None
        if not response.data.has_more:
            next_token = None

        return spaces, next_token

    def list_all_spaces(self) -> list[WikiSpaceInfo]:
        """Convenience: paginate through all wiki spaces.

        Returns:
            A flat list of all ``WikiSpaceInfo`` objects.
        """
        all_spaces: list[WikiSpaceInfo] = []
        page_token: str | None = None
        while True:
            spaces, page_token = self.list_spaces(page_token=page_token)
            all_spaces.extend(spaces)
            if page_token is None:
                break
        return all_spaces

    # ------------------------------------------------------------------
    # List nodes
    # ------------------------------------------------------------------

    def list_nodes(
        self,
        space_id: str,
        *,
        parent_node_token: str | None = None,
        page_size: int = 50,
        page_token: str | None = None,
    ) -> tuple[list[WikiNodeInfo], str | None]:
        """List nodes in a wiki space, returning one page.

        Args:
            space_id: The wiki space to list nodes from.
            parent_node_token: Optional parent to list children of.
                If omitted, lists root-level nodes.
            page_size: Number of nodes per page.
            page_token: Pagination cursor from a previous call.

        Returns:
            A tuple of ``(nodes, next_page_token)``.

        Raises:
            RuntimeError: If the API call fails.
        """
        builder = (
            ListSpaceNodeRequest.builder()
            .space_id(space_id)
            .page_size(page_size)
        )
        if parent_node_token:
            builder = builder.parent_node_token(parent_node_token)
        if page_token:
            builder = builder.page_token(page_token)

        request = builder.build()
        response: ListSpaceNodeResponse = (
            self._client.wiki.v2.space_node.list(request)
        )
        self._check_response(response, f"list nodes in space {space_id}")

        nodes = self._parse_nodes(response.data.items)
        next_token: str | None = response.data.page_token or None
        if not response.data.has_more:
            next_token = None

        return nodes, next_token

    def list_all_nodes(
        self,
        space_id: str,
        *,
        parent_node_token: str | None = None,
    ) -> list[WikiNodeInfo]:
        """Convenience: paginate through all nodes in a space.

        Args:
            space_id: The wiki space.
            parent_node_token: Optional parent node filter.

        Returns:
            A flat list of all ``WikiNodeInfo`` objects.
        """
        all_nodes: list[WikiNodeInfo] = []
        page_token: str | None = None
        while True:
            nodes, page_token = self.list_nodes(
                space_id,
                parent_node_token=parent_node_token,
                page_token=page_token,
            )
            all_nodes.extend(nodes)
            if page_token is None:
                break
        return all_nodes

    # ------------------------------------------------------------------
    # Get node
    # ------------------------------------------------------------------

    def get_node(self, token: str, *, obj_type: str | None = None) -> WikiNodeInfo:
        """Get a wiki node by its token.

        Args:
            token: The node or object token.
            obj_type: Optional object type hint (e.g. ``"doc"``, ``"docx"``).

        Returns:
            A ``WikiNodeInfo`` with the node's metadata.

        Raises:
            RuntimeError: If the API call fails.
        """
        builder = GetNodeSpaceRequest.builder().token(token)
        if obj_type:
            builder = builder.obj_type(obj_type)

        request = builder.build()
        response: GetNodeSpaceResponse = self._client.wiki.v2.space.get_node(request)
        self._check_response(response, f"get wiki node {token}")

        node: Node = response.data.node
        return self._node_to_info(node)

    # ------------------------------------------------------------------
    # Create node
    # ------------------------------------------------------------------

    def create_node(
        self,
        space_id: str,
        node: Node,
    ) -> WikiNodeInfo:
        """Create a new node in a wiki space.

        Args:
            space_id: The wiki space to create the node in.
            node: A ``Node`` object built via ``Node.builder()``.

        Returns:
            A ``WikiNodeInfo`` for the newly created node.

        Raises:
            RuntimeError: If the API call fails.
        """
        request = (
            CreateSpaceNodeRequest.builder()
            .space_id(space_id)
            .request_body(node)
            .build()
        )

        response = self._client.wiki.v2.space_node.create(request)
        self._check_response(response, f"create node in space {space_id}")

        return self._node_to_info(response.data.node)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _node_to_info(node: Node) -> WikiNodeInfo:
        return WikiNodeInfo(
            node_token=node.node_token or "",
            space_id=node.space_id or "",
            obj_token=node.obj_token or "",
            obj_type=node.obj_type or "",
            parent_node_token=node.parent_node_token or "",
            title=node.title or "",
            has_child=bool(node.has_child),
        )

    @staticmethod
    def _parse_nodes(items: list[Node] | None) -> list[WikiNodeInfo]:
        if not items:
            return []
        return [
            WikiNodeInfo(
                node_token=n.node_token or "",
                space_id=n.space_id or "",
                obj_token=n.obj_token or "",
                obj_type=n.obj_type or "",
                parent_node_token=n.parent_node_token or "",
                title=n.title or "",
                has_child=bool(n.has_child),
            )
            for n in items
        ]

    @staticmethod
    def _check_response(response: Any, operation: str) -> None:
        """Raise ``RuntimeError`` if the Lark API response indicates failure."""
        if not response.success():
            raise RuntimeError(
                f"Lark API error during '{operation}': "
                f"code={response.code}, msg={response.msg}, "
                f"log_id={response.get_log_id()}"
            )
