"""Convert a flat list of Lark document blocks into a Markdown string.

The Lark Open API returns document content as a **flat** list of blocks where
parent/child relationships are expressed via ``parent_id`` and ``children``
fields.  This module reconstructs the tree and renders each block type to the
corresponding Markdown syntax.
"""

from __future__ import annotations

from typing import Any

from lark_sync.converter.block_types import BlockType
from lark_sync.converter.text_elements import elements_to_markdown


def _children(block: dict[str, Any]) -> list[Any]:
    """Safely get children list, handling None values from the API."""
    return block.get("children") or []


class LarkToMarkdownConverter:
    """Stateless converter: Lark block list -> Markdown text."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def convert(self, blocks: list[dict[str, Any]]) -> str:
        """Convert a flat Lark block list into a Markdown string.

        Parameters
        ----------
        blocks:
            List of block dicts as returned by the Lark ``GET /blocks`` API.
            Each dict is expected to contain at least ``block_id``,
            ``block_type``, and ``parent_id``.  Body content lives under a
            key that varies by type (e.g. ``text``, ``heading1``, ``code``).

        Returns
        -------
        str
            The rendered Markdown document.
        """
        tree = self._build_tree(blocks)
        lines: list[str] = []
        root_ids = self._root_children(blocks, tree)
        for block_id in root_ids:
            self._render_block(tree, block_id, lines, depth=0)
        return "\n".join(lines).strip() + "\n"

    # ------------------------------------------------------------------
    # Tree construction
    # ------------------------------------------------------------------

    @staticmethod
    def _build_tree(blocks: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """Index blocks by ``block_id`` and return the lookup dict."""
        return {b["block_id"]: b for b in blocks}

    @staticmethod
    def _root_children(
        blocks: list[dict[str, Any]],
        tree: dict[str, dict[str, Any]],
    ) -> list[str]:
        """Return the ordered list of top-level block IDs.

        The root (PAGE) block should be the first block in the list; its
        ``children`` array determines the render order.  If no PAGE block is
        found, fall back to blocks whose ``parent_id`` is missing from the
        tree (i.e. orphans).
        """
        for b in blocks:
            if BlockType.from_value(b.get("block_type", 0)) == BlockType.PAGE:
                return b.get("children", [])
        # Fallback: blocks whose parent is not in the tree.
        ids_in_tree = set(tree)
        return [
            b["block_id"]
            for b in blocks
            if b.get("parent_id") not in ids_in_tree
        ]

    # ------------------------------------------------------------------
    # Block-level rendering
    # ------------------------------------------------------------------

    def _render_block(
        self,
        tree: dict[str, dict[str, Any]],
        block_ref: str | dict[str, Any],
        lines: list[str],
        depth: int,
    ) -> None:
        """Render a single block (and recurse into children) into *lines*.

        *block_ref* may be a string block ID (flat Lark API format) **or** an
        inline block dict (as produced by ``MarkdownToLarkConverter``).
        """
        block = self._resolve_block(tree, block_ref)
        if block is None:
            return

        bt = BlockType.from_value(block.get("block_type", 0))
        handler = self._HANDLERS.get(bt, self._render_unsupported)
        handler(self, tree, block, lines, depth)

    @staticmethod
    def _resolve_block(
        tree: dict[str, dict[str, Any]],
        ref: str | dict[str, Any],
    ) -> dict[str, Any] | None:
        """Return the block dict for a child reference.

        Handles both string IDs (looked up in *tree*) and inline dicts.
        """
        if isinstance(ref, dict):
            return ref
        return tree.get(ref)

    # -- TEXT / PARAGRAPH --------------------------------------------------

    def _render_text(
        self,
        tree: dict[str, dict[str, Any]],
        block: dict[str, Any],
        lines: list[str],
        depth: int,
    ) -> None:
        body = block.get("text") or {}
        md = elements_to_markdown(body.get("elements") or [])
        lines.append(md)
        lines.append("")

    # -- HEADINGS ----------------------------------------------------------

    def _render_heading(
        self,
        tree: dict[str, dict[str, Any]],
        block: dict[str, Any],
        lines: list[str],
        depth: int,
    ) -> None:
        bt = BlockType.from_value(block.get("block_type", 0))
        level = BlockType.heading_level(bt) or 1
        # Lark stores heading body under a key like ``heading1``, ``heading2``, ...
        body_key = f"heading{level}"
        body = block.get(body_key) or {}
        md = elements_to_markdown(body.get("elements") or [])
        prefix = "#" * min(level, 6)  # Markdown only supports h1-h6
        lines.append(f"{prefix} {md}")
        lines.append("")

    # -- BULLET LIST -------------------------------------------------------

    def _render_bullet(
        self,
        tree: dict[str, dict[str, Any]],
        block: dict[str, Any],
        lines: list[str],
        depth: int,
    ) -> None:
        body = block.get("bullet") or {}
        md = elements_to_markdown(body.get("elements") or [])
        indent = "  " * depth
        lines.append(f"{indent}- {md}")
        for child_id in _children(block):
            self._render_block(tree, child_id, lines, depth + 1)

    # -- ORDERED LIST ------------------------------------------------------

    def _render_ordered(
        self,
        tree: dict[str, dict[str, Any]],
        block: dict[str, Any],
        lines: list[str],
        depth: int,
    ) -> None:
        body = block.get("ordered") or {}
        md = elements_to_markdown(body.get("elements") or [])
        indent = "  " * depth
        lines.append(f"{indent}1. {md}")
        for child_id in _children(block):
            self._render_block(tree, child_id, lines, depth + 1)

    # -- CODE BLOCK --------------------------------------------------------

    def _render_code(
        self,
        tree: dict[str, dict[str, Any]],
        block: dict[str, Any],
        lines: list[str],
        depth: int,
    ) -> None:
        body = block.get("code") or {}
        style = body.get("style") or {}
        language = style.get("language", "")
        # Map Lark language enum ints to string labels if needed.
        if isinstance(language, int):
            language = _LANG_MAP.get(language, "")
        elements = body.get("elements") or []
        code_text = elements_to_markdown(elements)
        lines.append(f"```{language}")
        lines.append(code_text)
        lines.append("```")
        lines.append("")

    # -- QUOTE_CONTAINER ---------------------------------------------------

    def _render_quote_container(
        self,
        tree: dict[str, dict[str, Any]],
        block: dict[str, Any],
        lines: list[str],
        depth: int,
    ) -> None:
        child_lines: list[str] = []
        for child_id in _children(block):
            self._render_block(tree, child_id, child_lines, depth=0)
        for line in child_lines:
            lines.append(f"> {line}" if line else ">")
        lines.append("")

    # -- QUOTE (single block, legacy) -------------------------------------

    def _render_quote(
        self,
        tree: dict[str, dict[str, Any]],
        block: dict[str, Any],
        lines: list[str],
        depth: int,
    ) -> None:
        body = block.get("quote") or {}
        md = elements_to_markdown(body.get("elements") or [])
        lines.append(f"> {md}")
        lines.append("")

    # -- DIVIDER -----------------------------------------------------------

    def _render_divider(
        self,
        tree: dict[str, dict[str, Any]],
        block: dict[str, Any],
        lines: list[str],
        depth: int,
    ) -> None:
        lines.append("---")
        lines.append("")

    # -- TODO / TASK LIST --------------------------------------------------

    def _render_todo(
        self,
        tree: dict[str, dict[str, Any]],
        block: dict[str, Any],
        lines: list[str],
        depth: int,
    ) -> None:
        body = block.get("todo") or {}
        md = elements_to_markdown(body.get("elements") or [])
        done = (body.get("style") or {}).get("done", False)
        checkbox = "[x]" if done else "[ ]"
        indent = "  " * depth
        lines.append(f"{indent}- {checkbox} {md}")
        for child_id in _children(block):
            self._render_block(tree, child_id, lines, depth + 1)

    # -- TABLE + TABLE_CELL ------------------------------------------------

    def _render_table(
        self,
        tree: dict[str, dict[str, Any]],
        block: dict[str, Any],
        lines: list[str],
        depth: int,
    ) -> None:
        """Render a Lark table block as a GitHub-flavoured Markdown table.

        Lark tables are structured as a TABLE block whose ``children`` are
        TABLE_CELL blocks laid out in row-major order.  The table body carries
        ``property.row_size`` and ``property.column_size``.
        """
        table_body = block.get("table") or {}
        prop = table_body.get("property") or {}
        row_count: int = prop.get("row_size", 0)
        col_count: int = prop.get("column_size", 0)
        if row_count == 0 or col_count == 0:
            return

        child_refs: list[str | dict[str, Any]] = _children(block)

        # Build a row x col matrix of rendered cell text.
        rows: list[list[str]] = []
        idx = 0
        for _r in range(row_count):
            row: list[str] = []
            for _c in range(col_count):
                cell_text = ""
                if idx < len(child_refs):
                    cell_block = self._resolve_block(tree, child_refs[idx])
                    if cell_block:
                        cell_text = self._render_table_cell(tree, cell_block)
                    idx += 1
                row.append(cell_text)
            rows.append(row)

        # Emit header row.
        if rows:
            lines.append("| " + " | ".join(rows[0]) + " |")
            lines.append("| " + " | ".join("---" for _ in range(col_count)) + " |")
            for row in rows[1:]:
                lines.append("| " + " | ".join(row) + " |")
            lines.append("")

    def _render_table_cell(
        self,
        tree: dict[str, dict[str, Any]],
        cell_block: dict[str, Any],
    ) -> str:
        """Render the content of a single table cell to inline Markdown."""
        # TABLE_CELL blocks contain child blocks (typically TEXT).
        child_parts: list[str] = []
        for child_ref in _children(cell_block):
            child = self._resolve_block(tree, child_ref)
            if child is None:
                continue
            bt = BlockType.from_value(child.get("block_type", 0))
            if bt == BlockType.TEXT:
                body = child.get("text") or {}
                child_parts.append(elements_to_markdown(body.get("elements", [])))
            else:
                # Fallback: try generic text extraction.
                for key in ("text", "heading1", "heading2", "heading3"):
                    body = child.get(key)
                    if body and "elements" in body:
                        child_parts.append(
                            elements_to_markdown(body["elements"])
                        )
                        break
        return " ".join(child_parts).replace("|", "\\|").replace("\n", " ")

    # -- IMAGE -------------------------------------------------------------

    def _render_image(
        self,
        tree: dict[str, dict[str, Any]],
        block: dict[str, Any],
        lines: list[str],
        depth: int,
    ) -> None:
        body = block.get("image") or {}
        token = body.get("token", "")
        alt = body.get("alt", "image")
        lines.append(f"![{alt}]({token})")
        lines.append("")

    # -- CALLOUT -----------------------------------------------------------

    def _render_callout(
        self,
        tree: dict[str, dict[str, Any]],
        block: dict[str, Any],
        lines: list[str],
        depth: int,
    ) -> None:
        body = block.get("callout") or {}
        # Lark callout has an emoji_id that hints at the callout kind.
        emoji_id = body.get("emoji_id", "")
        callout_label = _CALLOUT_LABEL_MAP.get(emoji_id, "NOTE")

        child_lines: list[str] = []
        for child_id in _children(block):
            self._render_block(tree, child_id, child_lines, depth=0)

        lines.append(f"> [!{callout_label}]")
        for line in child_lines:
            lines.append(f"> {line}" if line else ">")
        lines.append("")

    # -- UNSUPPORTED / FALLBACK --------------------------------------------

    def _render_unsupported(
        self,
        tree: dict[str, dict[str, Any]],
        block: dict[str, Any],
        lines: list[str],
        depth: int,
    ) -> None:
        bt = BlockType.from_value(block.get("block_type", 0))
        block_id = block.get("block_id", "unknown")
        lines.append(f"<!-- lark:{bt.name}:{block_id} -->")
        lines.append("")

    # ------------------------------------------------------------------
    # Handler dispatch table
    # ------------------------------------------------------------------

    _HANDLERS: dict[BlockType, Any] = {
        BlockType.PAGE: lambda self, tree, block, lines, depth: [
            self._render_block(tree, cid, lines, depth)
            for cid in _children(block)
        ],
        BlockType.TEXT: _render_text,
        BlockType.HEADING1: _render_heading,
        BlockType.HEADING2: _render_heading,
        BlockType.HEADING3: _render_heading,
        BlockType.HEADING4: _render_heading,
        BlockType.HEADING5: _render_heading,
        BlockType.HEADING6: _render_heading,
        BlockType.HEADING7: _render_heading,
        BlockType.HEADING8: _render_heading,
        BlockType.HEADING9: _render_heading,
        BlockType.BULLET: _render_bullet,
        BlockType.ORDERED: _render_ordered,
        BlockType.CODE: _render_code,
        BlockType.QUOTE: _render_quote,
        BlockType.QUOTE_CONTAINER: _render_quote_container,
        BlockType.DIVIDER: _render_divider,
        BlockType.TODO: _render_todo,
        BlockType.TABLE: _render_table,
        BlockType.IMAGE: _render_image,
        BlockType.CALLOUT: _render_callout,
    }


# ---------------------------------------------------------------------------
# Static lookup tables
# ---------------------------------------------------------------------------

# Lark language enum -> string (a representative subset; expand as needed).
_LANG_MAP: dict[int, str] = {
    1: "plaintext",
    2: "abap",
    3: "ada",
    4: "apache",
    5: "apex",
    6: "assembly",
    7: "bash",
    8: "c",
    9: "c#",
    10: "c++",
    11: "cobol",
    12: "css",
    13: "coffeescript",
    14: "d",
    15: "dart",
    16: "delphi",
    17: "django",
    18: "dockerfile",
    19: "elixir",
    20: "erlang",
    21: "fortran",
    22: "foxpro",
    23: "go",
    24: "groovy",
    25: "html",
    26: "haskell",
    27: "http",
    28: "json",
    29: "java",
    30: "javascript",
    31: "julia",
    32: "kotlin",
    33: "latex",
    34: "lisp",
    35: "logo",
    36: "lua",
    37: "matlab",
    38: "makefile",
    39: "markdown",
    40: "nginx",
    41: "objective-c",
    42: "openedgeabl",
    43: "perl",
    44: "php",
    45: "pl/sql",
    46: "powershell",
    47: "prolog",
    48: "protobuf",
    49: "python",
    50: "r",
    51: "rpg",
    52: "ruby",
    53: "rust",
    54: "sas",
    55: "scss",
    56: "sql",
    57: "scala",
    58: "scheme",
    59: "scratch",
    60: "shell",
    61: "swift",
    62: "thrift",
    63: "typescript",
    64: "vbscript",
    65: "visual-basic",
    66: "xml",
    67: "yaml",
}

# Map common Lark callout emoji_ids to GFM-style callout labels.
_CALLOUT_LABEL_MAP: dict[str, str] = {
    "bulb": "TIP",
    "warning": "WARNING",
    "red_circle": "CAUTION",
    "info": "NOTE",
    "check": "NOTE",
    "star": "IMPORTANT",
}
