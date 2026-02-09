"""Convert a Markdown string into a flat list of Lark document block dicts.

Uses ``markdown-it-py`` to tokenise the Markdown source, then walks the token
stream and emits block dicts that match the Lark Open API *create block*
request schema.

Each emitted block dict has the shape::

    {
        "block_type": <int>,          # BlockType enum value
        "<type_key>": {               # e.g. "text", "heading2", "code", ...
            "elements": [...],        # list of TextElement dicts
            "style": {...},           # optional style payload
        },
        "children": [...],            # child block dicts (e.g. for tables)
    }

The caller is responsible for assigning ``block_id`` / ``parent_id`` when
sending the blocks to the Lark API.
"""

from __future__ import annotations

import uuid
from typing import Any

from markdown_it import MarkdownIt

from lark_sync.converter.block_types import BlockType
from lark_sync.converter.text_elements import parse_inline_markdown


class MarkdownToLarkConverter:
    """Stateless converter: Markdown text -> Lark block list."""

    def __init__(self) -> None:
        self._md = MarkdownIt("commonmark", {"typographer": False})
        self._md.enable("table")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def convert(self, markdown_text: str) -> list[dict[str, Any]]:
        """Parse *markdown_text* and return a list of Lark block dicts.

        The returned list is flat and ordered; nested structures (e.g.
        quote children, table cells) are expressed through the ``children``
        key on the parent block.
        """
        tokens = self._md.parse(markdown_text)
        blocks: list[dict[str, Any]] = []
        idx = 0
        while idx < len(tokens):
            idx = self._consume_token(tokens, idx, blocks, list_depth=0)
        return blocks

    # ------------------------------------------------------------------
    # Token consumers
    # ------------------------------------------------------------------

    def _consume_token(
        self,
        tokens: list[Any],
        idx: int,
        blocks: list[dict[str, Any]],
        *,
        list_depth: int = 0,
    ) -> int:
        """Dispatch on the current token type and return the next index."""
        tok = tokens[idx]

        if tok.type == "heading_open":
            return self._consume_heading(tokens, idx, blocks)
        if tok.type == "paragraph_open":
            return self._consume_paragraph(tokens, idx, blocks)
        if tok.type == "bullet_list_open":
            return self._consume_list(
                tokens, idx, blocks, ordered=False, list_depth=list_depth
            )
        if tok.type == "ordered_list_open":
            return self._consume_list(
                tokens, idx, blocks, ordered=True, list_depth=list_depth
            )
        if tok.type == "fence" or tok.type == "code_block":
            return self._consume_code(tokens, idx, blocks)
        if tok.type == "blockquote_open":
            return self._consume_blockquote(tokens, idx, blocks)
        if tok.type == "hr":
            return self._consume_hr(tokens, idx, blocks)
        if tok.type == "html_block":
            return self._consume_html_block(tokens, idx, blocks)

        # Table handling (markdown-it-py table plugin).
        if tok.type == "table_open":
            return self._consume_table(tokens, idx, blocks)

        # Skip tokens we do not handle (close tags, etc.).
        return idx + 1

    # -- Heading -----------------------------------------------------------

    def _consume_heading(
        self,
        tokens: list[Any],
        idx: int,
        blocks: list[dict[str, Any]],
    ) -> int:
        open_tok = tokens[idx]
        level = int(open_tok.tag.replace("h", ""))  # "h1" -> 1
        # The inline content is the next token.
        inline_tok = tokens[idx + 1]
        content = inline_tok.content or ""
        # idx+2 should be heading_close.
        elements = parse_inline_markdown(content)
        block_type_value = BlockType.HEADING1.value + level - 1
        body_key = f"heading{level}"
        blocks.append(
            {
                "block_type": block_type_value,
                body_key: {"elements": elements},
            }
        )
        return idx + 3  # open, inline, close

    # -- Paragraph ---------------------------------------------------------

    def _consume_paragraph(
        self,
        tokens: list[Any],
        idx: int,
        blocks: list[dict[str, Any]],
    ) -> int:
        inline_tok = tokens[idx + 1]
        content = inline_tok.content or ""

        # Check for image-only paragraph.
        if inline_tok.children:
            image_block = self._try_extract_image(inline_tok)
            if image_block is not None:
                blocks.append(image_block)
                return idx + 3

        # Check for task-list style checkbox: ``- [ ] text`` or ``- [x] text``.
        todo = self._try_parse_todo(content)
        if todo is not None:
            blocks.append(todo)
            return idx + 3

        elements = parse_inline_markdown(content)
        blocks.append(
            {
                "block_type": BlockType.TEXT.value,
                "text": {"elements": elements},
            }
        )
        return idx + 3  # open, inline, close

    # -- List (bullet / ordered) -------------------------------------------

    def _consume_list(
        self,
        tokens: list[Any],
        idx: int,
        blocks: list[dict[str, Any]],
        *,
        ordered: bool,
        list_depth: int,
    ) -> int:
        close_type = "ordered_list_close" if ordered else "bullet_list_close"
        idx += 1  # skip list_open
        while idx < len(tokens) and tokens[idx].type != close_type:
            if tokens[idx].type == "list_item_open":
                idx = self._consume_list_item(
                    tokens, idx, blocks, ordered=ordered, list_depth=list_depth
                )
            else:
                idx += 1
        return idx + 1  # skip list_close

    def _consume_list_item(
        self,
        tokens: list[Any],
        idx: int,
        blocks: list[dict[str, Any]],
        *,
        ordered: bool,
        list_depth: int,
    ) -> int:
        idx += 1  # skip list_item_open
        item_blocks: list[dict[str, Any]] = []

        while idx < len(tokens) and tokens[idx].type != "list_item_close":
            tok = tokens[idx]
            if tok.type == "paragraph_open":
                inline_tok = tokens[idx + 1]
                content = inline_tok.content or ""

                # Detect task-list checkbox inside list items.
                todo = self._try_parse_todo(content)
                if todo is not None:
                    item_blocks.append(todo)
                    idx += 3
                    continue

                elements = parse_inline_markdown(content)
                bt = BlockType.ORDERED if ordered else BlockType.BULLET
                body_key = "ordered" if ordered else "bullet"
                item_blocks.append(
                    {
                        "block_type": bt.value,
                        body_key: {"elements": elements},
                    }
                )
                idx += 3
            elif tok.type in ("bullet_list_open", "ordered_list_open"):
                # Nested list -> recurse, children will be appended after
                # the parent item in the flat list.
                nested_ordered = tok.type == "ordered_list_open"
                idx = self._consume_list(
                    tokens,
                    idx,
                    item_blocks,
                    ordered=nested_ordered,
                    list_depth=list_depth + 1,
                )
            else:
                idx += 1

        blocks.extend(item_blocks)
        return idx + 1  # skip list_item_close

    # -- Code block --------------------------------------------------------

    def _consume_code(
        self,
        tokens: list[Any],
        idx: int,
        blocks: list[dict[str, Any]],
    ) -> int:
        tok = tokens[idx]
        language = tok.info.strip() if tok.info else ""
        code_content = tok.content or ""
        # Strip trailing newline that markdown-it includes.
        if code_content.endswith("\n"):
            code_content = code_content[:-1]

        elements = [
            {
                "text_run": {
                    "content": code_content,
                    "text_element_style": {
                        "bold": False,
                        "italic": False,
                        "strikethrough": False,
                        "inline_code": False,
                    },
                }
            }
        ]

        lang_value = _reverse_lang_lookup(language)

        blocks.append(
            {
                "block_type": BlockType.CODE.value,
                "code": {
                    "elements": elements,
                    "style": {"language": lang_value, "wrap": False},
                },
            }
        )
        return idx + 1

    # -- Block quote -------------------------------------------------------

    def _consume_blockquote(
        self,
        tokens: list[Any],
        idx: int,
        blocks: list[dict[str, Any]],
    ) -> int:
        idx += 1  # skip blockquote_open
        children: list[dict[str, Any]] = []
        while idx < len(tokens) and tokens[idx].type != "blockquote_close":
            idx = self._consume_token(tokens, idx, children)
        container_id = _temp_id()
        # Attach children inline.
        blocks.append(
            {
                "block_type": BlockType.QUOTE_CONTAINER.value,
                "quote_container": {},
                "children": children,
            }
        )
        return idx + 1  # skip blockquote_close

    # -- Horizontal rule ---------------------------------------------------

    def _consume_hr(
        self,
        tokens: list[Any],
        idx: int,
        blocks: list[dict[str, Any]],
    ) -> int:
        blocks.append({"block_type": BlockType.DIVIDER.value, "divider": {}})
        return idx + 1

    # -- HTML block (passthrough as TEXT) ----------------------------------

    def _consume_html_block(
        self,
        tokens: list[Any],
        idx: int,
        blocks: list[dict[str, Any]],
    ) -> int:
        tok = tokens[idx]
        content = (tok.content or "").strip()
        if content:
            elements = [
                {
                    "text_run": {
                        "content": content,
                        "text_element_style": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "inline_code": False,
                        },
                    }
                }
            ]
            blocks.append(
                {
                    "block_type": BlockType.TEXT.value,
                    "text": {"elements": elements},
                }
            )
        return idx + 1

    # -- Table -------------------------------------------------------------

    def _consume_table(
        self,
        tokens: list[Any],
        idx: int,
        blocks: list[dict[str, Any]],
    ) -> int:
        """Parse markdown-it table tokens into a TABLE block with TABLE_CELL children."""
        idx += 1  # skip table_open
        rows: list[list[str]] = []
        current_row: list[str] = []

        while idx < len(tokens) and tokens[idx].type != "table_close":
            tok = tokens[idx]
            if tok.type in ("thead_open", "thead_close", "tbody_open", "tbody_close"):
                idx += 1
                continue
            if tok.type == "tr_open":
                current_row = []
                idx += 1
                continue
            if tok.type == "tr_close":
                rows.append(current_row)
                idx += 1
                continue
            if tok.type in ("th_open", "td_open"):
                # Next token is inline content.
                inline_tok = tokens[idx + 1]
                current_row.append(inline_tok.content or "")
                idx += 3  # open, inline, close
                continue
            idx += 1

        idx += 1  # skip table_close

        if not rows:
            return idx

        col_count = max(len(r) for r in rows) if rows else 0
        row_count = len(rows)

        # Build TABLE_CELL children (each cell wraps a TEXT block).
        cell_children: list[dict[str, Any]] = []
        for row in rows:
            for c in range(col_count):
                cell_content = row[c] if c < len(row) else ""
                text_elements = parse_inline_markdown(cell_content)
                text_block = {
                    "block_type": BlockType.TEXT.value,
                    "text": {"elements": text_elements},
                }
                cell_children.append(
                    {
                        "block_type": BlockType.TABLE_CELL.value,
                        "table_cell": {},
                        "children": [text_block],
                    }
                )

        blocks.append(
            {
                "block_type": BlockType.TABLE.value,
                "table": {
                    "property": {
                        "row_size": row_count,
                        "column_size": col_count,
                    },
                },
                "children": cell_children,
            }
        )
        return idx

    # -- Image extraction --------------------------------------------------

    @staticmethod
    def _try_extract_image(inline_tok: Any) -> dict[str, Any] | None:
        """If the inline token contains only an image, return an IMAGE block."""
        if not inline_tok.children:
            return None
        # Filter out softbreak / text-only children.
        image_children = [c for c in inline_tok.children if c.type == "image"]
        if len(image_children) != 1:
            return None
        img = image_children[0]
        src = img.attrGet("src") or ""
        alt = img.attrGet("alt") or img.content or "image"
        return {
            "block_type": BlockType.IMAGE.value,
            "image": {"token": src, "alt": alt},
        }

    # -- TODO checkbox parsing ---------------------------------------------

    @staticmethod
    def _try_parse_todo(content: str) -> dict[str, Any] | None:
        """Parse ``[ ] text`` or ``[x] text`` checkbox syntax."""
        stripped = content.strip()
        done: bool | None = None
        text = ""
        if stripped.startswith("[x] ") or stripped.startswith("[X] "):
            done = True
            text = stripped[4:]
        elif stripped.startswith("[ ] "):
            done = False
            text = stripped[4:]
        else:
            return None

        elements = parse_inline_markdown(text)
        return {
            "block_type": BlockType.TODO.value,
            "todo": {
                "elements": elements,
                "style": {"done": done},
            },
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _temp_id() -> str:
    """Generate a temporary block ID (UUID4 hex) for local use."""
    return uuid.uuid4().hex


# Reverse lookup: language string -> Lark enum int.
# Built from the forward map in ``lark_to_markdown.py`` to avoid duplication
# at module level.  We inline a compact version here.
_REVERSE_LANG_MAP: dict[str, int] = {
    "plaintext": 1,
    "abap": 2,
    "ada": 3,
    "apache": 4,
    "apex": 5,
    "assembly": 6,
    "bash": 7,
    "sh": 7,
    "shell": 60,
    "c": 8,
    "c#": 9,
    "csharp": 9,
    "cs": 9,
    "c++": 10,
    "cpp": 10,
    "cobol": 11,
    "css": 12,
    "coffeescript": 13,
    "d": 14,
    "dart": 15,
    "delphi": 16,
    "django": 17,
    "dockerfile": 18,
    "elixir": 19,
    "erlang": 20,
    "fortran": 21,
    "foxpro": 22,
    "go": 23,
    "golang": 23,
    "groovy": 24,
    "html": 25,
    "haskell": 26,
    "http": 27,
    "json": 28,
    "java": 29,
    "javascript": 30,
    "js": 30,
    "julia": 31,
    "kotlin": 32,
    "kt": 32,
    "latex": 33,
    "tex": 33,
    "lisp": 34,
    "logo": 35,
    "lua": 36,
    "matlab": 37,
    "makefile": 38,
    "make": 38,
    "markdown": 39,
    "md": 39,
    "nginx": 40,
    "objective-c": 41,
    "objc": 41,
    "openedgeabl": 42,
    "perl": 43,
    "php": 44,
    "pl/sql": 45,
    "plsql": 45,
    "powershell": 46,
    "ps1": 46,
    "prolog": 47,
    "protobuf": 48,
    "proto": 48,
    "python": 49,
    "py": 49,
    "r": 50,
    "rpg": 51,
    "ruby": 52,
    "rb": 52,
    "rust": 53,
    "rs": 53,
    "sas": 54,
    "scss": 55,
    "sql": 56,
    "scala": 57,
    "scheme": 58,
    "scratch": 59,
    "swift": 61,
    "thrift": 62,
    "typescript": 63,
    "ts": 63,
    "vbscript": 64,
    "visual-basic": 65,
    "vb": 65,
    "xml": 66,
    "yaml": 67,
    "yml": 67,
}


def _reverse_lang_lookup(language: str) -> int | str:
    """Map a language string to a Lark enum int, or pass through the string."""
    if not language:
        return 1  # plaintext
    lower = language.lower().strip()
    return _REVERSE_LANG_MAP.get(lower, lower)
