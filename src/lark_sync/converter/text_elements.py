"""Bi-directional conversion between Lark TextElement lists and Markdown inline text.

Lark represents rich text as a list of ``TextElement`` dicts.  Each element
carries a ``text_run`` (or ``mention_user`` etc.) with a ``content`` string and
a ``text_element_style`` that toggles **bold**, *italic*, ~~strikethrough~~,
``inline_code``, and hyperlinks.

This module converts between that representation and standard Markdown inline
formatting so the rest of the converter pipeline can work with plain strings.
"""

from __future__ import annotations

import re
from typing import Any


# ---------------------------------------------------------------------------
# Lark TextElements -> Markdown string
# ---------------------------------------------------------------------------

def elements_to_markdown(elements: list[dict[str, Any]]) -> str:
    """Convert a list of Lark ``TextElement`` dicts into a Markdown string.

    Each element is expected to have the shape::

        {
            "text_run": {
                "content": "hello",
                "text_element_style": {
                    "bold": true,
                    "italic": false,
                    "strikethrough": false,
                    "inline_code": false,
                    "link": {"url": "https://..."}
                }
            }
        }

    Unsupported element kinds (``mention_user``, ``equation``, etc.) fall back
    to their ``content`` field with no decoration.
    """
    parts: list[str] = []
    for elem in elements:
        text_run: dict[str, Any] | None = elem.get("text_run")
        if text_run is None:
            # Graceful fallback for mention_user / equation / file etc.
            parts.append(_fallback_content(elem))
            continue

        content: str = text_run.get("content", "")
        if not content:
            continue

        style: dict[str, Any] = text_run.get("text_element_style", {})
        parts.append(_apply_inline_formatting(content, style))

    return "".join(parts)


def _fallback_content(elem: dict[str, Any]) -> str:
    """Extract display text from a non-text_run element."""
    for key in ("mention_user", "equation", "file", "reminder", "undefined"):
        sub = elem.get(key)
        if isinstance(sub, dict):
            return sub.get("content", "")
    return ""


def _apply_inline_formatting(content: str, style: dict[str, Any]) -> str:
    """Wrap *content* with the appropriate Markdown inline markers."""
    # Inline code is exclusive -- no nested bold/italic inside backticks.
    if style.get("inline_code"):
        content = f"`{content}`"
        return _maybe_link(content, style)

    if style.get("bold") and style.get("italic"):
        content = f"***{content}***"
    elif style.get("bold"):
        content = f"**{content}**"
    elif style.get("italic"):
        content = f"*{content}*"

    if style.get("strikethrough"):
        content = f"~~{content}~~"

    return _maybe_link(content, style)


def _maybe_link(content: str, style: dict[str, Any]) -> str:
    """Wrap *content* in a Markdown link if the style contains a URL."""
    link: dict[str, Any] | None = style.get("link")
    if link:
        url = link.get("url", "")
        if url:
            return f"[{content}]({url})"
    return content


# ---------------------------------------------------------------------------
# Markdown string -> Lark TextElements
# ---------------------------------------------------------------------------

# Order matters: longer / more specific patterns must come first so that
# ``***bold+italic***`` is matched before ``**bold**`` or ``*italic*``.
_INLINE_PATTERN = re.compile(
    r"(?P<bold_italic>\*\*\*(?P<bi_text>.+?)\*\*\*)"          # ***text***
    r"|(?P<bold>\*\*(?P<b_text>.+?)\*\*)"                     # **text**
    r"|(?P<italic>\*(?P<i_text>.+?)\*)"                        # *text*
    r"|(?P<strikethrough>~~(?P<s_text>.+?)~~)"                 # ~~text~~
    r"|(?P<inline_code>`(?P<c_text>[^`]+?)`)"                  # `text`
    r"|(?P<link>\[(?P<l_text>[^\]]*?)\]\((?P<l_url>[^)]+?)\))" # [text](url)
)


def parse_inline_markdown(text: str) -> list[dict[str, Any]]:
    """Parse a Markdown inline string into a list of Lark ``TextElement`` dicts.

    Handles bold, italic, bold+italic, strikethrough, inline code, and links.
    Nested formatting (e.g. ``**bold *and italic***``) is supported via
    recursive descent on the inner content of each top-level match.

    Returns a list of dicts with the canonical ``text_run`` shape expected by
    the Lark document API.
    """
    if not text:
        return []

    elements: list[dict[str, Any]] = []
    last_end = 0

    for m in _INLINE_PATTERN.finditer(text):
        # Plain text before the current match.
        if m.start() > last_end:
            plain = text[last_end : m.start()]
            if plain:
                elements.append(_make_text_element(plain))

        if m.group("bold_italic"):
            inner = m.group("bi_text")
            # Recurse for any nested patterns inside the bold+italic span.
            for child in _ensure_elements(inner):
                _merge_style(child, bold=True, italic=True)
                elements.append(child)

        elif m.group("bold"):
            inner = m.group("b_text")
            for child in _ensure_elements(inner):
                _merge_style(child, bold=True)
                elements.append(child)

        elif m.group("italic"):
            inner = m.group("i_text")
            for child in _ensure_elements(inner):
                _merge_style(child, italic=True)
                elements.append(child)

        elif m.group("strikethrough"):
            inner = m.group("s_text")
            for child in _ensure_elements(inner):
                _merge_style(child, strikethrough=True)
                elements.append(child)

        elif m.group("inline_code"):
            code_text = m.group("c_text")
            elements.append(_make_text_element(code_text, inline_code=True))

        elif m.group("link"):
            link_text = m.group("l_text")
            link_url = m.group("l_url")
            # The visible text inside the link may itself contain formatting.
            for child in _ensure_elements(link_text):
                _merge_style(child, link_url=link_url)
                elements.append(child)

        last_end = m.end()

    # Trailing plain text after the last match.
    if last_end < len(text):
        trailing = text[last_end:]
        if trailing:
            elements.append(_make_text_element(trailing))

    # If nothing matched at all, return the whole string as a plain element.
    if not elements:
        elements.append(_make_text_element(text))

    return elements


# ---------------------------------------------------------------------------
# Internal helpers for building TextElement dicts
# ---------------------------------------------------------------------------

def _make_text_element(
    content: str,
    *,
    bold: bool = False,
    italic: bool = False,
    strikethrough: bool = False,
    inline_code: bool = False,
    link_url: str | None = None,
) -> dict[str, Any]:
    """Build a single Lark ``TextElement`` dict."""
    style: dict[str, Any] = {
        "bold": bold,
        "italic": italic,
        "strikethrough": strikethrough,
        "inline_code": inline_code,
    }
    if link_url:
        style["link"] = {"url": link_url}

    return {
        "text_run": {
            "content": content,
            "text_element_style": style,
        }
    }


def _merge_style(
    element: dict[str, Any],
    *,
    bold: bool = False,
    italic: bool = False,
    strikethrough: bool = False,
    inline_code: bool = False,
    link_url: str | None = None,
) -> None:
    """Merge additional style flags into an existing ``TextElement`` **in-place**."""
    style = element["text_run"]["text_element_style"]
    if bold:
        style["bold"] = True
    if italic:
        style["italic"] = True
    if strikethrough:
        style["strikethrough"] = True
    if inline_code:
        style["inline_code"] = True
    if link_url:
        style["link"] = {"url": link_url}


def _ensure_elements(text: str) -> list[dict[str, Any]]:
    """Recursively parse *text* into elements, guaranteeing at least one."""
    result = parse_inline_markdown(text)
    if not result:
        result = [_make_text_element(text)]
    return result
