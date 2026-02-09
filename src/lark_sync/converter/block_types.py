"""Enum mapping for all Lark document block types.

The integer values correspond to the ``block_type`` field returned by
the Lark Open API (``/open-apis/docx/v1/documents/:document_id/blocks``).
"""

from __future__ import annotations

from enum import IntEnum


class BlockType(IntEnum):
    """Lark document block type identifiers."""

    PAGE = 1
    TEXT = 2
    HEADING1 = 3
    HEADING2 = 4
    HEADING3 = 5
    HEADING4 = 6
    HEADING5 = 7
    HEADING6 = 8
    HEADING7 = 9
    HEADING8 = 10
    HEADING9 = 11
    BULLET = 12
    ORDERED = 13
    CODE = 14
    QUOTE = 15
    TODO = 17
    BITABLE = 18
    CALLOUT = 19
    DIVIDER = 22
    FILE = 23
    GRID = 24
    GRID_COLUMN = 25
    IFRAME = 26
    IMAGE = 27
    SHEET = 30
    TABLE = 31
    TABLE_CELL = 32
    VIEW = 33
    QUOTE_CONTAINER = 34
    UNDEFINED = 999

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    @classmethod
    def heading_types(cls) -> frozenset[BlockType]:
        """Return the set of all heading block types."""
        return frozenset(
            {
                cls.HEADING1,
                cls.HEADING2,
                cls.HEADING3,
                cls.HEADING4,
                cls.HEADING5,
                cls.HEADING6,
                cls.HEADING7,
                cls.HEADING8,
                cls.HEADING9,
            }
        )

    @classmethod
    def heading_level(cls, block_type: BlockType | int) -> int | None:
        """Return the heading level (1-9) for a heading block type, or ``None``."""
        try:
            bt = cls(block_type)
        except ValueError:
            return None
        if bt in cls.heading_types():
            return bt.value - cls.HEADING1.value + 1
        return None

    @classmethod
    def from_value(cls, value: int) -> BlockType:
        """Resolve an integer to a ``BlockType``, falling back to ``UNDEFINED``."""
        try:
            return cls(value)
        except ValueError:
            return cls.UNDEFINED
