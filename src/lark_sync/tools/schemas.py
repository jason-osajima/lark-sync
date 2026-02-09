"""Pydantic models for MCP tool inputs and outputs."""

from __future__ import annotations

from pydantic import BaseModel, Field


class DocumentInfoResponse(BaseModel):
    """Response for document info operations."""

    document_id: str
    title: str
    url: str = ""
    action: str = ""  # "created" | "updated"


class DocumentListItem(BaseModel):
    """Single item in a document listing."""

    document_id: str = ""
    title: str = ""
    doc_type: str = ""
    url: str = ""
    owner_id: str = ""


class SyncResultResponse(BaseModel):
    """Response from a sync operation."""

    success: bool
    message: str
    document_id: str = ""
    document_url: str = ""
    local_path: str = ""
    conflict: str = ""
    diff_summary: str = ""


class SyncStatusResponse(BaseModel):
    """Response for sync status check."""

    entries: list[SyncStatusEntryResponse] = Field(default_factory=list)


class SyncStatusEntryResponse(BaseModel):
    """Single entry in a sync status response."""

    local_path: str
    document_id: str
    document_url: str = ""
    status: str  # in_sync | local_ahead | remote_ahead | conflict | unlinked
    last_synced: str | None = None
