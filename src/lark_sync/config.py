from __future__ import annotations

import os


class Settings:
    """Application settings loaded from environment variables."""

    def __init__(self) -> None:
        self.app_id: str = os.environ.get("LARK_APP_ID", "")
        self.app_secret: str = os.environ.get("LARK_APP_SECRET", "")
        self.domain: str = os.environ.get("LARK_DOMAIN", "https://open.larksuite.com")
        self.sync_dir: str = os.environ.get("LARK_SYNC_DIR", "./lark-docs/")
        self.sync_state_file: str = os.environ.get(
            "LARK_SYNC_STATE_FILE", ".lark-sync-state.json"
        )

    def validate(self) -> None:
        if not self.app_id:
            raise ValueError("LARK_APP_ID environment variable is required")
        if not self.app_secret:
            raise ValueError("LARK_APP_SECRET environment variable is required")


settings = Settings()
