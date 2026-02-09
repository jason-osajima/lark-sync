"""CLI entrypoint for lark-sync, designed for GitHub Actions usage.

Provides commands to sync local Markdown files to Lark documents
without needing the MCP server running.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import click

from lark_sync.converter import LarkToMarkdownConverter, MarkdownToLarkConverter
from lark_sync.lark_client import LarkClient
from lark_sync.sync.engine import SyncEngine
from lark_sync.sync.state import SyncStateManager


def _build_engine(project_root: Path) -> SyncEngine:
    """Construct a SyncEngine with a project-local state manager."""
    state_file = project_root / SyncEngine.PROJECT_STATE_FILENAME
    state_mgr = SyncStateManager(str(state_file), project_root=project_root)

    client = LarkClient()
    return SyncEngine(
        lark_client=client,
        state_manager=state_mgr,
        lark_to_md_converter=LarkToMarkdownConverter(),
        md_to_lark_converter=MarkdownToLarkConverter(),
    )


@click.group()
def cli() -> None:
    """lark-sync CLI — sync Markdown files to Lark documents."""


@cli.command()
@click.option("--local-path", required=True, help="Path to the local .md file.")
@click.option("--document-id", default=None, help="Target Lark document ID.")
@click.option("--force", is_flag=True, help="Overwrite remote even on conflict.")
def sync_to_lark(local_path: str, document_id: str | None, force: bool) -> None:
    """Push a single Markdown file to its mapped Lark document."""
    path = Path(local_path).resolve()
    project_root = _find_project_root(path)
    if project_root is None:
        click.echo(f"Error: no .lark-sync.json found for {local_path}", err=True)
        sys.exit(1)

    engine = _build_engine(project_root)
    result = engine.sync_to_lark(
        local_path=str(path),
        document_id=document_id,
        force=force,
    )

    if result.success:
        click.echo(f"OK: {local_path} -> {result.document_id}")
    else:
        click.echo(f"FAIL: {local_path} — {result.message}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--base-ref",
    default="HEAD~1",
    help="Git ref to diff against (default: HEAD~1).",
)
def sync_changed(base_ref: str) -> None:
    """Detect changed tracked files and sync them to Lark.

    Reads .lark-sync.json from the current directory's Git root,
    runs git diff to find changed files, and syncs any that have
    a mapping.
    """
    cwd = Path.cwd()
    project_root = _find_project_root(cwd)
    if project_root is None:
        click.echo("Error: no .lark-sync.json found in this repository.", err=True)
        sys.exit(1)

    state_file = project_root / SyncEngine.PROJECT_STATE_FILENAME
    raw = json.loads(state_file.read_text(encoding="utf-8"))
    tracked_paths = {m["local_path"] for m in raw.get("mappings", [])}

    if not tracked_paths:
        click.echo("No tracked mappings in .lark-sync.json — nothing to sync.")
        return

    # Get changed files from git diff.
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", base_ref, "HEAD"],
            capture_output=True,
            text=True,
            cwd=str(project_root),
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        click.echo(f"Error running git diff: {exc.stderr}", err=True)
        sys.exit(1)

    changed_files = {
        line.strip() for line in result.stdout.splitlines() if line.strip()
    }

    # Find intersection: changed files that are tracked.
    to_sync = tracked_paths & changed_files
    if not to_sync:
        click.echo("No tracked files changed — nothing to sync.")
        return

    click.echo(f"Syncing {len(to_sync)} changed file(s)...")

    engine = _build_engine(project_root)
    failures = 0

    for rel_path in sorted(to_sync):
        abs_path = str(project_root / rel_path)
        sync_result = engine.sync_to_lark(local_path=abs_path, force=True)
        if sync_result.success:
            click.echo(f"  OK: {rel_path} -> {sync_result.document_id}")
        else:
            click.echo(f"  FAIL: {rel_path} — {sync_result.message}", err=True)
            failures += 1

    if failures:
        click.echo(f"\n{failures} file(s) failed to sync.", err=True)
        sys.exit(failures)
    else:
        click.echo(f"\nAll {len(to_sync)} file(s) synced successfully.")


def _find_project_root(start: Path) -> Path | None:
    """Walk up from *start* to find a directory with .lark-sync.json."""
    current = start.resolve()
    if current.is_file():
        current = current.parent
    for parent in [current, *current.parents]:
        if (parent / SyncEngine.PROJECT_STATE_FILENAME).exists():
            return parent
    return None


if __name__ == "__main__":
    cli()
