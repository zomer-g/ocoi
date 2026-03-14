"""CLI entry point for the local processor.

Usage:
    python -m tools.local_processor run [--limit N] [--skip-extract]
    python -m tools.local_processor import [--limit N] [--query "..."]
    python -m tools.local_processor convert [--limit N]
    python -m tools.local_processor extract [--limit N]
    python -m tools.local_processor push [--limit N] [--skip-extract]
    python -m tools.local_processor status
    python -m tools.local_processor reset
"""

import asyncio
import sys

import click

from .config import load_config
from . import state as st


@click.group()
def cli():
    """Local processor — import, convert, extract, push documents."""
    pass


@cli.command("run")
@click.option("--limit", type=int, default=None, help="Max documents to process")
@click.option("--query", type=str, default=None, help="Override CKAN search query")
@click.option("--skip-extract", is_flag=True, help="Push without extraction")
def run_all(limit, query, skip_extract):
    """Run all phases: import → convert → extract → push."""
    cfg = load_config()
    errors = cfg.validate()
    if not skip_extract and "DEEPSEEK_API_KEY" in " ".join(errors):
        print("Warning: DEEPSEEK_API_KEY not set — extraction will be skipped")
    if "PUSH_API_KEY" in " ".join(errors):
        print("Error: PUSH_API_KEY is required")
        sys.exit(1)

    async def _run():
        from .ckan_fetch import run_import
        from .convert import run_convert
        from .extract import run_extract
        from .push import run_push

        print("=" * 60)
        print("  Local Processor — Full Pipeline")
        print("=" * 60)

        downloaded = await run_import(cfg, limit=limit, query=query)
        converted = run_convert(limit=None)  # Convert all downloaded

        if not skip_extract and cfg.deepseek_api_key:
            extracted = await run_extract(cfg, limit=None)
        else:
            print("\n=== Extract Phase (skipped) ===")
            extracted = 0

        pushed = await run_push(cfg, skip_extract=skip_extract or not cfg.deepseek_api_key)

        print("\n" + "=" * 60)
        print(f"  Summary: {downloaded} downloaded, {converted} converted, "
              f"{extracted} extracted, {pushed} pushed")
        print("=" * 60)

    asyncio.run(_run())


@cli.command("import")
@click.option("--limit", type=int, default=None, help="Max documents to download")
@click.option("--query", type=str, default=None, help="Override CKAN search query")
def cmd_import(limit, query):
    """Search CKAN and download PDFs."""
    cfg = load_config()

    async def _run():
        from .ckan_fetch import run_import
        await run_import(cfg, limit=limit, query=query)

    asyncio.run(_run())


@cli.command("convert")
@click.option("--limit", type=int, default=None, help="Max documents to convert")
def cmd_convert(limit):
    """Convert downloaded PDFs to markdown."""
    from .convert import run_convert
    run_convert(limit=limit)


@cli.command("extract")
@click.option("--limit", type=int, default=None, help="Max documents to extract")
def cmd_extract(limit):
    """Extract entities using DeepSeek API."""
    cfg = load_config()
    if not cfg.deepseek_api_key:
        print("Error: DEEPSEEK_API_KEY is required for extraction")
        sys.exit(1)

    async def _run():
        from .extract import run_extract
        await run_extract(cfg, limit=limit)

    asyncio.run(_run())


@cli.command("push")
@click.option("--limit", type=int, default=None, help="Max documents to push")
@click.option("--skip-extract", is_flag=True, help="Push converted docs without extraction")
def cmd_push(limit, skip_extract):
    """Push processed documents to the server."""
    cfg = load_config()
    if not cfg.push_api_key:
        print("Error: PUSH_API_KEY is required")
        sys.exit(1)

    async def _run():
        from .push import run_push
        await run_push(cfg, skip_extract=skip_extract, limit=limit)

    asyncio.run(_run())


@cli.command("status")
def cmd_status():
    """Show processing status summary."""
    state = st.load_state()
    if not state:
        print("No documents tracked yet.")
        return

    counts = st.summary(state)
    total = sum(counts.values())
    print(f"\nTotal tracked: {total}")
    print("-" * 30)
    for status, count in sorted(counts.items()):
        print(f"  {status:15s} {count:5d}")
    print()


@cli.command("reset")
@click.confirmation_option(prompt="Delete all local state?")
def cmd_reset():
    """Clear local state (does NOT delete cached files)."""
    st.reset_state()
    print("State cleared.")


if __name__ == "__main__":
    cli()
