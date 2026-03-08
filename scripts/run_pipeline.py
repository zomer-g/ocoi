"""Full pipeline orchestrator: Import -> Convert -> Extract -> Match.

Usage:
    python -m uv run scripts/run_pipeline.py --steps all
    python -m uv run scripts/run_pipeline.py --steps import,convert
"""

import asyncio
import sys
from pathlib import Path

# Add packages to path
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "ocoi-common" / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "ocoi-db" / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "ocoi-importer" / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "ocoi-converter" / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "ocoi-extractor" / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "packages" / "ocoi-matcher" / "src"))

import click
from ocoi_common.config import settings
from ocoi_common.logging import setup_logging

logger = setup_logging("ocoi.pipeline")


@click.command()
@click.option("--steps", default="all", help="Comma-separated: import,convert,extract,match,all")
@click.option("--limit", type=int, default=0, help="Max documents per step (0=all)")
@click.option("--source", default="all", help="Import source: ckan,govil,all")
def run_pipeline(steps: str, limit: int, source: str):
    """Run the full data processing pipeline."""
    settings.ensure_dirs()
    step_list = steps.split(",") if steps != "all" else ["import", "convert", "extract", "match"]
    asyncio.run(_run(step_list, limit, source))


async def _run(steps: list[str], limit: int, source: str):
    logger.info(f"Pipeline starting: steps={steps}, limit={limit}")

    if "import" in steps:
        logger.info("=== Step 1: IMPORT ===")
        from ocoi_importer.cli import _import
        await _import(source, limit, download=True)

    if "convert" in steps:
        logger.info("=== Step 2: CONVERT ===")
        from ocoi_converter.cli import _convert_pending
        await _convert_pending(limit or 1000)

    if "extract" in steps:
        logger.info("=== Step 3: EXTRACT ===")
        from ocoi_extractor.cli import _extract_pending
        await _extract_pending(limit or 1000, use_llm=True, use_ner=True)

    if "match" in steps:
        logger.info("=== Step 4: MATCH ===")
        from ocoi_matcher.cli import _match_unmatched
        await _match_unmatched(threshold=0.7, limit=limit or 500)

    # Print stats
    from ocoi_db.engine import async_session_factory
    from ocoi_db.crud import count_entities
    async with async_session_factory() as session:
        stats = await count_entities(session)
        logger.info(f"=== PIPELINE COMPLETE ===")
        for key, count in stats.items():
            logger.info(f"  {key}: {count}")


if __name__ == "__main__":
    run_pipeline()
