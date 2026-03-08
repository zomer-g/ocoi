"""CLI entry point for corporation matching."""

import asyncio

import click

from ocoi_common.logging import setup_logging
from ocoi_db.engine import async_session_factory

logger = setup_logging("ocoi.matcher")


@click.group()
def cli():
    """Match companies to official registration numbers."""
    pass


@cli.command()
@click.option("--name", required=True, help="Company name to search for")
def match(name: str):
    """Match a single company name to its registration number."""
    asyncio.run(_match_one(name))


async def _match_one(name: str):
    from ocoi_matcher.opencorporates import OpenCorporatesClient
    from ocoi_matcher.fuzzy_match import find_best_match

    client = OpenCorporatesClient()
    results = await client.search_company(name)

    if not results:
        click.echo(f"No results found for: {name}")
        return

    match = find_best_match(name, results, name_field="name")
    if match:
        click.echo(f"Match: {match.get('name')}")
        click.echo(f"  Registration: {match.get('company_number')}")
        click.echo(f"  Status: {match.get('current_status')}")
        click.echo(f"  Score: {match.get('_match_score', 0):.2f}")
    else:
        click.echo("No confident match found. Candidates:")
        for r in results[:3]:
            click.echo(f"  - {r.get('name')} ({r.get('company_number')})")


@cli.command()
@click.option("--threshold", type=float, default=0.7, help="Minimum match confidence")
@click.option("--limit", type=int, default=100, help="Max companies to process")
def match_unmatched(threshold: float, limit: int):
    """Match all companies in the database that don't have registration numbers."""
    asyncio.run(_match_unmatched(threshold, limit))


async def _match_unmatched(threshold: float, limit: int):
    from sqlalchemy import select
    from ocoi_db.models import Company
    from ocoi_matcher.opencorporates import OpenCorporatesClient
    from ocoi_matcher.fuzzy_match import find_best_match

    client = OpenCorporatesClient()

    async with async_session_factory() as session:
        result = await session.execute(
            select(Company)
            .where(Company.registration_number.is_(None))
            .limit(limit)
        )
        companies = result.scalars().all()
        logger.info(f"Found {len(companies)} companies without registration numbers")

        matched = 0
        for i, company in enumerate(companies):
            results = await client.search_company(company.name_hebrew)
            if results:
                best = find_best_match(company.name_hebrew, results, name_field="name", threshold=threshold)
                if best:
                    company.registration_number = best.get("company_number")
                    company.match_confidence = best.get("_match_score")
                    company.status = best.get("current_status")
                    matched += 1
                    logger.info(
                        f"[{i+1}/{len(companies)}] Matched: {company.name_hebrew} "
                        f"-> {company.registration_number} ({company.match_confidence:.2f})"
                    )

            # Rate limiting
            await asyncio.sleep(0.5)

        await session.commit()
    logger.info(f"Matching complete: {matched}/{len(companies)} companies matched")


if __name__ == "__main__":
    cli()
