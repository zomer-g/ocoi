"""CLI entry point for entity extraction."""

import asyncio

import click

from ocoi_common.logging import setup_logging
from ocoi_db.engine import async_session_factory
from ocoi_db.crud import (
    get_documents_by_status,
    upsert_person,
    upsert_company,
    upsert_association,
    upsert_domain,
    create_relationship,
    create_extraction_run,
)

logger = setup_logging("ocoi.extractor")


@click.group()
def cli():
    """Extract entities from converted documents."""
    pass


@cli.command()
@click.option("--limit", type=int, default=100)
@click.option("--use-llm/--no-llm", default=True, help="Use DeepSeek LLM extraction")
@click.option("--use-ner/--no-ner", default=True, help="Use DictaBERT NER extraction")
def extract_pending(limit: int, use_llm: bool, use_ner: bool):
    """Extract entities from all documents with conversion_status=converted."""
    asyncio.run(_extract_pending(limit, use_llm, use_ner))


async def _extract_pending(limit: int, use_llm: bool, use_ner: bool):
    from ocoi_extractor.entity_merger import merge_results
    from ocoi_common.models import ExtractionResult

    ner_extractor = None
    llm_extractor = None

    if use_ner:
        from ocoi_extractor.dictabert_ner import DictaBertNER
        ner_extractor = DictaBertNER()

    if use_llm:
        from ocoi_extractor.llm_extractor import LLMExtractor
        llm_extractor = LLMExtractor()

    async with async_session_factory() as session:
        docs = await get_documents_by_status(session, "extraction_status", "pending", limit)
        # Only process documents that have been converted
        docs = [d for d in docs if d.conversion_status == "converted" and d.markdown_content]
        logger.info(f"Found {len(docs)} documents to extract entities from")

        for i, doc in enumerate(docs):
            try:
                text = doc.markdown_content

                # Run extractors
                ner_entities = []
                llm_result = ExtractionResult()

                if ner_extractor:
                    ner_entities = ner_extractor.extract(text)
                    logger.info(f"  NER found {len(ner_entities)} entities")

                if llm_extractor:
                    llm_result = await llm_extractor.extract(text)
                    logger.info(
                        f"  LLM found {len(llm_result.persons)}P, "
                        f"{len(llm_result.companies)}C, "
                        f"{len(llm_result.relationships)}R"
                    )

                # Merge results
                merged = merge_results(ner_entities, llm_result)

                # Save entities to database
                entity_id_map = {}

                for person in merged.persons:
                    db_person = await upsert_person(
                        session,
                        name_hebrew=person.name_hebrew,
                        name_english=person.name_english,
                        title=person.title,
                        position=person.position,
                        ministry=person.ministry,
                    )
                    entity_id_map[("person", person.name_hebrew)] = db_person.id

                for company in merged.companies:
                    db_company = await upsert_company(
                        session,
                        name_hebrew=company.name_hebrew,
                        name_english=company.name_english,
                        company_type=company.company_type,
                    )
                    entity_id_map[("company", company.name_hebrew)] = db_company.id

                for assoc in merged.associations:
                    db_assoc = await upsert_association(
                        session,
                        name_hebrew=assoc.name_hebrew,
                        registration_number=assoc.registration_number,
                    )
                    entity_id_map[("association", assoc.name_hebrew)] = db_assoc.id

                for domain in merged.domains:
                    db_domain = await upsert_domain(
                        session,
                        name_hebrew=domain.name_hebrew,
                    )
                    entity_id_map[("domain", domain.name_hebrew)] = db_domain.id

                # Save relationships
                rels_saved = 0
                for rel in merged.relationships:
                    src_id = entity_id_map.get((rel.source_type.value, rel.source_name))
                    tgt_id = entity_id_map.get((rel.target_type.value, rel.target_name))
                    if src_id and tgt_id:
                        await create_relationship(
                            session,
                            source_entity_type=rel.source_type.value,
                            source_entity_id=src_id,
                            target_entity_type=rel.target_type.value,
                            target_entity_id=tgt_id,
                            relationship_type=rel.relationship_type.value,
                            document_id=doc.id,
                            details=rel.details,
                            restriction_type=rel.restriction_type.value if rel.restriction_type else None,
                            restriction_end_date=rel.restriction_end_date,
                            confidence=rel.confidence,
                        )
                        rels_saved += 1

                # Record extraction run
                await create_extraction_run(
                    session,
                    document_id=doc.id,
                    extractor_type="merged" if (use_ner and use_llm) else ("llm" if use_llm else "ner"),
                    entities_found=len(merged.persons) + len(merged.companies) + len(merged.associations),
                    relationships_found=rels_saved,
                    raw_output_json=merged.model_dump(),
                )

                doc.extraction_status = "extracted"
                logger.info(
                    f"[{i+1}/{len(docs)}] Extracted: {doc.title} "
                    f"({len(merged.persons)}P, {len(merged.companies)}C, {rels_saved}R)"
                )

            except Exception as e:
                logger.error(f"Failed to extract from {doc.title}: {e}")
                doc.extraction_status = "failed"

        await session.commit()
    logger.info("Extraction complete")


if __name__ == "__main__":
    cli()
