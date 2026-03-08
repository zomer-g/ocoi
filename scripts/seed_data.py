"""Seed the database with sample data so the site is usable before running the full pipeline."""

import asyncio
import sys
from pathlib import Path

# Add packages to path
for pkg_dir in Path(__file__).parent.parent.joinpath("packages").iterdir():
    src = pkg_dir / "src"
    if src.exists():
        sys.path.insert(0, str(src))

from ocoi_common.config import settings
from ocoi_common.logging import setup_logging

logger = setup_logging("ocoi.seed")


async def seed():
    settings.ensure_dirs()

    from ocoi_db.engine import create_all_tables, async_session_factory
    from ocoi_db.crud import (
        get_or_create_source,
        create_document,
        upsert_person,
        upsert_company,
        upsert_association,
        upsert_domain,
        create_relationship,
    )

    await create_all_tables()
    logger.info("Tables created")

    async with async_session_factory() as session:
        # Create a sample source
        src = await get_or_create_source(
            session,
            source_type="govil",
            source_id="demo_source_1",
            title="הסדרי ניגוד עניינים - לדוגמה",
            url="https://www.gov.il/he/departments/dynamiccollectors/ministers_conflict",
        )

        # Create sample documents
        doc1 = await create_document(
            session,
            source_id=src.id,
            title="הסדר ניגוד עניינים - ישראל ישראלי",
            file_url="https://example.gov.il/coi-doc-1.pdf",
        )
        doc1.conversion_status = "converted"
        doc1.extraction_status = "extracted"
        doc1.markdown_content = "הסדר למניעת ניגוד עניינים עבור השר ישראל ישראלי..."

        doc2 = await create_document(
            session,
            source_id=src.id,
            title="הסדר ניגוד עניינים - שרה כהן",
            file_url="https://example.gov.il/coi-doc-2.pdf",
        )
        doc2.conversion_status = "converted"
        doc2.extraction_status = "extracted"

        # Create sample persons
        p1 = await upsert_person(session, name_hebrew="ישראל ישראלי", title="שר", position="שר האוצר", ministry="משרד האוצר")
        p2 = await upsert_person(session, name_hebrew="שרה כהן", title="שרה", position="שרת התקשורת", ministry="משרד התקשורת")
        p3 = await upsert_person(session, name_hebrew="דוד לוי", title="מנכ\"ל", position="מנכ\"ל משרד הביטחון", ministry="משרד הביטחון")
        p4 = await upsert_person(session, name_hebrew="רחל גולד", position="בת זוג")

        # Create sample companies
        c1 = await upsert_company(session, name_hebrew="חברת אנרגיה בע\"מ", registration_number="51-234567-8", company_type="חברה פרטית", status="פעילה")
        c2 = await upsert_company(session, name_hebrew="טכנולוגיות מתקדמות בע\"מ", registration_number="51-345678-9", company_type="חברה ציבורית", status="פעילה")
        c3 = await upsert_company(session, name_hebrew="נדל\"ן ישראל בע\"מ", company_type="חברה פרטית")
        c4 = await upsert_company(session, name_hebrew="תקשורת גלובל בע\"מ", registration_number="51-456789-0", company_type="חברה ציבורית", status="פעילה")
        c5 = await upsert_company(session, name_hebrew="בנק הפועלים", registration_number="52-000123-4", company_type="חברה ציבורית", status="פעילה")

        # Create sample associations
        a1 = await upsert_association(session, name_hebrew="עמותת קידום החינוך", registration_number="58-012345-6")
        a2 = await upsert_association(session, name_hebrew="ארגון למען הסביבה")

        # Create sample domains
        d1 = await upsert_domain(session, name_hebrew="אנרגיה")
        d2 = await upsert_domain(session, name_hebrew="תקשורת")
        d3 = await upsert_domain(session, name_hebrew="נדל\"ן")
        d4 = await upsert_domain(session, name_hebrew="בנקאות")
        d5 = await upsert_domain(session, name_hebrew="טכנולוגיה")

        # Create relationships
        # ישראל ישראלי -> restricted from חברת אנרגיה
        await create_relationship(session, "person", p1.id, "company", c1.id, "restricted_from", doc1.id,
                                  details="מנוע מלטפל בענייני חברת אנרגיה", restriction_type="full", confidence=0.95)
        # ישראל ישראלי -> owns נדל"ן ישראל
        await create_relationship(session, "person", p1.id, "company", c3.id, "owns", doc1.id,
                                  details="בעלות של 25%", confidence=0.9)
        # ישראל ישראלי -> operates in אנרגיה
        await create_relationship(session, "person", p1.id, "domain", d1.id, "operates_in", doc1.id, confidence=0.8)
        # ישראל ישראלי -> operates in נדל"ן
        await create_relationship(session, "person", p1.id, "domain", d3.id, "operates_in", doc1.id, confidence=0.8)
        # ישראל ישראלי -> family member רחל גולד
        await create_relationship(session, "person", p1.id, "person", p4.id, "family_member", doc1.id,
                                  details="בת זוג", confidence=0.95)
        # רחל גולד -> manages טכנולוגיות מתקדמות
        await create_relationship(session, "person", p4.id, "company", c2.id, "manages", doc1.id,
                                  details="מנהלת", confidence=0.85)
        # שרה כהן -> restricted from תקשורת גלובל
        await create_relationship(session, "person", p2.id, "company", c4.id, "restricted_from", doc2.id,
                                  details="מנועה מלטפל בענייני חברה", restriction_type="full", confidence=0.9)
        # שרה כהן -> board member עמותת קידום החינוך
        await create_relationship(session, "person", p2.id, "association", a1.id, "board_member", doc2.id, confidence=0.85)
        # שרה כהן -> operates in תקשורת
        await create_relationship(session, "person", p2.id, "domain", d2.id, "operates_in", doc2.id, confidence=0.8)
        # דוד לוי -> employed_by טכנולוגיות מתקדמות (previously)
        await create_relationship(session, "person", p3.id, "company", c2.id, "employed_by", doc1.id,
                                  details="עבד בחברה לפני המינוי", confidence=0.85)
        # דוד לוי -> restricted from טכנולוגיות מתקדמות
        await create_relationship(session, "person", p3.id, "company", c2.id, "restricted_from", doc1.id,
                                  details="תקופת צינון", restriction_type="cooling_off", confidence=0.9)
        # דוד לוי -> operates in טכנולוגיה
        await create_relationship(session, "person", p3.id, "domain", d5.id, "operates_in", doc1.id, confidence=0.7)
        # חברת אנרגיה -> related to ארגון למען הסביבה
        await create_relationship(session, "company", c1.id, "association", a2.id, "related_to", doc1.id, confidence=0.6)
        # בנק הפועלים -> related to ישראל ישראלי
        await create_relationship(session, "person", p1.id, "company", c5.id, "related_to", doc1.id,
                                  details="חשבונות ונכסים פיננסיים", confidence=0.7)
        # ישראל ישראלי -> operates in בנקאות
        await create_relationship(session, "person", p1.id, "domain", d4.id, "operates_in", doc1.id, confidence=0.7)

        await session.commit()

    logger.info("Seed data inserted successfully!")
    logger.info("  4 persons, 5 companies, 2 associations, 5 domains, 15 relationships")


if __name__ == "__main__":
    asyncio.run(seed())
