"""Generic / placeholder names that should never be treated as real
entities or shown as graph nodes.

Two effects:

1. **At rest** — every Person/Company/Association/Domain row whose
   `name_hebrew` matches one of these strings has `hidden=true`. The
   public API filters these out of search results, top-connected
   listings, entity-detail pages, and any graph payload — relationships
   that touch a hidden entity are dropped entirely so the graph never
   uses them as connectors.

2. **At import time** — every importer (MK expenses, LLM extractor)
   checks names against this list before upserting. A blocked name
   never produces a new entity / relationship.

Add new strings here when you spot another category-label or
generic-placeholder polluting the data.
"""

DOMAIN_BLOCKLIST: frozenset[str] = frozenset({
    # "personal matters" catch-alls
    "עניינים אישיים",
    "ענייני קרובי משפחה",
    "קרובי משפחה",
    "ענייני האישים",
    "נושאים אישיים",
    "ענייני משפחה",
    "נושאים משפחתיים",
    "ענייני אדם אחר עם זיקה",
    # generic abstractions
    "כללי",
    "מידע תפקידי",
    "הגבלות לאחר פרישה",
    "עסקים",
    "עסקים קודמים",
    "עיסוקים קודמים",
})

COMPANY_BLOCKLIST: frozenset[str] = frozenset({
    # MK-expenses category labels masquerading as suppliers
    "פרסומים",
    "טלפונים ניידים",
    "תרגומים והדפסות",
    "אירוח וכיבודים",
    "דואר ומשלוחים",
    "שכר עבודה בלתי צמית",
    "שכר בארץ",
    # internal Knesset accounting ledgers
    "חשבון התאמה לרכש פרי",
    "חשבון מעבר ניכוי עוב",
    "חשבון מעבר",
    # broken / null records
    "null",
    "NULL",
    "None",
})

ASSOCIATION_BLOCKLIST: frozenset[str] = frozenset()

PERSON_BLOCKLIST: frozenset[str] = frozenset()


_BY_TYPE: dict[str, frozenset[str]] = {
    "person": PERSON_BLOCKLIST,
    "company": COMPANY_BLOCKLIST,
    "association": ASSOCIATION_BLOCKLIST,
    "domain": DOMAIN_BLOCKLIST,
}


def is_blocked(entity_type: str, name: str | None) -> bool:
    """Return True if the (entity_type, name) pair is on the canonical
    block list. Whitespace is normalised before comparison."""
    if not name:
        return False
    bl = _BY_TYPE.get(entity_type)
    if bl is None:
        return False
    return " ".join(name.split()) in bl


def blocklist_for(entity_type: str) -> frozenset[str]:
    return _BY_TYPE.get(entity_type, frozenset())
