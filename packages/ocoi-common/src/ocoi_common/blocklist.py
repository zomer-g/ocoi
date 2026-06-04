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
    # CoI declaration template phrasings — these aren't domains, they're
    # boilerplate descriptions that the extractor mistakenly elevated to
    # an entity. Add new variants here when more surface in the data.
    "ענייני אדם אחר שיש לו זיקה אישית, פוליטית, כלכלית או עסקית",
    "עניינים קונקרטיים שטופלו/מטופלים ע\"י אחיו או משרדו",
    "נושאים מטופלים על ידי בן הזוג",
    "נושאים מטופלים על ידי בן/בת הזוג",
    # generic abstractions
    "כללי",
    "מידע תפקידי",
    "הגבלות לאחר פרישה",
    "עסקים",
    "עסקים קודמים",
    "עיסוקים קודמים",
    "עניינים עסקיים",
    "נושאים ניהוליים",
    "תיקים פרטניים",
    "פרישה",
    "תנאי עבודה",
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


import re as _re

# A name made up entirely of punctuation / placeholder characters — the
# extractor emits these when it can't read a name off a redacted
# (מושחר) document: rows of asterisks, dashes, underscores, or the
# literal strings "null"/"none". They must never become real entities.
#
# Examples caught: "***", "----------", "________________",
# "**** *** ***** ** ** *** * ***", "null", "None", "N/A".
_PLACEHOLDER_CHARS_RE = _re.compile(r"^[\*_\-–—=\.,\s'\"`׳״]+$")
_PLACEHOLDER_WORDS = frozenset({"null", "none", "n/a", "nan", "undefined"})


def is_placeholder_name(name: str | None) -> bool:
    """Return True for names that are pure OCR/extraction noise rather
    than a real entity name — independent of entity_type.

    This is the *shape*-based check (all-punctuation, or a known
    null-literal). ``is_blocked`` layers the curated per-type string
    lists on top of it.
    """
    if name is None:
        return True
    s = " ".join(str(name).split())  # normalise whitespace
    if not s:
        return True
    if s.lower() in _PLACEHOLDER_WORDS:
        return True
    if _PLACEHOLDER_CHARS_RE.match(s):
        return True
    return False


def is_blocked(entity_type: str, name: str | None) -> bool:
    """Return True if the (entity_type, name) pair should be treated as a
    non-entity. Two layers:

    1. Shape check — pure placeholder/punctuation names
       (``is_placeholder_name``), applies to every entity type.
    2. Curated per-type string block list.

    Whitespace is normalised before comparison.
    """
    if not name:
        return True  # an empty name is itself a placeholder
    if is_placeholder_name(name):
        return True
    bl = _BY_TYPE.get(entity_type)
    if bl is None:
        return False
    return " ".join(name.split()) in bl


def blocklist_for(entity_type: str) -> frozenset[str]:
    return _BY_TYPE.get(entity_type, frozenset())
