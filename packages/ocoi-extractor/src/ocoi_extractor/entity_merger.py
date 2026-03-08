"""Merge and deduplicate entities from multiple extractors."""

from rapidfuzz import fuzz

from ocoi_common.logging import setup_logging
from ocoi_common.models import (
    ExtractionResult,
    ExtractedPerson,
    ExtractedCompany,
    ExtractedAssociation,
    ExtractedDomain,
)

logger = setup_logging("ocoi.extractor.merger")

# Minimum similarity score for fuzzy matching (0-100)
SIMILARITY_THRESHOLD = 85


def merge_results(
    ner_entities: list[dict],
    llm_result: ExtractionResult,
) -> ExtractionResult:
    """Merge NER entities with LLM extraction results.

    Strategy:
    - LLM results are preferred for structured data (relationships, restrictions)
    - NER results fill in entities that LLM may have missed
    - Fuzzy matching prevents duplicates
    """
    merged = ExtractionResult(
        persons=list(llm_result.persons),
        companies=list(llm_result.companies),
        associations=list(llm_result.associations),
        domains=list(llm_result.domains),
        relationships=list(llm_result.relationships),
    )

    # Add NER-discovered entities that weren't found by LLM
    for entity in ner_entities:
        etype = entity["entity_type"]
        text = entity["text"]

        if etype == "person" and not _is_duplicate_name(
            text, [p.name_hebrew for p in merged.persons]
        ):
            merged.persons.append(ExtractedPerson(name_hebrew=text))

        elif etype == "company" and not _is_duplicate_name(
            text, [c.name_hebrew for c in merged.companies]
        ) and not _is_duplicate_name(
            text, [a.name_hebrew for a in merged.associations]
        ):
            # NER labels ORG — could be company or association
            if _looks_like_association(text):
                merged.associations.append(ExtractedAssociation(name_hebrew=text))
            else:
                merged.companies.append(ExtractedCompany(name_hebrew=text))

        elif etype == "title":
            # Skip titles, they're attributes of persons
            pass

    return merged


def _is_duplicate_name(name: str, existing_names: list[str]) -> bool:
    """Check if a name is a fuzzy duplicate of any existing name."""
    normalized = _normalize_hebrew_name(name)
    for existing in existing_names:
        existing_norm = _normalize_hebrew_name(existing)
        if fuzz.ratio(normalized, existing_norm) >= SIMILARITY_THRESHOLD:
            return True
    return False


def _normalize_hebrew_name(name: str) -> str:
    """Normalize a Hebrew name for comparison."""
    # Remove common titles
    titles = ["ד\"ר", "פרופ'", "עו\"ד", "רו\"ח", "מר", "גב'", "השר", "השרה", "סגן"]
    result = name
    for title in titles:
        result = result.replace(title, "").strip()
    # Remove extra whitespace
    result = " ".join(result.split())
    return result


def _looks_like_association(name: str) -> bool:
    """Heuristic: does this ORG name look like an association vs company?"""
    association_markers = ["עמותת", "עמותה", "ארגון", "מועצת", "איגוד", "התאחדות"]
    return any(marker in name for marker in association_markers)
