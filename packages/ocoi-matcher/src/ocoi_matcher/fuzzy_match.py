"""Fuzzy matching for Hebrew company names."""

import re
from rapidfuzz import fuzz

from ocoi_common.logging import setup_logging

logger = setup_logging("ocoi.matcher.fuzzy")

# Common suffixes/prefixes to strip for matching
STRIP_PATTERNS = [
    r'\bבע"מ\b',
    r"\bבע״מ\b",
    r"\bבע'מ\b",
    r"\bחברה ל\b",
    r"\bחברת\b",
    r"\bמפעלי\b",
    r"\bקבוצת\b",
    r"\bתעשיות\b",
    r"\bהולדינגס\b",
    r"\bישראל\b",
    r"\bלישראל\b",
    r"\bבית השקעות\b",
    r"\(\s*\)",
    r"\s+",
]


def normalize_company_name(name: str) -> str:
    """Normalize a Hebrew company name for matching."""
    result = name.strip()
    for pattern in STRIP_PATTERNS:
        result = re.sub(pattern, " ", result)
    result = " ".join(result.split()).strip()
    return result


def match_score(name1: str, name2: str) -> float:
    """Calculate similarity score between two company names (0-1)."""
    n1 = normalize_company_name(name1)
    n2 = normalize_company_name(name2)

    # Try exact match first
    if n1 == n2:
        return 1.0

    # Fuzzy ratio
    ratio = fuzz.ratio(n1, n2) / 100.0
    partial = fuzz.partial_ratio(n1, n2) / 100.0
    token_sort = fuzz.token_sort_ratio(n1, n2) / 100.0

    # Weighted average
    return max(ratio, partial * 0.9, token_sort * 0.95)


def find_best_match(
    target_name: str,
    candidates: list[dict],
    name_field: str = "name",
    threshold: float = 0.7,
) -> dict | None:
    """Find the best matching company from a list of candidates."""
    best_match = None
    best_score = 0.0

    for candidate in candidates:
        candidate_name = candidate.get(name_field, "")
        score = match_score(target_name, candidate_name)
        if score > best_score and score >= threshold:
            best_score = score
            best_match = {**candidate, "_match_score": score}

    return best_match
