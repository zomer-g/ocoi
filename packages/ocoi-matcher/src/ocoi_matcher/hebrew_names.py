"""Hebrew-aware name normalisation + fuzzy similarity, designed for the
duplicate-entity detector.

The existing ``fuzzy_match.normalize_company_name`` was built for matching
companies against the gov.il registry — it strips legal suffixes ("בע״מ",
"חברה ל…") but knows nothing about *people*. People are the most common
source of duplicates ("משה כחלון" vs "כחלון משה"), so this module adds:

* honorific stripping ("מר", "ד״ר", "עו״ד", "פרופ׳"…)
* whitespace + dash + quote normalisation (gershayim, smart quotes)
* token-sort comparison so word order doesn't matter
* a deterministic "blocking key" so the scan doesn't compare every name
  against every other name in O(N²)
"""

from __future__ import annotations

import re
from typing import Iterable


# ── Honorifics + noise tokens ───────────────────────────────────────────

# Words that almost never disambiguate a person and routinely cause false
# negatives when present on one side and absent on the other.
HONORIFICS: frozenset[str] = frozenset({
    # academic / honorific
    "מר", "גב", "גבת", "האדון", "הגב", "האדם",
    "ד", "דר", "דוקטור", "ד״ר",
    "פרופ", "פרופסור",
    "עו", "עוד", "עו״ד",
    # Common OCR / parsing residues of "עו״ד":
    # * "וד" — appears when gershayim get stripped from "ע ו״ד" and the
    #   stray "ע" gets filtered as a 1-char token, leaving just "וד".
    # * "עוייד" — OCR misread of the gershayim as "יי".
    # * "עויד" / "עוד״" — other gershayim-mangling variants seen in raw imports.
    "וד", "עוייד", "עויד", "עוד״",
    "רב", "הרב",
    "מהנדס", "אדריכל",
    # common Hebrew titles
    "השר", "השרה", "שר", "שרה", "סגן", "סגנית",
    "ח״כ", "חכ", "ראש", "ראשת",
})

# Whitespace + punctuation cleanup map — gershayim / geresh / various
# dash forms all collapse to a single canonical form before tokenising.
_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[\"'״׳`׳]")
_DASH_RE = re.compile(r"[‐-―\-]+")  # any kind of hyphen / dash


# Company-style suffixes (mirrors fuzzy_match but kept local so we can
# extend independently).
COMPANY_SUFFIXES: frozenset[str] = frozenset({
    "בעמ", "בע", "ב.ע.מ",
    "חברה",
    "אגודה", "אגודה שיתופית", "שיתופית",
    "עמותה",
    "ושות", "ושותפיו", "ושותפים",
    "ltd", "limited", "llc", "inc", "corp",
})

# Hebrew construct-state organisation prefixes — almost always boilerplate
# that obscures the real name. Without stripping these, "עמותת הצלחה"
# (tokens: ["עמותת", "הצלחה"]) and "תנועת הצלחה" (["תנועת", "הצלחה"])
# look only ~50% similar to the matcher; with stripping both collapse to
# ["הצלחה"] and score 1.0.
ORG_PREFIXES: frozenset[str] = frozenset({
    "עמותת", "תנועת", "חברת", "אגודת", "מפלגת", "ארגון", "ארגונת",
    "קרן", "קבוצת", "מכון", "מועדון", "מפעל", "ועד", "ועדת",
    "ר", "רעמ",  # rare honorific shorthands seen in scraped names
    # final form variants
    "עמותהת", "תנועהת",
    # plural variants
    "עמותות", "תנועות", "חברות", "מפלגות",
    # noisy "of-the-X" tokens
    "ה", "של", "את",
})


def _strip_punct(s: str) -> str:
    s = _PUNCT_RE.sub("", s)
    s = _DASH_RE.sub(" ", s)
    return s


def normalize(name: str) -> str:
    """Return a canonical lowercase form: punctuation + quote chars
    stripped, whitespace collapsed, leading/trailing spaces trimmed.
    Honorifics are NOT removed here — that's done at the token stage
    so the raw normalised string remains a faithful display value.
    """
    if not name:
        return ""
    s = _strip_punct(name).strip()
    s = _WHITESPACE_RE.sub(" ", s)
    # Lowercase only the Latin portion; Hebrew is unicase.
    return s.lower()


def tokens(name: str, *, kind: str = "person") -> list[str]:
    """Sorted, honorific-stripped token list. Empty when ``name`` has no
    informative content after stripping.

    Use ``kind='company'`` (or any non-person kind) to also strip legal
    suffixes (בע״מ, חברה …) and construct-state organisation prefixes
    (עמותת, תנועת, חברת …). Without that, two clearly-identical orgs
    like "עמותת הצלחה" and "תנועת הצלחה" score only ~0.5 — well below
    the duplicate threshold — and we miss obvious cleanup wins.
    """
    if not name:
        return []
    cleaned = normalize(name)
    raw = [t for t in cleaned.split(" ") if t]
    if kind == "person":
        drops = HONORIFICS
    else:
        drops = HONORIFICS | COMPANY_SUFFIXES | ORG_PREFIXES
    # Single-character Hebrew tokens are never disambiguating names — they
    # are artefacts of gershayim/quote splitting ("ע ו״ד" → "ע", "וד")
    # or stray initials. Dropping them prevents the "אלעד מן" vs
    # "ע ו״ד אלעד מן" pair from scoring 0.73 instead of 0.97.
    out = [t for t in raw if t not in drops and len(t) > 1]
    out.sort()
    return out


def blocking_key(name: str, *, kind: str = "person") -> str:
    """A deterministic prefix used to bucket candidate pairs without an
    N×N scan. Two names that don't share a blocking key are never
    compared. Trade-off: a too-narrow key misses pairs; too-wide produces
    a worse-than-N² scan inside the bucket.

    Strategy: sort the token list, then take the shortest two characters
    from the *longest* token (usually the family name). For a person
    "משה כחלון" and "כחלון משה", tokens=["כחלון", "משה"], longest="כחלון",
    key="כח". For "פלאפון תקשורת בע״מ" → tokens=["פלאפון", "תקשורת"]
    (after stripping בעמ), longest="תקשורת", key="תק".
    """
    ts = tokens(name, kind=kind)
    if not ts:
        return ""
    longest = max(ts, key=len)
    return longest[:2]


def _token_sort_ratio(a: list[str], b: list[str]) -> float:
    """rapidfuzz-style token-sort similarity, implemented locally to avoid
    pulling the dependency into a column unless we have to. Range: 0..1."""
    if not a or not b:
        return 0.0
    sa = " ".join(a)
    sb = " ".join(b)
    if sa == sb:
        return 1.0
    # Levenshtein ratio (cheap approximation: 1 - distance / max-len).
    dist = _levenshtein(sa, sb)
    max_len = max(len(sa), len(sb))
    if max_len == 0:
        return 0.0
    return 1.0 - dist / max_len


def _levenshtein(a: str, b: str) -> int:
    """Plain Wagner-Fischer Levenshtein. N×M = small (tens of chars)."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i] + [0] * len(b)
        for j, cb in enumerate(b, 1):
            cost = 0 if ca == cb else 1
            cur[j] = min(
                cur[j - 1] + 1,
                prev[j] + 1,
                prev[j - 1] + cost,
            )
        prev = cur
    return prev[-1]


def _jaccard(a: Iterable[str], b: Iterable[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    inter = sa & sb
    union = sa | sb
    return len(inter) / len(union)


def similarity(a: str, b: str, *, kind: str = "person") -> tuple[float, list[str]]:
    """Return ``(score, reasons)`` where score ∈ [0, 1] and reasons is a
    list of short strings naming the signals that fired (handy for the
    admin review UI).

    Combines three signals:
      • token_sort_ratio (Levenshtein over alpha-sorted tokens)
      • Jaccard overlap on the token set
      • +0.08 bonus when the same two tokens appear in swapped order
        (the "משה כחלון" / "כחלון משה" case)
    """
    if not a or not b:
        return 0.0, []
    norm_a = normalize(a)
    norm_b = normalize(b)
    if norm_a == norm_b:
        return 1.0, ["exact_normalised"]

    ta = tokens(a, kind=kind)
    tb = tokens(b, kind=kind)
    if not ta or not tb:
        return 0.0, []

    reasons: list[str] = []

    # If the token sets are identical post-normalisation, that's a very
    # strong "same person, words reordered" signal.
    if set(ta) == set(tb):
        reasons.append("tokens_identical")
        # Even when the sets match, return a score tightly below 1.0 so
        # truly exact strings can be sorted above word-swap matches.
        return 0.97, reasons

    # token_subset is STRICTLY for organisations — Hebrew first names
    # ("בנימין", "משה") overlap across unrelated people, so any kind
    # of subset rule for persons chains them transitively in union-find
    # and creates "everyone called X" mega-clusters.
    #
    # We DELIBERATELY do NOT have a prefix rule any more. Earlier
    # iterations tried (a) substring containment (b) left-prefix with a
    # length floor, but both got swamped by chain stores ("ארומה" is a
    # prefix of every "ארומה <city>" → 95-member cluster, all of which
    # are *legitimately different* branches the admin doesn't want to
    # merge). The cleanup wins from prefix matching weren't worth the
    # downstream confusion.
    #
    # Genuine "short form vs full form" duplicates (e.g. "עמותת הצלחה"
    # vs "תנועת הצלחה") are caught by tokens_identical after
    # ORG_PREFIXES + COMPANY_SUFFIXES stripping. Genuine "long form vs
    # longer form" duplicates (e.g. "הצלחה לקידום חברה" vs "הצלחה -
    # התנועה הצרכנית לקידום חברה כלכלית הוגנת") are caught by the
    # subset rule below.
    if kind != "person":
        sa, sb = set(ta), set(tb)
        small, big = (sa, sb) if len(sa) <= len(sb) else (sb, sa)
        # Require ≥2 tokens on the shorter side AND ≥50% coverage of
        # the larger. The 2-token floor kills the "single common
        # token" pattern that lets brand names chain across stores.
        if len(small) >= 2 and small.issubset(big):
            coverage = len(small) / len(big)
            if coverage >= 0.5:
                reasons.append(f"token_subset={len(small)}/{len(big)}")
                return max(0.88, 0.7 + 0.25 * coverage), reasons

    tsr = _token_sort_ratio(ta, tb)
    jac = _jaccard(ta, tb)
    if tsr > 0:
        reasons.append(f"token_sort={tsr:.2f}")
    if jac > 0:
        reasons.append(f"jaccard={jac:.2f}")

    # Special case: 2-token names where the order is swapped → strong
    # signal even if Levenshtein under-counts because of length.
    swap_bonus = 0.0
    if len(ta) == 2 and len(tb) == 2 and set(ta) == set(tb) and ta != tb:
        swap_bonus = 0.08
        reasons.append("two_token_swap")

    # First token in common is a meaningful prior for Hebrew names.
    if ta[0] == tb[0]:
        reasons.append("first_token_match")

    score = 0.6 * tsr + 0.4 * jac + swap_bonus
    score = max(0.0, min(1.0, score))
    return score, reasons
