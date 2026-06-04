"""Manual duplicate-analysis pass over the OCOI entity dump.

Loads /tmp/ocoi_dump/{persons,companies,associations}.jsonl, normalises
every name with a few cleanup heuristics, and groups identical
normalised forms. Reports the largest groups so a human can eyeball
which are real duplicates vs coincidence.
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import os
DUMP_DIR = Path(os.path.expanduser(r"~/AppData/Local/Temp/ocoi_dump"))

# ── normalisation ──
# Common Hebrew construct-state prefixes that don't disambiguate. Same
# list the production matcher uses, kept in sync here for the analysis.
ORG_PREFIXES = {
    "עמותת", "תנועת", "חברת", "אגודת", "מפלגת", "ארגון",
    "קרן", "קבוצת", "מכון", "מועדון", "מפעל", "ועד", "ועדת",
    "עמותה", "תנועה", "חברה", "אגודה", "מפלגה",
    "אגודה שיתופית", "שיתופית",
    "האגודה", "העמותה", "החברה", "התנועה",
    "בית", "מרכז",
}

# Honorifics for people
HONORIFICS = {
    "מר", "גב", "ד", "דר", "ד״ר", "פרופ", "פרופ׳",
    "עו", "עוד", "עו״ד", "רב", "הרב",
    "השר", "השרה", "שר", "שרה", "סגן", "סגנית",
    "ח״כ", "חכ", "ראש", "ראשת",
    "מהנדס", "אדריכל",
}

LEGAL_SUFFIXES = {
    "בעמ", "בע", "בעי", "ב.ע.מ",
    "ושות", "ושותפיו", "ושותפים",
    "ltd", "limited", "llc", "inc", "corp",
}

# Common parenthetical / honorific noise to strip
NOISE_TOKENS = {
    "ער", "עי", "ר", "כ",  # ע"ר variants after gershayim removal
    "ה", "של", "את", "בן", "בת",
}

GERSH = re.compile(r"[\"'״׳`]")
DASH = re.compile(r"[‐-―\-–—]+")
WS = re.compile(r"\s+")
PUNCT = re.compile(r"[()\[\]{}\.,;:!?*/\\׳״]")


def normalize(name: str, *, kind: str) -> tuple[str, list[str]]:
    """Return (joined_normalised, sorted_tokens).
    Tokens have prefixes/suffixes/honorifics stripped."""
    s = (name or "").lower()
    s = GERSH.sub("", s)
    s = DASH.sub(" ", s)
    s = PUNCT.sub(" ", s)
    s = WS.sub(" ", s).strip()
    raw = [t for t in s.split(" ") if t]
    if kind == "person":
        drops = HONORIFICS | NOISE_TOKENS
    else:
        drops = ORG_PREFIXES | LEGAL_SUFFIXES | NOISE_TOKENS
    # Drop tokens that are pure digits or single-character (after Hebrew)
    tokens = [t for t in raw if t not in drops and len(t) > 1]
    return (" ".join(sorted(tokens)), sorted(tokens))


def load(kind: str) -> list[dict]:
    p = DUMP_DIR / f"{kind}.jsonl"
    out = []
    with p.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def group_by_norm(rows: list[dict], kind: str) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        name = r.get("name_hebrew") or ""
        if not name or name in ("***", "null", "________________"):
            continue
        norm, _tokens = normalize(name, kind=kind)
        if not norm:
            continue
        groups[norm].append(r)
    return groups


def first_token_groups(rows: list[dict], kind: str) -> dict[str, list[dict]]:
    """Group by FIRST meaningful token only — looser, surfaces likely-
    related entities that the strict normalised-name match misses."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        name = r.get("name_hebrew") or ""
        if not name or name in ("***", "null", "________________"):
            continue
        _norm, tokens = normalize(name, kind=kind)
        if not tokens:
            continue
        # Sort by length desc and take the longest informative token
        # — that's usually the brand/family name rather than a generic
        # qualifier like "לקידום".
        longest = sorted(tokens, key=lambda t: (-len(t), t))[0]
        groups[longest].append(r)
    return groups


def report_groups(label: str, groups: dict[str, list[dict]], min_size: int = 2, top: int = 25) -> None:
    sized = [(k, v) for k, v in groups.items() if len(v) >= min_size]
    sized.sort(key=lambda kv: -len(kv[1]))
    print(f"\n=== {label}: {len(sized)} groups (≥{min_size} members), top {top} ===\n")
    for k, v in sized[:top]:
        names = [(r.get("name_hebrew", ""), r.get("id", "")[:8]) for r in v]
        print(f"  [{len(v)}] norm={k!r}")
        for n, i in names[:8]:
            print(f"      · {n}   ({i})")
        if len(names) > 8:
            print(f"      … +{len(names) - 8} more")
    print()


if __name__ == "__main__":
    for kind in ("persons", "companies", "associations"):
        rows = load(kind)
        print(f"\n##### {kind.upper()} — {len(rows)} total #####")
        # Pass 1: strict normalised-name match (very high confidence)
        ngrp = group_by_norm(rows, "person" if kind == "persons" else kind[:-1])
        report_groups(f"{kind}: identical normalised names", ngrp, min_size=2, top=30)
        # Pass 2: same longest token (looser — manual review needed)
        if kind != "persons":  # too noisy for persons
            tgrp = first_token_groups(rows, kind[:-1])
            report_groups(f"{kind}: same longest token (review needed)", tgrp, min_size=4, top=15)
