"""Turn analyse-duplicates output into a concrete merge plan.

Reads the cached entity dump, regroups by stable-normalised name, picks
the canonical row in each group (most connections proxy — longest
``name_hebrew`` as a cheap stand-in since the public listing doesn't
expose connection counts), and writes a JSON file the next step can
POST to /admin/matches/clusters/merge-batch.

Filters that drop confusable cases:
* persons named "בן זוג" / "בת זוג" — these are role labels, not
  duplicates of each other
* groups where any member is a placeholder like "***" / "null" — leave
  for manual review
* groups where the longest token is one of the brand-chain markers
  ("ארומה", "רולדין", …) — even with the same normalised form those
  could be different branches; safer to skip

The output is a list of merge ops per entity_type so the frontend's
merge-batch endpoint can swallow them in one round-trip.
"""

from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from pathlib import Path

DUMP_DIR = Path(os.path.expanduser(r"~/AppData/Local/Temp/ocoi_dump"))
OUT_FILE = DUMP_DIR / "merge_plan.json"

# Brand prefixes whose same-normalised-name groups should NOT be auto-
# merged — these are stores in a chain that share the same root token
# after stripping prefixes. The admin can merge them manually if they
# really want, but our default heuristic refuses.
BRAND_BLOCKLIST = {
    "ארומה", "רולדין", "שופרסל", "yellow", "פזyellow",
    "מאפיית", "אלונית", "kspstore",
}

PLACEHOLDER_NAMES = {"***", "null", "", "________________"}

ORG_PREFIXES = {
    "עמותת", "תנועת", "חברת", "אגודת", "מפלגת", "ארגון",
    "קרן", "קבוצת", "מכון", "מועדון", "מפעל", "ועד", "ועדת",
    "עמותה", "תנועה", "חברה", "אגודה", "מפלגה",
    "אגודה שיתופית", "שיתופית",
    "האגודה", "העמותה", "החברה", "התנועה",
    "בית", "מרכז",
}

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

NOISE_TOKENS = {
    "ער", "עי", "ר", "כ", "ה", "של", "את", "בן", "בת",
}

# These ROLE LABELS aren't real people — should never merge
ROLE_LABEL_NAMES = {
    "בן זוג", "בת זוג", "ילד", "ילדה", "בן משפחה",
    "אישה", "בעל", "אם", "אב",
}

GERSH = re.compile(r"[\"'״׳`]")
DASH = re.compile(r"[‐-―\-–—]+")
WS = re.compile(r"\s+")
PUNCT = re.compile(r"[()\[\]{}\.,;:!?*/\\׳״]")


def normalize(name: str, *, kind: str) -> tuple[str, list[str]]:
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


def pick_canonical(group: list[dict]) -> dict:
    """Heuristic without DB access: longer name_hebrew often = the more
    careful / less-truncated entry; tie-break on lowest id for stability."""
    return sorted(
        group,
        key=lambda r: (-len(r.get("name_hebrew") or ""), r.get("id") or ""),
    )[0]


def safe_to_auto_merge(group: list[dict], kind: str) -> bool:
    """Decide whether this group is safe to feed into the bulk merge."""
    names = [r.get("name_hebrew") or "" for r in group]
    if any(n.strip() in PLACEHOLDER_NAMES for n in names):
        return False
    if kind == "person":
        # Role-label names ("בן זוג") are not duplicates of each other.
        if any(n.strip() in ROLE_LABEL_NAMES for n in names):
            return False
    # Skip groups whose longest token is on the brand blocklist —
    # even when their stripped tokens match, they may be different
    # branches.
    flat_tokens: list[str] = []
    for r in group:
        _, tokens = normalize(r.get("name_hebrew") or "", kind=kind)
        flat_tokens.extend(tokens)
    if flat_tokens:
        longest = max(flat_tokens, key=len)
        if longest.lower() in BRAND_BLOCKLIST:
            return False
    return True


def build_plan() -> dict:
    plan: dict[str, list[dict]] = {"person": [], "company": [], "association": []}
    stats: dict[str, dict] = {}
    for kind_plural, kind_singular in (
        ("persons", "person"), ("companies", "company"), ("associations", "association")
    ):
        rows = load(kind_plural)
        groups: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            name = r.get("name_hebrew") or ""
            if name.strip() in PLACEHOLDER_NAMES:
                continue
            norm, _t = normalize(name, kind=kind_singular)
            if not norm:
                continue
            groups[norm].append(r)
        sized = [(k, v) for k, v in groups.items() if len(v) >= 2]
        skipped = []
        ops = []
        for k, v in sized:
            if not safe_to_auto_merge(v, kind_singular):
                skipped.append({"norm": k, "size": len(v),
                                 "names": [r.get("name_hebrew") for r in v]})
                continue
            canonical = pick_canonical(v)
            ops.append({
                "entity_type": kind_singular,
                "canonical_id": canonical["id"],
                "canonical_name": canonical.get("name_hebrew"),
                "member_ids": [r["id"] for r in v],
                "all_names": [r.get("name_hebrew") for r in v],
            })
        plan[kind_singular] = ops
        stats[kind_singular] = {
            "total_groups": len(sized),
            "auto_merge_ops": len(ops),
            "skipped_groups": len(skipped),
            "skipped_examples": skipped[:10],
        }
    OUT_FILE.write_text(
        json.dumps({"stats": stats, "plan": plan}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return {"stats": stats, "plan_file": str(OUT_FILE)}


if __name__ == "__main__":
    out = build_plan()
    print(json.dumps(out, ensure_ascii=False, indent=2)[:3000])
