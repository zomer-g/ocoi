"""Extraction phase — call DeepSeek API to extract entities from markdown."""

import asyncio
import json
from pathlib import Path

from .config import LocalConfig
from . import state as st


# --- Prompts (same as extraction_service.py) ---

SYSTEM_PROMPT = """אתה מומחה בניתוח הסדרים למניעת ניגוד עניינים בישראל.

כללים קריטיים:
1. חלץ אך ורק מידע שכתוב במפורש במסמך. אסור להמציא או לנחש שמות, חברות או עובדות.
2. אם שם לא מופיע במפורש — כתוב null. לעולם אל תנחש.
3. שמות חברות וארגונים — העתק בדיוק כפי שמופיע במסמך.
4. החזר JSON תקין בלבד."""

USER_PROMPT = """נתח את מסמך ניגוד העניינים הבא.

שלב 1 — זהה את הנושא המרכזי (בעל התפקיד):
מסמכים אלה יכולים להיות מכמה סוגים:
- חוות דעת למניעת ניגוד עניינים — נשלחת אל בעל התפקיד. חפש את שמו בשורת "לכבוד" (למשל: "לכבוד חה"כ אבי דיכטר") או בשורת "הנדון".
- פראפרזה/סיכום הסדר — השם מופיע בשורת "הנדון" (למשל: "הנדון: פראפרזה אודות ההסדר... של שר המדע... יעקב פרי").
- הצהרת ניגוד עניינים עצמית — בעל התפקיד הוא הכותב, לא הנמען. חפש את שם החותם בתחתית המסמך.
- הסדר למניעת ניגוד עניינים — השם מופיע בשורת "אל:" או "הנדון".

חשוב: חלץ את השם המלא (שם פרטי + שם משפחה), לא רק תואר. למשל: "יעקב פרי" ולא "שר", "אבי דיכטר" ולא "השר".
אם הכותרת של המסמך מכילה שם — זה בדרך כלל שם בעל התפקיד.

שלב 2 — זהה חברות, בנקים וארגונים:
חלץ את השם המדויק של כל חברה/בנק/ארגון כפי שמופיע במסמך.
לכל חברה, ציין את סוג הקשר לבעל התפקיד:
- owns = בעלות/החזקת מניות
- manages = ניהול
- board_member = חבר דירקטוריון / יו"ר דירקטוריון
- employed_by = מועסק / עובד
- related_to = קשר אחר (עסקי, משפחתי של קרוב, וכד')

שלב 3 — זהה מגבלות/הגבלות:
לכל מגבלה, ציין:
- תיאור ההגבלה (מה בעל התפקיד מנוע מלעשות)
- שם החברה/גוף שלגביו חלה ההגבלה
- סוג ההגבלה: full (מלאה), partial (חלקית/מותנית בהתייעצות), cooling_off (תקופת צינון)

שלב 4 — בני משפחה עם קשרים עסקיים:
רק אם מוזכרים במפורש עם קשר עסקי.
אם השם מושחר/מצונזר (כוכביות ********) — כתוב "מצונזר" בשדה השם.

החזר JSON במבנה הבא:

{{
  "office_holder": {{
    "name_hebrew": "השם המלא כפי שמופיע במסמך",
    "name_english": null,
    "title": "שר / סגן שר / מנכ\\"ל / יו\\"ר / עיתונאי / וכד'",
    "position": "התפקיד המדויק",
    "ministry": "שם המשרד או הגוף (למשל: משרד החקלאות ופיתוח הכפר)"
  }},
  "restrictions": [
    {{
      "description": "תיאור ההגבלה כפי שמופיע במסמך",
      "related_entities": ["שם מדויק של חברה/גוף"],
      "related_domains": ["תחום עסקי אם צוין"],
      "restriction_type": "full|partial|cooling_off",
      "end_date": "YYYY-MM-DD or null",
      "details": "פרטים נוספים"
    }}
  ],
  "companies": [
    {{
      "name_hebrew": "שם החברה/בנק בדיוק כפי שמופיע במסמך",
      "name_english": null,
      "company_type": "פרטית|ציבורית|ממשלתית|בנק|חל\\"צ|קבוצה|null",
      "relationship_to_holder": "owns|manages|employed_by|board_member|related_to"
    }}
  ],
  "associations": [
    {{
      "name_hebrew": "שם העמותה בדיוק כפי שמופיע",
      "relationship_to_holder": "manages|board_member|related_to"
    }}
  ],
  "family_members": [
    {{
      "name": "שם או מצונזר",
      "relation": "חבר קרוב / בן זוג / ילד / הורה / אח",
      "related_companies": ["שם חברה אם רלוונטי"]
    }}
  ],
  "domains": ["תחום עסקי ספציפי בלבד — למשל: בנקאות, חקלאות, תקשורת, נדל\\"ן, ביטחון"]
}}

חשוב:
- אל תכלול תחומים גנריים כמו "החזקת מניות", "מכירת מניות", "דיונים פרלמנטריים", "החלטות ממשלה". כלול רק מגזרים עסקיים ספציפיים.
- אם אין חברות במסמך — החזר רשימה ריקה.
- אם אין מגבלות — החזר רשימה ריקה.
- אם המסמך קובע שלא נדרשות הגבלות — ציין זאת ב-restrictions עם description מתאים ו-restriction_type: null.

טקסט המסמך:
{document_text}"""


async def extract_single(cfg: LocalConfig, title: str, markdown: str) -> dict | None:
    """Call DeepSeek API to extract entities from a single document."""
    from openai import AsyncOpenAI

    client = AsyncOpenAI(
        api_key=cfg.deepseek_api_key,
        base_url=cfg.deepseek_base_url,
    )

    title_prefix = f"כותרת המסמך: {title}\n\n" if title else ""
    truncated = title_prefix + markdown[:15000]
    user_prompt = USER_PROMPT.format(document_text=truncated)

    response = await client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
        max_tokens=4000,
        response_format={"type": "json_object"},
    )

    content = response.choices[0].message.content
    return json.loads(content)


async def run_extract(cfg: LocalConfig, limit: int | None = None) -> int:
    """Extract entities from all converted documents.

    Returns the number of newly extracted documents.
    """
    print("\n=== Extract Phase ===")

    local_state = st.load_state()
    to_extract = st.get_by_status(local_state, "converted")

    # Filter out docs with no markdown content
    to_extract = [
        url for url in to_extract
        if local_state[url].get("markdown_chars", 0) > 50
    ]

    if limit:
        to_extract = to_extract[:limit]

    if not to_extract:
        print("  Nothing to extract.")
        return 0

    errors = cfg.validate()
    if "DEEPSEEK_API_KEY" in " ".join(errors):
        print("  ERROR: DEEPSEEK_API_KEY not set. Skipping extraction.")
        return 0

    print(f"  Documents to extract: {len(to_extract)}")
    extracted = 0

    for i, url in enumerate(to_extract, 1):
        info = local_state[url]
        title = info.get("title", "")[:60]
        md_path = info.get("markdown_path")

        if not md_path or not Path(md_path).exists():
            print(f"  [{i}/{len(to_extract)}] {title} — markdown not found, skipping")
            continue

        try:
            print(f"  [{i}/{len(to_extract)}] Extracting: {title}...")
            markdown = Path(md_path).read_text(encoding="utf-8")
            result = await extract_single(cfg, info.get("title", ""), markdown)

            if result:
                # Save extraction JSON next to markdown
                json_path = Path(md_path).with_suffix(".json")
                json_path.write_text(
                    json.dumps(result, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

                st.mark(
                    local_state, url, "extracted",
                    extraction_path=str(json_path),
                )
                extracted += 1

                # Summarize what was found
                holder = result.get("office_holder", {}).get("name_hebrew", "?")
                n_companies = len(result.get("companies", []))
                n_restrictions = len(result.get("restrictions", []))
                print(f"    OK — {holder}, {n_companies} companies, {n_restrictions} restrictions")
            else:
                st.mark(local_state, url, "extracted", extraction_path=None)
                extracted += 1
                print(f"    Empty extraction result")

        except json.JSONDecodeError as e:
            print(f"    Failed — invalid JSON from DeepSeek: {e}")
            st.mark(local_state, url, "failed", error=f"json_parse: {e}")
        except Exception as e:
            print(f"    Failed: {e}")
            st.mark(local_state, url, "failed", error=str(e)[:200])

        # Rate limit
        await asyncio.sleep(0.5)

    print(f"\n  Extracted: {extracted}/{len(to_extract)}")
    return extracted
