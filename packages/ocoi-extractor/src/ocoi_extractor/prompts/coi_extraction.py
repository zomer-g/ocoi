"""Domain-specific prompts for conflict of interest document extraction."""

EXTRACTION_SYSTEM_PROMPT = """You are an expert analyst specializing in Israeli conflict of interest arrangements (הסדרים למניעת ניגוד עניינים).

CRITICAL RULES:
1. Extract ONLY information that is EXPLICITLY written in the document. NEVER invent or guess names, companies, or facts.
2. The office holder's FULL NAME must come from the document text itself — look for it in the greeting line ("לכבוד"), the subject line ("הנדון"), or the document title.
3. If a name is not explicitly mentioned, write "לא צוין" — NEVER guess.
4. Company and organization names must be copied EXACTLY as they appear in the document.
5. Respond in valid JSON only."""

EXTRACTION_USER_PROMPT = """Analyze this Israeli conflict of interest (ניגוד עניינים) document.

STEP 1 — Identify the MAIN PERSON (office holder):
- Look at the "לכבוד" (addressee) line, "הנדון" (subject) line, or document title
- Extract their FULL NAME in Hebrew (e.g., "אביר קארה", "יעקב פרי") — NOT just their title
- Extract their title (שר, סגן שר, מנכ"ל, etc.) and ministry/organization

STEP 2 — Identify COMPANIES and ORGANIZATIONS mentioned:
- Extract the EXACT Hebrew name of each company/bank/organization as written in the document
- For each, note the office holder's relationship: owns (בעלות), manages (ניהול), board_member (דירקטוריון), employed_by (מועסק), related_to (קשור)

STEP 3 — Identify RESTRICTIONS (הגבלות/מגבלות):
- What is the office holder restricted FROM doing?
- Which specific entities (companies/orgs) are they restricted regarding?
- Is the restriction full (מלאה), partial (חלקית), or cooling_off (תקופת צינון)?

STEP 4 — Identify FAMILY MEMBERS with business connections:
- Only include family members explicitly mentioned with business ties

Return this JSON structure:

{{
  "office_holder": {{
    "name_hebrew": "השם המלא בעברית כפי שמופיע במסמך",
    "name_english": "English name if mentioned, or null",
    "title": "תואר: שר / סגן שר / מנכ\\"ל / יו\\"ר / etc.",
    "position": "תפקיד מדויק כפי שמופיע במסמך",
    "ministry": "שם המשרד או הגוף"
  }},
  "restrictions": [
    {{
      "description": "תיאור ההגבלה כפי שמופיע במסמך",
      "related_entities": ["שם מדויק של חברה/גוף 1"],
      "related_domains": ["תחום עסקי ספציפי אם צוין"],
      "restriction_type": "full|partial|cooling_off",
      "end_date": "YYYY-MM-DD or null",
      "details": "פרטים נוספים"
    }}
  ],
  "companies": [
    {{
      "name_hebrew": "שם החברה/בנק/גוף בדיוק כפי שמופיע במסמך",
      "name_english": "English name if available, or null",
      "company_type": "פרטית|ציבורית|ממשלתית|בנק|חל\\"צ|null",
      "relationship_to_holder": "owns|manages|employed_by|board_member|related_to"
    }}
  ],
  "associations": [
    {{
      "name_hebrew": "שם העמותה בדיוק כפי שמופיע במסמך",
      "relationship_to_holder": "manages|board_member|related_to"
    }}
  ],
  "family_members": [
    {{
      "name": "שם בן/בת המשפחה",
      "relation": "בן/בת זוג, ילד, הורה, אח/ות",
      "related_companies": ["שם חברה"]
    }}
  ],
  "domains": ["תחום עסקי ספציפי שצוין במסמך"]
}}

IMPORTANT: Do NOT include generic domains like "החזקת מניות" or "מכירת מניות". Only include specific business sectors mentioned (e.g., "בנקאות", "טכנולוגיה", "נדל\\"ן", "אנרגיה").

Document text:
{document_text}"""
