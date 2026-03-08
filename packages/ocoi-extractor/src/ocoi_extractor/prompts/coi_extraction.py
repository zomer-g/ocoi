"""Domain-specific prompts for conflict of interest document extraction."""

EXTRACTION_SYSTEM_PROMPT = """You are an expert analyst specializing in Israeli conflict of interest arrangements (הסדרים למניעת ניגוד עניינים).
You extract structured data from Hebrew legal documents with high precision.
Always respond in valid JSON matching the requested schema."""

EXTRACTION_USER_PROMPT = """Analyze this Israeli conflict of interest arrangement document.
Extract ALL entities and relationships mentioned.

Return a JSON object with these fields:

{{
  "office_holder": {{
    "name_hebrew": "שם בעברית",
    "name_english": "English name if mentioned",
    "title": "תואר (שר, סגן שר, מנכ\"ל, etc.)",
    "position": "תפקיד ספציפי",
    "ministry": "משרד"
  }},
  "restrictions": [
    {{
      "description": "תיאור ההגבלה",
      "related_entities": ["שם גוף 1", "שם גוף 2"],
      "related_domains": ["תחום 1", "תחום 2"],
      "restriction_type": "full|partial|cooling_off",
      "end_date": "YYYY-MM-DD or null",
      "details": "פרטים נוספים"
    }}
  ],
  "companies": [
    {{
      "name_hebrew": "שם החברה",
      "name_english": "English name if available",
      "company_type": "סוג (פרטית, ציבורית, ממשלתית)",
      "relationship_to_holder": "owns|manages|employed_by|board_member|related_to"
    }}
  ],
  "associations": [
    {{
      "name_hebrew": "שם העמותה",
      "relationship_to_holder": "manages|board_member|related_to"
    }}
  ],
  "family_members": [
    {{
      "name": "שם",
      "relation": "בן/בת זוג, ילד, הורה, etc.",
      "related_companies": ["שם חברה"]
    }}
  ],
  "domains": ["תחום 1", "תחום 2"]
}}

Document text:
{document_text}"""
