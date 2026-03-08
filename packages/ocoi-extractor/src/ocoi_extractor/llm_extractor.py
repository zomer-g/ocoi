"""LLM-based structured extraction using DeepSeek API."""

import json

from openai import AsyncOpenAI

from ocoi_common.config import settings
from ocoi_common.logging import setup_logging
from ocoi_common.models import ExtractionResult, ExtractedPerson, ExtractedCompany, \
    ExtractedAssociation, ExtractedDomain, ExtractedRelationship, \
    EntityType, RelationshipType, RestrictionType
from ocoi_extractor.prompts.coi_extraction import EXTRACTION_SYSTEM_PROMPT, EXTRACTION_USER_PROMPT

logger = setup_logging("ocoi.extractor.llm")


class LLMExtractor:
    """Extract structured entities and relationships using DeepSeek LLM."""

    def __init__(self):
        self.client = AsyncOpenAI(
            api_key=settings.deepseek_api_key,
            base_url=settings.deepseek_base_url,
        )
        self.model = "deepseek-chat"

    async def extract(self, text: str) -> ExtractionResult:
        """Extract entities and relationships from document text."""
        # Truncate very long documents
        if len(text) > 15000:
            text = text[:15000]

        prompt = EXTRACTION_USER_PROMPT.format(document_text=text)

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                max_tokens=4000,
                response_format={"type": "json_object"},
            )

            content = response.choices[0].message.content
            data = json.loads(content)
            return self._parse_response(data)

        except Exception as e:
            logger.error(f"LLM extraction failed: {e}")
            return ExtractionResult()

    def _parse_response(self, data: dict) -> ExtractionResult:
        """Parse LLM JSON response into ExtractionResult."""
        result = ExtractionResult()

        # Extract office holder as person
        if holder := data.get("office_holder"):
            if name := holder.get("name_hebrew"):
                result.persons.append(ExtractedPerson(
                    name_hebrew=name,
                    name_english=holder.get("name_english"),
                    title=holder.get("title"),
                    position=holder.get("position"),
                    ministry=holder.get("ministry"),
                ))

        # Extract companies
        for comp in data.get("companies", []):
            if name := comp.get("name_hebrew"):
                result.companies.append(ExtractedCompany(
                    name_hebrew=name,
                    name_english=comp.get("name_english"),
                    company_type=comp.get("company_type"),
                ))

                # Create relationship between holder and company
                if holder_name := data.get("office_holder", {}).get("name_hebrew"):
                    rel_type = self._map_relationship(comp.get("relationship_to_holder"))
                    result.relationships.append(ExtractedRelationship(
                        source_type=EntityType.PERSON,
                        source_name=holder_name,
                        target_type=EntityType.COMPANY,
                        target_name=name,
                        relationship_type=rel_type,
                        confidence=0.8,
                    ))

        # Extract associations
        for assoc in data.get("associations", []):
            if name := assoc.get("name_hebrew"):
                result.associations.append(ExtractedAssociation(name_hebrew=name))
                if holder_name := data.get("office_holder", {}).get("name_hebrew"):
                    rel_type = self._map_relationship(assoc.get("relationship_to_holder"))
                    result.relationships.append(ExtractedRelationship(
                        source_type=EntityType.PERSON,
                        source_name=holder_name,
                        target_type=EntityType.ASSOCIATION,
                        target_name=name,
                        relationship_type=rel_type,
                        confidence=0.8,
                    ))

        # Extract domains
        for domain_name in data.get("domains", []):
            result.domains.append(ExtractedDomain(name_hebrew=domain_name))
            if holder_name := data.get("office_holder", {}).get("name_hebrew"):
                result.relationships.append(ExtractedRelationship(
                    source_type=EntityType.PERSON,
                    source_name=holder_name,
                    target_type=EntityType.DOMAIN,
                    target_name=domain_name,
                    relationship_type=RelationshipType.OPERATES_IN,
                    confidence=0.7,
                ))

        # Extract restrictions as relationships
        for restriction in data.get("restrictions", []):
            holder_name = data.get("office_holder", {}).get("name_hebrew", "")
            for entity_name in restriction.get("related_entities", []):
                result.relationships.append(ExtractedRelationship(
                    source_type=EntityType.PERSON,
                    source_name=holder_name,
                    target_type=EntityType.COMPANY,
                    target_name=entity_name,
                    relationship_type=RelationshipType.RESTRICTED_FROM,
                    details=restriction.get("description"),
                    restriction_type=self._map_restriction(restriction.get("restriction_type")),
                    confidence=0.9,
                ))

        # Extract family members
        for member in data.get("family_members", []):
            if name := member.get("name"):
                result.persons.append(ExtractedPerson(name_hebrew=name))
                if holder_name := data.get("office_holder", {}).get("name_hebrew"):
                    result.relationships.append(ExtractedRelationship(
                        source_type=EntityType.PERSON,
                        source_name=holder_name,
                        target_type=EntityType.PERSON,
                        target_name=name,
                        relationship_type=RelationshipType.FAMILY_MEMBER,
                        details=member.get("relation"),
                        confidence=0.9,
                    ))

        return result

    def _map_relationship(self, rel: str | None) -> RelationshipType:
        mapping = {
            "owns": RelationshipType.OWNS,
            "manages": RelationshipType.MANAGES,
            "employed_by": RelationshipType.EMPLOYED_BY,
            "board_member": RelationshipType.BOARD_MEMBER,
            "related_to": RelationshipType.RELATED_TO,
        }
        return mapping.get(rel or "", RelationshipType.RELATED_TO)

    def _map_restriction(self, rtype: str | None) -> RestrictionType | None:
        mapping = {
            "full": RestrictionType.FULL,
            "partial": RestrictionType.PARTIAL,
            "cooling_off": RestrictionType.COOLING_OFF,
        }
        return mapping.get(rtype or "")
