"""Hebrew NER using DictaBERT (dicta-il/dictabert-ner)."""

from ocoi_common.logging import setup_logging

logger = setup_logging("ocoi.extractor.dictabert")

# Entity type mapping from DictaBERT labels to our types
LABEL_MAP = {
    "PER": "person",
    "ORG": "company",  # Could be company or association - resolved by merger
    "GPE": "domain",   # Geo-political entities
    "TTL": "title",    # Titles
    "TIMEX": "time",   # Time expressions
    "LOC": "location",
}


class DictaBertNER:
    """Extract named entities from Hebrew text using DictaBERT NER model."""

    def __init__(self):
        self._pipeline = None

    def _get_pipeline(self):
        if self._pipeline is None:
            from transformers import pipeline

            logger.info("Loading DictaBERT NER model...")
            self._pipeline = pipeline(
                "token-classification",
                model="dicta-il/dictabert-ner",
                aggregation_strategy="simple",
            )
            logger.info("DictaBERT NER model loaded")
        return self._pipeline

    def extract(self, text: str) -> list[dict]:
        """Extract named entities from Hebrew text.

        Returns list of dicts with keys: entity_type, text, score
        """
        pipe = self._get_pipeline()

        # DictaBERT has a max sequence length, process in chunks
        chunks = self._split_text(text, max_length=500)
        all_entities = []

        for chunk in chunks:
            results = pipe(chunk)
            for entity in results:
                label = entity.get("entity_group", "")
                mapped_type = LABEL_MAP.get(label)
                if mapped_type and mapped_type != "time" and mapped_type != "location":
                    all_entities.append({
                        "entity_type": mapped_type,
                        "text": entity["word"].strip(),
                        "score": entity["score"],
                        "original_label": label,
                    })

        # Deduplicate
        seen = set()
        unique = []
        for e in all_entities:
            key = (e["entity_type"], e["text"])
            if key not in seen:
                seen.add(key)
                unique.append(e)

        return unique

    def _split_text(self, text: str, max_length: int = 500) -> list[str]:
        """Split text into chunks that respect sentence boundaries."""
        sentences = text.replace("\n", " ").split(".")
        chunks = []
        current_chunk = ""

        for sentence in sentences:
            if len(current_chunk) + len(sentence) < max_length:
                current_chunk += sentence + "."
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = sentence + "."

        if current_chunk.strip():
            chunks.append(current_chunk.strip())

        return chunks or [text[:max_length]]
