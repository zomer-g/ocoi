"""PDF to Markdown conversion using Marker (Surya OCR) for Hebrew support."""

from pathlib import Path

from ocoi_common.logging import setup_logging

logger = setup_logging("ocoi.converter.marker")


class MarkerConverter:
    """Converts PDFs to Markdown using Marker with Surya OCR.

    Best for scanned PDFs or PDFs with complex layouts.
    Supports Hebrew text via Surya's multilingual OCR.
    """

    def __init__(self):
        self._converter = None

    def _get_converter(self):
        if self._converter is None:
            from marker.converters.pdf import PdfConverter
            from marker.config.parser import ConfigParser

            config = ConfigParser(
                {
                    "output_format": "markdown",
                    "languages": ["he", "en"],
                    "force_ocr": False,
                }
            )
            self._converter = PdfConverter(config=config)
        return self._converter

    def convert(self, pdf_path: Path) -> str:
        """Convert a PDF file to Markdown text."""
        logger.info(f"Converting with Marker: {pdf_path.name}")
        converter = self._get_converter()
        result = converter(str(pdf_path))
        markdown = result.markdown if hasattr(result, "markdown") else str(result)
        logger.info(f"Converted {pdf_path.name}: {len(markdown)} chars")
        return markdown


def convert_with_marker(pdf_path: Path) -> str:
    """Convenience function for one-off conversion."""
    converter = MarkerConverter()
    return converter.convert(pdf_path)
