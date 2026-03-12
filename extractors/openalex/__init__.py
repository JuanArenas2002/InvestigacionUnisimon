"""
Paquete extractors.openalex

Re-exporta las clases públicas para compatibilidad con el código
existente que importa directamente desde 'extractors.openalex':

    from extractors.openalex import OpenAlexExtractor
    from extractors.openalex import OpenAlexEnricher, OpenAlexRateLimitError

Sub-módulos:
  _rate_limit  — Excepciones de cuota y utilidad extract_retry_after().
  extractor    — OpenAlexExtractor (extracción masiva por ROR + búsqueda DOI).
  enricher     — OpenAlexEnricher  (enriquecimiento de listados Excel).
"""
from .extractor import OpenAlexExtractor
from .enricher import OpenAlexEnricher
from ._rate_limit import OpenAlexAPIError, OpenAlexRateLimitError

__all__ = [
    "OpenAlexExtractor",
    "OpenAlexEnricher",
    "OpenAlexAPIError",
    "OpenAlexRateLimitError",
]
