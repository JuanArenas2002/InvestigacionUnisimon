"""
Paquete extractor de CVLAC — Currículum Vitae de Latinoamérica y el Caribe.

Portal de Minciencias Colombia para registrar la producción científica
de investigadores nacionales. Funciona por web scraping (no hay API pública).

Estructura interna (arquitectura DDD):
  domain/         → lógica pura: html_parser (extracción HTML), record_parser
  application/    → orquestación: profile_service (itera secciones del perfil)
  infrastructure/ → I/O: http_client (sesión HTTP con User-Agent institucional)
  extractor.py    → implementación de BaseExtractor (glue delgado)

Punto de entrada público. Re-exporta los símbolos que el resto del
proyecto importa, manteniendo compatibilidad total con todos los
imports existentes:

    from extractors.cvlac import CvlacExtractor
    from extractors.cvlac import CvlacExtractor, CvlacScrapingError
"""

from extractors.cvlac.extractor import CvlacExtractor
from extractors.cvlac._exceptions import CvlacScrapingError

__all__ = ["CvlacExtractor", "CvlacScrapingError"]
