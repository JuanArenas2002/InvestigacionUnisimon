"""
Paquete extractor de Web of Science (WoS) — Clarivate Analytics.

Estructura interna (arquitectura DDD):
  domain/         → lógica pura: query_builder, record_parser
  application/    → orquestación: search_service (paginación)
  infrastructure/ → I/O: http_client (sesión autenticada)
  extractor.py    → implementación de BaseExtractor (glue delgado)

Punto de entrada público. Re-exporta los símbolos que el resto del
proyecto importa, manteniendo compatibilidad total con todos los
imports existentes:

    from extractors.wos import WosExtractor
    from extractors.wos import WosExtractor, WosAPIError
"""

from extractors.wos.extractor import WosExtractor
from extractors.wos._exceptions import WosAPIError

__all__ = ["WosExtractor", "WosAPIError"]
