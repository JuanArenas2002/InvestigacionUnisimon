"""
Paquete extractor de Scopus — base de datos bibliográfica de Elsevier.

La Scopus Search API permite buscar publicaciones por afiliación institucional
(AF-ID), ORCID de autor, DOI, o cualquier combinación de campos avanzados.

Estructura interna (arquitectura DDD):
  domain/         → lógica pura: query_builder (operadores Scopus), record_parser (XML/JSON)
  application/    → orquestación: search_service (paginación, DOI lookup, batch)
  infrastructure/ → I/O: http_client (sesión con API Key e Inst Token)
  extractor.py    → implementación de BaseExtractor + métodos de utilidad

Punto de entrada público. Re-exporta los símbolos que el resto del
proyecto importa, manteniendo compatibilidad total con todos los
imports existentes:

    from extractors.scopus import ScopusExtractor
    from extractors.scopus import ScopusExtractor, ScopusAPIError
"""

from extractors.scopus.extractor import ScopusExtractor
from extractors.scopus._exceptions import ScopusAPIError

__all__ = ["ScopusExtractor", "ScopusAPIError"]
