"""
Paquete extractor de Google Scholar.

Google Scholar no tiene API pública oficial. Este extractor usa la
librería `scholarly` (scraping) para obtener publicaciones desde perfiles
de autores.

Estructura interna (arquitectura DDD):
  domain/         → lógica pura: record_parser
  application/    → orquestación: profile_service (itera publicaciones del perfil)
  infrastructure/ → I/O: (sin estado, scholarly maneja la sesión)
  extractor.py    → implementación de BaseExtractor

Punto de entrada público:

    from extractors.google_scholar import GoogleScholarExtractor
    from extractors.google_scholar import GoogleScholarExtractor, GoogleScholarError

Prerequisito:
    pip install scholarly
"""

from extractors.google_scholar.extractor import GoogleScholarExtractor
from extractors.google_scholar._exceptions import GoogleScholarError

__all__ = ["GoogleScholarExtractor", "GoogleScholarError"]
