"""
Módulo compartido: utilidades reutilizadas por toda la aplicación.

Exporta los normalizadores comunes para que cualquier módulo pueda
importarlos desde un único lugar:

    from shared import normalize_doi, normalize_year
    from shared.normalizers import normalize_text
"""
from .normalizers import (
    normalize_doi,
    normalize_year,
    normalize_text,
    normalize_author_name,
    normalize_title_for_search,
)

__all__ = [
    "normalize_doi",
    "normalize_year",
    "normalize_text",
    "normalize_author_name",
    "normalize_title_for_search",
]
